/*-------------------------------------------------------------------------
 *
 * neon.c
 *	  Utility functions to expose neon specific information to user
 *
 * IDENTIFICATION
 *	 contrib/neon/neon.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "fmgr.h"

#include "miscadmin.h"
#include "access/subtrans.h"
#include "access/twophase.h"
#include "access/xlog.h"
#if PG_MAJORVERSION_NUM >= 15
#include "access/xlogrecovery.h"
#endif
#include "replication/logical.h"
#include "replication/logicallauncher.h"
#include "replication/slot.h"
#include "replication/walsender.h"
#include "storage/proc.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"
#include "utils/guc.h"
#include "utils/guc_tables.h"

#include "extension_server.h"
#include "neon.h"
#include "control_plane_connector.h"
#include "logical_replication_monitor.h"
#include "unstable_extensions.h"
#include "walsender_hooks.h"
#if PG_MAJORVERSION_NUM >= 16
#include "storage/ipc.h"
#endif

PG_MODULE_MAGIC;
void		_PG_init(void);


static int  running_xacts_overflow_policy;

#if PG_MAJORVERSION_NUM >= 16
static shmem_startup_hook_type prev_shmem_startup_hook;

static void neon_shmem_startup_hook(void);
#endif
#if PG_MAJORVERSION_NUM >= 17
uint32		WAIT_EVENT_NEON_LFC_MAINTENANCE;
uint32		WAIT_EVENT_NEON_LFC_READ;
uint32		WAIT_EVENT_NEON_LFC_TRUNCATE;
uint32		WAIT_EVENT_NEON_LFC_WRITE;
uint32		WAIT_EVENT_NEON_LFC_PREWARM_IO;
uint32		WAIT_EVENT_NEON_LFC_PREWARM_ACCESS;
uint32		WAIT_EVENT_NEON_PS_STARTING;
uint32		WAIT_EVENT_NEON_PS_CONFIGURING;
uint32		WAIT_EVENT_NEON_PS_SEND;
uint32		WAIT_EVENT_NEON_PS_READ;
uint32		WAIT_EVENT_NEON_WAL_DL;
#endif

enum RunningXactsOverflowPolicies {
	OP_IGNORE,
	OP_SKIP,
	OP_WAIT
};

static const struct config_enum_entry running_xacts_overflow_policies[] = {
	{"ignore", OP_IGNORE, false},
	{"skip", OP_SKIP, false},
	{"wait", OP_WAIT, false},
	{NULL, 0, false}
};

/*
 * XXX: These private to procarray.c, but we need them here.
 */
#define PROCARRAY_MAXPROCS	(MaxBackends + max_prepared_xacts)
#define TOTAL_MAX_CACHED_SUBXIDS \
	((PGPROC_MAX_CACHED_SUBXIDS + 1) * PROCARRAY_MAXPROCS)

/*
 * Restore running-xact information by scanning the CLOG at startup.
 *
 * In PostgreSQL, a standby always has to wait for a running-xacts WAL record
 * to arrive before it can start accepting queries. Furthermore, if there are
 * transactions with too many subxids (> 64) open to fit in the in-memory
 * subxids cache, the running-xacts record will be marked as "suboverflowed",
 * and the standby will need to also wait for the currently in-progress
 * transactions to finish.
 *
 * That's not great in PostgreSQL, because a hot standby does not necessary
 * open up for queries immediately as you might expect. But it's worse in
 * Neon: A standby in Neon doesn't need to start WAL replay from a checkpoint
 * record; it can start at any LSN. Postgres arranges things so that there is
 * a running-xacts record soon after every checkpoint record, but when you
 * start from an arbitrary LSN, that doesn't help. If the primary is idle, or
 * not running at all, it might never write a new running-xacts record,
 * leaving the replica in a limbo where it can never start accepting queries.
 *
 * To mitigate that, we have an additional mechanism to find the running-xacts
 * information: we scan the CLOG, making note of any XIDs not marked as
 * committed or aborted. They are added to the Postgres known-assigned XIDs
 * array by calling ProcArrayApplyRecoveryInfo() in the caller of this
 * function.
 *
 * There is one big limitation with that mechanism: The size of the
 * known-assigned XIDs is limited, so if there are a lot of in-progress XIDs,
 * we have to give up. Furthermore, we don't know how many of the in-progress
 * XIDs are subtransactions, and if we use up all the space in the
 * known-assigned XIDs array for subtransactions, we might run out of space in
 * the array later during WAL replay, causing the replica to shut down with
 * "ERROR: too many KnownAssignedXids". The safe # of XIDs that we can add to
 * the known-assigned array without risking that error later is very low,
 * merely PGPROC_MAX_CACHED_SUBXIDS == 64, so we take our chances and use up
 * to half of the known-assigned XIDs array for the subtransactions, even
 * though that risks getting the error later.
 *
 * Note: It's OK if the recovered list of XIDs includes some transactions that
 * have crashed in the primary, and hence will never commit. They will be seen
 * as in-progress, until we see a new next running-acts record with an
 * oldestActiveXid that invalidates them. That's how the known-assigned XIDs
 * array always works.
 *
 * If scraping the CLOG doesn't succeed for some reason, like the subxid
 * overflow, Postgres will fall back to waiting for a running-xacts record
 * like usual.
 *
 * Returns true if a complete list of in-progress XIDs was scraped.
 */
static bool
RestoreRunningXactsFromClog(CheckPoint *checkpoint, TransactionId **xids, int *nxids)
{
	TransactionId from;
	TransactionId till;
	int			max_xcnt;
	TransactionId *prepared_xids = NULL;
	int			n_prepared_xids;
	TransactionId *restored_xids = NULL;
	int			n_restored_xids;
	int			next_prepared_idx;

	Assert(*xids == NULL);

	/*
	 * If the checkpoint doesn't have a valid oldestActiveXid, bail out. We
	 * don't know where to start the scan.
	 *
	 * This shouldn't happen, because the pageserver always maintains a valid
	 * oldestActiveXid nowadays. Except when starting at an old point in time
	 * that was ingested before the pageserver was taught to do that.
	 */
	if (!TransactionIdIsValid(checkpoint->oldestActiveXid))
	{
		elog(LOG, "cannot restore running-xacts from CLOG because oldestActiveXid is not set");
		goto fail;
	}

	/*
	 * We will scan the CLOG starting from the oldest active XID.
	 *
	 * In some corner cases, the oldestActiveXid from the last checkpoint
	 * might already have been truncated from the CLOG. That is,
	 * oldestActiveXid might be older than oldestXid. That's possible because
	 * oldestActiveXid is only updated at checkpoints. After the last
	 * checkpoint, the oldest transaction might have committed, and the CLOG
	 * might also have been already truncated. So if oldestActiveXid is older
	 * than oldestXid, start at oldestXid instead. (Otherwise we'd try to
	 * access CLOG segments that have already been truncated away.)
	 */
	from = TransactionIdPrecedes(checkpoint->oldestXid, checkpoint->oldestActiveXid)
		? checkpoint->oldestActiveXid : checkpoint->oldestXid;
	till = XidFromFullTransactionId(checkpoint->nextXid);

	/*
	 * To avoid "too many KnownAssignedXids" error later during replay, we
	 * limit number of collected transactions. This is a tradeoff: if we are
	 * willing to consume more of the KnownAssignedXids space for the XIDs
	 * now, that allows us to start up, but we might run out of space later.
	 *
	 * The size of the KnownAssignedXids array is TOTAL_MAX_CACHED_SUBXIDS,
	 * which is (PGPROC_MAX_CACHED_SUBXIDS + 1) * PROCARRAY_MAXPROCS). In
	 * PostgreSQL, that's always enough because the primary will always write
	 * an XLOG_XACT_ASSIGNMENT record if a transaction has more than
	 * PGPROC_MAX_CACHED_SUBXIDS subtransactions. Seeing that record allows
	 * the standby to mark the XIDs in pg_subtrans and removing them from the
	 * KnowingAssignedXids array.
	 *
	 * Here, we don't know which XIDs belong to subtransactions that have
	 * already been WAL-logged with an XLOG_XACT_ASSIGNMENT record. If we
	 * wanted to be totally safe and avoid the possibility of getting a "too
	 * many KnownAssignedXids" error later, we would have to limit ourselves
	 * to PGPROC_MAX_CACHED_SUBXIDS, which is not much. And that includes top
	 * transaction IDs too, because we cannot distinguish between top
	 * transaction IDs and subtransactions here.
	 *
	 * Somewhat arbitrarily, we use up to half of KnownAssignedXids. That
	 * strikes a sensible balance between being useful, and risking a "too
	 * many KnownAssignedXids" error later.
	 */
	max_xcnt = TOTAL_MAX_CACHED_SUBXIDS / 2;

	/*
	 * Collect XIDs of prepared transactions in an array. This includes only
	 * their top-level XIDs. We assume that StandbyRecoverPreparedTransactions
	 * has already been called, so we can find all the sub-transactions in
	 * pg_subtrans.
	 */
	PrescanPreparedTransactions(&prepared_xids, &n_prepared_xids);
	qsort(prepared_xids, n_prepared_xids, sizeof(TransactionId), xidLogicalComparator);

	/*
	 * Scan the CLOG, collecting in-progress XIDs into 'restored_xids'.
	 */
	elog(DEBUG1, "scanning CLOG between %u and %u for in-progress XIDs", from, till);
	restored_xids = (TransactionId *) palloc(max_xcnt * sizeof(TransactionId));
	n_restored_xids = 0;
	next_prepared_idx = 0;

	for (TransactionId xid = from; xid != till;)
	{
		XLogRecPtr	xidlsn;
		XidStatus	xidstatus;

		xidstatus = TransactionIdGetStatus(xid, &xidlsn);

		/*
		 * "Merge" the prepared transactions into the restored_xids array as
		 * we go.  The prepared transactions array is sorted. This is mostly
		 * a sanity check to ensure that all the prepared transactions are
		 * seen as in-progress. (There is a check after the loop that we didn't
		 * miss any.)
		 */
		if (next_prepared_idx < n_prepared_xids && xid == prepared_xids[next_prepared_idx])
		{
			/*
			 * This is a top-level transaction ID of a prepared transaction.
			 * Include it in the array.
			 */

			/* sanity check */
			if (xidstatus != TRANSACTION_STATUS_IN_PROGRESS)
			{
				elog(LOG, "prepared transaction %u has unexpected status %X, cannot restore running-xacts from CLOG",
					 xid, xidstatus);
				Assert(false);
				goto fail;
			}

			elog(DEBUG1, "XID %u: was next prepared xact (%d / %d)", xid, next_prepared_idx, n_prepared_xids);
			next_prepared_idx++;
		}
		else if (xidstatus == TRANSACTION_STATUS_COMMITTED)
		{
			elog(DEBUG1, "XID %u: was committed", xid);
			goto skip;
		}
		else if (xidstatus == TRANSACTION_STATUS_ABORTED)
		{
			elog(DEBUG1, "XID %u: was aborted", xid);
			goto skip;
		}
		else if (xidstatus == TRANSACTION_STATUS_IN_PROGRESS)
		{
			/*
			 * In-progress transactions are included in the array.
			 *
			 * Except subtransactions of the prepared transactions. They are
			 * already set in pg_subtrans, and hence don't need to be tracked
			 * in the known-assigned XIDs array.
			 */
			if (n_prepared_xids > 0)
			{
				TransactionId parent = SubTransGetParent(xid);

				if (TransactionIdIsValid(parent))
				{
					/*
					 * This is a subtransaction belonging to a prepared
					 * transaction.
					 *
					 * Sanity check that it is in the prepared XIDs array. It
					 * should be, because StandbyRecoverPreparedTransactions
					 * populated pg_subtrans, and no other XID should be set
					 * in it yet. (This also relies on the fact that
					 * StandbyRecoverPreparedTransactions sets the parent of
					 * each subxid to point directly to the top-level XID,
					 * rather than restoring the original subtransaction
					 * hierarchy.)
					 */
					if (bsearch(&parent, prepared_xids, next_prepared_idx,
								sizeof(TransactionId), xidLogicalComparator) == NULL)
					{
						elog(LOG, "sub-XID %u has unexpected parent %u, cannot restore running-xacts from CLOG",
							 xid, parent);
						Assert(false);
						goto fail;
					}
					elog(DEBUG1, "XID %u: was a subtransaction of prepared xid %u", xid, parent);
					goto skip;
				}
			}

			/* include it in the array */
			elog(DEBUG1, "XID %u: is in progress", xid);
		}
		else
		{
			/*
			 * SUB_COMMITTED is a transient state used at commit. We don't
			 * expect to see that here.
			 */
			elog(LOG, "XID %u has unexpected status %X in pg_xact, cannot restore running-xacts from CLOG",
				 xid, xidstatus);
			Assert(false);
			goto fail;
		}

		if (n_restored_xids >= max_xcnt)
		{
			/*
			 * Overflowed. We won't be able to install the RunningTransactions
			 * snapshot.
			 */
			elog(LOG, "too many running xacts to restore from the CLOG; oldestXid=%u oldestActiveXid=%u nextXid %u",
				 checkpoint->oldestXid, checkpoint->oldestActiveXid,
				 XidFromFullTransactionId(checkpoint->nextXid));

			switch (running_xacts_overflow_policy)
			{
				case OP_WAIT:
					goto fail;
				case OP_IGNORE:
					goto success;
				case OP_SKIP:
					n_restored_xids = 0;
					goto success;
			}
		}

		restored_xids[n_restored_xids++] = xid;

	skip:
		TransactionIdAdvance(xid);
	}

	/* sanity check */
	if (next_prepared_idx != n_prepared_xids)
	{
		elog(LOG, "prepared transaction ID %u was not visited in the CLOG scan, cannot restore running-xacts from CLOG",
			 prepared_xids[next_prepared_idx]);
		Assert(false);
		goto fail;
	}
   success:
	elog(LOG, "restored %d running xacts by scanning the CLOG; oldestXid=%u oldestActiveXid=%u nextXid %u",
		 n_restored_xids, checkpoint->oldestXid, checkpoint->oldestActiveXid, XidFromFullTransactionId(checkpoint->nextXid));
	*nxids = n_restored_xids;
	*xids = restored_xids;
	if (prepared_xids)
		pfree(prepared_xids);
	return true;

 fail:
	*nxids = 0;
	*xids = NULL;
	if (restored_xids)
		pfree(restored_xids);
	if (prepared_xids)
		pfree(prepared_xids);
	return false;
}


/*
 * pgbouncer is able to track GUCs reported by Postgres.
 * But most parameters cannot be tracked this way. The only parameters that can be tracked are ones
 * that Postgres reports to the client. Unfortunately `search_path` is not reported by Postgres:
 * https://www.postgresql.org/message-id/flat/CAGECzQQ6xFcgrg%2Be0p9mCumtK362TiA6vTiiZKoYbS8OXggwuQ%40mail.gmail.com#be4bfd7a9cf1f0633bdb2d1790a0a1be
 * This code sets GUC_REPORT flag for `search_path`making it possible to include it in
 * pgbouncer's `track_extra_parameters` list.
 *
 * This code is inspired by how the Citus extension does this, see
 * https://github.com/citusdata/citus/blob/2a263fe69a707d16ef24378f7650742386b0968f/src/backend/distributed/shared_library_init.c#L2694
 */
static void
ReportSearchPath(void)
{
#if PG_VERSION_NUM >= 160000
	int nGucs = 0;
	struct config_generic **gucs = get_guc_variables(&nGucs);
#else
	struct config_generic **gucs = get_guc_variables();
	int nGucs = GetNumConfigOptions();
#endif

	for (int i = 0; i < nGucs; i++)
	{
		struct config_generic *guc = (struct config_generic *) gucs[i];

		if (strcmp(guc->name, "search_path") == 0)
		{
			guc->flags |= GUC_REPORT;
		}
	}
}

void
_PG_init(void)
{
	/*
	 * Also load 'neon_rmgr'. This makes it unnecessary to list both 'neon'
	 * and 'neon_rmgr' in shared_preload_libraries.
	 */
#if PG_VERSION_NUM >= 160000
	load_file("$libdir/neon_rmgr", false);

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = neon_shmem_startup_hook;
#endif

	pg_init_libpagestore();
	pg_init_walproposer();
	Custom_XLogReaderRoutines = NeonOnDemandXLogReaderRoutines;

	InitUnstableExtensionsSupport();
	InitLogicalReplicationMonitor();
	InitControlPlaneConnector();

	pg_init_extension_server();

	restore_running_xacts_callback = RestoreRunningXactsFromClog;

	DefineCustomBoolVariable(
							"neon.disable_logical_replication_subscribers",
							"Disables incomming logical replication",
							NULL,
							&disable_logical_replication_subscribers,
							false,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	DefineCustomBoolVariable(
							"neon.allow_replica_misconfig",
							"Allow replica startup when some critical GUCs have smaller value than on primary node",
							NULL,
							&allowReplicaMisconfig,
							true,
							PGC_POSTMASTER,
							0,
							NULL, NULL, NULL);

	DefineCustomEnumVariable(
							"neon.running_xacts_overflow_policy",
							"Action performed on snapshot overflow when restoring runnings xacts from CLOG",
							NULL,
							&running_xacts_overflow_policy,
							OP_IGNORE,
							running_xacts_overflow_policies,
							PGC_POSTMASTER,
							0,
							NULL, NULL, NULL);

	/*
	 * Important: This must happen after other parts of the extension are
	 * loaded, otherwise any settings to GUCs that were set before the
	 * extension was loaded will be removed.
	 */
	EmitWarningsOnPlaceholders("neon");

	ReportSearchPath();
}

PG_FUNCTION_INFO_V1(pg_cluster_size);
PG_FUNCTION_INFO_V1(backpressure_lsns);
PG_FUNCTION_INFO_V1(backpressure_throttling_time);

Datum
pg_cluster_size(PG_FUNCTION_ARGS)
{
	int64		size;

	size = GetNeonCurrentClusterSize();

	if (size == 0)
		PG_RETURN_NULL();

	PG_RETURN_INT64(size);
}

Datum
backpressure_lsns(PG_FUNCTION_ARGS)
{
	XLogRecPtr	writePtr;
	XLogRecPtr	flushPtr;
	XLogRecPtr	applyPtr;
	Datum		values[3];
	bool		nulls[3];
	TupleDesc	tupdesc;

	replication_feedback_get_lsns(&writePtr, &flushPtr, &applyPtr);

	tupdesc = CreateTemplateTupleDesc(3);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "received_lsn", PG_LSNOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "disk_consistent_lsn", PG_LSNOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "remote_consistent_lsn", PG_LSNOID, -1, 0);
	tupdesc = BlessTupleDesc(tupdesc);

	MemSet(nulls, 0, sizeof(nulls));
	values[0] = LSNGetDatum(writePtr);
	values[1] = LSNGetDatum(flushPtr);
	values[2] = LSNGetDatum(applyPtr);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

Datum
backpressure_throttling_time(PG_FUNCTION_ARGS)
{
	PG_RETURN_UINT64(BackpressureThrottlingTime());
}

#if PG_MAJORVERSION_NUM >= 16
static void
neon_shmem_startup_hook(void)
{
	/* Initialize */
	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

#if PG_MAJORVERSION_NUM >= 17
	WAIT_EVENT_NEON_LFC_MAINTENANCE = WaitEventExtensionNew("Neon/FileCache_Maintenance");
	WAIT_EVENT_NEON_LFC_READ = WaitEventExtensionNew("Neon/FileCache_Read");
	WAIT_EVENT_NEON_LFC_TRUNCATE = WaitEventExtensionNew("Neon/FileCache_Truncate");
	WAIT_EVENT_NEON_LFC_WRITE = WaitEventExtensionNew("Neon/FileCache_Write");
	WAIT_EVENT_NEON_LFC_PREWARM_IO = WaitEventExtensionNew("Neon/FileCachePrewarm_ConcurrentWrites");
	WAIT_EVENT_NEON_LFC_PREWARM_ACCESS = WaitEventExtensionNew("Neon/FileCachePrewarmAccess");
	WAIT_EVENT_NEON_PS_STARTING = WaitEventExtensionNew("Neon/PS_Starting");
	WAIT_EVENT_NEON_PS_CONFIGURING = WaitEventExtensionNew("Neon/PS_Configuring");
	WAIT_EVENT_NEON_PS_SEND = WaitEventExtensionNew("Neon/PS_SendIO");
	WAIT_EVENT_NEON_PS_READ = WaitEventExtensionNew("Neon/PS_ReadIO");
	WAIT_EVENT_NEON_WAL_DL = WaitEventExtensionNew("Neon/WAL_Download");
#endif
}
#endif
