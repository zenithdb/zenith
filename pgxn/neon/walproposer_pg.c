/*
 * Implementation of postgres based walproposer disk and IO routines, i.e. the
 * real ones. The reason this is separate from walproposer.c is ability to
 * replace them with mocks, allowing to do simulation testing.
 *
 * Also contains initialization of postgres based walproposer.
 */

#include "postgres.h"

#include <signal.h>
#include <unistd.h>
#include <sys/stat.h>
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "access/xlogutils.h"
#include "access/xloginsert.h"
#if PG_VERSION_NUM >= 150000
#include "access/xlogrecovery.h"
#endif
#include "storage/fd.h"
#include "storage/latch.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "access/xlog.h"
#include "libpq/pqformat.h"
#include "replication/slot.h"
#include "replication/walreceiver.h"
#include "replication/walsender_private.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "postmaster/postmaster.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/timestamp.h"

#include "libpq-fe.h"

#include "libpqwalproposer.h"
#include "neon.h"
#include "neon_walreader.h"
#include "walproposer.h"

#define XLOG_HDR_SIZE (1 + 8 * 3)	/* 'w' + startPos + walEnd + timestamp */
#define XLOG_HDR_START_POS 1	/* offset of start position in wal sender*
								 * message header */

#define MB ((XLogRecPtr)1024 * 1024)

#define WAL_PROPOSER_SLOT_NAME "wal_proposer_slot"

/* GUCs */
char	   *wal_acceptors_list = "";
int			wal_acceptor_reconnect_timeout = 1000;
int			wal_acceptor_connection_timeout = 10000;
int			safekeeper_protocol_version = 2;

/* Set to true in the walproposer bgw. */
static bool am_walproposer;
static WalproposerShmemState *walprop_shared;
static WalProposerConfig walprop_config;
static XLogRecPtr sentPtr = InvalidXLogRecPtr;
static const walproposer_api walprop_pg;
static volatile sig_atomic_t got_SIGUSR2 = false;
static bool reported_sigusr2 = false;

static XLogRecPtr standby_flush_lsn = InvalidXLogRecPtr;
static XLogRecPtr standby_apply_lsn = InvalidXLogRecPtr;
static HotStandbyFeedback agg_hs_feedback;

static void nwp_shmem_startup_hook(void);
static void nwp_register_gucs(void);
static void assign_neon_safekeepers(const char *newval, void *extra);
static void nwp_prepare_shmem(void);
static uint64 backpressure_lag_impl(void);
static uint64 startup_backpressure_wrap(void);
static bool backpressure_throttling_impl(void);
static void walprop_register_bgworker(void);

static void walprop_pg_init_standalone_sync_safekeepers(void);
static void walprop_pg_init_walsender(void);
static void walprop_pg_init_bgworker(void);
static TimestampTz walprop_pg_get_current_timestamp(WalProposer *wp);
static void walprop_pg_load_libpqwalreceiver(void);

static process_interrupts_callback_t PrevProcessInterruptsCallback = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook_type;
#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static void walproposer_shmem_request(void);
#endif
static void WalproposerShmemInit_SyncSafekeeper(void);


static void StartProposerReplication(WalProposer *wp, StartReplicationCmd *cmd);
static void WalSndLoop(WalProposer *wp);
static void XLogBroadcastWalProposer(WalProposer *wp);

static void add_nwr_event_set(Safekeeper *sk, uint32 events);
static void update_nwr_event_set(Safekeeper *sk, uint32 events);
static void rm_safekeeper_event_set(Safekeeper *to_remove, bool is_sk);

static void CheckGracefulShutdown(WalProposer *wp);

static void
init_walprop_config(bool syncSafekeepers)
{
	walprop_config.neon_tenant = neon_tenant;
	walprop_config.neon_timeline = neon_timeline;
	/* WalProposerCreate scribbles directly on it, so pstrdup */
	walprop_config.safekeepers_list = pstrdup(wal_acceptors_list);
	walprop_config.safekeeper_reconnect_timeout = wal_acceptor_reconnect_timeout;
	walprop_config.safekeeper_connection_timeout = wal_acceptor_connection_timeout;
	walprop_config.wal_segment_size = wal_segment_size;
	walprop_config.syncSafekeepers = syncSafekeepers;
	if (!syncSafekeepers)
		walprop_config.systemId = GetSystemIdentifier();
	else
		walprop_config.systemId = 0;
	walprop_config.pgTimeline = walprop_pg_get_timeline_id();
	walprop_config.proto_version = safekeeper_protocol_version;
}

/*
 * Entry point for `postgres --sync-safekeepers`.
 */
PGDLLEXPORT void
WalProposerSync(int argc, char *argv[])
{
	WalProposer *wp;

	init_walprop_config(true);
	WalproposerShmemInit_SyncSafekeeper();
	walprop_pg_init_standalone_sync_safekeepers();
	walprop_pg_load_libpqwalreceiver();

	wp = WalProposerCreate(&walprop_config, walprop_pg);

	WalProposerStart(wp);
}

/*
 * WAL proposer bgworker entry point.
 */
PGDLLEXPORT void
WalProposerMain(Datum main_arg)
{
	WalProposer *wp;

	init_walprop_config(false);
	walprop_pg_init_bgworker();
	am_walproposer = true;
	walprop_pg_load_libpqwalreceiver();

	wp = WalProposerCreate(&walprop_config, walprop_pg);
	wp->last_reconnect_attempt = walprop_pg_get_current_timestamp(wp);

	walprop_pg_init_walsender();
	WalProposerStart(wp);
}

/*
 * Initialize GUCs, bgworker, shmem and backpressure.
 */
void
pg_init_walproposer(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	nwp_register_gucs();

	nwp_prepare_shmem();

	delay_backend_us = &startup_backpressure_wrap;
	PrevProcessInterruptsCallback = ProcessInterruptsCallback;
	ProcessInterruptsCallback = backpressure_throttling_impl;

	walprop_register_bgworker();
}

static void
nwp_register_gucs(void)
{
	DefineCustomStringVariable(
							   "neon.safekeepers",
							   "List of Neon WAL acceptors (host:port)",
							   NULL,	/* long_desc */
							   &wal_acceptors_list, /* valueAddr */
							   "",	/* bootValue */
							   PGC_SIGHUP,
							   GUC_LIST_INPUT,	/* extensions can't use*
												 * GUC_LIST_QUOTE */
							   NULL, assign_neon_safekeepers, NULL);

	DefineCustomIntVariable(
							"neon.safekeeper_reconnect_timeout",
							"Walproposer reconnects to offline safekeepers once in this interval.",
							NULL,
							&wal_acceptor_reconnect_timeout,
							1000, 0, INT_MAX,	/* default, min, max */
							PGC_SIGHUP, /* context */
							GUC_UNIT_MS,	/* flags */
							NULL, NULL, NULL);

	DefineCustomIntVariable(
							"neon.safekeeper_connect_timeout",
							"Connection or connection attempt to safekeeper is terminated if no message is received (or connection attempt doesn't finish) within this period.",
							NULL,
							&wal_acceptor_connection_timeout,
							10000, 0, INT_MAX,
							PGC_SIGHUP,
							GUC_UNIT_MS,
							NULL, NULL, NULL);

	DefineCustomIntVariable(
							"neon.safekeeper_protocol_version",
							"Version of compute <-> safekeeper protocol.",
							"Used while migrating from 2 to 3.",
							&safekeeper_protocol_version,
							2, 0, INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL, NULL, NULL);
}


static int
split_safekeepers_list(char *safekeepers_list, char *safekeepers[])
{
	int			n_safekeepers = 0;
	char	   *curr_sk = safekeepers_list;

	for (char *coma = safekeepers_list; coma != NULL && *coma != '\0'; curr_sk = coma)
	{
		if (++n_safekeepers >= MAX_SAFEKEEPERS)
		{
			wpg_log(FATAL, "too many safekeepers");
		}

		coma = strchr(coma, ',');
		safekeepers[n_safekeepers - 1] = curr_sk;

		if (coma != NULL)
		{
			*coma++ = '\0';
		}
	}

	return n_safekeepers;
}

/*
 * Accept two coma-separated strings with list of safekeeper host:port addresses.
 * Split them into arrays and return false if two sets do not match, ignoring the order.
 */
static bool
safekeepers_cmp(char *old, char *new)
{
	char	   *safekeepers_old[MAX_SAFEKEEPERS];
	char	   *safekeepers_new[MAX_SAFEKEEPERS];
	int			len_old = 0;
	int			len_new = 0;

	len_old = split_safekeepers_list(old, safekeepers_old);
	len_new = split_safekeepers_list(new, safekeepers_new);

	if (len_old != len_new)
	{
		return false;
	}

	qsort(&safekeepers_old, len_old, sizeof(char *), pg_qsort_strcmp);
	qsort(&safekeepers_new, len_new, sizeof(char *), pg_qsort_strcmp);

	for (int i = 0; i < len_new; i++)
	{
		if (strcmp(safekeepers_old[i], safekeepers_new[i]) != 0)
		{
			return false;
		}
	}

	return true;
}

/*
 * GUC assign_hook for neon.safekeepers. Restarts walproposer through FATAL if
 * the list changed.
 */
static void
assign_neon_safekeepers(const char *newval, void *extra)
{
	char	   *newval_copy;
	char	   *oldval;

	if (!am_walproposer)
		return;

	if (!newval)
	{
		/* should never happen */
		wpg_log(FATAL, "neon.safekeepers is empty");
	}

	/* Copy values because we will modify them in split_safekeepers_list() */
	newval_copy = pstrdup(newval);
	oldval = pstrdup(wal_acceptors_list);

	/*
	 * TODO: restarting through FATAL is stupid and introduces 1s delay before
	 * next bgw start. We should refactor walproposer to allow graceful exit
	 * and thus remove this delay. XXX: If you change anything here, sync with
	 * test_safekeepers_reconfigure_reorder.
	 */
	if (!safekeepers_cmp(oldval, newval_copy))
	{
		wpg_log(FATAL, "restarting walproposer to change safekeeper list from %s to %s",
				wal_acceptors_list, newval);
	}
	pfree(newval_copy);
	pfree(oldval);
}

/* Check if we need to suspend inserts because of lagging replication. */
static uint64
backpressure_lag_impl(void)
{
	if (max_replication_apply_lag > 0 || max_replication_flush_lag > 0 || max_replication_write_lag > 0)
	{
		XLogRecPtr	writePtr;
		XLogRecPtr	flushPtr;
		XLogRecPtr	applyPtr;
#if PG_VERSION_NUM >= 150000
		XLogRecPtr	myFlushLsn = GetFlushRecPtr(NULL);
#else
		XLogRecPtr	myFlushLsn = GetFlushRecPtr();
#endif
		replication_feedback_get_lsns(&writePtr, &flushPtr, &applyPtr);

		elog(DEBUG2, "current flushLsn %X/%X PageserverFeedback: write %X/%X flush %X/%X apply %X/%X",
			 LSN_FORMAT_ARGS(myFlushLsn),
			 LSN_FORMAT_ARGS(writePtr),
			 LSN_FORMAT_ARGS(flushPtr),
			 LSN_FORMAT_ARGS(applyPtr));

		if ((writePtr != InvalidXLogRecPtr && max_replication_write_lag > 0 && myFlushLsn > writePtr + max_replication_write_lag * MB))
		{
			return (myFlushLsn - writePtr - max_replication_write_lag * MB);
		}

		if ((flushPtr != InvalidXLogRecPtr && max_replication_flush_lag > 0 && myFlushLsn > flushPtr + max_replication_flush_lag * MB))
		{
			return (myFlushLsn - flushPtr - max_replication_flush_lag * MB);
		}

		if ((applyPtr != InvalidXLogRecPtr && max_replication_apply_lag > 0 && myFlushLsn > applyPtr + max_replication_apply_lag * MB))
		{
			return (myFlushLsn - applyPtr - max_replication_apply_lag * MB);
		}
	}
	return 0;
}

/*
 * We don't apply backpressure when we're the postmaster, or the startup
 * process, because in postmaster we can't apply backpressure, and in
 * the startup process we can't afford to slow down.
 */
static uint64
startup_backpressure_wrap(void)
{
	if (AmStartupProcess() || !IsUnderPostmaster)
		return 0;

	delay_backend_us = &backpressure_lag_impl;

	return backpressure_lag_impl();
}

/*
 * WalproposerShmemSize --- report amount of shared memory space needed
 */
static Size
WalproposerShmemSize(void)
{
	return sizeof(WalproposerShmemState);
}

static bool
WalproposerShmemInit(void)
{
	bool		found;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	walprop_shared = ShmemInitStruct("Walproposer shared state",
									 sizeof(WalproposerShmemState),
									 &found);

	if (!found)
	{
		memset(walprop_shared, 0, WalproposerShmemSize());
		SpinLockInit(&walprop_shared->mutex);
		pg_atomic_init_u64(&walprop_shared->propEpochStartLsn, 0);
		pg_atomic_init_u64(&walprop_shared->mineLastElectedTerm, 0);
		pg_atomic_init_u64(&walprop_shared->backpressureThrottlingTime, 0);
		pg_atomic_init_u64(&walprop_shared->currentClusterSize, 0);
	}
	LWLockRelease(AddinShmemInitLock);

	return found;
}

static void
WalproposerShmemInit_SyncSafekeeper(void)
{
	walprop_shared = palloc(WalproposerShmemSize());
	memset(walprop_shared, 0, WalproposerShmemSize());
	SpinLockInit(&walprop_shared->mutex);
	pg_atomic_init_u64(&walprop_shared->propEpochStartLsn, 0);
	pg_atomic_init_u64(&walprop_shared->mineLastElectedTerm, 0);
	pg_atomic_init_u64(&walprop_shared->backpressureThrottlingTime, 0);
}

#define BACK_PRESSURE_DELAY 10000L // 0.01 sec

static bool
backpressure_throttling_impl(void)
{
	uint64		lag;
	TimestampTz start,
				stop;
	bool		retry = false;
	char	   *new_status = NULL;
	const char *old_status;
	int			len;

	if (PointerIsValid(PrevProcessInterruptsCallback))
		retry = PrevProcessInterruptsCallback();

	/*
	 * Don't throttle read only transactions or wal sender. Do throttle CREATE
	 * INDEX CONCURRENTLY, however. It performs some stages outside a
	 * transaction, even though it writes a lot of WAL. Check PROC_IN_SAFE_IC
	 * flag to cover that case.
	 */
	if (am_walsender
		|| (!(MyProc->statusFlags & PROC_IN_SAFE_IC)
			&& !TransactionIdIsValid(GetCurrentTransactionIdIfAny())))
		return retry;

	/* Calculate replicas lag */
	lag = backpressure_lag_impl();
	if (lag == 0)
		return retry;


	old_status = get_ps_display(&len);
	new_status = (char *) palloc(len + 64 + 1);
	memcpy(new_status, old_status, len);
	snprintf(new_status + len, 64, "backpressure throttling: lag %lu", lag);
	set_ps_display(new_status);
	new_status[len] = '\0';		/* truncate off " backpressure ..." to later
								 * reset the ps */

	elog(DEBUG2, "backpressure throttling: lag %lu", lag);
	start = GetCurrentTimestamp();
	pg_usleep(BACK_PRESSURE_DELAY);
	stop = GetCurrentTimestamp();
	pg_atomic_add_fetch_u64(&walprop_shared->backpressureThrottlingTime, stop - start);

	/* Reset ps display */
	set_ps_display(new_status);
	pfree(new_status);

	return true;
}

uint64
BackpressureThrottlingTime(void)
{
	return pg_atomic_read_u64(&walprop_shared->backpressureThrottlingTime);
}

/*
 * Register a background worker proposing WAL to wal acceptors.
 */
static void
walprop_register_bgworker(void)
{
	BackgroundWorker bgw;

	/* If no wal acceptors are specified, don't start the background worker. */
	if (*wal_acceptors_list == '\0')
		return;

	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN, "neon");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "WalProposerMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN, "WAL proposer");
	snprintf(bgw.bgw_type, BGW_MAXLEN, "WAL proposer");
	bgw.bgw_restart_time = 1;
	bgw.bgw_notify_pid = 0;
	bgw.bgw_main_arg = (Datum) 0;

	RegisterBackgroundWorker(&bgw);
}

/* shmem handling */

static void
nwp_prepare_shmem(void)
{
#if PG_VERSION_NUM >= 150000
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = walproposer_shmem_request;
#else
	RequestAddinShmemSpace(WalproposerShmemSize());
#endif
	prev_shmem_startup_hook_type = shmem_startup_hook;
	shmem_startup_hook = nwp_shmem_startup_hook;
}

#if PG_VERSION_NUM >= 150000
/*
 * shmem_request hook: request additional shared resources.  We'll allocate or
 * attach to the shared resources in nwp_shmem_startup_hook().
 */
static void
walproposer_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(WalproposerShmemSize());
}
#endif

static void
nwp_shmem_startup_hook(void)
{
	if (prev_shmem_startup_hook_type)
		prev_shmem_startup_hook_type();

	WalproposerShmemInit();
}

WalproposerShmemState *
GetWalpropShmemState(void)
{
	Assert(walprop_shared != NULL);
	return walprop_shared;
}

static WalproposerShmemState *
walprop_pg_get_shmem_state(WalProposer *wp)
{
	Assert(walprop_shared != NULL);
	return walprop_shared;
}

/*
 * Record new ps_feedback in the array with shards and update min_feedback.
 */
static PageserverFeedback
record_pageserver_feedback(PageserverFeedback *ps_feedback)
{
	PageserverFeedback min_feedback;

	Assert(ps_feedback->present);
	Assert(ps_feedback->shard_number < MAX_SHARDS);

	SpinLockAcquire(&walprop_shared->mutex);

	/* Update the number of shards */
	if (ps_feedback->shard_number + 1 > walprop_shared->num_shards)
		walprop_shared->num_shards = ps_feedback->shard_number + 1;

	/* Update the feedback */
	memcpy(&walprop_shared->shard_ps_feedback[ps_feedback->shard_number], ps_feedback, sizeof(PageserverFeedback));

	/* Calculate min LSNs */
	memcpy(&min_feedback, ps_feedback, sizeof(PageserverFeedback));
	for (int i = 0; i < walprop_shared->num_shards; i++)
	{
		PageserverFeedback *feedback = &walprop_shared->shard_ps_feedback[i];

		if (feedback->present)
		{
			if (min_feedback.last_received_lsn == InvalidXLogRecPtr || feedback->last_received_lsn < min_feedback.last_received_lsn)
				min_feedback.last_received_lsn = feedback->last_received_lsn;

			if (min_feedback.disk_consistent_lsn == InvalidXLogRecPtr || feedback->disk_consistent_lsn < min_feedback.disk_consistent_lsn)
				min_feedback.disk_consistent_lsn = feedback->disk_consistent_lsn;

			if (min_feedback.remote_consistent_lsn == InvalidXLogRecPtr || feedback->remote_consistent_lsn < min_feedback.remote_consistent_lsn)
				min_feedback.remote_consistent_lsn = feedback->remote_consistent_lsn;
		}
	}
	/* Copy min_feedback back to shmem */
	memcpy(&walprop_shared->min_ps_feedback, &min_feedback, sizeof(PageserverFeedback));

	SpinLockRelease(&walprop_shared->mutex);

	return min_feedback;
}

void
replication_feedback_get_lsns(XLogRecPtr *writeLsn, XLogRecPtr *flushLsn, XLogRecPtr *applyLsn)
{
	SpinLockAcquire(&walprop_shared->mutex);
	*writeLsn = walprop_shared->min_ps_feedback.last_received_lsn;
	*flushLsn = walprop_shared->min_ps_feedback.disk_consistent_lsn;
	*applyLsn = walprop_shared->min_ps_feedback.remote_consistent_lsn;
	SpinLockRelease(&walprop_shared->mutex);
}

/*
 * Start walproposer streaming replication
 */
static void
walprop_pg_start_streaming(WalProposer *wp, XLogRecPtr startpos)
{
	StartReplicationCmd cmd;

	wpg_log(LOG, "WAL proposer starts streaming at %X/%X",
			LSN_FORMAT_ARGS(startpos));
	cmd.slotname = WAL_PROPOSER_SLOT_NAME;
	cmd.timeline = wp->config->pgTimeline;
	cmd.startpoint = startpos;
	StartProposerReplication(wp, &cmd);
}

static void
walprop_pg_init_walsender(void)
{
	am_walsender = true;
	InitWalSender();
	InitProcessPhase2();

	/* Create replication slot for WAL proposer if not exists */
	if (SearchNamedReplicationSlot(WAL_PROPOSER_SLOT_NAME, false) == NULL)
	{
#if PG_MAJORVERSION_NUM >= 17
		ReplicationSlotCreate(WAL_PROPOSER_SLOT_NAME, false, RS_PERSISTENT,
							  false, false, false);
#else
		ReplicationSlotCreate(WAL_PROPOSER_SLOT_NAME, false, RS_PERSISTENT, false);
#endif
		ReplicationSlotReserveWal();
		/* Write this slot to disk */
		ReplicationSlotMarkDirty();
		ReplicationSlotSave();
		ReplicationSlotRelease();
	}
}

static void
walprop_pg_init_standalone_sync_safekeepers(void)
{
	struct stat stat_buf;

#if PG_VERSION_NUM < 150000
	ThisTimeLineID = 1;
#endif

	/*
	 * Initialize postmaster_alive_fds as WaitEventSet checks them.
	 *
	 * Copied from InitPostmasterDeathWatchHandle()
	 */
	if (pipe(postmaster_alive_fds) < 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg_internal("could not create pipe to monitor postmaster death: %m")));
	if (fcntl(postmaster_alive_fds[POSTMASTER_FD_WATCH], F_SETFL, O_NONBLOCK) == -1)
		ereport(FATAL,
				(errcode_for_socket_access(),
				 errmsg_internal("could not set postmaster death monitoring pipe to nonblocking mode: %m")));

	ChangeToDataDir();

	/* Create pg_wal directory, if it doesn't exist */
	if (stat(XLOGDIR, &stat_buf) != 0)
	{
		ereport(LOG, (errmsg("creating missing WAL directory \"%s\"", XLOGDIR)));
		if (MakePGDirectory(XLOGDIR) < 0)
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not create directory \"%s\": %m",
							XLOGDIR)));
			exit(1);
		}
	}
	BackgroundWorkerUnblockSignals();
}

/*
 * We pretend to be a walsender process, and the lifecycle of a walsender is
 * slightly different than other procesess. At shutdown, walsender processes
 * stay alive until the very end, after the checkpointer has written the
 * shutdown checkpoint. When the checkpointer exits, the postmaster sends all
 * remaining walsender processes SIGUSR2. On receiving SIGUSR2, we try to send
 * the remaining WAL, and then exit. This ensures that the checkpoint record
 * reaches durable storage (in safekeepers), before the server shuts down
 * completely.
 */
static void
walprop_sigusr2(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGUSR2 = true;
	SetLatch(MyLatch);
	errno = save_errno;
}

static void
walprop_pg_init_bgworker(void)
{
#if PG_VERSION_NUM >= 150000
	TimeLineID	tli;
#endif

	/* Establish signal handlers. */
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);
	pqsignal(SIGUSR2, walprop_sigusr2);

	BackgroundWorkerUnblockSignals();

	application_name = (char *) "walproposer";	/* for
												 * synchronous_standby_names */

#if PG_VERSION_NUM >= 150000
	/* FIXME pass proper tli to WalProposerInit ? */
	GetXLogReplayRecPtr(&tli);
#else
	GetXLogReplayRecPtr(&ThisTimeLineID);
#endif
}

static XLogRecPtr
walprop_pg_get_flush_rec_ptr(WalProposer *wp)
{
#if PG_MAJORVERSION_NUM < 15
	return GetFlushRecPtr();
#else
	return GetFlushRecPtr(NULL);
#endif
}

static TimestampTz
walprop_pg_get_current_timestamp(WalProposer *wp)
{
	return GetCurrentTimestamp();
}

TimeLineID
walprop_pg_get_timeline_id(void)
{
#if PG_VERSION_NUM >= 150000
	/* FIXME don't use hardcoded timeline id */
	return 1;
#else
	return ThisTimeLineID;
#endif
}

static void
walprop_pg_load_libpqwalreceiver(void)
{
	load_file("libpqwalreceiver", false);
	if (WalReceiverFunctions == NULL)
		wpg_log(ERROR, "libpqwalreceiver didn't initialize correctly");
}

static void
walprop_pg_update_donor(WalProposer *wp, Safekeeper *donor, XLogRecPtr donor_lsn)
{
	WalproposerShmemState *wps = wp->api.get_shmem_state(wp);
	char		donor_name[64];

	pg_snprintf(donor_name, sizeof(donor_name), "%s:%s", donor->host, donor->port);
	SpinLockAcquire(&wps->mutex);
	memcpy(wps->donor_name, donor_name, sizeof(donor_name));
	memcpy(wps->donor_conninfo, donor->conninfo, sizeof(donor->conninfo));
	wps->donor_lsn = donor_lsn;
	SpinLockRelease(&wps->mutex);
}

/* Helper function */
static bool
ensure_nonblocking_status(WalProposerConn *conn, bool is_nonblocking)
{
	/* If we're already correctly blocking or nonblocking, all good */
	if (is_nonblocking == conn->is_nonblocking)
		return true;

	/* Otherwise, set it appropriately */
	if (PQsetnonblocking(conn->pg_conn, is_nonblocking) == -1)
		return false;

	conn->is_nonblocking = is_nonblocking;
	return true;
}

/* Exported function definitions */
static char *
walprop_error_message(Safekeeper *sk)
{
	return PQerrorMessage(sk->conn->pg_conn);
}

static WalProposerConnStatusType
walprop_status(Safekeeper *sk)
{
	switch (PQstatus(sk->conn->pg_conn))
	{
		case CONNECTION_OK:
			return WP_CONNECTION_OK;
		case CONNECTION_BAD:
			return WP_CONNECTION_BAD;
		default:
			return WP_CONNECTION_IN_PROGRESS;
	}
}

WalProposerConn *
libpqwp_connect_start(char *conninfo)
{

	PGconn	   *pg_conn;
	WalProposerConn *conn;
	const char *keywords[3];
	const char *values[3];
	int			n;
	char	   *password = neon_auth_token;


	/*
	 * Connect using the given connection string. If the NEON_AUTH_TOKEN
	 * environment variable was set, use that as the password.
	 *
	 * The connection options are parsed in the order they're given, so when
	 * we set the password before the connection string, the connection string
	 * can override the password from the env variable. Seems useful, although
	 * we don't currently use that capability anywhere.
	 */
	n = 0;
	if (password)
	{
		keywords[n] = "password";
		values[n] = password;
		n++;
	}
	keywords[n] = "dbname";
	values[n] = conninfo;
	n++;
	keywords[n] = NULL;
	values[n] = NULL;
	n++;
	pg_conn = PQconnectStartParams(keywords, values, 1);

	/*
	 * "If the result is null, then libpq has been unable to allocate a new
	 * PGconn structure"
	 */
	if (!pg_conn)
		wpg_log(FATAL, "failed to allocate new PGconn object");

	/*
	 * And in theory this allocation can fail as well, but it's incredibly
	 * unlikely if we just successfully allocated a PGconn.
	 *
	 * palloc will exit on failure though, so there's not much we could do if
	 * it *did* fail.
	 */
	conn = palloc(sizeof(WalProposerConn));
	conn->pg_conn = pg_conn;
	conn->is_nonblocking = false;	/* connections always start in blocking
									 * mode */
	conn->recvbuf = NULL;
	return conn;
}

static void
walprop_connect_start(Safekeeper *sk)
{
	Assert(sk->conn == NULL);
	sk->conn = libpqwp_connect_start(sk->conninfo);
}

static WalProposerConnectPollStatusType
walprop_connect_poll(Safekeeper *sk)
{
	WalProposerConnectPollStatusType return_val;

	switch (PQconnectPoll(sk->conn->pg_conn))
	{
		case PGRES_POLLING_FAILED:
			return_val = WP_CONN_POLLING_FAILED;
			break;
		case PGRES_POLLING_READING:
			return_val = WP_CONN_POLLING_READING;
			break;
		case PGRES_POLLING_WRITING:
			return_val = WP_CONN_POLLING_WRITING;
			break;
		case PGRES_POLLING_OK:
			return_val = WP_CONN_POLLING_OK;
			break;

			/*
			 * There's a comment at its source about this constant being
			 * unused. We'll expect it's never returned.
			 */
		case PGRES_POLLING_ACTIVE:
			wpg_log(FATAL, "unexpected PGRES_POLLING_ACTIVE returned from PQconnectPoll");

			/*
			 * This return is never actually reached, but it's here to make
			 * the compiler happy
			 */
			return WP_CONN_POLLING_FAILED;

		default:
			Assert(false);
			return_val = WP_CONN_POLLING_FAILED;	/* keep the compiler quiet */
	}

	return return_val;
}

extern bool
libpqwp_send_query(WalProposerConn *conn, char *query)
{
	/*
	 * We need to be in blocking mode for sending the query to run without
	 * requiring a call to PQflush
	 */
	if (!ensure_nonblocking_status(conn, false))
		return false;

	/* PQsendQuery returns 1 on success, 0 on failure */
	if (!PQsendQuery(conn->pg_conn, query))
		return false;

	return true;
}

static bool
walprop_send_query(Safekeeper *sk, char *query)
{
	return libpqwp_send_query(sk->conn, query);
}

WalProposerExecStatusType
libpqwp_get_query_result(WalProposerConn *conn)
{

	PGresult   *result;
	WalProposerExecStatusType return_val;

	/* Marker variable if we need to log an unexpected success result */
	char	   *unexpected_success = NULL;

	/* Consume any input that we might be missing */
	if (!PQconsumeInput(conn->pg_conn))
		return WP_EXEC_FAILED;

	if (PQisBusy(conn->pg_conn))
		return WP_EXEC_NEEDS_INPUT;


	result = PQgetResult(conn->pg_conn);

	/*
	 * PQgetResult returns NULL only if getting the result was successful &
	 * there's no more of the result to get.
	 */
	if (!result)
	{
		wpg_log(WARNING, "[libpqwalproposer] Unexpected successful end of command results");
		return WP_EXEC_UNEXPECTED_SUCCESS;
	}

	/* Helper macro to reduce boilerplate */
#define UNEXPECTED_SUCCESS(msg) \
		return_val = WP_EXEC_UNEXPECTED_SUCCESS; \
		unexpected_success = msg; \
		break;


	switch (PQresultStatus(result))
	{
			/* "true" success case */
		case PGRES_COPY_BOTH:
			return_val = WP_EXEC_SUCCESS_COPYBOTH;
			break;

			/* Unexpected success case */
		case PGRES_EMPTY_QUERY:
			UNEXPECTED_SUCCESS("empty query return");
		case PGRES_COMMAND_OK:
			UNEXPECTED_SUCCESS("data-less command end");
		case PGRES_TUPLES_OK:
			UNEXPECTED_SUCCESS("tuples return");
		case PGRES_COPY_OUT:
			UNEXPECTED_SUCCESS("'Copy Out' response");
		case PGRES_COPY_IN:
			UNEXPECTED_SUCCESS("'Copy In' response");
		case PGRES_SINGLE_TUPLE:
			UNEXPECTED_SUCCESS("single tuple return");
		case PGRES_PIPELINE_SYNC:
			UNEXPECTED_SUCCESS("pipeline sync point");

			/* Failure cases */
		case PGRES_BAD_RESPONSE:
		case PGRES_NONFATAL_ERROR:
		case PGRES_FATAL_ERROR:
		case PGRES_PIPELINE_ABORTED:
			return_val = WP_EXEC_FAILED;
			break;

		default:
			Assert(false);
			return_val = WP_EXEC_FAILED;	/* keep the compiler quiet */
	}

	if (unexpected_success)
		wpg_log(WARNING, "[libpqwalproposer] Unexpected successful %s", unexpected_success);

	return return_val;
}

static WalProposerExecStatusType
walprop_get_query_result(Safekeeper *sk)
{
	return libpqwp_get_query_result(sk->conn);
}

static pgsocket
walprop_socket(Safekeeper *sk)
{
	return PQsocket(sk->conn->pg_conn);
}

static int
walprop_flush(Safekeeper *sk)
{
	return (PQflush(sk->conn->pg_conn));
}

/* Like libpqrcv_receive. *buf is valid until the next call. */
PGAsyncReadResult
libpqwp_async_read(WalProposerConn *conn, char **buf, int *amount)
{
	int			rawlen;

	if (conn->recvbuf != NULL)
	{
		PQfreemem(conn->recvbuf);
		conn->recvbuf = NULL;
	}

	/* Try to receive a CopyData message */
	rawlen = PQgetCopyData(conn->pg_conn, &conn->recvbuf, true);
	if (rawlen == 0)
	{
		/* Try consuming some data. */
		if (!PQconsumeInput(conn->pg_conn))
		{
			*amount = 0;
			*buf = NULL;
			return PG_ASYNC_READ_FAIL;
		}
		/* Now that we've consumed some input, try again */
		rawlen = PQgetCopyData(conn->pg_conn, &conn->recvbuf, true);
	}

	/*
	 * The docs for PQgetCopyData list the return values as: 0 if the copy is
	 * still in progress, but no "complete row" is available -1 if the copy is
	 * done -2 if an error occurred (> 0) if it was successful; that value is
	 * the amount transferred.
	 *
	 * The protocol we use between walproposer and safekeeper means that we
	 * *usually* wouldn't expect to see that the copy is done, but this can
	 * sometimes be triggered by the server returning an ErrorResponse (which
	 * also happens to have the effect that the copy is done).
	 */
	switch (rawlen)
	{
		case 0:
			*amount = 0;
			*buf = NULL;
			return PG_ASYNC_READ_TRY_AGAIN;
		case -1:
			{
				/*
				 * If we get -1, it's probably because of a server error; the
				 * safekeeper won't normally send a CopyDone message.
				 *
				 * We can check PQgetResult to make sure that the server
				 * failed; it'll always result in PGRES_FATAL_ERROR
				 */
				ExecStatusType status = PQresultStatus(PQgetResult(conn->pg_conn));

				if (status != PGRES_FATAL_ERROR)
					wpg_log(FATAL, "unexpected result status %d after failed PQgetCopyData", status);

				/*
				 * If there was actually an error, it'll be properly reported
				 * by calls to PQerrorMessage -- we don't have to do anything
				 * else
				 */
				*amount = 0;
				*buf = NULL;
				return PG_ASYNC_READ_FAIL;
			}
		case -2:
			*amount = 0;
			*buf = NULL;
			return PG_ASYNC_READ_FAIL;
		default:
			/* Positive values indicate the size of the returned result */
			*amount = rawlen;
			*buf = conn->recvbuf;
			return PG_ASYNC_READ_SUCCESS;
	}
}

/*
 * Receive a message from the safekeeper.
 *
 * On success, the data is placed in *buf. It is valid until the next call
 * to this function.
 */
static PGAsyncReadResult
walprop_async_read(Safekeeper *sk, char **buf, int *amount)
{
	return libpqwp_async_read(sk->conn, buf, amount);
}

static PGAsyncWriteResult
walprop_async_write(Safekeeper *sk, void const *buf, size_t size)
{
	int			result;

	/* If we aren't in non-blocking mode, switch to it. */
	if (!ensure_nonblocking_status(sk->conn, true))
		return PG_ASYNC_WRITE_FAIL;

	/*
	 * The docs for PQputcopyData list the return values as: 1 if the data was
	 * queued, 0 if it was not queued because of full buffers, or -1 if an
	 * error occurred
	 */
	result = PQputCopyData(sk->conn->pg_conn, buf, size);

	/*
	 * We won't get a result of zero because walproposer always empties the
	 * connection's buffers before sending more
	 */
	Assert(result != 0);

	switch (result)
	{
		case 1:
			/* good -- continue */
			break;
		case -1:
			return PG_ASYNC_WRITE_FAIL;
		default:
			wpg_log(FATAL, "invalid return %d from PQputCopyData", result);
	}

	/*
	 * After queueing the data, we still need to flush to get it to send. This
	 * might take multiple tries, but we don't want to wait around until it's
	 * done.
	 *
	 * PQflush has the following returns (directly quoting the docs): 0 if
	 * sucessful, 1 if it was unable to send all the data in the send queue
	 * yet -1 if it failed for some reason
	 */
	switch (result = PQflush(sk->conn->pg_conn))
	{
		case 0:
			return PG_ASYNC_WRITE_SUCCESS;
		case 1:
			return PG_ASYNC_WRITE_TRY_FLUSH;
		case -1:
			return PG_ASYNC_WRITE_FAIL;
		default:
			wpg_log(FATAL, "invalid return %d from PQflush", result);
	}
}

/*
 * This function is very similar to walprop_async_write. For more
 * information, refer to the comments there.
 */
static bool
walprop_blocking_write(Safekeeper *sk, void const *buf, size_t size)
{
	int			result;

	/* If we are in non-blocking mode, switch out of it. */
	if (!ensure_nonblocking_status(sk->conn, false))
		return false;

	if ((result = PQputCopyData(sk->conn->pg_conn, buf, size)) == -1)
		return false;

	Assert(result == 1);

	/* Because the connection is non-blocking, flushing returns 0 or -1 */

	if ((result = PQflush(sk->conn->pg_conn)) == -1)
		return false;

	Assert(result == 0);
	return true;
}

void
libpqwp_disconnect(WalProposerConn *conn)
{
	if (conn->recvbuf != NULL)
		PQfreemem(conn->recvbuf);
	PQfinish(conn->pg_conn);
	pfree(conn);
}

static void
walprop_finish(Safekeeper *sk)
{
	if (sk->conn)
	{
		libpqwp_disconnect(sk->conn);
		sk->conn = NULL;
	}

	/* free xlogreader */
	if (sk->xlogreader)
	{
		NeonWALReaderFree(sk->xlogreader);
		sk->xlogreader = NULL;
	}
	rm_safekeeper_event_set(sk, false);
}

/*
 * Subscribe for new WAL and stream it in the loop to safekeepers.
 *
 * At the moment, this never returns, but an ereport(ERROR) will take us back
 * to the main loop.
 */
static void
StartProposerReplication(WalProposer *wp, StartReplicationCmd *cmd)
{
	XLogRecPtr	FlushPtr;
	__attribute__((unused)) TimeLineID currTLI;

#if PG_VERSION_NUM < 150000
	if (ThisTimeLineID == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("IDENTIFY_SYSTEM has not been run before START_REPLICATION")));
#endif

	/*
	 * We assume here that we're logging enough information in the WAL for
	 * log-shipping, since this is checked in PostmasterMain().
	 *
	 * NOTE: wal_level can only change at shutdown, so in most cases it is
	 * difficult for there to be WAL data that we can still see that was
	 * written at wal_level='minimal'.
	 */

	if (cmd->slotname)
	{
		ReplicationSlotAcquire(cmd->slotname, true);
		if (SlotIsLogical(MyReplicationSlot))
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("cannot use a logical replication slot for physical replication")));

		/*
		 * We don't need to verify the slot's restart_lsn here; instead we
		 * rely on the caller requesting the starting point to use.  If the
		 * WAL segment doesn't exist, we'll fail later.
		 */
	}

	/*
	 * Select the timeline. If it was given explicitly by the client, use
	 * that. Otherwise use the timeline of the last replayed record, which is
	 * kept in ThisTimeLineID.
	 *
	 * Neon doesn't currently use PG Timelines, but it may in the future, so
	 * we keep this code around to lighten the load for when we need it.
	 */
#if PG_VERSION_NUM >= 150000
	FlushPtr = GetFlushRecPtr(&currTLI);
#else
	FlushPtr = GetFlushRecPtr();
	currTLI = ThisTimeLineID;
#endif

	/*
	 * XXX: Move straight to STOPPING state, skipping the STREAMING state.
	 *
	 * This is a bit weird. Normal walsenders stay in STREAMING state, until
	 * the checkpointer signals them that it is about to start writing the
	 * shutdown checkpoint. The walsenders acknowledge that they have received
	 * that signal by switching to STOPPING state. That tells the walsenders
	 * that they must not write any new WAL.
	 *
	 * However, we cannot easily intercept that signal from the checkpointer.
	 * It's sent by WalSndInitStopping(), using
	 * SendProcSignal(PROCSIGNAL_WALSND_INIT_STOPPING). It's received by
	 * HandleWalSndInitStopping, which sets a process-local got_STOPPING flag.
	 * However, that's all private to walsender.c.
	 *
	 * We don't need to do anything special upon receiving the signal, the
	 * walproposer doesn't write any WAL anyway, so we skip the STREAMING
	 * state and go directly to STOPPING mode. That way, the checkpointer
	 * won't wait for us.
	 */
	WalSndSetState(WALSNDSTATE_STOPPING);

	/*
	 * Don't allow a request to stream from a future point in WAL that hasn't
	 * been flushed to disk in this server yet.
	 */
	if (FlushPtr < cmd->startpoint)
	{
		ereport(ERROR,
				(errmsg("requested starting point %X/%X is ahead of the WAL flush position of this server %X/%X",
						LSN_FORMAT_ARGS(cmd->startpoint),
						LSN_FORMAT_ARGS(FlushPtr))));
	}

	/* Start streaming from the requested point */
	sentPtr = cmd->startpoint;

	/* Initialize shared memory status, too */
	SpinLockAcquire(&MyWalSnd->mutex);
	MyWalSnd->sentPtr = sentPtr;
	SpinLockRelease(&MyWalSnd->mutex);

	SyncRepInitConfig();

	/* Infinite send loop, never returns */
	WalSndLoop(wp);

	WalSndSetState(WALSNDSTATE_STARTUP);

	if (cmd->slotname)
		ReplicationSlotRelease();
}

/*
 * Main loop that waits for LSN updates and calls the walproposer.
 * Synchronous replication sets latch in WalSndWakeup at walsender.c
 */
static void
WalSndLoop(WalProposer *wp)
{
	/* Clear any already-pending wakeups */
	ResetLatch(MyLatch);

	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		XLogBroadcastWalProposer(wp);
		WalProposerPoll(wp);
	}
}

/*
 * Notify walproposer about the new WAL position.
 */
static void
XLogBroadcastWalProposer(WalProposer *wp)
{
	XLogRecPtr	startptr;
	XLogRecPtr	endptr;

	/* Start from the last sent position */
	startptr = sentPtr;

	/*
	 * Streaming the current timeline on a primary.
	 *
	 * Attempt to send all data that's already been written out and fsync'd to
	 * disk.  We cannot go further than what's been written out given the
	 * current implementation of WALRead().  And in any case it's unsafe to
	 * send WAL that is not securely down to disk on the primary: if the
	 * primary subsequently crashes and restarts, standbys must not have
	 * applied any WAL that got lost on the primary.
	 */
#if PG_VERSION_NUM >= 150000
	endptr = GetFlushRecPtr(NULL);
#else
	endptr = GetFlushRecPtr();
#endif

	/*
	 * Record the current system time as an approximation of the time at which
	 * this WAL location was written for the purposes of lag tracking.
	 *
	 * In theory we could make XLogFlush() record a time in shmem whenever WAL
	 * is flushed and we could get that time as well as the LSN when we call
	 * GetFlushRecPtr() above (and likewise for the cascading standby
	 * equivalent), but rather than putting any new code into the hot WAL path
	 * it seems good enough to capture the time here.  We should reach this
	 * after XLogFlush() runs WalSndWakeupProcessRequests(), and although that
	 * may take some time, we read the WAL flush pointer and take the time
	 * very close to together here so that we'll get a later position if it is
	 * still moving.
	 *
	 * Because LagTrackerWrite ignores samples when the LSN hasn't advanced,
	 * this gives us a cheap approximation for the WAL flush time for this
	 * LSN.
	 *
	 * Note that the LSN is not necessarily the LSN for the data contained in
	 * the present message; it's the end of the WAL, which might be further
	 * ahead.  All the lag tracking machinery cares about is finding out when
	 * that arbitrary LSN is eventually reported as written, flushed and
	 * applied, so that it can measure the elapsed time.
	 */
	LagTrackerWrite(endptr, GetCurrentTimestamp());

	/* Do we have any work to do? */
	Assert(startptr <= endptr);
	if (endptr <= startptr)
		return;

	WalProposerBroadcast(wp, startptr, endptr);
	sentPtr = endptr;

	/* Update shared memory status */
	{
		WalSnd	   *walsnd = MyWalSnd;

		SpinLockAcquire(&walsnd->mutex);
		walsnd->sentPtr = sentPtr;
		SpinLockRelease(&walsnd->mutex);
	}

	/* Report progress of XLOG streaming in PS display */
	if (update_process_title)
	{
		char		activitymsg[50];

		snprintf(activitymsg, sizeof(activitymsg), "streaming %X/%X",
				 LSN_FORMAT_ARGS(sentPtr));
		set_ps_display(activitymsg);
	}
}

/*
  Used to download WAL before basebackup for walproposer/logical walsenders. No
  longer used, replaced by neon_walreader; but callback still exists because
  simulation tests use it.
 */
static bool
WalProposerRecovery(WalProposer *wp, Safekeeper *sk)
{
	return true;
}

static void
walprop_pg_wal_reader_allocate(Safekeeper *sk)
{
	char		log_prefix[64];

	snprintf(log_prefix, sizeof(log_prefix), WP_LOG_PREFIX "sk %s:%s nwr: ", sk->host, sk->port);
	Assert(!sk->xlogreader);
	sk->xlogreader = NeonWALReaderAllocate(wal_segment_size, sk->wp->propEpochStartLsn, log_prefix);
	if (sk->xlogreader == NULL)
		wpg_log(FATAL, "failed to allocate xlog reader");
}

static NeonWALReadResult
walprop_pg_wal_read(Safekeeper *sk, char *buf, XLogRecPtr startptr, Size count, char **errmsg)
{
	NeonWALReadResult res;

	res = NeonWALRead(sk->xlogreader,
					  buf,
					  startptr,
					  count,
					  walprop_pg_get_timeline_id());

	if (res == NEON_WALREAD_SUCCESS)
	{
		/*
		 * If we have the socket subscribed, but walreader doesn't need any
		 * events, it must mean that remote connection just closed hoping to
		 * do next read locally. Remove the socket then. It is important to do
		 * as otherwise next read might open another connection and we won't
		 * be able to distinguish whether we have correct socket added in wait
		 * event set.
		 */
		if (NeonWALReaderEvents(sk->xlogreader) == 0)
			rm_safekeeper_event_set(sk, false);
	}
	else if (res == NEON_WALREAD_ERROR)
	{
		*errmsg = NeonWALReaderErrMsg(sk->xlogreader);
	}

	return res;
}

static uint32
walprop_pg_wal_reader_events(Safekeeper *sk)
{
	return NeonWALReaderEvents(sk->xlogreader);
}

static WaitEventSet *waitEvents;

static void
walprop_pg_free_event_set(WalProposer *wp)
{
	if (waitEvents)
	{
		FreeWaitEventSet(waitEvents);
		waitEvents = NULL;
	}

	for (int i = 0; i < wp->n_safekeepers; i++)
	{
		wp->safekeeper[i].eventPos = -1;
		wp->safekeeper[i].nwrEventPos = -1;
		wp->safekeeper[i].nwrConnEstablished = false;
	}
}

static void
walprop_pg_init_event_set(WalProposer *wp)
{
	if (waitEvents)
		wpg_log(FATAL, "double-initialization of event set");

	/* for each sk, we have socket plus potentially socket for neon walreader */
#if PG_MAJORVERSION_NUM >= 17
	waitEvents = CreateWaitEventSet(NULL, 2 + 2 * wp->n_safekeepers);
#else
	waitEvents = CreateWaitEventSet(TopMemoryContext, 2 + 2 * wp->n_safekeepers);
#endif
	AddWaitEventToSet(waitEvents, WL_LATCH_SET, PGINVALID_SOCKET,
					  MyLatch, NULL);
	AddWaitEventToSet(waitEvents, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET,
					  NULL, NULL);

	for (int i = 0; i < wp->n_safekeepers; i++)
	{
		wp->safekeeper[i].eventPos = -1;
		wp->safekeeper[i].nwrEventPos = -1;
		wp->safekeeper[i].nwrConnEstablished = false;
	}
}

/* add safekeeper socket to wait event set */
static void
walprop_pg_add_safekeeper_event_set(Safekeeper *sk, uint32 events)
{
	Assert(sk->eventPos == -1);
	sk->eventPos = AddWaitEventToSet(waitEvents, events, walprop_socket(sk), NULL, sk);
}

/* add neon wal reader socket to wait event set */
static void
add_nwr_event_set(Safekeeper *sk, uint32 events)
{
	Assert(sk->nwrEventPos == -1);
	sk->nwrEventPos = AddWaitEventToSet(waitEvents, events, NeonWALReaderSocket(sk->xlogreader), NULL, sk);
	sk->nwrConnEstablished = NeonWALReaderIsRemConnEstablished(sk->xlogreader);
	wpg_log(DEBUG5, "sk %s:%s: added nwr socket events %d", sk->host, sk->port, events);
}

static void
walprop_pg_update_event_set(Safekeeper *sk, uint32 events)
{
	/* eventPos = -1 when we don't have an event */
	Assert(sk->eventPos != -1);

	ModifyWaitEvent(waitEvents, sk->eventPos, events, NULL);
}

/*
 * Update neon_walreader event.
 * Can be called when nwr socket doesn't exist, does nothing in this case.
 */
static void
update_nwr_event_set(Safekeeper *sk, uint32 events)
{
	/* eventPos = -1 when we don't have an event */
	if (sk->nwrEventPos != -1)
		ModifyWaitEvent(waitEvents, sk->nwrEventPos, events, NULL);
}


static void
walprop_pg_active_state_update_event_set(Safekeeper *sk)
{
	uint32		sk_events;
	uint32		nwr_events;

	Assert(sk->state == SS_ACTIVE);
	SafekeeperStateDesiredEvents(sk, &sk_events, &nwr_events);

	/*
	 * If we need to wait for neon_walreader, ensure we have up to date socket
	 * in the wait event set.
	 */
	if (sk->active_state == SS_ACTIVE_READ_WAL)
	{
		/*
		 * If conn is established and socket is thus stable, update the event
		 * directly; otherwise re-add it.
		 */
		if (sk->nwrConnEstablished)
		{
			Assert(sk->nwrEventPos != -1);
			update_nwr_event_set(sk, nwr_events);
		}
		else
		{
			rm_safekeeper_event_set(sk, false);
			add_nwr_event_set(sk, nwr_events);
		}
	}
	else
	{
		/*
		 * Hack: we should always set 0 here, but for random reasons
		 * WaitEventSet (WaitEventAdjustEpoll) asserts that there is at least
		 * some event. Since there is also no way to remove socket except
		 * reconstructing the whole set, SafekeeperStateDesiredEvents instead
		 * gives WL_SOCKET_CLOSED if socket exists. We never expect it to
		 * trigger.
		 *
		 * On PG 14 which doesn't have WL_SOCKET_CLOSED resort to event
		 * removal.
		 */
#if PG_VERSION_NUM >= 150000
		Assert(nwr_events == WL_SOCKET_CLOSED || nwr_events == 0);
		update_nwr_event_set(sk, WL_SOCKET_CLOSED);
#else							/* pg 14 */
		rm_safekeeper_event_set(sk, false);
#endif
	}
	walprop_pg_update_event_set(sk, sk_events);
}

static void
walprop_pg_rm_safekeeper_event_set(Safekeeper *to_remove)
{
	rm_safekeeper_event_set(to_remove, true);
}

/*
 * A hacky way to remove single event from the event set. Can be called if event
 * doesn't exist, does nothing in this case.
 *
 * Note: Internally, this completely reconstructs the event set. It should be
 * avoided if possible.
 *
 * If is_sk is true, socket of connection to safekeeper is removed; otherwise
 * socket of neon_walreader.
 */
static void
rm_safekeeper_event_set(Safekeeper *to_remove, bool is_sk)
{
	WalProposer *wp = to_remove->wp;

	wpg_log(DEBUG5, "sk %s:%s: removing event, is_sk %d",
			to_remove->host, to_remove->port, is_sk);

	/*
	 * Shortpath for exiting if have nothing to do. We never call this
	 * function with safekeeper socket not existing, but do that with neon
	 * walreader socket.
	 */
	if ((is_sk && to_remove->eventPos == -1) ||
		(!is_sk && to_remove->nwrEventPos == -1))
	{
		return;
	}

	/* Remove the existing event set, assign sk->eventPos = -1 */
	walprop_pg_free_event_set(wp);

	/* Re-initialize it without adding any safekeeper events */
	wp->api.init_event_set(wp);

	/*
	 * loop through the existing safekeepers. If they aren't the one we're
	 * removing, and if they have a socket we can use, re-add the applicable
	 * events.
	 */
	for (int i = 0; i < wp->n_safekeepers; i++)
	{
		Safekeeper *sk = &wp->safekeeper[i];

		/*
		 * If this safekeeper isn't offline, add events for it, except for the
		 * event requested to remove.
		 */
		if (sk->state != SS_OFFLINE)
		{
			uint32		sk_events;
			uint32		nwr_events;

			SafekeeperStateDesiredEvents(sk, &sk_events, &nwr_events);

			if (sk != to_remove || !is_sk)
			{
				/* will set sk->eventPos */
				wp->api.add_safekeeper_event_set(sk, sk_events);
			}
			if ((sk != to_remove || is_sk) && nwr_events)
			{
				add_nwr_event_set(sk, nwr_events);
			}
		}
	}
}

static int
walprop_pg_wait_event_set(WalProposer *wp, long timeout, Safekeeper **sk, uint32 *events)
{
	WaitEvent	event = {0};
	int			rc = 0;
	bool		late_cv_trigger = false;

	*sk = NULL;
	*events = 0;

#if PG_MAJORVERSION_NUM >= 16
	if (WalSndCtl != NULL)
		ConditionVariablePrepareToSleep(&WalSndCtl->wal_flush_cv);

	/*
	 * Now that we prepared the condvar, check flush ptr again -- it might
	 * have changed before we subscribed to cv so we missed the wakeup.
	 *
	 * Do that only when we're interested in new WAL: without sync-safekeepers
	 * and if election already passed.
	 */
	if (!wp->config->syncSafekeepers && wp->availableLsn != InvalidXLogRecPtr && GetFlushRecPtr(NULL) > wp->availableLsn)
	{
		ConditionVariableCancelSleep();
		ResetLatch(MyLatch);

		CheckGracefulShutdown(wp);

		*events = WL_LATCH_SET;
		return 1;
	}
#endif

	/*
	 * Wait for a wait event to happen, or timeout: - Safekeeper socket can
	 * become available for READ or WRITE - Our latch got set, because *
	 * PG15-: We got woken up by a process triggering the WalSender * PG16+:
	 * WalSndCtl->wal_flush_cv was triggered
	 */
	rc = WaitEventSetWait(waitEvents, timeout,
						  &event, 1, WAIT_EVENT_WAL_SENDER_MAIN);
#if PG_MAJORVERSION_NUM >= 16
	if (WalSndCtl != NULL)
		late_cv_trigger = ConditionVariableCancelSleep();
#endif

	/*
	 * Process config if requested. This restarts walproposer if safekeepers
	 * list changed. Don't do that for sync-safekeepers because quite probably
	 * it (re-reading config) won't work without some effort, and
	 * sync-safekeepers should be quick to finish anyway.
	 */
	if (!wp->config->syncSafekeepers && ConfigReloadPending)
	{
		ConfigReloadPending = false;
		ProcessConfigFile(PGC_SIGHUP);
	}

	/*
	 * If wait is terminated by latch set (walsenders' latch is set on each
	 * wal flush). (no need for pm death check due to WL_EXIT_ON_PM_DEATH)
	 */
	if ((rc == 1 && (event.events & WL_LATCH_SET)) || late_cv_trigger)
	{
		/* Reset our latch */
		ResetLatch(MyLatch);
		*events = WL_LATCH_SET;
		return 1;
	}

	/*
	 * If the event contains something about the socket, it means we got an
	 * event from a safekeeper socket.
	 */
	if (rc == 1 && (event.events & WL_SOCKET_MASK))
	{
		*sk = (Safekeeper *) event.user_data;
		*events = event.events;
		return 1;
	}

	/* XXX: Can we have non-timeout event here? */
	*events = event.events;
	return rc;
}

static void
walprop_pg_finish_sync_safekeepers(WalProposer *wp, XLogRecPtr lsn)
{
	fprintf(stdout, "%X/%X\n", LSN_FORMAT_ARGS(lsn));
	exit(0);
}

/*
 * Like vanilla walsender, on sigusr2 send all remaining WAL and exit.
 *
 * Note that unlike sync-safekeepers waiting here is not reliable: we
 * don't check that majority of safekeepers received and persisted
 * commit_lsn -- only that walproposer reached it (which immediately
 * broadcasts new value). Doing that without incurring redundant control
 * file syncing would need wp -> sk protocol change. OTOH unlike
 * sync-safekeepers which must bump commit_lsn or basebackup will fail,
 * this catchup is important only for tests where safekeepers/network
 * don't crash on their own.
 */
static void
CheckGracefulShutdown(WalProposer *wp)
{
	if (got_SIGUSR2)
	{
		if (!reported_sigusr2)
		{
			XLogRecPtr	flushPtr = walprop_pg_get_flush_rec_ptr(wp);

			wpg_log(LOG, "walproposer will send and wait for remaining WAL between %X/%X and %X/%X",
					LSN_FORMAT_ARGS(wp->commitLsn), LSN_FORMAT_ARGS(flushPtr));
			reported_sigusr2 = true;
		}

		if (wp->commitLsn >= walprop_pg_get_flush_rec_ptr(wp))
		{
			wpg_log(LOG, "walproposer sent all WAL up to %X/%X, exiting",
					LSN_FORMAT_ARGS(wp->commitLsn));
			proc_exit(0);
		}
	}
}

/*
 * Combine hot standby feedbacks from all safekeepers.
 */
static void
CombineHotStanbyFeedbacks(HotStandbyFeedback *hs, WalProposer *wp)
{
	hs->ts = 0;
	hs->xmin = InvalidFullTransactionId;
	hs->catalog_xmin = InvalidFullTransactionId;

	for (int i = 0; i < wp->n_safekeepers; i++)
	{

		if (wp->safekeeper[i].state == SS_ACTIVE)
		{
			HotStandbyFeedback *skhs = &wp->safekeeper[i].appendResponse.hs;

			if (FullTransactionIdIsNormal(skhs->xmin)
				&& (!FullTransactionIdIsValid(hs->xmin) || FullTransactionIdPrecedes(skhs->xmin, hs->xmin)))
			{
				hs->xmin = skhs->xmin;
				hs->ts = skhs->ts;
			}
			if (FullTransactionIdIsNormal(skhs->catalog_xmin)
				&& (!FullTransactionIdIsValid(hs->catalog_xmin) || FullTransactionIdPrecedes(skhs->catalog_xmin, hs->catalog_xmin)))
			{
				hs->catalog_xmin = skhs->catalog_xmin;
				hs->ts = skhs->ts;
			}
		}
	}
}

/*
 * Based on commitLsn and safekeeper responses including pageserver feedback,
 * 1) Propagate cluster size received from ps to ensure the limit.
 * 2) Propagate pageserver LSN positions to ensure backpressure limits.
 * 3) Advance walproposer slot to commitLsn (releasing WAL & waking up waiters).
 * 4) Propagate hot standby feedback.
 *
 * None of that is functional in sync-safekeepers.
 */
static void
walprop_pg_process_safekeeper_feedback(WalProposer *wp, Safekeeper *sk)
{
	HotStandbyFeedback hsFeedback;
	bool		needToAdvanceSlot = false;

	if (wp->config->syncSafekeepers)
		return;

	/* handle fresh ps_feedback */
	if (sk->appendResponse.ps_feedback.present)
	{
		PageserverFeedback min_feedback = record_pageserver_feedback(&sk->appendResponse.ps_feedback);

		/* Only one main shard sends non-zero currentClusterSize */
		if (sk->appendResponse.ps_feedback.currentClusterSize > 0)
			SetNeonCurrentClusterSize(sk->appendResponse.ps_feedback.currentClusterSize);

		if (min_feedback.disk_consistent_lsn != standby_apply_lsn)
		{
			standby_apply_lsn = min_feedback.disk_consistent_lsn;
			needToAdvanceSlot = true;
		}
	}

	if (wp->commitLsn > standby_flush_lsn)
	{
		standby_flush_lsn = wp->commitLsn;
		needToAdvanceSlot = true;
	}

	if (needToAdvanceSlot)
	{
		/*
		 * Advance the replication slot to commitLsn. WAL before it is
		 * hardened and will be fetched from one of safekeepers by
		 * neon_walreader if needed.
		 *
		 * Also wakes up syncrep waiters.
		 */
		ProcessStandbyReply(
		/* write_lsn -  This is what durably stored in safekeepers quorum. */
							standby_flush_lsn,
		/* flush_lsn - This is what durably stored in safekeepers quorum. */
							standby_flush_lsn,

		/*
		 * apply_lsn - This is what processed and durably saved at*
		 * pageserver.
		 */
							standby_apply_lsn,
							walprop_pg_get_current_timestamp(wp), false);
	}

	CombineHotStanbyFeedbacks(&hsFeedback, wp);
	if (memcmp(&hsFeedback, &agg_hs_feedback, sizeof hsFeedback) != 0)
	{
		FullTransactionId xmin = hsFeedback.xmin;
		FullTransactionId catalog_xmin = hsFeedback.catalog_xmin;
		FullTransactionId next_xid = ReadNextFullTransactionId();

		/*
		 * Page server is updating nextXid in checkpoint each 1024
		 * transactions, so feedback xmin can be actually larger then nextXid
		 * and function TransactionIdInRecentPast return false in this case,
		 * preventing update of slot's xmin.
		 */
		if (FullTransactionIdPrecedes(next_xid, xmin))
			xmin = next_xid;
		if (FullTransactionIdPrecedes(next_xid, catalog_xmin))
			catalog_xmin = next_xid;
		agg_hs_feedback = hsFeedback;
		elog(DEBUG2, "ProcessStandbyHSFeedback(xmin=%d, catalog_xmin=%d", XidFromFullTransactionId(hsFeedback.xmin), XidFromFullTransactionId(hsFeedback.catalog_xmin));
		ProcessStandbyHSFeedback(hsFeedback.ts,
								 XidFromFullTransactionId(xmin),
								 EpochFromFullTransactionId(xmin),
								 XidFromFullTransactionId(catalog_xmin),
								 EpochFromFullTransactionId(catalog_xmin));
	}

	CheckGracefulShutdown(wp);
}

static XLogRecPtr
walprop_pg_get_redo_start_lsn(WalProposer *wp)
{
	return GetRedoStartLsn();
}

static bool
walprop_pg_strong_random(WalProposer *wp, void *buf, size_t len)
{
	return pg_strong_random(buf, len);
}

static void
walprop_pg_log_internal(WalProposer *wp, int level, const char *line)
{
	elog(FATAL, "unexpected log_internal message at level %d: %s", level, line);
}

void
SetNeonCurrentClusterSize(uint64 size)
{
	pg_atomic_write_u64(&walprop_shared->currentClusterSize, size);
}

uint64
GetNeonCurrentClusterSize(void)
{
	return pg_atomic_read_u64(&walprop_shared->currentClusterSize);
}
uint64		GetNeonCurrentClusterSize(void);


static const walproposer_api walprop_pg = {
	.get_shmem_state = walprop_pg_get_shmem_state,
	.start_streaming = walprop_pg_start_streaming,
	.get_flush_rec_ptr = walprop_pg_get_flush_rec_ptr,
	.update_donor = walprop_pg_update_donor,
	.get_current_timestamp = walprop_pg_get_current_timestamp,
	.conn_error_message = walprop_error_message,
	.conn_status = walprop_status,
	.conn_connect_start = walprop_connect_start,
	.conn_connect_poll = walprop_connect_poll,
	.conn_send_query = walprop_send_query,
	.conn_get_query_result = walprop_get_query_result,
	.conn_flush = walprop_flush,
	.conn_finish = walprop_finish,
	.conn_async_read = walprop_async_read,
	.conn_async_write = walprop_async_write,
	.conn_blocking_write = walprop_blocking_write,
	.recovery_download = WalProposerRecovery,
	.wal_reader_allocate = walprop_pg_wal_reader_allocate,
	.wal_read = walprop_pg_wal_read,
	.wal_reader_events = walprop_pg_wal_reader_events,
	.init_event_set = walprop_pg_init_event_set,
	.update_event_set = walprop_pg_update_event_set,
	.active_state_update_event_set = walprop_pg_active_state_update_event_set,
	.add_safekeeper_event_set = walprop_pg_add_safekeeper_event_set,
	.rm_safekeeper_event_set = walprop_pg_rm_safekeeper_event_set,
	.wait_event_set = walprop_pg_wait_event_set,
	.strong_random = walprop_pg_strong_random,
	.get_redo_start_lsn = walprop_pg_get_redo_start_lsn,
	.finish_sync_safekeepers = walprop_pg_finish_sync_safekeepers,
	.process_safekeeper_feedback = walprop_pg_process_safekeeper_feedback,
	.log_internal = walprop_pg_log_internal,
};
