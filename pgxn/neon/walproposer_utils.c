#include "postgres.h"

#include "access/timeline.h"
#include "access/xlogutils.h"
#include "common/logging.h"
#include "common/ip.h"
#include "funcapi.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "postmaster/interrupt.h"
#include "replication/slot.h"
#include "walproposer_utils.h"
#include "replication/walsender_private.h"

#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/ps_status.h"

#include "libpq-fe.h"
#include <netinet/tcp.h>
#include <unistd.h>

#if PG_VERSION_NUM >= 150000
#include "access/xlogutils.h"
#include "access/xlogrecovery.h"
#endif
#if PG_MAJORVERSION_NUM >= 16
#include "utils/guc.h"
#endif

/*
 * These variables are used similarly to openLogFile/SegNo,
 * but for walproposer to write the XLOG during recovery. walpropFileTLI is the TimeLineID
 * corresponding the filename of walpropFile.
 */
static int	walpropFile = -1;
static TimeLineID walpropFileTLI = 0;
static XLogSegNo walpropSegNo = 0;

/* START cloned file-local variables and functions from walsender.c */

/*
 * How far have we sent WAL already? This is also advertised in
 * MyWalSnd->sentPtr.  (Actually, this is the next WAL location to send.)
 */
static XLogRecPtr sentPtr = InvalidXLogRecPtr;

static void WalSndLoop(void);
static void XLogBroadcastWalProposer(void);
/* END cloned file-level variables and functions from walsender.c */

int
CompareLsn(const void *a, const void *b)
{
	XLogRecPtr	lsn1 = *((const XLogRecPtr *) a);
	XLogRecPtr	lsn2 = *((const XLogRecPtr *) b);

	if (lsn1 < lsn2)
		return -1;
	else if (lsn1 == lsn2)
		return 0;
	else
		return 1;
}

/* Returns a human-readable string corresponding to the SafekeeperState
 *
 * The string should not be freed.
 *
 * The strings are intended to be used as a prefix to "state", e.g.:
 *
 *   elog(LOG, "currently in %s state", FormatSafekeeperState(sk->state));
 *
 * If this sort of phrasing doesn't fit the message, instead use something like:
 *
 *   elog(LOG, "currently in state [%s]", FormatSafekeeperState(sk->state));
 */
char *
FormatSafekeeperState(SafekeeperState state)
{
	char	   *return_val = NULL;

	switch (state)
	{
		case SS_OFFLINE:
			return_val = "offline";
			break;
		case SS_CONNECTING_READ:
		case SS_CONNECTING_WRITE:
			return_val = "connecting";
			break;
		case SS_WAIT_EXEC_RESULT:
			return_val = "receiving query result";
			break;
		case SS_HANDSHAKE_RECV:
			return_val = "handshake (receiving)";
			break;
		case SS_VOTING:
			return_val = "voting";
			break;
		case SS_WAIT_VERDICT:
			return_val = "wait-for-verdict";
			break;
		case SS_SEND_ELECTED_FLUSH:
			return_val = "send-announcement-flush";
			break;
		case SS_IDLE:
			return_val = "idle";
			break;
		case SS_ACTIVE:
			return_val = "active";
			break;
	}

	Assert(return_val != NULL);

	return return_val;
}

/* Asserts that the provided events are expected for given safekeeper's state */
void
AssertEventsOkForState(uint32 events, Safekeeper *sk)
{
	uint32		expected = SafekeeperStateDesiredEvents(sk->state);

	/*
	 * The events are in-line with what we're expecting, under two conditions:
	 * (a) if we aren't expecting anything, `events` has no read- or
	 * write-ready component. (b) if we are expecting something, there's
	 * overlap (i.e. `events & expected != 0`)
	 */
	bool		events_ok_for_state;	/* long name so the `Assert` is more
										 * clear later */

	if (expected == WL_NO_EVENTS)
		events_ok_for_state = ((events & (WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE)) == 0);
	else
		events_ok_for_state = ((events & expected) != 0);

	if (!events_ok_for_state)
	{
		/*
		 * To give a descriptive message in the case of failure, we use elog
		 * and then an assertion that's guaranteed to fail.
		 */
		elog(WARNING, "events %s mismatched for safekeeper %s:%s in state [%s]",
			 FormatEvents(events), sk->host, sk->port, FormatSafekeeperState(sk->state));
		Assert(events_ok_for_state);
	}
}

/* Returns the set of events a safekeeper in this state should be waiting on
 *
 * This will return WL_NO_EVENTS (= 0) for some events. */
uint32
SafekeeperStateDesiredEvents(SafekeeperState state)
{
	uint32		result = WL_NO_EVENTS;

	/* If the state doesn't have a modifier, we can check the base state */
	switch (state)
	{
			/* Connecting states say what they want in the name */
		case SS_CONNECTING_READ:
			result = WL_SOCKET_READABLE;
			break;
		case SS_CONNECTING_WRITE:
			result = WL_SOCKET_WRITEABLE;
			break;

			/* Reading states need the socket to be read-ready to continue */
		case SS_WAIT_EXEC_RESULT:
		case SS_HANDSHAKE_RECV:
		case SS_WAIT_VERDICT:
			result = WL_SOCKET_READABLE;
			break;

			/*
			 * Idle states use read-readiness as a sign that the connection
			 * has been disconnected.
			 */
		case SS_VOTING:
		case SS_IDLE:
			result = WL_SOCKET_READABLE;
			break;

			/*
			 * Flush states require write-ready for flushing. Active state
			 * does both reading and writing.
			 *
			 * TODO: SS_ACTIVE sometimes doesn't need to be write-ready. We
			 * should check sk->flushWrite here to set WL_SOCKET_WRITEABLE.
			 */
		case SS_SEND_ELECTED_FLUSH:
		case SS_ACTIVE:
			result = WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE;
			break;

			/* The offline state expects no events. */
		case SS_OFFLINE:
			result = WL_NO_EVENTS;
			break;

		default:
			Assert(false);
			break;
	}

	return result;
}

/* Returns a human-readable string corresponding to the event set
 *
 * If the events do not correspond to something set as the `events` field of a `WaitEvent`, the
 * returned string may be meaingless.
 *
 * The string should not be freed. It should also not be expected to remain the same between
 * function calls. */
char *
FormatEvents(uint32 events)
{
	static char return_str[8];

	/* Helper variable to check if there's extra bits */
	uint32		all_flags = WL_LATCH_SET
	| WL_SOCKET_READABLE
	| WL_SOCKET_WRITEABLE
	| WL_TIMEOUT
	| WL_POSTMASTER_DEATH
	| WL_EXIT_ON_PM_DEATH
	| WL_SOCKET_CONNECTED;

	/*
	 * The formatting here isn't supposed to be *particularly* useful -- it's
	 * just to give an sense of what events have been triggered without
	 * needing to remember your powers of two.
	 */

	return_str[0] = (events & WL_LATCH_SET) ? 'L' : '_';
	return_str[1] = (events & WL_SOCKET_READABLE) ? 'R' : '_';
	return_str[2] = (events & WL_SOCKET_WRITEABLE) ? 'W' : '_';
	return_str[3] = (events & WL_TIMEOUT) ? 'T' : '_';
	return_str[4] = (events & WL_POSTMASTER_DEATH) ? 'D' : '_';
	return_str[5] = (events & WL_EXIT_ON_PM_DEATH) ? 'E' : '_';
	return_str[5] = (events & WL_SOCKET_CONNECTED) ? 'C' : '_';

	if (events & (~all_flags))
	{
		elog(WARNING, "Event formatting found unexpected component %d",
			 events & (~all_flags));
		return_str[6] = '*';
		return_str[7] = '\0';
	}
	else
		return_str[6] = '\0';

	return (char *) &return_str;
}

/*
 * Convert a character which represents a hexadecimal digit to an integer.
 *
 * Returns -1 if the character is not a hexadecimal digit.
 */
static int
HexDecodeChar(char c)
{
	if (c >= '0' && c <= '9')
		return c - '0';
	if (c >= 'a' && c <= 'f')
		return c - 'a' + 10;
	if (c >= 'A' && c <= 'F')
		return c - 'A' + 10;

	return -1;
}

/*
 * Decode a hex string into a byte string, 2 hex chars per byte.
 *
 * Returns false if invalid characters are encountered; otherwise true.
 */
bool
HexDecodeString(uint8 *result, char *input, int nbytes)
{
	int			i;

	for (i = 0; i < nbytes; ++i)
	{
		int			n1 = HexDecodeChar(input[i * 2]);
		int			n2 = HexDecodeChar(input[i * 2 + 1]);

		if (n1 < 0 || n2 < 0)
			return false;
		result[i] = n1 * 16 + n2;
	}

	return true;
}

/* --------------------------------
 *		pq_getmsgint32_le	- get a binary 4-byte int from a message buffer in native (LE) order
 * --------------------------------
 */
uint32
pq_getmsgint32_le(StringInfo msg)
{
	uint32		n32;

	pq_copymsgbytes(msg, (char *) &n32, sizeof(n32));

	return n32;
}

/* --------------------------------
 *		pq_getmsgint64	- get a binary 8-byte int from a message buffer in native (LE) order
 * --------------------------------
 */
uint64
pq_getmsgint64_le(StringInfo msg)
{
	uint64		n64;

	pq_copymsgbytes(msg, (char *) &n64, sizeof(n64));

	return n64;
}

/* append a binary [u]int32 to a StringInfo buffer in native (LE) order */
void
pq_sendint32_le(StringInfo buf, uint32 i)
{
	enlargeStringInfo(buf, sizeof(uint32));
	memcpy(buf->data + buf->len, &i, sizeof(uint32));
	buf->len += sizeof(uint32);
}

/* append a binary [u]int64 to a StringInfo buffer in native (LE) order */
void
pq_sendint64_le(StringInfo buf, uint64 i)
{
	enlargeStringInfo(buf, sizeof(uint64));
	memcpy(buf->data + buf->len, &i, sizeof(uint64));
	buf->len += sizeof(uint64);
}

/*
 * Write XLOG data to disk.
 */
void
XLogWalPropWrite(char *buf, Size nbytes, XLogRecPtr recptr)
{
	int			startoff;
	int			byteswritten;

	while (nbytes > 0)
	{
		int			segbytes;

		/* Close the current segment if it's completed */
		if (walpropFile >= 0 && !XLByteInSeg(recptr, walpropSegNo, wal_segment_size))
			XLogWalPropClose(recptr);

		if (walpropFile < 0)
		{
#if PG_VERSION_NUM >= 150000
			/* FIXME Is it ok to use hardcoded value here? */
			TimeLineID	tli = 1;
#else
			bool		use_existent = true;
#endif
			/* Create/use new log file */
			XLByteToSeg(recptr, walpropSegNo, wal_segment_size);
#if PG_VERSION_NUM >= 150000
			walpropFile = XLogFileInit(walpropSegNo, tli);
			walpropFileTLI = tli;
#else
			walpropFile = XLogFileInit(walpropSegNo, &use_existent, false);
			walpropFileTLI = ThisTimeLineID;
#endif
		}

		/* Calculate the start offset of the received logs */
		startoff = XLogSegmentOffset(recptr, wal_segment_size);

		if (startoff + nbytes > wal_segment_size)
			segbytes = wal_segment_size - startoff;
		else
			segbytes = nbytes;

		/* OK to write the logs */
		errno = 0;

		byteswritten = pg_pwrite(walpropFile, buf, segbytes, (off_t) startoff);
		if (byteswritten <= 0)
		{
			char		xlogfname[MAXFNAMELEN];
			int			save_errno;

			/* if write didn't set errno, assume no disk space */
			if (errno == 0)
				errno = ENOSPC;

			save_errno = errno;
			XLogFileName(xlogfname, walpropFileTLI, walpropSegNo, wal_segment_size);
			errno = save_errno;
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not write to log segment %s "
							"at offset %u, length %lu: %m",
							xlogfname, startoff, (unsigned long) segbytes)));
		}

		/* Update state for write */
		recptr += byteswritten;

		nbytes -= byteswritten;
		buf += byteswritten;
	}

	/*
	 * Close the current segment if it's fully written up in the last cycle of
	 * the loop.
	 */
	if (walpropFile >= 0 && !XLByteInSeg(recptr, walpropSegNo, wal_segment_size))
	{
		XLogWalPropClose(recptr);
	}
}

/*
 * Close the current segment.
 */
void
XLogWalPropClose(XLogRecPtr recptr)
{
	Assert(walpropFile >= 0 && !XLByteInSeg(recptr, walpropSegNo, wal_segment_size));

	if (close(walpropFile) != 0)
	{
		char		xlogfname[MAXFNAMELEN];

		XLogFileName(xlogfname, walpropFileTLI, walpropSegNo, wal_segment_size);

		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not close log segment %s: %m",
						xlogfname)));
	}

	walpropFile = -1;
}

/* START of cloned functions from walsender.c */

/*
 * Subscribe for new WAL and stream it in the loop to safekeepers.
 *
 * At the moment, this never returns, but an ereport(ERROR) will take us back
 * to the main loop.
 */
void
StartProposerReplication(StartReplicationCmd *cmd)
{
	XLogRecPtr	FlushPtr;
	TimeLineID	currTLI;

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
	 * When we first start replication the standby will be behind the
	 * primary. For some applications, for example synchronous
	 * replication, it is important to have a clear state for this initial
	 * catchup mode, so we can trigger actions when we change streaming
	 * state later. We may stay in this state for a long time, which is
	 * exactly why we want to be able to monitor whether or not we are
	 * still here.
	 */
	WalSndSetState(WALSNDSTATE_CATCHUP);

	/*
	 * Don't allow a request to stream from a future point in WAL that
	 * hasn't been flushed to disk in this server yet.
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
	WalSndLoop();

	WalSndSetState(WALSNDSTATE_STARTUP);

	if (cmd->slotname)
		ReplicationSlotRelease();
}

/*
 * Main loop that waits for LSN updates and calls the walproposer.
 * Synchronous replication sets latch in WalSndWakeup at walsender.c
 */
static void
WalSndLoop(void)
{
	/* Clear any already-pending wakeups */
	ResetLatch(MyLatch);

	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		XLogBroadcastWalProposer();

		if (MyWalSnd->state == WALSNDSTATE_CATCHUP)
			WalSndSetState(WALSNDSTATE_STREAMING);
		WalProposerPoll();
	}
}

/*
 * Notify walproposer about the new WAL position.
 */
static void
XLogBroadcastWalProposer(void)
{
	XLogRecPtr	startptr;
	XLogRecPtr	endptr;

	/* Start from the last sent position */
	startptr = sentPtr;

	/*
	 * Streaming the current timeline on a primary.
	 *
	 * Attempt to send all data that's already been written out and
	 * fsync'd to disk.  We cannot go further than what's been written out
	 * given the current implementation of WALRead().  And in any case
	 * it's unsafe to send WAL that is not securely down to disk on the
	 * primary: if the primary subsequently crashes and restarts, standbys
	 * must not have applied any WAL that got lost on the primary.
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

	WalProposerBroadcast(startptr, endptr);
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
