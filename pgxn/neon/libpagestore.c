/*-------------------------------------------------------------------------
 *
 * libpagestore.c
 *	  Handles network communications with the remote pagestore.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	 contrib/neon/libpqpagestore.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "common/hashfn.h"
#include "fmgr.h"
#include "libpq-fe.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "portability/instr_time.h"
#include "postmaster/interrupt.h"
#include "storage/buf_internals.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/pg_shmem.h"
#include "utils/guc.h"

#include "neon.h"
#include "neon_perf_counters.h"
#include "neon_utils.h"
#include "pagestore_client.h"
#include "walproposer.h"

#define PageStoreTrace DEBUG5

#define MIN_RECONNECT_INTERVAL_USEC 1000
#define MAX_RECONNECT_INTERVAL_USEC 1000000

/* GUCs */
char	   *neon_timeline;
char	   *neon_tenant;
int32		max_cluster_size;
char	   *page_server_connstring;
char	   *neon_auth_token;

int			readahead_buffer_size = 128;
int			flush_every_n_requests = 8;

int         neon_protocol_version = 2;

static int	max_reconnect_attempts = 60;
static int	stripe_size;

typedef struct
{
	char		connstring[MAX_SHARDS][MAX_PAGESERVER_CONNSTRING_SIZE];
	size_t		num_shards;
} ShardMap;

/*
 * PagestoreShmemState is kept in shared memory. It contains the connection
 * strings for each shard.
 *
 * The "neon.pageserver_connstring" GUC is marked with the PGC_SIGHUP option,
 * allowing it to be changed using pg_reload_conf(). The control plane can
 * update the connection string if the pageserver crashes, is relocated, or
 * new shards are added. A parsed copy of the current value of the GUC is kept
 * in shared memory, updated by the postmaster, because regular backends don't
 * reload the config during query execution, but we might need to re-establish
 * the pageserver connection with the new connection string even in the middle
 * of a query.
 *
 * The shared memory copy is protected by a lockless algorithm using two
 * atomic counters. The counters allow a backend to quickly check if the value
 * has changed since last access, and to detect and retry copying the value if
 * the postmaster changes the value concurrently. (Postmaster doesn't have a
 * PGPROC entry and therefore cannot use LWLocks.)
 */
typedef struct
{
	pg_atomic_uint64 begin_update_counter;
	pg_atomic_uint64 end_update_counter;
	ShardMap	shard_map;
} PagestoreShmemState;

#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
#endif
static shmem_startup_hook_type prev_shmem_startup_hook;
static PagestoreShmemState *pagestore_shared;
static uint64 pagestore_local_counter = 0;

typedef enum PSConnectionState {
	PS_Disconnected,			/* no connection yet */
	PS_Connecting_Startup,		/* connection starting up */
	PS_Connecting_PageStream,	/* negotiating pagestream */ 
	PS_Connected,				/* connected, pagestream established */
} PSConnectionState;

/* This backend's per-shard connections */
typedef struct
{
	TimestampTz		last_connect_time; /* read-only debug value */
	TimestampTz		last_reconnect_time;
	uint32			delay_us;
	int				n_reconnect_attempts;

	/*---
	 * Pageserver connection state, i.e.
	 *	disconnected: conn == NULL, wes == NULL;
	 *	conn_startup: connection initiated, waiting for connection establishing
	 *	conn_ps:      PageStream query sent, waiting for confirmation
	 *	connected:    PageStream established
	 */
	PSConnectionState state;
	PGconn		   *conn;

	/* request / response counters for debugging */
	uint64			nrequests_sent;
	uint64			nresponses_received;

	/*---
	 * WaitEventSet containing:
	 *	- WL_SOCKET_READABLE on 'conn'
	 *	- WL_LATCH_SET on MyLatch, and
	 *	- WL_EXIT_ON_PM_DEATH.
	 */
	WaitEventSet   *wes_read;
} PageServer;

static PageServer page_servers[MAX_SHARDS];

static bool pageserver_flush(shardno_t shard_no);
static void pageserver_disconnect(shardno_t shard_no);
static void pageserver_disconnect_shard(shardno_t shard_no);

static bool
PagestoreShmemIsValid(void)
{
	return pagestore_shared && UsedShmemSegAddr;
}

/*
 * Parse a comma-separated list of connection strings into a ShardMap.
 *
 * If 'result' is NULL, just checks that the input is valid. If the input is
 * not valid, returns false. The contents of *result are undefined in
 * that case, and must not be relied on.
 */
static bool
ParseShardMap(const char *connstr, ShardMap *result)
{
	const char *p;
	int			nshards = 0;

	if (result)
		memset(result, 0, sizeof(ShardMap));

	p = connstr;
	nshards = 0;
	for (;;)
	{
		const char *sep;
		size_t		connstr_len;

		sep = strchr(p, ',');
		connstr_len = sep != NULL ? sep - p : strlen(p);

		if (connstr_len == 0 && sep == NULL)
			break;				/* ignore trailing comma */

		if (nshards >= MAX_SHARDS)
		{
			neon_log(LOG, "Too many shards");
			return false;
		}
		if (connstr_len >= MAX_PAGESERVER_CONNSTRING_SIZE)
		{
			neon_log(LOG, "Connection string too long");
			return false;
		}
		if (result)
		{
			memcpy(result->connstring[nshards], p, connstr_len);
			result->connstring[nshards][connstr_len] = '\0';
		}
		nshards++;

		if (sep == NULL)
			break;
		p = sep + 1;
	}
	if (result)
		result->num_shards = nshards;

	return true;
}

static bool
CheckPageserverConnstring(char **newval, void **extra, GucSource source)
{
	char	   *p = *newval;

	return ParseShardMap(p, NULL);
}

static void
AssignPageserverConnstring(const char *newval, void *extra)
{
	ShardMap	shard_map;

	/*
	 * Only postmaster updates the copy in shared memory.
	 */
	if (!PagestoreShmemIsValid() || IsUnderPostmaster)
		return;

	if (!ParseShardMap(newval, &shard_map))
	{
		/*
		 * shouldn't happen, because we already checked the value in
		 * CheckPageserverConnstring
		 */
		elog(ERROR, "could not parse shard map");
	}

	if (memcmp(&pagestore_shared->shard_map, &shard_map, sizeof(ShardMap)) != 0)
	{
		pg_atomic_add_fetch_u64(&pagestore_shared->begin_update_counter, 1);
		pg_write_barrier();
		memcpy(&pagestore_shared->shard_map, &shard_map, sizeof(ShardMap));
		pg_write_barrier();
		pg_atomic_add_fetch_u64(&pagestore_shared->end_update_counter, 1);
	}
	else
	{
		/* no change */
	}
}

/*
 * Get the current number of shards, and/or the connection string for a
 * particular shard from the shard map in shared memory.
 *
 * If num_shards_p is not NULL, it is set to the current number of shards.
 *
 * If connstr_p is not NULL, the connection string for 'shard_no' is copied to
 * it. It must point to a buffer at least MAX_PAGESERVER_CONNSTRING_SIZE bytes
 * long.
 *
 * As a side-effect, if the shard map in shared memory had changed since the
 * last call, terminates all existing connections to all pageservers.
 */
static void
load_shard_map(shardno_t shard_no, char *connstr_p, shardno_t *num_shards_p)
{
	uint64		begin_update_counter;
	uint64		end_update_counter;
	ShardMap   *shard_map = &pagestore_shared->shard_map;
	shardno_t	num_shards;

	/*
	 * Postmaster can update the shared memory values concurrently, in which
	 * case we would copy a garbled mix of the old and new values. We will
	 * detect it because the counter's won't match, and retry. But it's
	 * important that we don't do anything within the retry-loop that would
	 * depend on the string having valid contents.
	 */
	do
	{
		begin_update_counter = pg_atomic_read_u64(&pagestore_shared->begin_update_counter);
		end_update_counter = pg_atomic_read_u64(&pagestore_shared->end_update_counter);

		num_shards = shard_map->num_shards;
		if (connstr_p && shard_no < MAX_SHARDS)
			strlcpy(connstr_p, shard_map->connstring[shard_no], MAX_PAGESERVER_CONNSTRING_SIZE);
		pg_memory_barrier();
	}
	while (begin_update_counter != end_update_counter
		   || begin_update_counter != pg_atomic_read_u64(&pagestore_shared->begin_update_counter)
		   || end_update_counter != pg_atomic_read_u64(&pagestore_shared->end_update_counter));

	if (connstr_p && shard_no >= num_shards)
		neon_log(ERROR, "Shard %d is greater or equal than number of shards %d",
				 shard_no, num_shards);

	/*
	 * If any of the connection strings changed, reset all connections.
	 */
	if (pagestore_local_counter != end_update_counter)
	{
		for (shardno_t i = 0; i < MAX_SHARDS; i++)
		{
			if (page_servers[i].conn)
				pageserver_disconnect(i);
		}
		pagestore_local_counter = end_update_counter;
	}

	if (num_shards_p)
		*num_shards_p = num_shards;
}

#define MB (1024*1024)

shardno_t
get_shard_number(BufferTag *tag)
{
	shardno_t	n_shards;
	uint32		hash;

	load_shard_map(0, NULL, &n_shards);

#if PG_MAJORVERSION_NUM < 16
	hash = murmurhash32(tag->rnode.relNode);
	hash = hash_combine(hash, murmurhash32(tag->blockNum / stripe_size));
#else
	hash = murmurhash32(tag->relNumber);
	hash = hash_combine(hash, murmurhash32(tag->blockNum / stripe_size));
#endif

	return hash % n_shards;
}

static inline void
CLEANUP_AND_DISCONNECT(PageServer *shard) 
{
	if (shard->wes_read)
	{
		FreeWaitEventSet(shard->wes_read);
		shard->wes_read = NULL;
	}
	if (shard->conn)
	{
		MyNeonCounters->pageserver_disconnects_total++;
		PQfinish(shard->conn);
		shard->conn = NULL;
	}

	shard->state = PS_Disconnected;
}

/*
 * Connect to a pageserver, or continue to try to connect if we're yet to
 * complete the connection (e.g. due to receiving an earlier cancellation
 * during connection start).
 * Returns true if successfully connected; false if the connection failed.
 * 
 * Throws errors in unrecoverable situations, or when this backend's query
 * is canceled.
 */
static bool
pageserver_connect(shardno_t shard_no, int elevel)
{
	PageServer *shard = &page_servers[shard_no];
	char		connstr[MAX_PAGESERVER_CONNSTRING_SIZE];

	/*
	 * Get the connection string for this shard. If the shard map has been
	 * updated since we last looked, this will also disconnect any existing
	 * pageserver connections as a side effect.
	 * Note that connstr is used both during connection start, and when we
	 * log the successful connection.
	 */
	load_shard_map(shard_no, connstr, NULL);

	switch (shard->state)
	{
	case PS_Disconnected:
	{
		const char *keywords[3];
		const char *values[3];
		int			n_pgsql_params;
		TimestampTz	now;
		int64		us_since_last_attempt;

		/* Make sure we start with a clean slate */
		CLEANUP_AND_DISCONNECT(shard);

		neon_shard_log(shard_no, DEBUG5, "Connection state: Disconnected");

		now = GetCurrentTimestamp();
		us_since_last_attempt = (int64) (now - shard->last_reconnect_time);
		shard->last_reconnect_time = now;

		/*
		 * Make sure we don't do exponential backoff with a constant multiplier
		 * of 0 us, as that doesn't really do much for timeouts...
		 *
		 * cf. https://github.com/neondatabase/neon/issues/7897
		 */
		if (shard->delay_us == 0)
			shard->delay_us = MIN_RECONNECT_INTERVAL_USEC;

		/*
		 * If we did other tasks between reconnect attempts, then we won't
		 * need to wait as long as a full delay.
		 */
		if (us_since_last_attempt < shard->delay_us)
		{
			pg_usleep(shard->delay_us - us_since_last_attempt);
		}

		/* update the delay metric */
		shard->delay_us = Min(shard->delay_us * 2, MAX_RECONNECT_INTERVAL_USEC);

		/*
		 * Connect using the connection string we got from the
		 * neon.pageserver_connstring GUC. If the NEON_AUTH_TOKEN environment
		 * variable was set, use that as the password.
		 *
		 * The connection options are parsed in the order they're given, so when
		 * we set the password before the connection string, the connection string
		 * can override the password from the env variable. Seems useful, although
		 * we don't currently use that capability anywhere.
		 */
		keywords[0] = "dbname";
		values[0] = connstr;
		n_pgsql_params = 1;

		if (neon_auth_token)
		{
			keywords[1] = "password";
			values[1] = neon_auth_token;
			n_pgsql_params++;
		}

		keywords[n_pgsql_params] = NULL;
		values[n_pgsql_params] = NULL;

		shard->conn = PQconnectStartParams(keywords, values, 1);
		if (PQstatus(shard->conn) == CONNECTION_BAD)
		{
			char	   *msg = pchomp(PQerrorMessage(shard->conn));
			CLEANUP_AND_DISCONNECT(shard);
			ereport(elevel,
					(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
						errmsg(NEON_TAG "[shard %d] could not establish connection to pageserver", shard_no),
						errdetail_internal("%s", msg)));
			pfree(msg);
			return false;
		}
		shard->state = PS_Connecting_Startup;
	}
	/* FALLTHROUGH */
	case PS_Connecting_Startup:
	{
		char	   *pagestream_query;
		int			ps_send_query_ret;
		bool		connected = false;
		int poll_result = PGRES_POLLING_WRITING;
		neon_shard_log(shard_no, DEBUG5, "Connection state: Connecting_Startup");

		do
		{
			switch (poll_result)
			{
			default: /* unknown/unused states are handled as a failed connection */
			case PGRES_POLLING_FAILED:
				{
					char	   *pqerr = PQerrorMessage(shard->conn);
					char	   *msg = NULL;
					neon_shard_log(shard_no, DEBUG5, "POLLING_FAILED");

					if (pqerr)
						msg = pchomp(pqerr);

					CLEANUP_AND_DISCONNECT(shard);

					if (msg)
					{
						neon_shard_log(shard_no, elevel,
									   "could not connect to pageserver: %s",
									   msg);
						pfree(msg);
					}
					else
						neon_shard_log(shard_no, elevel,
									   "could not connect to pageserver");

					return false;
				}
			case PGRES_POLLING_READING:
				/* Sleep until there's something to do */
				while (true)
				{
					int rc = WaitLatchOrSocket(MyLatch,
											   WL_EXIT_ON_PM_DEATH | WL_LATCH_SET | WL_SOCKET_READABLE,
											   PQsocket(shard->conn),
											   0,
											   WAIT_EVENT_NEON_PS_STARTING);
					elog(DEBUG5, "PGRES_POLLING_READING=>%d", rc);
					if (rc & WL_LATCH_SET)
					{
						ResetLatch(MyLatch);
						/* query cancellation, backend shutdown */
						CHECK_FOR_INTERRUPTS();
					}
					if (rc & WL_SOCKET_READABLE)
						break;
				}
				/* PQconnectPoll() handles the socket polling state updates */

				break;
			case PGRES_POLLING_WRITING:
				/* Sleep until there's something to do */
				while (true)
				{
					int rc = WaitLatchOrSocket(MyLatch,
											   WL_EXIT_ON_PM_DEATH | WL_LATCH_SET | WL_SOCKET_WRITEABLE,
											   PQsocket(shard->conn),
											   0,
											   WAIT_EVENT_NEON_PS_STARTING);
					elog(DEBUG5, "PGRES_POLLING_WRITING=>%d", rc);
					if (rc & WL_LATCH_SET)
					{
						ResetLatch(MyLatch);
						/* query cancellation, backend shutdown */
						CHECK_FOR_INTERRUPTS();
					}
					if (rc & WL_SOCKET_WRITEABLE)
						break;
				}
				/* PQconnectPoll() handles the socket polling state updates */

				break;
			case PGRES_POLLING_OK:
				neon_shard_log(shard_no, DEBUG5, "POLLING_OK");
				connected = true;
				break;
			}
			poll_result = PQconnectPoll(shard->conn);
			elog(DEBUG5, "PQconnectPoll=>%d", poll_result);
		}
		while (!connected);

		/* No more polling needed; connection succeeded */
		shard->last_connect_time = GetCurrentTimestamp();

#if PG_MAJORVERSION_NUM >= 17
		shard->wes_read = CreateWaitEventSet(NULL, 3);
#else
		shard->wes_read = CreateWaitEventSet(TopMemoryContext, 3);
#endif
		AddWaitEventToSet(shard->wes_read, WL_LATCH_SET, PGINVALID_SOCKET,
						  MyLatch, NULL);
		AddWaitEventToSet(shard->wes_read, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET,
						  NULL, NULL);
		AddWaitEventToSet(shard->wes_read, WL_SOCKET_READABLE, PQsocket(shard->conn), NULL, NULL);


		switch (neon_protocol_version)
		{
		case 3:
			pagestream_query = psprintf("pagestream_v3 %s %s", neon_tenant, neon_timeline);
			break;
		case 2:
			pagestream_query = psprintf("pagestream_v2 %s %s", neon_tenant, neon_timeline);
			break;
		default:
			elog(ERROR, "unexpected neon_protocol_version %d", neon_protocol_version);
		}

		if (PQstatus(shard->conn) == CONNECTION_BAD)
		{
			char	   *msg = pchomp(PQerrorMessage(shard->conn));

			CLEANUP_AND_DISCONNECT(shard);

			ereport(elevel,
					(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
						errmsg(NEON_TAG "[shard %d] could not establish connection to pageserver", shard_no),
						errdetail_internal("%s", msg)));
			pfree(msg);
			return false;
		}

		ps_send_query_ret = PQsendQuery(shard->conn, pagestream_query);
		pfree(pagestream_query);
		if (ps_send_query_ret != 1)
		{
			CLEANUP_AND_DISCONNECT(shard);

			neon_shard_log(shard_no, elevel, "could not send pagestream command to pageserver");
			return false;
		}

		shard->state = PS_Connecting_PageStream;
	}
	/* FALLTHROUGH */
	case PS_Connecting_PageStream:
	{
		neon_shard_log(shard_no, DEBUG5, "Connection state: Connecting_PageStream");

		if (PQstatus(shard->conn) == CONNECTION_BAD)
		{
			char	   *msg = pchomp(PQerrorMessage(shard->conn));
			CLEANUP_AND_DISCONNECT(shard);
			ereport(elevel,
					(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
						errmsg(NEON_TAG "[shard %d] could not establish connection to pageserver", shard_no),
						errdetail_internal("%s", msg)));
			pfree(msg);
			return false;
		}

		while (PQisBusy(shard->conn))
		{
			WaitEvent	event;

			/* Sleep until there's something to do */
			(void) WaitEventSetWait(shard->wes_read, -1L, &event, 1,
									WAIT_EVENT_NEON_PS_CONFIGURING);
			ResetLatch(MyLatch);

			CHECK_FOR_INTERRUPTS();

			/* Data available in socket? */
			if (event.events & WL_SOCKET_READABLE)
			{
				if (!PQconsumeInput(shard->conn))
				{
					char	   *msg = pchomp(PQerrorMessage(shard->conn));

					CLEANUP_AND_DISCONNECT(shard);
					neon_shard_log(shard_no, elevel, "could not complete handshake with pageserver: %s",
								   msg);
					pfree(msg);
					return false;
				}
			}
		}

		shard->state = PS_Connected;
		shard->nrequests_sent = 0;
		shard->nresponses_received = 0;
	}
	/* FALLTHROUGH */
	case PS_Connected:
		/*
		 * We successfully connected. Future connections to this PageServer
		 * will do fast retries again, with exponential backoff.
		 */
		shard->delay_us = MIN_RECONNECT_INTERVAL_USEC;

		neon_shard_log(shard_no, DEBUG5, "Connection state: Connected");
		neon_shard_log(shard_no, LOG, "libpagestore: connected to '%s' with protocol version %d", connstr, neon_protocol_version);
		return true;
	default:
		neon_shard_log(shard_no, ERROR, "libpagestore: invalid connection state %d", shard->state);
	}
	/* This shouldn't be hit */
	Assert(false);
}

/*
 * A wrapper around PQgetCopyData that checks for interrupts while sleeping.
 */
static int
call_PQgetCopyData(shardno_t shard_no, char **buffer)
{
	int			ret;
	PageServer *shard = &page_servers[shard_no];
	PGconn	   *pageserver_conn = shard->conn;
	instr_time	now,
				start_ts,
				since_start,
				last_log_ts,
				since_last_log;
	bool		logged = false;

	/*
	 * As a debugging aid, if we don't get a response for a long time, print a
	 * log message.
	 *
	 * 10 s is a very generous threshold, normally we expect a response in a
	 * few milliseconds. We have metrics to track latencies in normal ranges,
	 * but in the cases that take exceptionally long, it's useful to log the
	 * exact timestamps.
	 */
#define LOG_INTERVAL_MS		INT64CONST(10 * 1000)

	INSTR_TIME_SET_CURRENT(now);
	start_ts = last_log_ts = now;
	INSTR_TIME_SET_ZERO(since_last_log);

retry:
	ret = PQgetCopyData(pageserver_conn, buffer, 1 /* async */ );

	if (ret == 0)
	{
		WaitEvent	event;
		long		timeout;

		timeout = Max(0, LOG_INTERVAL_MS - INSTR_TIME_GET_MILLISEC(since_last_log));

		/* Sleep until there's something to do */
		(void) WaitEventSetWait(shard->wes_read, timeout, &event, 1,
								WAIT_EVENT_NEON_PS_READ);
		ResetLatch(MyLatch);

		CHECK_FOR_INTERRUPTS();

		/* Data available in socket? */
		if (event.events & WL_SOCKET_READABLE)
		{
			if (!PQconsumeInput(pageserver_conn))
			{
				char	   *msg = pchomp(PQerrorMessage(pageserver_conn));

				neon_shard_log(shard_no, LOG, "could not get response from pageserver: %s", msg);
				pfree(msg);
				return -1;
			}
		}

		/*
		 * Print a message to the log if a long time has passed with no
		 * response.
		 */
		INSTR_TIME_SET_CURRENT(now);
		since_last_log = now;
		INSTR_TIME_SUBTRACT(since_last_log, last_log_ts);
		if (INSTR_TIME_GET_MILLISEC(since_last_log) >= LOG_INTERVAL_MS)
		{
			since_start = now;
			INSTR_TIME_SUBTRACT(since_start, start_ts);
			neon_shard_log(shard_no, LOG, "no response received from pageserver for %0.3f s, still waiting (sent " UINT64_FORMAT " requests, received " UINT64_FORMAT " responses)",
						   INSTR_TIME_GET_DOUBLE(since_start),
						   shard->nrequests_sent, shard->nresponses_received);
			last_log_ts = now;
			logged = true;
		}

		goto retry;
	}

	/*
	 * If we logged earlier that the response is taking a long time, log
	 * another message when the response is finally received.
	 */
	if (logged)
	{
		INSTR_TIME_SET_CURRENT(now);
		since_start = now;
		INSTR_TIME_SUBTRACT(since_start, start_ts);
		neon_shard_log(shard_no, LOG, "received response from pageserver after %0.3f s",
					   INSTR_TIME_GET_DOUBLE(since_start));
	}

	return ret;
}

/*
 * Reset prefetch and drop connection to the shard.
 * It also drops connection to all other shards involved in prefetch, through
 * prefetch_on_ps_disconnect().
 */
static void
pageserver_disconnect(shardno_t shard_no)
{
	/*
	 * If the connection to any pageserver is lost, we throw away the
	 * whole prefetch queue, even for other pageservers. It should not
	 * cause big problems, because connection loss is supposed to be a
	 * rare event.
	 */
	prefetch_on_ps_disconnect();

	pageserver_disconnect_shard(shard_no);
}

/*
 * Disconnect from specified shard
 */
static void
pageserver_disconnect_shard(shardno_t shard_no)
{
	PageServer *shard = &page_servers[shard_no];
	/*
	 * If anything goes wrong while we were sending a request, it's not clear
	 * what state the connection is in. For example, if we sent the request
	 * but didn't receive a response yet, we might receive the response some
	 * time later after we have already sent a new unrelated request. Close
	 * the connection to avoid getting confused.
	 * Similarly, even when we're in PS_DISCONNECTED, we may have junk to
	 * clean up: It is possible that we encountered an error allocating any
	 * of the wait event sets or the psql connection, or failed when we tried
	 * to attach wait events to the WaitEventSets.
	 */
	CLEANUP_AND_DISCONNECT(shard);

	shard->state = PS_Disconnected;
}

static bool
pageserver_send(shardno_t shard_no, NeonRequest *request)
{
	StringInfoData req_buff;
	PageServer *shard = &page_servers[shard_no];
	PGconn	   *pageserver_conn;

	MyNeonCounters->pageserver_requests_sent_total++;

	/* If the connection was lost for some reason, reconnect */
	if (shard->state == PS_Connected && PQstatus(shard->conn) == CONNECTION_BAD)
	{
		neon_shard_log(shard_no, LOG, "pageserver_send disconnect bad connection");
		pageserver_disconnect(shard_no);
		pageserver_conn = NULL;
	}

	req_buff = nm_pack_request(request);

	/*
	 * If pageserver is stopped, the connections from compute node are broken.
	 * The compute node doesn't notice that immediately, but it will cause the
	 * next request to fail, usually on the next query. That causes
	 * user-visible errors if pageserver is restarted, or the tenant is moved
	 * from one pageserver to another. See
	 * https://github.com/neondatabase/neon/issues/1138 So try to reestablish
	 * connection in case of failure.
	 */
	if (shard->state != PS_Connected)
	{
		while (!pageserver_connect(shard_no, shard->n_reconnect_attempts < max_reconnect_attempts ? LOG : ERROR))
		{
			shard->n_reconnect_attempts += 1;
		}
		shard->n_reconnect_attempts = 0;
	} else {
		Assert(shard->conn != NULL);
	}

	pageserver_conn = shard->conn;

	/*
	 * Send request.
	 *
	 * In principle, this could block if the output buffer is full, and we
	 * should use async mode and check for interrupts while waiting. In
	 * practice, our requests are small enough to always fit in the output and
	 * TCP buffer.
	 *
	 * Note that this also will fail when the connection is in the
	 * PGRES_POLLING_WRITING state. It's kinda dirty to disconnect at this
	 * point, but on the grand scheme of things it's only a small issue.
	 */
	shard->nrequests_sent++;
	if (PQputCopyData(pageserver_conn, req_buff.data, req_buff.len) <= 0)
	{
		char	   *msg = pchomp(PQerrorMessage(pageserver_conn));

		pageserver_disconnect(shard_no);
		neon_shard_log(shard_no, LOG, "pageserver_send disconnected: failed to send page request (try to reconnect): %s", msg);
		pfree(msg);
		pfree(req_buff.data);
		return false;
	}

	pfree(req_buff.data);

	if (message_level_is_interesting(PageStoreTrace))
	{
		char	   *msg = nm_to_string((NeonMessage *) request);

		neon_shard_log(shard_no, PageStoreTrace, "sent request: %s", msg);
		pfree(msg);
	}

	return true;
}

static NeonResponse *
pageserver_receive(shardno_t shard_no)
{
	StringInfoData resp_buff;
	NeonResponse *resp;
	PageServer *shard = &page_servers[shard_no];
	PGconn	   *pageserver_conn = shard->conn;
	/* read response */
	int			rc;

	if (shard->state != PS_Connected)
	{
		neon_shard_log(shard_no, LOG,
					   "pageserver_receive: returning NULL for non-connected pageserver connection: 0x%02x",
					   shard->state);
		return NULL;
	}

	Assert(pageserver_conn);

	rc = call_PQgetCopyData(shard_no, &resp_buff.data);
	if (rc >= 0)
	{
		/* call_PQgetCopyData handles rc == 0 */
		Assert(rc > 0);

		PG_TRY();
		{
			resp_buff.len = rc;
			resp_buff.cursor = 0;
			resp = nm_unpack_response(&resp_buff);
			PQfreemem(resp_buff.data);
		}
		PG_CATCH();
		{
			neon_shard_log(shard_no, LOG, "pageserver_receive: disconnect due malformatted response");
			pageserver_disconnect(shard_no);
			PG_RE_THROW();
		}
		PG_END_TRY();

		if (message_level_is_interesting(PageStoreTrace))
		{
			char	   *msg = nm_to_string((NeonMessage *) resp);

			neon_shard_log(shard_no, PageStoreTrace, "got response: %s", msg);
			pfree(msg);
		}
	}
	else if (rc == -1)
	{
		neon_shard_log(shard_no, LOG, "pageserver_receive disconnect: psql end of copy data: %s", pchomp(PQerrorMessage(pageserver_conn)));
		pageserver_disconnect(shard_no);
		resp = NULL;
	}
	else if (rc == -2)
	{
		char	   *msg = pchomp(PQerrorMessage(pageserver_conn));

		pageserver_disconnect(shard_no);
		neon_shard_log(shard_no, ERROR, "pageserver_receive disconnect: could not read COPY data: %s", msg);
	}
	else
	{
		pageserver_disconnect(shard_no);
		neon_shard_log(shard_no, ERROR, "pageserver_receive disconnect: unexpected PQgetCopyData return value: %d", rc);
	}

	shard->nresponses_received++;
	return (NeonResponse *) resp;
}


static bool
pageserver_flush(shardno_t shard_no)
{
	PGconn	   *pageserver_conn = page_servers[shard_no].conn;

	if (page_servers[shard_no].state != PS_Connected)
	{
		neon_shard_log(shard_no, WARNING, "Tried to flush while disconnected");
	}
	else
	{
		MyNeonCounters->pageserver_send_flushes_total++;
		if (PQflush(pageserver_conn))
		{
			char	   *msg = pchomp(PQerrorMessage(pageserver_conn));

			pageserver_disconnect(shard_no);
			neon_shard_log(shard_no, LOG, "pageserver_flush disconnect because failed to flush page requests: %s", msg);
			pfree(msg);
			return false;
		}
	}

	return true;
}

page_server_api api =
{
	.send = pageserver_send,
	.flush = pageserver_flush,
	.receive = pageserver_receive,
	.disconnect = pageserver_disconnect_shard
};

static bool
check_neon_id(char **newval, void **extra, GucSource source)
{
	uint8		id[16];

	return **newval == '\0' || HexDecodeString(id, *newval, 16);
}

static Size
PagestoreShmemSize(void)
{
	return add_size(sizeof(PagestoreShmemState), NeonPerfCountersShmemSize());
}

static bool
PagestoreShmemInit(void)
{
	bool		found;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	pagestore_shared = ShmemInitStruct("libpagestore shared state",
									   sizeof(PagestoreShmemState),
									   &found);
	if (!found)
	{
		pg_atomic_init_u64(&pagestore_shared->begin_update_counter, 0);
		pg_atomic_init_u64(&pagestore_shared->end_update_counter, 0);
		memset(&pagestore_shared->shard_map, 0, sizeof(ShardMap));
		AssignPageserverConnstring(page_server_connstring, NULL);
	}

	NeonPerfCountersShmemInit();

	LWLockRelease(AddinShmemInitLock);
	return found;
}

static void
pagestore_shmem_startup_hook(void)
{
	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	PagestoreShmemInit();
}

static void
pagestore_shmem_request(void)
{
#if PG_VERSION_NUM >= 150000
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();
#endif

	RequestAddinShmemSpace(PagestoreShmemSize());
}

static void
pagestore_prepare_shmem(void)
{
#if PG_VERSION_NUM >= 150000
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = pagestore_shmem_request;
#else
	pagestore_shmem_request();
#endif
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pagestore_shmem_startup_hook;
}

/*
 * Module initialization function
 */
void
pg_init_libpagestore(void)
{
	pagestore_prepare_shmem();

	DefineCustomStringVariable("neon.pageserver_connstring",
							   "connection string to the page server",
							   NULL,
							   &page_server_connstring,
							   "",
							   PGC_SIGHUP,
							   0,	/* no flags required */
							   CheckPageserverConnstring, AssignPageserverConnstring, NULL);

	DefineCustomStringVariable("neon.timeline_id",
							   "Neon timeline_id the server is running on",
							   NULL,
							   &neon_timeline,
							   "",
							   PGC_POSTMASTER,
							   0,	/* no flags required */
							   check_neon_id, NULL, NULL);

	DefineCustomStringVariable("neon.tenant_id",
							   "Neon tenant_id the server is running on",
							   NULL,
							   &neon_tenant,
							   "",
							   PGC_POSTMASTER,
							   0,	/* no flags required */
							   check_neon_id, NULL, NULL);

	DefineCustomIntVariable("neon.stripe_size",
							"sharding stripe size",
							NULL,
							&stripe_size,
							32768, 1, INT_MAX,
							PGC_SIGHUP,
							GUC_UNIT_BLOCKS,
							NULL, NULL, NULL);

	DefineCustomIntVariable("neon.max_cluster_size",
							"cluster size limit",
							NULL,
							&max_cluster_size,
							-1, -1, INT_MAX,
							PGC_SIGHUP,
							GUC_UNIT_MB,
							NULL, NULL, NULL);
	DefineCustomIntVariable("neon.flush_output_after",
							"Flush the output buffer after every N unflushed requests",
							NULL,
							&flush_every_n_requests,
							8, -1, INT_MAX,
							PGC_USERSET,
							0,	/* no flags required */
							NULL, NULL, NULL);
	DefineCustomIntVariable("neon.max_reconnect_attempts",
							"Maximal attempts to reconnect to pages server (with 1 second timeout)",
							NULL,
							&max_reconnect_attempts,
							60, 0, INT_MAX,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);
	DefineCustomIntVariable("neon.readahead_buffer_size",
							"number of prefetches to buffer",
							"This buffer is used to hold and manage prefetched "
							"data; so it is important that this buffer is at "
							"least as large as the configured value of all "
							"tablespaces' effective_io_concurrency and "
							"maintenance_io_concurrency, and your sessions' "
							"values for these settings.",
							&readahead_buffer_size,
							128, 16, 1024,
							PGC_USERSET,
							0,	/* no flags required */
							NULL, (GucIntAssignHook) &readahead_buffer_resize, NULL);
	DefineCustomIntVariable("neon.protocol_version",
							"Version of compute<->page server protocol",
							NULL,
							&neon_protocol_version,
							2,	/* use protocol version 2 */
							2,	/* min */
							3,	/* max */
							PGC_SU_BACKEND,
							0,	/* no flags required */
							NULL, NULL, NULL);

	relsize_hash_init();

	if (page_server != NULL)
		neon_log(ERROR, "libpagestore already loaded");

	neon_log(PageStoreTrace, "libpagestore already loaded");
	page_server = &api;

	/*
	 * Retrieve the auth token to use when connecting to pageserver and
	 * safekeepers
	 */
	neon_auth_token = getenv("NEON_AUTH_TOKEN");
	if (neon_auth_token)
		neon_log(LOG, "using storage auth token from NEON_AUTH_TOKEN environment variable");

	if (page_server_connstring && page_server_connstring[0])
	{
		neon_log(PageStoreTrace, "set neon_smgr hook");
		smgr_hook = smgr_neon;
		smgr_init_hook = smgr_init_neon;
		dbsize_hook = neon_dbsize;
	}

	memset(page_servers, 0, sizeof(page_servers));

	lfc_init();
}
