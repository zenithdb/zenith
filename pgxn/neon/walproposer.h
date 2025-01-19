#ifndef __NEON_WALPROPOSER_H__
#define __NEON_WALPROPOSER_H__

#include "access/transam.h"
#include "access/xlogdefs.h"
#include "access/xlog_internal.h"
#include "nodes/replnodes.h"
#include "replication/walreceiver.h"
#include "utils/uuid.h"

#include "libpqwalproposer.h"
#include "neon_walreader.h"
#include "pagestore_client.h"

#define MAX_SAFEKEEPERS 32
#define MAX_SEND_SIZE (XLOG_BLCKSZ * 16)	/* max size of a single* WAL
											 * message */
/*
 * In the spirit of WL_SOCKET_READABLE and others, this corresponds to no events having occurred,
 * because all WL_* events are given flags equal to some (1 << i), starting from i = 0
 */
#define WL_NO_EVENTS 0

struct WalProposerConn;			/* Defined in libpqwalproposer.h */
typedef struct WalProposerConn WalProposerConn;

/*
 * WAL safekeeper state, which is used to wait for some event.
 *
 * States are listed here in the order that they're executed.
 *
 * Most states, upon failure, will move back to SS_OFFLINE by calls to
 * ResetConnection or ShutdownConnection.
 */
typedef enum
{
	/*
	 * Does not have an active connection and will stay that way until further
	 * notice.
	 *
	 * Moves to SS_CONNECTING_WRITE by calls to ResetConnection.
	 */
	SS_OFFLINE,

	/*
	 * Connecting states. "_READ" waits for the socket to be available for
	 * reading, "_WRITE" waits for writing. There's no difference in the code
	 * they execute when polled, but we have this distinction in order to
	 * recreate the event set in HackyRemoveWalProposerEvent.
	 *
	 * After the connection is made, "START_WAL_PUSH" query is sent.
	 */
	SS_CONNECTING_WRITE,
	SS_CONNECTING_READ,

	/*
	 * Waiting for the result of the "START_WAL_PUSH" command.
	 *
	 * After we get a successful result, sends handshake to safekeeper.
	 */
	SS_WAIT_EXEC_RESULT,

	/*
	 * Executing the receiving half of the handshake. After receiving, moves
	 * to SS_VOTING.
	 */
	SS_HANDSHAKE_RECV,

	/*
	 * Waiting to participate in voting, but a quorum hasn't yet been reached.
	 * This is an idle state - we do not expect AdvancePollState to be called.
	 *
	 * Moved externally by execution of SS_HANDSHAKE_RECV, when we received a
	 * quorum of handshakes.
	 */
	SS_VOTING,

	/*
	 * Already sent voting information, waiting to receive confirmation from
	 * the node. After receiving, moves to SS_IDLE, if the quorum isn't
	 * reached yet.
	 */
	SS_WAIT_VERDICT,

	/* Need to flush ProposerElected message. */
	SS_SEND_ELECTED_FLUSH,

	/*
	 * Waiting for quorum to send WAL. Idle state. If the socket becomes
	 * read-ready, the connection has been closed.
	 *
	 * Moves to SS_ACTIVE only by call to StartStreaming.
	 */
	SS_IDLE,

	/*
	 * Active phase, when we acquired quorum and have WAL to send or feedback
	 * to read.
	 */
	SS_ACTIVE,
} SafekeeperState;

/*
 * Sending WAL substates of SS_ACTIVE.
 */
typedef enum
{
	/*
	 * We are ready to send more WAL, waiting for latch set to learn about
	 * more WAL becoming available (or just a timeout to send heartbeat).
	 */
	SS_ACTIVE_SEND,

	/*
	 * Polling neon_walreader to receive chunk of WAL (probably remotely) to
	 * send to this safekeeper.
	 *
	 * Note: socket management is done completely inside walproposer_pg for
	 * simplicity, and thus simulation doesn't test it. Which is fine as
	 * simulation is mainly aimed at consensus checks, not waiteventset
	 * management.
	 *
	 * Also, while in this state we don't touch safekeeper socket, so in
	 * theory it might close connection as inactive. This can be addressed if
	 * needed; however, while fetching WAL we should regularly send it, so the
	 * problem is unlikely. Vice versa is also true (SS_ACTIVE doesn't handle
	 * walreader socket), but similarly shouldn't be a problem.
	 */
	SS_ACTIVE_READ_WAL,

	/*
	 * Waiting for write readiness to flush the socket.
	 */
	SS_ACTIVE_FLUSH,
} SafekeeperActiveState;

/* Consensus logical timestamp. */
typedef uint64 term_t;

/* neon storage node id */
typedef uint64 NNodeId;

/*
 * Number uniquely identifying safekeeper membership configuration.
 * This and following structs pair ones in membership.rs.
 */
typedef uint32 Generation;

typedef struct SafekeeperId
{
	NNodeId		node_id;
	char		host[MAXCONNINFO];
	uint16		port;
} SafekeeperId;

/* Set of safekeepers. */
typedef struct MemberSet
{
	uint32		n_members;
	SafekeeperId *members;
} MemberSet;

/* Timeline safekeeper membership configuration. */
typedef struct MembershipConfiguration
{
	Generation	generation;
	MemberSet	members;
	/* Has 0 n_members in non joint conf. */
	MemberSet	new_members;
} MembershipConfiguration;

/*
 * Proposer <-> Acceptor messaging.
 */

typedef struct ProposerAcceptorMessage
{
	uint8		tag;
} ProposerAcceptorMessage;

/* Initial Proposer -> Acceptor message */
typedef struct ProposerGreeting
{
	ProposerAcceptorMessage pam;	/* message tag */
	uint32		proto_version;	/* proposer-safekeeper protocol version */

	/*
	 * tenant/timeline ids as C strings with standard hex notation for ease of
	 * printing. In principle they are not strictly needed as ttid is also
	 * passed as libpq options.
	 */
	char	   *tenant_id;
	char	   *timeline_id;
	MembershipConfiguration mconf;

	/*
	 * pg_version and wal_seg_size are used for timeline creation until we
	 * fully migrate to doing externally. systemId is only used as a sanity
	 * cross check.
	 */
	uint32		pg_version;		/* in PG_VERSION_NUM format */
	uint64		system_id;		/* Postgres system identifier. */
	uint32		wal_seg_size;
} ProposerGreeting;

/* protocol v2 variant, kept while wp supports it */
typedef struct ProposerGreetingV2
{
	uint64		tag;			/* message tag */
	uint32		protocolVersion;	/* proposer-safekeeper protocol version */
	uint32		pgVersion;
	pg_uuid_t	proposerId;
	uint64		systemId;		/* Postgres system identifier */
	uint8		timeline_id[16];	/* Neon timeline id */
	uint8		tenant_id[16];
	TimeLineID	timeline;
	uint32		walSegSize;
} ProposerGreetingV2;

typedef struct AcceptorProposerMessage
{
	uint64		tag;
} AcceptorProposerMessage;

/*
 * Acceptor -> Proposer initial response: the highest term acceptor voted for.
 */
typedef struct AcceptorGreeting
{
	AcceptorProposerMessage apm;
	term_t		term;
	NNodeId		nodeId;
} AcceptorGreeting;

/*
 * Proposer -> Acceptor vote request.
 */
typedef struct VoteRequest
{
	uint64		tag;
	term_t		term;
	pg_uuid_t	proposerId;		/* for monitoring/debugging */
} VoteRequest;

/* Element of term switching chain. */
typedef struct TermSwitchEntry
{
	term_t		term;
	XLogRecPtr	lsn;
} TermSwitchEntry;

typedef struct TermHistory
{
	uint32		n_entries;
	TermSwitchEntry *entries;
} TermHistory;

/* Vote itself, sent from safekeeper to proposer */
typedef struct VoteResponse
{
	AcceptorProposerMessage apm;
	term_t		term;
	uint64		voteGiven;

	/*
	 * Safekeeper flush_lsn (end of WAL) + history of term switches allow
	 * proposer to choose the most advanced one.
	 */
	XLogRecPtr	flushLsn;
	XLogRecPtr	truncateLsn;	/* minimal LSN which may be needed for*
								 * recovery of some safekeeper */
	TermHistory termHistory;
	XLogRecPtr	timelineStartLsn;	/* timeline globally starts at this LSN */
} VoteResponse;

/*
 * Proposer -> Acceptor message announcing proposer is elected and communicating
 * epoch history to it.
 */
typedef struct ProposerElected
{
	uint64		tag;
	term_t		term;
	/* proposer will send since this point */
	XLogRecPtr	startStreamingAt;
	/* history of term switches up to this proposer */
	TermHistory *termHistory;
	/* timeline globally starts at this LSN */
	XLogRecPtr	timelineStartLsn;
} ProposerElected;

/*
 * Header of request with WAL message sent from proposer to safekeeper.
 */
typedef struct AppendRequestHeader
{
	uint64		tag;
	term_t		term;			/* term of the proposer */

	/*
	 * LSN since which current proposer appends WAL (begin_lsn of its first
	 * record); determines epoch switch point.
	 */
	XLogRecPtr	epochStartLsn;
	XLogRecPtr	beginLsn;		/* start position of message in WAL */
	XLogRecPtr	endLsn;			/* end position of message in WAL */
	XLogRecPtr	commitLsn;		/* LSN committed by quorum of safekeepers */

	/*
	 * minimal LSN which may be needed for recovery of some safekeeper (end
	 * lsn + 1 of last chunk streamed to everyone)
	 */
	XLogRecPtr	truncateLsn;
	pg_uuid_t	proposerId;		/* for monitoring/debugging */
	/* in the AppendRequest message, WAL data follows */
} AppendRequestHeader;

/*
 * Hot standby feedback received from replica
 */
typedef struct HotStandbyFeedback
{
	TimestampTz ts;
	FullTransactionId xmin;
	FullTransactionId catalog_xmin;
} HotStandbyFeedback;

typedef struct PageserverFeedback
{
	/* true if AppendResponse contains this feedback */
	bool		present;
	/* current size of the timeline on pageserver */
	uint64		currentClusterSize;
	/* standby_status_update fields that safekeeper received from pageserver */
	XLogRecPtr	last_received_lsn;
	XLogRecPtr	disk_consistent_lsn;
	XLogRecPtr	remote_consistent_lsn;
	TimestampTz replytime;
	uint32		shard_number;
} PageserverFeedback;

typedef struct WalproposerShmemState
{
	pg_atomic_uint64 propEpochStartLsn;
	char		donor_name[64];
	char		donor_conninfo[MAXCONNINFO];
	XLogRecPtr	donor_lsn;

	slock_t		mutex;
	pg_atomic_uint64 mineLastElectedTerm;
	pg_atomic_uint64 backpressureThrottlingTime;
	pg_atomic_uint64 currentClusterSize;

	/* last feedback from each shard */
	PageserverFeedback shard_ps_feedback[MAX_SHARDS];
	int			num_shards;

	/* aggregated feedback with min LSNs across shards */
	PageserverFeedback min_ps_feedback;
} WalproposerShmemState;

/*
 * Report safekeeper state to proposer
 */
typedef struct AppendResponse
{
	AcceptorProposerMessage apm;

	/*
	 * Current term of the safekeeper; if it is higher than proposer's, the
	 * compute is out of date.
	 */
	term_t		term;
	/* TODO: add comment */
	XLogRecPtr	flushLsn;
	/* Safekeeper reports back his awareness about which WAL is committed, as */
	/* this is a criterion for walproposer --sync mode exit */
	XLogRecPtr	commitLsn;
	HotStandbyFeedback hs;
	/* Feedback received from pageserver includes standby_status_update fields */
	/* and custom neon feedback. */
	/* This part of the message is extensible. */
	PageserverFeedback ps_feedback;
} AppendResponse;

/*  PageserverFeedback is extensible part of the message that is parsed separately */
/*  Other fields are fixed part */
#define APPENDRESPONSE_FIXEDPART_SIZE 56

struct WalProposer;
typedef struct WalProposer WalProposer;

/*
 * Descriptor of safekeeper
 */
typedef struct Safekeeper
{
	WalProposer *wp;

	char const *host;
	char const *port;

	/*
	 * connection string for connecting/reconnecting.
	 *
	 * May contain private information like password and should not be logged.
	 */
	char		conninfo[MAXCONNINFO];

	/*
	 * Temporary buffer for the message being sent to the safekeeper.
	 */
	StringInfoData outbuf;

	/*
	 * Streaming will start here; must be record boundary.
	 */
	XLogRecPtr	startStreamingAt;

	XLogRecPtr	streamingAt;	/* current streaming position */
	AppendRequestHeader appendRequest;	/* request for sending to safekeeper */

	SafekeeperState state;		/* safekeeper state machine state */
	SafekeeperActiveState active_state;
	TimestampTz latestMsgReceivedAt;	/* when latest msg is received */
	AcceptorGreeting greetResponse; /* acceptor greeting */
	VoteResponse voteResponse;	/* the vote */
	AppendResponse appendResponse;	/* feedback for master */


	/* postgres-specific fields */
#ifndef WALPROPOSER_LIB

	/*
	 * postgres protocol connection to the WAL acceptor
	 *
	 * Equals NULL only when state = SS_OFFLINE. Nonblocking is set once we
	 * reach SS_ACTIVE; not before.
	 */
	WalProposerConn *conn;

	/*
	 * WAL reader, allocated for each safekeeper.
	 */
	NeonWALReader *xlogreader;

	/*
	 * Position in wait event set. Equal to -1 if no event
	 */
	int			eventPos;

	/*
	 * Neon WAL reader position in wait event set, or -1 if no socket. Note
	 * that event must be removed not only on error/failure, but also on
	 * successful *local* read, as next read might again be remote, but with
	 * different socket.
	 */
	int			nwrEventPos;

	/*
	 * Per libpq docs, during connection establishment socket might change,
	 * remember here if it is stable to avoid readding to the event set if
	 * possible. Must be reset whenever nwr event is deleted.
	 */
	bool		nwrConnEstablished;
#endif


	/* WalProposer library specifics */
#ifdef WALPROPOSER_LIB

	/*
	 * Buffer for incoming messages. Usually Rust vector is stored here.
	 * Caller is responsible for freeing the buffer.
	 */
	StringInfoData inbuf;
#endif
} Safekeeper;

/* Re-exported PostgresPollingStatusType */
typedef enum
{
	WP_CONN_POLLING_FAILED = 0,
	WP_CONN_POLLING_READING,
	WP_CONN_POLLING_WRITING,
	WP_CONN_POLLING_OK,

	/*
	 * 'libpq-fe.h' still has PGRES_POLLING_ACTIVE, but says it's unused.
	 * We've removed it here to avoid clutter.
	 */
} WalProposerConnectPollStatusType;

/* Re-exported ConnStatusType */
typedef enum
{
	WP_CONNECTION_OK,
	WP_CONNECTION_BAD,

	/*
	 * The original ConnStatusType has many more tags, but requests that they
	 * not be relied upon (except for displaying to the user). We don't need
	 * that extra functionality, so we collect them into a single tag here.
	 */
	WP_CONNECTION_IN_PROGRESS,
} WalProposerConnStatusType;

/*
 * Collection of hooks for walproposer, to call postgres functions,
 * read WAL and send it over the network.
 */
typedef struct walproposer_api
{
	/*
	 * Get WalproposerShmemState. This is used to store information about last
	 * elected term.
	 */
	WalproposerShmemState *(*get_shmem_state) (WalProposer *wp);

	/*
	 * Start receiving notifications about new WAL. This is an infinite loop
	 * which calls WalProposerBroadcast() and WalProposerPoll() to send the
	 * WAL.
	 */
	void		(*start_streaming) (WalProposer *wp, XLogRecPtr startpos);

	/* Get pointer to the latest available WAL. */
	XLogRecPtr	(*get_flush_rec_ptr) (WalProposer *wp);

	/* Update current donor info in WalProposer Shmem */
	void		(*update_donor) (WalProposer *wp, Safekeeper *donor, XLogRecPtr donor_lsn);

	/* Get current time. */
	TimestampTz (*get_current_timestamp) (WalProposer *wp);

	/* Current error message, aka PQerrorMessage. */
	char	   *(*conn_error_message) (Safekeeper *sk);

	/* Connection status, aka PQstatus. */
	WalProposerConnStatusType (*conn_status) (Safekeeper *sk);

	/* Start the connection, aka PQconnectStart. */
	void		(*conn_connect_start) (Safekeeper *sk);

	/* Poll an asynchronous connection, aka PQconnectPoll. */
	WalProposerConnectPollStatusType (*conn_connect_poll) (Safekeeper *sk);

	/* Send a blocking SQL query, aka PQsendQuery. */
	bool		(*conn_send_query) (Safekeeper *sk, char *query);

	/* Read the query result, aka PQgetResult. */
	WalProposerExecStatusType (*conn_get_query_result) (Safekeeper *sk);

	/* Flush buffer to the network, aka PQflush. */
	int			(*conn_flush) (Safekeeper *sk);

	/* Reset sk state: close pq connection, deallocate xlogreader. */
	void		(*conn_finish) (Safekeeper *sk);

	/*
	 * Try to read CopyData message from the safekeeper, aka PQgetCopyData.
	 *
	 * On success, the data is placed in *buf. It is valid until the next call
	 * to this function.
	 *
	 * Returns PG_ASYNC_READ_FAIL on closed connection.
	 */
	PGAsyncReadResult (*conn_async_read) (Safekeeper *sk, char **buf, int *amount);

	/* Try to write CopyData message, aka PQputCopyData. */
	PGAsyncWriteResult (*conn_async_write) (Safekeeper *sk, void const *buf, size_t size);

	/* Blocking CopyData write, aka PQputCopyData + PQflush. */
	bool		(*conn_blocking_write) (Safekeeper *sk, void const *buf, size_t size);

	/*
	 * Download WAL before basebackup for logical walsenders from sk, if
	 * needed
	 */
	bool		(*recovery_download) (WalProposer *wp, Safekeeper *sk);

	/* Allocate WAL reader. */
	void		(*wal_reader_allocate) (Safekeeper *sk);

	/* Read WAL from disk to buf. */
	NeonWALReadResult (*wal_read) (Safekeeper *sk, char *buf, XLogRecPtr startptr, Size count, char **errmsg);

	/* Returns events to be awaited on WAL reader, if any. */
	uint32		(*wal_reader_events) (Safekeeper *sk);

	/* Initialize event set. */
	void		(*init_event_set) (WalProposer *wp);

	/* Update events for an existing safekeeper connection. */
	void		(*update_event_set) (Safekeeper *sk, uint32 events);

	/* Configure wait event set for yield in SS_ACTIVE. */
	void		(*active_state_update_event_set) (Safekeeper *sk);

	/* Add a new safekeeper connection to the event set. */
	void		(*add_safekeeper_event_set) (Safekeeper *sk, uint32 events);

	/* Remove safekeeper connection from event set */
	void		(*rm_safekeeper_event_set) (Safekeeper *sk);

	/*
	 * Wait until some event happens: - timeout is reached - socket event for
	 * safekeeper connection - new WAL is available
	 *
	 * Returns 0 if timeout is reached, 1 if some event happened. Updates
	 * events mask to indicate events and sets sk to the safekeeper which has
	 * an event.
	 *
	 * On timeout, events is set to WL_NO_EVENTS. On socket event, events is
	 * set to WL_SOCKET_READABLE and/or WL_SOCKET_WRITEABLE. When socket is
	 * closed, events is set to WL_SOCKET_READABLE.
	 *
	 * WL_SOCKET_WRITEABLE is usually set only when we need to flush the
	 * buffer. It can be returned only if caller asked for this event in the
	 * last *_event_set call.
	 */
	int			(*wait_event_set) (WalProposer *wp, long timeout, Safekeeper **sk, uint32 *events);

	/* Read random bytes. */
	bool		(*strong_random) (WalProposer *wp, void *buf, size_t len);

	/*
	 * Get a basebackup LSN. Used to cross-validate with the latest available
	 * LSN on the safekeepers.
	 */
	XLogRecPtr	(*get_redo_start_lsn) (WalProposer *wp);

	/*
	 * Finish sync safekeepers with the given LSN. This function should not
	 * return and should exit the program.
	 */
	void		(*finish_sync_safekeepers) (WalProposer *wp, XLogRecPtr lsn);

	/*
	 * Called after every AppendResponse from the safekeeper. Used to
	 * propagate backpressure feedback and to confirm WAL persistence (has
	 * been commited on the quorum of safekeepers).
	 */
	void		(*process_safekeeper_feedback) (WalProposer *wp, Safekeeper *sk);

	/*
	 * Write a log message to the internal log processor. This is used only
	 * when walproposer is compiled as a library. Otherwise, all logging is
	 * handled by elog().
	 */
	void		(*log_internal) (WalProposer *wp, int level, const char *line);
} walproposer_api;

/*
 * Configuration of the WAL proposer.
 */
typedef struct WalProposerConfig
{
	/* hex-encoded TenantId cstr */
	char	   *neon_tenant;

	/* hex-encoded TimelineId cstr */
	char	   *neon_timeline;

	/*
	 * Comma-separated list of safekeepers, in the following format:
	 * host1:port1,host2:port2,host3:port3
	 *
	 * This cstr should be editable.
	 */
	char	   *safekeepers_list;

	/*
	 * WalProposer reconnects to offline safekeepers once in this interval.
	 * Time is in milliseconds.
	 */
	int			safekeeper_reconnect_timeout;

	/*
	 * WalProposer terminates the connection if it doesn't receive any message
	 * from the safekeeper in this interval. Time is in milliseconds.
	 */
	int			safekeeper_connection_timeout;

	/*
	 * WAL segment size. Will be passed to safekeepers in greet request. Also
	 * used to detect page headers.
	 */
	int			wal_segment_size;

	/*
	 * If safekeeper was started in sync mode, walproposer will not subscribe
	 * for new WAL and will exit when quorum of safekeepers will be synced to
	 * the latest available LSN.
	 */
	bool		syncSafekeepers;

	/* Will be passed to safekeepers in greet request. */
	uint64		systemId;

	/* Will be passed to safekeepers in greet request. */
	TimeLineID	pgTimeline;

	int			proto_version;

#ifdef WALPROPOSER_LIB
	void	   *callback_data;
#endif
} WalProposerConfig;


/*
 * WAL proposer state.
 */
typedef struct WalProposer
{
	WalProposerConfig *config;
	int			n_safekeepers;

	/* (n_safekeepers / 2) + 1 */
	int			quorum;

	Safekeeper	safekeeper[MAX_SAFEKEEPERS];

	/* WAL has been generated up to this point */
	XLogRecPtr	availableLsn;

	/* cached GetAcknowledgedByQuorumWALPosition result */
	XLogRecPtr	commitLsn;

	ProposerGreeting greetRequest;
	ProposerGreetingV2 greetRequestV2;

	/* Vote request for safekeeper */
	VoteRequest voteRequest;

	/*
	 * Minimal LSN which may be needed for recovery of some safekeeper,
	 * record-aligned (first record which might not yet received by someone).
	 */
	XLogRecPtr	truncateLsn;

	/*
	 * Term of the proposer. We want our term to be highest and unique, so we
	 * collect terms from safekeepers quorum, choose max and +1. After that
	 * our term is fixed and must not change. If we observe that some
	 * safekeeper has higher term, it means that we have another running
	 * compute, so we must stop immediately.
	 */
	term_t		propTerm;

	/* term history of the proposer */
	TermHistory propTermHistory;

	/* epoch start lsn of the proposer */
	XLogRecPtr	propEpochStartLsn;

	/* Most advanced acceptor epoch */
	term_t		donorEpoch;

	/* Most advanced acceptor */
	int			donor;

	/* timeline globally starts at this LSN */
	XLogRecPtr	timelineStartLsn;

	/* number of votes collected from safekeepers */
	int			n_votes;

	/* number of successful connections over the lifetime of walproposer */
	int			n_connected;

	/*
	 * Timestamp of the last reconnection attempt. Related to
	 * config->safekeeper_reconnect_timeout
	 */
	TimestampTz last_reconnect_attempt;

	walproposer_api api;
} WalProposer;

extern WalProposer *WalProposerCreate(WalProposerConfig *config, walproposer_api api);
extern void WalProposerStart(WalProposer *wp);
extern void WalProposerBroadcast(WalProposer *wp, XLogRecPtr startpos, XLogRecPtr endpos);
extern void WalProposerPoll(WalProposer *wp);
extern void WalProposerFree(WalProposer *wp);

extern WalproposerShmemState *GetWalpropShmemState(void);

/*
 * WaitEventSet API doesn't allow to remove socket, so walproposer_pg uses it to
 * recreate set from scratch, hence the export.
 */
extern void SafekeeperStateDesiredEvents(Safekeeper *sk, uint32 *sk_events, uint32 *nwr_events);
extern TimeLineID walprop_pg_get_timeline_id(void);


#define WPEVENT		1337		/* special log level for walproposer internal
								 * events */

#define WP_LOG_PREFIX "[WP] "

/*
 * wp_log is used in pure wp code (walproposer.c), allowing API callback to
 * catch logging.
 */
#ifdef WALPROPOSER_LIB
extern void WalProposerLibLog(WalProposer *wp, int elevel, char *fmt,...) pg_attribute_printf(3, 4);
#define wp_log(elevel, fmt, ...) WalProposerLibLog(wp, elevel, fmt, ## __VA_ARGS__)
#else
#define wp_log(elevel, fmt, ...) elog(elevel, WP_LOG_PREFIX fmt, ## __VA_ARGS__)
#endif

/*
 * And wpg_log is used all other (postgres specific) walproposer code, just
 * adding prefix.
 */
#define wpg_log(elevel, fmt, ...) elog(elevel, WP_LOG_PREFIX fmt, ## __VA_ARGS__)

#endif							/* __NEON_WALPROPOSER_H__ */
