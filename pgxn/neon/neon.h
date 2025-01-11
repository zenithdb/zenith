/*-------------------------------------------------------------------------
 *
 * neon.h
 *	  Functions used in the initialization of this extension.
 *
 * IDENTIFICATION
 *	 contrib/neon/neon.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef NEON_H
#define NEON_H
#include "access/xlogreader.h"
#include "utils/wait_event.h"

/* GUCs */
extern char *neon_auth_token;
extern char *neon_timeline;
extern char *neon_tenant;

extern char *wal_acceptors_list;
extern int	wal_acceptor_reconnect_timeout;
extern int	wal_acceptor_connection_timeout;

#if PG_MAJORVERSION_NUM >= 17
extern uint32		WAIT_EVENT_NEON_LFC_MAINTENANCE;
extern uint32		WAIT_EVENT_NEON_LFC_READ;
extern uint32		WAIT_EVENT_NEON_LFC_TRUNCATE;
extern uint32		WAIT_EVENT_NEON_LFC_WRITE;
extern uint32		WAIT_EVENT_NEON_LFC_PREWARM_IO;
extern uint32		WAIT_EVENT_NEON_LFC_PREWARM_ACCESS;
extern uint32		WAIT_EVENT_NEON_PS_STARTING;
extern uint32		WAIT_EVENT_NEON_PS_CONFIGURING;
extern uint32		WAIT_EVENT_NEON_PS_SEND;
extern uint32		WAIT_EVENT_NEON_PS_READ;
extern uint32		WAIT_EVENT_NEON_WAL_DL;
#else
#define WAIT_EVENT_NEON_LFC_MAINTENANCE	PG_WAIT_EXTENSION
#define WAIT_EVENT_NEON_LFC_READ		WAIT_EVENT_BUFFILE_READ
#define WAIT_EVENT_NEON_LFC_TRUNCATE	WAIT_EVENT_BUFFILE_TRUNCATE
#define WAIT_EVENT_NEON_LFC_WRITE		WAIT_EVENT_BUFFILE_WRITE
#define WAIT_EVENT_NEON_LFC_PREWARM_IO	WAIT_EVENT_BUFFER_IO
#define WAIT_EVENT_NEON_LFC_PREWARM_ACCESS	WAIT_EVENT_BUFFER_IO
#define WAIT_EVENT_NEON_PS_STARTING		PG_WAIT_EXTENSION
#define WAIT_EVENT_NEON_PS_CONFIGURING	PG_WAIT_EXTENSION
#define WAIT_EVENT_NEON_PS_SEND			PG_WAIT_EXTENSION
#define WAIT_EVENT_NEON_PS_READ			PG_WAIT_EXTENSION
#define WAIT_EVENT_NEON_WAL_DL			WAIT_EVENT_WAL_READ
#endif

extern void pg_init_libpagestore(void);
extern void pg_init_walproposer(void);

extern uint64 BackpressureThrottlingTime(void);
extern void SetNeonCurrentClusterSize(uint64 size);
extern uint64 GetNeonCurrentClusterSize(void);
extern void replication_feedback_get_lsns(XLogRecPtr *writeLsn, XLogRecPtr *flushLsn, XLogRecPtr *applyLsn);

extern void PGDLLEXPORT WalProposerSync(int argc, char *argv[]);
extern void PGDLLEXPORT WalProposerMain(Datum main_arg);
PGDLLEXPORT void LogicalSlotsMonitorMain(Datum main_arg);

#endif							/* NEON_H */
