/*-------------------------------------------------------------------------
 *
 * pagestore_client.h
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/neon/pagestore_client.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef pageserver_h
#define pageserver_h

#include "neon_pgversioncompat.h"

#include "access/slru.h"
#include "access/xlogdefs.h"
#include RELFILEINFO_HDR
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "storage/block.h"
#include "storage/buf_internals.h"
#include "storage/smgr.h"
#include "utils/memutils.h"

#define MAX_SHARDS 128
#define MAX_PAGESERVER_CONNSTRING_SIZE 256

typedef enum
{
	/* pagestore_client -> pagestore */
	T_NeonExistsRequest = 0,
	T_NeonNblocksRequest,
	T_NeonGetPageRequest,
	T_NeonDbSizeRequest,
	T_NeonGetSlruSegmentRequest,

	/* pagestore -> pagestore_client */
	T_NeonExistsResponse = 100,
	T_NeonNblocksResponse,
	T_NeonGetPageResponse,
	T_NeonErrorResponse,
	T_NeonDbSizeResponse,
	T_NeonGetSlruSegmentResponse,
} NeonMessageTag;

/* base struct for c-style inheritance */
typedef struct
{
	NeonMessageTag tag;
} NeonMessage;

#define messageTag(m) (((const NeonMessage *)(m))->tag)

#define NEON_TAG "[NEON_SMGR] "
#define neon_log(tag, fmt, ...) ereport(tag,                                  \
										(errmsg(NEON_TAG fmt, ##__VA_ARGS__), \
										 errhidestmt(true), errhidecontext(true), errposition(0), internalerrposition(0)))
#define neon_shard_log(shard_no, tag, fmt, ...) ereport(tag,	\
														(errmsg(NEON_TAG "[shard %d] " fmt, shard_no, ##__VA_ARGS__), \
														 errhidestmt(true), errhidecontext(true), errposition(0), internalerrposition(0)))

/* SLRUs downloadable from page server */
typedef enum {
	SLRU_CLOG,
	SLRU_MULTIXACT_MEMBERS,
	SLRU_MULTIXACT_OFFSETS
} SlruKind;

/*
 * supertype of all the Neon*Request structs below
 *
 * If 'latest' is true, we are requesting the latest page version, and 'lsn'
 * is just a hint to the server that we know there are no versions of the page
 * (or relation size, for exists/nblocks requests) later than the 'lsn'.
 */
typedef struct
{
	NeonMessageTag tag;
	bool		latest;			/* if true, request latest page version */
	XLogRecPtr	lsn;			/* request page version @ this LSN */
} NeonRequest;

typedef struct
{
	NeonRequest req;
	NRelFileInfo rinfo;
	ForkNumber	forknum;
} NeonExistsRequest;

typedef struct
{
	NeonRequest req;
	NRelFileInfo rinfo;
	ForkNumber	forknum;
} NeonNblocksRequest;

typedef struct
{
	NeonRequest req;
	Oid			dbNode;
} NeonDbSizeRequest;

typedef struct
{
	NeonRequest req;
	NRelFileInfo rinfo;
	ForkNumber	forknum;
	BlockNumber blkno;
} NeonGetPageRequest;

typedef struct
{
	NeonRequest req;
	SlruKind kind;
	int      segno;
} NeonGetSlruSegmentRequest;

/* supertype of all the Neon*Response structs below */
typedef struct
{
	NeonMessageTag tag;
} NeonResponse;

typedef struct
{
	NeonMessageTag tag;
	bool		exists;
} NeonExistsResponse;

typedef struct
{
	NeonMessageTag tag;
	uint32		n_blocks;
} NeonNblocksResponse;

typedef struct
{
	NeonMessageTag tag;
	NRelFileInfo rinfo;
	ForkNumber	forknum;
	BlockNumber blkno;
	char		page[FLEXIBLE_ARRAY_MEMBER];
} NeonGetPageResponse;

#define PS_GETPAGERESPONSE_SIZE (MAXALIGN(offsetof(NeonGetPageResponse, page) + BLCKSZ))

typedef struct
{
	NeonMessageTag tag;
	int64		db_size;
} NeonDbSizeResponse;

typedef struct
{
	NeonMessageTag tag;
	char		message[FLEXIBLE_ARRAY_MEMBER]; /* null-terminated error
												 * message */
} NeonErrorResponse;

typedef struct
{
	NeonMessageTag tag;
	int         n_blocks;
	char		data[BLCKSZ * SLRU_PAGES_PER_SEGMENT];
} NeonGetSlruSegmentResponse;


extern StringInfoData nm_pack_request(NeonRequest *msg);
extern NeonResponse *nm_unpack_response(StringInfo s);
extern char *nm_to_string(NeonMessage *msg);

/*
 * API
 */

typedef unsigned shardno_t;

typedef struct
{
	bool		(*send) (shardno_t  shard_no, NeonRequest * request);
	NeonResponse *(*receive) (shardno_t shard_no);
	bool		(*flush) (shardno_t shard_no);
	void        (*disconnect) (shardno_t shard_no);
} page_server_api;

extern void prefetch_on_ps_disconnect(void);

extern page_server_api *page_server;

extern char *page_server_connstring;
extern int	flush_every_n_requests;
extern int	readahead_buffer_size;
extern char *neon_timeline;
extern char *neon_tenant;
extern int32 max_cluster_size;

extern shardno_t get_shard_number(BufferTag* tag);

extern const f_smgr *smgr_neon(BackendId backend, NRelFileInfo rinfo);
extern void smgr_init_neon(void);
extern void readahead_buffer_resize(int newsize, void *extra);

/* Neon storage manager functionality */

extern void neon_init(void);
extern void neon_open(SMgrRelation reln);
extern void neon_close(SMgrRelation reln, ForkNumber forknum);
extern void neon_create(SMgrRelation reln, ForkNumber forknum, bool isRedo);
extern bool neon_exists(SMgrRelation reln, ForkNumber forknum);
extern void neon_unlink(NRelFileInfoBackend rnode, ForkNumber forknum, bool isRedo);
#if PG_MAJORVERSION_NUM < 16
extern void neon_extend(SMgrRelation reln, ForkNumber forknum,
						BlockNumber blocknum, char *buffer, bool skipFsync);
#else
extern void neon_extend(SMgrRelation reln, ForkNumber forknum,
						BlockNumber blocknum, const void *buffer, bool skipFsync);
extern void neon_zeroextend(SMgrRelation reln, ForkNumber forknum,
							BlockNumber blocknum, int nbuffers, bool skipFsync);
#endif

extern bool neon_prefetch(SMgrRelation reln, ForkNumber forknum,
						  BlockNumber blocknum);

#if PG_MAJORVERSION_NUM < 16
extern void neon_read(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
					  char *buffer);
extern PGDLLEXPORT void neon_read_at_lsn(NRelFileInfo rnode, ForkNumber forkNum, BlockNumber blkno,
										 XLogRecPtr request_lsn, bool request_latest, char *buffer);
extern void neon_write(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber blocknum, char *buffer, bool skipFsync);
#else
extern void neon_read(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
					  void *buffer);
extern PGDLLEXPORT void neon_read_at_lsn(NRelFileInfo rnode, ForkNumber forkNum, BlockNumber blkno,
										 XLogRecPtr request_lsn, bool request_latest, void *buffer);
extern void neon_write(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber blocknum, const void *buffer, bool skipFsync);
#endif
extern void neon_writeback(SMgrRelation reln, ForkNumber forknum,
						   BlockNumber blocknum, BlockNumber nblocks);
extern BlockNumber neon_nblocks(SMgrRelation reln, ForkNumber forknum);
extern int64 neon_dbsize(Oid dbNode);
extern void neon_truncate(SMgrRelation reln, ForkNumber forknum,
						  BlockNumber nblocks);
extern void neon_immedsync(SMgrRelation reln, ForkNumber forknum);

/* utils for neon relsize cache */
extern void relsize_hash_init(void);
extern bool get_cached_relsize(NRelFileInfo rinfo, ForkNumber forknum, BlockNumber *size);
extern void set_cached_relsize(NRelFileInfo rinfo, ForkNumber forknum, BlockNumber size);
extern void update_cached_relsize(NRelFileInfo rinfo, ForkNumber forknum, BlockNumber size);
extern void forget_cached_relsize(NRelFileInfo rinfo, ForkNumber forknum);

/* functions for local file cache */
#if PG_MAJORVERSION_NUM < 16
extern void lfc_write(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber blkno,
					  char *buffer);
#else
extern void lfc_write(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber blkno,
					  const void *buffer);
#endif
extern bool lfc_read(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber blkno, char *buffer);
extern bool lfc_cache_contains(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber blkno);
extern void lfc_evict(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber blkno);
extern void lfc_init(void);


#endif
