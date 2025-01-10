/*-------------------------------------------------------------------------
 *
 * pagestore_client.h
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
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

typedef uint64 NeonRequestId;

/* base struct for c-style inheritance */
typedef struct
{
	NeonMessageTag tag;
	NeonRequestId reqid;
	XLogRecPtr	lsn;
	XLogRecPtr	not_modified_since;
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


/*--
 * supertype of all the Neon*Request structs below.
 *
 * All requests contain two LSNs:
 *
 * lsn:                request page (or relation size, etc) at this LSN
 * not_modified_since: Hint that the page hasn't been modified between
 *                     this LSN and the request LSN (`lsn`).
 *
 * To request the latest version of a page, you can use MAX_LSN as the request
 * LSN.
 *
 * If you don't know any better, you can always set 'not_modified_since' equal
 * to 'lsn', but providing a lower value can speed up processing the request
 * in the pageserver, as it doesn't need to wait for the WAL to arrive, and it
 * can skip traversing through recent layers which we know to not contain any
 * versions for the requested page.
 *
 * These structs describe the V2 of these requests. (The old now-defunct V1
 * protocol contained just one LSN and a boolean 'latest' flag.)
 *
 * V3 version of protocol adds request ID to all requests. This request ID is also included in response
 * as well as other fields from requests, which allows to verify that we receive response for our request.
 * We copy fields from request to response to make checking more reliable: request ID is formed from process ID
 * and local counter, so in principle there can be duplicated requests IDs if process PID is reused.
 */
typedef NeonMessage NeonRequest;

typedef struct
{
	NeonRequest hdr;
	NRelFileInfo rinfo;
	ForkNumber	forknum;
} NeonExistsRequest;

typedef struct
{
	NeonRequest hdr;
	NRelFileInfo rinfo;
	ForkNumber	forknum;
} NeonNblocksRequest;

typedef struct
{
	NeonRequest hdr;
	Oid			dbNode;
} NeonDbSizeRequest;

typedef struct
{
	NeonRequest hdr;
	NRelFileInfo rinfo;
	ForkNumber	forknum;
	BlockNumber blkno;
} NeonGetPageRequest;

typedef struct
{
	NeonRequest hdr;
	SlruKind	kind;
	int			segno;
} NeonGetSlruSegmentRequest;

/* supertype of all the Neon*Response structs below */
typedef NeonMessage NeonResponse;

typedef struct
{
	NeonExistsRequest req;
	bool		exists;
} NeonExistsResponse;

typedef struct
{
	NeonNblocksRequest req;
	uint32		n_blocks;
} NeonNblocksResponse;

typedef struct
{
	NeonGetPageRequest req;
	char		page[FLEXIBLE_ARRAY_MEMBER];
} NeonGetPageResponse;

#define PS_GETPAGERESPONSE_SIZE (MAXALIGN(offsetof(NeonGetPageResponse, page) + BLCKSZ))

typedef struct
{
	NeonDbSizeRequest req;
	int64		db_size;
} NeonDbSizeResponse;

typedef struct
{
	NeonResponse req;
	char		message[FLEXIBLE_ARRAY_MEMBER]; /* null-terminated error
												 * message */
} NeonErrorResponse;

typedef struct
{
	NeonGetSlruSegmentRequest req;
	int			n_blocks;
	char		data[BLCKSZ * SLRU_PAGES_PER_SEGMENT];
} NeonGetSlruSegmentResponse;


extern StringInfoData nm_pack_request(NeonRequest *msg);
extern NeonResponse *nm_unpack_response(StringInfo s);
extern char *nm_to_string(NeonMessage *msg);

/*
 * API
 */

typedef uint16 shardno_t;

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
extern int  neon_protocol_version;

extern shardno_t get_shard_number(BufferTag* tag);

extern const f_smgr *smgr_neon(ProcNumber backend, NRelFileInfo rinfo);
extern void smgr_init_neon(void);
extern void readahead_buffer_resize(int newsize, void *extra);

/*
 * LSN values associated with each request to the pageserver
 */
typedef struct
{
	/*
	 * 'request_lsn' is the main value that determines which page version to
	 * fetch.
	 */
	XLogRecPtr request_lsn;

	/*
	 * A hint to the pageserver that the requested page hasn't been modified
	 * between this LSN and 'request_lsn'. That allows the pageserver to
	 * return the page faster, without waiting for 'request_lsn' to arrive in
	 * the pageserver, as long as 'not_modified_since' has arrived.
	 */
	XLogRecPtr not_modified_since;

	/*
	 * 'effective_request_lsn' is not included in the request that's sent to
	 * the pageserver, but is used to keep track of the latest LSN of when the
	 * request was made. In a standby server, this is always the same as the
	 * 'request_lsn', but in the primary we use UINT64_MAX as the
	 * 'request_lsn' to request the latest page version, so we need this
	 * separate field to remember that latest LSN was when the request was
	 * made. It's needed to manage prefetch request, to verify if the response
	 * to a prefetched request is still valid.
	 */
	XLogRecPtr effective_request_lsn;
} neon_request_lsns;

#if PG_MAJORVERSION_NUM < 16
extern PGDLLEXPORT void neon_read_at_lsn(NRelFileInfo rnode, ForkNumber forkNum, BlockNumber blkno,
										 neon_request_lsns request_lsns, char *buffer);
#else
extern PGDLLEXPORT void neon_read_at_lsn(NRelFileInfo rnode, ForkNumber forkNum, BlockNumber blkno,
										 neon_request_lsns request_lsns, void *buffer);
#endif
extern int64 neon_dbsize(Oid dbNode);

/* utils for neon relsize cache */
extern void relsize_hash_init(void);
extern bool get_cached_relsize(NRelFileInfo rinfo, ForkNumber forknum, BlockNumber *size);
extern void set_cached_relsize(NRelFileInfo rinfo, ForkNumber forknum, BlockNumber size);
extern void update_cached_relsize(NRelFileInfo rinfo, ForkNumber forknum, BlockNumber size);
extern void forget_cached_relsize(NRelFileInfo rinfo, ForkNumber forknum);

/* functions for local file cache */
extern void lfc_writev(NRelFileInfo rinfo, ForkNumber forkNum,
					   BlockNumber blkno, const void *const *buffers,
					   BlockNumber nblocks);
/* returns number of blocks read, with one bit set in *read for each  */
extern int lfc_readv_select(NRelFileInfo rinfo, ForkNumber forkNum,
							BlockNumber blkno, void **buffers,
							BlockNumber nblocks, bits8 *mask);

extern bool lfc_cache_contains(NRelFileInfo rinfo, ForkNumber forkNum,
							   BlockNumber blkno);
extern int lfc_cache_containsv(NRelFileInfo rinfo, ForkNumber forkNum,
							   BlockNumber blkno, int nblocks, bits8 *bitmap);
extern void lfc_evict(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber blkno);
extern void lfc_init(void);

static inline bool
lfc_read(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber blkno,
		 void *buffer)
{
	bits8		rv = 0;
	return lfc_readv_select(rinfo, forkNum, blkno, &buffer, 1, &rv) == 1;
}

static inline void
lfc_write(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber blkno,
		  const void *buffer)
{
	return lfc_writev(rinfo, forkNum, blkno, &buffer, 1);
}

#endif
