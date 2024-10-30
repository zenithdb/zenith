#include "postgres.h"

#if PG_MAJORVERSION_NUM >= 16

#include "access/heapam_xlog.h"
#include "access/neon_xlog.h"
#include "replication/decode.h"
#include "replication/logical.h"
#include "replication/snapbuild.h"

#include "neon_rmgr.h"

#endif /* PG >= 16 */

#if PG_MAJORVERSION_NUM == 16

/* individual record(group)'s handlers */
static void DecodeNeonInsert(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);
static void DecodeNeonUpdate(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);
static void DecodeNeonDelete(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);
static void DecodeNeonMultiInsert(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);

/* common function to decode tuples */
static void DecodeXLogTuple(char *data, Size len, ReorderBufferTupleBuf *tuple);


void
neon_rm_decode(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
	uint8		info = XLogRecGetInfo(buf->record) & XLOG_NEON_OPMASK;
	TransactionId xid = XLogRecGetXid(buf->record);
	SnapBuild  *builder = ctx->snapshot_builder;

	ReorderBufferProcessXid(ctx->reorder, xid, buf->origptr);

	/*
	 * If we don't have snapshot or we are just fast-forwarding, there is no
	 * point in decoding data changes.
	 */
	if (SnapBuildCurrentState(builder) < SNAPBUILD_FULL_SNAPSHOT ||
		ctx->fast_forward)
		return;

	switch (info)
	{
		case XLOG_NEON_HEAP_INSERT:
			if (SnapBuildProcessChange(builder, xid, buf->origptr))
				DecodeNeonInsert(ctx, buf);
			break;
		case XLOG_NEON_HEAP_DELETE:
			if (SnapBuildProcessChange(builder, xid, buf->origptr))
				DecodeNeonDelete(ctx, buf);
			break;
		case XLOG_NEON_HEAP_UPDATE:
		case XLOG_NEON_HEAP_HOT_UPDATE:
			if (SnapBuildProcessChange(builder, xid, buf->origptr))
				DecodeNeonUpdate(ctx, buf);
			break;
		case XLOG_NEON_HEAP_LOCK:
			break;
		case XLOG_NEON_HEAP_MULTI_INSERT:
			if (SnapBuildProcessChange(builder, xid, buf->origptr))
				DecodeNeonMultiInsert(ctx, buf);
			break;
		default:
			elog(ERROR, "unexpected RM_HEAP_ID record type: %u", info);
			break;
	}
}

static inline bool
FilterByOrigin(LogicalDecodingContext *ctx, RepOriginId origin_id)
{
	if (ctx->callbacks.filter_by_origin_cb == NULL)
		return false;

	return filter_by_origin_cb_wrapper(ctx, origin_id);
}

/*
 * Parse XLOG_HEAP_INSERT (not MULTI_INSERT!) records into tuplebufs.
 *
 * Deletes can contain the new tuple.
 */
static void
DecodeNeonInsert(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
	Size		datalen;
	char	   *tupledata;
	Size		tuplelen;
	XLogReaderState *r = buf->record;
	xl_neon_heap_insert *xlrec;
	ReorderBufferChange *change;
	RelFileLocator target_locator;

	xlrec = (xl_neon_heap_insert *) XLogRecGetData(r);

	/*
	 * Ignore insert records without new tuples (this does happen when
	 * raw_heap_insert marks the TOAST record as HEAP_INSERT_NO_LOGICAL).
	 */
	if (!(xlrec->flags & XLH_INSERT_CONTAINS_NEW_TUPLE))
		return;

	/* only interested in our database */
	XLogRecGetBlockTag(r, 0, &target_locator, NULL, NULL);
	if (target_locator.dbOid != ctx->slot->data.database)
		return;

	/* output plugin doesn't look for this origin, no need to queue */
	if (FilterByOrigin(ctx, XLogRecGetOrigin(r)))
		return;

	change = ReorderBufferGetChange(ctx->reorder);
	if (!(xlrec->flags & XLH_INSERT_IS_SPECULATIVE))
		change->action = REORDER_BUFFER_CHANGE_INSERT;
	else
		change->action = REORDER_BUFFER_CHANGE_INTERNAL_SPEC_INSERT;
	change->origin_id = XLogRecGetOrigin(r);

	memcpy(&change->data.tp.rlocator, &target_locator, sizeof(RelFileLocator));

	tupledata = XLogRecGetBlockData(r, 0, &datalen);
	tuplelen = datalen - SizeOfNeonHeapHeader;

	change->data.tp.newtuple =
		ReorderBufferGetTupleBuf(ctx->reorder, tuplelen);

	DecodeXLogTuple(tupledata, datalen, change->data.tp.newtuple);

	change->data.tp.clear_toast_afterwards = true;

	ReorderBufferQueueChange(ctx->reorder, XLogRecGetXid(r), buf->origptr,
							 change,
							 xlrec->flags & XLH_INSERT_ON_TOAST_RELATION);
}

/*
 * Parse XLOG_HEAP_DELETE from wal into proper tuplebufs.
 *
 * Deletes can possibly contain the old primary key.
 */
static void
DecodeNeonDelete(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
	XLogReaderState *r = buf->record;
	xl_neon_heap_delete *xlrec;
	ReorderBufferChange *change;
	RelFileLocator target_locator;

	xlrec = (xl_neon_heap_delete *) XLogRecGetData(r);

	/* only interested in our database */
	XLogRecGetBlockTag(r, 0, &target_locator, NULL, NULL);
	if (target_locator.dbOid != ctx->slot->data.database)
		return;

	/* output plugin doesn't look for this origin, no need to queue */
	if (FilterByOrigin(ctx, XLogRecGetOrigin(r)))
		return;

	change = ReorderBufferGetChange(ctx->reorder);

	if (xlrec->flags & XLH_DELETE_IS_SUPER)
		change->action = REORDER_BUFFER_CHANGE_INTERNAL_SPEC_ABORT;
	else
		change->action = REORDER_BUFFER_CHANGE_DELETE;

	change->origin_id = XLogRecGetOrigin(r);

	memcpy(&change->data.tp.rlocator, &target_locator, sizeof(RelFileLocator));

	/* old primary key stored */
	if (xlrec->flags & XLH_DELETE_CONTAINS_OLD)
	{
		Size		datalen = XLogRecGetDataLen(r) - SizeOfNeonHeapHeader;
		Size		tuplelen = datalen - SizeOfNeonHeapHeader;

		Assert(XLogRecGetDataLen(r) > (SizeOfNeonHeapDelete + SizeOfNeonHeapHeader));

		change->data.tp.oldtuple =
			ReorderBufferGetTupleBuf(ctx->reorder, tuplelen);

		DecodeXLogTuple((char *) xlrec + SizeOfNeonHeapDelete,
						datalen, change->data.tp.oldtuple);
	}

	change->data.tp.clear_toast_afterwards = true;

	ReorderBufferQueueChange(ctx->reorder, XLogRecGetXid(r), buf->origptr,
							 change, false);
}

/*
 * Parse XLOG_HEAP_UPDATE and XLOG_HEAP_HOT_UPDATE, which have the same layout
 * in the record, from wal into proper tuplebufs.
 *
 * Updates can possibly contain a new tuple and the old primary key.
 */
static void
DecodeNeonUpdate(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
	XLogReaderState *r = buf->record;
	xl_neon_heap_update *xlrec;
	ReorderBufferChange *change;
	char	   *data;
	RelFileLocator target_locator;

	xlrec = (xl_neon_heap_update *) XLogRecGetData(r);

	/* only interested in our database */
	XLogRecGetBlockTag(r, 0, &target_locator, NULL, NULL);
	if (target_locator.dbOid != ctx->slot->data.database)
		return;

	/* output plugin doesn't look for this origin, no need to queue */
	if (FilterByOrigin(ctx, XLogRecGetOrigin(r)))
		return;

	change = ReorderBufferGetChange(ctx->reorder);
	change->action = REORDER_BUFFER_CHANGE_UPDATE;
	change->origin_id = XLogRecGetOrigin(r);
	memcpy(&change->data.tp.rlocator, &target_locator, sizeof(RelFileLocator));

	if (xlrec->flags & XLH_UPDATE_CONTAINS_NEW_TUPLE)
	{
		Size		datalen;
		Size		tuplelen;

		data = XLogRecGetBlockData(r, 0, &datalen);

		tuplelen = datalen - SizeOfNeonHeapHeader;

		change->data.tp.newtuple =
			ReorderBufferGetTupleBuf(ctx->reorder, tuplelen);

		DecodeXLogTuple(data, datalen, change->data.tp.newtuple);
	}

	if (xlrec->flags & XLH_UPDATE_CONTAINS_OLD)
	{
		Size		datalen;
		Size		tuplelen;

		/* caution, remaining data in record is not aligned */
		data = XLogRecGetData(r) + SizeOfNeonHeapUpdate;
		datalen = XLogRecGetDataLen(r) - SizeOfNeonHeapUpdate;
		tuplelen = datalen - SizeOfNeonHeapHeader;

		change->data.tp.oldtuple =
			ReorderBufferGetTupleBuf(ctx->reorder, tuplelen);

		DecodeXLogTuple(data, datalen, change->data.tp.oldtuple);
	}

	change->data.tp.clear_toast_afterwards = true;

	ReorderBufferQueueChange(ctx->reorder, XLogRecGetXid(r), buf->origptr,
							 change, false);
}

/*
 * Decode XLOG_HEAP2_MULTI_INSERT_insert record into multiple tuplebufs.
 *
 * Currently MULTI_INSERT will always contain the full tuples.
 */
static void
DecodeNeonMultiInsert(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
	XLogReaderState *r = buf->record;
	xl_neon_heap_multi_insert *xlrec;
	int			i;
	char	   *data;
	char	   *tupledata;
	Size		tuplelen;
	RelFileLocator rlocator;

	xlrec = (xl_neon_heap_multi_insert *) XLogRecGetData(r);

	/*
	 * Ignore insert records without new tuples.  This happens when a
	 * multi_insert is done on a catalog or on a non-persistent relation.
	 */
	if (!(xlrec->flags & XLH_INSERT_CONTAINS_NEW_TUPLE))
		return;

	/* only interested in our database */
	XLogRecGetBlockTag(r, 0, &rlocator, NULL, NULL);
	if (rlocator.dbOid != ctx->slot->data.database)
		return;

	/* output plugin doesn't look for this origin, no need to queue */
	if (FilterByOrigin(ctx, XLogRecGetOrigin(r)))
		return;

	/*
	 * We know that this multi_insert isn't for a catalog, so the block should
	 * always have data even if a full-page write of it is taken.
	 */
	tupledata = XLogRecGetBlockData(r, 0, &tuplelen);
	Assert(tupledata != NULL);

	data = tupledata;
	for (i = 0; i < xlrec->ntuples; i++)
	{
		ReorderBufferChange *change;
		xl_neon_multi_insert_tuple *xlhdr;
		int			datalen;
		ReorderBufferTupleBuf *tuple;
		HeapTupleHeader header;

		change = ReorderBufferGetChange(ctx->reorder);
		change->action = REORDER_BUFFER_CHANGE_INSERT;
		change->origin_id = XLogRecGetOrigin(r);

		memcpy(&change->data.tp.rlocator, &rlocator, sizeof(RelFileLocator));

		xlhdr = (xl_neon_multi_insert_tuple *) SHORTALIGN(data);
		data = ((char *) xlhdr) + SizeOfNeonMultiInsertTuple;
		datalen = xlhdr->datalen;

		change->data.tp.newtuple =
			ReorderBufferGetTupleBuf(ctx->reorder, datalen);

		tuple = change->data.tp.newtuple;
		header = tuple->tuple.t_data;

		/* not a disk based tuple */
		ItemPointerSetInvalid(&tuple->tuple.t_self);

		/*
		 * We can only figure this out after reassembling the transactions.
		 */
		tuple->tuple.t_tableOid = InvalidOid;

		tuple->tuple.t_len = datalen + SizeofHeapTupleHeader;

		memset(header, 0, SizeofHeapTupleHeader);

		memcpy((char *) tuple->tuple.t_data + SizeofHeapTupleHeader,
			   (char *) data,
			   datalen);
		header->t_infomask = xlhdr->t_infomask;
		header->t_infomask2 = xlhdr->t_infomask2;
		header->t_hoff = xlhdr->t_hoff;

		/*
		 * Reset toast reassembly state only after the last row in the last
		 * xl_multi_insert_tuple record emitted by one heap_multi_insert()
		 * call.
		 */
		if (xlrec->flags & XLH_INSERT_LAST_IN_MULTI &&
			(i + 1) == xlrec->ntuples)
			change->data.tp.clear_toast_afterwards = true;
		else
			change->data.tp.clear_toast_afterwards = false;

		ReorderBufferQueueChange(ctx->reorder, XLogRecGetXid(r),
								 buf->origptr, change, false);

		/* move to the next xl_neon_multi_insert_tuple entry */
		data += datalen;
	}
	Assert(data == tupledata + tuplelen);
}

/*
 * Read a HeapTuple as WAL logged by heap_insert, heap_update and heap_delete
 * (but not by heap_multi_insert) into a tuplebuf.
 *
 * The size 'len' and the pointer 'data' in the record need to be
 * computed outside as they are record specific.
 */
static void
DecodeXLogTuple(char *data, Size len, ReorderBufferTupleBuf *tuple)
{
	xl_neon_heap_header xlhdr;
	int			datalen = len - SizeOfNeonHeapHeader;
	HeapTupleHeader header;

	Assert(datalen >= 0);

	tuple->tuple.t_len = datalen + SizeofHeapTupleHeader;
	header = tuple->tuple.t_data;

	/* not a disk based tuple */
	ItemPointerSetInvalid(&tuple->tuple.t_self);

	/* we can only figure this out after reassembling the transactions */
	tuple->tuple.t_tableOid = InvalidOid;

	/* data is not stored aligned, copy to aligned storage */
	memcpy((char *) &xlhdr,
		   data,
		   SizeOfNeonHeapHeader);

	memset(header, 0, SizeofHeapTupleHeader);

	memcpy(((char *) tuple->tuple.t_data) + SizeofHeapTupleHeader,
		   data + SizeOfNeonHeapHeader,
		   datalen);

	header->t_infomask = xlhdr.t_infomask;
	header->t_infomask2 = xlhdr.t_infomask2;
	header->t_hoff = xlhdr.t_hoff;
}
#endif

#if PG_MAJORVERSION_NUM == 17

/* individual record(group)'s handlers */
static void DecodeNeonInsert(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);
static void DecodeNeonUpdate(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);
static void DecodeNeonDelete(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);
static void DecodeNeonMultiInsert(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);

/* common function to decode tuples */
static void DecodeXLogTuple(char *data, Size len, HeapTuple tuple);


void
neon_rm_decode(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
	uint8		info = XLogRecGetInfo(buf->record) & XLOG_NEON_OPMASK;
	TransactionId xid = XLogRecGetXid(buf->record);
	SnapBuild  *builder = ctx->snapshot_builder;

	ReorderBufferProcessXid(ctx->reorder, xid, buf->origptr);

	/*
	 * If we don't have snapshot or we are just fast-forwarding, there is no
	 * point in decoding data changes.
	 */
	if (SnapBuildCurrentState(builder) < SNAPBUILD_FULL_SNAPSHOT ||
		ctx->fast_forward)
		return;

	switch (info)
	{
		case XLOG_NEON_HEAP_INSERT:
			if (SnapBuildProcessChange(builder, xid, buf->origptr))
				DecodeNeonInsert(ctx, buf);
			break;
		case XLOG_NEON_HEAP_DELETE:
			if (SnapBuildProcessChange(builder, xid, buf->origptr))
				DecodeNeonDelete(ctx, buf);
			break;
		case XLOG_NEON_HEAP_UPDATE:
		case XLOG_NEON_HEAP_HOT_UPDATE:
			if (SnapBuildProcessChange(builder, xid, buf->origptr))
				DecodeNeonUpdate(ctx, buf);
			break;
		case XLOG_NEON_HEAP_LOCK:
			break;
		case XLOG_NEON_HEAP_MULTI_INSERT:
			if (SnapBuildProcessChange(builder, xid, buf->origptr))
				DecodeNeonMultiInsert(ctx, buf);
			break;
		case XLOG_NEON_LFC_PREWARM:
			break;
		default:
			elog(ERROR, "unexpected RM_HEAP_ID record type: %u", info);
			break;
	}
}

static inline bool
FilterByOrigin(LogicalDecodingContext *ctx, RepOriginId origin_id)
{
	if (ctx->callbacks.filter_by_origin_cb == NULL)
		return false;

	return filter_by_origin_cb_wrapper(ctx, origin_id);
}

/*
 * Parse XLOG_HEAP_INSERT (not MULTI_INSERT!) records into tuplebufs.
 *
 * Deletes can contain the new tuple.
 */
static void
DecodeNeonInsert(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
	Size		datalen;
	char	   *tupledata;
	Size		tuplelen;
	XLogReaderState *r = buf->record;
	xl_neon_heap_insert *xlrec;
	ReorderBufferChange *change;
	RelFileLocator target_locator;

	xlrec = (xl_neon_heap_insert *) XLogRecGetData(r);

	/*
	 * Ignore insert records without new tuples (this does happen when
	 * raw_heap_insert marks the TOAST record as HEAP_INSERT_NO_LOGICAL).
	 */
	if (!(xlrec->flags & XLH_INSERT_CONTAINS_NEW_TUPLE))
		return;

	/* only interested in our database */
	XLogRecGetBlockTag(r, 0, &target_locator, NULL, NULL);
	if (target_locator.dbOid != ctx->slot->data.database)
		return;

	/* output plugin doesn't look for this origin, no need to queue */
	if (FilterByOrigin(ctx, XLogRecGetOrigin(r)))
		return;

	change = ReorderBufferGetChange(ctx->reorder);
	if (!(xlrec->flags & XLH_INSERT_IS_SPECULATIVE))
		change->action = REORDER_BUFFER_CHANGE_INSERT;
	else
		change->action = REORDER_BUFFER_CHANGE_INTERNAL_SPEC_INSERT;
	change->origin_id = XLogRecGetOrigin(r);

	memcpy(&change->data.tp.rlocator, &target_locator, sizeof(RelFileLocator));

	tupledata = XLogRecGetBlockData(r, 0, &datalen);
	tuplelen = datalen - SizeOfHeapHeader;

	change->data.tp.newtuple =
		ReorderBufferGetTupleBuf(ctx->reorder, tuplelen);

	DecodeXLogTuple(tupledata, datalen, change->data.tp.newtuple);

	change->data.tp.clear_toast_afterwards = true;

	ReorderBufferQueueChange(ctx->reorder, XLogRecGetXid(r), buf->origptr,
							 change,
							 xlrec->flags & XLH_INSERT_ON_TOAST_RELATION);
}

/*
 * Parse XLOG_HEAP_DELETE from wal into proper tuplebufs.
 *
 * Deletes can possibly contain the old primary key.
 */
static void
DecodeNeonDelete(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
	XLogReaderState *r = buf->record;
	xl_neon_heap_delete *xlrec;
	ReorderBufferChange *change;
	RelFileLocator target_locator;

	xlrec = (xl_neon_heap_delete *) XLogRecGetData(r);

	/* only interested in our database */
	XLogRecGetBlockTag(r, 0, &target_locator, NULL, NULL);
	if (target_locator.dbOid != ctx->slot->data.database)
		return;

	/* output plugin doesn't look for this origin, no need to queue */
	if (FilterByOrigin(ctx, XLogRecGetOrigin(r)))
		return;

	change = ReorderBufferGetChange(ctx->reorder);

	if (xlrec->flags & XLH_DELETE_IS_SUPER)
		change->action = REORDER_BUFFER_CHANGE_INTERNAL_SPEC_ABORT;
	else
		change->action = REORDER_BUFFER_CHANGE_DELETE;

	change->origin_id = XLogRecGetOrigin(r);

	memcpy(&change->data.tp.rlocator, &target_locator, sizeof(RelFileLocator));

	/* old primary key stored */
	if (xlrec->flags & XLH_DELETE_CONTAINS_OLD)
	{
		Size		datalen = XLogRecGetDataLen(r) - SizeOfNeonHeapHeader;
		Size		tuplelen = datalen - SizeOfNeonHeapHeader;

		Assert(XLogRecGetDataLen(r) > (SizeOfNeonHeapDelete + SizeOfNeonHeapHeader));

		change->data.tp.oldtuple =
			ReorderBufferGetTupleBuf(ctx->reorder, tuplelen);

		DecodeXLogTuple((char *) xlrec + SizeOfNeonHeapDelete,
						datalen, change->data.tp.oldtuple);
	}

	change->data.tp.clear_toast_afterwards = true;

	ReorderBufferQueueChange(ctx->reorder, XLogRecGetXid(r), buf->origptr,
							 change, false);
}

/*
 * Parse XLOG_HEAP_UPDATE and XLOG_HEAP_HOT_UPDATE, which have the same layout
 * in the record, from wal into proper tuplebufs.
 *
 * Updates can possibly contain a new tuple and the old primary key.
 */
static void
DecodeNeonUpdate(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
	XLogReaderState *r = buf->record;
	xl_neon_heap_update *xlrec;
	ReorderBufferChange *change;
	char	   *data;
	RelFileLocator target_locator;

	xlrec = (xl_neon_heap_update *) XLogRecGetData(r);

	/* only interested in our database */
	XLogRecGetBlockTag(r, 0, &target_locator, NULL, NULL);
	if (target_locator.dbOid != ctx->slot->data.database)
		return;

	/* output plugin doesn't look for this origin, no need to queue */
	if (FilterByOrigin(ctx, XLogRecGetOrigin(r)))
		return;

	change = ReorderBufferGetChange(ctx->reorder);
	change->action = REORDER_BUFFER_CHANGE_UPDATE;
	change->origin_id = XLogRecGetOrigin(r);
	memcpy(&change->data.tp.rlocator, &target_locator, sizeof(RelFileLocator));

	if (xlrec->flags & XLH_UPDATE_CONTAINS_NEW_TUPLE)
	{
		Size		datalen;
		Size		tuplelen;

		data = XLogRecGetBlockData(r, 0, &datalen);

		tuplelen = datalen - SizeOfNeonHeapHeader;

		change->data.tp.newtuple =
			ReorderBufferGetTupleBuf(ctx->reorder, tuplelen);

		DecodeXLogTuple(data, datalen, change->data.tp.newtuple);
	}

	if (xlrec->flags & XLH_UPDATE_CONTAINS_OLD)
	{
		Size		datalen;
		Size		tuplelen;

		/* caution, remaining data in record is not aligned */
		data = XLogRecGetData(r) + SizeOfNeonHeapUpdate;
		datalen = XLogRecGetDataLen(r) - SizeOfNeonHeapUpdate;
		tuplelen = datalen - SizeOfNeonHeapHeader;

		change->data.tp.oldtuple =
			ReorderBufferGetTupleBuf(ctx->reorder, tuplelen);

		DecodeXLogTuple(data, datalen, change->data.tp.oldtuple);
	}

	change->data.tp.clear_toast_afterwards = true;

	ReorderBufferQueueChange(ctx->reorder, XLogRecGetXid(r), buf->origptr,
							 change, false);
}

/*
 * Decode XLOG_HEAP2_MULTI_INSERT_insert record into multiple tuplebufs.
 *
 * Currently MULTI_INSERT will always contain the full tuples.
 */
static void
DecodeNeonMultiInsert(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
	XLogReaderState *r = buf->record;
	xl_neon_heap_multi_insert *xlrec;
	int			i;
	char	   *data;
	char	   *tupledata;
	Size		tuplelen;
	RelFileLocator rlocator;

	xlrec = (xl_neon_heap_multi_insert *) XLogRecGetData(r);

	/*
	 * Ignore insert records without new tuples.  This happens when a
	 * multi_insert is done on a catalog or on a non-persistent relation.
	 */
	if (!(xlrec->flags & XLH_INSERT_CONTAINS_NEW_TUPLE))
		return;

	/* only interested in our database */
	XLogRecGetBlockTag(r, 0, &rlocator, NULL, NULL);
	if (rlocator.dbOid != ctx->slot->data.database)
		return;

	/* output plugin doesn't look for this origin, no need to queue */
	if (FilterByOrigin(ctx, XLogRecGetOrigin(r)))
		return;

	/*
	 * We know that this multi_insert isn't for a catalog, so the block should
	 * always have data even if a full-page write of it is taken.
	 */
	tupledata = XLogRecGetBlockData(r, 0, &tuplelen);
	Assert(tupledata != NULL);

	data = tupledata;
	for (i = 0; i < xlrec->ntuples; i++)
	{
		ReorderBufferChange *change;
		xl_neon_multi_insert_tuple *xlhdr;
		int			datalen;
		HeapTuple	tuple;
		HeapTupleHeader header;

		change = ReorderBufferGetChange(ctx->reorder);
		change->action = REORDER_BUFFER_CHANGE_INSERT;
		change->origin_id = XLogRecGetOrigin(r);

		memcpy(&change->data.tp.rlocator, &rlocator, sizeof(RelFileLocator));

		xlhdr = (xl_neon_multi_insert_tuple *) SHORTALIGN(data);
		data = ((char *) xlhdr) + SizeOfNeonMultiInsertTuple;
		datalen = xlhdr->datalen;

		change->data.tp.newtuple =
			ReorderBufferGetTupleBuf(ctx->reorder, datalen);

		tuple = change->data.tp.newtuple;
		header = tuple->t_data;

		/* not a disk based tuple */
		ItemPointerSetInvalid(&tuple->t_self);

		/*
		 * We can only figure this out after reassembling the transactions.
		 */
		tuple->t_tableOid = InvalidOid;

		tuple->t_len = datalen + SizeofHeapTupleHeader;

		memset(header, 0, SizeofHeapTupleHeader);

		memcpy((char *) tuple->t_data + SizeofHeapTupleHeader,
			   (char *) data,
			   datalen);
		header->t_infomask = xlhdr->t_infomask;
		header->t_infomask2 = xlhdr->t_infomask2;
		header->t_hoff = xlhdr->t_hoff;

		/*
		 * Reset toast reassembly state only after the last row in the last
		 * xl_multi_insert_tuple record emitted by one heap_multi_insert()
		 * call.
		 */
		if (xlrec->flags & XLH_INSERT_LAST_IN_MULTI &&
			(i + 1) == xlrec->ntuples)
			change->data.tp.clear_toast_afterwards = true;
		else
			change->data.tp.clear_toast_afterwards = false;

		ReorderBufferQueueChange(ctx->reorder, XLogRecGetXid(r),
								 buf->origptr, change, false);

		/* move to the next xl_neon_multi_insert_tuple entry */
		data += datalen;
	}
	Assert(data == tupledata + tuplelen);
}

/*
 * Read a HeapTuple as WAL logged by heap_insert, heap_update and heap_delete
 * (but not by heap_multi_insert) into a tuplebuf.
 *
 * The size 'len' and the pointer 'data' in the record need to be
 * computed outside as they are record specific.
 */
static void
DecodeXLogTuple(char *data, Size len, HeapTuple tuple)
{
	xl_neon_heap_header xlhdr;
	int			datalen = len - SizeOfNeonHeapHeader;
	HeapTupleHeader header;

	Assert(datalen >= 0);

	tuple->t_len = datalen + SizeofHeapTupleHeader;
	header = tuple->t_data;

	/* not a disk based tuple */
	ItemPointerSetInvalid(&tuple->t_self);

	/* we can only figure this out after reassembling the transactions */
	tuple->t_tableOid = InvalidOid;

	/* data is not stored aligned, copy to aligned storage */
	memcpy((char *) &xlhdr,
		   data,
		   SizeOfNeonHeapHeader);

	memset(header, 0, SizeofHeapTupleHeader);

	memcpy(((char *) tuple->t_data) + SizeofHeapTupleHeader,
		   data + SizeOfNeonHeapHeader,
		   datalen);

	header->t_infomask = xlhdr.t_infomask;
	header->t_infomask2 = xlhdr.t_infomask2;
	header->t_hoff = xlhdr.t_hoff;
}
#endif
