#include "postgres.h"
#if PG_MAJORVERSION_NUM >= 16
#include "access/heapam_xlog.h"
#include "access/neon_xlog.h"
#include "access/rmgr.h"
#include "access/rmgrdesc_utils.h"
#include "access/xlog_internal.h"
#include "miscadmin.h"
#include "storage/buf.h"
#include "storage/bufpage.h"

#include "neon_rmgr.h"

/*
 * NOTE: "keyname" argument cannot have trailing spaces or punctuation
 * characters
 */
static void
infobits_desc(StringInfo buf, uint8 infobits, const char *keyname)
{
	appendStringInfo(buf, "%s: [", keyname);

	Assert(buf->data[buf->len - 1] != ' ');

	if (infobits & XLHL_XMAX_IS_MULTI)
		appendStringInfoString(buf, "IS_MULTI, ");
	if (infobits & XLHL_XMAX_LOCK_ONLY)
		appendStringInfoString(buf, "LOCK_ONLY, ");
	if (infobits & XLHL_XMAX_EXCL_LOCK)
		appendStringInfoString(buf, "EXCL_LOCK, ");
	if (infobits & XLHL_XMAX_KEYSHR_LOCK)
		appendStringInfoString(buf, "KEYSHR_LOCK, ");
	if (infobits & XLHL_KEYS_UPDATED)
		appendStringInfoString(buf, "KEYS_UPDATED, ");

	if (buf->data[buf->len - 1] == ' ')
	{
		/* Truncate-away final unneeded ", "  */
		Assert(buf->data[buf->len - 2] == ',');
		buf->len -= 2;
		buf->data[buf->len] = '\0';
	}

	appendStringInfoString(buf, "]");
}

void
neon_rm_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	info &= XLOG_NEON_OPMASK;

	if (info == XLOG_NEON_HEAP_INSERT)
	{
		xl_neon_heap_insert *xlrec = (xl_neon_heap_insert *) rec;

		appendStringInfo(buf, "off: %u, flags: 0x%02X",
						 xlrec->offnum,
						 xlrec->flags);
	}
	else if (info == XLOG_NEON_HEAP_DELETE)
	{
		xl_neon_heap_delete *xlrec = (xl_neon_heap_delete *) rec;

		appendStringInfo(buf, "xmax: %u, off: %u, ",
						 xlrec->xmax, xlrec->offnum);
		infobits_desc(buf, xlrec->infobits_set, "infobits");
		appendStringInfo(buf, ", flags: 0x%02X", xlrec->flags);
	}
	else if (info == XLOG_NEON_HEAP_UPDATE)
	{
		xl_neon_heap_update *xlrec = (xl_neon_heap_update *) rec;

		appendStringInfo(buf, "old_xmax: %u, old_off: %u, ",
						 xlrec->old_xmax, xlrec->old_offnum);
		infobits_desc(buf, xlrec->old_infobits_set, "old_infobits");
		appendStringInfo(buf, ", flags: 0x%02X, new_xmax: %u, new_off: %u",
						 xlrec->flags, xlrec->new_xmax, xlrec->new_offnum);
	}
	else if (info == XLOG_NEON_HEAP_HOT_UPDATE)
	{
		xl_neon_heap_update *xlrec = (xl_neon_heap_update *) rec;

		appendStringInfo(buf, "old_xmax: %u, old_off: %u, ",
						 xlrec->old_xmax, xlrec->old_offnum);
		infobits_desc(buf, xlrec->old_infobits_set, "old_infobits");
		appendStringInfo(buf, ", flags: 0x%02X, new_xmax: %u, new_off: %u",
						 xlrec->flags, xlrec->new_xmax, xlrec->new_offnum);
	}
	else if (info == XLOG_NEON_HEAP_LOCK)
	{
		xl_neon_heap_lock *xlrec = (xl_neon_heap_lock *) rec;

		appendStringInfo(buf, "xmax: %u, off: %u, ",
						 xlrec->xmax, xlrec->offnum);
		infobits_desc(buf, xlrec->infobits_set, "infobits");
		appendStringInfo(buf, ", flags: 0x%02X", xlrec->flags);
	}
	else if (info == XLOG_NEON_HEAP_MULTI_INSERT)
	{
		xl_neon_heap_multi_insert *xlrec = (xl_neon_heap_multi_insert *) rec;
		bool		isinit = (XLogRecGetInfo(record) & XLOG_NEON_INIT_PAGE) != 0;

		appendStringInfo(buf, "ntuples: %d, flags: 0x%02X", xlrec->ntuples,
						 xlrec->flags);

		if (XLogRecHasBlockData(record, 0) && !isinit)
		{
			appendStringInfoString(buf, ", offsets:");
			array_desc(buf, xlrec->offsets, sizeof(OffsetNumber),
					   xlrec->ntuples, &offset_elem_desc, NULL);
		}
	}
	else if (info == XLOG_NEON_LFC_PREWARM)
	{
		appendStringInfo(buf, "%d chunks", XLogRecGetDataLen(record));
	}
}

const char *
neon_rm_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & ~XLR_INFO_MASK)
	{
		case XLOG_NEON_HEAP_INSERT:
			id = "INSERT";
			break;
		case XLOG_NEON_HEAP_INSERT | XLOG_NEON_INIT_PAGE:
			id = "INSERT+INIT";
			break;
		case XLOG_NEON_HEAP_DELETE:
			id = "DELETE";
			break;
		case XLOG_NEON_HEAP_UPDATE:
			id = "UPDATE";
			break;
		case XLOG_NEON_HEAP_UPDATE | XLOG_NEON_INIT_PAGE:
			id = "UPDATE+INIT";
			break;
		case XLOG_NEON_HEAP_HOT_UPDATE:
			id = "HOT_UPDATE";
			break;
		case XLOG_NEON_HEAP_HOT_UPDATE | XLOG_HEAP_INIT_PAGE:
			id = "HOT_UPDATE+INIT";
			break;
		case XLOG_NEON_HEAP_LOCK:
			id = "LOCK";
			break;
		case XLOG_NEON_HEAP_MULTI_INSERT:
			id = "MULTI_INSERT";
			break;
		case XLOG_NEON_HEAP_MULTI_INSERT | XLOG_NEON_INIT_PAGE:
			id = "MULTI_INSERT+INIT";
			break;
		case XLOG_NEON_LFC_PREWARM:
			id = "LFC_PREWARM";
			break;
	}

	return id;
}

#endif
