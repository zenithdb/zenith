/*
 * Contains copied/adapted functions from libpq and some internal postgres functions.
 * This is needed to avoid linking to full postgres server installation. This file
 * is compiled as a part of libwalproposer static library.
 */
#include "postgres.h"

#include <stdio.h>

#include "miscadmin.h"
#include "utils/datetime.h"
#include "walproposer.h"

void
ExceptionalCondition(const char *conditionName,
					 const char *fileName, int lineNumber)
{
	fprintf(stderr, "ExceptionalCondition: %s:%d: %s\n",
			fileName, lineNumber, conditionName);
	fprintf(stderr, "aborting...\n");
	exit(1);
}

void
pq_copymsgbytes(StringInfo msg, char *buf, int datalen)
{
	if (datalen < 0 || datalen > (msg->len - msg->cursor))
		ExceptionalCondition("insufficient data left in message", __FILE__, __LINE__);
	memcpy(buf, &msg->data[msg->cursor], datalen);
	msg->cursor += datalen;
}

/* --------------------------------
 *		pq_getmsgint	- get a binary integer from a message buffer
 *
 *		Values are treated as unsigned.
 * --------------------------------
 */
unsigned int
pq_getmsgint(StringInfo msg, int b)
{
	unsigned int result;
	unsigned char n8;
	uint16		n16;
	uint32		n32;

	switch (b)
	{
		case 1:
			pq_copymsgbytes(msg, (char *) &n8, 1);
			result = n8;
			break;
		case 2:
			pq_copymsgbytes(msg, (char *) &n16, 2);
			result = pg_ntoh16(n16);
			break;
		case 4:
			pq_copymsgbytes(msg, (char *) &n32, 4);
			result = pg_ntoh32(n32);
			break;
		default:
			fprintf(stderr, "unsupported integer size %d\n", b);
			ExceptionalCondition("unsupported integer size", __FILE__, __LINE__);
			result = 0;			/* keep compiler quiet */
			break;
	}
	return result;
}

/* --------------------------------
 *		pq_getmsgint64	- get a binary 8-byte int from a message buffer
 *
 * It is tempting to merge this with pq_getmsgint, but we'd have to make the
 * result int64 for all data widths --- that could be a big performance
 * hit on machines where int64 isn't efficient.
 * --------------------------------
 */
int64
pq_getmsgint64(StringInfo msg)
{
	uint64		n64;

	pq_copymsgbytes(msg, (char *) &n64, sizeof(n64));

	return pg_ntoh64(n64);
}

/* --------------------------------
 *		pq_getmsgbyte	- get a raw byte from a message buffer
 * --------------------------------
 */
int
pq_getmsgbyte(StringInfo msg)
{
	if (msg->cursor >= msg->len)
		ExceptionalCondition("no data left in message", __FILE__, __LINE__);
	return (unsigned char) msg->data[msg->cursor++];
}

/* --------------------------------
 *		pq_getmsgbytes	- get raw data from a message buffer
 *
 *		Returns a pointer directly into the message buffer; note this
 *		may not have any particular alignment.
 * --------------------------------
 */
const char *
pq_getmsgbytes(StringInfo msg, int datalen)
{
	const char *result;

	if (datalen < 0 || datalen > (msg->len - msg->cursor))
		ExceptionalCondition("insufficient data left in message", __FILE__, __LINE__);
	result = &msg->data[msg->cursor];
	msg->cursor += datalen;
	return result;
}

/* --------------------------------
 *		pq_getmsgstring - get a null-terminated text string (with conversion)
 *
 *		May return a pointer directly into the message buffer, or a pointer
 *		to a palloc'd conversion result.
 * --------------------------------
 */
const char *
pq_getmsgstring(StringInfo msg)
{
	char	   *str;
	int			slen;

	str = &msg->data[msg->cursor];

	/*
	 * It's safe to use strlen() here because a StringInfo is guaranteed to
	 * have a trailing null byte.  But check we found a null inside the
	 * message.
	 */
	slen = strlen(str);
	if (msg->cursor + slen >= msg->len)
		ExceptionalCondition("invalid string in message", __FILE__, __LINE__);
	msg->cursor += slen + 1;

	return str;
}

/* --------------------------------
 *		pq_getmsgend	- verify message fully consumed
 * --------------------------------
 */
void
pq_getmsgend(StringInfo msg)
{
	if (msg->cursor != msg->len)
		ExceptionalCondition("invalid msg format", __FILE__, __LINE__);
}

/* --------------------------------
 *		pq_sendbytes	- append raw data to a StringInfo buffer
 * --------------------------------
 */
void
pq_sendbytes(StringInfo buf, const void *data, int datalen)
{
	/* use variant that maintains a trailing null-byte, out of caution */
	appendBinaryStringInfo(buf, data, datalen);
}

/*
 * Produce a C-string representation of a TimestampTz.
 *
 * This is mostly for use in emitting messages.
 */
const char *
timestamptz_to_str(TimestampTz t)
{
	static char buf[MAXDATELEN + 1];

	snprintf(buf, sizeof(buf), "TimestampTz(%ld)", t);
	return buf;
}

bool
TimestampDifferenceExceeds(TimestampTz start_time,
						   TimestampTz stop_time,
						   int msec)
{
	TimestampTz diff = stop_time - start_time;

	return (diff >= msec * INT64CONST(1000));
}

void
WalProposerLibLog(WalProposer *wp, int elevel, char *fmt,...)
{
	char		buf[1024];
	va_list		args;

	fmt = _(fmt);

	va_start(args, fmt);
	vsnprintf(buf, sizeof(buf), fmt, args);
	va_end(args);

	wp->api.log_internal(wp, elevel, buf);
}
