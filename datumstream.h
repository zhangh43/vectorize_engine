/*-------------------------------------------------------------------------
 *
 * datumstream.h
 *
 * Portions Copyright (c) 2008, Greenplum Inc.
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/utils/datumstream.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef VECTOR_DATUMSTREAM_H
#define VECTOR_DATUMSTREAM_H

#include "catalog/pg_attribute.h"
#include "utils/datumstreamblock.h"
#include "datumstreamblock.h"

inline static void
vdatumstreamread_get(DatumStreamRead * acc, Datum *datum, bool *null, int minimalBatchSize)
{
	if (acc->largeObjectState == DatumStreamLargeObjectState_None)
	{
		/*
		 * Small objects are handled by the DatumStreamBlockRead module.
		 */
		vDatumStreamBlockRead_Get(&acc->blockRead, datum, null, minimalBatchSize);
	}
	else
	{
		elog(ERROR, "datumstreamread_getlarge not supported");
		datumstreamread_getlarge(acc, datum, null);
	}
}
#endif
