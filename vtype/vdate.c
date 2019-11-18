#include "vdate.h"
#include "vtype/vtimestamp.h"

#include "catalog/pg_type.h"
#include "utils/date.h"

PG_FUNCTION_INFO_V1(vdate_le_timestamp);
PG_FUNCTION_INFO_V1(vdate_mi_interval);
PG_FUNCTION_INFO_V1(vdate_le);
PG_FUNCTION_INFO_V1(vdate_in);
PG_FUNCTION_INFO_V1(vdate_out);


static vtimestamp* vdate2vtimestamp(vdate* vdateVal);
/*
 * Internal routines for promoting date to timestamp and timestamp with
 * time zone
 */

vdate* buildvdate(int dim, bool *skip)
{
	return (vdate *)buildvtype(DATEOID, dim, skip);	
}


static vtimestamp*
vdate2vtimestamp(vdate* vdateVal)
{
	vtimestamp	*result;
	int 		i;
	DateADT		dateVal;
	Timestamp	tmp;	
	
	result = buildvtimestamp(vdateVal->dim, vdateVal->skipref);
	for (i = 0; i < BATCHSIZE; i++)
	{
		if (vdateVal->skipref[i])
			continue;
		dateVal = DatumGetDateADT(vdateVal->values[i]);	
#ifdef HAVE_INT64_TIMESTAMP
		/* date is days since 2000, timestamp is microseconds since same... */
		tmp = dateVal * USECS_PER_DAY;
		
		/* Date's range is wider than timestamp's, so must check for overflow */
		if (tmp / USECS_PER_DAY != dateVal)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("date out of range for timestamp")));

		result->values[i] = TimestampGetDatum(tmp);
#else
		/* date is days since 2000, timestamp is seconds since same... */
		result->values[i] = TimestampGetDatum(dateVal * (double) SECS_PER_DAY);
#endif
	}
	return result;
}
Datum
vdate_le_timestamp(PG_FUNCTION_ARGS)
{
	vdate		*vdateVal = (vdate *)PG_GETARG_POINTER(0);
	Timestamp	dt2 = PG_GETARG_TIMESTAMP(1);
	vtimestamp	*vdt1;
	int			i;
	vbool		*result;
	Timestamp	dt1;	
	
	vdt1 = vdate2vtimestamp(vdateVal);

	result = buildvbool(vdt1->dim, vdt1->skipref);
#ifdef HAVE_INT64_TIMESTAMP
	for (i = 0; i < BATCHSIZE; i++ )
	{
		if (vdt1->skipref[i])
			continue;
		dt1 = DatumGetTimestamp(vdt1->values[i]);
		result->values[i] = BoolGetDatum((dt1 <= dt2) ? true :false);
	}
	return PointerGetDatum(result);
#else
	elog(ERROR, "HAVE_INT64_TIMESTAMP must be enabled in vectorize executor.");
#endif
}


Datum vdate_mi_interval(PG_FUNCTION_ARGS)
{
	vdate		*vdateVal = (vdate *)PG_GETARG_POINTER(0);
	Interval   *span = PG_GETARG_INTERVAL_P(1);
	vtimestamp	*vdateStamp;

	vdateStamp = vdate2vtimestamp(vdateVal);

	return DirectFunctionCall2(vtimestamp_mi_interval,
							   PointerGetDatum(vdateStamp),
							   PointerGetDatum(span));
}

Datum
vdate_le(PG_FUNCTION_ARGS)
{
	vdate		*vdt1 = (vdate *)PG_GETARG_POINTER(0);
	DateADT		dateVal2 = PG_GETARG_DATEADT(1);
	vbool		*result;
	int			i;
	
	result = buildvbool(vdt1->dim, vdt1->skipref);
	for (i = 0; i < BATCHSIZE; i++ )
	{
		if (vdt1->skipref[i])
			continue;
		result->values[i] = BoolGetDatum(DatumGetDateADT(vdt1->values[i]) <= dateVal2);
	}
	PG_RETURN_POINTER(result);
}

Datum vdate_in(PG_FUNCTION_ARGS)
{
	elog(ERROR, "vdate_in not supported");
}

Datum vdate_out(PG_FUNCTION_ARGS)
{
	elog(ERROR, "vdate_out not supported");
}
