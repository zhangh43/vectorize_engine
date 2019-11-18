#include "vtimestamp.h"
#include "vtype.h"

#include "datatype/timestamp.h"
#include "catalog/pg_type.h"

PG_FUNCTION_INFO_V1(vtimestamp_mi_interval);
PG_FUNCTION_INFO_V1(vtimestamp_pl_interval);
PG_FUNCTION_INFO_V1(vtimestamp_in);
PG_FUNCTION_INFO_V1(vtimestamp_out);

vtimestamp* buildvtimestamp(int dim, bool *skip)
{
	return (vtimestamp*)buildvtype(TIMESTAMPOID, dim, skip);
}
/*
 * We are currently sharing some code between timestamp and timestamptz.
 * The comparison functions are among them. - thomas 2001-09-25
 *
 *		timestamp_relop - is timestamp1 relop timestamp2
 *
 *		collate invalid timestamp at the end
 */
Datum
vtimestamp_timestamp_cmp_internal(vtimestamp *vdt1, Timestamp dt2)
{
	int		i;
	vint4	*result;
	Timestamp dt1;	

	result = buildvtype(INT4OID,vdt1->dim, vdt1->skipref);
#ifdef HAVE_INT64_TIMESTAMP
	for (i = 0; i < BATCHSIZE; i++ )
	{
		if (vdt1->skipref[i])
			continue;
		dt1 = DatumGetTimestamp(vdt1->values[i]);
		result->values[i] = Int32GetDatum((dt1 < dt2) ? -1 : ((dt1 > dt2) ? 1 : 0));
	}
	return PointerGetDatum(result);
#else
	elog(ERROR, "HAVE_INT64_TIMESTAMP must be enabled in vectorize executor.");
#endif
}


/* timestamp_pl_interval()
 * Add an interval to a timestamp data type.
 * Note that interval has provisions for qualitative year/month and day
 *	units, so try to do the right thing with them.
 * To add a month, increment the month, and use the same day of month.
 * Then, if the next month has fewer days, set the day of month
 *	to the last day of month.
 * To add a day, increment the mday, and use the same time of day.
 * Lastly, add in the "quantitative time".
 */
Datum
vtimestamp_pl_interval(PG_FUNCTION_ARGS)
{
	vtimestamp	*vts = (vtimestamp *)PG_GETARG_POINTER(0);
	Interval	*span = PG_GETARG_INTERVAL_P(1);
	Timestamp	timestamp;
	vtimestamp	*result;
	int i;

	result = buildvtimestamp(vts->dim, vts->skipref);

	for(i = 0; i< vts->dim; i++)
	{
		if (vts->skipref[i])
			continue;
		timestamp = DatumGetTimestamp(vts->values[i]);
		if (TIMESTAMP_NOT_FINITE(timestamp))
			result->values[i] = TimestampGetDatum(timestamp);
		else
		{
			if (span->month != 0)
			{
				struct pg_tm tt,
							 *tm = &tt;
				fsec_t		fsec;

				if (timestamp2tm(timestamp, NULL, tm, &fsec, NULL, NULL) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
							 errmsg("timestamp out of range")));

				tm->tm_mon += span->month;
				if (tm->tm_mon > MONTHS_PER_YEAR)
				{
					tm->tm_year += (tm->tm_mon - 1) / MONTHS_PER_YEAR;
					tm->tm_mon = ((tm->tm_mon - 1) % MONTHS_PER_YEAR) + 1;
				}
				else if (tm->tm_mon < 1)
				{
					tm->tm_year += tm->tm_mon / MONTHS_PER_YEAR - 1;
					tm->tm_mon = tm->tm_mon % MONTHS_PER_YEAR + MONTHS_PER_YEAR;
				}

				/* adjust for end of month boundary problems... */
				if (tm->tm_mday > day_tab[isleap(tm->tm_year)][tm->tm_mon - 1])
					tm->tm_mday = (day_tab[isleap(tm->tm_year)][tm->tm_mon - 1]);

				if (tm2timestamp(tm, fsec, NULL, &timestamp) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
							 errmsg("timestamp out of range")));
			}

			if (span->day != 0)
			{
				struct pg_tm tt,
							 *tm = &tt;
				fsec_t		fsec;
				int			julian;

				if (timestamp2tm(timestamp, NULL, tm, &fsec, NULL, NULL) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
							 errmsg("timestamp out of range")));

				/* Add days by converting to and from Julian */
				julian = date2j(tm->tm_year, tm->tm_mon, tm->tm_mday) + span->day;
				j2date(julian, &tm->tm_year, &tm->tm_mon, &tm->tm_mday);

				if (tm2timestamp(tm, fsec, NULL, &timestamp) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
							 errmsg("timestamp out of range")));
			}

			timestamp += span->time;

			if (!IS_VALID_TIMESTAMP(timestamp))
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range")));

			result->values[i] = TimestampGetDatum(timestamp);
		}
	}
	PG_RETURN_POINTER(result);
}

Datum
vtimestamp_mi_interval(PG_FUNCTION_ARGS)
{
	vtimestamp	*vts = (vtimestamp *)PG_GETARG_POINTER(0);
	Interval	*span = PG_GETARG_INTERVAL_P(1);
	Interval	tspan;

	tspan.month = -span->month;
	tspan.day = -span->day;
	tspan.time = -span->time;

	return DirectFunctionCall2(vtimestamp_pl_interval,
							   PointerGetDatum(vts),
							   PointerGetDatum(&tspan));
}


Datum vtimestamp_in(PG_FUNCTION_ARGS)
{
	elog(ERROR, "vtimestamp_in not supported");
}
Datum vtimestamp_out(PG_FUNCTION_ARGS)
{
	elog(ERROR, "vtimestamp_out not supported");
}
