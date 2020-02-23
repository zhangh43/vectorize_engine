#include "vint.h"
#include "vtype.h"
#include "nodes/execnodes.h"
#include "executor/nodeAgg.h"

PG_FUNCTION_INFO_V1(vint8inc_any);
PG_FUNCTION_INFO_V1(vint4_sum);
PG_FUNCTION_INFO_V1(vint8inc);

Datum vint8inc_any(PG_FUNCTION_ARGS)
{
	int64		result;
	int64		arg;
	int			i;
	TupleHashEntry *entries;
	vtype		*batch;
	AggStatePerGroup group;
	int32 groupOffset = fcinfo->fncollation;

	if (groupOffset < 0)
	{
		/* Not called as an aggregate, so just do it the dumb way */
		arg = PG_GETARG_INT64(0);
		batch = (vtype *) PG_GETARG_POINTER(1);

		result = arg;

		for (i = 0; i < BATCHSIZE; i++)
		{
			if (batch->skipref[i])
				continue;
			result++;
		}

		/* Overflow check */
		if (result < 0 && arg > 0)
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("bigint out of range")));

		PG_RETURN_INT64(result);
	}

	entries = (TupleHashEntry *)PG_GETARG_POINTER(0);
	batch = (vtype *) PG_GETARG_POINTER(1);

	for (i = 0; i < BATCHSIZE; i++)
	{
		if (batch->skipref[i] || !entries[i])
			continue;

		group = (AggStatePerGroupData*)entries[i]->additional + groupOffset;

		arg = DatumGetInt64(group->transValue);
		result = arg + 1;
		/* Overflow check */
		if (result < 0 && arg > 0)
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("bigint out of range")));

		group->transValue = Int64GetDatum(result);
	}

	PG_RETURN_INT64(0);
}

Datum
vint4_sum(PG_FUNCTION_ARGS)
{
	TupleHashEntry *entries;
	vtype	*batch;
	int		i;
	int64	result;
	AggStatePerGroup group;
	int32	groupOffset = fcinfo->fncollation;

#if 0
	if (PG_ARGISNULL(0))
	{
		/* No non-null input seen so far... */
		if (PG_ARGISNULL(1))
			PG_RETURN_NULL();	/* still no non-null */
		/* This is the first non-null input. */
		newval = (int64) PG_GETARG_INT32(1);
		PG_RETURN_INT64(newval);
	}
#endif


	if (groupOffset < 0)
	{
		/* Not called as an aggregate, so just do it the dumb way */
		result = PG_GETARG_INT64(0);
		batch = (vtype *) PG_GETARG_POINTER(1);

		for (i = 0; i < BATCHSIZE; i++)
		{
			if (batch->skipref[i])
				continue;

			result += DatumGetInt32(batch->values[i]);
		}

		PG_RETURN_INT64(result);
	}

	entries = (TupleHashEntry *)PG_GETARG_POINTER(0);
	batch = (vtype *) PG_GETARG_POINTER(1);
	for (i = 0; i < BATCHSIZE; i++)
	{
		if (batch->skipref[i] || !entries[i])
			continue;

		group = (AggStatePerGroupData*)entries[i]->additional + groupOffset;

		result = DatumGetInt64(group->transValue);
		result += DatumGetInt32(batch->values[i]);

		group->transValue = Int64GetDatum(result);
	}

	PG_RETURN_INT64(0);
}

Datum vint8inc(PG_FUNCTION_ARGS)
{
	int64		result;
	int64		arg;
	int			i;
	TupleHashEntry *entries;
	vtype		*batch;
	AggStatePerGroup group;
	int32 groupOffset = fcinfo->fncollation;

	if (groupOffset < 0)
	{
		/* Not called as an aggregate, so just do it the dumb way */
		result = arg = PG_GETARG_INT64(0);
		batch = (vtype *) PG_GETARG_POINTER(1);

		for (i = 0; i < BATCHSIZE; i++)
		{
			if (batch->skipref[i])
				continue;
			result++;
		}
		/* Overflow check */
		if (result < 0 && arg > 0)
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("bigint out of range")));

		PG_RETURN_INT64(result);
	}

	entries = (TupleHashEntry *)PG_GETARG_POINTER(0);
	batch = (vtype *) PG_GETARG_POINTER(1);
	for (i = 0; i < BATCHSIZE; i++)
	{
		if (batch->skipref[i] || !entries[i])
			continue;

		group = (AggStatePerGroupData*)entries[i]->additional + groupOffset;
		arg = DatumGetInt64(group->transValue);
		result = arg + 1;
		/* Overflow check */
		if (result < 0 && arg > 0)
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("bigint out of range")));

		group->transValue = Int64GetDatum(result);
	}

	PG_RETURN_INT64(0);
}
