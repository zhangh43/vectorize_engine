#include "vint.h"
#include "vtype.h"

PG_FUNCTION_INFO_V1(vint8inc_any);
PG_FUNCTION_INFO_V1(vint4_sum);
PG_FUNCTION_INFO_V1(vint8inc);

Datum vint8inc_any(PG_FUNCTION_ARGS)
{
	int64		result;
	int64		arg;
	int			i;
	char		**entries;
	vtype		*batch;
	Datum *transVal;
	
	int32 groupOffset = PG_GETARG_INT32(1);

	if (groupOffset < 0)
	{
		/* Not called as an aggregate, so just do it the dumb way */
		arg = PG_GETARG_INT64(0);
		batch = (vtype *) PG_GETARG_POINTER(2);
		
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

	entries = (char **)PG_GETARG_POINTER(0);
	batch = (vtype *) PG_GETARG_POINTER(2);

	for (i = 0; i < BATCHSIZE; i++)
	{
		if (batch->skipref[i])
			continue;

		transVal = (Datum *)(entries[i] + groupOffset);	

		arg = DatumGetInt64(*transVal);
		result = arg + 1;
		/* Overflow check */
		if (result < 0 && arg > 0)
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("bigint out of range")));

		*transVal = Int64GetDatum(result);
	}

	PG_RETURN_INT64(0);
}

Datum
vint4_sum(PG_FUNCTION_ARGS)
{
	char	**entries;
	vtype	*batch;
	int		i;
	int64	result;
	Datum *transVal;
	int32	groupOffset = PG_GETARG_INT32(1);

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
		batch = (vtype *) PG_GETARG_POINTER(2);

		for (i = 0; i < BATCHSIZE; i++)
		{
			if (batch->skipref[i])
				continue;

			result += DatumGetInt32(batch->values[i]);
		}

		PG_RETURN_INT64(result);
	}

	entries = (char **)PG_GETARG_POINTER(0);
	batch = (vtype *) PG_GETARG_POINTER(2);
	for (i = 0; i < BATCHSIZE; i++)
	{
		if (batch->skipref[i])
			continue;

		transVal = (Datum *)(entries[i] + groupOffset);	

		result = DatumGetInt64(*transVal);
		result += DatumGetInt32(batch->values[i]);

		*transVal = Int64GetDatum(result);
	}

	PG_RETURN_INT64(0);
}

Datum vint8inc(PG_FUNCTION_ARGS)
{
	int64		result;
	int64		arg;
	int			i;
	char		**entries;
	vtype		*batch;
	Datum *transVal;
	int32 groupOffset = PG_GETARG_INT32(1);

	if (groupOffset < 0)
	{
		/* Not called as an aggregate, so just do it the dumb way */
		result = arg = PG_GETARG_INT64(0);
		batch = (vtype *) PG_GETARG_POINTER(2);

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

	entries = (char **)PG_GETARG_POINTER(0);
	batch = (vtype *) PG_GETARG_POINTER(2);
	for (i = 0; i < BATCHSIZE; i++)
	{
		if (batch->skipref[i])
			continue;

		transVal = (Datum *)(entries[i] + groupOffset);	
		arg = DatumGetInt64(*transVal);
		result = arg + 1;
		/* Overflow check */
		if (result < 0 && arg > 0)
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("bigint out of range")));

		*transVal = Int64GetDatum(result);
	}

	PG_RETURN_INT64(0);
}
