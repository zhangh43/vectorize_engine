#include "vfloat.h"
#include "vtype.h"
#include "math.h"
#include "utils/array.h"
#include "catalog/pg_type.h"
#include "nodes/execnodes.h"

PG_FUNCTION_INFO_V1(vfloat8vfloat8mul2);
PG_FUNCTION_INFO_V1(vfloat8pl);
PG_FUNCTION_INFO_V1(vfloat8_accum);
PG_FUNCTION_INFO_V1(vfloat8_avg);
PG_FUNCTION_INFO_V1(vfloat8_combine);

/*
 * A higher level structure to earily reference grouping keys and their
 * aggregate values.
 */
typedef struct GroupKeysAndAggs
{
    struct MemTupleData *tuple[BATCHSIZE]; /* tuple that contains grouping keys */
    AggStatePerGroup aggs[BATCHSIZE]; /* the location for the first aggregate values. */
    bool            skip[BATCHSIZE];
} GroupKeysAndAggs;

typedef struct AggStatePerGroupData
{
    Datum       transValue;     /* current transition value */
    bool        transValueIsNull;

    bool        noTransValue;   /* true if transValue not set yet */

    /*
     * Note: noTransValue initially has the same value as transValueIsNull,
     * and if true both are cleared to false at the same time.  They are not
     * the same though: if transfn later returns a NULL, we want to keep that
     * NULL and not auto-replace it with a later input value. Only the first
     * non-NULL input will be auto-substituted.
     */
} AggStatePerGroupData;

typedef struct AggHashEntryData *AggHashEntry;

typedef struct AggHashEntryData
{
    TupleHashEntryData shared;  /* common header for hash table entries */
    /* per-aggregate transition status array */
    AggStatePerGroupData pergroup[FLEXIBLE_ARRAY_MEMBER];
}   AggHashEntryData;

static float8 *
check_float8_array(ArrayType *transarray, const char *caller, int n);

#define CHECKFLOATVAL(val, inf_is_valid, zero_is_valid)			\
do {															\
	if (isinf(val) && !(inf_is_valid))							\
		ereport(ERROR,											\
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),	\
		  errmsg("value out of range: overflow")));				\
																\
	if ((val) == 0.0 && !(zero_is_valid))						\
		ereport(ERROR,											\
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),	\
		 errmsg("value out of range: underflow")));				\
} while(0)


Datum vfloat8vfloat8mul2(PG_FUNCTION_ARGS)
{
	vtype		*arg1 = (vtype *)PG_GETARG_POINTER(0);
	vtype		*arg2 = (vtype *)PG_GETARG_POINTER(1);
	vtype		*result;
	float8		mul;
	int			i;

	result = buildvtype(FLOAT8OID, BATCHSIZE, arg1->skipref);

	for (i = 0; i < BATCHSIZE; i++ )
	{
		if (arg1->skipref[i])
			continue;
		mul = DatumGetFloat8(arg1->values[i]) * DatumGetFloat8(arg2->values[i]);
		result->values[i] = Float8GetDatum(mul);
	}

	PG_RETURN_POINTER(result);
}

Datum vfloat8pl(PG_FUNCTION_ARGS)
{
	float8		result;
	float8		arg1;
	float8		arg2;
	int			i;
	GroupKeysAndAggs *entries;
	vtype		*batch;
	int32 groupno = PG_GETARG_INT32(1);

	if (groupno < 0)
		elog(ERROR, "Not implemented");

	entries = (GroupKeysAndAggs *)PG_GETARG_POINTER(0);
	batch = (vtype *) PG_GETARG_POINTER(2);
	for (i = 0; i < BATCHSIZE; i++)
	{
		if (batch->skipref[i])
			continue;
		
		arg1 = DatumGetFloat8(entries->aggs[i][groupno].transValue);
		arg2 = DatumGetFloat8(batch->values[i]);
		result = arg1 + arg2;

		CHECKFLOATVAL(result, isinf(arg1) || isinf(arg2), true);
		entries->aggs[i][groupno].transValue = Float8GetDatum(result);
	}

	PG_RETURN_INT64(0);
}


Datum
vfloat8_accum(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray;
	float8		newval;
	float8	   *transvalues;
	float8		N,
				sumX,
				sumX2;

	GroupKeysAndAggs *entries;
	int			i;
	vtype		*batch;
	int32		groupno = PG_GETARG_INT32(1);

	if (groupno < 0)
		elog(ERROR, "Not implemented");

	entries = (GroupKeysAndAggs *)PG_GETARG_POINTER(0);
	batch = (vtype *) PG_GETARG_POINTER(2);

	for (i = 0; i < BATCHSIZE; i++)
	{
		if (batch->skipref[i])
			continue;
		
		transarray = DatumGetArrayTypeP(entries->aggs[i][groupno].transValue);
		//transDatum = (Datum *)(entries[i] + groupOffset);
		//transarray = DatumGetArrayTypeP(*transDatum);
		transvalues = check_float8_array(transarray, "float8_accum", 3);
		N = transvalues[0];
		sumX = transvalues[1];
		sumX2 = transvalues[2];
		newval = DatumGetFloat8(batch->values[i]);

		N += 1.0;
		sumX += newval;
		CHECKFLOATVAL(sumX, isinf(transvalues[1]) || isinf(newval), true);
		sumX2 += newval * newval;
		CHECKFLOATVAL(sumX2, isinf(transvalues[2]) || isinf(newval), true);

		/*
		 * If we're invoked as an aggregate, we can cheat and modify our first
		 * parameter in-place to reduce palloc overhead. Otherwise we construct a
		 * new array with the updated transition data and return it.
		 */
		if (AggCheckCallContext(fcinfo, NULL))
		{
			transvalues[0] = N;
			transvalues[1] = sumX;
			transvalues[2] = sumX2;
		}
#if 0
		else
		{
			Datum		transdatums[3];
			ArrayType  *result;

			transdatums[0] = Float8GetDatumFast(N);
			transdatums[1] = Float8GetDatumFast(sumX);
			transdatums[2] = Float8GetDatumFast(sumX2);

			result = construct_array(transdatums, 3,
					FLOAT8OID,
					sizeof(float8), FLOAT8PASSBYVAL, 'd');
		}
#endif
	}
	PG_RETURN_ARRAYTYPE_P(0);
}

/*
 * float8_combine
 *
 * An aggregate combine function used to combine two 3 fields
 * aggregate transition data into a single transition data.
 * This function is used only in two stage aggregation and
 * shouldn't be called outside aggregate context.
 */
Datum
vfloat8_combine(PG_FUNCTION_ARGS)
{
	GroupKeysAndAggs *entries;
	int			i;
	vtype		*batch;
	int32		groupno = PG_GETARG_INT32(1);
	ArrayType  *transarray1;
	ArrayType  *transarray2;
	float8	   *transvalues1;
	float8	   *transvalues2;
	float8		N,
				sumX,
				sumX2;

	if (groupno < 0)
		elog(ERROR, "Not implemented");
	if (!AggCheckCallContext(fcinfo, NULL))
		elog(ERROR, "aggregate function called in non-aggregate context");

	entries = (GroupKeysAndAggs *)PG_GETARG_POINTER(0);
	batch = (vtype *) PG_GETARG_POINTER(2);

	for (i = 0; i < BATCHSIZE; i++)
	{
		if (batch->skipref[i])
			continue;

		transarray1 = DatumGetArrayTypeP(entries->aggs[i][groupno].transValue);
		transarray2 = DatumGetArrayTypeP(batch->values[i]);

		transvalues1 = check_float8_array(transarray1, "float8_combine", 3);
		N = transvalues1[0];
		sumX = transvalues1[1];
		sumX2 = transvalues1[2];

		transvalues2 = check_float8_array(transarray2, "float8_combine", 3);

		N += transvalues2[0];
		sumX += transvalues2[1];
		CHECKFLOATVAL(sumX, isinf(transvalues1[1]) || isinf(transvalues2[1]),
				true);
		sumX2 += transvalues2[2];
		CHECKFLOATVAL(sumX2, isinf(transvalues1[2]) || isinf(transvalues2[2]),
				true);

		transvalues1[0] = N;
		transvalues1[1] = sumX;
		transvalues1[2] = sumX2;
	}

	PG_RETURN_ARRAYTYPE_P(0);
}

Datum
vfloat8_avg(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
	float8	   *transvalues;
	float8		N,
				sumX;

	transvalues = check_float8_array(transarray, "float8_avg", 3);
	N = transvalues[0];
	sumX = transvalues[1];
	/* ignore sumX2 */

	/* SQL defines AVG of no values to be NULL */
	if (N == 0.0)
		PG_RETURN_NULL();

	PG_RETURN_FLOAT8(sumX / N);
}

static float8 *
check_float8_array(ArrayType *transarray, const char *caller, int n)
{
	/*
	 * We expect the input to be an N-element float array; verify that. We
	 * don't need to use deconstruct_array() since the array data is just
	 * going to look like a C array of N float8 values.
	 */
	if (ARR_NDIM(transarray) != 1 ||
		ARR_DIMS(transarray)[0] != n ||
		ARR_HASNULL(transarray) ||
		ARR_ELEMTYPE(transarray) != FLOAT8OID)
		elog(ERROR, "%s: expected %d-element float8 array", caller, n);
	return (float8 *) ARR_DATA_PTR(transarray);
}
