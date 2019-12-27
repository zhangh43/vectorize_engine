/*-------------------------------------------------------------------------
 *
 * nodeAgg.h
 *	  TODO file description
 *
 *
 * Copyright (c) 2019-Present Pivotal Software, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */

#ifndef VECTOR_ENGINE_NODE_AGG_H
#define VECTOR_ENGINE_NODE_AGG_H

#include "nodes/plannodes.h"
#include "vtype/vtype.h"
/*
 * VectorAggState - state object of vectoragg on executor.
 */
typedef struct VectorAggState
{
	CustomScanState	css;

	/* Attributes for vectorization */
	AggState		*aggstate;
	TupleTableSlot	*resultSlot;
} VectorAggState;

/*
 * A higher level structure to earily reference grouping keys and their
 * aggregate values.
 */
typedef struct GroupKeysAndAggs
{
	struct MemTupleData *tuple[BATCHSIZE]; /* tuple that contains grouping keys */
	AggStatePerGroup aggs[BATCHSIZE]; /* the location for the first aggregate values. */
	bool			skip[BATCHSIZE];
} GroupKeysAndAggs;

extern CustomScan *MakeCustomScanForAgg(void);
extern void InitVectorAgg(void);
extern void vadvance_aggregates(AggState *aggstate, GroupKeysAndAggs *entries);
extern void vcombine_aggregates(AggState *aggstate, AggStatePerGroup *entries);
extern void vadvance_combine_function(AggState *aggstate, AggStatePerTrans pertrans, AggStatePerGroup *entries, FunctionCallInfo fcinfo);
#endif   /* VECTOR_ENGINE_NODE_AGG_H */
