#ifndef VECTOR_ENGINE_EXECUTOR_H
#define VECTOR_ENGINE_EXECUTOR_H

#include "postgres.h"
#include "executor/execdesc.h"
#include "nodes/parsenodes.h"

#include "nodeSeqscan.h"

/*
 * prototypes from functions in execExpr.c
 */
extern ExprState *VExecInitExpr(Expr *node, PlanState *parent);
extern ExprState *VExecInitExprWithParams(Expr *node, ParamListInfo ext_params);
extern ExprState *VExecInitQual(List *qual, PlanState *parent);
extern ExprState *VExecInitCheck(List *qual, PlanState *parent);
extern List *VExecInitExprList(List *nodes, PlanState *parent);
extern ExprState *VExecBuildAggTrans(AggState *aggstate, struct AggStatePerPhaseData *phase,
									bool doSort, bool doHash);
extern ExprState *VExecBuildGroupingEqual(TupleDesc ldesc, TupleDesc rdesc,
										 const TupleTableSlotOps *lops, const TupleTableSlotOps *rops,
										 int numCols,
										 const AttrNumber *keyColIdx,
										 const Oid *eqfunctions,
										 const Oid *collations,
										 PlanState *parent);
extern ProjectionInfo *VExecBuildProjectionInfo(List *targetList,
											   ExprContext *econtext,
											   TupleTableSlot *slot,
											   PlanState *parent,
											   TupleDesc inputDesc);
extern ExprState *VExecPrepareExpr(Expr *node, EState *estate);
extern ExprState *VExecPrepareQual(List *qual, EState *estate);
extern ExprState *VExecPrepareCheck(List *qual, EState *estate);
extern List *VExecPrepareExprList(List *nodes, EState *estate);

/*
 * prototypes from functions in execScan.c
 */
extern TupleTableSlot *VExecScan(ScanState *node, ExecScanAccessMtd accessMtd,
								ExecScanRecheckMtd recheckMtd);
extern void VExecAssignScanProjectionInfo(ScanState *node);
extern void VExecAssignScanProjectionInfoWithVarno(ScanState *node, Index varno);
extern void VExecScanReScan(ScanState *node);

/*
 * prototypes from functions in execTuples.c
 */
extern void VectorExecInitResultTypeTL(PlanState *planstate);

/*
 * prototypes from functions in execUtils.c
 */
extern const TupleTableSlotOps *VectorExecGetResultSlotOps(PlanState *planstate,
													 bool *isfixed);
extern void VectorExecAssignProjectionInfo(PlanState *planstate,
									 TupleDesc inputDesc);
extern void VectorExecConditionalAssignProjectionInfo(PlanState *planstate,
												TupleDesc inputDesc, Index varno);

/*
 * ExecEvalExprSwitchContext
 *
 * Same as ExecEvalExpr, but get into the right allocation context explicitly.
 */
#ifndef FRONTEND
static inline Datum
VExecEvalExprSwitchContext(ExprState *state,
						  ExprContext *econtext,
						  bool *isNull)
{
	Datum		retDatum;
	MemoryContext oldContext;

	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
	retDatum = state->evalfunc(state, econtext, isNull);
	MemoryContextSwitchTo(oldContext);
	return retDatum;
}
#endif

/*
 * ExecProject
 *
 * Projects a tuple based on projection info and stores it in the slot passed
 * to ExecBuildProjectionInfo().
 *
 * Note: the result is always a virtual tuple; therefore it may reference
 * the contents of the exprContext's scan tuples and/or temporary results
 * constructed in the exprContext.  If the caller wishes the result to be
 * valid longer than that data will be valid, he must call ExecMaterializeSlot
 * on the result slot.
 */
#ifndef FRONTEND
static inline TupleTableSlot *
VExecProject(ProjectionInfo *projInfo)
{
	ExprContext *econtext = projInfo->pi_exprContext;
	ExprState  *state = &projInfo->pi_state;
	TupleTableSlot *slot = state->resultslot;
	bool		isnull;

	/*
	 * Clear any former contents of the result slot.  This makes it safe for
	 * us to use the slot's Datum/isnull arrays as workspace.
	 */
	ExecClearTuple(slot);

	/* Run the expression, discarding scalar result from the last column. */
	(void) VExecEvalExprSwitchContext(state, econtext, &isnull);

	/*
	 * Successfully formed a result row.  Mark the result slot as containing a
	 * valid virtual tuple (inlined version of ExecStoreVirtualTuple()).
	 */
	slot->tts_flags &= ~TTS_FLAG_EMPTY;
	slot->tts_nvalid = slot->tts_tupleDescriptor->natts;

	return slot;
}
#endif

/*
 * ExecQual - evaluate a qual prepared with ExecInitQual (possibly via
 * ExecPrepareQual).  Returns true if qual is satisfied, else false.
 *
 * Note: ExecQual used to have a third argument "resultForNull".  The
 * behavior of this function now corresponds to resultForNull == false.
 * If you want the resultForNull == true behavior, see ExecCheck.
 */
#ifndef FRONTEND
static inline bool
VExecQual(ExprState *state, ExprContext *econtext)
{
	Datum		ret;
	bool		isnull;

	/* short-circuit (here and in ExecInitQual) for empty restriction list */
	if (state == NULL)
		return true;

	/* verify that expression was compiled using ExecInitQual */
	Assert(state->flags & EEO_FLAG_IS_QUAL);

	ret = VExecEvalExprSwitchContext(state, econtext, &isnull);

	/* EEOP_QUAL should never return NULL */
	Assert(!isnull);

	return DatumGetBool(ret);
}
#endif

#endif
