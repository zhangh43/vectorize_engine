/*-------------------------------------------------------------------------
 *
 * execUtils.c
 *	  miscellaneous executor utility routines
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/execUtils.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		CreateExecutorState		Create/delete executor working state
 *		FreeExecutorState
 *		CreateExprContext
 *		CreateStandaloneExprContext
 *		FreeExprContext
 *		ReScanExprContext
 *
 *		ExecAssignExprContext	Common code for plan node init routines.
 *		etc
 *
 *		ExecOpenScanRelation	Common code for scan node init routines.
 *
 *		ExecInitRangeTable		Set up executor's range-table-related data.
 *
 *		ExecGetRangeTableRelation		Fetch Relation for a rangetable entry.
 *
 *		executor_errposition	Report syntactic position of an error.
 *
 *		RegisterExprContextCallback    Register function shutdown callback
 *		UnregisterExprContextCallback  Deregister function shutdown callback
 *
 *		GetAttributeByName		Runtime extraction of columns from tuples.
 *		GetAttributeByNum
 *
 *	 NOTES
 *		This file has traditionally been the place to stick misc.
 *		executor support stuff that doesn't really go anyplace else.
 */

#include "postgres.h"

#include "access/parallel.h"
#include "access/relscan.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "executor/executor.h"
#include "jit/jit.h"
#include "mb/pg_wchar.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "partitioning/partdesc.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/typcache.h"

#include "executor.h"
#include "tuptable.h"

static bool tlist_matches_tupdesc(PlanState *ps, List *tlist, Index varno, TupleDesc tupdesc);

/* ----------------------------------------------------------------
 *				 Executor state and memory management functions
 * ----------------------------------------------------------------
 */

/*
 * ExecGetResultSlotOps - information about node's type of result slot
 */
const TupleTableSlotOps *
VectorExecGetResultSlotOps(PlanState *planstate, bool *isfixed)
{
	if (planstate->resultopsset && planstate->resultops)
	{
		if (isfixed)
			*isfixed = planstate->resultopsfixed;
		return planstate->resultops;
	}

	if (isfixed)
	{
		if (planstate->resultopsset)
			*isfixed = planstate->resultopsfixed;
		else if (planstate->ps_ResultTupleSlot)
			*isfixed = TTS_FIXED(planstate->ps_ResultTupleSlot);
		else
			*isfixed = false;
	}

	if (!planstate->ps_ResultTupleSlot)
		return &TTSOpsVirtual;

	return planstate->ps_ResultTupleSlot->tts_ops;
}


/* ----------------
 *		ExecAssignProjectionInfo
 *
 * forms the projection information from the node's targetlist
 *
 * Notes for inputDesc are same as for ExecBuildProjectionInfo: supply it
 * for a relation-scan node, can pass NULL for upper-level nodes
 * ----------------
 */
void
VectorExecAssignProjectionInfo(PlanState *planstate,
						 TupleDesc inputDesc)
{
	planstate->ps_ProjInfo =
		VExecBuildProjectionInfo(planstate->plan->targetlist,
								planstate->ps_ExprContext,
								planstate->ps_ResultTupleSlot,
								planstate,
								inputDesc);
}


/* ----------------
 *		ExecConditionalAssignProjectionInfo
 *
 * as ExecAssignProjectionInfo, but store NULL rather than building projection
 * info if no projection is required
 * ----------------
 */
void
VectorExecConditionalAssignProjectionInfo(PlanState *planstate, TupleDesc inputDesc,
									Index varno)
{
	if (tlist_matches_tupdesc(planstate,
							  planstate->plan->targetlist,
							  varno,
							  inputDesc))
	{
		planstate->ps_ProjInfo = NULL;
		planstate->resultopsset = planstate->scanopsset;
		planstate->resultopsfixed = planstate->scanopsfixed;
		planstate->resultops = planstate->scanops;
	}
	else
	{
		if (!planstate->ps_ResultTupleSlot)
		{
			ExecInitResultSlot(planstate, &TTSOpsVectorVirtual);
			planstate->resultops = &TTSOpsVectorVirtual;
			planstate->resultopsfixed = true;
			planstate->resultopsset = true;
		}
		VectorExecAssignProjectionInfo(planstate, inputDesc);
	}
}

static bool
tlist_matches_tupdesc(PlanState *ps, List *tlist, Index varno, TupleDesc tupdesc)
{
	int			numattrs = tupdesc->natts;
	int			attrno;
	ListCell   *tlist_item = list_head(tlist);

	/* Check the tlist attributes */
	for (attrno = 1; attrno <= numattrs; attrno++)
	{
		Form_pg_attribute att_tup = TupleDescAttr(tupdesc, attrno - 1);
		Var		   *var;

		if (tlist_item == NULL)
			return false;		/* tlist too short */
		var = (Var *) ((TargetEntry *) lfirst(tlist_item))->expr;
		if (!var || !IsA(var, Var))
			return false;		/* tlist item not a Var */
		/* if these Asserts fail, planner messed up */
		Assert(var->varno == varno);
		Assert(var->varlevelsup == 0);
		if (var->varattno != attrno)
			return false;		/* out of order */
		if (att_tup->attisdropped)
			return false;		/* table contains dropped columns */
		if (att_tup->atthasmissing)
			return false;		/* table contains cols with missing values */

		/*
		 * Note: usually the Var's type should match the tupdesc exactly, but
		 * in situations involving unions of columns that have different
		 * typmods, the Var may have come from above the union and hence have
		 * typmod -1.  This is a legitimate situation since the Var still
		 * describes the column, just not as exactly as the tupdesc does. We
		 * could change the planner to prevent it, but it'd then insert
		 * projection steps just to convert from specific typmod to typmod -1,
		 * which is pretty silly.
		 */
		if (var->vartype != att_tup->atttypid ||
			(var->vartypmod != att_tup->atttypmod &&
			 var->vartypmod != -1))
			return false;		/* type mismatch */

		tlist_item = lnext(tlist, tlist_item);
	}

	if (tlist_item)
		return false;			/* tlist too long */

	return true;
}
