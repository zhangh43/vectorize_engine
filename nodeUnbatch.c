/*-------------------------------------------------------------------------
 *
 * unbatch.c
 *	  TODO file description
 *
 *
 * Copyright (c) 2019-Present Pivotal Software, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "optimizer/planner.h"
#include "executor/nodeCustom.h"

#include "nodeUnbatch.h"
#include "execTuples.h"
#include "vtype/vtype.h"
#include "utils.h"
#include "vectorTupleSlot.h"


/*
 * UnbatchState - state object of vectorscan on executor.
 */
typedef struct UnbatchState
{
	CustomScanState	css;

	TupleTableSlot *ps_ResultVTupleSlot; /* slot for my result tuples */
	int				iter;

	/* Attributes for vectorization */
} UnbatchState;

static Node *CreateUnbatchState(CustomScan *custom_plan);
static TupleTableSlot *FetchRowFromBatch(UnbatchState *ubs);
static bool ReadNextVectorSlot(UnbatchState *ubs);
/* CustomScanExecMethods */
static void BeginUnbatch(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *ExecUnbatch(CustomScanState *node);
static void EndUnbatch(CustomScanState *node);

static CustomScanMethods	unbatch_methods = {
	"unbatch",			/* CustomName */
	CreateUnbatchState,	/* CreateCustomScanState */
};

static CustomExecMethods	unbatch_exec_methods = {
	"unbatch",			/* CustomName */
	BeginUnbatch,		/* BeginCustomScan */
	ExecUnbatch,			/* ExecCustomScan */
	EndUnbatch,			/* EndCustomScan */
	NULL,					/* ReScanCustomScan */
	NULL,					/* MarkPosCustomScan */
	NULL,					/* RestrPosCustomScan */
	NULL,					/* EstimateDSMCustomScan */
	NULL,					/* InitializeDSMCustomScan */
	NULL,					/* InitializeWorkerCustomScan */
	NULL,					/* ExplainCustomScan */
};

static void
BeginUnbatch(CustomScanState *node, EState *estate, int eflags)
{
	UnbatchState *vcs = (UnbatchState*) node;
	CustomScan     *cscan = (CustomScan *) node->ss.ps.plan;
	TupleDesc   tupdesc;

	outerPlanState(vcs) = ExecInitNode(outerPlan(cscan), estate, eflags);

	/* Convert Vtype in tupdesc to Ntype in unbatch Node */
	{
		node->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor = CreateTupleDescCopy(outerPlanState(vcs)->ps_ResultTupleSlot->tts_tupleDescriptor);
		tupdesc = node->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor;

		for (int i = 0; i < tupdesc->natts; i++)
		{
			Form_pg_attribute attr = tupdesc->attrs[i];
			Oid         typid = GetNtype(attr->atttypid);
			if (typid != InvalidOid)
				attr->atttypid = typid;
		}
		ExecSetSlotDescriptor(node->ss.ps.ps_ResultTupleSlot, tupdesc);
	}

	vcs->ps_ResultVTupleSlot = VExecInitExtraTupleSlot(estate);
	vcs->ps_ResultVTupleSlot->tts_tupleDescriptor = CreateTupleDescCopy(outerPlanState(vcs)->ps_ResultTupleSlot->tts_tupleDescriptor);
}

static TupleTableSlot*
FetchRowFromBatch(UnbatchState *ubs)
{
	VectorTupleSlot    *vslot;
	TupleTableSlot	   *slot;
	int					iter;
	int					natts;
	int					i;

	
	slot = ubs->css.ss.ps.ps_ResultTupleSlot;
	vslot = (VectorTupleSlot *)ubs->ps_ResultVTupleSlot;
	iter = ubs->iter;

	while(iter < BATCHSIZE && vslot->skip[iter])
		iter++;
	
	/* we have checked that natts is greater than zero */
	if (iter == BATCHSIZE)
		return NULL;

	ExecClearTuple(slot);
	natts = slot->tts_tupleDescriptor->natts;
	for(i = 0; i < natts; i++)
	{
		slot->tts_values[i] = ((vtype*)(vslot->tts.tts_values[i]))->values[iter];
		slot->tts_isnull[i] = false;
	}

	ubs->iter = ++iter;
	return ExecStoreVirtualTuple(slot);
}

/*
 *
 */
static TupleTableSlot *
ExecUnbatch(CustomScanState *node)
{
	UnbatchState		*ubs;
	TupleTableSlot	   *slot;

	ubs = (UnbatchState*) node;
	/* find a non skip tuple and return to client */
	while(true)
	{
		/* 
		 * iter = 0 indicate we finish unbatching the vector slot
		 * and need to read next vector slot
		 */
		slot = FetchRowFromBatch(ubs);
		if(slot)
			break;
		
		/* finish current batch, read next batch */
		if (!ReadNextVectorSlot(ubs))
			return NULL;
	}

	return slot;
}

static bool
ReadNextVectorSlot(UnbatchState *ubs)
{
	TupleTableSlot		*slot;

	slot = ExecProcNode(ubs->css.ss.ps.lefttree);
	if(TupIsNull(slot))
		return false;

	/* Make sure the tuple is fully deconstructed */
	slot_getallattrs(slot);

	ubs->ps_ResultVTupleSlot = slot;
	ubs->iter = 0;
	return true;
}
/*
 *
 */
static void
EndUnbatch(CustomScanState *node)
{
	PlanState  *outerPlan;
	outerPlan = outerPlanState(node);
	ExecEndNode(outerPlan);
}


static Node *
CreateUnbatchState(CustomScan *custom_plan)
{
	UnbatchState *vss = palloc0(sizeof(UnbatchState));

	NodeSetTag(vss, T_CustomScanState);
	vss->css.methods = &unbatch_exec_methods;

	return (Node *) &vss->css;
}


/*
 * Add unbatch Node at top to make batch to tuple
 */
Plan *
AddUnbatchNodeAtTop(Plan *node)
{
    CustomScan *convert = makeNode(CustomScan);
    convert->methods = &unbatch_methods;
	convert->scan.plan.lefttree = node;
    convert->scan.plan.righttree = NULL;
	return &convert->scan.plan;
}

/*
 * Initialize vectorscan CustomScan node.
 */
void
InitUnbatch(void)
{
    RegisterCustomScanMethods(&unbatch_methods);
}
