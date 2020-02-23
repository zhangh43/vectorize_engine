/*-------------------------------------------------------------------------
 *
 * nodeSeqscan.c
 *	  Support routines for sequential scans of relations.
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeSeqscan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecSeqScan				sequentially scans a relation.
 *		ExecSeqNext				retrieve next tuple in sequential order.
 *		ExecInitSeqScan			creates and initializes a seqscan node.
 *		ExecEndSeqScan			releases any storage allocated.
 *		ExecReScanSeqScan		rescans the relation
 *
 *		ExecSeqScanEstimate		estimates DSM space needed for parallel scan
 *		ExecSeqScanInitializeDSM initialize DSM for parallel scan
 *		ExecSeqScanInitializeWorker attach to DSM info in parallel worker
 */
#include "postgres.h"

#include "access/relscan.h"
#include "access/heapam.h"
#include "executor/execdebug.h"
#include "executor/nodeSeqscan.h"
#include "utils/rel.h"

/*-------------------------- Vectorize part of nodeSeqScan ---------------------------------*/
#include "nodes/extensible.h"
#include "executor/nodeCustom.h"
#include "utils/memutils.h"

#include "executor.h"
#include "execTuples.h"
#include "nodeSeqscan.h"
#include "vectorTupleSlot.h"
#include "utils.h"
#include "vtype/vtype.h"


/* CustomScanMethods */
static Node *CreateVectorScanState(CustomScan *custom_plan);

/* CustomScanExecMethods */
static void BeginVectorScan(CustomScanState *node, EState *estate, int eflags);
static void ReScanVectorScan(CustomScanState *node);
static TupleTableSlot *ExecVectorScan(CustomScanState *node);
static void EndVectorScan(CustomScanState *node);

static SeqScanState *VExecInitSeqScan(SeqScan *node, EState *estate, int eflags);
static TupleTableSlot *VExecSeqScan(VectorScanState *vss);
static void VExecEndSeqScan(VectorScanState *vss);
static void VExecReScanSeqScan(VectorScanState *vss);

static void VInitScanRelation(SeqScanState *node, EState *estate, int eflags);
static TupleTableSlot *VSeqNext(VectorScanState *vss);
static bool VSeqRecheck(VectorScanState *node, TupleTableSlot *slot);

static CustomScanMethods	vectorscan_scan_methods = {
	"vectorscan",			/* CustomName */
	CreateVectorScanState,	/* CreateCustomScanState */
};

static CustomExecMethods	vectorscan_exec_methods = {
	"vectorscan",			/* CustomName */
	BeginVectorScan,		/* BeginCustomScan */
	ExecVectorScan,			/* ExecCustomScan */
	EndVectorScan,			/* EndCustomScan */
	ReScanVectorScan,		/* ReScanCustomScan */
	NULL,					/* MarkPosCustomScan */
	NULL,					/* RestrPosCustomScan */
	NULL,					/* EstimateDSMCustomScan */
	NULL,					/* InitializeDSMCustomScan */
	NULL,					/* InitializeWorkerCustomScan */
	NULL,					/* ExplainCustomScan */
};


/*
 * CreateVectorScanState - A method of CustomScan; that populate a custom
 * object being delivered from CustomScanState type, according to the
 * supplied CustomPath object.
 *
 */
static Node *
CreateVectorScanState(CustomScan *custom_plan)
{
	VectorScanState   *vss = MemoryContextAllocZero(CurTransactionContext,
													sizeof(VectorScanState));
	/* Set tag and executor callbacks */
	NodeSetTag(vss, T_CustomScanState);

	vss->css.methods = &vectorscan_exec_methods;
	if (custom_plan->custom_private)
		vss->maxVarattno = linitial_int(custom_plan->custom_private);
	return (Node *) vss;
}

/*
 * BeginVectorScan - A method of CustomScanState; that initializes
 * the supplied VectorScanState object, at beginning of the executor.
 *
 */
static void
BeginVectorScan(CustomScanState *css, EState *estate, int eflags)
{
	VectorScanState *vss;
	CustomScan  *cscan;
	SeqScan		*node;

	/* clear state initialized in ExecInitCustomScan */
	ClearCustomScanState(css);

	cscan = (CustomScan *)css->ss.ps.plan;
	node = (SeqScan *)linitial(cscan->custom_plans);

	vss = (VectorScanState*)css;
	vss->scanFinish = false;

	vss->seqstate = VExecInitSeqScan(node, estate, eflags);
	Assert(vss->seqstate->ss.ss_currentRelation != InvalidOid);
	vss->slot = ExecAllocTableSlot(&estate->es_tupleTable,
								   RelationGetDescr(vss->seqstate->ss.ss_currentRelation),
								   table_slot_callbacks(vss->seqstate->ss.ss_currentRelation));

	vss->css.ss.ps.ps_ResultTupleSlot = vss->seqstate->ss.ps.ps_ResultTupleSlot;
}

/*
 * ReScanVectorScan - A method of CustomScanState; that rewind the current
 * seek position.
 *
 * Derived from ExecReScanSeqScan().
 */
static void
ReScanVectorScan(CustomScanState *node)
{
	VExecReScanSeqScan((VectorScanState *)node);
}



/*
 * ExecVectorScan - A method of CustomScanState; that fetches a tuple
 * from the relation, if exist anymore.
 *
 * Derived from ExecSeqScan().
 */
static TupleTableSlot *
ExecVectorScan(CustomScanState *node)
{
	return VExecSeqScan((VectorScanState *)node);
}

/*
 * CTidEndCustomScan - A method of CustomScanState; that closes heap and
 * scan descriptor, and release other related resources.
 *
 * Derived from ExecEndSeqScan().
 */
static void
EndVectorScan(CustomScanState *node)
{
	VExecEndSeqScan((VectorScanState *)node);
}

/*
 * Interface to get the custom scan plan for vector scan
 */
CustomScan *
MakeCustomScanForSeqScan(void)
{
	CustomScan *cscan = (CustomScan *)makeNode(CustomScan);
	cscan->methods = &vectorscan_scan_methods;

	return cscan;
}

/*
 * Initialize vectorscan CustomScan node.
 */
void
InitVectorScan(void)
{
	/* Register a vscan type of custom scan node */
	RegisterCustomScanMethods(&vectorscan_scan_methods);
}

/*---------------------- End of vectorized part of nodeSeqscan ---------------------------*/


/* ----------------------------------------------------------------
 *						Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		SeqNext
 *
 *		This is a workhorse for ExecSeqScan
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
VSeqNext(VectorScanState *vss)
{
	TableScanDesc scandesc;
	EState	   *estate;
	ScanDirection direction;
	TupleTableSlot *slot;
	int				row;
	SeqScanState	*node = vss->seqstate;

	/*
	 * get information from the estate and scan state
	 */
	scandesc = node->ss.ss_currentScanDesc;
	estate = node->ss.ps.state;
	direction = estate->es_direction;
	slot = node->ss.ss_ScanTupleSlot;

	if (scandesc == NULL)
	{
		/*
		 * We reach here if the scan is not parallel, or if we're serially
		 * executing a scan that was planned to be parallel.
		 */
		scandesc = table_beginscan(node->ss.ss_currentRelation,
								   estate->es_snapshot,
								   0, NULL);
		node->ss.ss_currentScanDesc = scandesc;
	}

	VExecClearTuple(slot);

	/* return the last batch. */
	if (!vss->scanFinish)
	{
		int natts = vss->maxVarattno ? vss->maxVarattno : vss->slot->tts_tupleDescriptor->natts;
		/* fetch a batch of rows and fill them into VectorTupleSlot */
		for (row = 0; row < BATCHSIZE; row++)
		{
			/*
			 * get the next tuple from the table
			 */
			if (table_scan_getnextslot(scandesc, direction, vss->slot))
			{
				slot_getsomeattrs(vss->slot, natts);
				VExecStoreColumns(slot, vss->slot, natts);
			}
			else
			{
				/* scan finish, but we still need to emit current slot */
				vss->scanFinish = true;
				break;
			}
		}
		if (row > 0)
		{
			ExecStoreVirtualTuple(slot);
		}
	}
	return slot;
}

/*
 * SeqRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool
VSeqRecheck(VectorScanState *node, TupleTableSlot *slot)
{
	/*
	 * Note that unlike IndexScan, SeqScan never use keys in heap_beginscan
	 * (and this is very bad) - so, here we do not check are keys ok or not.
	 */
	return true;
}

/* ----------------------------------------------------------------
 *		ExecSeqScan(node)
 *
 *		Scans the relation sequentially and returns the next qualifying
 *		tuple.
 *		We call the ExecScan() routine and pass it the appropriate
 *		access method functions.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
VExecSeqScan(VectorScanState *node)
{
	return VExecScan(node,
					 (VExecScanAccessMtd) VSeqNext,
					 (VExecScanRecheckMtd) VSeqRecheck);
}

/* ----------------------------------------------------------------
 *		InitScanRelation
 *
 *		Set up to access the scan relation.
 * ----------------------------------------------------------------
 */
static void
VInitScanRelation(SeqScanState *node, EState *estate, int eflags)
{
	TupleDesc	vdesc;
	int 		i;

	/*
	 * open the scan relation
	 */
	node->ss.ss_currentRelation =
		ExecOpenScanRelation(estate,
							 ((SeqScan *) node->ss.ps.plan)->scanrelid,
							 eflags);

	/*
	 * since we will change the attr type of tuple desc to vector
	 * type. we need to copy it to avoid dirty the relcache
	 */
	vdesc = CreateTupleDescCopy(RelationGetDescr(node->ss.ss_currentRelation));
	/* change the attr type of tuple desc to vector type */
	for (i = 0; i < vdesc->natts; i++)
	{
		Form_pg_attribute	attr = TupleDescAttr(vdesc, i);
		Oid					vtypid = GetVtype(attr->atttypid);
		if (vtypid != InvalidOid)
			attr->atttypid = vtypid;
		else
			elog(ERROR, "cannot find vectorized type for type %d",
					attr->atttypid);
	}
	VExecInitScanTupleSlot(estate, &node->ss, vdesc);
}

/* ----------------------------------------------------------------
 *		ExecInitSeqScan
 * ----------------------------------------------------------------
 */
static SeqScanState *
VExecInitSeqScan(SeqScan *node, EState *estate, int eflags)
{
	SeqScanState *scanstate;

	/*
	 * Once upon a time it was possible to have an outerPlan of a SeqScan, but
	 * not any more.
	 */
	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create state structure
	 */
	scanstate = makeNode(SeqScanState);
	scanstate->ss.ps.plan = (Plan *) node;
	scanstate->ss.ps.state = estate;
	scanstate->ss.ps.ExecProcNode = (ExecProcNodeMtd)VExecSeqScan;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &scanstate->ss.ps);

	/*
	 * initialize scan relation
	 */
	VInitScanRelation(scanstate, estate, eflags);

	/*
	 * Initialize result type and projection.
	 */
	ExecInitResultTypeTL(&scanstate->ss.ps);

	/*
	 * tuple table initialization
	 */
	VExecInitResultTupleSlot(estate, &scanstate->ss.ps);

	ExecAssignScanProjectionInfo(&scanstate->ss);

	/*
	 * initialize child expressions
	 */
	scanstate->ss.ps.qual =
		ExecInitQual(node->plan.qual,
					 (PlanState *) scanstate);

	return scanstate;
}

/* ----------------------------------------------------------------
 *		ExecEndSeqScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
static void
VExecEndSeqScan(VectorScanState *vss)
{
	TableScanDesc scanDesc;
	SeqScanState *node = vss->seqstate;

	/*
	 * get information from node
	 */
	scanDesc = node->ss.ss_currentScanDesc;

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->ss.ps);

	/*
	 * clean out the tuple table
	 */
	VExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	VExecClearTuple(node->ss.ss_ScanTupleSlot);

	/*
	 * close heap scan
	 */
	if (scanDesc != NULL)
		heap_endscan(scanDesc);
}

/* ----------------------------------------------------------------
 *						Join Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecReScanSeqScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
static void
VExecReScanSeqScan(VectorScanState *vss)
{
	elog(ERROR, "vectorize rescan not implemented yet.");
}
