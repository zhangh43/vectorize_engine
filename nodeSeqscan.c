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
	HeapTuple	tuple;
	HeapScanDesc scandesc;
	EState	   *estate;
	ScanDirection direction;
	TupleTableSlot *slot;
	VectorTupleSlot	*vslot;
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
		scandesc = heap_beginscan(node->ss.ss_currentRelation,
								  estate->es_snapshot,
								  0, NULL);
		node->ss.ss_currentScanDesc = scandesc;
	}

	vslot = (VectorTupleSlot *)slot;

	/* return the last batch. */
	if (vss->scanFinish)
	{
		VExecClearTuple(slot);
		return slot;
	}
	VExecClearTuple(slot);

	/* fetch a batch of rows and fill them into VectorTupleSlot */
	for (row = 0 ; row < BATCHSIZE; row++)
	{
		/*
		 * get the next tuple from the table
		 */
		tuple = heap_getnext(scandesc, direction);

		/*
		 * save the tuple and the buffer returned to us by the access methods in
		 * our scan tuple slot and return the slot.  Note: we pass 'false' because
		 * tuples returned by heap_getnext() are pointers onto disk pages and were
		 * not created with palloc() and so should not be pfree()'d.  Note also
		 * that ExecStoreTuple will increment the refcount of the buffer; the
		 * refcount will not be dropped until the tuple table slot is cleared.
		 */
		if (tuple)
			VExecStoreTuple(tuple,	/* tuple to store */
					slot,	/* slot to store in */
					scandesc->rs_cbuf,		/* buffer associated with this
											 * tuple */
					false);	/* don't pfree this pointer */
		else
		{
			/* scan finish, but we still need to emit current vslot */
			vss->scanFinish = true;
			break;
		}
	}

	/*
	 * deform and generate virtual tuple
	 * TODO: late deform to avoid deform unneccessary columns.
	 */
	if (row > 0)
	{
		vslot->dim = row;
		memset(vslot->skip, false, sizeof(bool) * row);
		
		/* deform the vector slot now */
		Vslot_getallattrs(slot);
		ExecStoreVirtualTuple(slot);
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
	Relation	currentRelation;
	TupleDesc	vdesc;
	TupleTableSlot *slot;
	int 		i;

	/*
	 * get the relation object id from the relid'th entry in the range table,
	 * open that relation and acquire appropriate lock on it.
	 */
	currentRelation = ExecOpenScanRelation(estate,
								   ((SeqScan *) node->ss.ps.plan)->scanrelid,
										   eflags);

	node->ss.ss_currentRelation = currentRelation;

	/* 
	 * since we will change the attr type of tuple desc to vector
	 * type. we need to copy it to avoid dirty the relcache
	 */
	vdesc = CreateTupleDescCopyConstr(RelationGetDescr(currentRelation));

	/* change the attr type of tuple desc to vector type */
	for (i = 0; i < vdesc->natts; i++)
	{
		Form_pg_attribute	attr = vdesc->attrs[i];
		Oid					vtypid = GetVtype(attr->atttypid);
		if (vtypid != InvalidOid)
			attr->atttypid = vtypid;
		else
			elog(ERROR, "cannot find vectorized type for type %d",
					attr->atttypid);
	}

	/* and report the scan tuple slot's rowtype */
	ExecAssignScanType(&node->ss, vdesc);

	slot = node->ss.ss_ScanTupleSlot;
	InitializeVectorSlotColumn((VectorTupleSlot *)slot);
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

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &scanstate->ss.ps);

	/*
	 * initialize child expressions
	 */
	scanstate->ss.ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->plan.targetlist,
					 (PlanState *) scanstate);
	scanstate->ss.ps.qual = (List *)
		ExecInitExpr((Expr *) node->plan.qual,
					 (PlanState *) scanstate);

	/*
	 * tuple table initialization
	 */
	VExecInitResultTupleSlot(estate, &scanstate->ss.ps);
	VExecInitScanTupleSlot(estate, &scanstate->ss);

	/*
	 * initialize scan relation
	 */
	VInitScanRelation(scanstate, estate, eflags);

	scanstate->ss.ps.ps_TupFromTlist = false;

	/*
	 * Initialize result tuple type and projection info.
	 */
	VExecAssignResultTypeFromTL(&scanstate->ss.ps);
	ExecAssignScanProjectionInfo(&scanstate->ss);

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
	Relation	relation;
	HeapScanDesc scanDesc;
	SeqScanState *node = vss->seqstate;

	/*
	 * get information from node
	 */
	relation = node->ss.ss_currentRelation;
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

	/*
	 * close the heap relation.
	 */
	ExecCloseScanRelation(relation);
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
