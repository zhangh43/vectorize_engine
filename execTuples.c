/*-------------------------------------------------------------------------
 *
 * execTuples.c
 *	  Routines dealing with TupleTableSlots.  These are used for resource
 *	  management associated with tuples (eg, releasing buffer pins for
 *	  tuples in disk buffers, or freeing the memory occupied by transient
 *	  tuples).  Slots also provide access abstraction that lets us implement
 *	  "virtual" tuples to reduce data-copying overhead.
 *
 *	  Routines dealing with the type information for tuples. Currently,
 *	  the type information for a tuple is an array of FormData_pg_attribute.
 *	  This information is needed by routines manipulating tuples
 *	  (getattribute, formtuple, etc.).
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/execTuples.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *
 *	 SLOT CREATION/DESTRUCTION
 *		MakeTupleTableSlot		- create an empty slot
 *		ExecAllocTableSlot		- create a slot within a tuple table
 *		ExecResetTupleTable		- clear and optionally delete a tuple table
 *		MakeSingleTupleTableSlot - make a standalone slot, set its descriptor
 *		ExecDropSingleTupleTableSlot - destroy a standalone slot
 *
 *	 SLOT ACCESSORS
 *		ExecSetSlotDescriptor	- set a slot's tuple descriptor
 *		ExecStoreTuple			- store a physical tuple in the slot
 *		ExecStoreMinimalTuple	- store a minimal physical tuple in the slot
 *		ExecClearTuple			- clear contents of a slot
 *		ExecStoreVirtualTuple	- mark slot as containing a virtual tuple
 *		ExecCopySlotTuple		- build a physical tuple from a slot
 *		ExecCopySlotMinimalTuple - build a minimal physical tuple from a slot
 *		ExecMaterializeSlot		- convert virtual to physical storage
 *		ExecCopySlot			- copy one slot's contents to another
 *
 *	 CONVENIENCE INITIALIZATION ROUTINES
 *		ExecInitResultTupleSlot    \	convenience routines to initialize
 *		ExecInitScanTupleSlot		\	the various tuple slots for nodes
 *		ExecInitExtraTupleSlot		/	which store copies of tuples.
 *		ExecInitNullTupleSlot	   /
 *
 *	 Routines that probably belong somewhere else:
 *		ExecTypeFromTL			- form a TupleDesc from a target list
 *
 *	 EXAMPLE OF HOW TABLE ROUTINES WORK
 *		Suppose we have a query such as SELECT emp.name FROM emp and we have
 *		a single SeqScan node in the query plan.
 *
 *		At ExecutorStart()
 *		----------------
 *		- ExecInitSeqScan() calls ExecInitScanTupleSlot() and
 *		  ExecInitResultTupleSlot() to construct TupleTableSlots
 *		  for the tuples returned by the access methods and the
 *		  tuples resulting from performing target list projections.
 *
 *		During ExecutorRun()
 *		----------------
 *		- SeqNext() calls ExecStoreTuple() to place the tuple returned
 *		  by the access methods into the scan tuple slot.
 *
 *		- ExecSeqScan() calls ExecStoreTuple() to take the result
 *		  tuple from ExecProject() and place it into the result tuple slot.
 *
 *		- ExecutePlan() calls the output function.
 *
 *		The important thing to watch in the executor code is how pointers
 *		to the slots containing tuples are passed instead of the tuples
 *		themselves.  This facilitates the communication of related information
 *		(such as whether or not a tuple should be pfreed, what buffer contains
 *		this tuple, the tuple's tuple descriptor, etc).  It also allows us
 *		to avoid physically constructing projection tuples in many cases.
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/tuptoaster.h"
#include "funcapi.h"
#include "catalog/pg_type.h"
#include "nodes/nodeFuncs.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"

#include "execTuples.h"
#include "vtype/vtype.h"
#include "utils.h"
#include "vectorTupleSlot.h"

/* static vectorized functions */
static void VExecAssignResultType(PlanState *planstate, TupleDesc tupDesc);


/* ----------------
 *		VExecInitResultTupleSlot
 * ----------------
 */
void
VExecInitResultTupleSlot(EState *estate, PlanState *planstate)
{
	planstate->ps_ResultTupleSlot = VExecAllocTableSlot(&estate->es_tupleTable);
}

/* ----------------
 *		VExecInitScanTupleSlot
 * ----------------
 */
void
VExecInitScanTupleSlot(EState *estate, ScanState *scanstate)
{
	scanstate->ss_ScanTupleSlot = VExecAllocTableSlot(&estate->es_tupleTable);
}

/* ----------------
 *		VExecInitExtraTupleSlot
 * ----------------
 */
TupleTableSlot *
VExecInitExtraTupleSlot(EState *estate)
{
	return VExecAllocTableSlot(&estate->es_tupleTable);
}


/* ----------------
 *		VExecAssignResultTypeFromTL
 * ----------------
 */
void
VExecAssignResultTypeFromTL(PlanState *planstate)
{
	bool		hasoid;
	TupleDesc	tupDesc;

	if (ExecContextForcesOids(planstate, &hasoid))
	{
		/* context forces OID choice; hasoid is now set correctly */
	}
	else
	{
		/* given free choice, don't leave space for OIDs in result tuples */
		hasoid = false;
	}

	/*
	 * ExecTypeFromTL needs the parse-time representation of the tlist, not a
	 * list of ExprStates.  This is good because some plan nodes don't bother
	 * to set up planstate->targetlist ...
	 */
	tupDesc = ExecTypeFromTL(planstate->plan->targetlist, hasoid);
	VExecAssignResultType(planstate, tupDesc);
}


/* ----------------
 *		VExecAssignResultType
 * ----------------
 */
static void
VExecAssignResultType(PlanState *planstate, TupleDesc tupDesc)
{
	TupleTableSlot	*slot;
	
	slot = planstate->ps_ResultTupleSlot;

	ExecSetSlotDescriptor(slot, tupDesc);

	InitializeVectorSlotColumn((VectorTupleSlot *)slot);
}
