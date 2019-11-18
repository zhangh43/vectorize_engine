/*-------------------------------------------------------------------------
 *
 * vectortTupleSlot.h
 *	  vector tuple table support stuff
 *
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef VECTOR_TUPLE_SLOT_H
#define VECTOR_TUPLE_SLOT_H

#include "executor/tuptable.h"
#include "storage/bufmgr.h"

#include "vtype/vtype.h"
/*
 * VectorTupleSlot store a batch of tuples in each slot.
 */
typedef struct VectorTupleSlot
{
	TupleTableSlot	tts;
	/* how many tuples does this slot contain */ 
	int32			dim;
	int32			bufnum;

	/* batch of physical tuples */
	HeapTupleData	tts_tuples[BATCHSIZE];
	/*
	 * tuples in slot would across many heap blocks,
	 * we need pin these buffers if needed.
	 */
	Buffer			tts_buffers[BATCHSIZE];
	/* skip array to represent filtered tuples */
	bool			skip[BATCHSIZE];
} VectorTupleSlot;

/* vector tuple slot related interface */

extern TupleTableSlot *VMakeTupleTableSlot(void);
extern TupleTableSlot *VExecAllocTableSlot(List **tupleTable);
extern void InitializeVectorSlotColumn(VectorTupleSlot *vslot);

extern TupleTableSlot *VExecStoreTuple(HeapTuple tuple,
			   TupleTableSlot *slot,
			   Buffer buffer,
			   bool shouldFree);
extern TupleTableSlot *VExecClearTuple(TupleTableSlot *slot);

extern void Vslot_getsomeattrs(TupleTableSlot *slot, int attnum);
extern void Vslot_getallattrs(TupleTableSlot *slot);

#endif
