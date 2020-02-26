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

extern TupleTableSlot *VMakeTupleTableSlot(TupleDesc tupDesc);
extern TupleTableSlot *VExecAllocTableSlot(List **tupleTable, TupleDesc tupDesc);

extern void VExecStoreColumns(TupleTableSlot *dst_slot, TupleTableSlot *src_slot, int natts);
extern TupleTableSlot *VExecClearTuple(TupleTableSlot *slot);

#endif
