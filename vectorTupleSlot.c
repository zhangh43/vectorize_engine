/*-------------------------------------------------------------------------
 *
 * vectorTupleSlot.c
 *
 * Copyright (c) 1996-2019, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/sysattr.h"
#include "executor/tuptable.h"
#include "utils/expandeddatum.h"

#include "vectorTupleSlot.h"


/* --------------------------------
 *		VMakeTupleTableSlot
 *
 *		Basic routine to make an empty VectorTupleTableSlot.
 * --------------------------------
 */
TupleTableSlot *
VMakeTupleTableSlot(TupleDesc tupleDesc)
{
	TupleTableSlot	*slot;
	VectorTupleSlot	*vslot;
	int              i;
	Oid	             typid;
	vtype	        *column;
	static TupleTableSlotOps tts_ops;

	tts_ops = TTSOpsVirtual;
	tts_ops.base_slot_size = sizeof(VectorTupleSlot);

	slot = MakeTupleTableSlot(tupleDesc, &tts_ops);

	/* vectorized fields */
	vslot = (VectorTupleSlot*)slot;
	/* all tuples should be skipped in initialization */
	memset(vslot->skip, true, sizeof(vslot->skip));

	for (i = 0; i < tupleDesc->natts; i++)
	{
		typid = TupleDescAttr(tupleDesc, i)->atttypid;
		column = buildvtype(typid, BATCHSIZE, vslot->skip);
		column->dim = 0;
		vslot->tts.base.tts_values[i] = PointerGetDatum(column);
		/* tts_isnull not used yet */
		vslot->tts.base.tts_isnull[i] = false;
	}

	return slot;
}

/* --------------------------------
 *		VExecAllocTableSlot
 *
 *		Create a vector tuple table slot within a tuple table (which is just a List).
 * --------------------------------
 */
TupleTableSlot *
VExecAllocTableSlot(List **tupleTable, TupleDesc desc)
{
	TupleTableSlot *slot = VMakeTupleTableSlot(desc);

	*tupleTable = lappend(*tupleTable, slot);

	return slot;
}

/* --------------------------------
 *		VExecClearTuple
 *
 *		This function is used to clear out a slot in the tuple table.
 *
 *		NB: only the tuple is cleared, not the tuple descriptor (if any).
 * --------------------------------
 */
TupleTableSlot *
VExecClearTuple(TupleTableSlot *slot)	/* slot in which to store tuple */
{
	VectorTupleSlot *vslot;

	if (slot == NULL)
		return NULL;

	ExecClearTuple(slot);
	vslot = (VectorTupleSlot *)slot;
	vslot->dim = 0;
	return slot;
}


/* --------------------------------
 *		ExecStoreTuple
 *
 *		This function is used to store a physical tuple into a specified
 *		slot in the tuple table.
 *
 *		tuple:	tuple to store
 *		slot:	slot to store it in
 *		buffer: disk buffer if tuple is in a disk page, else InvalidBuffer
 *		shouldFree: true if ExecClearTuple should pfree() the tuple
 *					when done with it
 *
 * If 'buffer' is not InvalidBuffer, the tuple table code acquires a pin
 * on the buffer which is held until the slot is cleared, so that the tuple
 * won't go away on us.
 *
 * shouldFree is normally set 'true' for tuples constructed on-the-fly.
 * It must always be 'false' for tuples that are stored in disk pages,
 * since we don't want to try to pfree those.
 *
 * Another case where it is 'false' is when the referenced tuple is held
 * in a tuple table slot belonging to a lower-level executor Proc node.
 * In this case the lower-level slot retains ownership and responsibility
 * for eventually releasing the tuple.  When this method is used, we must
 * be certain that the upper-level Proc node will lose interest in the tuple
 * sooner than the lower-level one does!  If you're not certain, copy the
 * lower-level tuple with heap_copytuple and let the upper-level table
 * slot assume ownership of the copy!
 *
 * Return value is just the passed-in slot pointer.
 *
 * NOTE: before PostgreSQL 8.1, this function would accept a NULL tuple
 * pointer and effectively behave like ExecClearTuple (though you could
 * still specify a buffer to pin, which would be an odd combination).
 * This saved a couple lines of code in a few places, but seemed more likely
 * to mask logic errors than to be really useful, so it's now disallowed.
 * --------------------------------
 */
void
VExecStoreColumns(TupleTableSlot *dst_slot, TupleTableSlot *src_slot, int natts)
{
	VectorTupleSlot *vslot;
	int i, row;

	vslot = (VectorTupleSlot *)dst_slot;
	row = vslot->dim++;
	Assert(row < BATCHSIZE);

	for (i = 0; i < natts; i++)
	{
		vtype* column = (vtype *)dst_slot->tts_values[i];
		column->isnull[row] = src_slot->tts_isnull[i];
		column->values[row] = src_slot->tts_values[i];
		column->dim = row + 1;
	}
	vslot->skip[row] = false;
	dst_slot->tts_nvalid = natts;
}
