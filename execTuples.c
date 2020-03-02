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
 *
 *	 EXAMPLE OF HOW TABLE ROUTINES WORK
 *		Suppose we have a query such as SELECT emp.name FROM emp and we have
 *		a single SeqScan node in the query plan.
 *
 *		At ExecutorStart()
 *		----------------
 *
 *		- ExecInitSeqScan() calls ExecInitScanTupleSlot() to construct a
 *		  TupleTableSlots for the tuples returned by the access method, and
 *		  ExecInitResultTypeTL() to define the node's return
 *		  type. ExecAssignScanProjectionInfo() will, if necessary, create
 *		  another TupleTableSlot for the tuples resulting from performing
 *		  target list projections.
 *
 *		During ExecutorRun()
 *		----------------
 *		- SeqNext() calls ExecStoreBufferHeapTuple() to place the tuple
 *		  returned by the access method into the scan tuple slot.
 *
 *		- ExecSeqScan() (via ExecScan), if necessary, calls ExecProject(),
 *		  putting the result of the projection in the result tuple slot. If
 *		  not necessary, it directly returns the slot returned by SeqNext().
 *
 *		- ExecutePlan() calls the output function.
 *
 *		The important thing to watch in the executor code is how pointers
 *		to the slots containing tuples are passed instead of the tuples
 *		themselves.  This facilitates the communication of related information
 *		(such as whether or not a tuple should be pfreed, what buffer contains
 *		this tuple, the tuple's tuple descriptor, etc).  It also allows us
 *		to avoid physically constructing projection tuples in many cases.
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/execTuples.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heaptoast.h"
#include "access/htup_details.h"
#include "access/tupdesc_details.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "nodes/nodeFuncs.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
#include "utils/expandeddatum.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"

#include "executor.h"
#include "tuptable.h"
#include "utils.h"

static pg_attribute_always_inline void vslot_deform_heap_tuple(TupleTableSlot *slot, HeapTuple tuple, uint32 *offp,
															  int natts);

static pg_attribute_always_inline void vector_slot_deform_heap_tuple(TupleTableSlot *slot,
																  int natts);

static inline void tts_buffer_heap_store_tuple(TupleTableSlot *slot,
											   HeapTuple tuple,
											   Buffer buffer,
											   bool transfer_pin);

static inline void tts_vector_buffer_heap_store_tuple(TupleTableSlot *slot,
											   HeapTuple tuple,
											   Buffer buffer,
											   bool transfer_pin);

const TupleTableSlotOps TTSOpsVectorVirtual;
const TupleTableSlotOps TTSOpsVectorHeapTuple;
const TupleTableSlotOps TTSOpsVectorMinimalTuple;
const TupleTableSlotOps TTSOpsVectorBufferHeapTuple;


/*
 * TupleTableSlotOps implementations.
 */

/*
 * TupleTableSlotOps implementation for VirtualTupleTableSlot.
 */
static void
vector_tts_virtual_init(TupleTableSlot *slot)
{
	VectorTupleTableSlot *vslot = (VectorTupleTableSlot *) slot;
	TupleDesc	vdesc;
	vtype		*column;
	int			i;

	vslot->dim = 0;

	memset(vslot->skip, false, sizeof(bool) *BATCHSIZE );

    /*
     * since we will change the attr type of tuple desc to vector
     * type. we need to copy it to avoid dirty the relcache
     */
    vdesc = CreateTupleDescCopyConstr(slot->tts_tupleDescriptor);
	vslot->vector_tts_tupleDescriptor = vdesc;

	vslot->vector_tts_values = palloc(vdesc->natts * sizeof(Datum));

    /* change the attr type of tuple desc to vector type */
    for (i = 0; i < vdesc->natts; i++)
    {
        Form_pg_attribute   attr = TupleDescAttr(vdesc, i);
        Oid                 vtypid = GetVtype(attr->atttypid);
        if (vtypid != InvalidOid)
            attr->atttypid = vtypid;
        else
            elog(ERROR, "cannot find vectorized type for type %d",
                    attr->atttypid);
	
		/* fill vector_tts_values */
		column = buildvtype(vtypid, BATCHSIZE, vslot->skip);
		column->dim = 0;
		vslot->vector_tts_values[i]  = PointerGetDatum(column);
    }
}

static void
tts_virtual_release(TupleTableSlot *slot)
{
}

static void
vector_tts_virtual_clear(TupleTableSlot *slot)
{
	if (unlikely(TTS_SHOULDFREE(slot)))
	{
		VirtualTupleTableSlot *vslot = (VirtualTupleTableSlot *) slot;

		pfree(vslot->data);
		vslot->data = NULL;

		slot->tts_flags &= ~TTS_FLAG_SHOULDFREE;
	}

	slot->tts_nvalid = 0;
	slot->tts_flags |= TTS_FLAG_EMPTY;
	ItemPointerSetInvalid(&slot->tts_tid);
}

/*
 * Attribute values are readily available in tts_values and tts_isnull array
 * in a VirtualTupleTableSlot. So there should be no need to call either of the
 * following two functions.
 */
static void
vector_tts_virtual_getsomeattrs(TupleTableSlot *slot, int natts)
{
	elog(ERROR, "getsomeattrs is not required to be called on a virtual tuple table slot");
}

static Datum
tts_virtual_getsysattr(TupleTableSlot *slot, int attnum, bool *isnull)
{
	elog(ERROR, "virtual tuple table slot does not have system attributes");

	return 0;					/* silence compiler warnings */
}

/*
 * To materialize a virtual slot all the datums that aren't passed by value
 * have to be copied into the slot's memory context.  To do so, compute the
 * required size, and allocate enough memory to store all attributes.  That's
 * good for cache hit ratio, but more importantly requires only memory
 * allocation/deallocation.
 */
static void
tts_virtual_materialize(TupleTableSlot *slot)
{
	VirtualTupleTableSlot *vslot = (VirtualTupleTableSlot *) slot;
	TupleDesc	desc = slot->tts_tupleDescriptor;
	Size		sz = 0;
	char	   *data;

	/* already materialized */
	if (TTS_SHOULDFREE(slot))
		return;

	/* compute size of memory required */
	for (int natt = 0; natt < desc->natts; natt++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, natt);
		Datum		val;

		if (att->attbyval || slot->tts_isnull[natt])
			continue;

		val = slot->tts_values[natt];

		if (att->attlen == -1 &&
			VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(val)))
		{
			/*
			 * We want to flatten the expanded value so that the materialized
			 * slot doesn't depend on it.
			 */
			sz = att_align_nominal(sz, att->attalign);
			sz += EOH_get_flat_size(DatumGetEOHP(val));
		}
		else
		{
			sz = att_align_nominal(sz, att->attalign);
			sz = att_addlength_datum(sz, att->attlen, val);
		}
	}

	/* all data is byval */
	if (sz == 0)
		return;

	/* allocate memory */
	vslot->data = data = MemoryContextAlloc(slot->tts_mcxt, sz);
	slot->tts_flags |= TTS_FLAG_SHOULDFREE;

	/* and copy all attributes into the pre-allocated space */
	for (int natt = 0; natt < desc->natts; natt++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, natt);
		Datum		val;

		if (att->attbyval || slot->tts_isnull[natt])
			continue;

		val = slot->tts_values[natt];

		if (att->attlen == -1 &&
			VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(val)))
		{
			Size		data_length;

			/*
			 * We want to flatten the expanded value so that the materialized
			 * slot doesn't depend on it.
			 */
			ExpandedObjectHeader *eoh = DatumGetEOHP(val);

			data = (char *) att_align_nominal(data,
											  att->attalign);
			data_length = EOH_get_flat_size(eoh);
			EOH_flatten_into(eoh, data, data_length);

			slot->tts_values[natt] = PointerGetDatum(data);
			data += data_length;
		}
		else
		{
			Size		data_length = 0;

			data = (char *) att_align_nominal(data, att->attalign);
			data_length = att_addlength_datum(data_length, att->attlen, val);

			memcpy(data, DatumGetPointer(val), data_length);

			slot->tts_values[natt] = PointerGetDatum(data);
			data += data_length;
		}
	}
}

static void
tts_virtual_copyslot(TupleTableSlot *dstslot, TupleTableSlot *srcslot)
{
	TupleDesc	srcdesc = srcslot->tts_tupleDescriptor;

	Assert(srcdesc->natts <= dstslot->tts_tupleDescriptor->natts);

	vector_tts_virtual_clear(dstslot);

	slot_getallattrs(srcslot);

	for (int natt = 0; natt < srcdesc->natts; natt++)
	{
		dstslot->tts_values[natt] = srcslot->tts_values[natt];
		dstslot->tts_isnull[natt] = srcslot->tts_isnull[natt];
	}

	dstslot->tts_nvalid = srcdesc->natts;
	dstslot->tts_flags &= ~TTS_FLAG_EMPTY;

	/* make sure storage doesn't depend on external memory */
	tts_virtual_materialize(dstslot);
}

static HeapTuple
tts_virtual_copy_heap_tuple(TupleTableSlot *slot)
{
	Assert(!TTS_EMPTY(slot));

	return heap_form_tuple(slot->tts_tupleDescriptor,
						   slot->tts_values,
						   slot->tts_isnull);
}

static MinimalTuple
tts_virtual_copy_minimal_tuple(TupleTableSlot *slot)
{
	Assert(!TTS_EMPTY(slot));

	return heap_form_minimal_tuple(slot->tts_tupleDescriptor,
								   slot->tts_values,
								   slot->tts_isnull);
}


/*
 * TupleTableSlotOps implementation for HeapTupleTableSlot.
 */

static void
tts_heap_init(TupleTableSlot *slot)
{
}

static void
tts_heap_release(TupleTableSlot *slot)
{
}

static void
tts_heap_clear(TupleTableSlot *slot)
{
	HeapTupleTableSlot *hslot = (HeapTupleTableSlot *) slot;

	/* Free the memory for the heap tuple if it's allowed. */
	if (TTS_SHOULDFREE(slot))
	{
		heap_freetuple(hslot->tuple);
		slot->tts_flags &= ~TTS_FLAG_SHOULDFREE;
	}

	slot->tts_nvalid = 0;
	slot->tts_flags |= TTS_FLAG_EMPTY;
	ItemPointerSetInvalid(&slot->tts_tid);
	hslot->off = 0;
	hslot->tuple = NULL;
}

static void
tts_heap_getsomeattrs(TupleTableSlot *slot, int natts)
{
	HeapTupleTableSlot *hslot = (HeapTupleTableSlot *) slot;

	Assert(!TTS_EMPTY(slot));

	vslot_deform_heap_tuple(slot, hslot->tuple, &hslot->off, natts);
}

static Datum
tts_heap_getsysattr(TupleTableSlot *slot, int attnum, bool *isnull)
{
	HeapTupleTableSlot *hslot = (HeapTupleTableSlot *) slot;

	Assert(!TTS_EMPTY(slot));

	return heap_getsysattr(hslot->tuple, attnum,
						   slot->tts_tupleDescriptor, isnull);
}

static void
tts_heap_materialize(TupleTableSlot *slot)
{
	HeapTupleTableSlot *hslot = (HeapTupleTableSlot *) slot;
	MemoryContext oldContext;

	Assert(!TTS_EMPTY(slot));

	/* If slot has its tuple already materialized, nothing to do. */
	if (TTS_SHOULDFREE(slot))
		return;

	oldContext = MemoryContextSwitchTo(slot->tts_mcxt);

	/*
	 * Have to deform from scratch, otherwise tts_values[] entries could point
	 * into the non-materialized tuple (which might be gone when accessed).
	 */
	slot->tts_nvalid = 0;
	hslot->off = 0;

	if (!hslot->tuple)
		hslot->tuple = heap_form_tuple(slot->tts_tupleDescriptor,
									   slot->tts_values,
									   slot->tts_isnull);
	else
	{
		/*
		 * The tuple contained in this slot is not allocated in the memory
		 * context of the given slot (else it would have TTS_SHOULDFREE set).
		 * Copy the tuple into the given slot's memory context.
		 */
		hslot->tuple = heap_copytuple(hslot->tuple);
	}

	slot->tts_flags |= TTS_FLAG_SHOULDFREE;

	MemoryContextSwitchTo(oldContext);
}

static void
tts_heap_copyslot(TupleTableSlot *dstslot, TupleTableSlot *srcslot)
{
	HeapTuple	tuple;
	MemoryContext oldcontext;

	oldcontext = MemoryContextSwitchTo(dstslot->tts_mcxt);
	tuple = ExecCopySlotHeapTuple(srcslot);
	MemoryContextSwitchTo(oldcontext);

	ExecStoreHeapTuple(tuple, dstslot, true);
}

static HeapTuple
tts_heap_get_heap_tuple(TupleTableSlot *slot)
{
	HeapTupleTableSlot *hslot = (HeapTupleTableSlot *) slot;

	Assert(!TTS_EMPTY(slot));
	if (!hslot->tuple)
		tts_heap_materialize(slot);

	return hslot->tuple;
}

static HeapTuple
tts_heap_copy_heap_tuple(TupleTableSlot *slot)
{
	HeapTupleTableSlot *hslot = (HeapTupleTableSlot *) slot;

	Assert(!TTS_EMPTY(slot));
	if (!hslot->tuple)
		tts_heap_materialize(slot);

	return heap_copytuple(hslot->tuple);
}

static MinimalTuple
tts_heap_copy_minimal_tuple(TupleTableSlot *slot)
{
	HeapTupleTableSlot *hslot = (HeapTupleTableSlot *) slot;

	if (!hslot->tuple)
		tts_heap_materialize(slot);

	return minimal_tuple_from_heap_tuple(hslot->tuple);
}

/*
 * TupleTableSlotOps implementation for MinimalTupleTableSlot.
 */

static void
tts_minimal_init(TupleTableSlot *slot)
{
	MinimalTupleTableSlot *mslot = (MinimalTupleTableSlot *) slot;

	/*
	 * Initialize the heap tuple pointer to access attributes of the minimal
	 * tuple contained in the slot as if its a heap tuple.
	 */
	mslot->tuple = &mslot->minhdr;
}

static void
tts_minimal_release(TupleTableSlot *slot)
{
}

static void
tts_minimal_clear(TupleTableSlot *slot)
{
	MinimalTupleTableSlot *mslot = (MinimalTupleTableSlot *) slot;

	if (TTS_SHOULDFREE(slot))
	{
		heap_free_minimal_tuple(mslot->mintuple);
		slot->tts_flags &= ~TTS_FLAG_SHOULDFREE;
	}

	slot->tts_nvalid = 0;
	slot->tts_flags |= TTS_FLAG_EMPTY;
	ItemPointerSetInvalid(&slot->tts_tid);
	mslot->off = 0;
	mslot->mintuple = NULL;
}

static void
tts_minimal_getsomeattrs(TupleTableSlot *slot, int natts)
{
	MinimalTupleTableSlot *mslot = (MinimalTupleTableSlot *) slot;

	Assert(!TTS_EMPTY(slot));

	vslot_deform_heap_tuple(slot, mslot->tuple, &mslot->off, natts);
}

static Datum
tts_minimal_getsysattr(TupleTableSlot *slot, int attnum, bool *isnull)
{
	elog(ERROR, "minimal tuple table slot does not have system attributes");

	return 0;					/* silence compiler warnings */
}

static void
tts_minimal_materialize(TupleTableSlot *slot)
{
	MinimalTupleTableSlot *mslot = (MinimalTupleTableSlot *) slot;
	MemoryContext oldContext;

	Assert(!TTS_EMPTY(slot));

	/* If slot has its tuple already materialized, nothing to do. */
	if (TTS_SHOULDFREE(slot))
		return;

	oldContext = MemoryContextSwitchTo(slot->tts_mcxt);

	/*
	 * Have to deform from scratch, otherwise tts_values[] entries could point
	 * into the non-materialized tuple (which might be gone when accessed).
	 */
	slot->tts_nvalid = 0;
	mslot->off = 0;

	if (!mslot->mintuple)
	{
		mslot->mintuple = heap_form_minimal_tuple(slot->tts_tupleDescriptor,
												  slot->tts_values,
												  slot->tts_isnull);
	}
	else
	{
		/*
		 * The minimal tuple contained in this slot is not allocated in the
		 * memory context of the given slot (else it would have TTS_SHOULDFREE
		 * set).  Copy the minimal tuple into the given slot's memory context.
		 */
		mslot->mintuple = heap_copy_minimal_tuple(mslot->mintuple);
	}

	slot->tts_flags |= TTS_FLAG_SHOULDFREE;

	Assert(mslot->tuple == &mslot->minhdr);

	mslot->minhdr.t_len = mslot->mintuple->t_len + MINIMAL_TUPLE_OFFSET;
	mslot->minhdr.t_data = (HeapTupleHeader) ((char *) mslot->mintuple - MINIMAL_TUPLE_OFFSET);

	MemoryContextSwitchTo(oldContext);
}

static void
tts_minimal_copyslot(TupleTableSlot *dstslot, TupleTableSlot *srcslot)
{
	MemoryContext oldcontext;
	MinimalTuple mintuple;

	oldcontext = MemoryContextSwitchTo(dstslot->tts_mcxt);
	mintuple = ExecCopySlotMinimalTuple(srcslot);
	MemoryContextSwitchTo(oldcontext);

	ExecStoreMinimalTuple(mintuple, dstslot, true);
}

static MinimalTuple
tts_minimal_get_minimal_tuple(TupleTableSlot *slot)
{
	MinimalTupleTableSlot *mslot = (MinimalTupleTableSlot *) slot;

	if (!mslot->mintuple)
		tts_minimal_materialize(slot);

	return mslot->mintuple;
}

static HeapTuple
tts_minimal_copy_heap_tuple(TupleTableSlot *slot)
{
	MinimalTupleTableSlot *mslot = (MinimalTupleTableSlot *) slot;

	if (!mslot->mintuple)
		tts_minimal_materialize(slot);

	return heap_tuple_from_minimal_tuple(mslot->mintuple);
}

static MinimalTuple
tts_minimal_copy_minimal_tuple(TupleTableSlot *slot)
{
	MinimalTupleTableSlot *mslot = (MinimalTupleTableSlot *) slot;

	if (!mslot->mintuple)
		tts_minimal_materialize(slot);

	return heap_copy_minimal_tuple(mslot->mintuple);
}

static void
tts_minimal_store_tuple(TupleTableSlot *slot, MinimalTuple mtup, bool shouldFree)
{
	MinimalTupleTableSlot *mslot = (MinimalTupleTableSlot *) slot;

	tts_minimal_clear(slot);

	Assert(!TTS_SHOULDFREE(slot));
	Assert(TTS_EMPTY(slot));

	slot->tts_flags &= ~TTS_FLAG_EMPTY;
	slot->tts_nvalid = 0;
	mslot->off = 0;

	mslot->mintuple = mtup;
	Assert(mslot->tuple == &mslot->minhdr);
	mslot->minhdr.t_len = mtup->t_len + MINIMAL_TUPLE_OFFSET;
	mslot->minhdr.t_data = (HeapTupleHeader) ((char *) mtup - MINIMAL_TUPLE_OFFSET);
	/* no need to set t_self or t_tableOid since we won't allow access */

	if (shouldFree)
		slot->tts_flags |= TTS_FLAG_SHOULDFREE;
}


/*
 * TupleTableSlotOps implementation for BufferHeapTupleTableSlot.
 */

static void
vector_tts_buffer_heap_init(TupleTableSlot *slot)
{
	VectorTupleTableSlot *vslot = (VectorTupleTableSlot *) slot;
	VectorBufferHeapTupleTableSlot *vbhslot = (VectorBufferHeapTupleTableSlot *) slot;
	TupleDesc	vdesc;
	vtype		*column;
	int			i;

	vslot->dim = 0;
	vbhslot->bufnum = 0;

	memset(vslot->skip, false, sizeof(bool) *BATCHSIZE );

    /*
     * since we will change the attr type of tuple desc to vector
     * type. we need to copy it to avoid dirty the relcache
     */
    vdesc = CreateTupleDescCopyConstr(slot->tts_tupleDescriptor);
	vslot->vector_tts_tupleDescriptor = vdesc;

	vslot->vector_tts_values = palloc(vdesc->natts * sizeof(Datum));

    /* change the attr type of tuple desc to vector type */
    for (i = 0; i < vdesc->natts; i++)
    {
        Form_pg_attribute   attr = TupleDescAttr(vdesc, i);
        Oid                 vtypid = GetVtype(attr->atttypid);
        if (vtypid != InvalidOid)
            attr->atttypid = vtypid;
        else
            elog(ERROR, "cannot find vectorized type for type %d",
                    attr->atttypid);
		/* fill vector_tts_values */
		column = buildvtype(vtypid, BATCHSIZE, vslot->skip);
		column->dim = 0;
		vslot->vector_tts_values[i]  = PointerGetDatum(column);
    }
	memset(vbhslot->tts_buffers, 0, sizeof(Buffer) * BATCHSIZE);
}

static void
tts_buffer_heap_release(TupleTableSlot *slot)
{
}

static void
vector_tts_buffer_heap_clear(TupleTableSlot *slot)
{
	VectorTupleTableSlot *vslot = (VectorTupleTableSlot *) slot;
	VectorBufferHeapTupleTableSlot *vbhslot = (VectorBufferHeapTupleTableSlot *) slot;
	vtype   *column;
	int		i;

	slot->tts_nvalid = 0;
	slot->tts_flags |= TTS_FLAG_EMPTY;
	ItemPointerSetInvalid(&slot->tts_tid);

	/* vectorize part  */
	for(i = 0; i < vbhslot->bufnum; i++)
	{
		if(BufferIsValid(vbhslot->tts_buffers[i]))
		{
			ReleaseBuffer(vbhslot->tts_buffers[i]);
			vbhslot->tts_buffers[i] = InvalidBuffer;
		}
	}
	vslot->dim = 0;
	vbhslot->bufnum = 0;

	for (i = 0; i < slot->tts_tupleDescriptor->natts; i++)
	{
		column = (vtype *)DatumGetPointer(vslot->vector_tts_values[i]);
		column->dim = 0;
	}

	memset(vslot->skip, false, sizeof(vslot->skip));
}

static void
vector_tts_buffer_heap_getsomeattrs(TupleTableSlot *slot, int natts)
{
	Assert(!TTS_EMPTY(slot));

	vector_slot_deform_heap_tuple(slot, natts);
}

static Datum
tts_buffer_heap_getsysattr(TupleTableSlot *slot, int attnum, bool *isnull)
{
	BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;

	Assert(!TTS_EMPTY(slot));

	return heap_getsysattr(bslot->base.tuple, attnum,
						   slot->tts_tupleDescriptor, isnull);
}

static void
tts_buffer_heap_materialize(TupleTableSlot *slot)
{
	BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;
	MemoryContext oldContext;

	Assert(!TTS_EMPTY(slot));

	/* If slot has its tuple already materialized, nothing to do. */
	if (TTS_SHOULDFREE(slot))
		return;

	oldContext = MemoryContextSwitchTo(slot->tts_mcxt);

	/*
	 * Have to deform from scratch, otherwise tts_values[] entries could point
	 * into the non-materialized tuple (which might be gone when accessed).
	 */
	bslot->base.off = 0;
	slot->tts_nvalid = 0;

	if (!bslot->base.tuple)
	{
		/*
		 * Normally BufferHeapTupleTableSlot should have a tuple + buffer
		 * associated with it, unless it's materialized (which would've
		 * returned above). But when it's useful to allow storing virtual
		 * tuples in a buffer slot, which then also needs to be
		 * materializable.
		 */
		bslot->base.tuple = heap_form_tuple(slot->tts_tupleDescriptor,
											slot->tts_values,
											slot->tts_isnull);
	}
	else
	{
		bslot->base.tuple = heap_copytuple(bslot->base.tuple);

		/*
		 * A heap tuple stored in a BufferHeapTupleTableSlot should have a
		 * buffer associated with it, unless it's materialized or virtual.
		 */
		if (likely(BufferIsValid(bslot->buffer)))
			ReleaseBuffer(bslot->buffer);
		bslot->buffer = InvalidBuffer;
	}

	/*
	 * We don't set TTS_FLAG_SHOULDFREE until after releasing the buffer, if
	 * any.  This avoids having a transient state that would fall foul of our
	 * assertions that a slot with TTS_FLAG_SHOULDFREE doesn't own a buffer.
	 * In the unlikely event that ReleaseBuffer() above errors out, we'd
	 * effectively leak the copied tuple, but that seems fairly harmless.
	 */
	slot->tts_flags |= TTS_FLAG_SHOULDFREE;

	MemoryContextSwitchTo(oldContext);
}

static void
tts_buffer_heap_copyslot(TupleTableSlot *dstslot, TupleTableSlot *srcslot)
{
	BufferHeapTupleTableSlot *bsrcslot = (BufferHeapTupleTableSlot *) srcslot;
	BufferHeapTupleTableSlot *bdstslot = (BufferHeapTupleTableSlot *) dstslot;

	/*
	 * If the source slot is of a different kind, or is a buffer slot that has
	 * been materialized / is virtual, make a new copy of the tuple. Otherwise
	 * make a new reference to the in-buffer tuple.
	 */
	if (dstslot->tts_ops != srcslot->tts_ops ||
		TTS_SHOULDFREE(srcslot) ||
		!bsrcslot->base.tuple)
	{
		MemoryContext oldContext;

		ExecClearTuple(dstslot);
		dstslot->tts_flags &= ~TTS_FLAG_EMPTY;
		oldContext = MemoryContextSwitchTo(dstslot->tts_mcxt);
		bdstslot->base.tuple = ExecCopySlotHeapTuple(srcslot);
		dstslot->tts_flags |= TTS_FLAG_SHOULDFREE;
		MemoryContextSwitchTo(oldContext);
	}
	else
	{
		Assert(BufferIsValid(bsrcslot->buffer));

		tts_buffer_heap_store_tuple(dstslot, bsrcslot->base.tuple,
									bsrcslot->buffer, false);

		/*
		 * The HeapTupleData portion of the source tuple might be shorter
		 * lived than the destination slot. Therefore copy the HeapTuple into
		 * our slot's tupdata, which is guaranteed to live long enough (but
		 * will still point into the buffer).
		 */
		memcpy(&bdstslot->base.tupdata, bdstslot->base.tuple, sizeof(HeapTupleData));
		bdstslot->base.tuple = &bdstslot->base.tupdata;
	}
}

static HeapTuple
tts_buffer_heap_get_heap_tuple(TupleTableSlot *slot)
{
	BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;

	Assert(!TTS_EMPTY(slot));

	if (!bslot->base.tuple)
		tts_buffer_heap_materialize(slot);

	return bslot->base.tuple;
}

static HeapTuple
tts_buffer_heap_copy_heap_tuple(TupleTableSlot *slot)
{
	BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;

	Assert(!TTS_EMPTY(slot));

	if (!bslot->base.tuple)
		tts_buffer_heap_materialize(slot);

	return heap_copytuple(bslot->base.tuple);
}

static MinimalTuple
tts_buffer_heap_copy_minimal_tuple(TupleTableSlot *slot)
{
	BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;

	Assert(!TTS_EMPTY(slot));

	if (!bslot->base.tuple)
		tts_buffer_heap_materialize(slot);

	return minimal_tuple_from_heap_tuple(bslot->base.tuple);
}

static inline void
tts_buffer_heap_store_tuple(TupleTableSlot *slot, HeapTuple tuple,
							Buffer buffer, bool transfer_pin)
{
	BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;

	if (TTS_SHOULDFREE(slot))
	{
		/* materialized slot shouldn't have a buffer to release */
		Assert(!BufferIsValid(bslot->buffer));

		heap_freetuple(bslot->base.tuple);
		slot->tts_flags &= ~TTS_FLAG_SHOULDFREE;
	}

	slot->tts_flags &= ~TTS_FLAG_EMPTY;
	slot->tts_nvalid = 0;
	bslot->base.tuple = tuple;
	bslot->base.off = 0;
	slot->tts_tid = tuple->t_self;

	/*
	 * If tuple is on a disk page, keep the page pinned as long as we hold a
	 * pointer into it.  We assume the caller already has such a pin.  If
	 * transfer_pin is true, we'll transfer that pin to this slot, if not
	 * we'll pin it again ourselves.
	 *
	 * This is coded to optimize the case where the slot previously held a
	 * tuple on the same disk page: in that case releasing and re-acquiring
	 * the pin is a waste of cycles.  This is a common situation during
	 * seqscans, so it's worth troubling over.
	 */
	if (bslot->buffer != buffer)
	{
		if (BufferIsValid(bslot->buffer))
			ReleaseBuffer(bslot->buffer);

		bslot->buffer = buffer;

		if (!transfer_pin && BufferIsValid(buffer))
			IncrBufferRefCount(buffer);
	}
	else if (transfer_pin && BufferIsValid(buffer))
	{
		/*
		 * In transfer_pin mode the caller won't know about the same-page
		 * optimization, so we gotta release its pin.
		 */
		ReleaseBuffer(buffer);
	}
}

static inline void
tts_vector_buffer_heap_store_tuple(TupleTableSlot *slot, HeapTuple tuple,
							Buffer buffer, bool transfer_pin)
{
	VectorTupleTableSlot *vslot = (VectorTupleTableSlot *)slot;
	VectorBufferHeapTupleTableSlot *vbhslot = (VectorBufferHeapTupleTableSlot *) slot;

	slot->tts_flags &= ~TTS_FLAG_EMPTY;
	slot->tts_nvalid = 0;
	slot->tts_tid = tuple->t_self;

	//TODO: do we need to copy
	memcpy(&vbhslot->base.tts_tuples[vslot->dim], tuple, sizeof(HeapTupleData));

	/*
	 * If tuple is on a disk page, keep the page pinned as long as we hold a
	 * pointer into it.  We assume the caller already has such a pin.  If
	 * transfer_pin is true, we'll transfer that pin to this slot, if not
	 * we'll pin it again ourselves.
	 *
	 * This is coded to optimize the case where the slot previously held a
	 * tuple on the same disk page: in that case releasing and re-acquiring
	 * the pin is a waste of cycles.  This is a common situation during
	 * seqscans, so it's worth troubling over.
	 */
	if (vbhslot->bufnum == 0 || vbhslot->tts_buffers[vbhslot->bufnum-1] != buffer)
	{
		if (BufferIsValid(vbhslot->tts_buffers[vbhslot->bufnum]))
			ReleaseBuffer(vbhslot->tts_buffers[vbhslot->bufnum]);
		vbhslot->tts_buffers[vbhslot->bufnum] = buffer;
		vbhslot->bufnum++;
		if (!transfer_pin && BufferIsValid(buffer))
			IncrBufferRefCount(buffer);
	}
	else if (transfer_pin && BufferIsValid(buffer))
	{
		/*
		 * In transfer_pin mode the caller won't know about the same-page
		 * optimization, so we gotta release its pin.
		 */
		ReleaseBuffer(buffer);
	}
	vslot->dim++;
}

/*
 * slot_deform_heap_tuple
 *		Given a TupleTableSlot, extract data from the slot's physical tuple
 *		into its Datum/isnull arrays.  Data is extracted up through the
 *		natts'th column (caller must ensure this is a legal column number).
 *
 *		This is essentially an incremental version of heap_deform_tuple:
 *		on each call we extract attributes up to the one needed, without
 *		re-computing information about previously extracted attributes.
 *		slot->tts_nvalid is the number of attributes already extracted.
 *
 * This is marked as always inline, so the different offp for different types
 * of slots gets optimized away.
 */
static pg_attribute_always_inline void
vector_slot_deform_heap_tuple(TupleTableSlot *slot,
							  int natts)
{
	VectorTupleTableSlot *vslot = (VectorTupleTableSlot *) slot;
	VectorBufferHeapTupleTableSlot *bslot = (VectorBufferHeapTupleTableSlot *) slot;
	TupleDesc	tupleDesc = slot->tts_tupleDescriptor;
	//Datum	   *values = slot->tts_values;
	//bool	   *isnull = slot->tts_isnull;
	HeapTupleHeader tup;// = tuple->t_data;
	bool		hasnulls;// = HeapTupleHasNulls(tuple);
	int			attnum;
	char	   *tp;				/* ptr to tuple data */
	uint32		off;			/* offset in tuple data */
	bits8	   *bp;// = tup->t_bits;	/* ptr to null bitmap in tuple */
	bool		slow;			/* can we use/set attcacheoff? */
	int 		row;
	HeapTuple	tuple;
	vtype		*column;


	/*
	 * Check whether the first call for this tuple, and initialize or restore
	 * loop state.
	 */

	for (row = 0; row < vslot->dim; row++)
	{
		tuple = &bslot->base.tts_tuples[row];
		tup = tuple->t_data;
		bp = tup->t_bits;
		hasnulls = HeapTupleHasNulls(tuple);

		attnum = slot->tts_nvalid;
		/* We can only fetch as many attributes as the tuple has. */
		natts = Min(HeapTupleHeaderGetNatts(tuple->t_data), natts);

		/* vectorize engine deform once for now  */
		if (attnum == 0)
		{
			off = 0;
			slow = false;
		}
		else
		{
			/* Restore state from previous execution */
			off = bslot->base.tts_offs[row];
			/*TODO: handle slow,hack here to consider there is no nulls */
			slow = false;
			//slow = TTS_SLOW(slot);
		}

		tp = (char *) tup + tup->t_hoff;

		for (; attnum < natts; attnum++)
		{
			Form_pg_attribute thisatt = TupleDescAttr(tupleDesc, attnum);
			column = (vtype *)vslot->vector_tts_values[attnum];

			if (hasnulls && att_isnull(attnum, bp))
			{
				//values[attnum] = (Datum) 0;
				//isnull[attnum] = true;
				column->values[row] = (Datum) 0;
				column->isnull[row] = true;
				slow = true;		/* can't use attcacheoff anymore */
				continue;
			}

			column->isnull[row] = false;

			if (!slow && thisatt->attcacheoff >= 0)
				off = thisatt->attcacheoff;
			else if (thisatt->attlen == -1)
			{
				/*
				 * We can only cache the offset for a varlena attribute if the
				 * offset is already suitably aligned, so that there would be no
				 * pad bytes in any case: then the offset will be valid for either
				 * an aligned or unaligned value.
				 */
				if (!slow &&
					off == att_align_nominal(off, thisatt->attalign))
					thisatt->attcacheoff = off;
				else
				{
					off = att_align_pointer(off, thisatt->attalign, -1,
											tp + off);
					slow = true;
				}
			}
			else
			{
				/* not varlena, so safe to use att_align_nominal */
				off = att_align_nominal(off, thisatt->attalign);

				if (!slow)
					thisatt->attcacheoff = off;
			}
			column->values[row] = fetchatt(thisatt, tp + off);

			off = att_addlength_pointer(off, thisatt->attlen, tp + off);

			if (thisatt->attlen <= 0)
				slow = true;		/* can't use attcacheoff anymore */
		}
		bslot->base.tts_offs[row] = off;
	}

	attnum = slot->tts_nvalid;
	for (; attnum < natts; attnum++)
	{
		column = (vtype *)vslot->vector_tts_values[attnum];
		column->dim = vslot->dim;
	}
	/*
	 * Save state for next execution
	 */
	slot->tts_nvalid = attnum;
	
	/**offp = off;
	if (slow)
		slot->tts_flags |= TTS_FLAG_SLOW;
	else
		slot->tts_flags &= ~TTS_FLAG_SLOW;*/
}

/*
 * slot_deform_heap_tuple
 *		Given a TupleTableSlot, extract data from the slot's physical tuple
 *		into its Datum/isnull arrays.  Data is extracted up through the
 *		natts'th column (caller must ensure this is a legal column number).
 *
 *		This is essentially an incremental version of heap_deform_tuple:
 *		on each call we extract attributes up to the one needed, without
 *		re-computing information about previously extracted attributes.
 *		slot->tts_nvalid is the number of attributes already extracted.
 *
 * This is marked as always inline, so the different offp for different types
 * of slots gets optimized away.
 */
static pg_attribute_always_inline void
vslot_deform_heap_tuple(TupleTableSlot *slot, HeapTuple tuple, uint32 *offp,
					   int natts)
{
	TupleDesc	tupleDesc = slot->tts_tupleDescriptor;
	Datum	   *values = slot->tts_values;
	bool	   *isnull = slot->tts_isnull;
	HeapTupleHeader tup = tuple->t_data;
	bool		hasnulls = HeapTupleHasNulls(tuple);
	int			attnum;
	char	   *tp;				/* ptr to tuple data */
	uint32		off;			/* offset in tuple data */
	bits8	   *bp = tup->t_bits;	/* ptr to null bitmap in tuple */
	bool		slow;			/* can we use/set attcacheoff? */

	/* We can only fetch as many attributes as the tuple has. */
	natts = Min(HeapTupleHeaderGetNatts(tuple->t_data), natts);

	/*
	 * Check whether the first call for this tuple, and initialize or restore
	 * loop state.
	 */
	attnum = slot->tts_nvalid;
	if (attnum == 0)
	{
		/* Start from the first attribute */
		off = 0;
		slow = false;
	}
	else
	{
		/* Restore state from previous execution */
		off = *offp;
		slow = TTS_SLOW(slot);
	}

	tp = (char *) tup + tup->t_hoff;

	for (; attnum < natts; attnum++)
	{
		Form_pg_attribute thisatt = TupleDescAttr(tupleDesc, attnum);

		if (hasnulls && att_isnull(attnum, bp))
		{
			values[attnum] = (Datum) 0;
			isnull[attnum] = true;
			slow = true;		/* can't use attcacheoff anymore */
			continue;
		}

		isnull[attnum] = false;

		if (!slow && thisatt->attcacheoff >= 0)
			off = thisatt->attcacheoff;
		else if (thisatt->attlen == -1)
		{
			/*
			 * We can only cache the offset for a varlena attribute if the
			 * offset is already suitably aligned, so that there would be no
			 * pad bytes in any case: then the offset will be valid for either
			 * an aligned or unaligned value.
			 */
			if (!slow &&
				off == att_align_nominal(off, thisatt->attalign))
				thisatt->attcacheoff = off;
			else
			{
				off = att_align_pointer(off, thisatt->attalign, -1,
										tp + off);
				slow = true;
			}
		}
		else
		{
			/* not varlena, so safe to use att_align_nominal */
			off = att_align_nominal(off, thisatt->attalign);

			if (!slow)
				thisatt->attcacheoff = off;
		}

		values[attnum] = fetchatt(thisatt, tp + off);

		off = att_addlength_pointer(off, thisatt->attlen, tp + off);

		if (thisatt->attlen <= 0)
			slow = true;		/* can't use attcacheoff anymore */
	}

	/*
	 * Save state for next execution
	 */
	slot->tts_nvalid = attnum;
	*offp = off;
	if (slow)
		slot->tts_flags |= TTS_FLAG_SLOW;
	else
		slot->tts_flags &= ~TTS_FLAG_SLOW;
}


const TupleTableSlotOps TTSOpsVectorVirtual = {
	.base_slot_size = sizeof(VectorVirtualTupleTableSlot),
	.init = vector_tts_virtual_init,
	.release = tts_virtual_release,
	.clear = vector_tts_virtual_clear,
	.getsomeattrs = vector_tts_virtual_getsomeattrs,
	.getsysattr = tts_virtual_getsysattr,
	.materialize = tts_virtual_materialize,
	.copyslot = tts_virtual_copyslot,

	/*
	 * A virtual tuple table slot can not "own" a heap tuple or a minimal
	 * tuple.
	 */
	.get_heap_tuple = NULL,
	.get_minimal_tuple = NULL,
	.copy_heap_tuple = tts_virtual_copy_heap_tuple,
	.copy_minimal_tuple = tts_virtual_copy_minimal_tuple
};

const TupleTableSlotOps TTSOpsVectorHeapTuple = {
	.base_slot_size = sizeof(VectorHeapTupleTableSlot),
	.init = tts_heap_init,
	.release = tts_heap_release,
	.clear = tts_heap_clear,
	.getsomeattrs = tts_heap_getsomeattrs,
	.getsysattr = tts_heap_getsysattr,
	.materialize = tts_heap_materialize,
	.copyslot = tts_heap_copyslot,
	.get_heap_tuple = tts_heap_get_heap_tuple,

	/* A heap tuple table slot can not "own" a minimal tuple. */
	.get_minimal_tuple = NULL,
	.copy_heap_tuple = tts_heap_copy_heap_tuple,
	.copy_minimal_tuple = tts_heap_copy_minimal_tuple
};

const TupleTableSlotOps TTSOpsVectorMinimalTuple = {
	.base_slot_size = sizeof(VectorMinimalTupleTableSlot),
	.init = tts_minimal_init,
	.release = tts_minimal_release,
	.clear = tts_minimal_clear,
	.getsomeattrs = tts_minimal_getsomeattrs,
	.getsysattr = tts_minimal_getsysattr,
	.materialize = tts_minimal_materialize,
	.copyslot = tts_minimal_copyslot,

	/* A minimal tuple table slot can not "own" a heap tuple. */
	.get_heap_tuple = NULL,
	.get_minimal_tuple = tts_minimal_get_minimal_tuple,
	.copy_heap_tuple = tts_minimal_copy_heap_tuple,
	.copy_minimal_tuple = tts_minimal_copy_minimal_tuple
};

const TupleTableSlotOps TTSOpsVectorBufferHeapTuple = {
	.base_slot_size = sizeof(VectorBufferHeapTupleTableSlot),
	.init = vector_tts_buffer_heap_init,
	.release = tts_buffer_heap_release,
	.clear = vector_tts_buffer_heap_clear,
	.getsomeattrs = vector_tts_buffer_heap_getsomeattrs,
	.getsysattr = tts_buffer_heap_getsysattr,
	.materialize = tts_buffer_heap_materialize,
	.copyslot = tts_buffer_heap_copyslot,
	.get_heap_tuple = tts_buffer_heap_get_heap_tuple,

	/* A buffer heap tuple table slot can not "own" a minimal tuple. */
	.get_minimal_tuple = NULL,
	.copy_heap_tuple = tts_buffer_heap_copy_heap_tuple,
	.copy_minimal_tuple = tts_buffer_heap_copy_minimal_tuple
};


TupleTableSlot *
VectorExecStoreBufferHeapTuple(HeapTuple tuple,
							   TupleTableSlot *slot,
							   Buffer buffer)
{
	/*
	 * sanity checks
	 */
	Assert(tuple != NULL);
	Assert(slot != NULL);
	Assert(slot->tts_tupleDescriptor != NULL);
	Assert(BufferIsValid(buffer));

	if (unlikely(!TTS_IS_VECTOR_BUFFERTUPLE(slot)))
		elog(ERROR, "trying to store an on-disk heap tuple into wrong type of slot");
	tts_vector_buffer_heap_store_tuple(slot, tuple, buffer, false);

	slot->tts_tableOid = tuple->t_tableOid;

	return slot;
}


/* ----------------------------------------------------------------
 *				convenience initialization routines
 * ----------------------------------------------------------------
 */

/* ----------------
 *		ExecInitResultTypeTL
 *
 *		Initialize result type, using the plan node's targetlist.
 * ----------------
 */
void
VectorExecInitResultTypeTL(PlanState *planstate)
{
	TupleDesc	tupDesc = ExecTypeFromTL(planstate->plan->targetlist);

	for (int i = 0; i < tupDesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupDesc, i);
		Oid         typid = GetNtype(attr->atttypid);
		if (typid != InvalidOid)
			attr->atttypid = typid;
	}

	planstate->ps_ResultTupleDesc = tupDesc;
}

/* ---------------------------------------------------------------
 *      Routines for setting/accessing attributes in a slot.
 * ---------------------------------------------------------------
 */

/*
 * Fill in missing values for a TupleTableSlot.
 *
 * This is only exposed because it's needed for JIT compiled tuple
 * deforming. That exception aside, there should be no callers outside of this
 * file.
 */
void
vector_slot_getmissingattrs(TupleTableSlot *slot, int startAttNum, int lastAttNum)
{
	//TODO: vector_slot_getmissingattrs is not implemnted yet.
	AttrMissing *attrmiss = NULL;

	if (slot->tts_tupleDescriptor->constr)
		attrmiss = slot->tts_tupleDescriptor->constr->missing;

	if (!attrmiss)
	{
		/* no missing values array at all, so just fill everything in as NULL */
		memset(slot->tts_values + startAttNum, 0,
			   (lastAttNum - startAttNum) * sizeof(Datum));
		memset(slot->tts_isnull + startAttNum, 1,
			   (lastAttNum - startAttNum) * sizeof(bool));
	}
	else
	{
		int			missattnum;

		/* if there is a missing values array we must process them one by one */
		for (missattnum = startAttNum;
			 missattnum < lastAttNum;
			 missattnum++)
		{
			slot->tts_values[missattnum] = attrmiss[missattnum].am_value;
			slot->tts_isnull[missattnum] = !attrmiss[missattnum].am_present;
		}
	}
}

/*
 * slot_getsomeattrs_int - workhorse for slot_getsomeattrs()
 */
void
vector_slot_getsomeattrs_int(TupleTableSlot *slot, int attnum)
{
	/* Check for caller errors */
	Assert(slot->tts_nvalid < attnum);	/* checked in slot_getsomeattrs */
	Assert(attnum > 0);

	if (unlikely(attnum > slot->tts_tupleDescriptor->natts))
		elog(ERROR, "invalid attribute number %d", attnum);

	/* Fetch as many attributes as possible from the underlying tuple. */
	slot->tts_ops->getsomeattrs(slot, attnum);

	/*
	 * If the underlying tuple doesn't have enough attributes, tuple
	 * descriptor must have the missing attributes.
	 */
	if (unlikely(slot->tts_nvalid < attnum))
	{
		vector_slot_getmissingattrs(slot, slot->tts_nvalid, attnum);
		slot->tts_nvalid = attnum;
	}
}

void
SetVectorBufferHeapTupleFinished(TupleTableSlot *slot)
{
	VectorBufferHeapTupleTableSlot *vbslot = (VectorBufferHeapTupleTableSlot *) slot;
	vbslot->finished = true;
}

bool
GetVectorBufferHeapTupleFinished(TupleTableSlot *slot)
{
	VectorBufferHeapTupleTableSlot *vbslot = (VectorBufferHeapTupleTableSlot *) slot;
	return vbslot->finished;
}

int32
GetVectorBufferHeapTupleNum(TupleTableSlot *slot)
{
	VectorTupleTableSlot *vslot = (VectorTupleTableSlot *) slot;
	return vslot->dim;
}
