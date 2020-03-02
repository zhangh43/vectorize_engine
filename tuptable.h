/*-------------------------------------------------------------------------
 *
 * tuptable.h
 *	  tuple table support stuff
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/tuptable.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VECTOR_ENGINE_TUPTABLE_H
#define VECTOR_ENGINE_TUPTABLE_H

#include "access/htup.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/tupdesc.h"
#include "storage/buf.h"

#include "executor/tuptable.h"
#include "vtype/vtype.h"

/*
 * Predefined TupleTableSlotOps for various types of TupleTableSlotOps. The
 * same are used to identify the type of a given slot.
 */
extern PGDLLIMPORT const TupleTableSlotOps TTSOpsVectorVirtual;
extern PGDLLIMPORT const TupleTableSlotOps TTSOpsVectorHeapTuple;
extern PGDLLIMPORT const TupleTableSlotOps TTSOpsVectorMinimalTuple;
extern PGDLLIMPORT const TupleTableSlotOps TTSOpsVectorBufferHeapTuple;

#define TTS_IS_VECTOR_VIRTUAL(slot) ((slot)->tts_ops == &TTSOpsVectorVirtual)
#define TTS_IS_VECTOR_HEAPTUPLE(slot) ((slot)->tts_ops == &TTSOpsVectorHeapTuple)
#define TTS_IS_VECTOR_MINIMALTUPLE(slot) ((slot)->tts_ops == &TTSOpsVectorMinimalTuple)
#define TTS_IS_VECTOR_BUFFERTUPLE(slot) ((slot)->tts_ops == &TTSOpsVectorBufferHeapTuple)


/*
 * Tuple table slot implementations.
 */


typedef struct VectorTupleTableSlot
{
	TupleTableSlot base;

	/* vectorize specific */
	/* how many tuples does this slot contain */ 
	int32			dim;

	/* skip array to represent filtered tuples */
	bool			skip[BATCHSIZE];

	TupleDesc   vector_tts_tupleDescriptor; /* slot's tuple descriptor */

	Datum	   *vector_tts_values;		/* current per-attribute values */
} VectorTupleTableSlot;

typedef struct VectorVirtualTupleTableSlot
{
	VectorTupleTableSlot base;

	char	   *data;			/* data for materialized slots */
} VectorVirtualTupleTableSlot;

typedef struct VectorHeapTupleTableSlot
{
	VectorTupleTableSlot base;
//TODO
#define FIELDNO_HEAPTUPLETABLESLOT_TUPLE 1
	//HeapTuple	tts_tuples[BATCHSIZE];			/* physical tuple */
	HeapTupleData	tts_tuples[BATCHSIZE];			/* physical tuple */
#define FIELDNO_HEAPTUPLETABLESLOT_OFF 2
	uint32		tts_offs[BATCHSIZE];			/* saved state for slot_deform_heap_tuple */
	//HeapTupleData tts_tupdatas[BATCHSIZE];		/* optional workspace for storing tuple */
} VectorHeapTupleTableSlot;

/* heap tuple residing in a buffer */
typedef struct VectorBufferHeapTupleTableSlot
{
	VectorHeapTupleTableSlot base;

	/*
	 * If buffer is not InvalidBuffer, then the slot is holding a pin on the
	 * indicated buffer page; drop the pin when we release the slot's
	 * reference to that buffer.  (TTS_FLAG_SHOULDFREE should not be set in
	 * such a case, since presumably tts_tuple is pointing into the buffer.)
	 */
	Buffer		tts_buffers[BATCHSIZE];			/* tuple's buffer, or InvalidBuffer */

	int32			bufnum;

	bool		finished;
	
} VectorBufferHeapTupleTableSlot;

typedef struct VectorMinimalTupleTableSlot
{
	VectorTupleTableSlot base;

	/*
	 * In a minimal slot tuple points at minhdr and the fields of that struct
	 * are set correctly for access to the minimal tuple; in particular,
	 * minhdr.t_data points MINIMAL_TUPLE_OFFSET bytes before mintuple.  This
	 * allows column extraction to treat the case identically to regular
	 * physical tuples.
	 */
#define FIELDNO_MINIMALTUPLETABLESLOT_TUPLE 1
	HeapTuple	tuple;			/* tuple wrapper */
	MinimalTuple mintuple;		/* minimal tuple, or NULL if none */
	HeapTupleData minhdr;		/* workspace for minimal-tuple-only case */
#define FIELDNO_MINIMALTUPLETABLESLOT_OFF 4
	uint32		off;			/* saved state for slot_deform_heap_tuple */

} VectorMinimalTupleTableSlot;

/* in executor/execTuples.c */
extern TupleTableSlot *VectorExecStoreBufferHeapTuple(HeapTuple tuple,
												TupleTableSlot *slot,
												Buffer buffer);

extern void vector_slot_getmissingattrs(TupleTableSlot *slot, int startAttNum,
								 int lastAttNum);
extern void vector_slot_getsomeattrs_int(TupleTableSlot *slot, int attnum);

/*
 * This function forces the entries of the slot's Datum/isnull arrays to be
 * valid at least up through the attnum'th entry.
 */
static inline void
vector_slot_getsomeattrs(TupleTableSlot *slot, int attnum)
{
	if (slot->tts_nvalid < attnum)
		vector_slot_getsomeattrs_int(slot, attnum);
}

/*
 * slot_getallattrs
 *		This function forces all the entries of the slot's Datum/isnull
 *		arrays to be valid.  The caller may then extract data directly
 *		from those arrays instead of using slot_getattr.
 */
static inline void
vector_slot_getallattrs(TupleTableSlot *slot)
{
	vector_slot_getsomeattrs(slot, slot->tts_tupleDescriptor->natts);
}


/*
 * slot_getattr - fetch one attribute of the slot's contents.
 */
static inline Datum
vector_slot_getattr(TupleTableSlot *slot, int attnum,
			 bool *isnull)
{
	VectorTupleTableSlot *vslot = (VectorTupleTableSlot *)slot;
	AssertArg(attnum > 0);

	if (attnum > slot->tts_nvalid)
		vector_slot_getsomeattrs(slot, attnum);

	*isnull = true;

	return vslot->vector_tts_values[attnum - 1];
}

/* vectorize specific */
extern void SetVectorBufferHeapTupleFinished(TupleTableSlot *slot);

extern bool GetVectorBufferHeapTupleFinished(TupleTableSlot *slot);

extern int32 GetVectorBufferHeapTupleNum(TupleTableSlot *slot);

#endif							/* TUPTABLE_H */
