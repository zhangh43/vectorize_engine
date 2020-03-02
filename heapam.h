/*-------------------------------------------------------------------------
 *
 * heapam.h
 *	  POSTGRES heap access method definitions.
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/heapam.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VECTOR_ENGINE_HEAPAM_H
#define VECTOR_ENGINE_HEAPAM_H

#include "access/relation.h"	/* for backward compatibility */
#include "access/relscan.h"
#include "access/sdir.h"
#include "access/skey.h"
#include "access/table.h"		/* for backward compatibility */
#include "access/tableam.h"
#include "nodes/lockoptions.h"
#include "nodes/primnodes.h"
#include "storage/bufpage.h"
#include "storage/dsm.h"
#include "storage/lockdefs.h"
#include "storage/shm_toc.h"
#include "utils/relcache.h"
#include "utils/snapshot.h"

#include "access/heapam.h"

/* ----------------
 *		function prototypes for heap access method
 *
 * heap_create, heap_create_with_catalog, and heap_drop_with_catalog
 * are declared in catalog/heap.h
 * ----------------
 */

extern TableScanDesc vector_heap_beginscan(Relation relation, Snapshot snapshot,
									int nkeys, ScanKey key,
									ParallelTableScanDesc parallel_scan,
									uint32 flags);
extern void vector_heap_rescan(TableScanDesc scan, ScanKey key, bool set_params,
						bool allow_strat, bool allow_sync, bool allow_pagemode);
extern void vector_heap_endscan(TableScanDesc scan);
extern bool vector_heap_getnextslot(TableScanDesc sscan,
							 ScanDirection direction, struct TupleTableSlot *slot);

#endif							/* HEAPAM_H */
