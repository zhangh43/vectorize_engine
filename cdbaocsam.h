/*-------------------------------------------------------------------------
 *
 * cdbaocsam.h
 *	  append-only columnar relation access method definitions.
 *
 * Portions Copyright (c) 2009, Greenplum Inc.
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/cdb/cdbaocsam.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VECTOR_CDB_AOCSAM_H
#define VECTOR_CDB_AOCSAM_H

#include "access/relscan.h"
#include "access/sdir.h"
#include "access/tupmacs.h"
#include "access/xlogutils.h"
#include "access/appendonlytid.h"
#include "access/appendonly_visimap.h"
#include "executor/tuptable.h"
#include "nodes/primnodes.h"
#include "storage/block.h"
#include "utils/rel.h"
#include "utils/tqual.h"
#include "cdb/cdbappendonlyblockdirectory.h"
#include "cdb/cdbappendonlystoragelayer.h"
#include "cdb/cdbappendonlystorageread.h"
#include "cdb/cdbappendonlystoragewrite.h"

#include "cdb/cdbaocsam.h"

#include "datumstream.h"

extern bool vaocs_getnext(AOCSScanDesc scan, ScanDirection direction, TupleTableSlot *slot);
#endif   /* AOCSAM_H */
