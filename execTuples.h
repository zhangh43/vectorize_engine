#ifndef VECTOR_ENGINE_EXEC_TUPLES_H
#define VECTOR_ENGINE_EXEC_TUPLES_H

#include "postgres.h"

#include "executor/execdesc.h"
#include "nodes/parsenodes.h"
#include "executor/tuptable.h"
#include "storage/buf.h"
#include "vtype/vtype.h"

/*
 * prototypes from functions in execTuples.c
 */
extern void VExecInitResultTupleSlot(EState *estate, PlanState *planstate);
extern void VExecInitScanTupleSlot(EState *estate, ScanState *scanstate);
extern TupleTableSlot *VExecInitExtraTupleSlot(EState *estate);
extern void VExecAssignResultTypeFromTL(PlanState *planstate);
#endif
