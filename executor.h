#ifndef VECTOR_ENGINE_EXECUTOR_H
#define VECTOR_ENGINE_EXECUTOR_H

#include "postgres.h"
#include "executor/execdesc.h"
#include "nodes/parsenodes.h"

#include "nodeSeqscan.h"

extern bool VExecScanQual(List *qual, ExprContext *econtext, bool resultForNull);
/*
 * prototypes from functions in execScan.c
 */
typedef TupleTableSlot *(*VExecScanAccessMtd) (VectorScanState *node);
typedef bool (*VExecScanRecheckMtd) (VectorScanState *node, TupleTableSlot *slot);

TupleTableSlot *
VExecScan(VectorScanState* node, VExecScanAccessMtd accessMtd,
			VExecScanRecheckMtd recheckMtd);
#endif
