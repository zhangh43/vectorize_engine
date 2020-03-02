/*-------------------------------------------------------------------------
 *
 * nodeSeqscan.h
 *
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeSeqscan.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef VECTOR_ENGINE_NODE_SCAN_H
#define VECTOR_ENGINE_NODE_SCAN_H

#include "access/parallel.h"
#include "nodes/execnodes.h"
#include "nodes/plannodes.h"

/*
 * VectorScanState - state object of vectorscan on executor.
 */
typedef struct VectorScanState
{
	CustomScanState	css;

	/* Attributes for vectorization */
	SeqScanState	*seqstate;
	bool		scanFinish;
} VectorScanState;

extern CustomScan *MakeCustomScanForSeqScan(void);
extern void InitVectorScan(void);

extern SeqScanState *VExecInitSeqScan(SeqScan *node, EState *estate, int eflags);
extern void VExecEndSeqScan(VectorScanState *node);
extern void VExecReScanSeqScan(VectorScanState *node);

/* parallel scan support */
extern void VExecSeqScanEstimate(SeqScanState *node, ParallelContext *pcxt);
extern void VExecSeqScanInitializeDSM(SeqScanState *node, ParallelContext *pcxt);
extern void VExecSeqScanReInitializeDSM(SeqScanState *node, ParallelContext *pcxt);
extern void VExecSeqScanInitializeWorker(SeqScanState *node,
										ParallelWorkerContext *pwcxt);

#endif   /* VECTOR_ENGINE_SCAN_H */
