/*-------------------------------------------------------------------------
 *
 * execExpr.h
 *	  Low level infrastructure related to expression evaluation
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/execExpr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VECTOR_ENGINE_EXEC_EXPR_H
#define VECTOR_ENGINE_EXEC_EXPR_H

#include "executor/nodeAgg.h"
#include "nodes/execnodes.h"

#include "execExpr.h"
#include "tuptable.h"

/* functions in execExprInterp.c */
extern void VectorExecReadyInterpretedExpr(ExprState *state);
extern ExprEvalOp VectorExecEvalStepOp(ExprState *state, ExprEvalStep *op);

extern Datum VectorExecInterpExprStillValid(ExprState *state, ExprContext *econtext, bool *isNull);
extern void VectorCheckExprStillValid(ExprState *state, ExprContext *econtext);

#endif							/* EXEC_EXPR_H */
