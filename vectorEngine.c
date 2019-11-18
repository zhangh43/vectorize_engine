/*-------------------------------------------------------------------------
 *
 * VectorEngine.c
 *	  Portal of vertorized engine for Postgres.
 *
 * Copyright (c) 2019-Present Pivotal Software, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "optimizer/planner.h"
#include "executor/nodeCustom.h"
#include "utils/guc.h"

#include "nodeUnbatch.h"
#include "nodeSeqscan.h"
#include "nodeAgg.h"
#include "plan.h"

PG_MODULE_MAGIC;

/* static variables */
static bool					enable_vectorize_engine;
static bool					enable_vectorize_notice;
static planner_hook_type    planner_hook_next;

/* static functionss */
static PlannedStmt *vector_post_planner(Query *parse, int cursorOptions,
									ParamListInfo boundParams);

void	_PG_init(void);

static PlannedStmt *
vector_post_planner(Query	*parse,
					int		cursorOptions,
					ParamListInfo	boundParams)
{
	PlannedStmt	*stmt;
	Plan		*savedPlanTree;
	List		*savedSubplan;

	if (planner_hook_next)
		stmt = planner_hook_next(parse, cursorOptions, boundParams);
	else
		stmt = standard_planner(parse, cursorOptions, boundParams);

	if (!enable_vectorize_engine)
		return stmt;

	/* modify plan by using vectorized nodes */
	savedPlanTree = stmt->planTree;
	savedSubplan = stmt->subplans;

	PG_TRY();
	{
		List		*subplans = NULL;
		ListCell	*cell;

		stmt->planTree = ReplacePlanNodeWalker((Node *) stmt->planTree);

		foreach(cell, stmt->subplans)
		{
			Plan	*subplan = ReplacePlanNodeWalker((Node *)lfirst(cell));
			subplans = lappend(subplans, subplan);
		}
		stmt->subplans = subplans;

		/* 
		 * vectorize executor exchange batch of tuples between plan nodes
		 * add unbatch node at top to convert batch to row and send to client.
		 */
		stmt->planTree = AddUnbatchNodeAtTop(stmt->planTree);
	}
	PG_CATCH();
	{
		ErrorData  *edata;
		edata = CopyErrorData();
		FlushErrorState();
		if (enable_vectorize_notice)
			ereport(NOTICE,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("query can't be vectorized"),
					 errdetail("%s", edata->message)));
		stmt->planTree = savedPlanTree;
		stmt->subplans = savedSubplan;
	}
	PG_END_TRY();

    return stmt;
}

void
_PG_init(void)
{
	elog(LOG, "Initialize vectorized extension");

	/* Register customscan node for vectorized scan and agg */
	InitVectorScan();
	InitVectorAgg();
	InitUnbatch();

    /* planner hook registration */
    planner_hook_next = planner_hook;
    planner_hook = vector_post_planner;

	DefineCustomBoolVariable("enable_vectorize_engine",
							 "Enables vectorize engine.",
							 NULL,
							 &enable_vectorize_engine,
							 false,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("enable_vectorize_notice",
							 "Enables vectorize engine.",
							 NULL,
							 &enable_vectorize_notice,
							 true,
							 PGC_USERSET,
							 GUC_NOT_IN_SAMPLE,
							 NULL, NULL, NULL);
}
