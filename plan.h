/*
 * vwalkers.h
 */

#ifndef VECTOR_ENGINE_PLAN_H_
#define VECTOR_ENGINE_PLAN_H_

typedef struct VectorizedContext
{
	int* maxAttvarno;
	int  level;
	bool is_target_list;
} VectorizedContext;

extern Plan* ReplacePlanNodeWalker(Node *node, VectorizedContext* ctx);
extern List* CustomBuildTlist(List* tlist);

#endif /* VECTOR_ENGINE_PLAN_H_ */
