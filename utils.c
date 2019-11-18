#include "postgres.h"

#include "catalog/namespace.h"
#include "executor/executor.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/hsearch.h"
#include "utils.h"

typedef struct VecTypeHashEntry
{
	Oid src;
	Oid dest;
}VecTypeHashEntry;

/* Map between the vectorized types and non-vectorized types */
static HTAB *hashMapN2V = NULL;
static HTAB *hashMapV2N = NULL;

#define BUILTIN_TYPE_NUM 12
#define TYPE_HASH_TABLE_SIZE 64
const char *typenames[] = { "any", "int2", "int4", "int8", "float4", "float8",
							"bool", "text", "date", "bpchar", "timestamp", "varchar"};
const char *vtypenames[] = { "vany", "vint2", "vint4", "vint8", "vfloat4",
							"vfloat8", "vbool", "vtext", "vdate", "vbpchar",
							"vtimestamp","vvarchar"};


/*
 * Clear common CustomScanState, since we would
 * use custom scan to do agg, hash etc.
 */
void
ClearCustomScanState(CustomScanState *node)
{
	/* Free the exprcontext */
	ExecFreeExprContext(&node->ss.ps);

	/* Clean out the tuple table */
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	/* Close the heap relation */
	if (node->ss.ss_currentRelation)
		ExecCloseScanRelation(node->ss.ss_currentRelation);
}


/*
 * map non-vectorized type to vectorized type.
 * To scan the PG_TYPE is inefficient, so we create a hashtable to map
 * the vectorized type and non-vectorized types.
 */
Oid GetVtype(Oid ntype)
{
	VecTypeHashEntry *entry = NULL;
	bool found = false;

	/* construct the hash table */
	if(NULL == hashMapN2V)
	{
		HASHCTL		hash_ctl;

		MemSet(&hash_ctl, 0, sizeof(hash_ctl));

		hash_ctl.keysize = sizeof(Oid);
		hash_ctl.entrysize = sizeof(VecTypeHashEntry);
		hash_ctl.hash = oid_hash;

		hashMapN2V = hash_create("vectorized_n2v",TYPE_HASH_TABLE_SIZE,
								&hash_ctl, HASH_ELEM | HASH_FUNCTION);
	}

	/* insert supported built-in type and vtypes */
	{
		int		i;
		Oid		vtypid;
		Oid		typid;
		for (i = 0; i < BUILTIN_TYPE_NUM; i++)
		{
			vtypid = TypenameGetTypid(vtypenames[i]);
			typid = TypenameGetTypid(typenames[i]);

			if (vtypid == InvalidOid)
				return InvalidOid;
			/* insert int4->vint4 mapping manually, may construct from catalog in future */
			entry = hash_search(hashMapN2V, &typid, HASH_ENTER, &found);
			entry->dest = vtypid;
		}
	}

	/* find the vectorized type in hash table */
	entry = hash_search(hashMapN2V, &ntype, HASH_FIND, &found);
	if(found)
		return entry->dest;

	return InvalidOid;
}



/*
 * map vectorized type to non-vectorized type.
 * To scan the PG_TYPE is inefficient, so we create a hashtable to map
 * the vectorized type and non-vectorized types.
 */
Oid GetNtype(Oid vtype)
{
	VecTypeHashEntry *entry = NULL;
	bool found = false;

	/* construct the hash table */
	if(NULL == hashMapV2N)
	{
		HASHCTL		hash_ctl;

		MemSet(&hash_ctl, 0, sizeof(hash_ctl));

		hash_ctl.keysize = sizeof(Oid);
		hash_ctl.entrysize = sizeof(VecTypeHashEntry);
		hash_ctl.hash = oid_hash;

		hashMapV2N = hash_create("vectorized_v2n", TYPE_HASH_TABLE_SIZE,
								&hash_ctl, HASH_ELEM | HASH_FUNCTION);
	}

	/* insert supported built-in type and vtypes */
	{
		int		i;
		Oid		vtypid;
		Oid		typid;
		for (i = 0; i < BUILTIN_TYPE_NUM; i++)
		{
			vtypid = TypenameGetTypid(vtypenames[i]);
			typid = TypenameGetTypid(typenames[i]);

			if (vtypid == InvalidOid)
				return InvalidOid;
			entry = hash_search(hashMapV2N, &vtypid, HASH_ENTER, &found);
			entry->dest = typid;
		}
	}

	/* find the vectorized type in hash table */
	entry = hash_search(hashMapV2N, &vtype, HASH_FIND, &found);
	if(found)
		return entry->dest;

	return InvalidOid;
}

Oid
GetTupDescAttVType(TupleDesc tupdesc, int i)
{
	Form_pg_attribute att = tupdesc->attrs[i];
	return GetVtype(att->atttypid);
}
