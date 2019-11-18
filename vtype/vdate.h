#ifndef VECTOR_ENGINE_VTYPE_VDATE_H
#define VECTOR_ENGINE_VTYPE_VDATE_H
#include "postgres.h"
#include "fmgr.h"
typedef struct vtype vdate;
extern vdate* buildvdate(int dim, bool *skip);

/* vdate oper op const */
extern Datum vdate_mi_interval(PG_FUNCTION_ARGS);

/* vdate oper cmp const */
extern Datum vdate_le_timestamp(PG_FUNCTION_ARGS);
extern Datum vdate_le(PG_FUNCTION_ARGS);

extern Datum vdate_in(PG_FUNCTION_ARGS);
extern Datum vdate_out(PG_FUNCTION_ARGS);

#endif
