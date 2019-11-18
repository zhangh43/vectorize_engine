#ifndef VECTOR_ENGINE_VTYPE_VPSEUDO_H
#define VECTOR_ENGINE_VTYPE_VPSEUDO_H
#include "postgres.h"
#include "fmgr.h"
typedef struct vtype vany;
extern vany *buildvany(int dim, bool *skip);

extern Datum vany_in(PG_FUNCTION_ARGS);
extern Datum vany_out(PG_FUNCTION_ARGS);

#endif
