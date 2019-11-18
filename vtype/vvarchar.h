#ifndef VECTOR_ENGINE_VTYPE_VVARCHAR_H
#define VECTOR_ENGINE_VTYPE_VVARCHAR_H
#include "postgres.h"
#include "fmgr.h"
typedef struct vtype vvarchar;
extern vvarchar *buildvvarchar(int dim, bool *skip);


extern Datum vvarcharin(PG_FUNCTION_ARGS);
extern Datum vvarcharout(PG_FUNCTION_ARGS);

#endif
