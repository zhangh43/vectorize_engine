#ifndef VECTOR_ENGINE_VTYPE_VTIMESTAMP_H
#define VECTOR_ENGINE_VTYPE_VTIMESTAMP_H
#include "postgres.h"

#include "fmgr.h"
#include "utils/datetime.h"
#include "utils/timestamp.h"

#include "vtype.h"


typedef vtype vtimestamp;

extern vtimestamp* buildvtimestamp(int dim, bool *skip);

extern Datum vtimestamp_timestamp_cmp_internal(vtimestamp *vdt1, Timestamp dt2);

extern Datum vtimestamp_mi_interval(PG_FUNCTION_ARGS);


extern Datum vtimestamp_pl_interval(PG_FUNCTION_ARGS);

extern Datum vtimestamp_in(PG_FUNCTION_ARGS);
extern Datum vtimestamp_out(PG_FUNCTION_ARGS);
#endif
