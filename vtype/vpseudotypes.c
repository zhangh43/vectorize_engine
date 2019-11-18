#include "postgres.h"
#include "catalog/pg_type.h"
#include "vpseudotypes.h"
#include "vtype.h"

PG_FUNCTION_INFO_V1(vany_in);
PG_FUNCTION_INFO_V1(vany_out);


vany* buildvany(int dim, bool *skip)
{
	return (vany *)buildvtype(ANYOID, dim, skip);	
}

Datum vany_in(PG_FUNCTION_ARGS)
{
	elog(ERROR, "vany_in not supported");
}

Datum vany_out(PG_FUNCTION_ARGS)
{
	elog(ERROR, "vany_out not supported");
}
