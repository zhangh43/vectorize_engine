#include "vvarchar.h"
#include "vtype.h"

PG_FUNCTION_INFO_V1(vvarchar_in);
PG_FUNCTION_INFO_V1(vvarchar_out);


Datum vvarcharin(PG_FUNCTION_ARGS)
{
	    elog(ERROR, "vvarchar_in not supported");
}

Datum vvarcharout(PG_FUNCTION_ARGS)
{
	    elog(ERROR, "vvarchar_out not supported");
}
