MODULE_big = vectorize_engine

EXTENSION = vectorize_engine
DATA = vectorize_engine--1.0.sql

PGFILEDESC = "vectorize engine for PostgreSQL"

REGRESS = vectorize_engine

OBJS += vectorEngine.o plan.o utils.o execScan.o execTuples.o nodeSeqscan.o nodeUnbatch.o heapam_handler.o heapam.o execExprInterp.o execExpr.o execUtils.o
#OBJS += nodeAgg.o
OBJS += vtype/vtype.o vtype/vtimestamp.o vtype/vint.o vtype/vfloat.o vtype/vpseudotypes.o vtype/vvarchar.o vtype/vdate.o

# print vectorize info when compile
# PG_CFLAGS = -fopt-info-vec

PG_CFLAGS = -Wno-int-in-bool-context
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
