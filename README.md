## About
Vectorize Execution is an extension for Postgres which utilize vectorized technique to accelerate query execution.

Vectorize Execution is based on Postgres 9.6 now, and will support newer Postgres version soon.

## Design
Below are features in our design.
1.  Pure extension. We didn't hack any code into postgres kernel.
2.  CustomScan node. We use CustomScan framework to replace original executor node such as SeqScan, Agg etc.
    Based on CustomScan, we could extend the CustomScanState, BeginCustomScan(), ExecCustomScan(), EndCustomScan()
    interface and implement vectorize executor node.
3.  Post planner hook. After plan is generated, we use plan_tree_walker to traverse the plan tree and check whether
    it could be vectorized. If yes, we will replace non-vectorized node(seqscan, agg etc.) with vectorized node(in
    form of customscan node) and use vectorized executor. If no, we will revert to the original plan and use non-vectorized executor.
4.  Inherit original executor code. Instead of rewriting the whole executor, we choose a more smooth method to modify
    current Postgres executor node and make it vectorized. We copy the current executor node into our extension, and
    add vectorize logic based on it. When Postgres enchance its executor, we could relatively easily merge them back.
5.  Pluggable storage. Postgres has supported pluggable storage now. TupleTableSlot is refactored as abstract struct
    TupleTableSlotOps. We will implement VectorTupleTableSlot in our extension when we upgrade the extension to latest PG.

## Usage
1.  Add GCC SIMD support option in configure when building Postgres. `-march=native`
2.  Build & Install.  `cd vectorize_engine; make install`
3.  Config postgres.conf & Restart database.  `shared_preload_libraries = 'vectorize_engine'`
4.  Run test.  `make installcheck`
5.  Initialize at database level. `create extension vectorize_engine;`
6.  Enable by GUC(default off). `set enable_vectorize_engine to on;`

## Performance
We run TPC-H 10G Q1 on machine at GCP(24G memory, 8 Core Intel(R) Xeon(R) CPU @ 2.20GHz).

standard PG run 50s and PG with vectorize engine version run 28s.

lineitem is stored as heap table with schema is as follows
```
              Table "public.lineitem"
     Column      |         Type          | Modifiers
-----------------+-----------------------+-----------
 l_orderkey      | bigint                | not null
 l_partkey       | integer               | not null
 l_suppkey       | integer               | not null
 l_linenumber    | integer               | not null
 l_quantity      | double precision      | not null
 l_extendedprice | double precision      | not null
 l_discount      | double precision      | not null
 l_tax           | double precision      | not null
 l_returnflag    | character(1)          | not null
 l_linestatus    | character(1)          | not null
 l_shipdate      | date                  | not null
 l_commitdate    | date                  | not null
 l_receiptdate   | date                  | not null
 l_shipinstruct  | character(25)         | not null
 l_shipmode      | character(10)         | not null
 l_comment       | character varying(44) | not null
```


TPC-H Q1 is
```
select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(l_discount) as count_order
from
    lineitem1
where
    l_shipdate <= date '1998-12-01' - interval '106 day'
group by
    l_returnflag,
    l_linestatus;
```

