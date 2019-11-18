--
-- Regression Tests for Custom Plan APIs
--

-- construction of test data

CREATE TABLE t1 (
    a   int,
	b	double precision
);
INSERT INTO t1 SELECT generate_series(1,3), 2.3;
INSERT INTO t1 SELECT generate_series(1,3), 3.3;
INSERT INTO t1 SELECT generate_series(1,3), 4.3;
VACUUM ANALYZE t1;

create extension vectorize_engine;
SET enable_vectorize_engine TO on;
SELECT * FROM t1;
SELECT b FROM t1;
SELECT b+1 FROM t1;
SELECT count(b) FROM t1;
SELECT a, sum(b), avg(b)  FROM t1 group by a;
SELECT a, sum(b), avg(b)  FROM t1 where a < 3 group by a;


drop extension vectorize_engine;
