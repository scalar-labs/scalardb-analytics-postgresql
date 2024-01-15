\getenv abs_srcdir PG_ABS_SRCDIR
\set config_file_path :abs_srcdir '/test/client.properties'

CREATE EXTENSION scalardb_fdw;

CREATE SERVER scalardb FOREIGN DATA WRAPPER scalardb_fdw OPTIONS (
    config_file_path :'config_file_path'
);


CREATE USER MAPPING FOR PUBLIC SERVER scalardb;

CREATE FOREIGN TABLE cassandrans_test (
    c_pk int,
    c_ck1 int,
    c_ck2 int,
    c_boolean_col boolean,
    c_int_col int,
    c_bigint_col bigint,
    c_float_col float,
    c_double_col double precision,
    c_text_col text,
    c_blob_col bytea
) SERVER scalardb OPTIONS (
    namespace 'cassandrans',
    table_name 'test'
);

select * from cassandrans_test;

CREATE FOREIGN TABLE postgresns_test (
    p_pk int,
    p_ck1 int,
    p_ck2 int,
    p_boolean_col boolean,
    p_int_col int,
    p_bigint_col bigint,
    p_float_col float,
    p_double_col double precision,
    p_text_col text,
    p_blob_col bytea
) SERVER scalardb OPTIONS (
    namespace 'postgresns',
    table_name 'test'
);

select * from postgresns_test;

CREATE FOREIGN TABLE postgresns_null_test (
    p_pk int,
    p_ck1 int,
    p_ck2 int,
    p_boolean_col boolean,
    p_int_col int,
    p_bigint_col bigint,
    p_float_col float,
    p_double_col double precision,
    p_text_col text,
    p_blob_col bytea
) SERVER scalardb OPTIONS (
    namespace 'postgresns',
    table_name 'null_test'
);

select * from postgresns_null_test;

select p_pk from postgresns_test;

select proname, prosrc, probin from pg_proc where proname = 'scalardb_fdw_get_jar_file_path';

explain select p_pk from postgresns_test;

explain verbose select p_pk from postgresns_test;

--
-- Prepare tables for testing data types
--
CREATE FOREIGN TABLE boolean_test (
    pk bool,
    ck bool,
    index bool,
    col bool
) SERVER scalardb OPTIONS (
    namespace 'postgresns',
    table_name 'boolean_test'
);

CREATE FOREIGN TABLE int_test (
    pk int,
    ck int,
    index int,
    col int
) SERVER scalardb OPTIONS (
    namespace 'postgresns',
    table_name 'int_test'
);

CREATE FOREIGN TABLE bigint_test (
    pk bigint,
    ck bigint,
    index bigint,
    col bigint
) SERVER scalardb OPTIONS (
    namespace 'postgresns',
    table_name 'bigint_test'
);

CREATE FOREIGN TABLE float_test (
    pk float,
    ck float,
    index float,
    col float
) SERVER scalardb OPTIONS (
    namespace 'postgresns',
    table_name 'float_test'
);

CREATE FOREIGN TABLE double_test (
    pk double precision,
    ck double precision,
    index double precision,
    col double precision
) SERVER scalardb OPTIONS (
    namespace 'postgresns',
    table_name 'double_test'
);

CREATE FOREIGN TABLE text_test (
    pk text,
    ck text,
    index text,
    col text
) SERVER scalardb OPTIONS (
    namespace 'postgresns',
    table_name 'text_test'
);

CREATE FOREIGN TABLE blob_test (
    pk bytea,
    ck bytea,
    index bytea,
    col bytea
) SERVER scalardb OPTIONS (
    namespace 'postgresns',
    table_name 'blob_test'
);

-- Test filtering push-down on partition keys
explain verbose select * from boolean_test where pk = true;
explain verbose select * from int_test where pk = 1;
explain verbose select * from bigint_test where pk = 1;
explain verbose select * from float_test where pk = 1.0;
explain verbose select * from double_test where pk = 1.0;
explain verbose select * from text_test where pk = '1';
explain verbose select * from blob_test where pk = E'\\xDEADBEEF';

-- Test filtering push-down on clustering keys
explain verbose select * from boolean_test where pk = true AND ck = true;
explain verbose select * from int_test where pk = 1 AND ck = 1;
explain verbose select * from bigint_test where pk = 1 AND ck = 1;
explain verbose select * from float_test where pk = 1.0 AND ck = 1.0;
explain verbose select * from double_test where pk = 1.0 AND ck = 1.0;
explain verbose select * from text_test where pk = '1' AND ck = '1';
explain verbose select * from blob_test where pk = E'\\xDEADBEEF' AND ck = E'\\xDEADBEEF';

-- Test filtering push-down on secondary indexes 
explain verbose select * from boolean_test where index = true;
explain verbose select * from int_test where index = 1;
explain verbose select * from bigint_test where index = 1;
explain verbose select * from float_test where index = 1.0;
explain verbose select * from double_test where index = 1.0;
explain verbose select * from text_test where index = '1';
explain verbose select * from blob_test where index = E'\\xDEADBEEF';

-- Test filtering push-down for boolean conditions
explain verbose select * from boolean_test where pk;
explain verbose select * from boolean_test where not pk;

-- Test priorities of filtering push-down
-- Conditions of partition keys have higher priorities than indexed columns
explain verbose select * from boolean_test where pk and index;

-- Test clustering key push-down inclusiveness
explain verbose select * from postgresns_test where p_pk = 1 AND p_ck1 < 1 ;
select * from postgresns_test where p_pk = 1 AND p_ck1 < 1;
explain verbose select * from postgresns_test where p_pk = 1 AND p_ck1 <= 1 ;
select * from postgresns_test where p_pk = 1 AND p_ck1 <= 1;
explain verbose select * from postgresns_test where p_pk = 1 AND p_ck1 > 1 ;
select * from postgresns_test where p_pk = 1 AND p_ck1 > 1;
explain verbose select * from postgresns_test where p_pk = 1 AND p_ck1 >= 1 ;
select * from postgresns_test where p_pk = 1 AND p_ck1 >= 1;

-- 
-- Test multi-column clustering key push-down
--
-- Should be pushed down if all clustering keys are specified
explain verbose select * from postgresns_test where p_pk = 1 AND p_ck1 = 1 AND p_ck2 = 1;
select * from postgresns_test where p_pk = 1 AND p_ck1 = 1 AND p_ck2 = 1;
-- Should NOT be pushed down if not partition key scan
explain verbose select * from postgresns_test where p_ck1 = 1 AND p_ck2 = 1;
select * from postgresns_test where p_ck1 = 1 AND p_ck2 = 1;
-- Should be pushed down if first part of clustering keys are specified
explain verbose select * from postgresns_test where p_pk = 1 AND p_ck1 = 1;
select * from postgresns_test where p_pk = 1 AND p_ck1 = 1;
-- Should NOT be pushed down if first part of clustering keys are not specified
explain verbose select * from postgresns_test where p_pk = 1 AND p_ck2 = 1;
select * from postgresns_test where p_pk = 1 AND p_ck2 = 1;

-- 
-- Test multi-column clustering key push-down with range conditions
--
-- If the range condition exists, only that condition and the condtions before it are pushed down.
explain verbose select * from postgresns_test where p_pk = 1 AND p_ck1 > 0 AND p_ck2 > 0;
select * from postgresns_test where p_pk = 1 AND p_ck1 > 0 AND p_ck2 > 0;
-- If the last part of clustering key condtion is range condition AND:
-- - that condition is the first clustering key condition, the condition is pushed down 
--   regardless of it is one-sided or not.
explain verbose select * from postgresns_test where p_pk = 1 AND p_ck1 > 0;
select * from postgresns_test where p_pk = 1 AND p_ck1 > 0;
-- - if the last part of clustering key condition is one-sided (one of start or end),
--   only the rest part of the condition (i.e. equal conditions) are pushed down.
explain verbose select * from postgresns_test where p_pk = 1 AND p_ck1 = 1 AND p_ck2 > 0;
select * from postgresns_test where p_pk = 1 AND p_ck1 = 1 AND p_ck2 > 0;

-- - if the last part of clustering key condition is both-sided (both start and end),
--   the whole condition is pushed down.
explain verbose select * from postgresns_test where p_pk = 1 AND p_ck1 = 1 AND p_ck2 > 0 AND p_ck2 <= 1;
select * from postgresns_test where p_pk = 1 AND p_ck1 = 1 AND p_ck2 > 0 AND p_ck2 <= 1;

-- 
-- Test sort push-down
--
-- Sorting can be pushed down only for partition key scan
explain verbose select * from int_test where pk = 1 order by ck; --OK
explain verbose select * from int_test order by ck; --NG
explain verbose select * from int_test where index = 1 order by ck; --NG

-- Whole ORDER BY clause must be able to be represented as clustering key orderings
explain verbose select * from postgresns_test where p_pk = 1 order by p_ck1; -- OK
explain verbose select * from postgresns_test where p_pk = 1 order by p_ck1, p_ck2; --OK
explain verbose select * from postgresns_test where p_pk = 1 order by p_ck1, p_int_col; --NG
explain verbose select * from postgresns_test where p_pk = 1 order by p_ck1, p_ck2, p_int_col; --NG

-- Whole sort order must be in the same order or inversed order with the clustering key order or the table
explain verbose select * from postgresns_test where p_pk = 1 order by p_ck1 ASC; -- OK
explain verbose select * from postgresns_test where p_pk = 1 order by p_ck1 DESC; -- OK
explain verbose select * from postgresns_test where p_pk = 1 order by p_ck1 ASC, p_ck2 ASC; -- OK
explain verbose select * from postgresns_test where p_pk = 1 order by p_ck1 DESC, p_ck2 DESC; -- OK
explain verbose select * from postgresns_test where p_pk = 1 order by p_ck1 ASC, p_ck2 DESC; -- NG

-- Columns that are required to evaluate WHERE clause locally at PostgreSQL side must be returned from the remote server
-- - ForeignScan on cassandrans_test must return c_boolean_col
explain verbose select p_pk from postgresns_test inner join cassandrans_test on p_pk = c_pk where c_boolean_col;
-- - The query must return 1 row
select p_pk from postgresns_test inner join cassandrans_test on p_pk = c_pk where c_boolean_col;
