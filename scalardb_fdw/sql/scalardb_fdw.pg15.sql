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
 c_pk | c_ck1 | c_ck2 | c_boolean_col | c_int_col | c_bigint_col | c_float_col | c_double_col | c_text_col | c_blob_col 
------+-------+-------+---------------+-----------+--------------+-------------+--------------+------------+------------
    1 |     1 |     1 | t             |         1 |            1 |           0 |            1 | test       | \x010203
(1 row)

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
 p_pk | p_ck1 | p_ck2 | p_boolean_col | p_int_col | p_bigint_col | p_float_col | p_double_col | p_text_col | p_blob_col 
------+-------+-------+---------------+-----------+--------------+-------------+--------------+------------+------------
    1 |     1 |     1 | t             |         1 |            1 |           0 |            1 | test       | \x010203
(1 row)

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
 p_pk | p_ck1 | p_ck2 | p_boolean_col | p_int_col | p_bigint_col | p_float_col | p_double_col | p_text_col | p_blob_col 
------+-------+-------+---------------+-----------+--------------+-------------+--------------+------------+------------
    1 |     1 |     1 |               |           |              |             |              |            | 
(1 row)

select p_pk from postgresns_test;
 p_pk 
------
    1
(1 row)

select proname, prosrc, probin from pg_proc where proname = 'scalardb_fdw_get_jar_file_path';
            proname             |             prosrc             |        probin        
--------------------------------+--------------------------------+----------------------
 scalardb_fdw_get_jar_file_path | scalardb_fdw_get_jar_file_path | $libdir/scalardb_fdw
(1 row)

explain select p_pk from postgresns_test;
                              QUERY PLAN                               
-----------------------------------------------------------------------
 Foreign Scan on postgresns_test  (cost=0.00..29.26 rows=2926 width=4)
   ScalarDB Namespace: postgresns
   ScalarDB Table: test
(3 rows)

explain verbose select p_pk from postgresns_test;
                                  QUERY PLAN                                  
------------------------------------------------------------------------------
 Foreign Scan on public.postgresns_test  (cost=0.00..29.26 rows=2926 width=4)
   Output: p_pk
   ScalarDB Namespace: postgresns
   ScalarDB Table: test
   ScalarDB Scan Type: all
   ScalarDB Scan Attribute: ("p_pk")
(6 rows)

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
                                QUERY PLAN                                 
---------------------------------------------------------------------------
 Foreign Scan on public.boolean_test  (cost=0.00..29.26 rows=1463 width=4)
   Output: pk, ck, index, col
   ScalarDB Namespace: postgresns
   ScalarDB Table: boolean_test
   ScalarDB Scan Type: partition key
   ScalarDB Scan Condition: pk = true
   ScalarDB Scan Attribute: ("pk" "ck" "index" "col")
(7 rows)

explain verbose select * from int_test where pk = 1;
                              QUERY PLAN                              
----------------------------------------------------------------------
 Foreign Scan on public.int_test  (cost=0.00..25.60 rows=10 width=16)
   Output: pk, ck, index, col
   ScalarDB Namespace: postgresns
   ScalarDB Table: int_test
   ScalarDB Scan Type: partition key
   ScalarDB Scan Condition: pk = 1
   ScalarDB Scan Attribute: ("pk" "ck" "index" "col")
(7 rows)

explain verbose select * from bigint_test where pk = 1;
                               QUERY PLAN                               
------------------------------------------------------------------------
 Foreign Scan on public.bigint_test  (cost=0.00..18.29 rows=7 width=32)
   Output: pk, ck, index, col
   ScalarDB Namespace: postgresns
   ScalarDB Table: bigint_test
   ScalarDB Scan Type: partition key
   ScalarDB Scan Condition: pk = 1
   ScalarDB Scan Attribute: ("pk" "ck" "index" "col")
(7 rows)

explain verbose select * from float_test where pk = 1.0;
                              QUERY PLAN                               
-----------------------------------------------------------------------
 Foreign Scan on public.float_test  (cost=0.00..18.29 rows=7 width=32)
   Output: pk, ck, index, col
   ScalarDB Namespace: postgresns
   ScalarDB Table: float_test
   ScalarDB Scan Type: partition key
   ScalarDB Scan Condition: pk = 1
   ScalarDB Scan Attribute: ("pk" "ck" "index" "col")
(7 rows)

explain verbose select * from double_test where pk = 1.0;
                               QUERY PLAN                               
------------------------------------------------------------------------
 Foreign Scan on public.double_test  (cost=0.00..18.29 rows=7 width=32)
   Output: pk, ck, index, col
   ScalarDB Namespace: postgresns
   ScalarDB Table: double_test
   ScalarDB Scan Type: partition key
   ScalarDB Scan Condition: pk = 1
   ScalarDB Scan Attribute: ("pk" "ck" "index" "col")
(7 rows)

explain verbose select * from text_test where pk = '1';
                              QUERY PLAN                              
----------------------------------------------------------------------
 Foreign Scan on public.text_test  (cost=0.00..6.74 rows=3 width=128)
   Output: pk, ck, index, col
   ScalarDB Namespace: postgresns
   ScalarDB Table: text_test
   ScalarDB Scan Type: partition key
   ScalarDB Scan Condition: pk = '1'
   ScalarDB Scan Attribute: ("pk" "ck" "index" "col")
(7 rows)

explain verbose select * from blob_test where pk = E'\\xDEADBEEF';
                              QUERY PLAN                              
----------------------------------------------------------------------
 Foreign Scan on public.blob_test  (cost=0.00..6.74 rows=3 width=128)
   Output: pk, ck, index, col
   ScalarDB Namespace: postgresns
   ScalarDB Table: blob_test
   ScalarDB Scan Type: partition key
   ScalarDB Scan Condition: pk = E'\\xdeadbeef'
   ScalarDB Scan Attribute: ("pk" "ck" "index" "col")
(7 rows)

-- Test filtering push-down on clustering keys
explain verbose select * from boolean_test where pk = true AND ck = true;
                                QUERY PLAN                                
--------------------------------------------------------------------------
 Foreign Scan on public.boolean_test  (cost=0.00..29.26 rows=731 width=4)
   Output: pk, ck, index, col
   ScalarDB Namespace: postgresns
   ScalarDB Table: boolean_test
   ScalarDB Scan Type: partition key
   ScalarDB Scan Condition: pk = true
   ScalarDB Scan Start: ck = true
   ScalarDB Scan End: ck = true
   ScalarDB Scan Attribute: ("pk" "ck" "index" "col")
(9 rows)

explain verbose select * from int_test where pk = 1 AND ck = 1;
                             QUERY PLAN                              
---------------------------------------------------------------------
 Foreign Scan on public.int_test  (cost=0.00..30.72 rows=1 width=16)
   Output: pk, ck, index, col
   ScalarDB Namespace: postgresns
   ScalarDB Table: int_test
   ScalarDB Scan Type: partition key
   ScalarDB Scan Condition: pk = 1
   ScalarDB Scan Start: ck = 1
   ScalarDB Scan End: ck = 1
   ScalarDB Scan Attribute: ("pk" "ck" "index" "col")
(9 rows)

explain verbose select * from bigint_test where pk = 1 AND ck = 1;
                               QUERY PLAN                               
------------------------------------------------------------------------
 Foreign Scan on public.bigint_test  (cost=0.00..21.94 rows=1 width=32)
   Output: pk, ck, index, col
   ScalarDB Namespace: postgresns
   ScalarDB Table: bigint_test
   ScalarDB Scan Type: partition key
   ScalarDB Scan Condition: pk = 1
   ScalarDB Scan Start: ck = 1
   ScalarDB Scan End: ck = 1
   ScalarDB Scan Attribute: ("pk" "ck" "index" "col")
(9 rows)

explain verbose select * from float_test where pk = 1.0 AND ck = 1.0;
                              QUERY PLAN                               
-----------------------------------------------------------------------
 Foreign Scan on public.float_test  (cost=0.00..21.94 rows=1 width=32)
   Output: pk, ck, index, col
   ScalarDB Namespace: postgresns
   ScalarDB Table: float_test
   ScalarDB Scan Type: partition key
   ScalarDB Scan Condition: pk = 1
   ScalarDB Scan Start: ck = 1
   ScalarDB Scan End: ck = 1
   ScalarDB Scan Attribute: ("pk" "ck" "index" "col")
(9 rows)

explain verbose select * from double_test where pk = 1.0 AND ck = 1.0;
                               QUERY PLAN                               
------------------------------------------------------------------------
 Foreign Scan on public.double_test  (cost=0.00..21.94 rows=1 width=32)
   Output: pk, ck, index, col
   ScalarDB Namespace: postgresns
   ScalarDB Table: double_test
   ScalarDB Scan Type: partition key
   ScalarDB Scan Condition: pk = 1
   ScalarDB Scan Start: ck = 1
   ScalarDB Scan End: ck = 1
   ScalarDB Scan Attribute: ("pk" "ck" "index" "col")
(9 rows)

explain verbose select * from text_test where pk = '1' AND ck = '1';
                              QUERY PLAN                              
----------------------------------------------------------------------
 Foreign Scan on public.text_test  (cost=0.00..8.08 rows=1 width=128)
   Output: pk, ck, index, col
   ScalarDB Namespace: postgresns
   ScalarDB Table: text_test
   ScalarDB Scan Type: partition key
   ScalarDB Scan Condition: pk = '1'
   ScalarDB Scan Start: ck = '1'
   ScalarDB Scan End: ck = '1'
   ScalarDB Scan Attribute: ("pk" "ck" "index" "col")
(9 rows)

explain verbose select * from blob_test where pk = E'\\xDEADBEEF' AND ck = E'\\xDEADBEEF';
                              QUERY PLAN                              
----------------------------------------------------------------------
 Foreign Scan on public.blob_test  (cost=0.00..8.08 rows=1 width=128)
   Output: pk, ck, index, col
   ScalarDB Namespace: postgresns
   ScalarDB Table: blob_test
   ScalarDB Scan Type: partition key
   ScalarDB Scan Condition: pk = E'\\xdeadbeef'
   ScalarDB Scan Start: ck = E'\\xdeadbeef'
   ScalarDB Scan End: ck = E'\\xdeadbeef'
   ScalarDB Scan Attribute: ("pk" "ck" "index" "col")
(9 rows)

-- Test filtering push-down on secondary indexes 
explain verbose select * from boolean_test where index = true;
                                QUERY PLAN                                 
---------------------------------------------------------------------------
 Foreign Scan on public.boolean_test  (cost=0.00..29.26 rows=1463 width=4)
   Output: pk, ck, index, col
   ScalarDB Namespace: postgresns
   ScalarDB Table: boolean_test
   ScalarDB Scan Type: secondary index
   ScalarDB Scan Condition: index = true
   ScalarDB Scan Attribute: ("pk" "ck" "index" "col")
(7 rows)

explain verbose select * from int_test where index = 1;
                              QUERY PLAN                              
----------------------------------------------------------------------
 Foreign Scan on public.int_test  (cost=0.00..25.60 rows=10 width=16)
   Output: pk, ck, index, col
   ScalarDB Namespace: postgresns
   ScalarDB Table: int_test
   ScalarDB Scan Type: secondary index
   ScalarDB Scan Condition: index = 1
   ScalarDB Scan Attribute: ("pk" "ck" "index" "col")
(7 rows)

explain verbose select * from bigint_test where index = 1;
                               QUERY PLAN                               
------------------------------------------------------------------------
 Foreign Scan on public.bigint_test  (cost=0.00..18.29 rows=7 width=32)
   Output: pk, ck, index, col
   ScalarDB Namespace: postgresns
   ScalarDB Table: bigint_test
   ScalarDB Scan Type: secondary index
   ScalarDB Scan Condition: index = 1
   ScalarDB Scan Attribute: ("pk" "ck" "index" "col")
(7 rows)

explain verbose select * from float_test where index = 1.0;
                              QUERY PLAN                               
-----------------------------------------------------------------------
 Foreign Scan on public.float_test  (cost=0.00..18.29 rows=7 width=32)
   Output: pk, ck, index, col
   ScalarDB Namespace: postgresns
   ScalarDB Table: float_test
   ScalarDB Scan Type: secondary index
   ScalarDB Scan Condition: index = 1
   ScalarDB Scan Attribute: ("pk" "ck" "index" "col")
(7 rows)

explain verbose select * from double_test where index = 1.0;
                               QUERY PLAN                               
------------------------------------------------------------------------
 Foreign Scan on public.double_test  (cost=0.00..18.29 rows=7 width=32)
   Output: pk, ck, index, col
   ScalarDB Namespace: postgresns
   ScalarDB Table: double_test
   ScalarDB Scan Type: secondary index
   ScalarDB Scan Condition: index = 1
   ScalarDB Scan Attribute: ("pk" "ck" "index" "col")
(7 rows)

explain verbose select * from text_test where index = '1';
                              QUERY PLAN                              
----------------------------------------------------------------------
 Foreign Scan on public.text_test  (cost=0.00..6.74 rows=3 width=128)
   Output: pk, ck, index, col
   ScalarDB Namespace: postgresns
   ScalarDB Table: text_test
   ScalarDB Scan Type: secondary index
   ScalarDB Scan Condition: index = '1'
   ScalarDB Scan Attribute: ("pk" "ck" "index" "col")
(7 rows)

explain verbose select * from blob_test where index = E'\\xDEADBEEF';
                              QUERY PLAN                              
----------------------------------------------------------------------
 Foreign Scan on public.blob_test  (cost=0.00..6.74 rows=3 width=128)
   Output: pk, ck, index, col
   ScalarDB Namespace: postgresns
   ScalarDB Table: blob_test
   ScalarDB Scan Type: secondary index
   ScalarDB Scan Condition: index = E'\\xdeadbeef'
   ScalarDB Scan Attribute: ("pk" "ck" "index" "col")
(7 rows)

-- Test filtering push-down for boolean conditions
explain verbose select * from boolean_test where pk;
                                QUERY PLAN                                 
---------------------------------------------------------------------------
 Foreign Scan on public.boolean_test  (cost=0.00..29.26 rows=1463 width=4)
   Output: pk, ck, index, col
   ScalarDB Namespace: postgresns
   ScalarDB Table: boolean_test
   ScalarDB Scan Type: partition key
   ScalarDB Scan Condition: pk = true
   ScalarDB Scan Attribute: ("pk" "ck" "index" "col")
(7 rows)

explain verbose select * from boolean_test where not pk;
                                QUERY PLAN                                 
---------------------------------------------------------------------------
 Foreign Scan on public.boolean_test  (cost=0.00..29.26 rows=1463 width=4)
   Output: pk, ck, index, col
   ScalarDB Namespace: postgresns
   ScalarDB Table: boolean_test
   ScalarDB Scan Type: partition key
   ScalarDB Scan Condition: pk = false
   ScalarDB Scan Attribute: ("pk" "ck" "index" "col")
(7 rows)

-- Test priorities of filtering push-down
-- Conditions of partition keys have higher priorities than indexed columns
explain verbose select * from boolean_test where pk and index;
                                QUERY PLAN                                
--------------------------------------------------------------------------
 Foreign Scan on public.boolean_test  (cost=0.00..29.26 rows=731 width=4)
   Output: pk, ck, index, col
   Filter: boolean_test.index
   ScalarDB Namespace: postgresns
   ScalarDB Table: boolean_test
   ScalarDB Scan Type: partition key
   ScalarDB Scan Condition: pk = true
   ScalarDB Scan Attribute: ("pk" "ck" "index" "col")
(8 rows)

-- Test clustering key push-down inclusiveness
explain verbose select * from postgresns_test where p_pk = 1 AND p_ck1 < 1 ;
                                                                      QUERY PLAN                                                                       
-------------------------------------------------------------------------------------------------------------------------------------------------------
 Foreign Scan on public.postgresns_test  (cost=0.00..9.53 rows=1 width=105)
   Output: p_pk, p_ck1, p_ck2, p_boolean_col, p_int_col, p_bigint_col, p_float_col, p_double_col, p_text_col, p_blob_col
   ScalarDB Namespace: postgresns
   ScalarDB Table: test
   ScalarDB Scan Type: partition key
   ScalarDB Scan Condition: p_pk = 1
   ScalarDB Scan Start: p_ck1 < 1
   ScalarDB Scan Attribute: ("p_pk" "p_ck1" "p_ck2" "p_boolean_col" "p_int_col" "p_bigint_col" "p_float_col" "p_double_col" "p_text_col" "p_blob_col")
(8 rows)

select * from postgresns_test where p_pk = 1 AND p_ck1 < 1;
 p_pk | p_ck1 | p_ck2 | p_boolean_col | p_int_col | p_bigint_col | p_float_col | p_double_col | p_text_col | p_blob_col 
------+-------+-------+---------------+-----------+--------------+-------------+--------------+------------+------------
(0 rows)

explain verbose select * from postgresns_test where p_pk = 1 AND p_ck1 <= 1 ;
                                                                      QUERY PLAN                                                                       
-------------------------------------------------------------------------------------------------------------------------------------------------------
 Foreign Scan on public.postgresns_test  (cost=0.00..9.53 rows=1 width=105)
   Output: p_pk, p_ck1, p_ck2, p_boolean_col, p_int_col, p_bigint_col, p_float_col, p_double_col, p_text_col, p_blob_col
   ScalarDB Namespace: postgresns
   ScalarDB Table: test
   ScalarDB Scan Type: partition key
   ScalarDB Scan Condition: p_pk = 1
   ScalarDB Scan Start: p_ck1 <= 1
   ScalarDB Scan Attribute: ("p_pk" "p_ck1" "p_ck2" "p_boolean_col" "p_int_col" "p_bigint_col" "p_float_col" "p_double_col" "p_text_col" "p_blob_col")
(8 rows)

select * from postgresns_test where p_pk = 1 AND p_ck1 <= 1;
 p_pk | p_ck1 | p_ck2 | p_boolean_col | p_int_col | p_bigint_col | p_float_col | p_double_col | p_text_col | p_blob_col 
------+-------+-------+---------------+-----------+--------------+-------------+--------------+------------+------------
    1 |     1 |     1 | t             |         1 |            1 |           0 |            1 | test       | \x010203
(1 row)

explain verbose select * from postgresns_test where p_pk = 1 AND p_ck1 > 1 ;
                                                                      QUERY PLAN                                                                       
-------------------------------------------------------------------------------------------------------------------------------------------------------
 Foreign Scan on public.postgresns_test  (cost=0.00..9.53 rows=1 width=105)
   Output: p_pk, p_ck1, p_ck2, p_boolean_col, p_int_col, p_bigint_col, p_float_col, p_double_col, p_text_col, p_blob_col
   ScalarDB Namespace: postgresns
   ScalarDB Table: test
   ScalarDB Scan Type: partition key
   ScalarDB Scan Condition: p_pk = 1
   ScalarDB Scan End: p_ck1 > 1
   ScalarDB Scan Attribute: ("p_pk" "p_ck1" "p_ck2" "p_boolean_col" "p_int_col" "p_bigint_col" "p_float_col" "p_double_col" "p_text_col" "p_blob_col")
(8 rows)

select * from postgresns_test where p_pk = 1 AND p_ck1 > 1;
 p_pk | p_ck1 | p_ck2 | p_boolean_col | p_int_col | p_bigint_col | p_float_col | p_double_col | p_text_col | p_blob_col 
------+-------+-------+---------------+-----------+--------------+-------------+--------------+------------+------------
(0 rows)

explain verbose select * from postgresns_test where p_pk = 1 AND p_ck1 >= 1 ;
                                                                      QUERY PLAN                                                                       
-------------------------------------------------------------------------------------------------------------------------------------------------------
 Foreign Scan on public.postgresns_test  (cost=0.00..9.53 rows=1 width=105)
   Output: p_pk, p_ck1, p_ck2, p_boolean_col, p_int_col, p_bigint_col, p_float_col, p_double_col, p_text_col, p_blob_col
   ScalarDB Namespace: postgresns
   ScalarDB Table: test
   ScalarDB Scan Type: partition key
   ScalarDB Scan Condition: p_pk = 1
   ScalarDB Scan End: p_ck1 >= 1
   ScalarDB Scan Attribute: ("p_pk" "p_ck1" "p_ck2" "p_boolean_col" "p_int_col" "p_bigint_col" "p_float_col" "p_double_col" "p_text_col" "p_blob_col")
(8 rows)

select * from postgresns_test where p_pk = 1 AND p_ck1 >= 1;
 p_pk | p_ck1 | p_ck2 | p_boolean_col | p_int_col | p_bigint_col | p_float_col | p_double_col | p_text_col | p_blob_col 
------+-------+-------+---------------+-----------+--------------+-------------+--------------+------------+------------
    1 |     1 |     1 | t             |         1 |            1 |           0 |            1 | test       | \x010203
(1 row)

-- 
-- Test multi-column clustering key push-down
--
-- Should be pushed down if all clustering keys are specified
explain verbose select * from postgresns_test where p_pk = 1 AND p_ck1 = 1 AND p_ck2 = 1;
                                                                      QUERY PLAN                                                                       
-------------------------------------------------------------------------------------------------------------------------------------------------------
 Foreign Scan on public.postgresns_test  (cost=0.00..11.11 rows=1 width=105)
   Output: p_pk, p_ck1, p_ck2, p_boolean_col, p_int_col, p_bigint_col, p_float_col, p_double_col, p_text_col, p_blob_col
   ScalarDB Namespace: postgresns
   ScalarDB Table: test
   ScalarDB Scan Type: partition key
   ScalarDB Scan Condition: p_pk = 1
   ScalarDB Scan Start: p_ck1 = 1 AND p_ck2 = 1
   ScalarDB Scan End: p_ck1 = 1 AND p_ck2 = 1
   ScalarDB Scan Attribute: ("p_pk" "p_ck1" "p_ck2" "p_boolean_col" "p_int_col" "p_bigint_col" "p_float_col" "p_double_col" "p_text_col" "p_blob_col")
(9 rows)

select * from postgresns_test where p_pk = 1 AND p_ck1 = 1 AND p_ck2 = 1;
 p_pk | p_ck1 | p_ck2 | p_boolean_col | p_int_col | p_bigint_col | p_float_col | p_double_col | p_text_col | p_blob_col 
------+-------+-------+---------------+-----------+--------------+-------------+--------------+------------+------------
    1 |     1 |     1 | t             |         1 |            1 |           0 |            1 | test       | \x010203
(1 row)

-- Should NOT be pushed down if not partition key scan
explain verbose select * from postgresns_test where p_ck1 = 1 AND p_ck2 = 1;
                                                                      QUERY PLAN                                                                       
-------------------------------------------------------------------------------------------------------------------------------------------------------
 Foreign Scan on public.postgresns_test  (cost=0.00..9.53 rows=1 width=105)
   Output: p_pk, p_ck1, p_ck2, p_boolean_col, p_int_col, p_bigint_col, p_float_col, p_double_col, p_text_col, p_blob_col
   Filter: ((postgresns_test.p_ck1 = 1) AND (postgresns_test.p_ck2 = 1))
   ScalarDB Namespace: postgresns
   ScalarDB Table: test
   ScalarDB Scan Type: all
   ScalarDB Scan Attribute: ("p_pk" "p_ck1" "p_ck2" "p_boolean_col" "p_int_col" "p_bigint_col" "p_float_col" "p_double_col" "p_text_col" "p_blob_col")
(7 rows)

select * from postgresns_test where p_ck1 = 1 AND p_ck2 = 1;
 p_pk | p_ck1 | p_ck2 | p_boolean_col | p_int_col | p_bigint_col | p_float_col | p_double_col | p_text_col | p_blob_col 
------+-------+-------+---------------+-----------+--------------+-------------+--------------+------------+------------
    1 |     1 |     1 | t             |         1 |            1 |           0 |            1 | test       | \x010203
(1 row)

-- Should be pushed down if first part of clustering keys are specified
explain verbose select * from postgresns_test where p_pk = 1 AND p_ck1 = 1;
                                                                      QUERY PLAN                                                                       
-------------------------------------------------------------------------------------------------------------------------------------------------------
 Foreign Scan on public.postgresns_test  (cost=0.00..9.53 rows=1 width=105)
   Output: p_pk, p_ck1, p_ck2, p_boolean_col, p_int_col, p_bigint_col, p_float_col, p_double_col, p_text_col, p_blob_col
   ScalarDB Namespace: postgresns
   ScalarDB Table: test
   ScalarDB Scan Type: partition key
   ScalarDB Scan Condition: p_pk = 1
   ScalarDB Scan Start: p_ck1 = 1
   ScalarDB Scan End: p_ck1 = 1
   ScalarDB Scan Attribute: ("p_pk" "p_ck1" "p_ck2" "p_boolean_col" "p_int_col" "p_bigint_col" "p_float_col" "p_double_col" "p_text_col" "p_blob_col")
(9 rows)

select * from postgresns_test where p_pk = 1 AND p_ck1 = 1;
 p_pk | p_ck1 | p_ck2 | p_boolean_col | p_int_col | p_bigint_col | p_float_col | p_double_col | p_text_col | p_blob_col 
------+-------+-------+---------------+-----------+--------------+-------------+--------------+------------+------------
    1 |     1 |     1 | t             |         1 |            1 |           0 |            1 | test       | \x010203
(1 row)

-- Should NOT be pushed down if first part of clustering keys are not specified
explain verbose select * from postgresns_test where p_pk = 1 AND p_ck2 = 1;
                                                                      QUERY PLAN                                                                       
-------------------------------------------------------------------------------------------------------------------------------------------------------
 Foreign Scan on public.postgresns_test  (cost=0.00..9.53 rows=1 width=105)
   Output: p_pk, p_ck1, p_ck2, p_boolean_col, p_int_col, p_bigint_col, p_float_col, p_double_col, p_text_col, p_blob_col
   Filter: (postgresns_test.p_ck2 = 1)
   ScalarDB Namespace: postgresns
   ScalarDB Table: test
   ScalarDB Scan Type: partition key
   ScalarDB Scan Condition: p_pk = 1
   ScalarDB Scan Attribute: ("p_pk" "p_ck1" "p_ck2" "p_boolean_col" "p_int_col" "p_bigint_col" "p_float_col" "p_double_col" "p_text_col" "p_blob_col")
(8 rows)

select * from postgresns_test where p_pk = 1 AND p_ck2 = 1;
 p_pk | p_ck1 | p_ck2 | p_boolean_col | p_int_col | p_bigint_col | p_float_col | p_double_col | p_text_col | p_blob_col 
------+-------+-------+---------------+-----------+--------------+-------------+--------------+------------+------------
    1 |     1 |     1 | t             |         1 |            1 |           0 |            1 | test       | \x010203
(1 row)

