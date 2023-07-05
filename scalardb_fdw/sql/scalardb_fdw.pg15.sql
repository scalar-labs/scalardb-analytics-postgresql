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

--
-- Test filtering push-down on partition keys, clustering keys and indexed columns
--
explain verbose select * from boolean_test where pk = true;
explain verbose select * from int_test where pk = 1;
explain verbose select * from bigint_test where pk = 1;
explain verbose select * from float_test where pk = 1.0;
explain verbose select * from double_test where pk = 1.0;
explain verbose select * from text_test where pk = '1';
explain verbose select * from blob_test where pk = E'\\xDEADBEEF';

explain verbose select * from boolean_test where index = true;
explain verbose select * from int_test where index = 1;
explain verbose select * from bigint_test where index = 1;
explain verbose select * from float_test where index = 1.0;
explain verbose select * from double_test where index = 1.0;
explain verbose select * from text_test where index = '1';
explain verbose select * from blob_test where index = E'\\xDEADBEEF';
