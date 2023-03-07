CREATE EXTENSION scalardb_fdw;

CREATE SERVER scalardb FOREIGN DATA WRAPPER scalardb_fdw OPTIONS (
    jar_file_path '/Users/aki/ghq/github.com/scalar-labs/scalardb-analytics-postgresql/scalardb_fdw/test/scalardb-3.8.0-all.jar',
    config_file_path '/Users/aki/ghq/github.com/scalar-labs/scalardb-analytics-postgresql/scalardb_fdw/test/client.properties'
);


CREATE USER MAPPING FOR PUBLIC SERVER scalardb;

CREATE FOREIGN TABLE cassandrans_test (
    c_pk int OPTIONS (scalardb_partition_key 'true'),
    c_ck1 int OPTIONS (scalardb_clustering_key 'true'),
    c_ck2 int OPTIONS (scalardb_clustering_key 'true'),
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
    p_pk int OPTIONS(scalardb_partition_key 'true'),
    p_ck1 int OPTIONS(scalardb_clustering_key 'true'),
    p_ck2 int OPTIONS(scalardb_clustering_key 'true'),
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
    p_pk int OPTIONS(scalardb_partition_key 'true'),
    p_ck1 int OPTIONS(scalardb_clustering_key 'true'),
    p_ck2 int OPTIONS(scalardb_clustering_key 'true'),
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
