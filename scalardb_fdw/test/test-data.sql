INSERT INTO cassandrans.test(
    c_pk,
    c_ck1,
    c_ck2,
    c_boolean_col,
    c_bigint_col,
    c_float_col,
    c_double_col,
    c_text_col
    -- c_blob_col -- skip a blob column because scalardb-sql-cli doesn't support blob literal
) VALUES (
    1,
    1,
    1,
    true,
    1,
    1.0,
    1.0,
    'test'
    -- 'test'
);

INSERT INTO postgresns.test(
    p_pk,
    p_ck1,
    p_ck2,
    p_boolean_col,
    p_bigint_col,
    p_float_col,
    p_double_col,
    p_text_col
    -- c_blob_col
) VALUES (
    1,
    1,
    1,
    true,
    1,
    1.0,
    1.0,
    'test'
);

INSERT INTO postgresns.null_test(
    p_pk,
    p_ck1,
    p_ck2
) VALUES (
    1,
    1,
    1
);
