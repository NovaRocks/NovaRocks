-- Migrated from: dev/test/sql/test_lead_lag/T/test_lead_lag_support_col_type
-- Test Objective:
-- 1. Validate that a column reference can be used as the default value for lead/lag.
-- 2. Validate IGNORE NULLS combined with column default.
-- 3. Verify FE rejects column expressions (not plain column refs) as default.
-- 4. Verify FE rejects type-mismatched column default (INT col, VARCHAR default).
-- 5. Validate ARRAY<INT> and VARCHAR column types with column defaults.
-- 6. Validate ARRAY<VARCHAR> column type with column default.
-- @order_sensitive=true

-- query 1
-- @skip_result_check=true
-- INT col with INT column default
CREATE TABLE ${case_db}.t_col_int (
    col_1 INT,
    col_2 INT,
    col_3 INT NOT NULL
) PROPERTIES (
    "compression"        = "LZ4",
    "replicated_storage" = "true",
    "replication_num"    = "1"
);

INSERT INTO ${case_db}.t_col_int (col_1, col_2, col_3) VALUES
    (1, 1, 11), (2, 2, 22), (3, 3, 33), (4, NULL, 44), (5, 5, 55), (6, 6, 66);

-- INT col with VARCHAR default (type mismatch → FE error)
CREATE TABLE ${case_db}.t_col_varchar_default (
    col_1 INT,
    col_2 INT,
    col_3 VARCHAR(255) NOT NULL
) PROPERTIES (
    "compression"        = "LZ4",
    "replicated_storage" = "true",
    "replication_num"    = "1"
);

INSERT INTO ${case_db}.t_col_varchar_default (col_1, col_2, col_3) VALUES
    (1, 1, '11'), (2, 2, '22'), (3, 3, '33'),
    (4, NULL, '44'), (5, 5, '55'), (6, 6, '66'),
    (7, 7, '7djaiojdoa'), (8, NULL, '8djaiojdoa'), (9, 9, '0djagdfoi');

-- ARRAY<INT> with ARRAY<INT> column default
CREATE TABLE ${case_db}.t_col_array_int (
    col_1 INT,
    arr1 ARRAY<INT>,
    arr2 ARRAY<INT> NOT NULL
) PROPERTIES (
    "compression"        = "LZ4",
    "replicated_storage" = "true",
    "replication_num"    = "1"
);

INSERT INTO ${case_db}.t_col_array_int (col_1, arr1, arr2) VALUES
    (1, [1, 11], [101, 111]),
    (2, [2, 22], [102, 112]),
    (3, [3, 33], [103, 113]),
    (4, NULL,    [104, 114]),
    (5, [5, 55], [105, 115]),
    (6, [6, 66], [106, 116]);

-- VARCHAR with VARCHAR column default
CREATE TABLE ${case_db}.t_col_varchar (
    col_1 INT,
    v1 VARCHAR(255),
    v2 VARCHAR(255) NOT NULL
) PROPERTIES (
    "compression"        = "LZ4",
    "replicated_storage" = "true",
    "replication_num"    = "1"
);

INSERT INTO ${case_db}.t_col_varchar (col_1, v1, v2) VALUES
    (1, '1',  '11'),
    (2, '2',  '22'),
    (3, '3',  '33'),
    (4, NULL, '44'),
    (5, '5',  '55'),
    (6, '6',  '66');

-- ARRAY<VARCHAR> with ARRAY<VARCHAR> column default
CREATE TABLE ${case_db}.t_col_array_varchar (
    col_1 INT,
    arr1 ARRAY<VARCHAR(10)>,
    arr2 ARRAY<VARCHAR(10)> NOT NULL
) PROPERTIES (
    "compression"        = "LZ4",
    "replicated_storage" = "true",
    "replication_num"    = "1"
);

INSERT INTO ${case_db}.t_col_array_varchar (col_1, arr1, arr2) VALUES
    (1, ['1','11'], ['101','111']),
    (2, ['2','22'], ['102','112']),
    (3, ['3','33'], ['103','113']),
    (4, NULL,       ['104','114']),
    (5, ['5','55'], ['105','115']),
    (6, ['6','66'], ['106','116']);

-- query 2
-- INT: LAG with INT column as default
SELECT col_1, col_2, LAG(col_2, 2, col_3) OVER (ORDER BY col_1) AS lag_result
FROM ${case_db}.t_col_int ORDER BY col_1;

-- query 3
-- INT: LEAD with INT column as default
SELECT col_1, col_2, LEAD(col_2, 2, col_3) OVER (ORDER BY col_1) AS lead_result
FROM ${case_db}.t_col_int ORDER BY col_1;

-- query 4
-- INT: column expression as default is rejected by FE
-- @expect_error=The type of the third parameter of LEAD/LAG not match the type INT.
SELECT col_1, col_2, LAG(col_2, 2, col_3 * 2 + 99) OVER (ORDER BY col_1) AS lag_result
FROM ${case_db}.t_col_int;

-- query 5
-- INT: LAG IGNORE NULLS with INT column as default
SELECT col_1, col_2, LAG(col_2 IGNORE NULLS, 2, col_3) OVER (ORDER BY col_1) AS lag_result
FROM ${case_db}.t_col_int ORDER BY col_1;

-- query 6
-- INT: LEAD IGNORE NULLS with INT column as default
SELECT col_1, col_2, LEAD(col_2 IGNORE NULLS, 2, col_3) OVER (ORDER BY col_1) AS lead_result
FROM ${case_db}.t_col_int ORDER BY col_1;

-- query 7
-- VARCHAR default for INT column is rejected by FE (type mismatch)
-- @expect_error=The type of the third parameter of LEAD/LAG not match the type INT.
SELECT col_1, col_2, LAG(col_2, 2, col_3) OVER (ORDER BY col_1) AS lag_result
FROM ${case_db}.t_col_varchar_default;

-- query 8
-- ARRAY<INT>: LAG with ARRAY<INT> column as default
SELECT col_1, arr1, LAG(arr1, 2, arr2) OVER (ORDER BY col_1) AS lag_result
FROM ${case_db}.t_col_array_int ORDER BY col_1;

-- query 9
-- ARRAY<INT>: LEAD with ARRAY<INT> column as default
SELECT col_1, arr1, LEAD(arr1, 2, arr2) OVER (ORDER BY col_1) AS lead_result
FROM ${case_db}.t_col_array_int ORDER BY col_1;

-- query 10
-- VARCHAR: LAG with VARCHAR column as default
SELECT col_1, v1, LAG(v1, 2, v2) OVER (ORDER BY col_1) AS lag_result
FROM ${case_db}.t_col_varchar ORDER BY col_1;

-- query 11
-- VARCHAR: LEAD with VARCHAR column as default
SELECT col_1, v1, LEAD(v1, 2, v2) OVER (ORDER BY col_1) AS lead_result
FROM ${case_db}.t_col_varchar ORDER BY col_1;

-- query 12
-- ARRAY<VARCHAR>: LAG with ARRAY<VARCHAR> column as default
SELECT col_1, arr1, LAG(arr1, 2, arr2) OVER (ORDER BY col_1) AS lag_result
FROM ${case_db}.t_col_array_varchar ORDER BY col_1;

-- query 13
-- ARRAY<VARCHAR>: LEAD with ARRAY<VARCHAR> column as default
SELECT col_1, arr1, LEAD(arr1, 2, arr2) OVER (ORDER BY col_1) AS lead_result
FROM ${case_db}.t_col_array_varchar ORDER BY col_1;
