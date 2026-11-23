-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- Migrated from dev/test/sql/test_agg/R/test_eliminate_agg
-- Test Objective:
-- Preserve legacy GROUP BY result coverage over unique-valued input rows
-- after Iceberg migration.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_eliminate_agg @mac
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE "test_agg_group_single_key_values" (
  id INT NOT NULL,
  big_value BIGINT,
  double_value DOUBLE,
  decimal_value DECIMAL(10, 5),
  varchar_value VARCHAR(255)
) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE "test_agg_group_multi_key_values" (
  id INT NOT NULL,
  big_value BIGINT,
  double_value DOUBLE,
  decimal_value DECIMAL(10, 5),
  varchar_value VARCHAR(255)
) WITH (format_version = 3);

-- query 4
-- @skip_result_check=true
USE ${case_db};
INSERT INTO "test_agg_group_single_key_values" (id, big_value, double_value, decimal_value, varchar_value) VALUES
ROW(1, 100000, 1.23, 123.45678, 'Test1'),
ROW(2, 200000, 2.34, 234.56789, 'Test2'),
ROW(3, 300000, 3.45, 345.67890, 'Test3'),
ROW(4, 400000, 4.56, 456.78901, 'Test4'),
ROW(5, 500000, 5.67, 567.89012, NULL),
ROW(6, 600000, 6.78, 678.90123, 'Test6'),
ROW(7, 700000, 7.89, 789.01234, NULL),
ROW(8, 800000, 8.90, 890.12345, 'Test8'),
ROW(9, 900000, 9.01, 901.23456, NULL),
ROW(10, 1000000, 10.12, 1012.34567, 'Test10'),
ROW(11, 1100000, 11.23, 1123.45678, 'Test11'),
ROW(12, 1200000, 12.34, 1234.56789, 'Test12'),
ROW(13, 1300000, 13.45, 1345.67890, NULL),
ROW(14, 1400000, 14.56, 1456.78901, 'Test14'),
ROW(15, 1500000, 15.67, 1567.89012, 'Test15'),
ROW(16, 1600000, 16.78, 1678.90123, NULL),
ROW(17, 1700000, 17.89, 1789.01234, 'Test17'),
ROW(18, 1800000, 18.90, 1890.12345, 'Test18'),
ROW(19, 1900000, 19.01, 1901.23456, NULL),
ROW(20, 2000000, 20.12, 2012.34567, 'Test20');

-- query 5
-- @skip_result_check=true
USE ${case_db};
INSERT INTO "test_agg_group_multi_key_values" (id, big_value, double_value, decimal_value, varchar_value) VALUES
ROW(1, 100000, 1.23, 123.45678, 'Test1'),
ROW(2, 200000, 2.34, 234.56789, 'Test2'),
ROW(3, 300000, 3.45, 345.67890, 'Test3'),
ROW(4, 400000, 4.56, 456.78901, 'Test4'),
ROW(5, 500000, 5.67, 567.89012, NULL),
ROW(6, 600000, 6.78, 678.90123, 'Test6'),
ROW(7, 700000, 7.89, 789.01234, NULL),
ROW(8, 800000, 8.90, 890.12345, 'Test8'),
ROW(9, 900000, 9.01, 901.23456, NULL),
ROW(10, 1000000, 10.12, 1012.34567, 'Test10'),
ROW(11, 1100000, 11.23, 1123.45678, 'Test11'),
ROW(12, 1200000, 12.34, 1234.56789, 'Test12'),
ROW(13, 1300000, 13.45, 1345.67890, NULL),
ROW(14, 1400000, 14.56, 1456.78901, 'Test14'),
ROW(15, 1500000, 15.67, 1567.89012, 'Test15'),
ROW(16, 1600000, 16.78, 1678.90123, NULL),
ROW(17, 1700000, 17.89, 1789.01234, 'Test17'),
ROW(18, 1800000, 18.90, 1890.12345, 'Test18'),
ROW(19, 1900000, 19.01, 1901.23456, NULL),
ROW(20, 2000000, 20.12, 2012.34567, 'Test20');

-- query 6
USE ${case_db};
SELECT
    id,
    SUM(big_value) AS sum_big_value
FROM
    test_agg_group_single_key_values
GROUP BY
    id
ORDER BY
    id;

-- query 7
USE ${case_db};
SELECT
    id,
    SUM(double_value) AS sum_double_value
FROM
    test_agg_group_single_key_values
GROUP BY
    id
ORDER BY
    id;

-- query 8
USE ${case_db};
SELECT
    id,
    AVG(big_value) AS avg_big_value
FROM
    test_agg_group_single_key_values
GROUP BY
    id
ORDER BY
    id;

-- query 9
USE ${case_db};
SELECT
    id,
    AVG(double_value) AS avg_double_value
FROM
    test_agg_group_single_key_values
GROUP BY
    id
ORDER BY
    id;

-- query 10
USE ${case_db};
SELECT
    id,
    COUNT(big_value) AS count_big_value
FROM
    test_agg_group_single_key_values
GROUP BY
    id
ORDER BY
    id;

-- query 11
USE ${case_db};
SELECT
    id,
    COUNT(varchar_value) AS count_varchar_value
FROM
    test_agg_group_single_key_values
GROUP BY
    id
ORDER BY
    id;

-- query 12
USE ${case_db};
SELECT
    id,
    MAX(decimal_value) AS max_decimal_value
FROM
    test_agg_group_single_key_values
GROUP BY
    id
ORDER BY
    id;

-- query 13
USE ${case_db};
SELECT
    id,
    MAX(double_value) AS max_double_value
FROM
    test_agg_group_single_key_values
GROUP BY
    id
ORDER BY
    id;

-- query 14
USE ${case_db};
SELECT
    id,
    MIN(double_value) AS min_double_value
FROM
    test_agg_group_single_key_values
GROUP BY
    id
ORDER BY
    id;

-- query 15
USE ${case_db};
SELECT
    id,
    MIN(big_value) AS min_big_value
FROM
    test_agg_group_single_key_values
GROUP BY
    id
ORDER BY
    id;

-- query 16
USE ${case_db};
SELECT
    id,
    LISTAGG(CAST(varchar_value AS VARCHAR), ',') WITHIN GROUP (ORDER BY varchar_value) AS group_concat_varchar_value
FROM
    test_agg_group_single_key_values
GROUP BY
    id
ORDER BY
    id;

-- query 17
USE ${case_db};
SELECT
    id,
    LISTAGG(CAST(double_value AS VARCHAR), ',') WITHIN GROUP (ORDER BY double_value) AS group_concat_double_value
FROM
    test_agg_group_single_key_values
GROUP BY
    id
ORDER BY
    id;

-- query 18
USE ${case_db};
SELECT
    id,
    big_value,
    SUM(double_value) AS sum_double_value
FROM
    test_agg_group_multi_key_values
GROUP BY
    id, big_value
ORDER BY
    id;

-- query 19
USE ${case_db};
SELECT
    id,
    big_value,
    AVG(decimal_value) AS avg_decimal_value
FROM
    test_agg_group_multi_key_values
GROUP BY
    id, big_value
ORDER BY
    id;

-- query 20
USE ${case_db};
SELECT
    id,
    big_value,
    COUNT(varchar_value) AS count_varchar_value
FROM
    test_agg_group_multi_key_values
GROUP BY
    id, big_value
ORDER BY
    id;

-- query 21
USE ${case_db};
SELECT
    id,
    big_value,
    MAX(double_value) AS max_double_value
FROM
    test_agg_group_multi_key_values
GROUP BY
    id, big_value
ORDER BY
    id;

-- query 22
USE ${case_db};
SELECT
    id,
    big_value,
    MIN(big_value) AS min_big_value
FROM
    test_agg_group_multi_key_values
GROUP BY
    id, big_value
ORDER BY
    id;

-- query 23
USE ${case_db};
SELECT
    id,
    big_value,
    LISTAGG(CAST(varchar_value AS VARCHAR), ',') WITHIN GROUP (ORDER BY varchar_value) AS group_concat_varchar_value
FROM
    test_agg_group_multi_key_values
GROUP BY
    id, big_value
ORDER BY
    id;

-- query 24
-- @skip_result_check=true
USE ${case_db};
drop table test_agg_group_single_key_values;

-- query 25
-- @skip_result_check=true
USE ${case_db};
drop table test_agg_group_multi_key_values;
