-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information regarding copyright
-- ownership.  The ASF licenses this file to you under the Apache License,
-- Version 2.0.

-- SQLP-5 syntax and DML admission failures use the original Trino wire.

-- query 1
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
DELETE FROM;

-- query 2
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
CREATE OR REPLACE TABLE sqlp5_duplicate_column (id INT, id BIGINT);

-- query 3
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
DELETE FROM sqlp5_reject_target;

-- query 4
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
UPDATE sqlp5_reject_target SET target.id = 1;

-- query 5
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
MERGE INTO sqlp5_reject_target target USING sqlp5_reject_source source ON target.id = source.id
WHEN MATCHED THEN DELETE
WHEN MATCHED THEN DELETE;

-- query 6
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
CREATE TEMPORARY TABLE sqlp5_reject_target (id INT);

-- query 7
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
CREATE TABLE sqlp5_reject_target (id INT) ENGINE = olap;

-- query 8
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
CREATE TABLE sqlp5_reject_target (id INT) ORDER BY (id);

-- query 9
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
CREATE OR REPLACE TABLE reject_ice.reject_ns.t AS SELECT CAST(1 AS bigint) AS id;

-- query 10
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
CREATE EXTERNAL TABLE reject_table (id integer);

-- query 11
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
CREATE TABLE reject_table (id integer) DISTRIBUTED BY HASH(id) BUCKETS 1;

-- query 12
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
CREATE TABLE reject_table LIKE stable_source;
