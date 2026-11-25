-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0.
--
-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.noncurrent_probe_source (id BIGINT NOT NULL) WITH (format_version = 3);
INSERT INTO ${case_db}.noncurrent_probe_source VALUES (1);
CREATE BRANCH pinned IN TABLE ${case_db}.noncurrent_probe_source;
ALTER TABLE ${case_db}.noncurrent_probe_source EXECUTE create_tag(name => 'saved', replace => false, if_not_exists => false);
CREATE VIEW ${case_db}.noncurrent_probe_view AS SELECT id FROM ${case_db}.noncurrent_probe_source FOR VERSION AS OF 'pinned';

-- query 2
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
CREATE MATERIALIZED VIEW ${case_db}.noncurrent_probe_mv AS SELECT id FROM ${case_db}.noncurrent_probe_view;

-- query 3
-- @skip_result_check=true
CREATE OR REPLACE VIEW ${case_db}.noncurrent_probe_view AS SELECT id FROM ${case_db}.noncurrent_probe_source FOR VERSION AS OF 'saved';

-- query 4
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
CREATE MATERIALIZED VIEW ${case_db}.noncurrent_probe_mv AS SELECT id FROM ${case_db}.noncurrent_probe_view;

-- query 5
-- @skip_result_check=true
CREATE OR REPLACE VIEW ${case_db}.noncurrent_probe_view AS SELECT id FROM ${case_db}.noncurrent_probe_source FOR TIMESTAMP AS OF '2999-12-31 00:00:00';

-- query 6
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
CREATE MATERIALIZED VIEW ${case_db}.noncurrent_probe_mv AS SELECT id FROM ${case_db}.noncurrent_probe_view;

-- query 7
-- @skip_result_check=true
CREATE OR REPLACE VIEW ${case_db}.noncurrent_probe_view AS SELECT id FROM ${case_db}.noncurrent_probe_source;
CREATE MATERIALIZED VIEW ${case_db}.noncurrent_probe_mv WITH (refresh_policy = 'MANUAL') AS SELECT id FROM ${case_db}.noncurrent_probe_view;
REFRESH MATERIALIZED VIEW ${case_db}.noncurrent_probe_mv;
CREATE OR REPLACE VIEW ${case_db}.noncurrent_probe_view AS SELECT id FROM ${case_db}.noncurrent_probe_source FOR VERSION AS OF 'pinned';

-- query 8
-- @expect_trino_error=VIEW_IS_STALE,USER_ERROR
REFRESH MATERIALIZED VIEW ${case_db}.noncurrent_probe_mv;

-- query 9
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS ${case_db}.noncurrent_probe_mv;
DROP VIEW IF EXISTS ${case_db}.noncurrent_probe_view;
DROP TABLE IF EXISTS ${case_db}.noncurrent_probe_source;
