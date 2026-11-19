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

-- Statement admission rejects complete unsupported Trino forms and retired spellings.

-- query 1
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
CREATE OR REPLACE MATERIALIZED VIEW reject_mv AS SELECT 1 AS id;

-- query 2
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
DROP MATERIALIZED VIEW reject_mv FORCE;

-- query 3
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
ALTER MATERIALIZED VIEW reject_mv SET FOO;

-- query 4
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
REFRESH MATERIALIZED VIEW reject_mv WITH BROKEN;

-- query 5
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SHOW MATERIALIZED VIEWS LIKE 'reject%';

-- Backend membership is owned by external orchestration, so ADD BACKEND and
-- DROP BACKEND are outside the Trino statement grammar.
-- query 6
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
ADD BACKEND 127.0.0.1:1234;

-- query 7
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
DROP BACKEND 127.0.0.1:1234;

-- query 8
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
ANALYZE TABLE reject_stats UPDATE HISTOGRAM ON c;

-- query 9
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
TRUNCATE TABLE reject_table PARTITION (p1);

-- query 10
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
ALTER TABLE reject_table CREATE BRANCH;

-- query 11
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
ALTER TABLE reject_table ADD FILES;

-- query 12
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
CALL ice.system.rewrite_manifests('db.t', table => );

-- query 13
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
CREATE CATALOG reject_catalog PROPERTIES ('type');

-- query 14
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
DROP CATALOG;

-- query 15
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
CREATE MATERIALIZED VIEW reject_mv
DISTRIBUTED BY HASH(k1) BUCKETS 1
PRIMARY KEY ()
AS SELECT k1 FROM source_table;

-- query 16
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
CREATE CATALOG reject_scope USING trc5e_reject_connector AUTHORIZATION USER operator;

-- query 17
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
DROP CATALOG reject_scope CASCADE;

-- query 18
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
CREATE SCHEMA reject_ice.p14_schema_authorization_witness AUTHORIZATION ROLE operator;

-- query 19
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
ALTER SCHEMA reject_ice.reject_ns RENAME TO next_ns;

-- query 20
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
COMMENT ON TABLE reject_ice.reject_ns.t IS NULL;

-- query 21
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
COMMENT ON VIEW reject_ice.reject_ns.v IS 'note';

-- query 22
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
COMMENT ON MATERIALIZED VIEW reject_ice.reject_ns.mv IS 'note';

-- query 23
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
COMMENT ON COLUMN reject_ice.reject_ns.t.id IS NULL;

-- query 24
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
ALTER TABLE IF EXISTS reject_ice.reject_ns.t RENAME TO reject_ice.reject_ns.t2;

-- query 25
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
ALTER TABLE IF EXISTS reject_ice.reject_ns.t ADD COLUMN extra bigint;

-- query 26
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
ALTER TABLE reject_ice.reject_ns.t ADD COLUMN IF NOT EXISTS extra bigint;

-- query 27
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
ALTER TABLE reject_ice.reject_ns.t RENAME COLUMN IF EXISTS id TO new_id;

-- query 28
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
ALTER TABLE reject_ice.reject_ns.t DROP COLUMN IF EXISTS id;

-- query 29
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
ALTER TABLE reject_ice.reject_ns.t ALTER COLUMN id SET DEFAULT 1;

-- query 30
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
ALTER TABLE reject_ice.reject_ns.t ALTER COLUMN id DROP DEFAULT;

-- query 31
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
ALTER TABLE IF EXISTS reject_ice.reject_ns.t ALTER COLUMN id SET DATA TYPE bigint;

-- query 32
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
ALTER TABLE IF EXISTS reject_ice.reject_ns.t ALTER COLUMN id DROP NOT NULL;

-- query 33
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
ALTER MATERIALIZED VIEW reject_ice.reject_ns.mv SET AUTHORIZATION ROLE reader;

-- query 34
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
CREATE MATERIALIZED VIEW reject_mv GRACE PERIOD INTERVAL '1' DAY AS SELECT 1 AS x;

-- query 35
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
CREATE MATERIALIZED VIEW reject_mv WHEN STALE FAIL AS SELECT 1 AS x;

-- query 36
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
CREATE MATERIALIZED VIEW reject_mv COMMENT 'note' AS SELECT 1 AS x;

-- query 37
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
CREATE VIEW reject_view SECURITY INVOKER AS SELECT 1 AS x;

-- query 38
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
CREATE VIEW reject_view WITH (a = DEFAULT) AS SELECT 1 AS x;

-- query 39
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
ALTER MATERIALIZED VIEW IF EXISTS reject_mv RENAME TO reject_mv2;

-- query 40
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
ALTER MATERIALIZED VIEW reject_mv EXECUTE p;

-- query 41
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
ALTER VIEW v RENAME TO w;

-- query 42
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
ALTER VIEW v REFRESH;

-- query 43
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
CREATE OR REPLACE FUNCTION f(x bigint) RETURNS bigint RETURN x;

-- query 44
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
DROP FUNCTION IF EXISTS f(x bigint);

-- query 45
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
CREATE BRANCH dev IN TABLE reject_ice.reject_ns.t FROM work;

-- query 46
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
ALTER BRANCH work IN TABLE reject_ice.reject_ns.t FAST FORWARD TO dev;

-- query 47
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SHOW BRANCHES FROM TABLE reject_ice.reject_ns.t;

-- query 48
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
CREATE ROLE reader WITH ADMIN CURRENT_USER IN reject_ice;

-- query 49
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
DROP ROLE IF EXISTS reader IN reject_ice;

-- query 50
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
GRANT r TO USER u;

-- query 51
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
GRANT SELECT ON TABLE reject_ice.reject_ns.t TO USER u;

-- query 52
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
REVOKE r FROM USER u;

-- query 53
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
REVOKE SELECT ON TABLE reject_ice.reject_ns.t FROM USER u;

-- query 54
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
DENY SELECT ON TABLE reject_ice.reject_ns.t TO USER u;

-- query 55
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SHOW GRANTS ON TABLE reject_ice.reject_ns.t;

-- query 56
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
EXPLAIN (FORMAT GRAPHVIZ) SELECT 1 AS answer;

-- query 57
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
EXPLAIN (FORMAT JSON) SELECT 1 AS answer;

-- query 58
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
EXPLAIN (TYPE VALIDATE) SELECT 1 AS answer;

-- query 59
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
EXPLAIN (TYPE IO) SELECT 1 AS answer;

-- query 60
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
KILL QUERY 1;

-- query 61
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
DROP TABLE reject_table FORCE;

-- query 62
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
DROP DATABASE reject_schema FORCE;

-- query 63
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
CREATE EXTERNAL CATALOG reject_scope PROPERTIES ("type" = "iceberg");
