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

-- SQLP-0 drift corpus for the existing command-family probes.  These cases
-- intentionally lock current parser/admission behavior, including routes that
-- reject a malformed command before its family parser is reached.

-- @expect_error_tier=drift
-- @expect_error=[sql.parse.unexpected_token] expected DISTRIBUTED BY HASH (...) BUCKETS <count>
CREATE MATERIALIZED VIEW reject_mv AS SELECT 1;

-- @expect_error_tier=drift
-- @expect_error=[sql.parse.unexpected_token] expected DROP MATERIALIZED VIEW without FORCE
DROP MATERIALIZED VIEW reject_mv FORCE;

-- @expect_error_tier=drift
-- @expect_error=[sql.parse.unexpected_token] expected REFRESH or TBLPROPERTIES
ALTER MATERIALIZED VIEW reject_mv SET FOO;

-- @expect_error_tier=drift
-- @expect_error=[sql.parse.unexpected_token] expected SYNC or ASYNC
REFRESH MATERIALIZED VIEW reject_mv WITH BROKEN;

-- @expect_error_tier=drift
-- @expect_error=[sql.parse.unexpected_token] expected unfiltered SHOW MATERIALIZED VIEWS
SHOW MATERIALIZED VIEWS LIKE 'reject%';

-- Backend membership is owned by external orchestration, so ADD BACKEND and
-- DROP BACKEND no longer exist as a command family. They are rejected as
-- recognized-but-unsupported statements rather than by a backend-address
-- grammar.
-- @expect_error_tier=drift
-- @expect_error=[sql.parse.unsupported_statement] recognized but unsupported statement `ADD`
ADD BACKEND 127.0.0.1:1234;

-- @expect_error_tier=drift
-- @expect_error=[sql.parse.unsupported_statement] recognized but unsupported statement `DROP`
DROP BACKEND 127.0.0.1:1234;

-- @expect_error_tier=drift
-- @expect_error=[sql.parse.unexpected_token] expected ';' or end of input
ANALYZE TABLE reject_stats UPDATE HISTOGRAM ON c;

-- @expect_error_tier=drift
-- @expect_error=[sql.parse.unexpected_token] expected bare TRUNCATE TABLE target
TRUNCATE TABLE reject_table PARTITION (p1);

-- @expect_error_tier=drift
-- @expect_error=[sql.parse.unexpected_token] expected identifier
ALTER TABLE reject_table CREATE BRANCH;

-- @expect_error_tier=drift
-- @expect_error=[sql.parse.unexpected_token] expected FROM
ALTER TABLE reject_table ADD FILES;

-- @expect_error_tier=drift
-- @expect_error=[sql.parse.unexpected_token] expected only named or only positional procedure arguments
CALL ice.system.rewrite_manifests('db.t', table => 'db.u');

-- @expect_error_tier=drift
-- @expect_error=[sql.parse.unexpected_token] expected =
CREATE CATALOG reject_catalog PROPERTIES ('type');

-- @expect_error_tier=drift
-- @expect_error=[sql.parse.unexpected_token] expected identifier
DROP CATALOG;

-- @expect_error_tier=drift
-- @expect_error=[sql.parse.unexpected_token] expected PRIMARY KEY clause requires at least one column
CREATE MATERIALIZED VIEW reject_mv
DISTRIBUTED BY HASH(k1) BUCKETS 1
PRIMARY KEY ()
AS SELECT k1 FROM source_table;
