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

-- @order_sensitive=true
-- NCP-6 bounds acceptance: an over-limit distributed write is refused, and the
-- refusal leaves no partial snapshot behind.
--
-- The prepared write set's entry budget is the one frozen budget a plain SQL
-- statement can actually drive to its boundary, because an Iceberg partitioned
-- INSERT writes exactly one data file -- and therefore exactly one commit
-- fragment -- per distinct partition value, independent of chunking and of
-- writer-driver parallelism.
--
-- The budget itself is asserted against its named constant by the unit tests
-- that own it (MAX_CONNECTOR_PREPARED_WRITE_SET_ENTRIES in
-- novarocks/spi/src/connector/write_stack/limits.rs, exercised in
-- .../write_stack/prepared.rs). SQL cannot name a Rust constant, so this case
-- restates the value once, as 32 * 32 * 16 + 1 = 16385 partition values: one
-- entry past the 16384 the design froze. Raising or lowering the budget makes
-- this case fail loudly rather than quietly stop testing -- an unexpected
-- success is a runner failure, not a pass.
--
-- This case deliberately writes 16385 tiny files, so it runs for roughly a
-- minute and a half. That cost is the budget: there is no shortcut to the
-- boundary of an entry count.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_write_set_entry_budget FORCE;
DROP TABLE IF EXISTS ${case_db}.seed_write_set_entry_budget FORCE;
CREATE TABLE ${case_db}.seed_write_set_entry_budget (n BIGINT);
INSERT INTO ${case_db}.seed_write_set_entry_budget VALUES
  (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),
  (16),(17),(18),(19),(20),(21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31);
CREATE TABLE ${case_db}.t_write_set_entry_budget (
  p BIGINT,
  v BIGINT
)
PARTITION BY identity(p);
INSERT INTO ${case_db}.t_write_set_entry_budget VALUES (-1, 1);

-- query 2
-- The committed state the refused write must not disturb.
SELECT count() AS snaps_before
FROM ${case_db}."t_write_set_entry_budget$snapshots";

-- query 3
-- 32 * 32 * 16 distinct partition values, plus one more: 16385 commit
-- fragments against a 16384-entry budget. The root aggregation refuses the
-- set rather than committing a prefix of it.
-- Match the public resource identity and the connector's exact entry refusal,
-- rather than the private source error's former Display text.
-- @expect_error=[query.execution.resource_exhausted] Resource failure (reason 4:44)
INSERT INTO ${case_db}.t_write_set_entry_budget
SELECT (a.n * 32 + b.n) * 16 + c.n AS p, 2 AS v
FROM ${case_db}.seed_write_set_entry_budget a
CROSS JOIN ${case_db}.seed_write_set_entry_budget b
CROSS JOIN (SELECT n FROM ${case_db}.seed_write_set_entry_budget WHERE n < 16) c
UNION ALL
SELECT 16384 AS p, 2 AS v;

-- query 4
-- No partial snapshot: the table still holds exactly the row it held before,
-- and none of the 16385 staged files became visible.
SELECT p, v
FROM ${case_db}.t_write_set_entry_budget
ORDER BY p;

-- query 5
-- Stronger than a row check: the refused write produced no snapshot at all,
-- so the external commit never ran.
SELECT count() AS snaps_after
FROM ${case_db}."t_write_set_entry_budget$snapshots";

-- query 6
-- @skip_result_check=true
DROP TABLE ${case_db}.t_write_set_entry_budget FORCE;
DROP TABLE ${case_db}.seed_write_set_entry_budget FORCE;
