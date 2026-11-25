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

-- @tags=subquery,json,mark_join
-- Fixed 13 probe values: 0..8 match; 9..12 have no TRUE and encounter SQL NULL.
-- Object member order differs, and a duplicate RHS occurrence cannot multiply rows.
CREATE SCHEMA IF NOT EXISTS ${case_db};
USE ${case_db};
CREATE TABLE json_mark_probe (id BIGINT, txt VARCHAR);
CREATE TABLE json_mark_rhs (txt VARCHAR);
INSERT INTO json_mark_probe SELECT generate_series, CONCAT('{"a":',CAST(generate_series AS VARCHAR),',"b":1}') FROM TABLE(sequence(0,12)) AS generate_series(generate_series);
INSERT INTO json_mark_rhs SELECT CONCAT('{"b":1,"a":',CAST(generate_series AS VARCHAR),'}') FROM TABLE(sequence(0,8)) AS generate_series(generate_series);
INSERT INTO json_mark_rhs VALUES ROW(NULL),ROW('{"b":1,"a":0}');
-- q16 oracle: 9 TRUE, 4 NULL, one output for each probe occurrence.
SELECT id, json_parse(txt) IN (SELECT json_parse(txt) FROM json_mark_rhs) AS included FROM json_mark_probe ORDER BY id;
-- q17 oracle: 9 FALSE, 4 NULL; NOT applies once after three-valued reduction.
SELECT id, json_parse(txt) NOT IN (SELECT json_parse(txt) FROM json_mark_rhs) AS excluded FROM json_mark_probe ORDER BY id;
-- Empty RHS returns FALSE/TRUE even for a SQL NULL probe.
SELECT json_parse(NULL) IN (SELECT json_parse(txt) FROM json_mark_rhs WHERE FALSE) AS included, json_parse(NULL) NOT IN (SELECT json_parse(txt) FROM json_mark_rhs WHERE FALSE) AS excluded;
-- JSON text null remains an ordinary JSON value, distinct from SQL NULL.
SELECT json_parse('null') IN (SELECT json_parse('null')) AS json_null, json_parse(NULL) IN (SELECT json_parse('null')) AS sql_null;
-- OR operands are value-form; direct top-level JSON membership stays rejected.
SELECT id FROM json_mark_probe WHERE id=0 OR json_parse(txt) IN (SELECT json_parse(txt) FROM json_mark_rhs) ORDER BY id;
SELECT MAX(id) AS hi FROM json_mark_probe HAVING FALSE OR json_parse(CAST(MAX(id) AS VARCHAR)) IN (SELECT json_parse('12'));
-- @expect_error=[sql.analyze.unsupported_query_shape]
SELECT id FROM json_mark_probe WHERE json_parse(txt) IN (SELECT json_parse(txt) FROM json_mark_rhs);
-- @expect_error=[sql.analyze.unsupported_query_shape]
SELECT id FROM json_mark_probe WHERE NOT json_parse(txt) IN (SELECT json_parse(txt) FROM json_mark_rhs);
-- @expect_error=[sql.analyze.unsupported_query_shape]
SELECT id FROM json_mark_probe WHERE CASE WHEN json_parse(txt) IN (SELECT json_parse(txt) FROM json_mark_rhs) THEN TRUE ELSE FALSE END;
-- @expect_error=[sql.analyze.unsupported_query_shape]
SELECT json_parse(txt) IN (SELECT txt FROM json_mark_rhs) FROM json_mark_probe;
-- @expect_error=[sql.analyze.unsupported_query_shape]
SELECT json_parse(p.txt) IN (SELECT json_parse(r.txt) FROM json_mark_rhs r WHERE r.txt=p.txt) FROM json_mark_probe p;
-- The strict dynamic candidate still runs after a hit and with a SQL NULL probe.
INSERT INTO json_mark_probe VALUES (99, 'not JSON');
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT JSON '1' IN (JSON '1', json_parse(txt)) FROM json_mark_probe WHERE id = 99;
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT json_parse(NULL) IN (json_parse(txt)) FROM json_mark_probe WHERE id = 99;
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT JSON '1' IN (SELECT json_parse(txt) FROM json_mark_probe WHERE id = 99);
DROP TABLE json_mark_probe;
DROP TABLE json_mark_rhs;
