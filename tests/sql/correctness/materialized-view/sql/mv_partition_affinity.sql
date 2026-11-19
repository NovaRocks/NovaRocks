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

-- @sequential=true
-- @order_sensitive=true

-- query 1
-- @skip_result_check=true
CREATE TABLE rwm13_base_${uuid0} (p BIGINT NOT NULL, id BIGINT) WITH (format_version = 3);
INSERT INTO rwm13_base_${uuid0} VALUES ROW(1, 1), ROW(2, 2), ROW(3, 3);
INSERT INTO rwm13_base_${uuid0} VALUES ROW(1, 4), ROW(2, 5), ROW(3, 6);
INSERT INTO rwm13_base_${uuid0} VALUES ROW(1, 7), ROW(2, 8), ROW(3, 9);
CREATE MATERIALIZED VIEW rwm13_mv_${uuid0}
WITH (partitioning = ARRAY['p'])
AS SELECT p, id FROM rwm13_base_${uuid0};
REFRESH MATERIALIZED VIEW rwm13_mv_${uuid0};

-- query 2
SELECT p, count(*) AS n, sum(id) AS id_sum FROM rwm13_mv_${uuid0}
GROUP BY p ORDER BY p;

-- query 3
SELECT count(*) AS data_files FROM "rwm13_mv_${uuid0}$files" WHERE content = 0;

-- query 4
SELECT count(*) AS partitions FROM "rwm13_mv_${uuid0}$partitions";

-- query 5
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS rwm13_mv_${uuid0};
DROP TABLE IF EXISTS rwm13_base_${uuid0} ;
