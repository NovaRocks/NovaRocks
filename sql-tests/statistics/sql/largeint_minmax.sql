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
-- Spark DECIMAL is limited to 38 digits, while the complete signed-LARGEINT
-- range has 39. Keep this boundary test on the native Hadoop Iceberg catalog;
-- the companion Puffin case covers the REST/Spark interoperability boundary.

-- query 1
-- @skip_result_check=true
CREATE DATABASE IF NOT EXISTS statistics_hadoop_${suite_uuid0}.nr_largeint_${suite_uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE statistics_hadoop_${suite_uuid0}.nr_largeint_${suite_uuid0}.largeint_minmax_${uuid0} (
    k LARGEINT NOT NULL
);

-- query 3
-- @skip_result_check=true
INSERT INTO statistics_hadoop_${suite_uuid0}.nr_largeint_${suite_uuid0}.largeint_minmax_${uuid0} VALUES
    (-170141183460469231731687303715884105728),
    (0),
    (170141183460469231731687303715884105727);

-- query 4
-- @skip_result_check=true
ANALYZE TABLE statistics_hadoop_${suite_uuid0}.nr_largeint_${suite_uuid0}.largeint_minmax_${uuid0};

-- query 5
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=SUCCEEDED
-- @skip_result_check=true
SHOW ANALYZE JOBS;

-- query 6
-- @result_contains=min-max stats
-- @skip_result_check=true
EXPLAIN VERBOSE
SELECT DISTINCT k
FROM statistics_hadoop_${suite_uuid0}.nr_largeint_${suite_uuid0}.largeint_minmax_${uuid0};

-- query 7
-- @skip_result_check=true
DROP TABLE statistics_hadoop_${suite_uuid0}.nr_largeint_${suite_uuid0}.largeint_minmax_${uuid0} FORCE;
