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
-- ANALYZE waits for its terminal durable observation before it returns. The
-- following SHOW executes immediately, without runner retry polling, and its
-- golden freezes the seven-column statistics presentation contract.

-- query 1
-- @skip_result_check=true
CREATE DATABASE IF NOT EXISTS statistics_cat_${suite_uuid0}.nr_synchronous_${suite_uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE statistics_cat_${suite_uuid0}.nr_synchronous_${suite_uuid0}.terminal_${uuid0} (
    k BIGINT NOT NULL
);

-- query 3
-- @skip_result_check=true
INSERT INTO statistics_cat_${suite_uuid0}.nr_synchronous_${suite_uuid0}.terminal_${uuid0} VALUES
    (1), (2), (3);

-- query 4
-- @skip_result_check=true
ANALYZE TABLE statistics_cat_${suite_uuid0}.nr_synchronous_${suite_uuid0}.terminal_${uuid0} (k);

-- query 5
SHOW STATS FOR statistics_cat_${suite_uuid0}.nr_synchronous_${suite_uuid0}.terminal_${uuid0};

-- query 6
-- @skip_result_check=true
DROP TABLE statistics_cat_${suite_uuid0}.nr_synchronous_${suite_uuid0}.terminal_${uuid0};
