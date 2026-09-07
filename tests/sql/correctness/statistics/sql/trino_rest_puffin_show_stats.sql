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
-- Released Trino 483 creates and analyzes the nonempty parent through the
-- real REST catalog. Nova then consumes that foreign Theta parent during a
-- collect-on-write append. A fresh Trino process must resolve the new current
-- snapshot and report the unioned numeric NDV through public SHOW STATS.

-- query 1
-- @result_contains=TRINO_PUFFIN_PARENT_READY version=483
shell: "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/datasketches-tck/interop/trino/verify_rest_catalog.sh" create-parent nr_statistics_${suite_uuid0} trino_puffin_${uuid0}

-- query 2
-- @skip_result_check=true
ALTER TABLE statistics_cat_${suite_uuid0}.nr_statistics_${suite_uuid0}.trino_puffin_${uuid0}
SET TBLPROPERTIES ('novarocks.statistics.collect-on-write' = 'true');

-- query 3
-- The duplicate parent value distinguishes a Theta union from row-counting:
-- Trino measured {1,2}; Nova writes {2,3}; the current NDV must be 3.
-- @skip_result_check=true
INSERT INTO statistics_cat_${suite_uuid0}.nr_statistics_${suite_uuid0}.trino_puffin_${uuid0}
VALUES (2, 20), (3, 30);

-- query 4
-- This starts a new released Trino process. Besides SHOW STATS NDV=3, it reads
-- the current data and snapshot through the same REST/S3 connector and asserts
-- four rows, three distinct ids, and the exact id range 1..3.
-- @result_contains=TRINO_NOVA_CURRENT_PUFFIN_OK version=483
shell: "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/datasketches-tck/interop/trino/verify_rest_catalog.sh" verify-current nr_statistics_${suite_uuid0} trino_puffin_${uuid0} id 4 3 1 3

-- query 5
-- @skip_result_check=true
DROP TABLE statistics_cat_${suite_uuid0}.nr_statistics_${suite_uuid0}.trino_puffin_${uuid0} FORCE;
