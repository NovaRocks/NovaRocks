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

-- Independent source rows: NULL parent, empty map, NULL value, populated value.
-- query 1
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE map_aggregate_contract (id INT, m MAP<INT,INT>) TBLPROPERTIES ("format-version"="3");
-- query 2
-- @skip_result_check=true
USE ${case_db};
INSERT INTO map_aggregate_contract VALUES (1,NULL),(2,map{}),(3,map{1:NULL}),(4,map{1:7});
-- query 3
USE ${case_db};
SELECT id, m IS NULL AS parent_null, m[1] AS value_at_one FROM map_aggregate_contract ORDER BY id;
-- query 4
USE ${case_db};
SELECT COUNT(*) AS rows_total, COUNT(m) AS maps_present, COUNT(m[1]) AS values_present FROM map_aggregate_contract;
-- query 5
USE ${case_db};
SELECT COUNT(m) AS maps_present FROM map_aggregate_contract WHERE id >= 2;
