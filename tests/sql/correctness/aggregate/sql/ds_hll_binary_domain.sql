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

-- Native byte-domain controls derived independently from five distinct raw byte strings.
-- query 1
USE ${case_db};
SELECT COUNT(DISTINCT b) AS exact_bytes,ndv(b) AS legacy_ndv,
approx_count_distinct(b) AS legacy_approx,ds_hll_count_distinct(b,10) AS ds_lgk10
FROM (SELECT to_binary('', 'hex') AS b UNION ALL
SELECT to_binary('00','hex') UNION ALL SELECT to_binary('FF','hex') UNION ALL
SELECT to_binary('0001FF80','hex') UNION ALL SELECT to_binary('76616C75655F31','hex') UNION ALL
SELECT to_binary('FF','hex') UNION ALL SELECT CAST(NULL AS VARBINARY)) v;

-- query 2
USE ${case_db};
SELECT COUNT(DISTINCT b) AS exact_bytes,ndv(b) AS legacy_ndv,
approx_count_distinct(b) AS legacy_approx,ds_hll_count_distinct(b,10) AS ds_lgk10
FROM (SELECT CAST(NULL AS VARBINARY) AS b UNION ALL SELECT CAST(NULL AS VARBINARY)) v;
