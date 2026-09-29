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

-- Test Objective: preserve all exact Decimal256 literal digits and expose the explicitly rounded DOUBLE domain.
-- query 1
USE ${case_db};
SELECT CAST(123456789012345678901234567890.123456789 AS VARCHAR) AS exact_text,
       murmur_hash3_32(123456789012345678901234567890.123456789) AS exact_hash,
       CAST(CAST(123456789012345678901234567890.123456789 AS DOUBLE) AS VARCHAR) AS double_text,
       murmur_hash3_32(CAST(123456789012345678901234567890.123456789 AS DOUBLE)) AS double_hash;

-- query 2
USE ${case_db};
SELECT SUM(murmur_hash3_32(123456789012345678901234567890.123456789)) AS exact_multiple,
       SUM(murmur_hash3_32(CAST(123456789012345678901234567890.123456789 AS DOUBLE))) AS double_multiple
FROM TABLE(generate_series(1,1328));
