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

-- query 1
-- @skip_result_check=true
SET enable_fold_constant_by_be = false;

-- query 2
SELECT bitmap_to_base64(bitmap_empty()) AS empty_text,
       bitmap_to_base64(to_bitmap(7)) AS singleton32_text,
       bitmap_to_base64(to_bitmap(4294967296)) AS singleton64_text;

-- query 3
SELECT concat('bitmap:',bitmap_to_base64(to_bitmap(7))) AS text_consumer,
       bitmap_to_base64(NULL) IS NULL AS bare_null,
       bitmap_to_base64(unhex('FF')) IS NULL AS malformed_null;

-- query 4
-- @order_sensitive=true
SELECT n,bitmap_to_base64(to_bitmap(n)) AS encoded
FROM (SELECT 0 AS n UNION ALL SELECT 7 UNION ALL SELECT 4294967296) t ORDER BY n;

-- query 5
-- @skip_result_check=true
SET enable_fold_constant_by_be = true;
