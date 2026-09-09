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
-- @order_sensitive=true
SELECT id, value
FROM (
    SELECT id, value FROM paimon_${suite_uuid0}.fixture.append_none
    UNION ALL
    SELECT id, value FROM paimon_${suite_uuid0}.fixture.append_snappy
    UNION ALL
    SELECT id, value FROM paimon_${suite_uuid0}.fixture.append_zstd
    UNION ALL
    SELECT id, value FROM paimon_${suite_uuid0}.fixture.append_lz4
) codecs
ORDER BY id;

-- query 2
-- @order_sensitive=true
SELECT part, id, value
FROM paimon_${suite_uuid0}.fixture.append_partitioned
ORDER BY part, id;

-- query 3
-- @order_sensitive=true
SELECT id, CAST(c_decimal AS STRING) AS c_decimal, HEX(c_binary) AS c_binary_hex,
       CAST(c_date AS STRING) AS c_date, CAST(c_timestamp AS STRING) AS c_timestamp
FROM paimon_${suite_uuid0}.fixture.type_matrix
ORDER BY id;

-- query 4
SELECT COUNT(*) AS row_count
FROM paimon_${suite_uuid0}.fixture.empty_append;
