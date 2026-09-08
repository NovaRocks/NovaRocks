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
SELECT id, metric, note
FROM paimon_${suite_uuid0}.fixture.pk_default
ORDER BY id;

-- query 2
-- @order_sensitive=true
SELECT id, metric, note
FROM paimon_${suite_uuid0}.fixture.pk_dynamic
ORDER BY id;

-- query 3
-- @order_sensitive=true
SELECT id, seq, value
FROM paimon_${suite_uuid0}.fixture.pk_sequence
ORDER BY id;

-- query 4
SELECT COUNT(*) AS row_count
FROM paimon_${suite_uuid0}.fixture.pk_all_deleted;
