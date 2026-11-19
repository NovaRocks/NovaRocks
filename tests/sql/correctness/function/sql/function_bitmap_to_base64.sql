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

-- Migrated from dev/test/sql/test_bitmap_functions/T/test_bitmap_to_base64
-- Test Objective:
-- 1. Validate bitmap_to_base64(NULL) returns NULL.
-- 2. Validate bitmap_to_base64 on invalid bitmap input returns NULL.

-- query 1
select bitmap_to_base64(null);

-- query 2
select bitmap_to_base64(bitmap_from_string('abc'));
