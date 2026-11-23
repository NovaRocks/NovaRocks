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

-- @order_sensitive=true
-- @tags=project,string,length_limit
-- LPAD and RPAD retain their requested length. CONCAT and CONCAT_WS include
-- all emitted UTF-8 bytes in the one-mebibyte limit.
-- query 1
SELECT length(lpad('x', 1048576, 'ab')) AS lpad_boundary,
       length(lpad('x', 1048577, 'ab')) AS lpad_past_boundary,
       length(rpad('x', 1048576, 'ab')) AS rpad_boundary,
       length(rpad('x', 1048577, 'ab')) AS rpad_past_boundary,
       length(concat(rpad('', 600000, 'a'), rpad('', 448576, 'b'))) AS concat_boundary,
       length(concat_ws('-', rpad('', 600000, 'a'), rpad('', 448575, 'b'))) AS concat_ws_boundary;

-- query 2
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT length(concat(rpad('', 600000, 'a'), rpad('', 448577, 'b')));

-- query 3
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT length(concat_ws('-', rpad('', 600000, 'a'), rpad('', 448576, 'b')));
