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
-- @tags=d04,project,date,date_trunc,months_diff
-- Trino subtracts the first timestamp from the second. Near a month
-- boundary, the larger date alone does not make a complete month.
SELECT
  date_diff('month', TIMESTAMP '2024-02-01 00:00:00', TIMESTAMP '2024-01-31 00:00:00') AS months_diff_neg_boundary_v,
  date_diff('month', TIMESTAMP '2024-01-31 00:00:00', TIMESTAMP '2024-02-01 00:00:00') AS months_diff_pos_boundary_v,
  date_diff('month', TIMESTAMP '2024-01-29 12:00:01', TIMESTAMP '2024-02-29 12:00:00') AS months_diff_time_boundary_v,
  date_trunc('millisecond', TIMESTAMP '2024-01-03 03:04:05.123456') AS date_trunc_millisecond_v,
  TRY(date_trunc('microsecond', TIMESTAMP '2024-01-03 03:04:05.123456')) AS date_trunc_microsecond_v;
