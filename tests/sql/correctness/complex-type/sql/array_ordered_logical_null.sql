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

-- Test Objective: untyped NULL elements obey ordered-array null policies.
-- @order_sensitive=true

-- query 1
SELECT array_sortby([30,10,20], [NULL,NULL,NULL]) AS ties;

-- query 2
SELECT array_sortby([30,10,20], [NULL,NULL,NULL], [3,1,2]) AS refined;

-- query 3
SELECT array_sortby([30,10,20], [3,1,2], [NULL,NULL,NULL]) AS trailing_null_key;

-- query 4
SELECT array_sort([NULL,NULL,NULL]) AS sorted, array_min([NULL,NULL,NULL]) AS smallest, array_max([NULL,NULL,NULL]) AS largest;

-- query 5
SELECT array_sortby([NULL,8,7], [NULL,NULL,NULL], [3,1,2]) AS null_value_moves;
