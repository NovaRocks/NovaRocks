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

SELECT [json_object('2:3')] AS json_values,
       [CAST(json_object('2:3') AS VARCHAR)] AS string_values;

SELECT array_sortby([json_object('k',1), json_object('k',2)], [2,1]) AS json_values,
       array_sortby([CAST(json_object('k',1) AS VARCHAR), CAST(json_object('k',2) AS VARCHAR)], [2,1]) AS string_values;

WITH q AS (SELECT [json_object('key','value1'), json_object('key','value2')] AS j)
SELECT array_sortby(j,[2,1]) AS json_values,
       array_sortby(CAST(j AS ARRAY<VARCHAR>),[2,1]) AS string_values FROM q;

SELECT ARRAY<JSON>[] AS json_values, CAST([] AS ARRAY<VARCHAR>) AS string_values;
