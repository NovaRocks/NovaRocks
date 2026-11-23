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
CREATE VIEW default_catalog.${case_db}.sequence_view AS
SELECT sequential_number AS n FROM TABLE(sequence(stop => 1 + 2));

-- query 2
SELECT n FROM default_catalog.${case_db}.sequence_view ORDER BY n;

-- query 3
-- @skip_result_check=true
DROP VIEW default_catalog.${case_db}.sequence_view;
