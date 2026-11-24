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

-- The limited branch finishes early while its sibling drains a multi-batch source.
WITH shared AS (
    SELECT sequential_number AS n FROM TABLE(sequence(1, 32768))
)
SELECT max(n) AS drained_count, min(n) AS limited_count
FROM (
    SELECT count(*) AS n FROM shared
    UNION ALL
    SELECT count(*) AS n FROM (SELECT n FROM shared LIMIT 1) AS limited
) AS branch_counts;
