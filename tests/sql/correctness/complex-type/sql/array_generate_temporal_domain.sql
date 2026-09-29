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

-- Test Objective: freeze ARRAY_GENERATE temporal domains before executing real columns.
-- query 1
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE temporal_range (id INT, d DATE, t DATETIME, v VARCHAR(40))
TBLPROPERTIES ("format-version" = "3");
INSERT INTO temporal_range VALUES
(1,DATE '2025-10-01',DATETIME '2025-10-01 01:00:00','2025-10-02'),
(2,DATE '2025-10-02',DATETIME '2025-10-02 01:00:00','abc'),
(3,NULL,NULL,NULL);

-- query 2
USE ${case_db};
SELECT id, array_generate(d,t,interval 1 day) AS forward,
       array_generate(t,d,interval 1 day) AS reverse,
       array_generate(d,v,interval 1 day) AS anchored_text
FROM temporal_range ORDER BY id;

-- query 3
USE ${case_db};
SELECT array_generate('2025-10-01','2025-10-03',interval 1 day) AS dates,
       array_generate('2025-01-31','2025-04-30',interval 1 month) AS month_end,
       array_generate('2025-10-03','2025-10-01',interval 1 day) AS descending;

-- query 4
USE ${case_db};
SELECT array_generate('2025-10-01 14:28:31','2025-10-01 14:28:32.800000',interval 500000 microsecond) AS forward,
       array_generate('2025-10-01 14:28:32.800000','2025-10-01 14:28:31',interval 500000 microsecond) AS reverse;

-- query 5
USE ${case_db};
SELECT array_generate(DATE '2025-10-01',DATE '2025-10-02',interval 12 hour) AS subday,
       array_generate(DATE '2025-10-01',DATETIME '2025-10-02 01:00:00',interval 12 hour) AS mixed;

-- query 6
USE ${case_db};
SELECT array_generate('2025-10-01','2025-10-02',interval 0 day) AS empty_range,
       array_generate('2025-10-01',10000,interval 0 day) AS invalid_bound,
       array_generate('2025-10-01','abc',1) AS invalid_text,
       array_generate(NULL,DATE '2025-10-01',1) AS null_bound,
       array_generate(1,1,NULL) AS numeric_null_step;

-- query 7
-- @expect_error=cannot freeze unanchored temporal or NULL bounds
USE ${case_db};
SELECT array_generate(v,v,1) FROM temporal_range;

-- query 8
-- @expect_error=cannot freeze unanchored temporal or NULL bounds
USE ${case_db};
SELECT array_generate(NULL,NULL,interval 1 day);

-- query 9
-- @expect_error=step parameter must be a constant integer
USE ${case_db};
SELECT array_generate(d,t,interval id day) FROM temporal_range;

-- query 10
-- @expect_error=step parameter must be a constant integer
USE ${case_db};
SELECT array_generate(DATE '2025-10-01',DATE '2025-10-02',NULL);

-- query 11
-- @expect_error=step parameter must be non-negative
USE ${case_db};
SELECT array_generate('2025-10-01','2025-10-02',interval -1 day);

-- query 12
-- @expect_error=cannot freeze unanchored temporal or NULL bounds
USE ${case_db};
SELECT array_generate('2025-02-30','2025-99-99',1);

-- query 13
USE ${case_db};
SELECT array_generate(3,1) AS default_desc,
       array_generate(1,5,2) AS numeric_step,
       array_generate(1,NULL,1) AS numeric_null;

-- query 14
-- @skip_result_check=true
USE ${case_db};
DROP TABLE temporal_range;
