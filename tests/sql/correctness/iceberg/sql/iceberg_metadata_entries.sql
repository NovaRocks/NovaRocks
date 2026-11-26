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
-- IV3-8: $entries exposes manifest entries with sequence numbers and status.

-- query 1
-- @skip_result_check=true
CREATE SCHEMA iceberg_cat_${suite_uuid0}.iv38e_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_cat_${suite_uuid0}.iv38e_db_${uuid0}.t_${uuid0} (id INT, v INT) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iv38e_db_${uuid0}.t_${uuid0} VALUES ROW(1,10);

-- query 4
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iv38e_db_${uuid0}.t_${uuid0} VALUES ROW(2,20);

-- query 5
-- $entries exposes entries with non-null sequence numbers; added entries exist.
SELECT count(*) > 0 AS has_entries
  FROM iceberg_cat_${suite_uuid0}.iv38e_db_${uuid0}."t_${uuid0}$entries";

-- query 6
SELECT count(*) > 0 AS has_seq
  FROM iceberg_cat_${suite_uuid0}.iv38e_db_${uuid0}."t_${uuid0}$entries"
  WHERE sequence_number IS NOT NULL;

-- query 7
SELECT count(*) > 0 AS has_added
  FROM iceberg_cat_${suite_uuid0}.iv38e_db_${uuid0}."t_${uuid0}$entries"
  WHERE status = 1;

-- query 8
SELECT sum(e.data_file.record_count) AS records,
       count(*) FILTER (WHERE typeof(e.readable_metrics) = 'json') AS metrics_rows
  FROM iceberg_cat_${suite_uuid0}.iv38e_db_${uuid0}."t_${uuid0}$entries" AS e
 WHERE e.status = 1;

-- query 9
-- @skip_result_check=true
DROP SCHEMA iceberg_cat_${suite_uuid0}.iv38e_db_${uuid0};
