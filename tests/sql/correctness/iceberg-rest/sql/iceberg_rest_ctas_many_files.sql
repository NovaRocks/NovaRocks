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
-- Partitioned CTAS publishes file facts through its process-local sealed receipt.
-- Row and partition totals are independent of writer placement and file splitting.

-- query 1
-- @skip_result_check=true
CREATE SCHEMA iceberg_rest_${suite_uuid0}.ctas_files_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_rest_${suite_uuid0}.ctas_files_${uuid0}.dst_${uuid0} WITH (partitioning = ARRAY['part']) AS SELECT CAST(generate_series AS INT) AS part,
          CAST(generate_series * 2 AS INT) AS value
   FROM TABLE(sequence(1, 240)) AS generate_series(generate_series);

-- query 3
SELECT COUNT(*) AS row_count, SUM(part) AS part_sum, SUM(value) AS value_sum
FROM iceberg_rest_${suite_uuid0}.ctas_files_${uuid0}.dst_${uuid0};

-- query 4
SELECT COUNT(*) AS partition_count
FROM iceberg_rest_${suite_uuid0}.ctas_files_${uuid0}."dst_${uuid0}$partitions";

-- query 5
SELECT COUNT(*) >= 240 AS enough_files
FROM iceberg_rest_${suite_uuid0}.ctas_files_${uuid0}."dst_${uuid0}$files";

-- query 6
-- @skip_result_check=true
DROP TABLE iceberg_rest_${suite_uuid0}.ctas_files_${uuid0}.dst_${uuid0};

-- query 7
-- @skip_result_check=true
DROP SCHEMA iceberg_rest_${suite_uuid0}.ctas_files_${uuid0};
