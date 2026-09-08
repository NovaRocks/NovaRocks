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
-- @expect_error=no distributed write capability
INSERT INTO paimon_${suite_uuid0}.fixture.pk_default
VALUES (9, 9, 'forbidden');

-- query 2
-- @expect_error=no catalog mutation capability
CREATE TABLE paimon_${suite_uuid0}.fixture.forbidden (id INT);

-- query 3
-- @expect_error=no metadata maintenance capability
ALTER TABLE paimon_${suite_uuid0}.fixture.pk_default OPTIMIZE;

-- query 4
-- @expect_error=no statistics capability
ANALYZE TABLE paimon_${suite_uuid0}.fixture.pk_default;

-- query 5
-- @expect_error=only Paimon Parquet data files are supported
SELECT * FROM paimon_${suite_uuid0}.fixture.unsupported_orc;

-- query 6
-- @expect_error=only Paimon Parquet data files are supported
SELECT * FROM paimon_${suite_uuid0}.fixture.unsupported_avro_data;

-- query 7
-- @expect_error=Paimon deletion vectors are unsupported
SELECT * FROM paimon_${suite_uuid0}.fixture.unsupported_dv;

-- query 8
-- @expect_error=only Paimon merge-engine=deduplicate is supported
SELECT * FROM paimon_${suite_uuid0}.fixture.unsupported_aggregation;

-- query 9
-- @expect_error=Paimon column type is unsupported for PAI-1
SELECT * FROM paimon_${suite_uuid0}.fixture.unsupported_nested;

-- query 10
-- @expect_error=Paimon column type is unsupported for PAI-1
SELECT * FROM paimon_${suite_uuid0}.fixture.unsupported_timestamp_ltz;

-- query 11
-- @expect_error=multiple Paimon sequence fields are unsupported
SELECT * FROM paimon_${suite_uuid0}.fixture.unsupported_multi_sequence;

-- query 12
-- @expect_error=Paimon postpone bucket mode is unsupported
SELECT * FROM paimon_${suite_uuid0}.fixture.unsupported_postpone;
