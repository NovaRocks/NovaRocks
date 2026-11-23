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

-- @tags=low-cardinality,dictionary,filter
-- Verify simple string filters over runtime dictionary carriers preserve
-- selected and passthrough string columns.
CREATE TABLE ${case_db}.dict_filter_fastpath_c1_t (
  id INT,
  status VARCHAR,
  channel VARCHAR
) WITH (format_version = 3);
INSERT INTO ${case_db}.dict_filter_fastpath_c1_t VALUES
  ROW(1, 'PAID', 'web'),
  ROW(2, 'PENDING', 'retail'),
  ROW(3, 'CLOSED', 'ops'),
  ROW(4, NULL, 'retail'),
  ROW(5, 'PAID', 'ops'),
  ROW(6, NULL, 'web');
SELECT id, status, channel
FROM ${case_db}.dict_filter_fastpath_c1_t
WHERE status = 'PAID'
ORDER BY id;
SELECT id, status
FROM ${case_db}.dict_filter_fastpath_c1_t
WHERE status IN ('PAID', 'CLOSED')
ORDER BY id;
SELECT id, status
FROM ${case_db}.dict_filter_fastpath_c1_t
WHERE status IS NULL
ORDER BY id;
