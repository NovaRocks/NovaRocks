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
USE ${case_db};
CREATE TABLE encode_ignored (id INT, v1 BIGINT, v2 STRING, v3 ARRAY<INT>)
TBLPROPERTIES ("format-version"="3");

-- query 2
-- @skip_result_check=true
USE ${case_db};
INSERT INTO encode_ignored VALUES (1,7,'x',[1,2]), (2,NULL,NULL,NULL);

-- query 3
USE ${case_db};
SELECT id, FROM_BINARY(encode_row_id(v1,v2,v3),'hex') AS row_id,
FROM_BINARY(encode_fingerprint_sha256(v1,v2,v3),'hex') AS fingerprint
FROM encode_ignored ORDER BY id;

-- query 4
USE ${case_db};
SELECT id, FROM_BINARY(encode_row_id(v3),'hex') AS ignored_array,
FROM_BINARY(encode_fingerprint_sha256(v3),'hex') AS ignored_alias
FROM encode_ignored ORDER BY id;

-- query 5
SELECT FROM_BINARY(encode_row_id(NULL),'hex') AS scalar_null,
FROM_BINARY(encode_row_id([NULL]),'hex') AS container_null;

-- query 6
SELECT FROM_BINARY(encode_sort_key(CAST(7 AS INT),'x'),'hex') AS sort_key;

-- query 7
-- @expect_error=[sql.analyze.type_mismatch]
USE ${case_db};
SELECT encode_sort_key(v1,v2,v3) FROM encode_ignored;

-- query 8
-- The ignored array carrier does not suppress evaluation of its child.
-- @expect_error=assert_true failed
SELECT encode_row_id([assert_true(FALSE)]);

-- query 9
-- @skip_result_check=true
USE ${case_db};
DROP TABLE encode_ignored;
