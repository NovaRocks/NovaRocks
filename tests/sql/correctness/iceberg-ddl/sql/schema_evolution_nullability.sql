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
-- ALTER COLUMN SET / DROP NOT NULL on top-level columns.
-- Identifier-field protection is exercised by unit tests; the standalone
-- INSERT path doesn't surface identifier_field_ids the way Spark does.
--
-- Note: the parquet reader enforces NOT NULL on existing data, so the
-- table is populated with non-NULL rows before SET NOT NULL is applied.

-- query 1
CREATE DATABASE iceberg_ddl_cat_${suite_uuid0}.schema_nullable_${uuid0};
USE iceberg_ddl_cat_${suite_uuid0}.schema_nullable_${uuid0};
DROP TABLE IF EXISTS members;
CREATE TABLE members (
  id INT,
  email STRING
) TBLPROPERTIES (
  "format-version" = "2"
);
INSERT INTO members VALUES (1, 'a@x.com'), (2, 'b@x.com');

-- query 2
SELECT id, email FROM members ORDER BY id;

-- query 3
-- DROP NOT NULL on already-nullable column is a no-op.
ALTER TABLE members ALTER COLUMN email DROP NOT NULL;

-- query 4
SELECT id, email FROM members ORDER BY id;

-- query 5
-- SET NOT NULL on currently-nullable column: succeeds without scanning;
-- attestation property novarocks.nullability.attested.email is recorded.
ALTER TABLE members ALTER COLUMN email SET NOT NULL;

-- query 6
SELECT id, email FROM members ORDER BY id;

-- query 7
-- DROP NOT NULL after SET NOT NULL: removes the attestation property.
ALTER TABLE members ALTER COLUMN email DROP NOT NULL;

-- query 8
SELECT id, email FROM members ORDER BY id;

-- query 9
DROP TABLE members;
DROP DATABASE iceberg_ddl_cat_${suite_uuid0}.schema_nullable_${uuid0};
