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
-- Parser-level rejects: empty parens, duplicate keys, unsupported grammar.

-- query 1
CREATE SCHEMA iceberg_ddl_cat_${suite_uuid0}.tblprops_grammar_${uuid0};
USE iceberg_ddl_cat_${suite_uuid0}.tblprops_grammar_${uuid0};
DROP TABLE IF EXISTS p;
CREATE TABLE p (id INT) WITH (format_version = 2);

-- query 2
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
ALTER TABLE p SET PROPERTIES ;

-- query 3
-- @expect_trino_error=INVALID_PROCEDURE_ARGUMENT,USER_ERROR
ALTER TABLE p EXECUTE unset_extra_properties(keys => ARRAY[], if_exists => false);

-- query 4
-- @expect_trino_error=INVALID_TABLE_PROPERTY,USER_ERROR
ALTER TABLE p SET PROPERTIES extra_properties = MAP(ARRAY['a', 'a'], ARRAY['1', '2']);

-- query 5
-- @expect_trino_error=INVALID_PROCEDURE_ARGUMENT,USER_ERROR
ALTER TABLE p EXECUTE unset_extra_properties(keys => ARRAY['a', 'a'], if_exists => false);

-- query 6
DROP TABLE p;
DROP SCHEMA iceberg_ddl_cat_${suite_uuid0}.tblprops_grammar_${uuid0};
