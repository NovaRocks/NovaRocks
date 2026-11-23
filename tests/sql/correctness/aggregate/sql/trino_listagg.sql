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
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT LISTAGG(NULL) WITHIN GROUP (ORDER BY 1);

-- query 2
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT "listagg"(NULL, '|', true, '...', false);

-- query 3
SELECT LISTAGG(CAST(NULL AS VARCHAR)) WITHIN GROUP (ORDER BY 1) AS value;

-- query 4
SELECT "listagg"(DISTINCT x,s,true,'...',false ORDER BY s DESC) AS value
FROM (VALUES ('a','|'), ('a',':'), ('a','|')) t(x,s);

-- query 5
SELECT "listagg"(v, sep, true, '...', false ORDER BY ord) AS value
FROM (VALUES ('a','-',2), ('b','|',1)) t(v,sep,ord);

-- query 6
SELECT "listagg"(v, sep, true, '...', false ORDER BY ord) AS value
FROM (VALUES ('a',CAST(NULL AS VARCHAR),1), ('b','|',2), ('c','-',3)) t(v,sep,ord);

-- query 7
SELECT "listagg"(v, '|', true, rpad('!', n, '!'), false ORDER BY ord)
       FILTER (WHERE keep_row) AS value
FROM (VALUES ('a',65537,1,false), ('b',1,2,true)) t(v,n,ord,keep_row);

-- query 8
SELECT "listagg"(v, '|', true, rpad('!', n, '!'), false) AS value
FROM (VALUES ('a',65536)) t(v,n);

-- query 9
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT "listagg"(v, '|', true, rpad('!', n, '!'), false)
FROM (VALUES ('a',65537)) t(v,n);

-- query 10
-- @expect_trino_error=EXPRESSION_NOT_IN_DISTINCT,USER_ERROR
SELECT LISTAGG(DISTINCT v) WITHIN GROUP (ORDER BY s)
FROM (VALUES ('a','b'), ('c','d')) t(v,s);

-- query 11
WITH concatenated AS (
    SELECT LISTAGG(v, '|' ON OVERFLOW TRUNCATE '!' WITH COUNT)
           WITHIN GROUP (ORDER BY ord) AS value
    FROM (VALUES (1,rpad('é',524288,'é')), (2,'b'), (3,'c')) t(ord,v)
)
SELECT CAST(length(value) AS BIGINT) AS characters, substr(value, -5) AS suffix FROM concatenated;

-- query 12
-- @expect_trino_error=EXCEEDED_FUNCTION_MEMORY_LIMIT,USER_ERROR
SELECT LISTAGG(v) WITHIN GROUP (ORDER BY ord)
FROM (VALUES (1,rpad('é',524288,'é')), (2,'b')) t(ord,v);

-- query 13
SELECT LISTAGG(v, '|' ON OVERFLOW TRUNCATE '!' WITH COUNT)
       WITHIN GROUP (ORDER BY ord) AS value
FROM (VALUES (1,''), (2,rpad('é',524288,'é'))) t(ord,v);

-- query 14
SELECT LISTAGG(v) WITHIN GROUP (ORDER BY v) FILTER (WHERE false) AS value
FROM (VALUES 'a') t(v);
