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
-- @sequential=true
-- @order_sensitive=true

-- query 1
SELECT CAST('a' AS varchar) || CAST('b' AS varchar) AS result;

-- query 2
SELECT CAST(NULL AS varchar) || CAST('b' AS varchar) AS result;

-- query 3
SELECT CAST('a' AS varchar) || CAST(NULL AS varchar) AS result;

-- query 4
SELECT CAST('a' AS varchar) || CAST('b' AS varchar) = CAST('ab' AS varchar) AS result;

-- query 5
SELECT ARRAY[CAST(1 AS integer)] || ARRAY[CAST(2 AS integer)] AS result;

-- query 6
SELECT "orders" FROM (VALUES (CAST(1 AS integer))) AS t("orders");

-- query 7
SELECT "a""b" FROM (VALUES (CAST(2 AS integer))) AS t("a""b");

-- query 8
SELECT "MiXeD", mixed, MIXED FROM (VALUES (CAST(3 AS integer))) AS t("MiXeD");

-- query 9
SELECT "select" FROM (VALUES (CAST(4 AS integer))) AS t("select");

-- query 10
SELECT "t$snapshots" FROM (VALUES (CAST(5 AS integer))) AS t("t$snapshots");

-- query 11
SELECT CAST('a''b' AS varchar) AS result;

-- query 12
SELECT CAST('a\tb' AS varchar) AS result;

-- query 13
SELECT CAST('a\' AS varchar) AS result;

-- query 14
SELECT CAST(U&'a\0009b' AS varchar) AS result;

-- query 15
SELECT CAST(U&'\+01F600' AS varchar) AS result;

-- query 16
SELECT CAST(U&'a!0009b' UESCAPE '!' AS varchar) AS result;

-- query 17
SELECT CAST(U&'a\\b' AS varchar) AS result;

-- query 18
SELECT CAST('a
b' AS varchar) AS result;

-- query 19
SELECT CAST('a_b' AS varchar) LIKE CAST('a\_b' AS varchar) AS result;

-- query 20
SELECT CAST('a_b' AS varchar) LIKE CAST('a\_b' AS varchar) ESCAPE '\' AS result;

-- query 21
SELECT CAST('a\xb' AS varchar) LIKE CAST('a\_b' AS varchar) AS result;

-- query 22
SELECT CAST('axb' AS varchar) LIKE CAST('a_b' AS varchar) AS result;

-- query 23
SELECT CAST(0x1F AS integer) AS result;

-- query 24
SELECT CAST(0o17 AS integer) AS result;

-- query 25
SELECT CAST(0b1011 AS integer) AS result;

-- query 26
SELECT CAST(1_234 AS integer) AS result;

-- query 27
SELECT CAST(0x_FF AS integer) AS result;

-- query 28
SELECT CAST(0o_17 AS integer) AS result;

-- query 29
SELECT CAST(0b_10_01 AS integer) AS result;

-- query 30
SELECT CAST(0XfF AS integer) AS result;

-- query 31
SELECT /*+ SET_VAR(no_such_property = 1) */ CAST(7 AS integer) AS result;

-- query 32
SELECT--text
CAST(8 AS integer) AS result;

-- query 33
SELECT /* ordinary */ CAST(9 AS integer) AS result;

-- query 34
SELECT /* outer /* inner */ CAST(10 AS integer) AS result;

-- query 35
SELECT l || r AS result FROM (
    VALUES
        (CAST(1 AS integer), ARRAY[CAST(1 AS integer), CAST(NULL AS integer)], ARRAY[CAST(2 AS integer)]),
        (CAST(2 AS integer), CAST(NULL AS ARRAY(integer)), ARRAY[CAST(3 AS integer)]),
        (CAST(3 AS integer), CAST(ARRAY[] AS ARRAY(integer)), ARRAY[CAST(4 AS integer)])
) AS t(id, l, r) ORDER BY id;
