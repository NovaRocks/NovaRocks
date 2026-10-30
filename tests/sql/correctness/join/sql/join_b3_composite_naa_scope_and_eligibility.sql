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
-- @tags=join,not_in,composite,scope,eligibility
-- Hand-authored expected IDs distinguish C UNKNOWN from E UNKNOWN; duplicates are physical rows.

-- query 1
WITH p AS (SELECT * FROM (VALUES
(1,1,1,10,0),(2,1,NULL,10,NULL),(3,1,2,20,0),(4,2,NULL,20,NULL),
(5,NULL,1,10,0),(6,3,NULL,99,NULL),(7,2,9,90,0),(7,2,9,90,0)) v(id,g,a,b,cutoff)),
b AS (SELECT * FROM (VALUES
(1,1,10,1),(1,1,10,1),(1,NULL,30,1),(1,2,NULL,1),(2,4,20,1),
(NULL,1,10,1),(3,1,10,1),(3,NULL,30,1)) v(g,a,b,weight))
SELECT p.id FROM p WHERE (p.a,p.b) NOT IN (SELECT b.a,b.b FROM b WHERE b.g=p.g) ORDER BY p.id;

-- query 2
WITH p AS (SELECT * FROM (VALUES
(1,1,1,10,0),(2,1,NULL,10,NULL),(3,1,2,20,0),(4,2,NULL,20,NULL),
(5,NULL,1,10,0),(6,3,NULL,99,NULL),(7,2,9,90,0),(7,2,9,90,0)) v(id,g,a,b,cutoff)),
b AS (SELECT * FROM (VALUES
(1,1,10,1),(1,1,10,1),(1,NULL,30,1),(1,2,NULL,1),(2,4,20,1),
(NULL,1,10,1),(3,1,10,1),(3,NULL,30,1)) v(g,a,b,weight))
SELECT p.id FROM p WHERE (p.a,p.b) NOT IN (SELECT b.a,b.b FROM b WHERE b.g <=> p.g) ORDER BY p.id;

-- query 3
WITH p AS (SELECT * FROM (VALUES
(1,1,1,10,0),(2,1,NULL,10,NULL),(3,1,2,20,0),(4,2,NULL,20,NULL),
(5,NULL,1,10,0),(6,3,NULL,99,NULL),(7,2,9,90,0),(7,2,9,90,0)) v(id,g,a,b,cutoff)),
b AS (SELECT * FROM (VALUES
(1,1,10,1),(1,1,10,1),(1,NULL,30,1),(1,2,NULL,1),(2,4,20,1),
(NULL,1,10,1),(3,1,10,1),(3,NULL,30,1)) v(g,a,b,weight))
SELECT p.id FROM p WHERE (p.a,p.b) NOT IN (SELECT b.a,b.b FROM b WHERE b.g=p.g AND b.weight>p.cutoff) ORDER BY p.id;

-- query 4
WITH p AS (SELECT * FROM (VALUES
(1,1,1,10,0),(2,1,NULL,10,NULL),(3,1,2,20,0),(4,2,NULL,20,NULL),
(5,NULL,1,10,0),(6,3,NULL,99,NULL),(7,2,9,90,0),(7,2,9,90,0)) v(id,g,a,b,cutoff)),
b AS (SELECT * FROM (VALUES
(1,1,10,1),(1,1,10,1),(1,NULL,30,1),(1,2,NULL,1),(2,4,20,1),
(NULL,1,10,1),(3,1,10,1),(3,NULL,30,1)) v(g,a,b,weight))
SELECT p.id FROM p WHERE (p.a,p.b) NOT IN (SELECT b.a,b.b FROM b WHERE b.g <=> p.g AND b.weight>p.cutoff) ORDER BY p.id;

-- query 5
WITH p AS (SELECT * FROM (VALUES
(1,1,1,10,0),(2,1,NULL,10,NULL),(3,1,2,20,0),(4,2,NULL,20,NULL),
(5,NULL,1,10,0),(6,3,NULL,99,NULL),(7,2,9,90,0),(7,2,9,90,0)) v(id,g,a,b,cutoff)),
b AS (SELECT * FROM (VALUES
(1,1,10,1),(1,1,10,1),(1,NULL,30,1),(1,2,NULL,1),(2,4,20,1),
(NULL,1,10,1),(3,1,10,1),(3,NULL,30,1)) v(g,a,b,weight))
SELECT p.id FROM p WHERE (p.a,p.b) NOT IN (SELECT b.a,b.b FROM b WHERE b.g=p.g AND b.weight<0) ORDER BY p.id;

-- query 6
WITH p AS (SELECT * FROM (VALUES
(1,1,1,10,0),(2,1,NULL,10,NULL),(3,1,2,20,0),(4,2,NULL,20,NULL),
(5,NULL,1,10,0),(6,3,NULL,99,NULL),(7,2,9,90,0),(7,2,9,90,0)) v(id,g,a,b,cutoff)),
b AS (SELECT * FROM (VALUES
(1,1,10,1),(1,1,10,1),(1,NULL,30,1),(1,2,NULL,1),(2,4,20,1),
(NULL,1,10,1),(3,1,10,1),(3,NULL,30,1)) v(g,a,b,weight))
SELECT p.id FROM p WHERE (p.a,p.b) NOT IN (SELECT b.a,b.b FROM b WHERE TRUE) ORDER BY p.id;
