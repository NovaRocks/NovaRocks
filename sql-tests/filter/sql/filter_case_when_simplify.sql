-- Migrated from dev/test/sql/test_case_when/T/test_case_when
-- Test Objective:
-- 1. Validate CASE WHEN simplification with ENABLE_SIMPLIFY_CASE_WHEN=true.
-- 2. Cover nullable column (ship_code INT): CASE WHEN col>=N THEN letter with =, <>, IN, NOT IN, IS NULL, IS NOT NULL.
-- 3. Cover string column (region): CASE WHEN col=X THEN N with various comparators.
-- 4. Cover IS NULL in WHEN clause, duplicate WHEN values, no-ELSE CASE.
-- 5. Cover IF() and NULLIF() simplification.
-- 6. Cover simple CASE (CASE col WHEN val THEN ...) form.
-- 7. Cover non-nullable column (ship_mode INT): same CASE structure without NULL inputs.
-- 8. Cover NULL predicates on CASE result: =NULL, <>NULL, <=>NULL, IN(NULL), NOT IN(NULL).
-- 9. Cover complex nested CASE WHEN LIKE/REPLACE on an empty table.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t0;
CREATE TABLE ${case_db}.t0 (
  `region` varchar(128) NOT NULL COMMENT "",
  `order_date` date NOT NULL COMMENT "",
  `income` decimal(7, 0) NOT NULL COMMENT "",
  `ship_mode` int NOT NULL COMMENT "",
  `ship_code` int) ENGINE=OLAP
DUPLICATE KEY(`region`, `order_date`)
COMMENT "OLAP"
PARTITION BY RANGE(`order_date`)
(PARTITION p20220101 VALUES [("2022-01-01"), ("2022-01-02")),
PARTITION p20220102 VALUES [("2022-01-02"), ("2022-01-03")),
PARTITION p20220103 VALUES [("2022-01-03"), ("2022-01-05")))
DISTRIBUTED BY HASH(`region`, `order_date`) BUCKETS 10
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.t0 (`region`, `order_date`, `income`, `ship_mode`, `ship_code`) VALUES
('USA', '2022-01-01', 12345, 50, 1),
('CHINA', '2022-01-02', 54321, 51, 4),
('JAPAN', '2022-01-03', 67890, 610, 6),
('UK', '2022-01-04', 98765, 75, 2),
('AUS', '2022-01-01', 23456, 25, 18),
('AFRICA', '2022-01-02', 87654, 125, 7),
('USA', '2022-01-03', 54321, 75, null),
('CHINA', '2022-01-04', 12345, 100, 3),
('JAPAN', '2022-01-01', 67890, 64, 10),
('UK', '2022-01-02', 54321, 25, 5),
('AUS', '2022-01-03', 98765, 150, 15),
('AFRICA', '2022-01-04', 23456, 75, null),
('USA', '2022-01-01', 87654, 125, 2),
('CHINA', '2022-01-02', 54321, 175, 12),
('JAPAN', '2022-01-03', 12345, 100, 3),
('UK', '2022-01-04', 67890, 50, 10),
('AUS', '2022-01-01', 54321, 25, 5),
('AFRICA', '2022-01-02', 98765, 150, 15),
('USA', '2022-01-03', 23456, 75, 18),
('CHINA', '2022-01-04', 87654, 125, 7),
('JAPAN', '2022-01-01', 54321, 175, 12),
('UK', '2022-01-02', 12345, 86, 3),
('AUS', '2022-01-03', 67890, 50, 10),
('AFRICA', '2022-01-04', 54321, 25, 95),
('USA', '2022-01-01', 98765, 150, 55),
('CHINA', '2022-01-02', 23456, 75, 88),
('JAPAN', '2022-01-03', 87654, 125, 67),
('UK', '2022-01-04', 54321, 82, 72),
('AUS', '2022-01-01', 12345, 90, 35),
('AFRICA', '2022-01-02', 67890, 50, 100),
('USA', '2022-01-03', 54321, 25, 5),
('CHINA', '2022-01-04', 98765, 150, 15),
('JAPAN', '2022-01-01', 23456, 75, null);

-- query 3
-- @skip_result_check=true
set ENABLE_SIMPLIFY_CASE_WHEN = true;

-- Section 1: Nullable INT column (ship_code) with CASE WHEN col>=N THEN letter

-- query 4
-- CASE ship_code = 'A' (ship_code >= 90)
select * from ${case_db}.t0 where (case
   when ship_code >= 90 then 'A'
   when ship_code >= 80 then 'B'
   when ship_code >= 70 then 'C'
   when ship_code >= 60 then 'D'
   else 'E' end) = 'A' order by 1,2,3,4,5;

-- query 5
-- CASE ship_code = 'B' (ship_code >= 80 and < 90)
select * from ${case_db}.t0 where (case
   when ship_code >= 90 then 'A'
   when ship_code >= 80 then 'B'
   when ship_code >= 70 then 'C'
   when ship_code >= 60 then 'D'
   else 'E' end) = 'B' order by 1,2,3,4,5;

-- query 6
-- CASE ship_code = 'C' (ship_code >= 70 and < 80)
select * from ${case_db}.t0 where (case
   when ship_code >= 90 then 'A'
   when ship_code >= 80 then 'B'
   when ship_code >= 70 then 'C'
   when ship_code >= 60 then 'D'
   else 'E' end) = 'C' order by 1,2,3,4,5;

-- query 7
-- CASE ship_code <> 'D': all rows except those with 60<=ship_code<70 (and NULL rows)
select * from ${case_db}.t0 where (case
   when ship_code >= 90 then 'A'
   when ship_code >= 80 then 'B'
   when ship_code >= 70 then 'C'
   when ship_code >= 60 then 'D'
   else 'E' end) <> 'D' order by 1,2,3,4,5;

-- query 8
-- CASE ship_code <> 'E': rows where ship_code >= 60 (NULL rows excluded)
select * from ${case_db}.t0 where (case
   when ship_code >= 90 then 'A'
   when ship_code >= 80 then 'B'
   when ship_code >= 70 then 'C'
   when ship_code >= 60 then 'D'
   else 'E' end) <> 'E' order by 1,2,3,4,5;

-- query 9
-- CASE ship_code IN ('A','B')
select * from ${case_db}.t0 where (case
   when ship_code >= 90 then 'A'
   when ship_code >= 80 then 'B'
   when ship_code >= 70 then 'C'
   when ship_code >= 60 then 'D'
   else 'E' end) in ('A','B') order by 1,2,3,4,5;

-- query 10
-- CASE ship_code IN ('D','E'): rows with ship_code < 70 (and NULL rows → 'E')
select * from ${case_db}.t0 where (case
   when ship_code >= 90 then 'A'
   when ship_code >= 80 then 'B'
   when ship_code >= 70 then 'C'
   when ship_code >= 60 then 'D'
   else 'E' end) in ('D','E') order by 1,2,3,4,5;

-- query 11
-- CASE ship_code IN (NULL): no rows matched (IN NULL predicate)
select * from ${case_db}.t0 where (case
   when ship_code >= 90 then 'A'
   when ship_code >= 80 then 'B'
   when ship_code >= 70 then 'C'
   when ship_code >= 60 then 'D'
   else 'E' end) in (NULL) order by 1,2,3,4,5;

-- query 12
-- CASE ship_code IN ('A','B','C','D','E'): all non-NULL ship_code rows
select * from ${case_db}.t0 where (case
   when ship_code >= 90 then 'A'
   when ship_code >= 80 then 'B'
   when ship_code >= 70 then 'C'
   when ship_code >= 60 then 'D'
   else 'E' end) in ('A','B','C','D','E') order by 1,2,3,4,5;

-- query 13
-- CASE ship_code NOT IN ('A','B','C'): rows with ship_code < 70 (excluding NULL)
select * from ${case_db}.t0 where (case
   when ship_code >= 90 then 'A'
   when ship_code >= 80 then 'B'
   when ship_code >= 70 then 'C'
   when ship_code >= 60 then 'D'
   else 'E' end) not in ('A','B', 'C') order by 1,2,3,4,5;

-- query 14
-- CASE ship_code NOT IN ('D','E'): rows with ship_code >= 70 (excluding NULL)
select * from ${case_db}.t0 where (case
   when ship_code >= 90 then 'A'
   when ship_code >= 80 then 'B'
   when ship_code >= 70 then 'C'
   when ship_code >= 60 then 'D'
   else 'E' end) not in ('D','E') order by 1,2,3,4,5;

-- query 15
-- CASE ship_code NOT IN (NULL): no rows returned (NOT IN with NULL list)
select * from ${case_db}.t0 where (case
   when ship_code >= 90 then 'A'
   when ship_code >= 80 then 'B'
   when ship_code >= 70 then 'C'
   when ship_code >= 60 then 'D'
   else 'E' end) not in (NULL) order by 1,2,3,4,5;

-- query 16
-- CASE ship_code IS NULL: ship_code IS NULL → CASE evaluates to 'E', not NULL;
-- ship_code IS NULL doesn't make CASE result NULL (ELSE 'E' is always non-NULL)
select * from ${case_db}.t0 where (case
   when ship_code >= 90 then 'A'
   when ship_code >= 80 then 'B'
   when ship_code >= 70 then 'C'
   when ship_code >= 60 then 'D'
   else 'E' end) is NULL order by 1,2,3,4,5;

-- query 17
-- CASE ship_code IS NOT NULL: all rows (ELSE always returns non-NULL)
select * from ${case_db}.t0 where (case
   when ship_code >= 90 then 'A'
   when ship_code >= 80 then 'B'
   when ship_code >= 70 then 'C'
   when ship_code >= 60 then 'D'
   else 'E' end) is NOT NULL order by 1,2,3,4,5;

-- Section 2: String column (region) with CASE WHEN col=X THEN N

-- query 18
-- CASE region = 1 (region = 'China')
select * from ${case_db}.t0 where
(case when region = 'China' then 1 when region = 'Japan' then 2 else 3 end) = 1 order by 1,2,3,4,5;

-- query 19
-- CASE region <> 1 (not China)
select * from ${case_db}.t0 where
(case when region = 'China' then 1 when region = 'Japan' then 2 else 3 end) <> 1 order by 1,2,3,4,5;

-- query 20
-- CASE region IN (1): same as = 1
select * from ${case_db}.t0 where
(case when region = 'China' then 1 when region = 'Japan' then 2 else 3 end) in (1) order by 1,2,3,4,5;

-- query 21
-- CASE region NOT IN (1): same as <> 1
select * from ${case_db}.t0 where
(case when region = 'China' then 1 when region = 'Japan' then 2 else 3 end) not in (1) order by 1,2,3,4,5;

-- query 22
-- CASE region IS NULL: impossible since region is NOT NULL and ELSE=3 is always non-NULL
select * from ${case_db}.t0 where
(case when region = 'China' then 1 when region = 'Japan' then 2 else 3 end) is null order by 1,2,3,4,5;

-- query 23
-- CASE region IS NOT NULL: all rows
select * from ${case_db}.t0 where
(case when region = 'China' then 1 when region = 'Japan' then 2 else 3 end) is not null order by 1,2,3,4,5;

-- query 24
-- No ELSE: CASE returns NULL when no WHEN matches; IS NULL = rows where region is not China or Japan
select * from ${case_db}.t0 where
(case when region = 'China' then 1 when region = 'Japan' then 2 end) is null order by 1,2,3,4,5;

-- query 25
-- No ELSE: IS NOT NULL = rows where region is China or Japan
select * from ${case_db}.t0 where
(case when region = 'China' then 1 when region = 'Japan' then 2 end) is not null order by 1,2,3,4,5;

-- query 26
-- NULL THEN value: IS NULL returns both China rows (explicit NULL) and non-match rows (implicit NULL)
select * from ${case_db}.t0 where
(case when region = 'China' then NULL when region = 'Japan' then 2 end) is null order by 1,2,3,4,5;

-- Section 3: IS NULL in WHEN clause

-- query 27
-- ship_code IS NULL check in WHEN: ship_code IS NULL → 'a', ship_code=1 → 'b', ship_code=2 → 'c', else 'd'
select * from ${case_db}.t0 where case when ship_code is null then 'a' when ship_code = 1 then 'b' when ship_code = 2 then 'c' else 'd' end != 'c' order by 1,2,3,4,5;

-- query 28
select * from ${case_db}.t0 where case when ship_code is null then 'a' when ship_code = 1 then 'b' when ship_code = 2 then 'c' else 'd' end in ('a', 'b', 'c') order by 1,2,3,4,5;

-- query 29
select * from ${case_db}.t0 where case when ship_code is null then 'a' when ship_code = 1 then 'b' when ship_code = 2 then 'c' end != 'c' order by 1,2,3,4,5;

-- Section 4: NULL predicates on CASE result (using region/simple cases)

-- query 30
-- CASE result = NULL: always false (NULL comparison)
select * from ${case_db}.t0 where
(case region when 'China' then 1 when 'Japan' then 2 else 3 end) = NULL order by 1,2,3,4,5;

-- query 31
-- CASE result <> NULL: always false
select * from ${case_db}.t0 where
(case region when 'China' then 1 when 'Japan' then 2 else 3 end) <> NULL order by 1,2,3,4,5;

-- query 32
-- CASE result <=> NULL: null-safe equal; returns rows where CASE result is NULL
-- (with ELSE=3, no rows match)
select * from ${case_db}.t0 where
(case region when 'China' then 1 when 'Japan' then 2 else 3 end) <=> NULL order by 1,2,3,4,5;

-- query 33
-- CASE result IN (1, NULL): rows where CASE result = 1 (NULL in list doesn't match via IN)
select * from ${case_db}.t0 where
(case region when 'China' then 1 when 'Japan' then 2 else 3 end) in (1,NULL) order by 1,2,3,4,5;

-- query 34
-- CASE result NOT IN (1, NULL): no rows (NOT IN with NULL in list always false)
select * from ${case_db}.t0 where
(case region when 'China' then 1 when 'Japan' then 2 else 3 end) not in (1,NULL) order by 1,2,3,4,5;

-- Section 5: IF() function simplification

-- query 35
select * from ${case_db}.t0 where if(region='USA', 1, 0) = 1 order by 1,2,3,4,5;

-- query 36
select * from ${case_db}.t0 where if(region='USA', 1, 0) = 0 order by 1,2,3,4,5;

-- query 37
select * from ${case_db}.t0 where if(region='USA', 1, 0) <> 1 order by 1,2,3,4,5;

-- query 38
select * from ${case_db}.t0 where if(region='USA', 1, 0) in (1) order by 1,2,3,4,5;

-- query 39
select * from ${case_db}.t0 where if(region='USA', 1, 0) not in (0) order by 1,2,3,4,5;

-- query 40
select * from ${case_db}.t0 where if(region='USA', 1, 0) is NULL order by 1,2,3,4,5;

-- query 41
select * from ${case_db}.t0 where if(region='USA', 1, 0) is NOT NULL order by 1,2,3,4,5;

-- query 42
-- IF(ship_code IS NULL, NULL, 0) IS NULL: rows where ship_code IS NULL
select * from ${case_db}.t0 where if(ship_code is null, null, 0) is NULL order by 1,2,3,4,5;

-- query 43
-- WITH clause using IF
with tmp as (select ship_mode, if(ship_code > 4, 1, 0) as layer0, if (ship_code >= 1 and ship_code <= 4, 1, 0) as layer1, if(ship_code is null or ship_code < 1, 1, 0) as layer2 from ${case_db}.t0) select * from tmp where layer2 = 1 and layer0 != 1 and layer1 !=1 order by 1,2,3;

-- Section 6: NULLIF() function simplification

-- query 44
select * from ${case_db}.t0 where nullif('China', region) = 'China' order by 1,2,3,4,5;

-- query 45
select * from ${case_db}.t0 where nullif('China', region) is NULL order by 1,2,3,4,5;

-- query 46
select * from ${case_db}.t0 where nullif('China', region) is NOT NULL order by 1,2,3,4,5;

-- query 47
select * from ${case_db}.t0 where nullif(1, ship_code) = 1 order by 1,2,3,4,5;

-- query 48
select * from ${case_db}.t0 where nullif(1, ship_code) is NULL order by 1,2,3,4,5;

-- query 49
select * from ${case_db}.t0 where nullif(1, ship_code) is NOT NULL order by 1,2,3,4,5;

-- Section 7: Simple CASE form (CASE col WHEN val THEN ...)

-- query 50
select * from ${case_db}.t0 where
(case region when 'China' then 1 when 'Japan' then 2 else 3 end) = 1 order by 1,2,3,4,5;

-- query 51
select * from ${case_db}.t0 where
(case region when 'China' then 1 when 'Japan' then 2 else 3 end) in (1,2) order by 1,2,3,4,5;

-- query 52
select * from ${case_db}.t0 where
(case region when 'China' then 1 when 'Japan' then 2 else 3 end) <> 1 order by 1,2,3,4,5;

-- Section 8: Non-nullable INT column (ship_mode) with CASE WHEN

-- query 53
select * from ${case_db}.t0 where (case
   when ship_mode >= 90 then 'A'
   when ship_mode >= 80 then 'B'
   when ship_mode >= 70 then 'C'
   when ship_mode >= 60 then 'D'
   else 'E' end) = 'A' order by 1,2,3,4,5;

-- query 54
select * from ${case_db}.t0 where (case
   when ship_mode >= 90 then 'A'
   when ship_mode >= 80 then 'B'
   when ship_mode >= 70 then 'C'
   when ship_mode >= 60 then 'D'
   else 'E' end) in ('A','B','C','D','E') order by 1,2,3,4,5;

-- query 55
-- IS NULL: impossible since ship_mode is NOT NULL and ELSE='E' covers all cases
select * from ${case_db}.t0 where (case
   when ship_mode >= 90 then 'A'
   when ship_mode >= 80 then 'B'
   when ship_mode >= 70 then 'C'
   when ship_mode >= 60 then 'D'
   else 'E' end) is NULL order by 1,2,3,4,5;

-- query 56
select * from ${case_db}.t0 where (case
   when ship_mode >= 90 then 'A'
   when ship_mode >= 80 then 'B'
   when ship_mode >= 70 then 'C'
   when ship_mode >= 60 then 'D'
   else 'E' end) not in ('A','B') order by 1,2,3,4,5;

-- Section 9: Complex nested CASE WHEN LIKE/REPLACE on empty table

-- query 57
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.ads_opt;
CREATE TABLE ${case_db}.ads_opt (
  `region` varchar(128) NOT NULL COMMENT "",
  `handed_dept_name` int NOT NULL COMMENT "")
ENGINE=OLAP
DUPLICATE KEY(`region`)
COMMENT "OLAP"
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);

-- query 58
-- Deeply nested CASE WHEN LIKE/REPLACE on empty table: result must be empty (0 rows)
SELECT *,
       CASE
         WHEN `handling_department_2` LIKE '%Ganzhou Transfer Yard%'
           THEN REPLACE(`handling_department_2`, 'Ganzhou Transfer Yard', 'Ganzhou Transfer Yard Loading/Unloading Team 1')
         WHEN `handling_department_2` LIKE '%Zhexi Hub Center%'
           THEN REPLACE(`handling_department_2`, 'Zhexi Hub Center', 'Hangzhou Transfer Yard')
         WHEN `handling_department_2` LIKE '%Shanghai Hub Center%'
           THEN REPLACE(`handling_department_2`, 'Shanghai Hub Center', 'Shanghai Transfer Yard')
         WHEN `handling_department_2` LIKE '%Changzhou Transfer Center%'
           THEN REPLACE(`handling_department_2`, 'Changzhou Transfer Center', 'Changzhou Transfer Yard')
         WHEN `handling_department_2` LIKE '%Nanjing Transfer Center%'
           THEN REPLACE(`handling_department_2`, 'Nanjing Transfer Center', 'Nanjing Transfer Yard')
         ELSE `handling_department_2`
       END AS `handling_department_3`
FROM (
    SELECT *,
           CASE
             WHEN `handling_department` LIKE '%Ganzhou Transfer Yard%'
               THEN REPLACE(`handling_department`, 'Ganzhou Transfer Yard', 'Ganzhou Transfer Yard Loading/Unloading Team 1')
             WHEN `handling_department` LIKE '%Zhexi Hub Center%'
               THEN REPLACE(`handling_department`, 'Zhexi Hub Center', 'Hangzhou Transfer Yard')
             WHEN `handling_department` LIKE '%Shanghai Hub Center%'
               THEN REPLACE(`handling_department`, 'Shanghai Hub Center', 'Shanghai Transfer Yard')
             WHEN `handling_department` LIKE '%Changzhou Transfer Center%'
               THEN REPLACE(`handling_department`, 'Changzhou Transfer Center', 'Changzhou Transfer Yard')
             WHEN `handling_department` LIKE '%Nanjing Transfer Center%'
               THEN REPLACE(`handling_department`, 'Nanjing Transfer Center', 'Nanjing Transfer Yard')
             ELSE `handling_department`
           END AS `handling_department_2`
    FROM (
        SELECT CAST(`handling_department` AS STRING) AS `handling_department`
        FROM (
            SELECT handed_dept_name AS `handling_department`
            FROM ${case_db}.ads_opt
        ) dataset_table
    ) table_1
) table_2;
