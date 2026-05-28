-- @tags=join,array,not_in,element_null
-- Test Objective:
-- Validate that `array NOT IN (subq)` propagates element-level NULL semantics
-- correctly. SQL standard: when comparing two arrays where either contains a
-- NULL element, equality is UNKNOWN; consequently `lhs NOT IN …` is UNKNOWN
-- and the WHERE filter must exclude that row.
--
-- Repro from `join_array_type` step 33: `d_1 NOT IN (SELECT i_0 FROM t)` with
-- `d_1=[1.00,null]` and a candidate row `i_0=[1,null]`. Before the fix the
-- ANTI JOIN saw the condition evaluate to NULL (treated as no-match) and
-- kept the row; expected: the row is dropped because the IN result is NULL.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.arr_lhs (
  `pk` int(11) NOT NULL,
  `d_1` Array<DECIMAL(26, 2)>
) ENGINE=OLAP
DUPLICATE KEY(`pk`)
DISTRIBUTED BY HASH(`pk`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);

-- query 2
-- @skip_result_check=true
CREATE TABLE ${case_db}.arr_rhs (
  `pk` int(11) NOT NULL,
  `i_0` Array<INT> NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`pk`)
DISTRIBUTED BY HASH(`pk`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.arr_lhs VALUES
    (1, [1.00, 2.00]),
    (2, [1.00, NULL]),
    (3, [3.00, 4.00]);

-- query 4
-- @skip_result_check=true
INSERT INTO ${case_db}.arr_rhs VALUES
    (1, [1, 2]),
    (2, [1, NULL]),
    (3, [9, 9]);

-- pk=1 d_1=[1.00,2.00]: matches arr_rhs.pk=1 [1,2] → IN=TRUE → NOT IN=FALSE → excluded
-- pk=2 d_1=[1.00,NULL]: arr_rhs.pk=2 [1,NULL] makes eq=NULL (element-NULL) → IN=NULL → NOT IN=NULL → excluded
-- pk=3 d_1=[3.00,4.00]: no array matches; subq does not contain a whole-NULL array; NOT IN=TRUE → kept

-- query 5
-- @order_sensitive=true
SELECT pk
FROM ${case_db}.arr_lhs
WHERE d_1 NOT IN (SELECT i_0 FROM ${case_db}.arr_rhs)
ORDER BY pk;
