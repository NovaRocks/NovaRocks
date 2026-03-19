-- Test Objective:
-- 1. Validate case-insensitive field access on STRUCT columns with mixed-case field names.
-- 2. Cover both C2.field and table.column.field qualified access patterns.
-- 3. Use PRIMARY KEY table engine with struct column.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.struct_upper_case
    (c1 int,
    c2 struct<c2_sub1 int, C2_SUB2 int>)
    PRIMARY KEY(c1)
    DISTRIBUTED BY HASH(c1)
    BUCKETS 1
    PROPERTIES ("replication_num" = "1");

insert into ${case_db}.struct_upper_case values (1, named_struct('c2_sub1', 1, 'c2_sub2', 1)), (2, named_struct('C2_SUB1', 2, 'C2_SUB2', 2));

-- query 2
-- Access uppercase field C2_SUB2 via uppercase alias C2
select C2.c2_sub1, C2.C2_SUB2 from ${case_db}.struct_upper_case order by c1;

-- query 3
-- Access fields using lowercase alias c2
select c2.C2_SUB1, c2.c2_sub2 from ${case_db}.struct_upper_case order by c1;

-- query 4
-- Fully qualified table.column.field access with mixed case
select ${case_db}.struct_upper_case.C2.C2_SUB1, ${case_db}.struct_upper_case.C2.c2_sub2 from ${case_db}.struct_upper_case order by c1;
