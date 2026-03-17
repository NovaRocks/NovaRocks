-- Migrated from dev/test/sql/test_function/T/test_math
-- Test Objective:
-- 1. Validate cosine_similarity() and cosine_similarity_norm() on ARRAY<FLOAT> columns and literals.
-- 2. Validate l2_distance() on ARRAY<FLOAT> columns and literals.
-- 3. Verify vector math functions work correctly with table data and cross joins.
-- 4. Cover ordering by similarity/distance results.

-- query 1
-- Setup: create t1 with array<float> column
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t1 (id int, data array<float>)
    ENGINE = olap
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES("replication_num" = "1");

-- query 2
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1 VALUES(1, array<float>[0.1, 0.2, 0.3]), (2, array<float>[0.2, 0.1, 0.3]), (3, array<float>[0.3, 0.2, 0.1]);

-- query 3
-- cosine_similarity with literal array vs table column
USE ${case_db};
SELECT round(cosine_similarity(array<float>[0.1, 0.2, 0.3], data), 3) AS dist, id FROM t1 ORDER BY dist desc, id;

-- query 4
-- cosine_similarity on two literal arrays
USE ${case_db};
SELECT round(cosine_similarity(array<float>[0.1, 0.2, 0.3], array<float>[0.1, 0.2, 0.3]), 3) AS dist;

-- query 5
-- cosine_similarity_norm on two literal arrays
USE ${case_db};
SELECT round(cosine_similarity_norm(array<float>[0.1, 0.2, 0.3], array<float>[0.1, 0.2, 0.3]), 3) AS dist;

-- query 6
-- l2_distance with literal array vs table column
USE ${case_db};
SELECT round(l2_distance(array<float>[0.1, 0.2, 0.3], data), 3) AS dist, id FROM t1 ORDER BY dist desc, id;

-- query 7
-- l2_distance on two literal arrays
USE ${case_db};
SELECT round(l2_distance(array<float>[0.1, 0.2, 0.3], array<float>[0.1, 0.2, 0.3]), 3) AS dist;

-- query 8
-- Setup: create test_vector table for cross join tests
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE test_vector (id int, data array<float>)
    ENGINE=olap
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES("replication_num" = "1");

-- query 9
-- @skip_result_check=true
USE ${case_db};
INSERT INTO test_vector VALUES (1, array<float>[0.1, 0.2, 0.3]), (2, array<float>[0.2, 0.1, 0.3]);

-- query 10
-- @skip_result_check=true
USE ${case_db};
INSERT INTO test_vector VALUES (3, array<float>[0.15, 0.25, 0.32]), (4, array<float>[0.12, 0.11, 0.32]);

-- query 11
-- @skip_result_check=true
USE ${case_db};
INSERT INTO test_vector VALUES (5, array<float>[0.25, 0.12, 0.13]), (6, array<float>[0.22, 0.01, 0.39]);

-- query 12
-- cosine_similarity with literal vs table column, ordered by similarity
USE ${case_db};
SELECT id, data, round(cosine_similarity(array<float>[0.1, 0.2, 0.3], data), 3) AS sim FROM test_vector ORDER BY sim desc, id;

-- query 13
-- cosine_similarity cross join (first execution)
USE ${case_db};
SELECT a.id, b.id, a.data, b.data, round(cosine_similarity(a.data, b.data), 3) AS sim FROM test_vector AS a CROSS JOIN test_vector AS b ORDER BY sim desc, a.id, b.id;

-- query 14
-- cosine_similarity cross join (repeated for consistency)
USE ${case_db};
SELECT a.id, b.id, a.data, b.data, round(cosine_similarity(a.data, b.data), 3) AS sim FROM test_vector AS a CROSS JOIN test_vector AS b ORDER BY sim desc, a.id, b.id;

-- query 15
-- l2_distance with literal vs table column, ordered by distance
USE ${case_db};
SELECT id, data, round(l2_distance(array<float>[0.1, 0.2, 0.3], data), 3) AS sim FROM test_vector ORDER BY sim desc, id;
