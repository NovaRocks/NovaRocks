-- Test Objective:
-- 1. Verify expression cloning correctness when semantically equivalent expressions
--    (e.g., `if(a>5 and b>5, ...)` vs `if(b>5 and a>5, ...)`) are reused/shared.
-- 2. Verify ds_hll_count_distinct with reused if-expressions produces consistent results
--    across unnest expansion and repeated inserts.

-- query 1
USE ${case_db};
CREATE TABLE t1 (
    id BIGINT NOT NULL,
    province VARCHAR(64),
    age SMALLINT,
    dt VARCHAR(10) NOT NULL
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 4
PROPERTIES("replication_num" = "1");
INSERT INTO t1 SELECT generate_series, generate_series, generate_series % 100, "2024-07-24"
FROM TABLE(generate_series(1, 10));
SELECT t2.dt, unnest, sum(d1), sum(d2)
FROM (
    SELECT * FROM (
        SELECT dt,
            ds_hll_count_distinct(if(province>5 and id>5, age, null), 21) as d1,
            ds_hll_count_distinct(if(id>5 and province>5, age, null), 21) as d2
        FROM t1 GROUP BY dt
    ) t0, unnest([1, 2, 3]) as unnest
) t2
GROUP BY 1, 2 ORDER BY 1, 2 LIMIT 10;

-- query 2
USE ${case_db};
INSERT INTO t1 SELECT generate_series, generate_series, generate_series % 100, "2024-07-24"
FROM TABLE(generate_series(1, 10));
SELECT t2.dt, unnest, sum(d1), sum(d2)
FROM (
    SELECT * FROM (
        SELECT dt,
            ds_hll_count_distinct(if(province>5 and id>5, age, null), 21) as d1,
            ds_hll_count_distinct(if(id>5 and province>5, age, null), 21) as d2
        FROM t1 GROUP BY dt
    ) t0, unnest([1, 2, 3]) as unnest
) t2
GROUP BY 1, 2 ORDER BY 1, 2 LIMIT 10;
