-- @order_sensitive=true
-- @tags=aggregate,map_agg
-- Test Objective:
-- 1. Validate MAP_AGG materialization and key extraction.
-- 2. Prevent regressions in map aggregate state merge/finalization.
-- Test Flow:
-- 1. Create/reset key-value source table.
-- 2. Insert deterministic grouped key-value rows.
-- 3. Aggregate to map and extract keys for assertions.
DROP TABLE IF EXISTS ${case_db}.t_agg_map_agg_extract;
CREATE TABLE ${case_db}.t_agg_map_agg_extract (
    g INT,
    k VARCHAR(10),
    v INT
);

INSERT INTO ${case_db}.t_agg_map_agg_extract VALUES
    (1, 'a', 10),
    (1, 'b', 20),
    (2, 'a', 30);

SELECT
    g,
    ELEMENT_AT(MAP_AGG(k, v), 'a') AS v_a,
    ELEMENT_AT(MAP_AGG(k, v), 'b') AS v_b,
    CARDINALITY(MAP_AGG(k, v)) AS map_size
FROM ${case_db}.t_agg_map_agg_extract
GROUP BY g
ORDER BY g;
