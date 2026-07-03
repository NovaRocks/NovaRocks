-- @order_sensitive=true
-- @tags=runtime_filter,cross_process,distributed,low-cardinality,dictionary
-- C5: 1FE+3BE cross-fragment RF over a low-cardinality string probe column
-- must stay value-domain correct while scan-side dictionary values are folded.

DROP TABLE IF EXISTS ${case_db}.rf_dist_lc_probe FORCE;
DROP TABLE IF EXISTS ${case_db}.rf_dist_lc_build FORCE;

CREATE TABLE ${case_db}.rf_dist_lc_probe (
  status STRING,
  payload INT
) TBLPROPERTIES ("format-version" = "3");

CREATE TABLE ${case_db}.rf_dist_lc_build (
  status STRING,
  flag STRING
) TBLPROPERTIES ("format-version" = "3");

INSERT INTO ${case_db}.rf_dist_lc_probe
SELECT CASE i % 5
         WHEN 0 THEN 'PAID'
         WHEN 1 THEN 'NEW'
         WHEN 2 THEN 'CLOSED'
         WHEN 3 THEN 'CANCELLED'
         ELSE NULL
       END AS status,
       i * 3 AS payload
FROM TABLE(generate_series(1, 2000)) t(i);

INSERT INTO ${case_db}.rf_dist_lc_build VALUES
  ('PAID', 'Y'),
  ('CLOSED', 'Y'),
  ('NEW', 'N'),
  (NULL, 'Y');

ANALYZE FULL TABLE ${case_db}.rf_dist_lc_probe;

SET global_runtime_filter_build_max_size = 10737418240;
SET global_runtime_filter_probe_min_selectivity = 0.0;
SET cbo_broadcast_node_mem_budget_bytes = 0;

SET disable_optimizer_rules = 'RuntimeFilterPushDown';
SELECT 'rf_off' AS mode, COUNT(*) AS row_count, COALESCE(SUM(p.payload), 0) AS payload_sum
FROM ${case_db}.rf_dist_lc_probe p
JOIN ${case_db}.rf_dist_lc_build b ON p.status = b.status
WHERE b.flag = 'Y';

SET disable_optimizer_rules = '';
-- @explain_contains=HASH JOIN (PARTITIONED
-- @explain_contains=build runtime filters:
-- @explain_contains=probe runtime filters:
-- @explain_contains=dict=[status]
SELECT 'rf_on' AS mode, COUNT(*) AS row_count, COALESCE(SUM(p.payload), 0) AS payload_sum
FROM ${case_db}.rf_dist_lc_probe p
JOIN ${case_db}.rf_dist_lc_build b ON p.status = b.status
WHERE b.flag = 'Y';
