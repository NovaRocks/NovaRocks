-- @tags=low-cardinality,dictionary,runtime-filter
-- C5: runtime filters keep value-domain semantics while scan-side dictionary
-- probe columns are folded to dictionary-key acceptance masks.

CREATE TABLE ${case_db}.dict_rf_probe_t (
  id INT,
  status STRING,
  payload INT
) TBLPROPERTIES ("format-version" = "3");

CREATE TABLE ${case_db}.dict_rf_build_t (
  status STRING,
  flag STRING
) TBLPROPERTIES ("format-version" = "3");

INSERT INTO ${case_db}.dict_rf_probe_t VALUES
  (1, 'PAID', 10),
  (2, 'NEW', 20),
  (3, 'CLOSED', 30),
  (4, NULL, 40),
  (5, 'PAID', 50),
  (6, 'CANCELLED', 60),
  (7, 'NEW', 70);

INSERT INTO ${case_db}.dict_rf_build_t VALUES
  ('PAID', 'Y'),
  ('NEW', 'N'),
  ('CLOSED', 'Y'),
  (NULL, 'Y');

ANALYZE FULL TABLE ${case_db}.dict_rf_probe_t;
ANALYZE FULL TABLE ${case_db}.dict_rf_build_t;

SET global_runtime_filter_build_max_size = 10737418240;
SET global_runtime_filter_probe_min_selectivity = 0.0;

SET disable_optimizer_rules = 'RuntimeFilterPushDown';
SELECT 'rf_off' AS mode, COUNT(*) AS c, COALESCE(SUM(p.payload), 0) AS payload_sum
FROM ${case_db}.dict_rf_probe_t p
JOIN ${case_db}.dict_rf_build_t b ON p.status = b.status
WHERE b.flag = 'Y';

SET disable_optimizer_rules = '';
-- @explain_contains=dict=[status]
-- @explain_contains=build runtime filters:
-- @explain_contains=probe runtime filters:
SELECT 'rf_on' AS mode, COUNT(*) AS c, COALESCE(SUM(p.payload), 0) AS payload_sum
FROM ${case_db}.dict_rf_probe_t p
JOIN ${case_db}.dict_rf_build_t b ON p.status = b.status
WHERE b.flag = 'Y';
