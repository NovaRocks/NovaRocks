-- @expect_error=unsupported be schema table be_metrics
SELECT *
FROM default_catalog.information_schema.be_metrics
LIMIT 1;
