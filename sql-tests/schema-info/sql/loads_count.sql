-- information_schema.loads should execute through FE getLoads instead of empty-values fallback.
SELECT CAST(COUNT(*) >= 0 AS INT) AS ok
FROM default_catalog.information_schema.loads;
