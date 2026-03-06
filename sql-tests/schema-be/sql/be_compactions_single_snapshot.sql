-- be_compactions should always return one BE-level snapshot row.
SELECT COUNT(*) AS row_count
FROM default_catalog.information_schema.be_compactions;
