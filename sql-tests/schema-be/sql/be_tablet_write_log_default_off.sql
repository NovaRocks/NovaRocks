-- Default config keeps tablet write log collection disabled.
SELECT COUNT(*) AS row_count
FROM default_catalog.information_schema.be_tablet_write_log;
