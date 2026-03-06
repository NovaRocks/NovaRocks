-- Validate be_txns scan path and impossible-key filter behavior.
SELECT COUNT(*) AS row_count
FROM default_catalog.information_schema.be_txns
WHERE txn_id = -1
  AND tablet_id = -1;
