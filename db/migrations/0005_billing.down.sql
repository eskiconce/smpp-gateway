-- db/migrations/0005_billing.down.sql
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS rate_entries;
DROP TABLE IF EXISTS rate_tables;
ALTER TABLE messages DROP COLUMN IF EXISTS amount;
ALTER TABLE tenants DROP COLUMN IF EXISTS api_key;
ALTER TABLE tenants DROP COLUMN IF EXISTS mode;
ALTER TABLE tenants DROP COLUMN IF EXISTS created_at;
