DROP INDEX IF EXISTS uq_tenants_smpp_system_id;
ALTER TABLE tenants DROP COLUMN IF EXISTS smpp_system_id;
ALTER TABLE tenants DROP COLUMN IF EXISTS smpp_password;
ALTER TABLE messages DROP COLUMN IF EXISTS source_channel;
