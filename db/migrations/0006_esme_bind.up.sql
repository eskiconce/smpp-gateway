ALTER TABLE tenants
    ADD COLUMN smpp_system_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN smpp_password  TEXT NOT NULL DEFAULT '';
CREATE UNIQUE INDEX uq_tenants_smpp_system_id
    ON tenants(smpp_system_id) WHERE smpp_system_id <> '';

ALTER TABLE messages
    ADD COLUMN source_channel TEXT NOT NULL DEFAULT 'api';
