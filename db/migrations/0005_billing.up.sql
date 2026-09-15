-- db/migrations/0005_billing.up.sql
ALTER TABLE tenants ADD COLUMN mode       TEXT NOT NULL DEFAULT 'prepaid'
    CHECK (mode IN ('prepaid', 'postpaid'));
ALTER TABLE tenants ADD COLUMN api_key    TEXT NOT NULL DEFAULT '';
ALTER TABLE tenants ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT now();
CREATE UNIQUE INDEX uq_tenants_api_key ON tenants(api_key) WHERE api_key <> '';

ALTER TABLE messages ADD COLUMN amount NUMERIC(14,4) NOT NULL DEFAULT 0;

CREATE TABLE rate_tables (
    id          SERIAL PRIMARY KEY,
    tenant_id   TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    active      BOOLEAN NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_rate_tables_tenant ON rate_tables(tenant_id);

CREATE TABLE rate_entries (
    id           SERIAL PRIMARY KEY,
    table_id     INT NOT NULL REFERENCES rate_tables(id) ON DELETE CASCADE,
    prefix       TEXT NOT NULL,
    price        NUMERIC(14,4) NOT NULL,
    connector_id INT REFERENCES connectors(id) ON DELETE SET NULL,
    valid_from   TIMESTAMPTZ,
    valid_to     TIMESTAMPTZ
);
CREATE INDEX idx_rate_entries_table ON rate_entries(table_id);

CREATE TABLE transactions (
    id             BIGSERIAL PRIMARY KEY,
    tenant_id      TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    message_id     TEXT,
    type           TEXT NOT NULL CHECK (type IN ('debit', 'credit')),
    amount         NUMERIC(14,4) NOT NULL,
    result_balance NUMERIC(14,4) NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX uq_transactions_debit_msg
    ON transactions(message_id) WHERE type = 'debit' AND message_id IS NOT NULL;
CREATE INDEX idx_transactions_tenant ON transactions(tenant_id, created_at DESC);
