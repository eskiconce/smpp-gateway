CREATE TABLE webhooks (
    id          SERIAL PRIMARY KEY,
    tenant_id   TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    url         TEXT NOT NULL,
    auth_token  TEXT NOT NULL DEFAULT '',
    events      TEXT[] NOT NULL DEFAULT '{}',
    active      BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE messages ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT now();
CREATE INDEX idx_messages_state_updated ON messages(state, updated_at);
