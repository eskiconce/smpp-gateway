CREATE TABLE tenants (
    id           TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'active',
    routing_tag  TEXT,
    balance      NUMERIC(14, 4) NOT NULL DEFAULT 0
);

CREATE TABLE connectors (
    id                       SERIAL PRIMARY KEY,
    name                     TEXT NOT NULL,
    type                     TEXT NOT NULL CHECK (type IN ('smpp', 'http')),
    host                     TEXT NOT NULL,
    port                     INT  NOT NULL,
    system_id                TEXT NOT NULL DEFAULT '',
    password                 TEXT NOT NULL DEFAULT '',
    bind_mode                TEXT NOT NULL DEFAULT 'transceiver',
    source_addr              TEXT NOT NULL DEFAULT '',
    source_ton               SMALLINT NOT NULL DEFAULT 0,
    source_npi               SMALLINT NOT NULL DEFAULT 0,
    dest_ton                  SMALLINT NOT NULL DEFAULT 1,
    dest_npi                  SMALLINT NOT NULL DEFAULT 1,
    concurrency              INT  NOT NULL DEFAULT 1,
    max_message_per_second   REAL NOT NULL DEFAULT 0,
    enquire_link_interval    INT  NOT NULL DEFAULT 30,
    tls                      BOOLEAN NOT NULL DEFAULT FALSE,
    enabled                  BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE routing_rules (
    id           SERIAL PRIMARY KEY,
    priority     INT NOT NULL,
    tenant_id    TEXT,
    prefix       TEXT NOT NULL DEFAULT '',
    regex        TEXT NOT NULL DEFAULT '',
    routing_tag  TEXT NOT NULL DEFAULT '',
    connector_id INT NOT NULL REFERENCES connectors(id),
    UNIQUE (priority, tenant_id)
);

CREATE TABLE messages (
    id           TEXT PRIMARY KEY,
    tenant_id    TEXT,
    source_addr  TEXT NOT NULL DEFAULT '',
    msisdn       TEXT NOT NULL,
    text         TEXT NOT NULL,
    segments     INT  NOT NULL DEFAULT 1,
    connector_id INT NOT NULL DEFAULT 0,
    state        TEXT NOT NULL,
    try_count    INT  NOT NULL DEFAULT 0,
    smsc_msgid   TEXT NOT NULL DEFAULT '',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_messages_state ON messages(state);
CREATE INDEX idx_messages_connector ON messages(connector_id);