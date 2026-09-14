CREATE TABLE groups (
    id   SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE group_members (
    group_id      INT  NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    connector_id  INT  NOT NULL REFERENCES connectors(id) ON DELETE CASCADE,
    weight        INT  NOT NULL DEFAULT 1 CHECK (weight >= 0),
    PRIMARY KEY (group_id, connector_id)
);

ALTER TABLE routing_rules
    ADD COLUMN group_id INT REFERENCES groups(id) ON DELETE CASCADE,
    ADD COLUMN from     TEXT NOT NULL DEFAULT '';

ALTER TABLE messages
    ADD COLUMN route_id INT NOT NULL DEFAULT 0;

CREATE INDEX idx_routing_rules_group ON routing_rules(group_id)
    WHERE group_id IS NOT NULL;
