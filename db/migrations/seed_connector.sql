INSERT INTO connectors (name, type, host, port, system_id, password, source_addr, enabled)
VALUES ('esp', 'smpp', '127.0.0.1', 2775, 'esp', 'secreto', 'shield', true)
ON CONFLICT DO NOTHING;

INSERT INTO routing_rules (priority, prefix, connector_id)
SELECT 1, '569', id FROM connectors WHERE name = 'esp'
ON CONFLICT DO NOTHING;
