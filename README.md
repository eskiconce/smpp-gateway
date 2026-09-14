# smpp-gateway (M1)

Motor SMPP en Go. Roles: `smppgw server` (API + pipeline) y `smppgw connector <id>`.

## Requisitos

- Go 1.22+, PostgreSQL 14+, Redis 7+.
- `golang-migrate` CLI para migraciones.

## Arranque rápido

1. `make migrate-up` (SMG_DB_URL apuntando a tu PG)
2. Insertar connector y regla de ruta (ver `db/migrations/seed_connector.sql`)
3. `make run-server` y `make run-connector`
4. `curl -X POST localhost:8080/api/v1/messages -H 'X-Tenant-ID: t1' -d '{"to":"569123","text":"hola"}'`

## Tests

- `make test` — unitarios herméticos (sin servicios).
- `make test-integration` — PG + Redis reales (SMG_TEST_INTEGRATION=1).
