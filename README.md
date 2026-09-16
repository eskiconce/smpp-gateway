# smpp-gateway

Pasarela SMPP 3.4 en Go: motor de mensajería corta con cola persistente,
ruteo por conector, estados de entrega (DLR), billing prepaid y panel
admin. Diseñada como un único binario `smppgw` con dos roles: `server`
(API + pipeline + ESME entrante) y `connector` (sesión SMPP outbound
hacia el SMSC).

[![Go](https://img.shields.io/badge/go-1.22+-00ADD8?logo=go)](https://go.dev)
[![v1.0.0](https://img.shields.io/badge/v1.0.0-completo-22c55e)]()

## Estado

**M1–M6 completados.** El proyecto cubre el ciclo completo de envío y
recepción de SMS:

| Fase | Estado | Alcance |
|------|--------|---------|
| M1 | ✅ | Núcleo: API, pipeline, router M1, cola Redis, worker, sesión SMPP, e2e |
| M2 | ✅ | Routing avanzado: grupos con peso, fallback y backoff exponencial |
| M3 | ✅ | DLR completo: correlación, webhooks, reconciler `accepted→expired` |
| M4 | ✅ | Billing: tarifas por prefijo, reserva/débito, API key por tenant |
| M5 | ✅ | ESME bind entrante: auth, submit_sm→pipeline, DLR cross-process |
| M6 | ✅ | Admin UI (React+Vite), auth JWT+RBAC, CRUD completo, deploy |

## Arquitectura

![Arquitectura de la solución](docs/architecture.png)

> Versión interactiva (viewer con evidencia de fuentes): [`docs/architecture.html`](docs/architecture.html)

### Componentes

| Componente | Rol |
|---|---|
| `internal/api` | API HTTP: envío (`POST /messages`), admin (login, CRUD, métricas, SSE) |
| `internal/auth` | JWT HS256, password KDF, RBAC (superadmin, admin, operador) |
| `internal/billing` | Servicio de tarifas por prefijo, reserva, débito y crédito |
| `internal/dlr` | Correlación de DLR (memoria+Redis), notificador webhooks, reconciler |
| `internal/esme` | ESME Server: bind entrante SMPP, auth por tenant, submit_sm→pipeline |
| `internal/pipeline` | Validación, ruteo, billing (reserva) y encolado |
| `internal/queue` | Cola por conector: Redis Streams (prod) o Memory (dev/CI) |
| `internal/router` | Routing avanzado: reglas con filtros, grupos con peso, candidatos ordenados |
| `internal/session` | Sesión SMPP outbound con throttle, reconexión y DLR cross-process |
| `internal/smpp` | Codec de PDUs tipados (bind, submit_sm, deliver_sm, TLV) |
| `internal/smscsim` | SMSC simulado (bind, acuses, DLR, drop on submit) para e2e |
| `internal/store` | Persistencia: PostgreSQL (messages, tenants, connectors, groups, rules, rates, webhooks, users) |
| `internal/worker` | Consumo de cola, envío, correlación de DLR, fallback a siguiente candidato, backoff |
| `web/` | Frontend React+Vite+TypeScript: dashboard, búsqueda, CRUD admin |
| `cmd/smppgw` | Binario único: `--role server` (API+pipeline+ESME) o `--role connector` (worker+session) |

## Stack

- **Go** 1.22+ (codec SMPP manual, tipado de PDUs), HTTP con `net/http`.
- **PostgreSQL 14+**: fuente de verdad (mensajes, tenants, tarifas, reglas, usuarios).
- **Redis 7+**: cola (Streams), correlación DLR efímera.
- **React + Vite + TypeScript**: panel admin.
- `golang-migrate` para migraciones (`db/migrations/`).
- Deploy: nativo vía systemd + nginx en VM.

## Configuración

Variables de entorno (`internal/config`):

| Variable | Por defecto |
|---|---|
| `SMG_ROLE` | — (`server` \| `connector`, requerida) |
| `SMG_HTTP_ADDR` | `:8080` |
| `SMG_SMPP_ADDR` | `:2775` |
| `SMG_DB_URL` | `postgres://smpp:smpp@localhost:5432/smpp?sslmode=disable` |
| `SMG_REDIS_URL` | `redis://localhost:6379/0` |
| `SMG_CONNECTOR_ID` | requerida para rol `connector` |

## Arranque rápido

```bash
make migrate-up                            # requiere SMG_DB_URL
make run-server                            # role server (API + pipeline + ESME)
make run-connector                         # role connector

# Envío HTTP
curl -X POST localhost:8080/api/v1/messages \
  -H 'X-Tenant-ID: t1' \
  -d '{"to":"569123","text":"hola"}'

# Panel admin
cd web/ && npm install && npm run dev      # localhost:5173
```

## Tests

```bash
make test              # unitarios herméticos (sin servicios)
make test-integration  # SMG_TEST_INTEGRATION=1, requiere PG + Redis reales
```

El e2e (`e2e/`) cubre:
- `TestE2EHTTPSubmitToDelivered`: HTTP → pipeline → cola → worker → SMSC → DLR
- `TestE2ESMPPSubmitToDeliverSM`: ESME entrante → pipeline → cola → SMSC → deliver_sm
- `TestAdminLoginYCRUD`: login JWT → messages admin

## Documentación del proyecto

### Manual de usuario

**Manual completo en español**: [`docs/MANUAL_DE_USO.md`](docs/MANUAL_DE_USO.md)

Guía de instalación, configuración, uso del panel admin, referencia de la API HTTP, troubleshooting y apéndice de códigos SMPP.

### Flujos de mensajes

**Diagramas gráficos**: [`docs/FLUJOS_DE_MENSAJES.md`](docs/FLUJOS_DE_MENSAJES.md)

11 diagramas Mermaid: envío HTTP end-to-end, recepción DLR, reconciliación, conexión SMSC, ESME entrante, routing con grupos/fallback, billing, estados del mensaje, arquitectura de componentes y secuencia de deploy.

### Documento general de diseño

**Especificación completa**: [`docs/superpowers/specs/2026-09-12-smpp-gateway-design.md`](docs/superpowers/specs/2026-09-12-smpp-gateway-design.md)

Este es el documento de referencia para la siguiente fase. Contiene:
- Arquitectura general y decisiones de diseño
- Requisitos funcionales y no funcionales
- Diagramas de flujo (envío, DLR, billing)
- Schema de base de datos
- Contratos de interfaz entre componentes

### Planes por fase

| Plan | Archivo |
|------|---------|
| M1 Núcleo | [`plans/2026-09-12-m1-smpp-core.md`](docs/superpowers/plans/2026-09-12-m1-smpp-core.md) |
| M2 Routing | [`plans/2026-09-13-m2-routing-avanzado.md`](docs/superpowers/plans/2026-09-13-m2-routing-avanzado.md) |
| M3 DLR | [`plans/2026-09-13-m3-dlr-completo.md`](docs/superpowers/plans/2026-09-13-m3-dlr-completo.md) |
| M4 Billing | [`plans/2026-09-13-m4-billing.md`](docs/superpowers/plans/2026-09-13-m4-billing.md) |
| M5 ESME | [`plans/2026-09-13-m5-esme-bind-entrante.md`](docs/superpowers/plans/2026-09-13-m5-esme-bind-entrante.md) |
| M6 Admin UI | [`plans/2026-09-13-m6-admin-ui-deploy.md`](docs/superpowers/plans/2026-09-13-m6-admin-ui-deploy.md) |

### Deploy

M6 Task 10 dejó configuración lista en:
- `deploy/smppgw.service` — servicio systemd
- `deploy/nginx.conf` — proxy reverso nginx
- `Makefile` — targets `migrate-up`, `run-server`, `run-connector`

Para aplicar en VM de producción, seguir la sección "Despliegue" del plan M6.
