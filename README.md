# smpp-gateway

Pasarela SMPP 3.4 en Go: motor de mensajería corta con cola persistente,
ruteo por conector y estados de entrega (DLR). Diseñada como un único
binario `smppgw` con dos roles: `server` (API + pipeline) y `connector`
(sesión SMPP outbound hacia el SMSC).

[![Go](https://img.shields.io/badge/go-1.22+-00ADD8?logo=go)](https://go.dev)
[![Status: M1](https://img.shields.io/badge/fase-M1%20implementado-22c55e)](docs/superpowers/plans/)

## Estado

- **M1 · Núcleo — implementado y verificado e2e.** El flujo completo
  `HTTP submit → pipeline → cola → worker → sesión SMPP → SMSC (DLR)` queda
  cubierto por un test end-to-end contra `smscsim`.
- Rol `connector` del binario: el wiring worker + sesión está listo en las
  librerías y verificado en e2e; su integración final en `smppgw` se completa
  con los siguientes hitos (M2–M6).

## Arquitectura

![Arquitectura de la solución](docs/architecture.png)

> Versión interactiva (viewer con evidencia de fuentes): [`docs/architecture.html`](docs/architecture.html)

### Componentes

| Componente | Rol |
|---|---|
| `internal/api` | API HTTP `/api/v1/messages` (POST de envío), servida por el role `server` |
| `internal/pipeline` | Validación, ruteo (Router) y encolado del mensaje |
| `internal/router` | Selección de conector por prioridad y prefijo de destino |
| `internal/queue` | Cola por conector: `Redis Streams` (prod) o `Memory` (dev/CI) |
| `internal/worker` | Consumo, correlación de DLR y actualización de estados |
| `internal/session` | Sesión SMPP outbound con throttle y reconexión |
| `internal/store` | Persistencia de mensajes/estados (PostgreSQL o memoria) |
| `internal/smpp` | Codec de PDUs tipados (bind, submit_sm, deliver_sm, TLV) |
| `internal/smscsim` | SMSC simulado (bind, acuses, DLR) para e2e |

## Stack

- **Go** (codec SMPP manual, tipado de PDUs), HTTP con `net/http`.
- **PostgreSQL** como fuente de verdad; **Redis 7** para la cola (efímero).
- `golang-migrate` para migraciones (`db/migrations`).
- Deploy objetivo: nativo vía systemd en VM.

## Roadmap

| Fase | Alcance |
|---|---|
| **M1** ✅ | Núcleo: API, pipeline, router, cola, worker, sesión y e2e |
| **M2** | Routing avanzado: grupos y peso, fallback y backoff |
| **M3** | DLR completo: estados, webhook y Reconciler |
| **M4** | Billing: tarifas, reserva y débito, API Key |
| **M5** | Bind ESME entrante (submit/deliver_sm enrutados) |
| **M6** | Panel admin React + API JWT/RBAC + nginx |

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
make run-server                            # role server (API + pipeline)
make run-connector                         # role connector

curl -X POST localhost:8080/api/v1/messages \
  -H 'X-Tenant-ID: t1' \
  -d '{"to":"569123","text":"hola"}'
```

El seed de un conector y una regla de ruta está en
`db/migrations/seed_connector.sql`.

## Tests

```bash
make test              # unitarios herméticos (sin servicios)
make test-integration  # SMG_TEST_INTEGRATION=1, requiere PG + Redis reales
```

El e2e (`e2e/`) levanta `smscsim` y recorre `HTTP → delivered` con DLR.

## Documentación

- Especificación y planes por fase: [`docs/superpowers/plans/`](docs/superpowers/plans/)
- Diagrama de arquitectura interactivo: [`docs/architecture.html`](docs/architecture.html)