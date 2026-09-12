# SMPP Gateway — Design Doc

Fecha: 2026-09-12
Estado: aprobado por el usuario (secciones validadas en sesión de brainstorming)

## Resumen

Plataforma de SMPP Gateway (SMS) construida desde cero con ideas de Jasmin (routing
tags, grupos, filtros, DLR), sobre un núcleo moderno en Go. Administración completa
vía frontend React, multi-tenant, billing por segmento, DLR con reconciliación,
routing avanzado con fallback y dashboard de métricas en vivo.

## Objetivos

- Motor SMPP propio en Go, testeable y de alto rendimiento (>1000 msg/s) en 1 VM.
- Frontend funcional para crear configuraciones, conectores de proveedores y tablas de rutas.
- Multi-tenant: clientes aislados con sus credenciales, rutas, tarifas y saldos.
- DLR y estados con persistencia y reconciliación.
- API HTTP + reintentos automáticos para integraciones externas.
- Billing por segmento (prepaid/postpaid) con transacciones auditables.
- Despliegue sobre VM sin Docker (systemd + nginx).

## Decisiones clave

| Decisión | Elección | Razón |
|---|---|---|
| Enfoque | Híbrido: núcleo nuevo, ideas de Jasmin | Control de un motor moderno sin arrastrar deuda de Twisted/Python |
| Lenguaje núcleo | Go | Single binary ideal para VM/systemd, librería go-smpp madura, alta concurrencia |
| Frontend | React + Vite + TS | SPA moderna, ecosistema de UI maduro |
| Servir frontend | nginx + reverse proxy | TLS fácil, separación de responsabilidades |
| BD | PostgreSQL (fuente de verdad) | Persistencia real de DLR/ledger/billing, JSONB |
| Cola | Redis Streams | Alta velocidad, cola por conector+prioridad |
| Topología | 1 VM, N procesos de gateway (systemd template) | >1000 msg/s sin k8s |
| Auth | JWT + RBAC (superadmin/admin/viewer); API Key por tenant | Seguridad por capas |

## Arquitectura

Un binario `smppgw` que corre en roles según flags, bajo units systemd:

- `smppgw server` — API REST (`/api`) + acepta binds SMPP entrantes (ESME multi-tenant).
  Corre: validación → billing → router → publicación en cola.
- `smppgw connector <id>` — un proceso por conector/proveedor. Consume su stream,
  mantiene bind SMPP outbound (transceiver) o prepara POST HTTP al webhook,
  aplica throttling por conexión, reconexión con backoff y fallback reencolando.

```
 Clientes externos                    Proveedores
 ┌───────────┬──────────────┐        ┌──────────────┐
 │SMPP bind  │ HTTP REST    │        │ SMSC SMPP    │
 │(ESME multi│ clientes     │        │ (outbound)   │
 │-tenant)   │ /api         │        └──────┬───────┘
 └─────┬─────┴──────┬───────┘        ┌──────┴───────┐
       │            │                │ Webhook HTTP │
 ┌─────▼────────────▼────────┐       └──────────────┘
 │ smppgw server   (1 proc)  │
 │  valida→billing→router    │
 └───────────┬───────────────┘
             │ publica en Redis Stream
      ┌──────▼───────────────┐
      │ Redis Streams        │  (cola por conector + prioridad)
      └──────┬───────────────┘
   ┌─────────▼───────────────┐
   │ smppgw connector <id> × N │  ← un proc por proveedor/SMSC
   │ consume→envía SMPP/HTTP  │  ← throttling, requeue, fallback
   └─────────┬────────────────┘
             │ deliver_sm (DLR) / webhook response
             ▼
       DLR processor → Postgres → notifica al cliente (deliver_sm o webhook)
```

El router corre en el proceso de entrada: cada conector solo envía lo que llega a su
stream, sin conocer el ruteo global. Fronteras limpias y testeables.

## Modelo de datos

### Postgres (esquema `smpp`)

| Tabla | Para qué | Campos clave |
|---|---|---|
| `tenants` | Clientes multi-tenant | id, nombre, estado, routing_tag, saldo_total |
| `users` | Admin de la plataforma (RBAC) | id, tenant_id, rol (superadmin/admin/viewer), hash bcrypt |
| `connectors` | Proveedores SMPP y HTTP | id, tipo, host, puerto, system_id/password, bind_mode, source_addr, concurrency, throttling (msg/s), enquire_link_interval, TLS, headers/timeouts (http) |
| `groups` | Grupos de connectors | id, nombre |
| `group_members` | Connector + peso en el grupo | group_id, connector_id, weight |
| `routing_rules` | Tabla de rutas ordenada | id, priority, filters (JSON), group_id/connector_id destino |
| `messages` | Ledger de envíos | id (UUID), tenant_id, msisdn, text, source_addr, segments, priority, state, try_count, connector_id, route_id, msgid_cliente, smsc_msgid, timestamps, amount |
| `rate_tables` / `rate_entries` | Tarifas | tabla, prefix_msisdn, precio por segmento, connector_id/ruta, vigencia |
| `transactions` | Billing | tenant_id, message_id, monto, saldo resultante, tipo |
| `webhooks` | Callbacks DLR clientes | tenant_id, url, auth_token, eventos suscritos |

### Redis (efímero)

- `stream:con:<id>:<prio>` — cola por conector+prioridad
- `tps:<connector_id>` — ventana deslizante para throttling
- `ratele:ten:<id>` — límites de envío del cliente
- `dlr:pend:<smsc_msgid>` — correlación DLR en caliente

Regla: Redis solo efímero; todo lo que debe sobrevivir un crash vive en Postgres.
Un `submit_sm_resp` recibido se persiste de inmediato.

## Motor de rutas

- `routing_rules` evaluadas por `priority`; primera que matchea gana.
- Filtros: tenant, from, to/msisdn (prefix/regex), routing_tag (`[TAG]` en texto).
- Destino: `group` (balanceo por peso) o `connector_id` directo.
- Fallback: error temporal o timeout → reencolar al siguiente candidato, `try_count++`
  con backoff; al llegar a `max_tries` → `UNDELIV`.
- El mensaje en cola lleva conector + tarifa ya resueltos.

## DLR y estados

- `registered_delivery=1` hacia el proveedor; se persiste `smsc_msgid` del `submit_sm_resp`.
- `deliver_sm` (DLR) → correlación por Redis `dlr:pend:<smsc_msgid>` → actualizar
  `messages.state` (`DELIVRD/EXPIRED/UNDELIV/...`) → notificar al cliente:
  `deliver_sm` para ESME o POST al webhook para HTTP.
- Job de reconciliación: `accepted` sin DLR tras X min → `EXPIRED`/alerta.

Ciclo de vida: `buffered → accepted → (frozen/resent) → final (DELIVRD/UNDELIV/EXPIRED/REJECTED)`

## Billing

- Débito al aceptar (`submit_sm_resp` OK), no al encolar: si el proveedor rechaza no se cobra.
- Tarifa: `rate_entries` por prefix_msisdn (prefijo más largo) según conector usado;
  precio por segmento (multiparte = N×tarifa).
- Reserva de saldo antes de encolar; convertir en débito real al aceptar (transacción con saldo resultante).
- Saldo insuficiente → rechazo en entrada (p.ej. SMPP 0512 en bind entrante).

## API REST (`smppgw server`, 8080)

- Auth: JWT Bearer (RBAC), API Key por tenant para el endpoint de clientes.
- `/api/v1/admin/{tenants,users,connectors,groups,routing-rules,rate-tables,webhooks}` — CRUD con RBAC.
- `/api/v1/messages` — envío HTTP (to, text, routing_tag opcional).
- `/api/v1/messages/{id}` — estado y DLR.
- `/api/v1/metrics` + `/api/v1/metrics/stream` (SSE). Opcional `/metrics` Prometheus.

## Frontend React (SPA vía nginx)

Login → Dashboard (TPS, enviados/fallidos por conector, latencia, estados) →
Tenants y saldos → Connectors SMPP/HTTP (+ test) → Groups → Routing rules
(drag&drop por prioridad) → Rate tables → Búsqueda de mensajes con DLR →
Webhooks → Usuarios/RBAC.

## Despliegue VM (sin Docker)

- systemd: `smppgw-server.service`, `smppgw-connector@<id>.service` (plantilla),
  `postgresql`, `redis`.
- nginx: sirve build React, reverse-proxy `/api` y SSE a 127.0.0.1:8080
  (`proxy_buffering off`), TLS con certbot.
- Migraciones con `golang-migrate`; `go:embed` opcional como fallback sin nginx.

## Testing

- Codec SMPP propio probado contra especificación 3.4 (PDUs, TLVs, multiparte concat).
- Simulador SMSC como paquete de test (fake provider: responde `submit_sm_resp` y envía DLR).
- TDD por paquete: codec, router, billing, dlr, connectors; throttling y fallback con tests de concurrencia.

## Fuera de alcance (v1)

- Clustering entre múltiples VMs (la arquitectura lo permite, no se implementa en v1).
- SSO/LDAP (diseño RBAC preparado para agregarlo).
- Soporte SMPP v5.0 (solo 3.4).