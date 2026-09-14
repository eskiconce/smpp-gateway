# M3 DLR Completo — Resultados

## Commits (feature/m3-dlr-completo)
| SHA | Descripción |
|------|-------------|
| fef5400 | Migración 0004: tabla webhooks + messages.updated_at |
| 9925f5f | Store: Message.UpdatedAt, ListStaleAccepted, Webhook, WebhookRepo |
| 8269a07 | dlr.Cache: correlación en memoria y Redis + MapStat |
| c97f334 | dlr.Processor: correlación smsc_msgid→message_id, estado final, notificación |
| d755992 | dlr.WebhookNotifier: POST JSON a webhooks del tenant |
| 1bbfe7c | dlr.Reconciler: ReconcileOnce + Run con ticker |
| 58d20a5 | smscsim DLRStatus configurable + worker con dlr.Processor |
| a2eadbb | API: GET /api/v1/messages/{id} + webhooks CRUD |
| 8a23d33 | Wiring: config durations, run.go, e2e con webhook |
| 5a88673 | Fix: PG queries (from, groups table names) + integration test data |

## Tests Unitarios
```
ok  e2e                         0.099s
ok  internal/api                0.006s
ok  internal/config             0.004s
ok  internal/dlr                0.337s
ok  internal/pipeline           0.007s
ok  internal/queue              0.005s
ok  internal/router             0.004s
ok  internal/session            0.206s
ok  internal/smpp               0.004s
ok  internal/smscsim            0.315s
ok  internal/store              0.006s
ok  internal/worker             0.418s
ok  test                        0.006s
```

## Tests de Integración (PG + Redis reales)
```
TestIntegrationSubmitToDelivered    PASS (0.17s)
TestIntegrationDLRPipeline          PASS (0.10s)
```

## Archivos Nuevos/Modificados
- `db/migrations/0004_webhooks_dlr.up.sql` / `.down.sql`
- `internal/store/msg.go` — Message.UpdatedAt, Webhook, WebhookRepo
- `internal/store/memory.go` — BackdoorSetUpdatedAt, webhooks methods
- `internal/store/pg.go` — updated_at, webhooks, stale accepted, fix column names
- `internal/dlr/stat.go` — MapStat
- `internal/dlr/cache.go` — Cache interface, MemCache, RedisCache
- `internal/dlr/processor.go` — Processor
- `internal/dlr/webhook.go` — WebhookNotifier
- `internal/dlr/reconcile.go` — Reconciler
- `internal/smscsim/smscsim.go` — Config.DLRStatus
- `internal/worker/worker.go` — WithDLR option
- `internal/api/server.go` — GET /api/v1/messages/{id}, webhooks CRUD
- `cmd/smppgw/run.go` — DLR wiring (redis cache, processor, webhook notifier, reconciler)
