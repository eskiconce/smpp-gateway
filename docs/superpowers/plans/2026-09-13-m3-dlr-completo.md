# DLR Completo Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Correlación de DLR por Redis (`dlr:pend:<smsc_msgid>`), normalización de estados finales (`DELIVRD/EXPIRED/UNDELIV/REJECTED`), notificación al cliente por webhook y job de reconciliación de mensajes `accepted` huérfanos.

**Architecture:** El worker del conector deja de guardar DLR en un map local (M1/M2) y delega en un `dlr.Processor` que correlaciona por Redis, persiste el estado final en Postgres y notifica. La notificación a clientes es un POST JSON a los webhooks del tenant (tabla `webhooks`); la entrega por `deliver_sm` a ESMEs entrantes se implementa en M5 (bind entrante). Un `Reconciler` periódico marca como `expired` los `accepted` sin DLR tras un timeout, apoyado en `messages.updated_at`.

**Tech Stack:** Go 1.22, `github.com/redis/go-redis/v9` (ya usado en `queue`), pgxpool (ya usado en `store`), `github.com/alicebob/miniredis/v2` (test, ya usado en M1), `net/http` estándar.

**Spec:** `docs/superpowers/specs/2026-09-12-smpp-gateway-design.md` — sección "DLR y estados" + tabla `webhooks` de "Modelo de datos" + endpoint `/api/v1/messages/{id}` de "API REST".

## Global Constraints

- Un solo binario `smppgw`; roles `server` (API + pipeline + reconciler) y `connector <id>` (consume su stream, mantiene bind outbound).
- Go 1.22+, PostgreSQL 14+, Redis 7+. Sin Docker.
- Solo SMPP 3.4; `registered_delivery=1` siempre; se persiste `smsc_msgid` en cuanto llega el `submit_sm_resp`.
- Postgres es fuente de verdad; Redis solo efímero (cola/caché). Todo lo que deba sobrevivir va a PG.
- Ciclo de vida: `buffered → accepted → (frozen/resent) → final (DELIVRD/UNDELIV/EXPIRED/REJECTED)`.
- TDD por paquete: test que falla → implementación mínima → test que pasa → commit.
- Sin comentarios en código salvo documentar invariantes. Commits en inglés (`feat:`/`docs:`). Nombres y copy en español (estados: `delivered/undeliv/expired/rejected`).
- Sin dependencias nuevas fuera de las ya usadas por M1/M2 (go-redis v9, miniredis v2, pgxpool, x/time/rate).

## Plan base previo (estado tras M1 y M2)

M1 entregó: `store.Message{ID, TenantID, SourceAddr, Msisdn, Text string; Segments, ConnectorID, TryCount int; State, SmscMsgid string; CreatedAt time.Time}` con `MessageRepo{CreateMessage, UpdateState, SetSmscMsgid, GetMessage}`, `store.NewPG(ctx, dsn) (*PGRepo, error)` (pgxpool) y `store.NewMemory()`. `session.Handler{OnSubmitResp(seq uint32, status smpp.CommandStatus, msgid string); OnDLR(msgid, stat string)}`; `session.Session.{Dial, Submit(dest, text string, regDelivery uint8) (uint32, error), Close}`. `worker.NewWorker(q, repo, opts...)` con `WithBackoff`, `SetSession(Sender)` y estado `failed/pending` → **M2 lo dejó con** `pending map[uint32]queue.Item`, `dlr map[string]string` (comentario "la persistencia real llega en M3"), `IncrementTry`/`SetConnector`, `fallback` a candidatos con backoff, y estados `undeliv` (transporte) / `rejected` (rechazo). `smscsim.Config{Addr, SystemID, Password, EnableDLR, DropOnSubmit, RespondSubmitStatus}` y `queue.Item{ID, TenantID, ConnectorID, Priority, DataCoding, Msisdn, SourceAddr, Text, Candidates []int, Try int}` con `queue.Key(conn, prio)`. `api.New(cfg, p, repo)`, `api.Server.Handler()`, mux con `POST /api/v1/messages` y `GET /healthz`. `config.Config{Role, ConnectorID, HTTPAddr, SMPPAddr, DBURL, RedisURL}`. El DLR actual lo procesa el worker local (solo `DELIVRD`→`delivered`, resto→`undeliv`) y el sim siempre envía `stat:DELIVRD`. M3 reemplaza toda esa capa.

---

### Task 1: Migración 0004 — tabla `webhooks` y `messages.updated_at`

**Files:**
- Create: `db/migrations/0004_webhooks_dlr.up.sql`
- Create: `db/migrations/0004_webhooks_dlr.down.sql`

**Interfaces:**
- Consumes: `db/migrations/` (0001, 0003 de M1/M2).
- Produces: tabla `webhooks(id, tenant_id, url, auth_token, events TEXT[], active, created_at)` y columna `messages.updated_at` con índice para el barrido de reconciliación. Las tasks 2-9 las consumen.

- [ ] **Step 1: Migración up**

```sql
-- db/migrations/0004_webhooks_dlr.up.sql
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
```

- [ ] **Step 2: Migración down**

```sql
-- db/migrations/0004_webhooks_dlr.down.sql
ALTER TABLE messages DROP COLUMN IF EXISTS updated_at;
DROP TABLE IF EXISTS webhooks;
```

- [ ] **Step 3: Commit**

```bash
git add db/migrations/0004_webhooks_dlr.up.sql db/migrations/0004_webhooks_dlr.down.sql
git commit -m "feat: migracion webhooks y updated_at para dlr"
```

> `messages.updated_at` lo tocan `UpdateState` y `SetSmscMsgid` (Task 2); es la base del job de reconciliación (Task 6).

---

### Task 2: Store — `Message.UpdatedAt`, `ListStaleAccepted`, `Webhook` y `WebhookRepo`

**Files:**
- Modify: `internal/store/msg.go` (struct + interface + tipo `Webhook` + interface `WebhookRepo`)
- Modify: `internal/store/pg.go` (SQL con `updated_at`, métodos de webhooks)
- Modify: `internal/store/memory.go` (misma interfaz + repos de webhooks en memoria)
- Modify: `internal/store/msg_test.go` (assert de `UpdatedAt` y `ListStaleAccepted`)
- Create: `internal/store/webhook_test.go`
- Test: `go test ./internal/store/ -v`

**Interfaces:**
- Consumes: `store.MessageRepo` (M1+M2), `messages.updated_at` (Task 1).
- Produces:
  - `store.Message` gana `UpdatedAt time.Time`.
  - `store.MessageRepo` gana `ListStaleAccepted(ctx, before time.Time) ([]Message, error)`.
  - `store.Webhook{ID int; TenantID, URL, AuthToken string; Events []string; Active bool; CreatedAt time.Time}`.
  - `store.WebhookRepo{ListWebhooks(ctx, tenantID) ([]Webhook, error); ListActiveByEvent(ctx, tenantID, event string) ([]Webhook, error); CreateWebhook(ctx, *Webhook) error; UpdateWebhook(ctx, Webhook) error; DeleteWebhook(ctx, id int) error}`.
  - `*PGRepo` y `*MemoryRepo` implementan ambas interfaces (una sola instancia sirve para las dos).

- [ ] **Step 1: Test del repo (memory) — lista de mensajes stale, webhooks CRUD**

```go
// internal/store/msg_test.go — añadir
func TestMessageUpdatedAtAndStale(t *testing.T) {
    ctx := context.Background()
    repo := NewMemory()

    old := &Message{ID: "m1", TenantID: "t1", Msisdn: "5691", Text: "a", State: "accepted"}
    if err := repo.CreateMessage(ctx, old); err != nil {
        t.Fatal(err)
    }
    fresh := &Message{ID: "m2", TenantID: "t1", Msisdn: "5692", Text: "b", State: "accepted"}
    if err := repo.CreateMessage(ctx, fresh); err != nil {
        t.Fatal(err)
    }

    // envejecer m1
    old.UpdatedAt = time.Now().Add(-time.Hour)

    stale, err := repo.ListStaleAccepted(ctx, time.Now().Add(-10*time.Minute))
    if err != nil {
        t.Fatal(err)
    }
    if len(stale) != 1 || stale[0].ID != "m1" {
        t.Fatalf("stale=%+v", stale)
    }

    if err := repo.UpdateState(ctx, "m1", "delivered"); err != nil {
        t.Fatal(err)
    }
    got, _ := repo.GetMessage(ctx, "m1")
    if got.UpdatedAt.Before(time.Now().Add(-time.Minute)) {
        t.Fatalf("updated_at no avanzo: %v", got.UpdatedAt)
    }
}
```

```go
// internal/store/webhook_test.go
package store

import (
    "context"
    "testing"
)

func TestWebhookRepoMemory(t *testing.T) {
    ctx := context.Background()
    repo := NewMemory()

    w := &Webhook{TenantID: "t1", URL: "https://cli.example.com/hook", AuthToken: "abc",
        Events: []string{"delivered", "undeliv"}, Active: true}
    if err := repo.CreateWebhook(ctx, w); err != nil {
        t.Fatal(err)
    }
    if w.ID == 0 {
        t.Fatal("sin id asignado")
    }

    got, err := repo.ListActiveByEvent(ctx, "t1", "delivered")
    if err != nil || len(got) != 1 {
        t.Fatalf("active=%+v err=%v", got, err)
    }
    // no matchea por evento ni por tenant
    if n, _ := repo.ListActiveByEvent(ctx, "t1", "expired"); len(n) != 0 {
        t.Fatalf("no debia matchear expired: %+v", n)
    }
    if n, _ := repo.ListActiveByEvent(ctx, "t2", "delivered"); len(n) != 0 {
        t.Fatalf("tenant distinto no debia matchear: %+v", n)
    }

    w.Active = false
    if err := repo.UpdateWebhook(ctx, *w); err != nil {
        t.Fatal(err)
    }
    if n, _ := repo.ListActiveByEvent(ctx, "t1", "delivered"); len(n) != 0 {
        t.Fatalf("inactivo no debia aparecer: %+v", n)
    }
    all, err := repo.ListWebhooks(ctx, "t1")
    if err != nil || len(all) != 1 || all[0].Active {
        t.Fatalf("all=%+v err=%v", all, err)
    }
    if err := repo.DeleteWebhook(ctx, w.ID); err != nil {
        t.Fatal(err)
    }
    if all, _ := repo.ListWebhooks(ctx, "t1"); len(all) != 0 {
        t.Fatalf("no borro: %+v", all)
    }
}
```

- [ ] **Step 2: Correr los tests y verificar que fallan**

Run: `go test ./internal/store/ -run 'TestMessageUpdatedAtAndStale|TestWebhookRepoMemory' -v`
Expected: FAIL — `UpdatedAt`, `Webhook`, `WebhookRepo`, `MemoryRepo` no compilan los métodos.

- [ ] **Step 3: Implementar msg.go (struct, interfaces, Webhook)**

```go
// internal/store/msg.go — Message (nuevo)
type Message struct {
    ID        string
    TenantID  string
    SourceAddr string
    Msisdn    string
    Text      string
    Segments  int
    ConnectorID int
    RouteID   int
    TryCount  int
    State     string
    SmscMsgid string
    CreatedAt time.Time
    UpdatedAt time.Time
}
```

```go
// internal/store/msg.go — MessageRepo (añadir método)
type MessageRepo interface {
    CreateMessage(ctx context.Context, m *Message) error
    UpdateState(ctx context.Context, id, state string) error
    SetSmscMsgid(ctx context.Context, id, smscMsgid string) error
    GetMessage(ctx context.Context, id string) (*Message, error)
    IncrementTry(ctx context.Context, id string) error
    SetConnector(ctx context.Context, id string, connectorID int) error
    ListStaleAccepted(ctx context.Context, before time.Time) ([]Message, error)
}
```

```go
// internal/store/msg.go — Webhook (nuevo)
type Webhook struct {
    ID        int
    TenantID  string
    URL       string
    AuthToken string
    Events    []string
    Active    bool
    CreatedAt time.Time
}

type WebhookRepo interface {
    ListWebhooks(ctx context.Context, tenantID string) ([]Webhook, error)
    ListActiveByEvent(ctx context.Context, tenantID, event string) ([]Webhook, error)
    CreateWebhook(ctx context.Context, w *Webhook) error
    UpdateWebhook(ctx context.Context, w Webhook) error
    DeleteWebhook(ctx context.Context, id int) error
}
```

- [ ] **Step 4: Implementar pg.go — `updated_at` y webhooks en Postgres**

```go
// internal/store/pg.go — actualizar métodos existentes
func (r *PGRepo) UpdateState(ctx context.Context, id, state string) error {
    _, err := r.pool.Exec(ctx, `UPDATE messages SET state=$2, updated_at=now() WHERE id=$1`, id, state)
    return err
}

func (r *PGRepo) SetSmscMsgid(ctx context.Context, id, smscMsgid string) error {
    _, err := r.pool.Exec(ctx, `UPDATE messages SET smsc_msgid=$2, state='accepted', updated_at=now() WHERE id=$1`, id, smscMsgid)
    return err
}
```

```go
// internal/store/pg.go — añadir
func (r *PGRepo) ListStaleAccepted(ctx context.Context, before time.Time) ([]Message, error) {
    rows, err := r.pool.Query(ctx, `
        SELECT id, tenant_id, source_addr, msisdn, text, segments, connector_id, route_id,
               state, try_count, smsc_msgid, created_at, updated_at
        FROM messages WHERE state='accepted' AND updated_at < $1
        ORDER BY updated_at LIMIT 500`, before)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    var out []Message
    for rows.Next() {
        var m Message
        if err := rows.Scan(&m.ID, &m.TenantID, &m.SourceAddr, &m.Msisdn, &m.Text,
            &m.Segments, &m.ConnectorID, &m.RouteID, &m.State, &m.TryCount,
            &m.SmscMsgid, &m.CreatedAt, &m.UpdatedAt); err != nil {
            return nil, err
        }
        out = append(out, m)
    }
    return out, rows.Err()
}
```

```go
// internal/store/pg.go — webhooks (nuevo)
const webhookCols = `id, tenant_id, url, auth_token, events, active, created_at`

func scanWebhooks(rows pgx.Rows) ([]Webhook, error) {
    defer rows.Close()
    var out []Webhook
    for rows.Next() {
        var w Webhook
        if err := rows.Scan(&w.ID, &w.TenantID, &w.URL, &w.AuthToken, &w.Events, &w.Active, &w.CreatedAt); err != nil {
            return nil, err
        }
        out = append(out, w)
    }
    return out, rows.Err()
}

func (r *PGRepo) ListWebhooks(ctx context.Context, tenantID string) ([]Webhook, error) {
    rows, err := r.pool.Query(ctx, `SELECT `+webhookCols+` FROM webhooks WHERE tenant_id=$1 ORDER BY id`, tenantID)
    if err != nil {
        return nil, err
    }
    return scanWebhooks(rows)
}

func (r *PGRepo) ListActiveByEvent(ctx context.Context, tenantID, event string) ([]Webhook, error) {
    rows, err := r.pool.Query(ctx, `SELECT `+webhookCols+` FROM webhooks
        WHERE tenant_id=$1 AND active AND $2 = ANY(events) ORDER BY id`, tenantID, event)
    if err != nil {
        return nil, err
    }
    return scanWebhooks(rows)
}

func (r *PGRepo) CreateWebhook(ctx context.Context, w *Webhook) error {
    return r.pool.QueryRow(ctx, `INSERT INTO webhooks (tenant_id, url, auth_token, events, active)
        VALUES ($1,$2,$3,$4,$5) RETURNING id, created_at`,
        w.TenantID, w.URL, w.AuthToken, w.Events, w.Active).Scan(&w.ID, &w.CreatedAt)
}

func (r *PGRepo) UpdateWebhook(ctx context.Context, w Webhook) error {
    _, err := r.pool.Exec(ctx, `UPDATE webhooks SET url=$2, auth_token=$3, events=$4, active=$5 WHERE id=$1`,
        w.ID, w.URL, w.AuthToken, w.Events, w.Active)
    return err
}

func (r *PGRepo) DeleteWebhook(ctx context.Context, id int) error {
    _, err := r.pool.Exec(ctx, `DELETE FROM webhooks WHERE id=$1`, id)
    return err
}
```

> pgx v5 encoda/decoda `[]string` ↔ `TEXT[]` nativamente (añadir el import de `pgx` si `pg.go` no lo tiene ya).

- [ ] **Step 5: Implementar memory.go — `UpdatedAt` y webhooks en memoria**

```go
// internal/store/memory.go — cambios en memoria de mensajes
func (r *MemoryRepo) CreateMessage(_ context.Context, m *Message) error {
    m.CreatedAt = time.Now()
    m.UpdatedAt = m.CreatedAt
    r.msgs[m.ID] = m
    return nil
}

func (r *MemoryRepo) UpdateState(_ context.Context, id, state string) error {
    m := r.msgs[id]
    if m == nil {
        return nil
    }
    m.State = state
    m.UpdatedAt = time.Now()
    return nil
}

func (r *MemoryRepo) SetSmscMsgid(_ context.Context, id, smscMsgid string) error {
    m := r.msgs[id]
    if m == nil {
        return nil
    }
    m.SmscMsgid = smscMsgid
    m.State = "accepted"
    m.UpdatedAt = time.Now()
    return nil
}

func (r *MemoryRepo) ListStaleAccepted(_ context.Context, before time.Time) ([]Message, error) {
    var out []Message
    for _, m := range r.msgs {
        if m.State == "accepted" && m.UpdatedAt.Before(before) {
            out = append(out, *m)
        }
    }
    return out, nil
}
```

```go
// internal/store/memory.go — webhooks en memoria (añadir al struct)
type MemoryRepo struct {
    msgs     map[string]*Message
    hooks    []Webhook
    nextHook int
}

func (r *MemoryRepo) ListWebhooks(_ context.Context, tenantID string) ([]Webhook, error) {
    var out []Webhook
    for _, w := range r.hooks {
        if w.TenantID == tenantID {
            out = append(out, w)
        }
    }
    return out, nil
}

func (r *MemoryRepo) ListActiveByEvent(_ context.Context, tenantID, event string) ([]Webhook, error) {
    var out []Webhook
    for _, w := range r.hooks {
        if w.TenantID == tenantID && w.Active {
            for _, e := range w.Events {
                if e == event {
                    out = append(out, w)
                    break
                }
            }
        }
    }
    return out, nil
}

func (r *MemoryRepo) CreateWebhook(_ context.Context, w *Webhook) error {
    r.nextHook++
    w.ID = r.nextHook
    w.CreatedAt = time.Now()
    r.hooks = append(r.hooks, *w)
    return nil
}

func (r *MemoryRepo) UpdateWebhook(_ context.Context, w Webhook) error {
    for i := range r.hooks {
        if r.hooks[i].ID == w.ID {
            r.hooks[i] = w
            return nil
        }
    }
    return nil
}

func (r *MemoryRepo) DeleteWebhook(_ context.Context, id int) error {
    for i := range r.hooks {
        if r.hooks[i].ID == id {
            r.hooks = append(r.hooks[:i], r.hooks[i+1:]...)
            return nil
        }
    }
    return nil
}
```

- [ ] **Step 6: Correr los tests y verificar que pasan**

Run: `go test ./internal/store/ -v`
Expected: PASS (incluye los tests M1/M2 existentes).

- [ ] **Step 7: Commit**

```bash
git add internal/store db/migrations/0004_webhooks_dlr.up.sql db/migrations/0004_webhooks_dlr.down.sql
git commit -m "feat: store con webhooks, updated_at y stale accepted"
```

---

### Task 3: `dlr.Cache` — correlación en memoria y Redis (`dlr:pend:`), `MapStat`

**Files:**
- Create: `internal/dlr/cache.go`
- Create: `internal/dlr/stat.go`
- Create: `internal/dlr/cache_test.go`
- Test: `go test ./internal/dlr/ -v`

**Interfaces:**
- Consumes: nada nuevo (go-redis v9, miniredis v2 de M1).
- Produces:
  - `dlr.Cache { Register(ctx, key, value string) error; Lookup(ctx, key string) (string, error); Delete(ctx, key string) error }` — `Lookup` de clave inexistente devuelve `("", nil)`.
  - `dlr.NewMemCache() *MemCache`
  - `dlr.NewRedisCache(url string, ttl time.Duration) (*RedisCache, error)` + `(*RedisCache).Close()`. Claves `dlr:pend:<smsc_msgid>` con TTL.
  - `dlr.MapStat(stat string) string`: `DELIVRD→delivered`, `EXPIRED→expired`, `UNDELIV→undeliv`, `REJECTED→rejected`; cualquier otro stat → `""` (no final, el mensaje sigue `accepted`).

- [ ] **Step 1: Tests del cache y del mapping**

```go
// internal/dlr/cache_test.go
package dlr

import (
    "context"
    "testing"

    "github.com/alicebob/miniredis/v2"
)

func TestMemCacheRoundTrip(t *testing.T) {
    c := NewMemCache()
    ctx := context.Background()
    if err := c.Register(ctx, "smsc-1", "m1"); err != nil {
        t.Fatal(err)
    }
    v, err := c.Lookup(ctx, "smsc-1")
    if err != nil || v != "m1" {
        t.Fatalf("lookup=%q err=%v", v, err)
    }
    if err := c.Delete(ctx, "smsc-1"); err != nil {
        t.Fatal(err)
    }
    if v, _ := c.Lookup(ctx, "smsc-1"); v != "" {
        t.Fatalf("no borro: %q", v)
    }
}

func TestRedisCacheRoundTrip(t *testing.T) {
    mr := miniredis.RunT(t)
    c, err := NewRedisCache("redis://"+mr.Addr(), 0)
    if err != nil {
        t.Fatal(err)
    }
    defer c.Close()
    ctx := context.Background()
    if err := c.Register(ctx, "x", "v"); err != nil {
        t.Fatal(err)
    }
    if v, _ := c.Lookup(ctx, "x"); v != "v" {
        t.Fatalf("lookup=%q", v)
    }
    if got := mr.Keys(); len(got) != 1 || got[0] != "dlr:pend:x" {
        t.Fatalf("keys=%v", got)
    }
    if v, _ := c.Lookup(ctx, "no-existe"); v != "" {
        t.Fatalf("esperaba vacio, got %q", v)
    }
}
```

```go
// internal/dlr/stat_test.go
package dlr

import "testing"

func TestMapStat(t *testing.T) {
    cases := map[string]string{
        "DELIVRD": "delivered",
        "EXPIRED": "expired",
        "UNDELIV": "undeliv",
        "REJECTED": "rejected",
        "ENROUTE": "",
        "":        "",
    }
    for in, want := range cases {
        if got := MapStat(in); got != want {
            t.Fatalf("MapStat(%q)=%q want %q", in, got, want)
        }
    }
}
```

- [ ] **Step 2: Correr los tests y verificar que fallan**

Run: `go test ./internal/dlr/ -v`
Expected: FAIL — paquete `dlr` no existe.

- [ ] **Step 3: Implementar stat.go y cache.go**

```go
// internal/dlr/stat.go
package dlr

var statMap = map[string]string{
    "DELIVRD":  "delivered",
    "EXPIRED":  "expired",
    "UNDELIV":  "undeliv",
    "REJECTED": "rejected",
}

// MapStat traduce el "stat" del DLR 3.4 al estado interno. Devuelve "" si el
// stat no es final (el mensaje se queda accepted esperando un DLR posterior).
func MapStat(stat string) string { return statMap[stat] }
```

```go
// internal/dlr/cache.go
package dlr

import (
    "context"
    "sync"
    "time"

    "github.com/redis/go-redis/v9"
)

type Cache interface {
    Register(ctx context.Context, key, value string) error
    Lookup(ctx context.Context, key string) (string, error)
    Delete(ctx context.Context, key string) error
}

type MemCache struct {
    mu    sync.Mutex
    items map[string]string
}

func NewMemCache() *MemCache { return &MemCache{items: map[string]string{}} }

func (c *MemCache) Register(_ context.Context, key, value string) error {
    c.mu.Lock()
    defer c.mu.Unlock()
    c.items[key] = value
    return nil
}

func (c *MemCache) Lookup(_ context.Context, key string) (string, error) {
    c.mu.Lock()
    defer c.mu.Unlock()
    return c.items[key], nil
}

func (c *MemCache) Delete(_ context.Context, key string) error {
    c.mu.Lock()
    defer c.mu.Unlock()
    delete(c.items, key)
    return nil
}

const keyPrefix = "dlr:pend:"

type RedisCache struct {
    rdb *redis.Client
    ttl time.Duration
}

func NewRedisCache(url string, ttl time.Duration) (*RedisCache, error) {
    opts, err := redis.ParseURL(url)
    if err != nil {
        return nil, err
    }
    rdb := redis.NewClient(opts)
    if err := rdb.Ping(context.Background()).Err(); err != nil {
        return nil, err
    }
    return &RedisCache{rdb: rdb, ttl: ttl}, nil
}

func (c *RedisCache) Close() error { return c.rdb.Close() }

func (c *RedisCache) Register(ctx context.Context, key, value string) error {
    return c.rdb.Set(ctx, keyPrefix+key, value, c.ttl).Err()
}

func (c *RedisCache) Lookup(ctx context.Context, key string) (string, error) {
    v, err := c.rdb.Get(ctx, keyPrefix+key).Result()
    if err == redis.Nil {
        return "", nil
    }
    return v, err
}

func (c *RedisCache) Delete(ctx context.Context, key string) error {
    return c.rdb.Del(ctx, keyPrefix+key).Err()
}
```

- [ ] **Step 4: Correr los tests y verificar que pasan**

Run: `go test ./internal/dlr/ -v`
Expected: PASS (miniredis soporta GET/SET/DEL).

- [ ] **Step 5: Commit**

```bash
git add internal/dlr
git commit -m "feat: cache de correlacion dlr (memoria y redis) con mapeo de stats"
```

---

### Task 4: `dlr.Processor` — correlación, estado final y notificación

**Files:**
- Create: `internal/dlr/processor.go`
- Create: `internal/dlr/processor_test.go`
- Test: `go test ./internal/dlr/ -v`

**Interfaces:**
- Consumes: `dlr.Cache` (Task 3), `store.MessageRepo`, `store.NewMemory()`.
- Produces:
  - `dlr.Event{TenantID, MessageID, SmscMsgid, Msisdn, State string; Timestamp time.Time}`
  - `dlr.Notifier { Notify(ctx, ev Event) error }`
  - `dlr.NewProcessor(cache Cache, repo store.MessageRepo, notifier Notifier) *Processor` (`notifier` puede ser `nil`).
  - `(*Processor).Register(ctx, smscMsgid, messageID string) error`
  - `(*Processor).Handle(ctx, smscMsgid, stat string) error` — correlaciona; si `MapStat` es final: `UpdateState`, borra la clave del cache y notifica (goroutine); si no es final o no hay correlación: no hace nada.

- [ ] **Step 1: Test del procesador con cache/repo/notifier falsos**

```go
// internal/dlr/processor_test.go
package dlr

import (
    "context"
    "sync"
    "testing"
    "time"

    "github.com/eskiconce/smpp-gateway/internal/store"
)

type recNotifier struct {
    mu  sync.Mutex
    evs []Event
}

func (r *recNotifier) Notify(_ context.Context, ev Event) error {
    r.mu.Lock()
    defer r.mu.Unlock()
    r.evs = append(r.evs, ev)
    return nil
}

func (r *recNotifier) count() int {
    r.mu.Lock()
    defer r.mu.Unlock()
    return len(r.evs)
}

func (r *recNotifier) last() Event {
    r.mu.Lock()
    defer r.mu.Unlock()
    return r.evs[len(r.evs)-1]
}

func newTestProcessor(t *testing.T) (*Processor, *store.MemoryRepo, *recNotifier) {
    t.Helper()
    repo := store.NewMemory()
    n := &recNotifier{}
    if err := repo.CreateMessage(context.Background(), &store.Message{
        ID: "m1", TenantID: "t1", Msisdn: "569123", Text: "hola", State: "accepted",
    }); err != nil {
        t.Fatal(err)
    }
    return NewProcessor(NewMemCache(), repo, n), repo, n
}

func TestHandleDelivered(t *testing.T) {
    p, repo, n := newTestProcessor(t)
    ctx := context.Background()
    if err := p.Register(ctx, "smsc-1", "m1"); err != nil {
        t.Fatal(err)
    }
    if err := p.Handle(ctx, "smsc-1", "DELIVRD"); err != nil {
        t.Fatal(err)
    }
    m, _ := repo.GetMessage(ctx, "m1")
    if m.State != "delivered" {
        t.Fatalf("state=%q", m.State)
    }
    deadline := time.Now().Add(time.Second)
    for n.count() == 0 && time.Now().Before(deadline) {
        time.Sleep(time.Millisecond)
    }
    if ev := n.last(); ev.State != "delivered" || ev.MessageID != "m1" || ev.SmscMsgid != "smsc-1" {
        t.Fatalf("event=%+v", ev)
    }
}

func TestHandleUndelivAndExpired(t *testing.T) {
    p, repo, n := newTestProcessor(t)
    ctx := context.Background()
    if err := p.Register(ctx, "s1", "m1"); err != nil {
        t.Fatal(err)
    }
    if err := p.Handle(ctx, "s1", "UNDELIV"); err != nil {
        t.Fatal(err)
    }
    m, _ := repo.GetMessage(ctx, "m1")
    if m.State != "undeliv" {
        t.Fatalf("state=%q", m.State)
    }
    deadline := time.Now().Add(time.Second)
    for n.count() == 0 && time.Now().Before(deadline) {
        time.Sleep(time.Millisecond)
    }
    if n.last().State != "undeliv" {
        t.Fatalf("event=%+v", n.last())
    }
}

func TestHandleNonFinalKeepsAccepted(t *testing.T) {
    p, repo, n := newTestProcessor(t)
    ctx := context.Background()
    if err := p.Register(ctx, "s1", "m1"); err != nil {
        t.Fatal(err)
    }
    if err := p.Handle(ctx, "s1", "ENROUTE"); err != nil {
        t.Fatal(err)
    }
    m, _ := repo.GetMessage(ctx, "m1")
    if m.State != "accepted" {
        t.Fatalf("state=%q", m.State)
    }
    if n.count() != 0 {
        t.Fatalf("no debia notificar")
    }
}

func TestHandleWithoutCorrelationIsNoop(t *testing.T) {
    p, repo, n := newTestProcessor(t)
    ctx := context.Background()
    if err := p.Handle(ctx, "smsc-desconocido", "DELIVRD"); err != nil {
        t.Fatal(err)
    }
    m, _ := repo.GetMessage(ctx, "m1")
    if m.State != "accepted" || n.count() != 0 {
        t.Fatalf("state=%q n=%d", m.State, n.count())
    }
}
```

- [ ] **Step 2: Correr los tests y verificar que fallan**

Run: `go test ./internal/dlr/ -run TestHandle -v`
Expected: FAIL — `NewProcessor`, `Event`, `Notifier` no existen.

- [ ] **Step 3: Implementar processor.go**

```go
// internal/dlr/processor.go
package dlr

import (
    "context"
    "time"

    "github.com/eskiconce/smpp-gateway/internal/store"
)

type Event struct {
    TenantID, MessageID, SmscMsgid, Msisdn, State string
    Timestamp                                     time.Time
}

type Notifier interface {
    Notify(ctx context.Context, ev Event) error
}

type Processor struct {
    cache    Cache
    repo     store.MessageRepo
    notifier Notifier
}

func NewProcessor(cache Cache, repo store.MessageRepo, notifier Notifier) *Processor {
    return &Processor{cache: cache, repo: repo, notifier: notifier}
}

// Register guarda la correlacion smsc_msgid -> messageID (llamado al recibir submit_sm_resp OK).
func (p *Processor) Register(ctx context.Context, smscMsgid, messageID string) error {
    return p.cache.Register(ctx, smscMsgid, messageID)
}

// Handle procesa un DLR: correlaciona, aplica estado final, limpia el cache y notifica.
func (p *Processor) Handle(ctx context.Context, smscMsgid, stat string) error {
    id, err := p.cache.Lookup(ctx, smscMsgid)
    if err != nil || id == "" {
        return err
    }
    state := MapStat(stat)
    if state == "" {
        return nil // stat no final: el mensaje queda accepted esperando DLR posterior
    }
    m, err := p.repo.GetMessage(ctx, id)
    if err != nil {
        return err
    }
    if err := p.repo.UpdateState(ctx, id, state); err != nil {
        return err
    }
    _ = p.cache.Delete(ctx, smscMsgid)
    if p.notifier != nil {
        go p.notifier.Notify(context.Background(), Event{
            TenantID: m.TenantID, MessageID: m.ID, SmscMsgid: smscMsgid,
            Msisdn: m.Msisdn, State: state, Timestamp: time.Now(),
        })
    }
    return nil
}
```

> La notificación por `deliver_sm` hacia un ESME entrante (cliente SMPP) se implementa en M5 (bind entrante); M3 notifica solo por webhook (clientes HTTP).

- [ ] **Step 4: Correr los tests y verificar que pasan**

Run: `go test ./internal/dlr/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/dlr
git commit -m "feat: procesador de dlr con correlacion, estado final y notificacion"
```

---

### Task 5: `dlr.WebhookNotifier` — POST JSON a los webhooks del tenant

**Files:**
- Create: `internal/dlr/webhook.go`
- Create: `internal/dlr/webhook_test.go`
- Test: `go test ./internal/dlr/ -v`

**Interfaces:**
- Consumes: `dlr.Notifier`, `dlr.Event` (Task 4), `store.WebhookRepo` (Task 2).
- Produces:
  - `dlr.NewWebhookNotifier(repo store.WebhookRepo, timeout time.Duration) *WebhookNotifier` (implementa `dlr.Notifier`; timeout mínimo 1s; default 5s).
  - Notifica a todos los webhooks activos del tenant suscritos al evento; body JSON `{event, message_id, smsc_msgid, msisdn, state, timestamp}`; header `Authorization: Bearer <token>` si hay token. Falla silenciosa (log `slog.Warn`) por webhook; `Notify` nunca falla por un webhook caído.

- [ ] **Step 1: Test con receptor httptest**

```go
// internal/dlr/webhook_test.go
package dlr

import (
    "context"
    "encoding/json"
    "io"
    "net/http"
    "net/http/httptest"
    "sync"
    "testing"
    "time"

    "github.com/eskiconce/smpp-gateway/internal/store"
)

func TestWebhookNotifierPosts(t *testing.T) {
    var mu sync.Mutex
    var got map[string]any
    var gotAuth string

    srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        b, _ := io.ReadAll(r.Body)
        mu.Lock()
        defer mu.Unlock()
        json.Unmarshal(b, &got)
        gotAuth = r.Header.Get("Authorization")
        w.WriteHeader(http.StatusOK)
    }))
    defer srv.Close()

    repo := store.NewMemory()
    if err := repo.CreateWebhook(context.Background(), &store.Webhook{
        TenantID: "t1", URL: srv.URL, AuthToken: "tok1",
        Events: []string{"delivered", "undeliv"}, Active: true,
    }); err != nil {
        t.Fatal(err)
    }

    n := NewWebhookNotifier(repo, 2*time.Second)
    err := n.Notify(context.Background(), Event{
        TenantID: "t1", MessageID: "m1", SmscMsgid: "smsc-1",
        Msisdn: "569123", State: "delivered", Timestamp: time.Now(),
    })
    if err != nil {
        t.Fatal(err)
    }

    deadline := time.Now().Add(time.Second)
    for time.Now().Before(deadline) {
        mu.Lock()
        ok := got != nil
        mu.Unlock()
        if ok {
            break
        }
        time.Sleep(time.Millisecond)
    }
    mu.Lock()
    defer mu.Unlock()
    if got == nil {
        t.Fatal("no llego el POST")
    }
    if got["event"] != "delivered" || got["message_id"] != "m1" || got["state"] != "delivered" {
        t.Fatalf("payload=%+v", got)
    }
    if gotAuth != "Bearer tok1" {
        t.Fatalf("auth=%q", gotAuth)
    }
}

func TestWebhookNotifierIgnoresDownWebhook(t *testing.T) {
    srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w.WriteHeader(http.StatusInternalServerError)
    }))
    url := srv.URL
    srv.Close() // webhook caido

    repo := store.NewMemory()
    if err := repo.CreateWebhook(context.Background(), &store.Webhook{
        TenantID: "t1", URL: url, Events: []string{"delivered"}, Active: true,
    }); err != nil {
        t.Fatal(err)
    }
    n := NewWebhookNotifier(repo, time.Second)
    if err := n.Notify(context.Background(), Event{
        TenantID: "t1", MessageID: "m1", State: "delivered",
    }); err != nil {
        t.Fatalf("no debia fallar por webhook caido: %v", err)
    }
}
```

- [ ] **Step 2: Correr los tests y verificar que fallan**

Run: `go test ./internal/dlr/ -run TestWebhook -v`
Expected: FAIL — `NewWebhookNotifier` no existe.

- [ ] **Step 3: Implementar webhook.go**

```go
// internal/dlr/webhook.go
package dlr

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "log/slog"
    "net/http"
    "time"

    "github.com/eskiconce/smpp-gateway/internal/store"
)

type WebhookNotifier struct {
    repo   store.WebhookRepo
    client *http.Client
}

func NewWebhookNotifier(repo store.WebhookRepo, timeout time.Duration) *WebhookNotifier {
    if timeout <= 0 {
        timeout = 5 * time.Second
    }
    return &WebhookNotifier{repo: repo, client: &http.Client{Timeout: timeout}}
}

// Notify publica el evento en todos los webhooks activos del tenant suscritos al estado.
// Un webhook que falla se loguea y no propaga error.
func (n *WebhookNotifier) Notify(ctx context.Context, ev Event) error {
    ws, err := n.repo.ListActiveByEvent(ctx, ev.TenantID, ev.State)
    if err != nil {
        return err
    }
    body, err := json.Marshal(map[string]any{
        "event":      ev.State,
        "message_id": ev.MessageID,
        "smsc_msgid": ev.SmscMsgid,
        "msisdn":     ev.Msisdn,
        "state":      ev.State,
        "timestamp":  ev.Timestamp.Format(time.RFC3339),
    })
    if err != nil {
        return err
    }
    for _, w := range ws {
        if err := n.post(ctx, w, body); err != nil {
            slog.Warn("webhook fallo", "url", w.URL, "err", err)
        }
    }
    return nil
}

func (n *WebhookNotifier) post(ctx context.Context, w store.Webhook, body []byte) error {
    req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.URL, bytes.NewReader(body))
    if err != nil {
        return err
    }
    req.Header.Set("Content-Type", "application/json")
    if w.AuthToken != "" {
        req.Header.Set("Authorization", "Bearer "+w.AuthToken)
    }
    resp, err := n.client.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()
    if resp.StatusCode >= 300 {
        return fmt.Errorf("status %d", resp.StatusCode)
    }
    return nil
}
```

- [ ] **Step 4: Correr los tests y verificar que pasan**

Run: `go test ./internal/dlr/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/dlr
git commit -m "feat: notificador de dlr por webhook"
```

---

### Task 6: `dlr.Reconciler` — `accepted` sin DLR → `expired`

**Files:**
- Create: `internal/dlr/reconcile.go`
- Create: `internal/dlr/reconcile_test.go`
- Test: `go test ./internal/dlr/ -v`

**Interfaces:**
- Consumes: `store.ListStaleAccepted` (Task 2), `dlr.Notifier` (Task 4).
- Produces:
  - `dlr.NewReconciler(repo store.MessageRepo, notifier Notifier, timeout, interval time.Duration) *Reconciler` (defaults 10m / 1m si ≤0; `notifier` puede ser nil).
  - `(*Reconciler).ReconcileOnce(ctx) (int, error)` — marca e notifica.
  - `(*Reconciler).Run(ctx) error` — ticker con `interval` hasta que `ctx` se cancele; loguea errores y cantidades.

- [ ] **Step 1: Test de reconciliación (memory)**

```go
// internal/dlr/reconcile_test.go
package dlr

import (
    "context"
    "sync"
    "testing"
    "time"

    "github.com/eskiconce/smpp-gateway/internal/store"
)

type countNotifier struct {
    mu  sync.Mutex
    cnt int
    evs []Event
}

func (c *countNotifier) Notify(_ context.Context, ev Event) error {
    c.mu.Lock()
    defer c.mu.Unlock()
    c.cnt++
    c.evs = append(c.evs, ev)
    return nil
}

func (c *countNotifier) total() int {
    c.mu.Lock()
    defer c.mu.Unlock()
    return c.cnt
}

func TestReconcileOnceMarksExpired(t *testing.T) {
    ctx := context.Background()
    repo := store.NewMemory()

    old := &store.Message{ID: "m1", TenantID: "t1", Msisdn: "5691", Text: "a", SmscMsgid: "smsc-9", State: "accepted"}
    if err := repo.CreateMessage(ctx, old); err != nil {
        t.Fatal(err)
    }
    old.UpdatedAt = time.Now().Add(-time.Hour)

    fresh := &store.Message{ID: "m2", TenantID: "t1", Msisdn: "5692", Text: "b", State: "accepted"}
    if err := repo.CreateMessage(ctx, fresh); err != nil {
        t.Fatal(err)
    }

    n := &countNotifier{}
    r := NewReconciler(repo, n, 10*time.Minute, time.Minute)
    count, err := r.ReconcileOnce(ctx)
    if err != nil {
        t.Fatal(err)
    }
    if count != 1 {
        t.Fatalf("count=%d", count)
    }
    m, _ := repo.GetMessage(ctx, "m1")
    if m.State != "expired" {
        t.Fatalf("state=%q", m.State)
    }
    if m2, _ := repo.GetMessage(ctx, "m2"); m2.State != "accepted" {
        t.Fatalf("m2 no debia tocarse: %q", m2.State)
    }
    deadline := time.Now().Add(time.Second)
    for n.total() == 0 && time.Now().Before(deadline) {
        time.Sleep(time.Millisecond)
    }
    if n.total() != 1 || n.evs[0].State != "expired" || n.evs[0].MessageID != "m1" {
        t.Fatalf("notificado=%d evs=%+v", n.total(), n.evs)
    }
}
```

- [ ] **Step 2: Correr el test y verificar que falla**

Run: `go test ./internal/dlr/ -run TestReconcile -v`
Expected: FAIL — `NewReconciler` no existe.

- [ ] **Step 3: Implementar reconcile.go**

```go
// internal/dlr/reconcile.go
package dlr

import (
    "context"
    "log/slog"
    "time"

    "github.com/eskiconce/smpp-gateway/internal/store"
)

type Reconciler struct {
    repo     store.MessageRepo
    notifier Notifier
    timeout  time.Duration
    interval time.Duration
}

func NewReconciler(repo store.MessageRepo, notifier Notifier, timeout, interval time.Duration) *Reconciler {
    if timeout <= 0 {
        timeout = 10 * time.Minute
    }
    if interval <= 0 {
        interval = time.Minute
    }
    return &Reconciler{repo: repo, notifier: notifier, timeout: timeout, interval: interval}
}

// ReconcileOnce marca como expired los accepted sin DLR mas viejos que timeout y los notifica.
func (r *Reconciler) ReconcileOnce(ctx context.Context) (int, error) {
    stale, err := r.repo.ListStaleAccepted(ctx, time.Now().Add(-r.timeout))
    if err != nil {
        return 0, err
    }
    for _, m := range stale {
        if err := r.repo.UpdateState(ctx, m.ID, "expired"); err != nil {
            return 0, err
        }
        if r.notifier != nil {
            go r.notifier.Notify(context.Background(), Event{
                TenantID: m.TenantID, MessageID: m.ID, SmscMsgid: m.SmscMsgid,
                Msisdn: m.Msisdn, State: "expired", Timestamp: time.Now(),
            })
        }
    }
    return len(stale), nil
}

// Run ejecuta ReconcileOnce cada interval hasta que ctx se cancele.
func (r *Reconciler) Run(ctx context.Context) error {
    ticker := time.NewTicker(r.interval)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            return nil
        case <-ticker.C:
            n, err := r.ReconcileOnce(ctx)
            if err != nil {
                slog.Error("reconciliacion dlr", "err", err)
                continue
            }
            if n > 0 {
                slog.Info("expired por falta de dlr", "n", n)
            }
        }
    }
}
```

- [ ] **Step 4: Correr los tests y verificar que pasan**

Run: `go test ./internal/dlr/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/dlr
git commit -m "feat: reconciliador de dlr (accepted sin dlr a expired)"
```

---

### Task 7: `smscsim` con `DLRStatus` + worker con `dlr.Processor`

**Files:**
- Modify: `internal/smscsim/smscsim.go` (config + texto DLR) y `internal/smscsim/smscsim_test.go`
- Modify: `internal/worker/worker.go` (reemplaza el map local por `dlr.Processor`)
- Modify: `internal/worker/worker_test.go` (tests con procesador y stats variados)
- Test: `go test ./internal/worker/ ./internal/smscsim/ -v`

**Interfaces:**
- Consumes: `dlr.Processor` (Task 4), `queue.Item`/`queue.Key` (M2), `session.Handler` (M1).
- Produces:
  - `smscsim.Config` gana `DLRStatus string` (default `"DELIVRD"` cuando `EnableDLR` y vacío).
  - `worker.NewWorker(q, repo, opts...)` con nueva opción `WithDLR(p *dlr.Processor) Option`.
  - `Worker.OnSubmitResp` ROK: `SetSmscMsgid` + `dlrProc.Register`; no ROK: `SetSmscMsgid` + `fallback` (sin cambio).
  - `Worker.OnDLR(msgid, stat)` → `dlrProc.Handle(ctx, msgid, stat)` (sin `dlr.Processor` configurado no hace nada). Se elimina el campo `dlr map[string]string`.
  - Estados: `delivered` (DELIVRD), `undeliv` (UNDELIV), `expired` (EXPIRED), `rejected` (REJECTED/DLR o rechazo exhausto).

- [ ] **Step 1: Test del sim con DLRStatus**

```go
// internal/smscsim/smscsim_test.go — añadir
func TestSimDLRStatus(t *testing.T) {
    srv := New(Config{Addr: "127.0.0.1:0", SystemID: "esp", Password: "secreto",
        EnableDLR: true, DLRStatus: "UNDELIV"})
    if err := srv.Start(); err != nil {
        t.Fatal(err)
    }
    defer srv.Close()

    conn, err := net.Dial("tcp", srv.Addr())
    if err != nil {
        t.Fatal(err)
    }
    defer conn.Close()

    conn.Write(smpp.Encode(smpp.NewBindTransceiver(1, "esp", "secreto", "", 0, 0, "")))
    p, err := readPDU(bufio.NewReader(conn))
    if err != nil || p.Header.ID != smpp.BindTransceiverResp {
        t.Fatalf("bind=%+v err=%v", p.Header, err)
    }

    conn.Write(smpp.Encode(mustSubmit(t, "test", "569123", "hola")))
    got, err := readPDU(bufio.NewReader(conn))
    if err != nil {
        t.Fatal(err)
    }
    if got.Header.ID == smpp.SubmitSMResp {
        got, err = readPDU(bufio.NewReader(conn))
        if err != nil {
            t.Fatal(err)
        }
    }
    f, err := smpp.ParseDeliverSM(got.Body)
    if err != nil {
        t.Fatal(err)
    }
    stat, _ := smpp.ParseDLR(f.ShortMessage)
    if stat != "UNDELIV" {
        t.Fatalf("stat=%q", stat)
    }
}
```

- [ ] **Step 2: Correr el test y verificar que falla**

Run: `go test ./internal/smscsim/ -run TestSimDLRStatus -v`
Expected: FAIL — `DLRStatus` no existe en `Config`.

- [ ] **Step 3: Implementar smscsim.go (cambio) y worker.go (nuevo)**

```go
// internal/smscsim/smscsim.go — Config
type Config struct {
    Addr                string
    SystemID            string
    Password            string
    EnableDLR           bool
    DropOnSubmit        bool
    RespondSubmitStatus smpp.CommandStatus
    DLRStatus           string
}
```

```go
// internal/smscsim/smscsim.go — dentro de case SubmitSM, al responder OK con DLR
    if status == smpp.ESME_ROK && s.cfg.EnableDLR {
        st := s.cfg.DLRStatus
        if st == "" {
            st = "DELIVRD"
        }
        dlr := "id:" + msgid + " sub:001 dlvrd:001 submit date:2609121230 done date:2609121231 stat:" + st + " err:000 text:"
        conn.Write(smpp.Encode(smpp.NewDeliverSM(s.nextSeq(), s.cfg.SystemID, "", dlr)))
    }
```

```go
// internal/worker/worker.go (M3 — reemplaza la version M2)
package worker

import (
    "context"
    "time"

    "github.com/eskiconce/smpp-gateway/internal/dlr"
    "github.com/eskiconce/smpp-gateway/internal/queue"
    "github.com/eskiconce/smpp-gateway/internal/smpp"
    "github.com/eskiconce/smpp-gateway/internal/store"
)

type Sender interface {
    Submit(dest, text string, regDelivery uint8) (uint32, error)
}

type Worker struct {
    q       queue.Queue
    repo    store.MessageRepo
    sess    Sender
    pending map[uint32]queue.Item
    dlrProc *dlr.Processor
    backoff func(try int) time.Duration
}

type Option func(*Worker)

func WithBackoff(f func(try int) time.Duration) Option {
    return func(w *Worker) { w.backoff = f }
}

// WithDLR activa la correlacion y notificacion de DLR via el procesador.
func WithDLR(p *dlr.Processor) Option {
    return func(w *Worker) { w.dlrProc = p }
}

func NewWorker(q queue.Queue, repo store.MessageRepo, opts ...Option) *Worker {
    w := &Worker{
        q: q, repo: repo,
        pending: map[uint32]queue.Item{},
        backoff: defaultBackoff,
    }
    for _, o := range opts {
        o(w)
    }
    return w
}

func defaultBackoff(try int) time.Duration {
    d := 500 * time.Millisecond << uint(try)
    if d > 30*time.Second {
        return 30 * time.Second
    }
    return d
}

func (w *Worker) SetSession(s Sender) { w.sess = s }

func (w *Worker) Handle(ctx context.Context, it queue.Item) error {
    w.repo.IncrementTry(ctx, it.ID)
    w.repo.SetConnector(ctx, it.ID, it.ConnectorID)
    seq, err := w.sess.Submit(it.Msisdn, it.Text, 1)
    if err != nil {
        w.fallback(it, true)
        return nil
    }
    w.pending[seq] = it
    return nil
}

// OnSubmitResp implementa session.Handler.
func (w *Worker) OnSubmitResp(seq uint32, status smpp.CommandStatus, msgid string) {
    it, ok := w.pending[seq]
    if !ok {
        return
    }
    delete(w.pending, seq)
    ctx := context.Background()
    w.repo.SetSmscMsgid(ctx, it.ID, msgid)
    if status != smpp.ESME_ROK {
        w.fallback(it, false)
        return
    }
    if w.dlrProc != nil {
        w.dlrProc.Register(ctx, msgid, it.ID)
    }
}

// OnDLR implementa session.Handler.
func (w *Worker) OnDLR(msgid, stat string) {
    if w.dlrProc == nil {
        return
    }
    w.dlrProc.Handle(context.Background(), msgid, stat)
}

// fallback intenta el siguiente candidato; si no hay mas, estado final.
func (w *Worker) fallback(it queue.Item, transportErr bool) {
    next := it.Try + 1
    if next >= len(it.Candidates) {
        state := "undeliv"
        if !transportErr {
            state = "rejected"
        }
        w.repo.UpdateState(context.Background(), it.ID, state)
        return
    }
    nxt := it
    nxt.ConnectorID = it.Candidates[next]
    nxt.Try = next
    delay := w.backoff(it.Try)
    go func() {
        if delay > 0 {
            time.Sleep(delay)
        }
        w.q.Enqueue(context.Background(), queue.Key(nxt.ConnectorID, nxt.Priority), nxt)
    }()
}
```

- [ ] **Step 4: Rehacer worker_test.go con procesador y stats variados**

```go
// internal/worker/worker_test.go (M3 — reemplaza la version M2)
package worker

import (
    "context"
    "net"
    "strconv"
    "sync"
    "testing"
    "time"

    "github.com/eskiconce/smpp-gateway/internal/dlr"
    "github.com/eskiconce/smpp-gateway/internal/pipeline"
    "github.com/eskiconce/smpp-gateway/internal/queue"
    "github.com/eskiconce/smpp-gateway/internal/router"
    "github.com/eskiconce/smpp-gateway/internal/session"
    "github.com/eskiconce/smpp-gateway/internal/smscsim"
    "github.com/eskiconce/smpp-gateway/internal/smpp"
    "github.com/eskiconce/smpp-gateway/internal/store"
)

func noBackoff(int) time.Duration { return 0 }

type recNotifier struct {
    mu  sync.Mutex
    evs []dlr.Event
}

func (r *recNotifier) Notify(_ context.Context, ev dlr.Event) error {
    r.mu.Lock()
    defer r.mu.Unlock()
    r.evs = append(r.evs, ev)
    return nil
}

func (r *recNotifier) count() int {
    r.mu.Lock()
    defer r.mu.Unlock()
    return len(r.evs)
}

func portOf2(addr string) int {
    _, portStr, err := net.SplitHostPort(addr)
    if err != nil {
        return 0
    }
    n, _ := strconv.Atoi(portStr)
    return n
}

func dialSess(t *testing.T, h session.Handler, simAddr string) *session.Session {
    t.Helper()
    s := session.New(session.Config{
        Host: "127.0.0.1", Port: portOf2(simAddr), SystemID: "esp", Password: "secreto",
        SourceAddr: "shield", MsgPerSecond: 100, MaxConcurrency: 1,
    }, h)
    if err := s.Dial(context.Background()); err != nil {
        t.Fatal(err)
    }
    t.Cleanup(func() { s.Close() })
    return s
}

func newProcWorker(t *testing.T, q queue.Queue, repo store.MessageRepo) (*Worker, *recNotifier, *dlr.Processor) {
    t.Helper()
    n := &recNotifier{}
    proc := dlr.NewProcessor(dlr.NewMemCache(), repo, n)
    return NewWorker(q, repo, WithBackoff(noBackoff), WithDLR(proc)), n, proc
}

func waitState(t *testing.T, repo store.MessageRepo, id, want string) *store.Message {
    t.Helper()
    deadline := time.Now().Add(3 * time.Second)
    for time.Now().Before(deadline) {
        m, err := repo.GetMessage(context.Background(), id)
        if err == nil && m != nil && m.State == want {
            return m
        }
        time.Sleep(10 * time.Millisecond)
    }
    m, _ := repo.GetMessage(context.Background(), id)
    t.Fatalf("no llego a %s (state=%s)", want, m.State)
    return nil
}

func TestWorkerDeliveredHappyPath(t *testing.T) {
    sim := smscsim.New(smscsim.Config{Addr: "127.0.0.1:0", SystemID: "esp", Password: "secreto", EnableDLR: true})
    if err := sim.Start(); err != nil {
        t.Fatal(err)
    }
    defer sim.Close()

    repo := store.NewMemory()
    q := queue.NewMemory()
    w, n, _ := newProcWorker(t, q, repo)
    sess := dialSess(t, w, sim.Addr())
    w.SetSession(sess)

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    go q.Consume(ctx, queue.Key(1, 0), "g", w.Handle)

    p := pipeline.NewPipeline(repo, q, &fixedRouter{ids: []int{1}})
    msgID, _, err := p.Submit(ctx, pipeline.Outgoing{
        TenantID: "t1", SourceAddr: "shield", Msisdn: "569123", Text: "hola", Priority: 0,
    })
    if err != nil {
        t.Fatal(err)
    }

    m := waitState(t, repo, msgID, "delivered")
    if m.SmscMsgid == "" {
        t.Fatal("sin smsc_msgid persistido")
    }
    deadline := time.Now().Add(time.Second)
    for n.count() == 0 && time.Now().Before(deadline) {
        time.Sleep(time.Millisecond)
    }
    if n.count() != 1 || n.evs[0].State != "delivered" {
        t.Fatalf("notificador: %d %+v", n.count(), n.evs)
    }
}

func TestWorkerDLRUndeliv(t *testing.T) {
    sim := smscsim.New(smscsim.Config{Addr: "127.0.0.1:0", SystemID: "esp", Password: "secreto",
        EnableDLR: true, DLRStatus: "UNDELIV"})
    if err := sim.Start(); err != nil {
        t.Fatal(err)
    }
    defer sim.Close()

    repo := store.NewMemory()
    q := queue.NewMemory()
    w, n, _ := newProcWorker(t, q, repo)
    w.SetSession(dialSess(t, w, sim.Addr()))

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    go q.Consume(ctx, queue.Key(1, 0), "g", w.Handle)

    p := pipeline.NewPipeline(repo, q, &fixedRouter{ids: []int{1}})
    msgID, _, err := p.Submit(ctx, pipeline.Outgoing{
        TenantID: "t1", SourceAddr: "shield", Msisdn: "569123", Text: "hola", Priority: 0,
    })
    if err != nil {
        t.Fatal(err)
    }

    waitState(t, repo, msgID, "undeliv")
    deadline := time.Now().Add(time.Second)
    for n.count() == 0 && time.Now().Before(deadline) {
        time.Sleep(time.Millisecond)
    }
    if n.count() != 1 || n.evs[0].State != "undeliv" {
        t.Fatalf("notificador: %d %+v", n.count(), n.evs)
    }
}

func TestWorkerDLRExpired(t *testing.T) {
    sim := smscsim.New(smscsim.Config{Addr: "127.0.0.1:0", SystemID: "esp", Password: "secreto",
        EnableDLR: true, DLRStatus: "EXPIRED"})
    if err := sim.Start(); err != nil {
        t.Fatal(err)
    }
    defer sim.Close()

    repo := store.NewMemory()
    q := queue.NewMemory()
    w, _, _ := newProcWorker(t, q, repo)
    w.SetSession(dialSess(t, w, sim.Addr()))

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    go q.Consume(ctx, queue.Key(1, 0), "g", w.Handle)

    p := pipeline.NewPipeline(repo, q, &fixedRouter{ids: []int{1}})
    msgID, _, err := p.Submit(ctx, pipeline.Outgoing{
        TenantID: "t1", SourceAddr: "shield", Msisdn: "569123", Text: "hola", Priority: 0,
    })
    if err != nil {
        t.Fatal(err)
    }
    waitState(t, repo, msgID, "expired")
}

func TestWorkerFallbackOnReject(t *testing.T) {
    simA := smscsim.New(smscsim.Config{Addr: "127.0.0.1:0", SystemID: "esp", Password: "secreto",
        RespondSubmitStatus: smpp.ESME_RSYSERR})
    if err := simA.Start(); err != nil {
        t.Fatal(err)
    }
    defer simA.Close()
    simB := smscsim.New(smscsim.Config{Addr: "127.0.0.1:0", SystemID: "esp", Password: "secreto", EnableDLR: true})
    if err := simB.Start(); err != nil {
        t.Fatal(err)
    }
    defer simB.Close()

    repo := store.NewMemory()
    q := queue.NewMemory()

    wA, _, _ := newProcWorker(t, q, repo)
    wA.SetSession(dialSess(t, wA, simA.Addr()))
    wB, _, _ := newProcWorker(t, q, repo)
    wB.SetSession(dialSess(t, wB, simB.Addr()))

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    go q.Consume(ctx, queue.Key(1, 0), "g", wA.Handle)
    go q.Consume(ctx, queue.Key(2, 0), "g", wB.Handle)

    r := router.New(&stubRules{groups: []router.Group{{
        ID: 3, Name: "ops",
        Members: []router.GroupMember{{ConnectorID: 1, Weight: 1}, {ConnectorID: 2, Weight: 1}},
    }}}, router.Config{})
    if err := r.Load(ctx); err != nil {
        t.Fatal(err)
    }

    p := pipeline.NewPipeline(repo, q, r)
    msgID, _, err := p.Submit(ctx, pipeline.Outgoing{
        TenantID: "t1", SourceAddr: "shield", Msisdn: "569123", Text: "hola", Priority: 0,
    })
    if err != nil {
        t.Fatal(err)
    }

    m := waitState(t, repo, msgID, "delivered")
    if m.ConnectorID != 2 || m.TryCount != 2 {
        t.Fatalf("conector=%d try=%d", m.ConnectorID, m.TryCount)
    }
}

func TestWorkerExhaustRejected(t *testing.T) {
    simA := smscsim.New(smscsim.Config{Addr: "127.0.0.1:0", SystemID: "esp", Password: "secreto",
        RespondSubmitStatus: smpp.ESME_RSYSERR})
    if err := simA.Start(); err != nil {
        t.Fatal(err)
    }
    defer simA.Close()

    repo := store.NewMemory()
    q := queue.NewMemory()
    wA, _, _ := newProcWorker(t, q, repo)
    wA.SetSession(dialSess(t, wA, simA.Addr()))

    ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
    defer cancel()
    go q.Consume(ctx, queue.Key(1, 0), "g", wA.Handle)

    p := pipeline.NewPipeline(repo, q, &fixedRouter{ids: []int{1}})
    msgID, _, err := p.Submit(ctx, pipeline.Outgoing{
        TenantID: "t1", SourceAddr: "shield", Msisdn: "569123", Text: "hola", Priority: 0,
    })
    if err != nil {
        t.Fatal(err)
    }
    waitState(t, repo, msgID, "rejected")
}

func TestWorkerFallbackOnTransport(t *testing.T) {
    simB := smscsim.New(smscsim.Config{Addr: "127.0.0.1:0", SystemID: "esp", Password: "secreto", EnableDLR: true})
    if err := simB.Start(); err != nil {
        t.Fatal(err)
    }
    defer simB.Close()

    repo := store.NewMemory()
    q := queue.NewMemory()

    wA, _, _ := newProcWorker(t, q, repo)
    sA := session.New(session.Config{Host: "127.0.0.1", Port: 1,
        SystemID: "esp", Password: "secreto", SourceAddr: "shield",
        MsgPerSecond: 100, MaxConcurrency: 1}, wA)
    sA.Close() // Submit falla de inmediato con "sin conexion"
    wA.SetSession(sA)

    wB, _, _ := newProcWorker(t, q, repo)
    wB.SetSession(dialSess(t, wB, simB.Addr()))

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    go q.Consume(ctx, queue.Key(1, 0), "g", wA.Handle)
    go q.Consume(ctx, queue.Key(2, 0), "g", wB.Handle)

    r := router.New(&stubRules{groups: []router.Group{{
        ID: 3, Name: "ops",
        Members: []router.GroupMember{{ConnectorID: 1, Weight: 1}, {ConnectorID: 2, Weight: 1}},
    }}}, router.Config{})
    if err := r.Load(ctx); err != nil {
        t.Fatal(err)
    }

    p := pipeline.NewPipeline(repo, q, r)
    msgID, _, err := p.Submit(ctx, pipeline.Outgoing{
        TenantID: "t1", SourceAddr: "shield", Msisdn: "569123", Text: "hola", Priority: 0,
    })
    if err != nil {
        t.Fatal(err)
    }

    m := waitState(t, repo, msgID, "delivered")
    if m.TryCount != 2 {
        t.Fatalf("try=%d", m.TryCount)
    }
}

type fixedRouter struct{ ids []int }

func (f *fixedRouter) Route(_ context.Context, in router.RouteInput) (router.RouteResult, error) {
    return router.RouteResult{RuleID: 1, Connectors: f.ids}, nil
}

type stubRules struct {
    rules  []router.Rule
    groups []router.Group
}

func (s *stubRules) ListRoutingRules(context.Context) ([]router.Rule, error) { return s.rules, nil }
func (s *stubRules) ListGroups(context.Context) ([]router.Group, error)      { return s.groups, nil }
```

- [ ] **Step 5: Correr los tests y verificar que pasan**

Run: `go test ./internal/worker/ ./internal/smscsim/ -v`
Expected: PASS (todos los escenarios de DLR y fallback).

- [ ] **Step 6: Commit**

```bash
git add internal/worker internal/smscsim
git commit -m "feat: worker con dlr via processor y sim con stats configurables"
```

---

### Task 8: API — `GET /api/v1/messages/{id}` y webhooks CRUD

**Files:**
- Modify: `internal/api/server.go` (deps, mux, handlers)
- Modify: `internal/api/server_test.go` (tests nuevos + ajustes)
- Test: `go test ./internal/api/ -v`

**Interfaces:**
- Consumes: `store.WebhookRepo` (Task 2), `api.Server` (M1; el repo de memoria de `store` implementa ambas interfaces).
- Produces:
  - `api.New(cfg, p, r)` pasa a recibir un único parámetro combinado: el `Server` guarda `repo store.MessageRepo` y `wh store.WebhookRepo` (el mismo valor las satisface). **Firma `api.New(cfg, p, repo store.MessageRepo, wh store.WebhookRepo)` si no se usa tipos combinados — se adopta esta opción intermedia:** se define en `api` un parametro `r repos` con `repos interface { store.MessageRepo; store.WebhookRepo }` para no duplicar argumentos.
  - Rutas nuevas:
    - `GET /api/v1/messages/{id}` → `{message_id, tenant_id, msisdn, state, smsc_msgid, try_count, segments, created_at, updated_at}`; 404 si no existe.
    - `GET /api/v1/admin/webhooks` → lista del tenant (`X-Tenant-ID`).
    - `POST /api/v1/admin/webhooks` body `{url, auth_token, events, active}` → 201 con id.
    - `PUT /api/v1/admin/webhooks/{id}` mismo body → 200.
    - `DELETE /api/v1/admin/webhooks/{id}` → 204.

- [ ] **Step 1: Tests de la API**

```go
// internal/api/server_test.go — añadir
func TestGetMessageEndpoint(t *testing.T) {
    ctx := context.Background()
    repo := store.NewMemory()
    q := queue.NewMemory()
    q.Consume(ctx, queue.Key(5, 0), "g", func(queue.Item) error { return nil })

    p := pipeline.NewPipeline(repo, q, stubRouterStore{})
    srv := New(config.Config{}, p, repo)

    repo.CreateMessage(ctx, &store.Message{ID: "abc", TenantID: "t1", Msisdn: "569", State: "delivered"})

    rr := httptest.NewRecorder()
    req := httptest.NewRequest("GET", "/api/v1/messages/abc", nil)
    srv.Handler().ServeHTTP(rr, req)
    if rr.Code != 200 {
        t.Fatalf("status %d: %s", rr.Code, rr.Body.String())
    }
    var out struct {
        MessageID string `json:"message_id"`
        State     string `json:"state"`
    }
    if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
        t.Fatal(err)
    }
    if out.MessageID != "abc" || out.State != "delivered" {
        t.Fatalf("out %+v", out)
    }

    rr = httptest.NewRecorder()
    req = httptest.NewRequest("GET", "/api/v1/messages/no-existe", nil)
    srv.Handler().ServeHTTP(rr, req)
    if rr.Code != 404 {
        t.Fatalf("status %d", rr.Code)
    }
}

func TestWebhooksCRUD(t *testing.T) {
    ctx := context.Background()
    repo := store.NewMemory()
    q := queue.NewMemory()
    q.Consume(ctx, queue.Key(5, 0), "g", func(queue.Item) error { return nil })

    p := pipeline.NewPipeline(repo, q, stubRouterStore{})
    srv := New(config.Config{}, p, repo)
    h := srv.Handler()

    // create
    rr := httptest.NewRecorder()
    req := httptest.NewRequest("POST", "/api/v1/admin/webhooks", strings.NewReader(
        `{"url":"https://cli.example.com/h","auth_token":"tok","events":["delivered"],"active":true}`))
    req.Header.Set("X-Tenant-ID", "t1")
    h.ServeHTTP(rr, req)
    if rr.Code != 201 {
        t.Fatalf("create status %d: %s", rr.Code, rr.Body.String())
    }
    var created struct {
        ID int `json:"id"`
    }
    json.Unmarshal(rr.Body.Bytes(), &created)
    if created.ID == 0 {
        t.Fatal("sin id")
    }

    // update
    rr = httptest.NewRecorder()
    req = httptest.NewRequest("PUT", "/api/v1/admin/webhooks/"+strconv.Itoa(created.ID), strings.NewReader(
        `{"url":"https://cli.example.com/h","auth_token":"tok","events":["expired"],"active":false}`))
    req.Header.Set("X-Tenant-ID", "t1")
    h.ServeHTTP(rr, req)
    if rr.Code != 200 {
        t.Fatalf("update status %d: %s", rr.Code, rr.Body.String())
    }

    // list del tenant
    rr = httptest.NewRecorder()
    req = httptest.NewRequest("GET", "/api/v1/admin/webhooks", nil)
    req.Header.Set("X-Tenant-ID", "t1")
    h.ServeHTTP(rr, req)
    var list []map[string]any
    json.Unmarshal(rr.Body.Bytes(), &list)
    if rr.Code != 200 || len(list) != 1 || list[0]["active"] != false {
        t.Fatalf("list status %d: %+v", rr.Code, list)
    }

    // delete
    rr = httptest.NewRecorder()
    req = httptest.NewRequest("DELETE", "/api/v1/admin/webhooks/"+strconv.Itoa(created.ID), nil)
    req.Header.Set("X-Tenant-ID", "t1")
    h.ServeHTTP(rr, req)
    if rr.Code != 204 {
        t.Fatalf("delete status %d", rr.Code)
    }
}
```

- [ ] **Step 2: Correr los tests y verificar que fallan**

Run: `go test ./internal/api/ -run 'TestGetMessageEndpoint|TestWebhooksCRUD' -v`
Expected: FAIL — rutas/handlers no existen.

- [ ] **Step 3: Implementar server.go — deps, mux y handlers**

```go
// internal/api/server.go — cambios
type repos interface {
    store.MessageRepo
    store.WebhookRepo
}

type Server struct {
    cfg  config.Config
    p    *pipeline.Pipeline
    repo repos
}

func New(cfg config.Config, p *pipeline.Pipeline, repo repos) *Server {
    return &Server{cfg: cfg, p: p, repo: repo}
}
```

```go
// internal/api/server.go — mux (nuevas rutas)
func (s *Server) mux() *http.ServeMux {
    mux := http.NewServeMux()
    mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
        w.WriteHeader(http.StatusOK)
        w.Write([]byte("ok"))
    })
    mux.HandleFunc("POST /api/v1/messages", s.handleSubmit)
    mux.HandleFunc("GET /api/v1/messages/{id}", s.handleGetMessage)
    mux.HandleFunc("GET /api/v1/admin/webhooks", s.handleListWebhooks)
    mux.HandleFunc("POST /api/v1/admin/webhooks", s.handleCreateWebhook)
    mux.HandleFunc("PUT /api/v1/admin/webhooks/{id}", s.handleUpdateWebhook)
    mux.HandleFunc("DELETE /api/v1/admin/webhooks/{id}", s.handleDeleteWebhook)
    return mux
}
```

```go
// internal/api/server.go — handlers nuevos
func (s *Server) handleGetMessage(w http.ResponseWriter, r *http.Request) {
    m, err := s.repo.GetMessage(r.Context(), r.PathValue("id"))
    if err != nil || m == nil {
        http.Error(w, "mensaje no encontrado", http.StatusNotFound)
        return
    }
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]any{
        "message_id": m.ID, "tenant_id": m.TenantID, "msisdn": m.Msisdn,
        "state": m.State, "smsc_msgid": m.SmscMsgid, "try_count": m.TryCount,
        "segments": m.Segments, "created_at": m.CreatedAt, "updated_at": m.UpdatedAt,
    })
}

type webhookReq struct {
    URL       string   `json:"url"`
    AuthToken string   `json:"auth_token"`
    Events    []string `json:"events"`
    Active    bool     `json:"active"`
}

func (s *Server) tenantOf(r *http.Request) string {
    return r.Header.Get("X-Tenant-ID") // M4: desde API key
}

func (s *Server) handleListWebhooks(w http.ResponseWriter, r *http.Request) {
    ws, err := s.repo.ListWebhooks(r.Context(), s.tenantOf(r))
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(ws)
}

func (s *Server) decodeWebhook(w http.ResponseWriter, r *http.Request) (webhookReq, bool) {
    var req webhookReq
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.URL == "" {
        http.Error(w, "json invalido o url requerida", http.StatusBadRequest)
        return req, false
    }
    if req.Events == nil {
        req.Events = []string{}
    }
    return req, true
}

func (s *Server) handleCreateWebhook(w http.ResponseWriter, r *http.Request) {
    req, ok := s.decodeWebhook(w, r)
    if !ok {
        return
    }
    wb := &store.Webhook{TenantID: s.tenantOf(r), URL: req.URL, AuthToken: req.AuthToken,
        Events: req.Events, Active: req.Active}
    if err := s.repo.CreateWebhook(r.Context(), wb); err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusCreated)
    json.NewEncoder(w).Encode(map[string]any{"id": wb.ID})
}

func (s *Server) handleUpdateWebhook(w http.ResponseWriter, r *http.Request) {
    id, err := strconv.Atoi(r.PathValue("id"))
    if err != nil {
        http.Error(w, "id invalido", http.StatusBadRequest)
        return
    }
    req, ok := s.decodeWebhook(w, r)
    if !ok {
        return
    }
    err = s.repo.UpdateWebhook(r.Context(), store.Webhook{
        ID: id, TenantID: s.tenantOf(r), URL: req.URL, AuthToken: req.AuthToken,
        Events: req.Events, Active: req.Active,
    })
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDeleteWebhook(w http.ResponseWriter, r *http.Request) {
    id, err := strconv.Atoi(r.PathValue("id"))
    if err != nil {
        http.Error(w, "id invalido", http.StatusBadRequest)
        return
    }
    if err := s.repo.DeleteWebhook(r.Context(), id); err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    w.WriteHeader(http.StatusNoContent)
}
```

- [ ] **Step 4: Correr los tests y verificar que pasan**

Run: `go test ./internal/api/ -v`
Expected: PASS (incluye `TestMessagesEndpoint` existente; los stubs que pasaban `repo` como two-implemented siguen compilando porque `store.NewMemory()` satisface `repos`).

> Si el `stubRouterStore`/`fixedRouter` usado en tests de API no compila (necesita `ListGroups` desde M2), replicar el stub de M2 que ya lo tiene.

- [ ] **Step 5: Commit**

```bash
git add internal/api
git commit -m "feat: api de consulta de mensajes y crud de webhooks"
```

---

### Task 9: Wiring (`config` + `run.go`) + e2e con webhook + integración gated PG/Redis

**Files:**
- Modify: `internal/config/config.go` y `internal/config/config_test.go` (durations nuevas)
- Modify: `cmd/smppgw/run.go` (server inicia Reconciler; connector arma Processor)
- Modify: `e2e/e2e_test.go` (webhook local + Processor en el worker)
- Modify: `test/integration_test.go` (caso PG+Redis con webhook, gated)
- Test: `go build ./...`, `make test`, y opcional `make test-integration`

**Interfaces:**
- Consumes: `config.Config` (M1), `dlr.*` (Tasks 3-6), `worker.WithDLR`, `api.Server` (Tasks 8).
- Produces:
  - `config.Config` gana `DLRTTL, ReconcileTimeout, ReconcileInterval, WebhookTimeout time.Duration` (env `SMG_DLR_TTL=168h`, `SMG_RECONCILE_TIMEOUT=10m`, `SMG_RECONCILE_INTERVAL=1m`, `SMG_WEBHOOK_TIMEOUT=5s`).
  - `runServer` arranca el reconciler en goroutine; `runConnector` construye `dlr.NewRedisCache(cfg.RedisURL, cfg.DLRTTL)` + `dlr.NewWebhookNotifier(repo, cfg.WebhookTimeout)` + `dlr.NewProcessor(...)` y lo pasa al worker con `worker.WithDLR`.

- [ ] **Step 1: Config — duration fields**

```go
// internal/config/config.go — añadir imports y campos
import (
    "fmt"
    "os"
    "strconv"
    "time"
)

type Config struct {
    Role        string
    ConnectorID int
    HTTPAddr    string
    SMPPAddr    string
    DBURL       string
    RedisURL    string
    DLRTTL             time.Duration
    ReconcileTimeout   time.Duration
    ReconcileInterval  time.Duration
    WebhookTimeout     time.Duration
}
```

```go
// internal/config/config.go — en Load(), después de RedisURL
    c.DLRTTL, err = time.ParseDuration(envOr("SMG_DLR_TTL", "168h"))
    if err != nil {
        return Config{}, fmt.Errorf("SMG_DLR_TTL invalido: %w", err)
    }
    c.ReconcileTimeout, err = time.ParseDuration(envOr("SMG_RECONCILE_TIMEOUT", "10m"))
    if err != nil {
        return Config{}, fmt.Errorf("SMG_RECONCILE_TIMEOUT invalido: %w", err)
    }
    c.ReconcileInterval, err = time.ParseDuration(envOr("SMG_RECONCILE_INTERVAL", "1m"))
    if err != nil {
        return Config{}, fmt.Errorf("SMG_RECONCILE_INTERVAL invalido: %w", err)
    }
    c.WebhookTimeout, err = time.ParseDuration(envOr("SMG_WEBHOOK_TIMEOUT", "5s"))
    if err != nil {
        return Config{}, fmt.Errorf("SMG_WEBHOOK_TIMEOUT invalido: %w", err)
    }
```

```go
// internal/config/config_test.go — añadir
func TestLoadDurations(t *testing.T) {
    os.Setenv("SMG_ROLE", "server")
    defer os.Unsetenv("SMG_ROLE")
    c, err := Load()
    if err != nil {
        t.Fatal(err)
    }
    if c.DLRTTL != 168*time.Hour || c.ReconcileTimeout != 10*time.Minute ||
        c.ReconcileInterval != time.Minute || c.WebhookTimeout != 5*time.Second {
        t.Fatalf("durations=%+v", c)
    }
}
```

- [ ] **Step 2: Correr tests de config y verificar que pasan**

Run: `go test ./internal/config/ -v`
Expected: PASS (el test existente sigue válido con los defaults).

- [ ] **Step 3: run.go — server y connector**

```go
// cmd/smppgw/run.go — runServer (sustituir el de M1/M2)
func runServer(ctx context.Context, cfg config.Config, repo *store.PGRepo, q queue.Queue) error {
    r := router.New(repo, router.Config{})
    if err := r.Load(ctx); err != nil {
        return err
    }
    p := pipeline.NewPipeline(repo, q, r)
    srv := api.New(cfg, p, repo)
    if cfg.ReconcileInterval > 0 {
        rec := dlr.NewReconciler(repo, dlr.NewWebhookNotifier(repo, cfg.WebhookTimeout),
            cfg.ReconcileTimeout, cfg.ReconcileInterval)
        go func() {
            _ = rec.Run(ctx)
        }()
    }
    return srv.Run(ctx)
}
```

```go
// cmd/smppgw/run.go — runConnector (sustituir el de M2)
func runConnector(ctx context.Context, cfg config.Config, repo *store.PGRepo, q queue.Queue, connectorID int) error {
    smppCfg, err := connectorSmppConfig(repo, connectorID)
    if err != nil {
        return err
    }
    cache, err := dlr.NewRedisCache(cfg.RedisURL, cfg.DLRTTL)
    if err != nil {
        return err
    }
    defer cache.Close()
    dlrProc := dlr.NewProcessor(cache, repo, dlr.NewWebhookNotifier(repo, cfg.WebhookTimeout))

    w := worker.NewWorker(q, repo, worker.WithDLR(dlrProc))
    sess := session.New(session.Config{
        ConnectorID: smppCfg.ID, Host: smppCfg.Host, Port: smppCfg.Port,
        SystemID: smppCfg.SystemID, Password: smppCfg.Password,
        SourceAddr: smppCfg.SourceAddr, MsgPerSecond: amtFloat(smppCfg.MaxMsgPerSec),
    }, w)
    w.SetSession(sess)
    if err := sess.Dial(ctx); err != nil {
        return err
    }
    defer sess.Close()
    for prio := 0; prio <= 2; prio++ {
        key := queue.Key(connectorID, prio)
        go func(p int) {
            _ = q.Consume(ctx, key, fmt.Sprintf("cn-%d-%d", connectorID, p), w.Handle)
        }(prio)
    }
    <-ctx.Done()
    return nil
}
```

> `connectorSmppConfig`, `amtFloat` y el `SELECT` de `connectors` ya existen (M1/M2). Importar `dlr` en `run.go`.

- [ ] **Step 4: e2e con webhook local**

```go
// e2e/e2e_test.go — dentro de TestE2EHTTPSubmitToDelivered, sustituir el wiring del worker
    // webhook receptor local
    var mu sync.Mutex
    webhookHits := 0
    whsrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        mu.Lock()
        webhookHits++
        mu.Unlock()
        w.WriteHeader(http.StatusOK)
    }))
    defer whsrv.Close()

    if err := repo.CreateWebhook(ctx, &store.Webhook{
        TenantID: "t1", URL: whsrv.URL, Events: []string{"delivered"}, Active: true,
    }); err != nil {
        t.Fatal(err)
    }

    w := worker.NewWorker(q, repo, worker.WithDLR(
        dlr.NewProcessor(dlr.NewMemCache(), repo, dlr.NewWebhookNotifier(repo, 2*time.Second))))
    sess := session.New(session.Config{
        Host: "127.0.0.1", Port: port, SystemID: "esp", Password: "secreto",
        SourceAddr: "shield", MsgPerSecond: 100, MaxConcurrency: 1,
    }, w)
    w.SetSession(sess)
    if err := sess.Dial(ctx); err != nil {
        t.Fatal(err)
    }
    defer sess.Close()
    go q.Consume(ctx, queue.Key(1, 0), "e2e", w.Handle)

    // ... el resto (submit HTTP + espera de delivered) como en M1/M2 ...
    deadline := time.Now().Add(3 * time.Second)
    for time.Now().Before(deadline) {
        m, _ := repo.GetMessage(ctx, out.MessageID)
        if m != nil && m.State == "delivered" {
            mu.Lock()
            n := webhookHits
            mu.Unlock()
            if n == 0 {
                t.Fatal("webhook no fue llamado")
            }
            return
        }
        time.Sleep(10 * time.Millisecond)
    }
    t.Fatal("e2e: no llego a delivered")
```

- [ ] **Step 5: Test de integración gated (PG + Redis reales)**

```go
// test/integration_test.go — añadir
func TestIntegrationDLRPipeline(t *testing.T) {
    if os.Getenv("SMG_TEST_INTEGRATION") != "1" {
        t.Skip("requiere SMG_TEST_INTEGRATION=1 y PG/Redis locales")
    }
    dsn := os.Getenv("SMG_DB_URL")
    redisURL := os.Getenv("SMG_REDIS_URL")
    if dsn == "" || redisURL == "" {
        t.Skip("SMG_DB_URL/SMG_REDIS_URL vacio")
    }
    ctx := context.Background()

    repo, err := store.NewPG(ctx, dsn)
    if err != nil {
        t.Fatal(err)
    }
    defer repo.Close()

    var received map[string]any
    var mu sync.Mutex
    whsrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        b, _ := io.ReadAll(r.Body)
        mu.Lock()
        json.Unmarshal(b, &received)
        mu.Unlock()
        w.WriteHeader(http.StatusOK)
    }))
    defer whsrv.Close()

    mid := "it-dlr-" + strconv.FormatInt(time.Now().UnixNano(), 10)
    if err := repo.CreateMessage(ctx, &store.Message{
        ID: mid, TenantID: "t1", Msisdn: "569123", Text: "hola", State: "accepted",
    }); err != nil {
        t.Fatal(err)
    }
    if err := repo.CreateWebhook(ctx, &store.Webhook{
        TenantID: "t1", URL: whsrv.URL, Events: []string{"delivered"}, Active: true,
    }); err != nil {
        t.Fatal(err)
    }

    cache, err := dlr.NewRedisCache(redisURL, 0)
    if err != nil {
        t.Fatal(err)
    }
    defer cache.Close()
    proc := dlr.NewProcessor(cache, repo, dlr.NewWebhookNotifier(repo, 2*time.Second))

    if err := proc.Register(ctx, "it-smsc-1", mid); err != nil {
        t.Fatal(err)
    }
    if err := proc.Handle(ctx, "it-smsc-1", "DELIVRD"); err != nil {
        t.Fatal(err)
    }

    m, err := repo.GetMessage(ctx, mid)
    if err != nil || m.State != "delivered" {
        t.Fatalf("state=%+v err=%v", m, err)
    }
    if v, _ := cache.Lookup(ctx, "it-smsc-1"); v != "" {
        t.Fatalf("cache deberia estar limpio, got %q", v)
    }
    deadline := time.Now().Add(2 * time.Second)
    for time.Now().Before(deadline) {
        mu.Lock()
        ok := received != nil
        mu.Unlock()
        if ok {
            break
        }
        time.Sleep(10 * time.Millisecond)
    }
    mu.Lock()
    defer mu.Unlock()
    if received == nil || received["state"] != "delivered" {
        t.Fatalf("webhook=%+v", received)
    }
}
```

- [ ] **Step 6: Verificación completa**

```bash
make test          # unitarios herméticos
go build ./...
make test-integration   # opcional, con PG+Redis + SMG_TEST_INTEGRATION=1 y migraciones aplicadas
```

- [ ] **Step 7: Commit**

```bash
git add cmd internal e2e test internal/config
git commit -m "feat: wiring dlr completo (reconciler, redis cache, webhooks) y e2e"
```

---

## Self-Review del plan

**1. Cobertura del spec (sección "DLR y estados" + webhooks + API):**
- `registered_delivery=1` hacia el proveedor y persistencia de `smsc_msgid` → ya en M1; M3 conserva `SetSmscMsgid` en `OnSubmitResp` (Task 7). ✓
- `deliver_sm` (DLR) correlacionado por Redis `dlr:pend:<smsc_msgid>` → Task 3 (`RedisCache`, prefijo `dlr:pend:`) + Task 4 (`Handle`/`Register`). ✓
- Actualizar `messages.state` (`DELIVRD/EXPIRED/UNDELIV/...`) → Task 3 (`MapStat`) + Task 4 (`UpdateState`). ✓
- Notificar al cliente: POST al webhook para HTTP → Tasks 2 (tabla/repo) y 5 (`WebhookNotifier`); `deliver_sm` para ESME queda documentado para M5 (bind entrante) en el note de Task 4. ✓
- Job de reconciliación: `accepted` sin DLR tras X min → `EXPIRED`/alerta → Task 2 (`updated_at` + `ListStaleAccepted`) + Task 6 (`Reconciler` con notify `expired`). ✓
- Tabla `webhooks` (tenant_id, url, auth_token, eventos suscritos) → Task 1 migración + Task 2 repo. ✓
- `/api/v1/messages/{id}` (estado y DLR) → Task 8. ✓
- Webhooks CRUD bajo `/api/v1/admin` → Task 8. ✓

**2. Placeholder scan:**
- No hay "TBD", "similar a Task N" ni pasos sin código. Todos los snippets son completos y autocontenidos (worker.go y worker_test.go reemplazan por entero las versiones M2). ✓
- Únicas notas son de decisión de diseño (notificación ESME en M5, pgx para `TEXT[]`, elegir stub con `ListGroups` si API test no compila, miniredis en RedisCache). ✓

**3. Type consistency:**
- `dlr.Cache{Register,Lookup,Delete}` (Task 3) → usado por `dlr.Processor` (Task 4) y `dlr.NewRedisCache` / `NewMemCache`. ✓
- `dlr.Notifier{Notify(ctx, Event)}` y `dlr.Event{TenantID,MessageID,SmscMsgid,Msisdn,State,Timestamp}` (Task 4) → consumidos idénticos por `WebhookNotifier` (Task 5), `Reconciler` (Task 6) y tests del worker (Task 7). ✓
- `store.WebhookRepo` (Task 2) → consumido por `WebhookNotifier` (Task 5), `api.Server` (Task 8), `run.go` y e2e (Task 9); `*PGRepo` y `*MemoryRepo` implementan ambas interfaces. ✓
- `store.Message.UpdatedAt` + `ListStaleAccepted` (Task 2) → usados en Task 6 y en tests. `UpdateState`/`SetSmscMsgid` tocan `updated_at` en PG y memoria (Task 2). ✓
- `worker.WithDLR(p *dlr.Processor)` (Task 7) → usado por run.go y e2e (Task 9). `queue.Key(conn, prio)` (M2) reutilizado. ✓
- `api.New(cfg, p, repo repos)` con `repos` = `{MessageRepo; WebhookRepo}` (Task 8) → `store.NewMemory()` y `*PGRepo` lo satisfacen; no rompe `apiConfig()`/e2e (Task 9). ✓
- `config.Config` durations (Task 9) → consumidas por `run.go` (Task 9). ✓
- `smscsim.Config.DLRStatus` (Task 7) → default `DELIVRD` cuando `EnableDLR`; los tests usan `UNDELIV`/`EXPIRED`. ✓

**Lag de ejecución:** Task 8 cambia la firma de `api.New` a un tipo combinado `repos`; al implementarla ajustar `e2e/e2e_test.go` (Task 9) y cualquier test que llame `api.New` (los stubs de memory pasan tal cual). El worker M2 sin `WithDLR` (si alguien lo invocara) simplemente no correlaciona DLR — el e2e y run.go siempre lo construyen con `WithDLR`.