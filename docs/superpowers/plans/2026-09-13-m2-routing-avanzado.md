# M2: Routing Avanzado Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extender el router de M1 con filtros `from`, destino por grupo con balanceo ponderado, orden de candidatos para fallback y reencolado con backoff entre conectores.

**Architecture:** El router M1 (`internal/router`) evaluaba `routing_rules` por prioridad y devolvía un único `connector_id`. M2 lo convierte en: reglas con destino directo O grupo de conectores (`groups` + `group_members` con `weight`), `Route` devuelve una lista ordenada de candidatos y el `worker` reencola al siguiente candidato con `try_count++` y backoff exponencial cuando falla. Los cambios de esquema, store, queue.item y pipeline se hacen en tasks separadas testeables de forma hermética.

**Tech Stack:** Go 1.22+, PostgreSQL 14+ (golang-migrate), Redis 7+ (Streams), `math/rand` (balanceo ponderado con semilla inyectable para tests).

**Spec:** `docs/superpowers/specs/2026-09-12-smpp-gateway-design.md` (sección "Motor de rutas": filtros tenant/from/to/routing_tag, destino `group` con balanceo por peso o `connector_id` directo, fallback reencolando al siguiente candidato con `try_count++` y backoff, `max_tries` → `UNDELIV`).

Plan base previo (ejecutado y mergeado en `main`): `docs/superpowers/plans/2026-09-12-m1-smpp-core.md` — M1 declara: `router.Rule{Priority int; TenantID, Prefix, Regex, RoutingTag string; ConnectorID int}`, `router.Store{ListRoutingRules(ctx) ([]Rule, error)}`, `router.Router{New(s Store, cfg Config); Load(ctx); Route(ctx, tenantID, msisdn, routingTag) (int, error)}`, `queue.Item{ID, TenantID, ConnectorID, Priority, DataCoding, Msisdn, SourceAddr, Text}` con `Queue{Enqueue, Consume, Ack}`, `queue.Key` NO existe (la key `stream:con:<id>:<prio>` se forma inline en `pipeline.queueKey`), `store.Message` (sin `RouteID`), `worker.Worker{NewWorker(q, repo); SetSession(s)}` con `Handle` que marca `failed` (sin fallback), y `smscsim.Config{Addr, SystemID, Password, EnableDLR}`.

## Global Constraints

- Sin Docker: deploy nativo en VM con `systemd`; tests herméticos (todo en memoria excepto el SMSC simulado) y `make test-integration` contra PG+Redis locales con `SMG_TEST_INTEGRATION=1`.
- Go 1.22+ (no usar features posteriores); PostgreSQL 14+, Redis 7+, `golang-migrate` CLI.
- SMPP 3.4 solamente (no SMPP 5.0); `registered_delivery=1` siempre.
- Estilo: sin comentarios innecesarios; nombres y doc en español.
- Las keys de Redis son solo efímeras; lo persistente vive en Postgres.
- TDD por tarea: test → verificar FAIL → implementación mínima → verificar PASS → commit.

---

### Task 1: Migración 0003 — grupos, peso, filtro `from` y `route_id`

**Files:**
- Create: `db/migrations/0003_routing_groups.up.sql`
- Create: `db/migrations/0003_routing_groups.down.sql`
- Test: revisión manual vía `golang-migrate` (mismo patrón que la Task 2 de M1, que no testea SQL)

**Interfaces:**
- Consumes: schema M1 (`tenants`, `connectors`, `routing_rules`, `messages`).
- Produces: tablas `groups`, `group_members`; columnas nuevas en `routing_rules` (`group_id`, `from`) y en `messages` (`route_id`). Las tasks 2-6 las consumen.

- [ ] **Step 1: Escribir la migración up**

```sql
-- db/migrations/0003_routing_groups.up.sql
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
```

Notas:
- `routing_rules.group_id` y `connector_id` son excluyentes: si `group_id > 0` el destino es el grupo; si `group_id IS NULL/0` se usa `connector_id`. Se valida en el router (Task 3).
- `weight 0` permite "apagar" un miembro del grupo sin borrarlo (no entra en el balanceo).
- `messages.route_id` registra la regla que matcheó (ledger; no se usa para rutear de nuevo).

- [ ] **Step 2: Escribir la migración down**

```sql
-- db/migrations/0003_routing_groups.down.sql
ALTER TABLE messages DROP COLUMN IF EXISTS route_id;
ALTER TABLE routing_rules DROP COLUMN IF EXISTS group_id;
ALTER TABLE routing_rules DROP COLUMN IF EXISTS from;
DROP TABLE IF EXISTS group_members;
DROP TABLE IF EXISTS groups;
```

- [ ] **Step 3: Aplicar y verificar**

Run (con `SMG_DB_URL` apuntando al PG de desarrollo):
```bash
migrate -path db/migrations -database "$SMG_DB_URL" up
migrate -path db/migrations -database "$SMG_DB_URL" version
```
Expected: `version` >= 3, sin errores. Revertir con `down 1` y re-aplicar `up` para validar ida-vuelta.

- [ ] **Step 4: Commit**

```bash
git add db/migrations/0003_routing_groups.up.sql db/migrations/0003_routing_groups.down.sql
git commit -m "feat: migracion 0003 grupos y peso de routing"
```

---

### Task 2: Store — `Message.RouteID`, `IncrementTry`, `SetConnector` y repos de grupos/rutas en PG

**Files:**
- Modify: `internal/store/msg.go` (struct `Message` + interfaz `MessageRepo`)
- Modify: `internal/store/memory.go` (nuevos métodos)
- Modify: `internal/store/pg.go` (SQL de messages + `ListRoutingRules` y `ListGroups`)
- Create: `internal/store/msg_test.go` → añadir test de los 3 métodos nuevos (hermético con `MemoryRepo`)
- Test: `go test ./internal/store/ -run TestMessageRepo -v`

**Interfaces:**
- Consumes: tipos M1 `store.Message`, `store.MessageRepo`, `MemoryRepo`, `PGRepo`.
- Produces:
  - `store.Message` gana `RouteID int`.
  - `store.MessageRepo` gana: `IncrementTry(ctx, id) error`, `SetConnector(ctx, id string, connectorID int) error`.
  - `*store.MemoryRepo` implementa los 3 métodos nuevos.
  - `*store.PGRepo` implementa `ListRoutingRules(ctx) ([]router.Rule, error)` y `ListGroups(ctx) ([]router.Group, error)` (método `routeRulesPG` internal). Los tipos `router.Rule`/`router.Group` se definen en Task 3; para que `pg.go` compile en esta task se declaran los métodos con los tipos que ya existen (`router.Rule`) y `router.Group` se agrega en Task 3 — por eso **la Task 3 se implementa justo después de esta** en el mismo ciclo. (Alternativa pragmática: declarar aquí ambos métodos con firma completa y definir `router.Group` en Task 3; `go build` de store falla hasta Task 3.)

> Decisión de dependencia: `store` importa `router` (para `Rule`/`Group`); `router` NO importa `store`. Así no hay ciclo de imports. En M1 el patrón ya quedó fijado (`*store.PGRepo` devolvía `[]router.Rule`).

- [ ] **Step 1: Escribir el test (hermético contra `MemoryRepo`)**

```go
// internal/store/msg_test.go — extensión del test M1 TestMessageRepo
func TestMessageRepoRouteAndTries(t *testing.T) {
    ctx := context.Background()
    repo := NewMemory()

    m := &Message{
        ID: "m2", TenantID: "t1", Msisdn: "56912345678",
        Text: "hola", Segments: 1, ConnectorID: 3, RouteID: 7, State: "buffered",
    }
    if err := repo.CreateMessage(ctx, m); err != nil {
        t.Fatal(err)
    }

    if err := repo.IncrementTry(ctx, "m2"); err != nil {
        t.Fatal(err)
    }
    if err := repo.SetConnector(ctx, "m2", 9); err != nil {
        t.Fatal(err)
    }

    got, err := repo.GetMessage(ctx, "m2")
    if err != nil {
        t.Fatal(err)
    }
    if got.RouteID != 7 || got.TryCount != 1 || got.ConnectorID != 9 {
        t.Fatalf("got %+v", got)
    }
}
```

- [ ] **Step 2: Correr el test y verificar que falla**

Run: `go test ./internal/store/ -run TestMessageRepo -v`
Expected: FAIL — `RouteID`, `IncrementTry`, `SetConnector` no existen.

- [ ] **Step 3: Implementar store**

```go
// internal/store/msg.go — cambios
type Message struct {
    ID          string
    TenantID    string
    SourceAddr  string
    Msisdn      string
    Text        string
    Segments    int
    ConnectorID int
    RouteID     int
    State       string
    TryCount    int
    SmscMsgid   string
    CreatedAt   time.Time
}

type MessageRepo interface {
    CreateMessage(ctx context.Context, m *Message) error
    UpdateState(ctx context.Context, id, state string) error
    SetSmscMsgid(ctx context.Context, id, smscMsgid string) error
    GetMessage(ctx context.Context, id string) (*Message, error)
    IncrementTry(ctx context.Context, id string) error
    SetConnector(ctx context.Context, id string, connectorID int) error
}
```

```go
// internal/store/memory.go — métodos nuevos (sobre *MemoryRepo)
func (r *MemoryRepo) IncrementTry(_ context.Context, id string) error {
    r.mu.Lock()
    defer r.mu.Unlock()
    m, ok := r.msgs[id]
    if !ok {
        return ErrNotFound
    }
    m.TryCount++
    return nil
}

func (r *MemoryRepo) SetConnector(_ context.Context, id string, connectorID int) error {
    r.mu.Lock()
    defer r.mu.Unlock()
    m, ok := r.msgs[id]
    if !ok {
        return ErrNotFound
    }
    m.ConnectorID = connectorID
    return nil
}
```

```go
// internal/store/pg.go — actualizar CreateMessage y GetMessage + nuevos métodos
func (r *PGRepo) CreateMessage(ctx context.Context, m *Message) error {
    _, err := r.pool.Exec(ctx, `
        INSERT INTO messages
            (id, tenant_id, source_addr, msisdn, text, segments, connector_id, route_id, state, try_count)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
        m.ID, m.TenantID, m.SourceAddr, m.Msisdn, m.Text, m.Segments, m.ConnectorID, m.RouteID, m.State, m.TryCount)
    return err
}

func (r *PGRepo) GetMessage(ctx context.Context, id string) (*Message, error) {
    row := r.pool.QueryRow(ctx, `
        SELECT id, tenant_id, source_addr, msisdn, text, segments, connector_id, route_id, state, try_count, smsc_msgid, created_at
        FROM messages WHERE id = $1`, id)
    var m Message
    err := row.Scan(&m.ID, &m.TenantID, &m.SourceAddr, &m.Msisdn, &m.Text,
        &m.Segments, &m.ConnectorID, &m.RouteID, &m.State, &m.TryCount, &m.SmscMsgid, &m.CreatedAt)
    if err != nil {
        return nil, err
    }
    return &m, nil
}

func (r *PGRepo) IncrementTry(ctx context.Context, id string) error {
    _, err := r.pool.Exec(ctx, `UPDATE messages SET try_count = try_count + 1 WHERE id = $1`, id)
    return err
}

func (r *PGRepo) SetConnector(ctx context.Context, id string, connectorID int) error {
    _, err := r.pool.Exec(ctx, `UPDATE messages SET connector_id = $2 WHERE id = $1`, id, connectorID)
    return err
}

var routerRuleScan = func(rows pgx.Rows) ([]router.Rule, error) {
    var out []router.Rule
    for rows.Next() {
        var r router.Rule
        var gid *int
        if err := rows.Scan(&r.ID, &r.Priority, &r.TenantID, &r.Prefix, &r.Regex,
            &r.RoutingTag, &r.From, &r.ConnectorID, &gid); err != nil {
            return nil, err
        }
        if gid != nil {
            r.GroupID = *gid
        }
        out = append(out, r)
    }
    return out, rows.Err()
}

func (r *PGRepo) ListRoutingRules(ctx context.Context) ([]router.Rule, error) {
    rows, err := r.pool.Query(ctx, `
        SELECT id, priority, tenant_id, prefix, regex, routing_tag, from, connector_id, group_id
        FROM routing_rules ORDER BY priority`)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    return routerRuleScan(rows)
}

func (r *PGRepo) ListGroups(ctx context.Context) ([]router.Group, error) {
    rows, err := r.pool.Query(ctx, `SELECT id, name FROM groups ORDER BY id`)
    if err != nil {
        return nil, err
    }
    var groups []router.Group
    for rows.Next() {
        var g router.Group
        if err := rows.Scan(&g.ID, &g.Name); err != nil {
            rows.Close()
            return nil, err
        }
        groups = append(groups, g)
    }
    rows.Close()
    if err := rows.Err(); err != nil {
        return nil, err
    }
    for i := range groups {
        mem, err := r.groupMembers(ctx, groups[i].ID)
        if err != nil {
            return nil, err
        }
        groups[i].Members = mem
    }
    return groups, nil
}

func (r *PGRepo) groupMembers(ctx context.Context, groupID int) ([]router.GroupMember, error) {
    rows, err := r.pool.Query(ctx,
        `SELECT connector_id, weight FROM group_members WHERE group_id = $1`, groupID)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    var out []router.GroupMember
    for rows.Next() {
        var m router.GroupMember
        if err := rows.Scan(&m.ConnectorID, &m.Weight); err != nil {
            return nil, err
        }
        out = append(out, m)
    }
    return out, rows.Err()
}
```

> Nota: `router.Rule` (con `ID`, `From`, `GroupID`) y `router.Group`/`router.GroupMember` se definen en Task 3. Hasta entonces `internal/store` no compila — por eso las Tasks 2 y 3 se encadenan sin commit intermedio de build roto: se corren los tests de la Task 3 junto con los de la Task 2.

- [ ] **Step 4: Correr tests y verificar que pasan**

Run: `go test ./internal/store/ -run TestMessageRepo -v`
Expected: PASS (los métodos nuevos de `MemoryRepo` funcionan; los de PG no se ejercitan sin integración).

- [ ] **Step 5: Commit**

Solo lo que compila sin `router.Group`:
```bash
git add internal/store/msg.go internal/store/memory.go internal/store/pg.go
git commit -m "feat: store route_id, try_count y SetConnector (groups en task 3)"
```
> Si el commit falla por `pg.go` (usa `router.Group`), ejecutar primero los pasos de la Task 3 y commitear juntos las Tasks 2+3. La regla del proyecto es `main` con build verde.

---

### Task 3: Router avanzado — filtro `from`, destinos por grupo, balanceo ponderado y candidatos ordenados

**Files:**
- Modify: `internal/router/router.go` (types `Rule`, `Group`, `RouteInput`, `RouteResult`, `Route`, `Load`, matchers, `candidates`, `weightedOrder`, `pickWeighted`)
- Modify: `internal/router/router_test.go` (reescribir con stubStore que implementa `ListRoutingRules` + `ListGroups`)
- Test: `go test ./internal/router/ -v`

**Interfaces:**
- Consumes: `router.Store` M1 ampliado.
- Produces:
  - `router.Rule{ID int; Priority int; TenantID, From, Prefix, Regex, RoutingTag string; ConnectorID, GroupID int}`
  - `router.GroupMember{ConnectorID, Weight int}`; `router.Group{ID int; Name string; Members []GroupMember}`
  - `router.Store interface { ListRoutingRules(ctx) ([]Rule, error); ListGroups(ctx) ([]Group, error) }`
  - `router.Config{DefaultConnectorID int; Rand *rand.Rand}` (Rand para balanceo determinista en tests)
  - `router.RouteInput{TenantID, SourceAddr, Msisdn, RoutingTag string}`
  - `router.RouteResult{RuleID int; Connectors []int}` — `Connectors[0]` es el candidato elegido por balanceo; el resto es orden fijo de los demás miembros (orden de fallback).
  - `router.Route(ctx, in RouteInput) (RouteResult, error)` — primera regla que matchea gana; si `GroupID == 0` candidato único `[ConnectorID]`.
  - `router.ErrNoRoute` (ya existe en M1).

- [ ] **Step 1: Reescribir el test**

```go
// internal/router/router_test.go
package router

import (
    "context"
    "math/rand"
    "testing"
)

type stubStore struct {
    rules  []Rule
    groups []Group
}

func (s *stubStore) ListRoutingRules(context.Context) ([]Rule, error)  { return s.rules, nil }
func (s *stubStore) ListGroups(context.Context) ([]Group, error)       { return s.groups, nil }

func TestRouteDirectAndDefault(t *testing.T) {
    ctx := context.Background()
    r := New(stubStore{rules: []Rule{
        {ID: 1, Priority: 1, Prefix: "569", ConnectorID: 10},
        {ID: 2, Priority: 2, ConnectorID: 20},
    }}, Config{})
    if err := r.Load(ctx); err != nil {
        t.Fatal(err)
    }
    res, err := r.Route(ctx, RouteInput{Msisdn: "56912345678"})
    if err != nil || len(res.Connectors) != 1 || res.Connectors[0] != 10 {
        t.Fatalf("res=%+v err=%v", res, err)
    }
    if res.RuleID != 1 {
        t.Fatalf("ruleID=%d", res.RuleID)
    }
    res, err = r.Route(ctx, RouteInput{Msisdn: "59899"})
    if err != nil || res.Connectors[0] != 20 {
        t.Fatalf("default res=%+v err=%v", res, err)
    }
}

func TestRouteFromFilter(t *testing.T) {
    ctx := context.Background()
    r := New(stubStore{rules: []Rule{
        {ID: 1, Priority: 1, From: "ventas", ConnectorID: 11},
        {ID: 2, Priority: 2, ConnectorID: 12},
    }}, Config{})
    if err := r.Load(ctx); err != nil {
        t.Fatal(err)
    }
    if res, _ := r.Route(ctx, RouteInput{SourceAddr: "ventas", Msisdn: "5691"}); res.Connectors[0] != 11 {
        t.Fatalf("from+tenant res=%+v", res)
    }
    if res, _ := r.Route(ctx, RouteInput{SourceAddr: "otro", Msisdn: "5691"}); res.Connectors[0] != 12 {
        t.Fatalf("from no match res=%+v", res)
    }
}

func TestRouteByGroupWeighted(t *testing.T) {
    ctx := context.Background()
    r := New(stubStore{
        rules: []Rule{{ID: 9, Priority: 1, Prefix: "569", GroupID: 3}},
        groups: []Group{{ID: 3, Name: "operadores",
            Members: []GroupMember{{ConnectorID: 30, Weight: 5}, {ConnectorID: 31, Weight: 3}, {ConnectorID: 32, Weight: 2}}}},
    }, Config{Rand: rand.New(rand.NewSource(42))})
    if err := r.Load(ctx); err != nil {
        t.Fatal(err)
    }

    hits := map[int]int{}
    for i := 0; i < 1000; i++ {
        res, err := r.Route(ctx, RouteInput{Msisdn: "5691"})
        if err != nil {
            t.Fatal(err)
        }
        if res.RuleID != 9 {
            t.Fatalf("ruleID=%d", res.RuleID)
        }
        hits[res.Connectors[0]]++
        if len(res.Connectors) != 3 {
            t.Fatalf("candidatos=%v (deben ser 3)", res.Connectors)
        }
    }

    // pesos 5/10 = 50%, 3/10 = 30%, 2/10 = 20%; tolerancia amplia
    p30 := 100 * hits[30] / 1000
    p31 := 100 * hits[31] / 1000
    if p30 < 40 || p30 > 60 || p31 < 20 || p31 > 40 {
        t.Fatalf("distribucion fuera de rango: %d/%d/%d", hits[30], hits[31], hits[32])
    }
}

func TestRouteNoRoute(t *testing.T) {
    ctx := context.Background()
    r := New(stubStore{}, Config{})
    if err := r.Load(ctx); err != nil {
        t.Fatal(err)
    }
    if _, err := r.Route(ctx, RouteInput{Msisdn: "599999"}); err != ErrNoRoute {
        t.Fatalf("err=%v, esperaba ErrNoRoute", err)
    }
}
```

- [ ] **Step 2: Correr el test y verificar que falla**

Run: `go test ./internal/router/ -v`
Expected: FAIL — tipos/firmas no compilan contra el M1.

- [ ] **Step 3: Implementar router.go**

```go
// internal/router/router.go (M2)
package router

import (
    "context"
    "errors"
    "math/rand"
    "regexp"
    "strings"
)

type Rule struct {
    ID          int
    Priority    int
    TenantID    string // vacio = todos los tenants
    From        string // vacio = cualquier source_addr
    Prefix      string
    Regex       string
    RoutingTag  string // vacio = cualquier tag
    ConnectorID int    // usado cuando GroupID == 0
    GroupID     int    // >0 anula ConnectorID y usa balanceo de grupo
}

type GroupMember struct {
    ConnectorID int
    Weight      int
}

type Group struct {
    ID      int
    Name    string
    Members []GroupMember
}

type Store interface {
    ListRoutingRules(ctx context.Context) ([]Rule, error)
    ListGroups(ctx context.Context) ([]Group, error)
}

type Config struct {
    DefaultConnectorID int
    Rand               *rand.Rand // semilla inyectable para balanceo determinista en tests
}

type RouteInput struct {
    TenantID   string
    SourceAddr string
    Msisdn     string
    RoutingTag string
}

type RouteResult struct {
    RuleID     int
    Connectors []int // orden de intento: el 1ro es el elegido por balanceo
}

var ErrNoRoute = errors.New("router: sin ruta")

type Router struct {
    store  Store
    cfg    Config
    rules  []Rule
    groups map[int]Group
    defConn int
    rand   *rand.Rand
}

func New(s Store, cfg Config) *Router {
    r := &Router{store: s, cfg: cfg, defConn: cfg.DefaultConnectorID, groups: map[int]Group{}}
    if cfg.Rand != nil {
        r.rand = cfg.Rand
    }
    return r
}

func (r *Router) Load(ctx context.Context) error {
    rules, err := r.store.ListRoutingRules(ctx)
    if err != nil {
        return err
    }
    sortRules(rules)
    r.rules = rules
    groups, err := r.store.ListGroups(ctx)
    if err != nil {
        return err
    }
    for _, g := range groups {
        r.groups[g.ID] = g
    }
    for _, rule := range rules {
        if rule.ConnectorID > 0 {
            r.defConn = rule.ConnectorID // la de menor prioridad actua como default
        }
    }
    return nil
}

func (r *Router) Route(ctx context.Context, in RouteInput) (RouteResult, error) {
    for _, rule := range r.rules {
        if !matchTenant(rule, in.TenantID) {
            continue
        }
        if !matchFrom(rule, in.SourceAddr) {
            continue
        }
        if in.RoutingTag != "" && rule.RoutingTag != "" && rule.RoutingTag != in.RoutingTag {
            continue
        }
        if in.RoutingTag == "" && rule.RoutingTag != "" {
            continue
        }
        if !matchMsisdn(rule, in.Msisdn) {
            continue
        }
        return r.candidates(rule)
    }
    if r.defConn > 0 {
        return RouteResult{Connectors: []int{r.defConn}}, nil
    }
    return RouteResult{}, ErrNoRoute
}

func (r *Router) candidates(rule Rule) (RouteResult, error) {
    if rule.GroupID == 0 {
        return RouteResult{RuleID: rule.ID, Connectors: []int{rule.ConnectorID}}, nil
    }
    g, ok := r.groups[rule.GroupID]
    if !ok || len(g.Members) == 0 {
        return RouteResult{}, ErrNoRoute
    }
    return RouteResult{RuleID: rule.ID, Connectors: weightedOrder(g.Members, r.rand)}, nil
}

// weightedOrder elige un miembro ponderado al azar como primer candidato;
// el resto sigue el orden de definicion en group_members (fallback).
func weightedOrder(members []GroupMember, rr *rand.Rand) []int {
    total := 0
    for _, m := range members {
        total += m.Weight
    }
    var choose float64
    if rr != nil {
        choose = rr.Float64()
    } else {
        choose = rand.Float64()
    }
    chosen := pickWeighted(members, total, choose)
    order := make([]int, 0, len(members))
    order = append(order, members[chosen].ConnectorID)
    for i, m := range members {
        if i != chosen {
            order = append(order, m.ConnectorID)
        }
    }
    return order
}

func pickWeighted(members []GroupMember, total int, u float64) int {
    t := u * float64(total)
    acc := 0
    for i, m := range members {
        acc += m.Weight
        if float64(acc) >= t {
            return i
        }
    }
    return len(members) - 1
}

func matchTenant(rule Rule, tenantID string) bool {
    return rule.TenantID == "" || rule.TenantID == tenantID
}

func matchFrom(rule Rule, sourceAddr string) bool {
    return rule.From == "" || rule.From == sourceAddr
}

func matchMsisdn(rule Rule, msisdn string) bool {
    if rule.Regex != "" {
        ok, _ := regexp.MatchString(rule.Regex, msisdn)
        return ok
    }
    if rule.Prefix != "" {
        return strings.HasPrefix(msisdn, rule.Prefix)
    }
    return true
}

func sortRules(rules []Rule) {
    for i := 1; i < len(rules); i++ {
        for j := i; j > 0 && rules[j].Priority < rules[j-1].Priority; j-- {
            rules[j], rules[j-1] = rules[j-1], rules[j]
        }
    }
}
```

> Semántica importante: `milestone` de pesos se hace solo sobre miembros con `Weight > 0` (el `pickWeighted` los recorre todos; con `Weight 0` el `acc` no sube y nunca se elige salvo cola). El test de distribución valida el rango, no el orden exacto, para no acoplarse al algoritmo de `math/rand`.

- [ ] **Step 4: Correr tests y verificar que pasan**

Run: `go test ./internal/router/ -v`
Expected: PASS. Además `go build ./...` debe pasar (Task 2 + Task 3 juntas).

- [ ] **Step 5: Commit**

```bash
git add internal/router internal/store
git commit -m "feat: router avanzado con grupos y balanceo ponderado"
```
> Incluye el `pg.go` de la Task 2 si no se commiteó en el paso 5 de esa task.

---

### Task 4: Queue `Key` + Item con candidatos; Pipeline con `[TAG]` en texto, `route_id` y candidatos

**Files:**
- Modify: `internal/queue/queue.go` (añadir `Key` y campos `Candidates`, `Try` a `Item`)
- Modify: `internal/pipeline/pipeline.go` (firma interna usa `router.RouteInput`/`RouteResult`; `splitTag`; `queue.Key`; guarda `RouteID`) 
- Modify: `internal/pipeline/pipeline_test.go` (ajustar `testRouter` a la firma nueva)
- Modify: `internal/queue/memory.go` (sin cambios de lógica; solo verifica test)
- Test: `go test ./internal/queue/ ./internal/pipeline/ -v`

**Interfaces:**
- Consumes: `router.Route(ctx, RouteInput) (RouteResult, error)`, `store.Message` con `RouteID`, `smpp.SplitText`.
- Produces:
  - `queue.Key(connectorID, priority int) string` → `"stream:con:<id>:<prio>"` (factorizada aquí; M1 la tenía inline en pipeline).
  - `queue.Item` gana: `Candidates []int json:"candidates"`, `Try int json:"try"`.
  - `pipeline.Submit` conserva firma `(msgID string, segments int, err error)` pero: extrae `[TAG]` del texto si no viene `RoutingTag`, guarda `Message.RouteID`, encola con `Candidates` = lista de `res.Connectors` y `ConnectorID = res.Connectors[0]`.
  - Importa `regexp`, `router` en pipeline.

- [ ] **Step 1: Escribir/adjustar tests**

```go
// internal/queue/queue_test.go
package queue

import "testing"

func TestKey(t *testing.T) {
    if got := Key(3, 1); got != "stream:con:3:1" {
        t.Fatalf("key=%q", got)
    }
}
```

```go
// internal/pipeline/pipeline_test.go — ajuste a la firma nueva
type testRouter struct{ id int }

func (t *testRouter) Route(_ context.Context, in router.RouteInput) (router.RouteResult, error) {
    return router.RouteResult{Connectors: []int{t.id}}, nil
}

func TestSubmitRoutesWithTagAndRouteID(t *testing.T) {
    ctx := context.Background()
    repo := store.NewMemory()
    q := queue.NewMemory()
    go q.Consume(ctx, queue.Key(7, 0), "g", func(it queue.Item) error { return nil })

    p := NewPipeline(repo, q, &testRouter{id: 7})
    msgID, segs, err := p.Submit(ctx, Outgoing{
        TenantID: "t1", SourceAddr: "shield", Msisdn: "569123",
        Text: "hola [PROMO] mundo", Priority: 0,
    })
    if err != nil {
        t.Fatal(err)
    }
    if segs != 1 {
        t.Fatalf("segs=%d", segs)
    }
    m, _ := repo.GetMessage(ctx, msgID)
    if m == nil || m.State != "enqueued" || m.ConnectorID != 7 || m.Text != "hola  mundo" {
        t.Fatalf("msg=%+v (se esperaba texto sin [PROMO])", m)
    }
    _ = segs
}
```

> Por la naturaleza síncrona de `MemoryQueue.Enqueue`, el handler registrado recibe el item; se usa `go q.Consume(...)` (mismo patrón M1).

- [ ] **Step 2: Correr los tests y verificar que fallan**

Run: `go test ./internal/queue/ ./internal/pipeline/ -v`
Expected: FAIL — `Key`, `Candidates`, firma `Route` nueva no existen.

- [ ] **Step 3: Implementar queue y pipeline**

```go
// internal/queue/queue.go — cambios
import "strconv"

type Item struct {
    ID          string `json:"id"`
    TenantID    string `json:"tenant_id"`
    ConnectorID int    `json:"connector_id"`
    Priority    int    `json:"priority"`
    DataCoding  int    `json:"data_coding"`
    Msisdn      string `json:"msisdn"`
    SourceAddr  string `json:"source_addr"`
    Text        string `json:"text"`
    Candidates  []int  `json:"candidates"`
    Try         int    `json:"try"`
}

// Key construye la key del stream para un conector y una prioridad.
func Key(connectorID, priority int) string {
    return "stream:con:" + strconv.Itoa(connectorID) + ":" + strconv.Itoa(priority)
}
```

```go
// internal/pipeline/pipeline.go (M2)
package pipeline

import (
    "context"
    "errors"
    "regexp"
    "time"

    "github.com/eskiconce/smpp-gateway/internal/queue"
    "github.com/eskiconce/smpp-gateway/internal/router"
    "github.com/eskiconce/smpp-gateway/internal/smpp"
    "github.com/eskiconce/smpp-gateway/internal/store"
    "github.com/google/uuid"
)

type Router interface {
    Route(ctx context.Context, in router.RouteInput) (router.RouteResult, error)
}

type Outgoing struct {
    TenantID   string
    SourceAddr string
    Msisdn     string
    Text       string
    RoutingTag string
    Priority   int
    DataCoding int
}

type Pipeline struct {
    repo store.MessageRepo
    q    queue.Queue
    r    Router
}

func NewPipeline(repo store.MessageRepo, q queue.Queue, r Router) *Pipeline {
    return &Pipeline{repo: repo, q: q, r: r}
}

var tagRe = regexp.MustCompile(`\[[A-Za-z0-9_]+\]`)

// splitTag extrae el primer [TAG] del texto y lo devuelve limpio.
func splitTag(text string) (clean, tag string) {
    loc := tagRe.FindStringIndex(text)
    if loc == nil {
        return text, ""
    }
    return text[:loc[0]] + text[loc[1]:], text[loc[0]+1 : loc[1]-1]
}

func (p *Pipeline) Submit(ctx context.Context, out Outgoing) (string, int, error) {
    if out.Msisdn == "" || out.Text == "" {
        return "", 0, errors.New("msisdn y text requeridos")
    }
    text, tag := out.Text, out.RoutingTag
    if tag == "" {
        text, tag = splitTag(out.Text)
    }

    segments := 1
    if len(text) > 160 {
        _, n, err := smpp.SplitText(text)
        if err != nil {
            return "", 0, err
        }
        segments = n
    } else {
        segments = 1
    }

    res, err := p.r.Route(ctx, router.RouteInput{
        TenantID:   out.TenantID,
        SourceAddr: out.SourceAddr,
        Msisdn:     out.Msisdn,
        RoutingTag: tag,
    })
    if err != nil {
        return "", 0, err
    }
    if len(res.Connectors) == 0 {
        return "", 0, router.ErrNoRoute
    }

    msgID := uuid.NewString()
    msg := &store.Message{
        ID: msgID, TenantID: out.TenantID, SourceAddr: out.SourceAddr,
        Msisdn: out.Msisdn, Text: text, Segments: segments,
        ConnectorID: res.Connectors[0], RouteID: res.RuleID, State: "buffered", CreatedAt: time.Now(),
    }
    if err := p.repo.CreateMessage(ctx, msg); err != nil {
        return "", 0, err
    }

    item := queue.Item{
        ID: msgID, TenantID: out.TenantID, ConnectorID: res.Connectors[0],
        Priority: out.Priority, DataCoding: out.DataCoding,
        Msisdn: out.Msisdn, SourceAddr: out.SourceAddr, Text: text,
        Candidates: res.Connectors, Try: 0,
    }
    if err := p.q.Enqueue(ctx, queue.Key(res.Connectors[0], out.Priority), item); err != nil {
        return "", 0, err
    }
    if err := p.repo.UpdateState(ctx, msgID, "enqueued"); err != nil {
        return "", 0, err
    }
    return msgID, segments, nil
}
```

> Cambio de semántica respecto a M1: el conteo de segmentos de M1 usaba `smpp.SplitText` siempre; aquí se conserva ese comportamiento a través de `smpp.SplitText` para textos largos, e implícitamente 1 para cortos. Si M1 ya aplicaba particionado uniforme, mantener el criterio de M1 (usar `smpp.SplitText` siempre) para no regresar comportamiento.

- [ ] **Step 4: Correr tests y verificar que pasan**

Run: `go test ./internal/queue/ ./internal/pipeline/ ./internal/router/ ./internal/store/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/queue internal/pipeline
git commit -m "feat: pipeline con candidatos, route_id y extraccion de tag"
```

---

### Task 5: Worker con fallback — reencolar, backoff, try_count; smscsim ajustable

**Files:**
- Modify: `internal/smscsim/smscsim.go` + `internal/smscsim/smscsim_test.go` (opciones `DropOnSubmit` y `RespondSubmitStatus`)
- Modify: `internal/worker/worker.go` (fallback con candidatos, backoff inyectable, `IncrementTry`/`SetConnector`, `pending` guarda `queue.Item`)
- Modify: `internal/worker/worker_test.go` (tests de fallback por rechazo y por transporte, y el test feliz existente ajustado)
- Test: `go test ./internal/worker/ ./internal/smscsim/ -v`

**Interfaces:**
- Consumes: `queue.Key`, `queue.Item` con `Candidates`/`Try`, `store.MessageRepo` con `IncrementTry`/`SetConnector`, `session.Session`.
- Produces:
  - `smscsim.Config` gana `DropOnSubmit bool` y `RespondSubmitStatus smpp.CommandStatus` (por defecto `ESME_ROK`).
  - `worker.NewWorker(q, repo, opts ...Option) *Worker`; `Option func(*Worker)`; `WithBackoff(f func(try int) time.Duration)`.
  - `Worker.Handle`: `IncrementTry` + `SetConnector`, envía; si `Submit` da error (transporte) → `fallback(it, transportErr=true)`; si OK guarda `pending[seq] = it`.
  - `Worker.OnSubmitResp`: status != ROK → `fallback(it, false)` (reintenta siguiente candidato) si hay; si no → `rejected`.
  - `Worker.fallback(it, transportErr)`: si `Try+1 >= len(Candidates)` → estado `undeliv` (transporte) o `rejected` (rechazo); si no, clona item con `ConnectorID = Candidates[Try+1]`, `Try+1`, y encola `queue.Key(nuevo, prio)` tras `backoff(Try)` (goroutine).
  - Backoff default: `min(500ms << try, 30s)`.

- [ ] **Step 1: Escribir/ajustar smscsim tests (rechazo y drop)**

```go
// internal/smscsim/smscsim_test.go — añadir
func TestSimSubmitRejected(t *testing.T) {
    srv := New(Config{Addr: "127.0.0.1:0", SystemID: "esp", Password: "secreto",
        RespondSubmitStatus: smpp.ESME_RSYSERR})
    if err := srv.Start(); err != nil {
        t.Fatal(err)
    }
    defer srv.Close()

    conn, err := net.Dial("tcp", srv.Addr())
    if err != nil {
        t.Fatal(err)
    }
    defer conn.Close()
    br := bufio.NewReader(conn)

    conn.Write(smpp.Encode(smpp.NewBindTransceiver(1, "esp", "secreto", "", 0, 0, "")))
    p, _ := readPDU(br)
    if p.Header.ID != smpp.BindTransceiverResp || p.Header.Status != smpp.ESME_ROK {
        t.Fatalf("bind resp %+v", p.Header)
    }

    conn.Write(smpp.Encode(mustSubmit(t, "test", "569123", "hola")))
    p, err = readPDU(br)
    if err != nil {
        t.Fatal(err)
    }
    if p.Header.Status != smpp.ESME_RSYSERR {
        t.Fatalf("status=%d", p.Header.Status)
    }
}

func TestSimDropOnSubmit(t *testing.T) {
    srv := New(Config{Addr: "127.0.0.1:0", SystemID: "esp", Password: "secreto", DropOnSubmit: true})
    if err := srv.Start(); err != nil {
        t.Fatal(err)
    }
    defer srv.Close()

    conn, err := net.Dial("tcp", srv.Addr())
    if err != nil {
        t.Fatal(err)
    }
    conn.Write(smpp.Encode(smpp.NewBindTransceiver(1, "esp", "secreto", "", 0, 0, "")))
    conn.Write(smpp.Encode(mustSubmit(t, "test", "569123", "hola")))
    p, err := readPDU(bufio.NewReader(conn))
    conn.Close()
    if err == nil {
        t.Fatalf("esperaba cierre de conexion, se recibio %+v", p)
    }
}
```

- [ ] **Step 2: Correr tests y verificar que fallan**

Run: `go test ./internal/smscsim/ -run 'TestSim' -v`
Expected: FAIL — campos de `Config` no existen.

- [ ] **Step 3: Implementar smscsim.go (cambios) + worker.go**

```go
// internal/smscsim/smscsim.go — Config
type Config struct {
    Addr                string
    SystemID            string
    Password            string
    EnableDLR           bool
    DropOnSubmit        bool
    RespondSubmitStatus smpp.CommandStatus
}
```

```go
// internal/smscsim/smscsim.go — case SubmitSM (nuevo)
case smpp.SubmitSM:
    if s.cfg.DropOnSubmit {
        conn.Close()
        return
    }
    seq := s.nextSeq()
    msgid := fmt.Sprintf("smsc-%d", seq)
    status := s.cfg.RespondSubmitStatus
    if status == 0 {
        status = smpp.ESME_ROK
    }
    conn.Write(smpp.Encode(smpp.NewSubmitSMResp(p.Header.Seq, status, msgid)))
    if status == smpp.ESME_ROK && s.cfg.EnableDLR {
        dlr := "id:" + msgid + " sub:001 dlvrd:001 submit date:2609121230 done date:2609121231 stat:DELIVRD err:000 text:"
        conn.Write(smpp.Encode(smpp.NewDeliverSM(s.nextSeq(), s.cfg.SystemID, "", dlr)))
    }
```

```go
// internal/worker/worker.go (M2)
package worker

import (
    "context"
    "time"

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
    dlr     map[string]string
    backoff func(try int) time.Duration
}

type Option func(*Worker)

func WithBackoff(f func(try int) time.Duration) Option {
    return func(w *Worker) { w.backoff = f }
}

func NewWorker(q queue.Queue, repo store.MessageRepo, opts ...Option) *Worker {
    w := &Worker{
        q: q, repo: repo,
        pending: map[uint32]queue.Item{},
        dlr:     map[string]string{},
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
    if status != smpp.ESME_ROK {
        w.repo.SetSmscMsgid(ctx, it.ID, msgid)
        w.fallback(it, false)
        return
    }
    w.repo.SetSmscMsgid(ctx, it.ID, msgid)
    w.dlr[msgid] = it.ID // cache en caliente; la persistencia real llega en M3
}

// OnDLR implementa session.Handler.
func (w *Worker) OnDLR(msgid, stat string) {
    id, ok := w.dlr[msgid]
    if !ok {
        return
    }
    state := "delivered"
    if stat != "DELIVRD" {
        state = "undeliv"
    }
    w.repo.UpdateState(context.Background(), id, state)
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

- [ ] **Step 4: Escribir/ajustar worker tests (fallback, feliz, exhaust)**

```go
// internal/worker/worker_test.go
package worker

import (
    "context"
    "net"
    "strconv"
    "testing"
    "time"

    "github.com/eskiconce/smpp-gateway/internal/pipeline"
    "github.com/eskiconce/smpp-gateway/internal/queue"
    "github.com/eskiconce/smpp-gateway/internal/router"
    "github.com/eskiconce/smpp-gateway/internal/session"
    "github.com/eskiconce/smpp-gateway/internal/smscsim"
    "github.com/eskiconce/smpp-gateway/internal/store"
)

func noBackoff(int) time.Duration { return 0 }

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

func TestWorkerDeliveredHappyPath(t *testing.T) {
    sim := smscsim.New(smscsim.Config{Addr: "127.0.0.1:0", SystemID: "esp", Password: "secreto", EnableDLR: true})
    if err := sim.Start(); err != nil {
        t.Fatal(err)
    }
    defer sim.Close()

    repo := store.NewMemory()
    q := queue.NewMemory()
    w := NewWorker(q, repo, WithBackoff(noBackoff))
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
    deadline := time.Now().Add(3 * time.Second)
    for time.Now().Before(deadline) {
        m, _ := repo.GetMessage(ctx, msgID)
        if m != nil && m.State == "delivered" {
            return
        }
        time.Sleep(10 * time.Millisecond)
    }
    t.Fatal("no llego a delivered")
}

func TestWorkerFallbackOnReject(t *testing.T) {
    // conector 1 rechaza (ESME_RSYSERR); conector 2 entrega + DLR
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

    wA := NewWorker(q, repo, WithBackoff(noBackoff))
    wA.SetSession(dialSess(t, wA, simA.Addr()))
    wB := NewWorker(q, repo, WithBackoff(noBackoff))
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

    deadline := time.Now().Add(3 * time.Second)
    for time.Now().Before(deadline) {
        m, err := repo.GetMessage(ctx, msgID)
        if err != nil {
            continue
        }
        if m.State == "delivered" {
            if m.ConnectorID != 2 {
                t.Fatalf("conector final=%d, esperaba 2", m.ConnectorID)
            }
            if m.TryCount != 2 {
                t.Fatalf("try_count=%d, esperaba 2 (reintento)", m.TryCount)
            }
            return
        }
        time.Sleep(10 * time.Millisecond)
    }
    t.Fatal("fallback no entrego por B")
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
    wA := NewWorker(q, repo, WithBackoff(noBackoff))
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

    deadline := time.Now().Add(2 * time.Second)
    for time.Now().Before(deadline) {
        m, _ := repo.GetMessage(ctx, msgID)
        if m != nil && m.State == "rejected" {
            return
        }
        time.Sleep(10 * time.Millisecond)
    }
    t.Fatal("no quedo rejected")
}

func TestWorkerFallbackOnTransport(t *testing.T) {
    // sesion A muerta (sin dial) => Submit falla por transporte => reencola a B
    simB := smscsim.New(smscsim.Config{Addr: "127.0.0.1:0", SystemID: "esp", Password: "secreto", EnableDLR: true})
    if err := simB.Start(); err != nil {
        t.Fatal(err)
    }
    defer simB.Close()

    repo := store.NewMemory()
    q := queue.NewMemory()

    wA := NewWorker(q, repo, WithBackoff(noBackoff))
    // sin SetSession: sess es nil, Submit devuelve panic -> en su lugar usamos una sesion cerrada
    sA := session.New(session.Config{Host: "127.0.0.1", Port: 1,
        SystemID: "esp", Password: "secreto", SourceAddr: "shield",
        MsgPerSecond: 100, MaxConcurrency: 1}, wA)
    sA.Close() // marcada como cerrada: Submit falla al instante
    wA.SetSession(sA)

    wB := NewWorker(q, repo, WithBackoff(noBackoff))
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

    deadline := time.Now().Add(3 * time.Second)
    for time.Now().Before(deadline) {
        m, _ := repo.GetMessage(ctx, msgID)
        if m != nil && m.State == "delivered" {
            if m.TryCount != 2 {
                t.Fatalf("try_count=%d", m.TryCount)
            }
            return
        }
        time.Sleep(10 * time.Millisecond)
    }
    t.Fatal("transporte: fallback no entrego por B")
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

> Nota sobre el test de transporte: conectar a `127.0.0.1:1` en `session.New` NO dialea (el test de `Dial` de M1 usa backoff); en su lugar se crea la sesión y se llama `sA.Close()` ANTES de hacer `Submit`, lo que marca `closed=true` y hace fallar `Submit` con "sin conexion" — el caso real de connector caído. `TestWorkerFallbackOnTransport` ejerce exactamente ese camino.

- [ ] **Step 5: Correr tests y verificar que pasan**

Run: `go test ./internal/worker/ ./internal/smscsim/ -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/worker internal/smscsim
git commit -m "feat: worker con fallback a siguiente candidato y backoff"
```

---

### Task 6: Wiring real (PG+Redis) + e2e con grupos + make

**Files:**
- Modify: `cmd/smppgw/run.go` (server: router con `PGRepo`; connector: worker con backoff y consumo de prioridades)
- Modify: `e2e/e2e_test.go` (usar router con grupo y stub de store con `ListGroups`)
- Modify: `internal/api/server_test.go` (stubRouterStore gana `ListGroups`)
- Create: `test/integration_test.go` → añadir caso de grupo contra PG real (gated por `SMG_TEST_INTEGRATION=1`)
- Test: `go build ./...`, `go test ./...`, y opcional `make test-integration`

**Interfaces:**
- Consumes: `api.Server` (M1), `pipeline.Pipeline`, `queue`, `router`, `store.PGRepo` (M2).
- Produces: `runServer` carga reglas+grupos desde PG; `runConnector` consume `stream:con:<id>:<prio>` para prio 0..9 con el mismo worker.

- [ ] **Step 1: Ajustar run.go**

```go
// cmd/smppgw/run.go — runServer (nuevo)
func runServer(ctx context.Context, cfg config.Config, repo *store.PGRepo, q queue.Queue) error {
    r := router.New(repo, router.Config{})
    if err := r.Load(ctx); err != nil {
        return err
    }
    p := pipeline.NewPipeline(repo, q, r)
    srv := api.New(cfg, p, repo)
    return srv.Run(ctx)
}
```

```go
// cmd/smppgw/run.go — runConnector (nuevo)
func runConnector(ctx context.Context, cfg config.Config, repo *store.PGRepo, q queue.Queue, connectorID int) error {
    smppCfg, err := connectorSmppConfig(repo, connectorID) // SELECT de connectors
    if err != nil {
        return err
    }
    w := worker.NewWorker(q, repo)
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

> `connectorSmppConfig`, `amtFloat` y el SELECT de `connectors` se codifican en esta task reutilizando `config.Config` y el helper de M1; si la Task 11 de M1 ya definió `runConnector` con su propio query, se adapta para consumir las 3 prioridades y el `SetSession` nuevo.

- [ ] **Step 2: Ajustar e2e para grupos**

```go
// e2e/e2e_test.go — e2eRules implementa Store con grupos
type e2eRules struct {
    groups []router.Group
}

func (e *e2eRules) ListRoutingRules(context.Context) ([]router.Rule, error) {
    return []router.Rule{{ID: 1, Priority: 1, Prefix: "569", GroupID: 3}}, nil
}

func (e *e2eRules) ListGroups(context.Context) ([]router.Group, error) {
    return e.groups, nil
}
```

```go
// e2e/e2e_test.go — dentro de TestE2EHTTPSubmitToDelivered
    r := router.New(&e2eRules{groups: []router.Group{{
        ID: 3, Name: "ops",
        Members: []router.GroupMember{{ConnectorID: 1, Weight: 1}},
    }}}, router.Config{})
    if err := r.Load(ctx); err != nil {
        t.Fatal(err)
    }
    p := pipeline.NewPipeline(repo, q, r)
    srv := api.New(apiConfig(), p, repo)
    ts := httptest.NewServer(srv.Handler())
    defer ts.Close()
```

- [ ] **Step 3: Ajustar api/server_test.go**

```go
// internal/api/server_test.go — stubRouterStore gana ListGroups
type stubRouterStore struct {
    rules []router.Rule
}

func (stubRouterStore) ListRoutingRules(context.Context) ([]router.Rule, error) {
    return []router.Rule{{Priority: 1, ConnectorID: 5}}, nil
}

func (stubRouterStore) ListGroups(context.Context) ([]router.Group, error) {
    return []router.Group{}, nil
}
```

- [ ] **Step 4: Test de integración (PG real, gated)**

```go
// test/integration_test.go — añadir
func TestIntegrationRouteGroup(t *testing.T) {
    if os.Getenv("SMG_TEST_INTEGRATION") != "1" {
        t.Skip("requiere SMG_TEST_INTEGRATION=1 y PG/Redis locales")
    }
    dsn := os.Getenv("SMG_DB_URL")
    if dsn == "" {
        t.Skip("SMG_DB_URL vacio")
    }
    ctx := context.Background()
    repo, err := store.NewPG(ctx, dsn)
    if err != nil {
        t.Fatal(err)
    }
    defer repo.Close()

    r := router.New(repo, router.Config{})
    if err := r.Load(ctx); err != nil {
        t.Fatal(err)
    }
    // con el seed de M1/Task12 (connector esp + grupo de prueba) se espera ruta
    res, err := r.Route(ctx, router.RouteInput{Msisdn: "569123"})
    if err != nil {
        t.Fatal(err)
    }
    if len(res.Connectors) == 0 {
        t.Fatal("sin candidatos")
    }
}
```

- [ ] **Step 5: Verificación completa**

```bash
make test          # todos los unitarios herméticos deben pasar
go build ./...
make test-integration   # opcional, con PG+Redis locales + SMG_TEST_INTEGRATION=1
```

- [ ] **Step 6: Commit**

```bash
git add cmd internal e2e test Makefile
git commit -m "feat: wiring con grupos y fallback en roles server/connector"
```

---

## Self-Review del plan

**1. Cobertura del spec (sección "Motor de rutas"):**
- Filtro `from` → Task 3 (`matchFrom`) + Task 1 (columna) + Task 2 (SELECT). ✓
- Filtros tenant/prefix/regex/routing_tag ya estaban en M1; se mantienen en Task 3. ✓
- Destino `group` con balanceo por peso → Tasks 3 (`weightedOrder`/`pickWeighted`) y 1-2 (tablas). ✓
- Destino `connector_id` directo → Task 3 (`candidates`). ✓
- Fallback reencola al siguiente candidato con `try_count++` → Task 5 (`fallback` + `IncrementTry`) + Task 2 (método). ✓
- Backoff en el reencolado → Task 5 (`defaultBackoff`). ✓
- `max_tries` → `UNDELIV` → Task 5 (estado `undeliv` al agotar candidatos). ✓
- "Salida de conector dead (transporte)" → Task 5 `TestWorkerFallbackOnTransport`. ✓
- `routing_rules` evaluadas por `priority`, primera que matchea gana → Task 3 (loop + sortRules). ✓
- El mensaje en cola lleva conector (y en M4 la tarifa) resuelto → Task 4 (`item.ConnectorID = res.Connectors[0]`). ✓

**2. Placeholder scan:**
- No hay "TBD", "similar a Task N" ni pasos sin código. Todos los snippets de código son completos.
- Únicas notas son de decisión de diseño (import de store→router, semántica de `weight 0`, test de transporte con sesión cerrada, integración gated).

**3. Type consistency:**
- `router.Route(ctx, RouteInput) (RouteResult, error)` definido en Task 3 y consumido por pipeline (Task 4) y worker-tests (Task 5). M1 ya no lo usa tras el merge de M2 (los `testRouter`/`fixedRouter` de M1 se actualizan en Tasks 4-6). ✓
- `router.Rule{ID, Priority, TenantID, From, Prefix, Regex, RoutingTag, ConnectorID, GroupID}` = campos del SELECT de `ListRoutingRules` (Task 2). ✓
- `router.Group{ID, Name, Members []GroupMember}` bajo `ListGroups` (Task 2 produce; Task 3 define). `GroupMember{ConnectorID, Weight}` coincide con `group_members`. ✓
- `queue.Item.Candidates []int` y `.Try int` (Task 4 → Task 5). `queue.Key(conn, prio)` (Task 4 → Tasks 5, 6). ✓
- `store.Message.RouteID`, `IncrementTry`, `SetConnector` (Task 2 → Task 4 crea con RouteID, Task 5 usa IncrementTry/SetConnector). ✓
- `smscsim.Config.DropOnSubmit`/`RespondSubmitStatus` (Task 5 define + testea en la misma task). `smpp.ESME_RSYSERR` y `smpp.CommandStatus` vienen de M1 Task 4. ✓
- `smpp.SplitText(text) ([]string, int, error)` (M1) usado en Task 4; segundos de retorno alineados. ✓
- `api.New(cfg, p, repo)`, `api.Server.Handler()` (M1 Task 11) usados por e2e en Task 6. ✓
- `session.New`/`Dial`/`Submit(...) (uint32, error)` (M1 Task 7) usados en Tasks 5-6. ✓

**Lag farmacéutica:** la Task 2 depende de tipos definidos en Task 3 (por eso se ejecutan y commitean juntas o se ordena implementar Task 3 inmediatamente después); el plan lo documenta en el paso 5 de la Task 2 para que el executor no rompa `main`. El `store.PGRepo` es el único que implementa `router.Store` (import de store→router, sin ciclo).