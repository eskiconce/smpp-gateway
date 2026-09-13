# M6 — API Admin + Frontend React + Deploy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Completar la plataforma: API admin (auth JWT+RBAC, CRUD de connectors/groups/routing-rules/users, búsqueda de mensajes, métricas con SSE), frontend React+Vite+TS servido por nginx (login, dashboard, todos los CRUD), y deploy en 1 VM sin Docker (systemd + nginx + golang-migrate).

**Architecture:** El binario `smppgw server` expone `/api/v1` con dos capas de auth: `Authorization: Bearer <api_key>` (clientes, envío) y JWT HS256 + RBAC para las rutas `/api/v1/admin/*` y `/api/v1/metrics`. El frontend es una SPA estática que solo consume la API (JWT en localStorage), servida por nginx que proxya `/api` a `127.0.0.1:8080` (`proxy_buffering off` para SSE). Metadatos admin (users) y métricas se calculan desde Postgres; no se requieren servicios extra.

**Tech Stack:** Go (net/http `http.ServeMux` con patterns, go-redis v9, pgxpool), React 18 + Vite 5 + TypeScript + Vitest (sin router/axios/libs extra), nginx, systemd (`smppgw-server.service`, `smppgw-connector@.service`), golang-migrate.

**Spec:** `docs/superpowers/specs/2026-09-12-smpp-gateway-design.md` — secciones "Decisiones clave" (auth JWT+RBAC), "Modelo de datos" (tabla `users`), "API REST" (`/api/v1/admin/{...}`, `/api/v1/metrics` + SSE, `/metrics` opcional), "Frontend React (SPA vía nginx)", "Despliegue VM (sin Docker)".

## Global Constraints

- Go 1.22+; PostgreSQL 14+; Redis 7+; sin Docker en ningún entorno.
- **Sin dependencias nuevas** de Go fuera de: `github.com/redis/go-redis/v9`, `github.com/alicebob/miniredis/v2`, `github.com/jackc/pgx/v5`, `golang.org/x/time/rate`. (No hay librería JWT ni bcrypt: JWT HS256 y password-KDF se implementan con stdlib.)
- Frontend sin librerías extra: solo `react`, `react-dom`, `vite`, `typescript`, `@vitejs/plugin-react`, `vitest`. Rutas por hash manual, sin react-router; estilos en un solo CSS.
- La spec dice "hash bcrypt" — v1 usa KDF de stdlib (HMAC-SHA256 iterado con salt aleatorio, `iter:salt:hash`) por la restricción de dependencias. El texto del login no cambia.
- Nombres de tablas/columnas/JSON y textos de UI en español (siguiendo M1–M5). Código/funciones en inglés.
- TDD: cada task empieza con el test que falla. Commits en inglés. Tasks con checkbox.
- `api.New(cfg, p, repo repos)` mantiene un único parámetro `repo` combinado (patrón M3/M4): el `repos` interface se extiende, no se cambia la firma del constructor.
- Métricas y búsqueda se leen de Postgres (fuente de verdad); Redis sigue siendo solo efímero.

---

### Task 1: Migración `0007_admin` — tabla `users` + índices de búsqueda

**Files:**
- Create: `db/migrations/0007_admin.up.sql`
- Create: `db/migrations/0007_admin.down.sql`
- Test: `make test` (solo compilación, sin Go)

**Interfaces:**
- Consumes: esquema existente (M1 `0001`, M2 `0003`, M3 `0004`, M4/M5 migraciones de rate/tenants/smpp).
- Produces:
  - Tabla `smpp.users` para RBAC: `id SERIAL` PK, `username TEXT UNIQUE`, `password_hash TEXT`, `role TEXT CHECK IN ('superadmin','admin','viewer')`, `tenant_id TEXT NULL`, `created_at TIMESTAMPTZ`.
  - Índices de búsqueda de mensajes: `idx_messages_tenant_created (tenant_id, created_at DESC)` y `idx_messages_msisdn (msisdn)`.
  - No hay seed de datos; el superadmin inicial se bootstrapa en Task 5 desde env del proceso.

- [ ] **Step 1: Migración up**

```sql
-- db/migrations/0007_admin.up.sql
CREATE TABLE users (
    id            SERIAL PRIMARY KEY,
    username      TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role          TEXT NOT NULL CHECK (role IN ('superadmin', 'admin', 'viewer')),
    tenant_id     TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_messages_tenant_created ON messages(tenant_id, created_at DESC);
CREATE INDEX idx_messages_msisdn ON messages(msisdn);
```

- [ ] **Step 2: Migración down**

```sql
-- db/migrations/0007_admin.down.sql
DROP TABLE IF EXISTS users;
DROP INDEX IF EXISTS idx_messages_tenant_created;
DROP INDEX IF EXISTS idx_messages_msisdn;
```

- [ ] **Step 3: Verificar aplicable/deshacer**

Run: `make migrate-up && make migrate-down && make migrate-up`
Expected: sin errores (aplicar, revertir, reaplicar limpio).

- [ ] **Step 4: Commit**

```bash
git add db/migrations/0007_admin.up.sql db/migrations/0007_admin.down.sql
git commit -m "feat: migration 0007 users table + message search indexes"
```

---

### Task 2: Store — `UserRepo`, `ConnectorRepo`, `GroupRepo`, `RuleRepo`, `StatsRepo`, `ListMessages`

**Files:**
- Modify: `internal/store/store.go` (tipos: `User`, `Connector`, `MessageFilter`, `ConnectorCount`; interfaces nuevas)
- Modify: `internal/store/pg.go` (implementación PG de las nuevas repos)
- Modify: `internal/store/memory.go` (implementación memory para tests)
- Modify: `internal/store/messages_test.go`, `internal/store/memory_test.go` (tests)
- Test: `go test ./internal/store/ -v`

**Interfaces:**
- Consumes: `store.Message` (M1/M3/M4), `router.Rule`/`router.Group`/`router.GroupMember` (M2, ya devueltos por `ListRoutingRules`/`ListGroups`), patrones de escaneo de M1–M4.
- Produces:
  - `store.User{ID int; Username, PasswordHash, Role, TenantID string; CreatedAt time.Time}`
  - `store.UserRepo{ListUsers(ctx) ([]User, error); GetUserByUsername(ctx, username string) (*User, error); CreateUser(ctx, u *User) error; DeleteUser(ctx, id int) error}`
  - `store.Connector{ID int; Name, Type, Host, SystemID, Password, BindMode, SourceAddr string; Port, Concurrency, EnquireLinkInterval int; SourceTON, SourceNPI, DestTON, DestNPI int; MaxMsgPerSec float64; TLS, Enabled bool}`
  - `store.ConnectorRepo{ListConnectors(ctx) ([]Connector, error); GetConnector(ctx, id int) (*Connector, error); CreateConnector(ctx, c *Connector) (int, error); UpdateConnector(ctx, c *Connector) error; DeleteConnector(ctx, id int) error}`
  - `store.GroupRepo{CreateGroup(ctx, name string) (int, error); DeleteGroup(ctx, id int) error; SetGroupMembers(ctx, groupID int, members []router.GroupMember) error}` — `ListGroups` (M2) se reutiliza.
  - `store.RuleRepo{CreateRoutingRule(ctx, r router.Rule) (int, error); DeleteRoutingRule(ctx, id int) error; UpdateRoutingRulePriority(ctx, id, priority int) error}` — `ListRoutingRules` (M2) se reutiliza.
  - `store.MessageFilter{TenantID, Msisdn, State string; ConnectorID int; Limit, Offset int}` y método `ListMessages` en `MessageRepo`: `ListMessages(ctx, f MessageFilter) ([]Message, int, error)` (items + total, orden `created_at DESC`; `Limit` 0 → 50, máximo 500; `Limit` es el tamaño de página y el total cuenta sin límite).
  - `store.ConnectorCount{ConnectorID, Count int}`
  - `store.StatsRepo{CountByState(ctx, tenantID string, since time.Time) (map[string]int, error); CountByConnector(ctx, tenantID string, since time.Time) ([]ConnectorCount, error); CountMessages(ctx, tenantID string, since time.Time) (int, error)}` — `tenantID == ""` = todos; `since` filtra `created_at`.

- [ ] **Step 1: Escribir los tests (memory)**

```go
// internal/store/memory_test.go — añadir
func TestUserRepoMemory(t *testing.T) {
    ctx := context.Background()
    repo := NewMemory()
    if err := repo.CreateUser(ctx, &User{Username: "admin", PasswordHash: "h", Role: "superadmin"}); err != nil {
        t.Fatal(err)
    }
    u, err := repo.GetUserByUsername(ctx, "admin")
    if err != nil || u.Role != "superadmin" {
        t.Fatalf("got=%+v err=%v", u, err)
    }
    if _, err := repo.GetUserByUsername(ctx, "nadie"); err != ErrNotFound {
        t.Fatalf("esperaba ErrNotFound, got %v", err)
    }
    users, _ := repo.ListUsers(ctx)
    if len(users) != 1 {
        t.Fatalf("users=%d", len(users))
    }
    if err := repo.DeleteUser(ctx, u.ID); err != nil {
        t.Fatal(err)
    }
}

func TestConnectorRepoMemory(t *testing.T) {
    ctx := context.Background()
    repo := NewMemory()
    id, err := repo.CreateConnector(ctx, &Connector{Name: "c1", Type: "smpp", Host: "h", Port: 2775})
    if err != nil || id == 0 {
        t.Fatalf("id=%d err=%v", id, err)
    }
    g, err := repo.GetConnector(ctx, id)
    if err != nil || g.Name != "c1" {
        t.Fatalf("got=%+v err=%v", g, err)
    }
    g.Port = 4000
    if err := repo.UpdateConnector(ctx, g); err != nil {
        t.Fatal(err)
    }
    got, _ := repo.GetConnector(ctx, id)
    if got.Port != 4000 {
        t.Fatalf("port=%d", got.Port)
    }
    list, _ := repo.ListConnectors(ctx)
    if len(list) != 1 {
        t.Fatalf("list=%d", len(list))
    }
    if err := repo.DeleteConnector(ctx, id); err != nil {
        t.Fatal(err)
    }
    if _, err := repo.GetConnector(ctx, id); err != ErrNotFound {
        t.Fatalf("esperaba ErrNotFound, got %v", err)
    }
}

func TestGroupRuleRepoMemory(t *testing.T) {
    ctx := context.Background()
    repo := NewMemory()
    cid, _ := repo.CreateConnector(ctx, &Connector{Name: "c1", Type: "smpp", Host: "h", Port: 2775})
    gid, err := repo.CreateGroup(ctx, "grupo-a")
    if err != nil || gid == 0 {
        t.Fatalf("gid=%d err=%v", gid, err)
    }
    if err := repo.SetGroupMembers(ctx, gid, []router.GroupMember{{ConnectorID: cid, Weight: 100}}); err != nil {
        t.Fatal(err)
    }
    groups, _ := repo.ListGroups(ctx)
    if len(groups) != 1 || len(groups[0].Members) != 1 || groups[0].Members[0].Weight != 100 {
        t.Fatalf("groups=%+v", groups)
    }
    rid, err := repo.CreateRoutingRule(ctx, router.Rule{Priority: 1, Prefix: "569", GroupID: gid})
    if err != nil || rid == 0 {
        t.Fatalf("rid=%d err=%v", rid, err)
    }
    rules, _ := repo.ListRoutingRules(ctx)
    if len(rules) != 1 || rules[0].Prefix != "569" {
        t.Fatalf("rules=%+v", rules)
    }
    if err := repo.UpdateRoutingRulePriority(ctx, rid, 5); err != nil {
        t.Fatal(err)
    }
    rules, _ = repo.ListRoutingRules(ctx)
    if rules[0].Priority != 5 {
        t.Fatalf("priority=%d", rules[0].Priority)
    }
    if err := repo.DeleteRoutingRule(ctx, rid); err != nil {
        t.Fatal(err)
    }
    if err := repo.DeleteGroup(ctx, gid); err != nil {
        t.Fatal(err)
    }
}

func TestListMessagesAndStatsMemory(t *testing.T) {
    ctx := context.Background()
    repo := NewMemory()
    for i := 0; i < 3; i++ {
        if err := repo.CreateMessage(ctx, &Message{
            ID: "m" + itoa(i), TenantID: "t1", Msisdn: "569" + itoa(i),
            State: "delivered", ConnectorID: 1, Segments: 1, Text: "hola",
        }); err != nil {
            t.Fatal(err)
        }
    }
    items, total, err := repo.ListMessages(ctx, MessageFilter{Msisdn: "56", State: "delivered", Limit: 2})
    if err != nil || total != 3 || len(items) != 2 {
        t.Fatalf("items=%d total=%d err=%v", len(items), total, err)
    }
    byState, err := repo.CountByState(ctx, "t1", time.Time{})
    if err != nil || byState["delivered"] != 3 {
        t.Fatalf("byState=%v err=%v", byState, err)
    }
    byConn, err := repo.CountByConnector(ctx, "", time.Time{})
    if err != nil || len(byConn) != 1 || byConn[0].Count != 3 {
        t.Fatalf("byConn=%+v err=%v", byConn, err)
    }
    n, err := repo.CountMessages(ctx, "", time.Time{})
    if err != nil || n != 3 {
        t.Fatalf("n=%d err=%v", n, err)
    }
}

func itoa(i int) string { return fmt.Sprintf("%d", i) }
```

> `fmt` ya se importa en `memory_test.go` (M1). Si no, añadirlo.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/store/ -run 'Memory' -v`
Expected: FAIL — métodos no compilan (`CreateUser` undefined etc.).

- [ ] **Step 3: Tipos e interfaces en store.go**

```go
// internal/store/store.go — añadir al final
type User struct {
    ID           int
    Username     string
    PasswordHash string
    Role         string
    TenantID     string
    CreatedAt    time.Time
}

type Connector struct {
    ID                    int
    Name                  string
    Type                  string
    Host                  string
    Port                  int
    SystemID              string
    Password              string
    BindMode              string
    SourceAddr            string
    SourceTON             int
    SourceNPI             int
    DestTON               int
    DestNPI               int
    Concurrency           int
    EnquireLinkInterval   int
    MaxMsgPerSec          float64
    TLS                   bool
    Enabled               bool
}
```

```go
// internal/store/store.go — interfaces nuevas (añadir al final)
type UserRepo interface {
    ListUsers(ctx context.Context) ([]User, error)
    GetUserByUsername(ctx context.Context, username string) (*User, error)
    CreateUser(ctx context.Context, u *User) error
    DeleteUser(ctx context.Context, id int) error
}

type ConnectorRepo interface {
    ListConnectors(ctx context.Context) ([]Connector, error)
    GetConnector(ctx context.Context, id int) (*Connector, error)
    CreateConnector(ctx context.Context, c *Connector) (int, error)
    UpdateConnector(ctx context.Context, c *Connector) error
    DeleteConnector(ctx context.Context, id int) error
}

type GroupRepo interface {
    CreateGroup(ctx context.Context, name string) (int, error)
    DeleteGroup(ctx context.Context, id int) error
    SetGroupMembers(ctx context.Context, groupID int, members []router.GroupMember) error
}

type RuleRepo interface {
    CreateRoutingRule(ctx context.Context, r router.Rule) (int, error)
    DeleteRoutingRule(ctx context.Context, id int) error
    UpdateRoutingRulePriority(ctx context.Context, id, priority int) error
}

type MessageFilter struct {
    TenantID    string
    Msisdn      string
    State       string
    ConnectorID int
    Limit       int
    Offset      int
}

type ConnectorCount struct {
    ConnectorID int
    Count       int
}

type StatsRepo interface {
    CountByState(ctx context.Context, tenantID string, since time.Time) (map[string]int, error)
    CountByConnector(ctx context.Context, tenantID string, since time.Time) ([]ConnectorCount, error)
    CountMessages(ctx context.Context, tenantID string, since time.Time) (int, error)
}
```

```go
// internal/store/store.go — ampliar MessageRepo (añadir el método a la interfaz existente)
type MessageRepo interface {
    // ... métodos M1/M3/M4 existentes ...
    ListMessages(ctx context.Context, f MessageFilter) ([]Message, int, error)
}
```

> `router` se importa en `store/store.go` (M2 ya lo hace en `pg.go`); añadir el import si falta.

- [ ] **Step 4: Implementación PG**

```go
// internal/store/pg.go — UserRepo/ConnectorRepo/GroupRepo/RuleRepo (añadir al final)
func (r *PGRepo) ListUsers(ctx context.Context) ([]User, error) {
    rows, err := r.pool.Query(ctx, `SELECT id, username, password_hash, role, COALESCE(tenant_id,''), created_at FROM users ORDER BY id`)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    users := []User{}
    for rows.Next() {
        var u User
        if err := rows.Scan(&u.ID, &u.Username, &u.PasswordHash, &u.Role, &u.TenantID, &u.CreatedAt); err != nil {
            return nil, err
        }
        users = append(users, u)
    }
    return users, rows.Err()
}

func (r *PGRepo) GetUserByUsername(ctx context.Context, username string) (*User, error) {
    var u User
    err := r.pool.QueryRow(ctx,
        `SELECT id, username, password_hash, role, COALESCE(tenant_id,''), created_at FROM users WHERE username=$1`,
        username).
        Scan(&u.ID, &u.Username, &u.PasswordHash, &u.Role, &u.TenantID, &u.CreatedAt)
    if errors.Is(err, pgx.ErrNoRows) {
        return nil, ErrNotFound
    }
    if err != nil {
        return nil, err
    }
    return &u, nil
}

func (r *PGRepo) CreateUser(ctx context.Context, u *User) error {
    return r.pool.QueryRow(ctx,
        `INSERT INTO users (username, password_hash, role, tenant_id) VALUES ($1,$2,$3,$4) RETURNING id, created_at`,
        u.Username, u.PasswordHash, u.Role, nullIfEmpty(u.TenantID)).
        Scan(&u.ID, &u.CreatedAt)
}

func (r *PGRepo) DeleteUser(ctx context.Context, id int) error {
    tag, err := r.pool.Exec(ctx, `DELETE FROM users WHERE id=$1`, id)
    if err != nil {
        return err
    }
    if tag.RowsAffected() == 0 {
        return ErrNotFound
    }
    return nil
}

func (r *PGRepo) ListConnectors(ctx context.Context) ([]Connector, error) {
    rows, err := r.pool.Query(ctx, `SELECT `+connectorCols+` FROM connectors ORDER BY id`)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    return scanConnectors(rows)
}

func (r *PGRepo) GetConnector(ctx context.Context, id int) (*Connector, error) {
    rows, err := r.pool.Query(ctx, `SELECT `+connectorCols+` FROM connectors WHERE id=$1`, id)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    list, err := scanConnectors(rows)
    if err != nil {
        return nil, err
    }
    if len(list) == 0 {
        return nil, ErrNotFound
    }
    return &list[0], nil
}

func (r *PGRepo) CreateConnector(ctx context.Context, c *Connector) (int, error) {
    err := r.pool.QueryRow(ctx,
        `INSERT INTO connectors (name, type, host, port, system_id, password, bind_mode, source_addr,
         source_ton, source_npi, dest_ton, dest_npi, concurrency, max_message_per_second, enquire_link_interval, tls, enabled)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17) RETURNING id`,
        c.Name, c.Type, c.Host, c.Port, c.SystemID, c.Password, c.BindMode, c.SourceAddr,
        c.SourceTON, c.SourceNPI, c.DestTON, c.DestNPI, c.Concurrency, c.MaxMsgPerSec, c.EnquireLinkInterval, c.TLS, c.Enabled).
        Scan(&c.ID)
    return c.ID, err
}

func (r *PGRepo) UpdateConnector(ctx context.Context, c *Connector) error {
    tag, err := r.pool.Exec(ctx,
        `UPDATE connectors SET name=$2, type=$3, host=$4, port=$5, system_id=$6, password=$7, bind_mode=$8,
         source_addr=$9, source_ton=$10, source_npi=$11, dest_ton=$12, dest_npi=$13, concurrency=$14,
         max_message_per_second=$15, enquire_link_interval=$16, tls=$17, enabled=$18 WHERE id=$1`,
        c.ID, c.Name, c.Type, c.Host, c.Port, c.SystemID, c.Password, c.BindMode, c.SourceAddr,
        c.SourceTON, c.SourceNPI, c.DestTON, c.DestNPI, c.Concurrency, c.MaxMsgPerSec, c.EnquireLinkInterval, c.TLS, c.Enabled)
    if err != nil {
        return err
    }
    if tag.RowsAffected() == 0 {
        return ErrNotFound
    }
    return nil
}

func (r *PGRepo) DeleteConnector(ctx context.Context, id int) error {
    tag, err := r.pool.Exec(ctx, `DELETE FROM connectors WHERE id=$1`, id)
    if err != nil {
        return err
    }
    if tag.RowsAffected() == 0 {
        return ErrNotFound
    }
    return nil
}

const connectorCols = `id, name, type, host, port, system_id, password, bind_mode, source_addr,
    source_ton, source_npi, dest_ton, dest_npi, concurrency, max_message_per_second, enquire_link_interval, tls, enabled`

func scanConnectors(rows pgx.Rows) ([]Connector, error) {
    list := []Connector{}
    for rows.Next() {
        var c Connector
        if err := rows.Scan(&c.ID, &c.Name, &c.Type, &c.Host, &c.Port, &c.SystemID, &c.Password,
            &c.BindMode, &c.SourceAddr, &c.SourceTON, &c.SourceNPI, &c.DestTON, &c.DestNPI,
            &c.Concurrency, &c.MaxMsgPerSec, &c.EnquireLinkInterval, &c.TLS, &c.Enabled); err != nil {
            return nil, err
        }
        list = append(list, c)
    }
    return list, rows.Err()
}

func (r *PGRepo) CreateGroup(ctx context.Context, name string) (int, error) {
    var id int
    err := r.pool.QueryRow(ctx, `INSERT INTO groups (name) VALUES ($1) RETURNING id`, name).Scan(&id)
    return id, err
}

func (r *PGRepo) DeleteGroup(ctx context.Context, id int) error {
    tag, err := r.pool.Exec(ctx, `DELETE FROM groups WHERE id=$1`, id)
    if err != nil {
        return err
    }
    if tag.RowsAffected() == 0 {
        return ErrNotFound
    }
    return nil
}

func (r *PGRepo) SetGroupMembers(ctx context.Context, groupID int, members []router.GroupMember) error {
    tx, err := r.pool.Begin(ctx)
    if err != nil {
        return err
    }
    defer tx.Rollback(ctx)
    if _, err := tx.Exec(ctx, `DELETE FROM group_members WHERE group_id=$1`, groupID); err != nil {
        return err
    }
    for _, m := range members {
        if m.Weight < 1 {
            continue
        }
        if _, err := tx.Exec(ctx,
            `INSERT INTO group_members (group_id, connector_id, weight) VALUES ($1,$2,$3)`,
            groupID, m.ConnectorID, m.Weight); err != nil {
            return err
        }
    }
    return tx.Commit(ctx)
}

func (r *PGRepo) CreateRoutingRule(ctx context.Context, rule router.Rule) (int, error) {
    var id int
    err := r.pool.QueryRow(ctx,
        `INSERT INTO routing_rules (priority, tenant_id, "from", prefix, regex, routing_tag, connector_id, group_id)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id`,
        rule.Priority, nullIfEmpty(rule.TenantID), rule.From, rule.Prefix, rule.Regex, rule.RoutingTag,
        rule.ConnectorID, nullIntIfZero(rule.GroupID)).Scan(&id)
    return id, err
}

func (r *PGRepo) DeleteRoutingRule(ctx context.Context, id int) error {
    tag, err := r.pool.Exec(ctx, `DELETE FROM routing_rules WHERE id=$1`, id)
    if err != nil {
        return err
    }
    if tag.RowsAffected() == 0 {
        return ErrNotFound
    }
    return nil
}

func (r *PGRepo) UpdateRoutingRulePriority(ctx context.Context, id, priority int) error {
    tag, err := r.pool.Exec(ctx, `UPDATE routing_rules SET priority=$2 WHERE id=$1`, id, priority)
    if err != nil {
        return err
    }
    if tag.RowsAffected() == 0 {
        return ErrNotFound
    }
    return nil
}

func nullIfEmpty(v string) any {
    if v == "" {
        return nil
    }
    return v
}

func nullIntIfZero(v int) any {
    if v == 0 {
        return nil
    }
    return v
}
```

```go
// internal/store/pg.go — ListMessages + Stats (añadir al final)
func (r *PGRepo) ListMessages(ctx context.Context, f MessageFilter) ([]Message, int, error) {
    where, args := []string{}, []any{}
    add := func(cond string, v any) {
        args = append(args, v)
        where = append(where, fmt.Sprintf("%s=$%d", cond, len(args)))
    }
    if f.TenantID != "" {
        add("tenant_id", f.TenantID)
    }
    if f.Msisdn != "" {
        add("msisdn", f.Msisdn)
    }
    if f.State != "" {
        add("state", f.State)
    }
    if f.ConnectorID != 0 {
        add("connector_id", f.ConnectorID)
    }
    whereSQL := ""
    if len(where) > 0 {
        whereSQL = " WHERE " + strings.Join(where, " AND ")
    }
    var total int
    if err := r.pool.QueryRow(ctx, `SELECT count(*) FROM messages`+whereSQL, args...).Scan(&total); err != nil {
        return nil, 0, err
    }
    limit, offset := 50, 0
    if f.Limit > 0 {
        limit = f.Limit
    }
    if limit > 500 {
        limit = 500
    }
    if f.Offset > 0 {
        offset = f.Offset
    }
    q := `SELECT id, tenant_id, source_addr, msisdn, text, segments, connector_id, state, try_count,
             smsc_msgid, route_id, amount, created_at, updated_at FROM messages` + whereSQL +
        ` ORDER BY created_at DESC, id LIMIT $` + itoa(len(args)+1) + ` OFFSET $` + itoa(len(args)+2)
    args = append(args, limit, offset)
    rows, err := r.pool.Query(ctx, q, args...)
    if err != nil {
        return nil, 0, err
    }
    defer rows.Close()
    items := []Message{}
    for rows.Next() {
        var m Message
        var createdAt, updatedAt time.Time
        var routeID *int
        var amount *float64
        if err := rows.Scan(&m.ID, &m.TenantID, &m.SourceAddr, &m.Msisdn, &m.Text, &m.Segments,
            &m.ConnectorID, &m.State, &m.TryCount, &m.SmscMsgid, &routeID, &amount,
            &createdAt, &updatedAt); err != nil {
            return nil, 0, err
        }
        if routeID != nil {
            m.RouteID = *routeID
        }
        if amount != nil {
            m.Amount = *amount
        }
        m.CreatedAt, m.UpdatedAt = createdAt, updatedAt
        items = append(items, m)
    }
    return items, total, rows.Err()
}

func (r *PGRepo) CountByState(ctx context.Context, tenantID string, since time.Time) (map[string]int, error) {
    where, args := r.sinceAndTenant("", tenantID, since)
    rows, err := r.pool.Query(ctx, `SELECT state, count(*) FROM messages`+where.cond+` GROUP BY state`, where.args...)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    out := map[string]int{}
    for rows.Next() {
        var st string
        var n int
        if err := rows.Scan(&st, &n); err != nil {
            return nil, err
        }
        out[st] = n
    }
    return out, rows.Err()
}

func (r *PGRepo) CountByConnector(ctx context.Context, tenantID string, since time.Time) ([]ConnectorCount, error) {
    where, args := r.sinceAndTenant("", tenantID, since)
    rows, err := r.pool.Query(ctx, `SELECT connector_id, count(*) FROM messages`+where.cond+
        ` GROUP BY connector_id ORDER BY connector_id`, where.args...)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    out := []ConnectorCount{}
    for rows.Next() {
        var cc ConnectorCount
        if err := rows.Scan(&cc.ConnectorID, &cc.Count); err != nil {
            return nil, err
        }
        out = append(out, cc)
    }
    return out, rows.Err()
}

func (r *PGRepo) CountMessages(ctx context.Context, tenantID string, since time.Time) (int, error) {
    where, args := r.sinceAndTenant("", tenantID, since)
    var n int
    err := r.pool.QueryRow(ctx, `SELECT count(*) FROM messages`+where.cond, where.args...).Scan(&n)
    return n, err
}

type whereClause struct {
    cond string
    args []any
}

// sinceAndTenant arma "WHERE tenant_id=$1 AND created_at >= $2" (omitendo partes vacías/zero). El $1 de no pasar columna base se usa con prefijo "".
func (r *PGRepo) sinceAndTenant(base, tenantID string, since time.Time) whereClause {
    var conds []string
    var args []any
    if tenantID != "" {
        args = append(args, tenantID)
        conds = append(conds, "tenant_id = $"+itoa(len(args)))
    }
    if !since.IsZero() {
        args = append(args, since)
        conds = append(conds, "created_at >= $"+itoa(len(args)))
    }
    w := ""
    if len(conds) > 0 {
        w = " WHERE " + strings.Join(conds, " AND ")
    }
    return whereClause{cond: w, args: args}
}

func itoa(i int) string { return fmt.Sprintf("%d", i) }
```

> `itoa` puede colisionar con uno existente en el paquete: si `strconv.Itoa` ya se usa, preferir `strconv.Itoa` en los `$N` y definir `itoa` solo si no existe (grep antes de añadir).

- [ ] **Step 5: Implementación Memory**

```go
// internal/store/memory.go — añadir campos y métodos
// En la struct MemoryRepo añadir:
//   users []User
//   connectors []Connector
//   groups []router.Group
//   rules []router.Rule
//   nextConnectorID, nextGroupID, nextRuleID int
```

```go
// internal/store/memory.go — añadir al final
var _ UserRepo = (*MemoryRepo)(nil)
var _ ConnectorRepo = (*MemoryRepo)(nil)
var _ GroupRepo = (*MemoryRepo)(nil)
var _ RuleRepo = (*MemoryRepo)(nil)
var _ StatsRepo = (*MemoryRepo)(nil)

func (r *MemoryRepo) ListUsers(_ context.Context) ([]User, error) {
    r.mu.Lock()
    defer r.mu.Unlock()
    out := make([]User, len(r.users))
    copy(out, r.users)
    return out, nil
}

func (r *MemoryRepo) GetUserByUsername(_ context.Context, username string) (*User, error) {
    r.mu.Lock()
    defer r.mu.Unlock()
    for i := range r.users {
        if r.users[i].Username == username {
            u := r.users[i]
            return &u, nil
        }
    }
    return nil, ErrNotFound
}

func (r *MemoryRepo) CreateUser(_ context.Context, u *User) error {
    r.mu.Lock()
    defer r.mu.Unlock()
    if u == nil {
        return fmt.Errorf("user nil")
    }
    for _, x := range r.users {
        if x.Username == u.Username {
            return fmt.Errorf("username ya existe")
        }
    }
    r.nextID++
    u.ID = r.nextID
    u.CreatedAt = time.Now()
    r.users = append(r.users, *u)
    return nil
}

func (r *MemoryRepo) DeleteUser(_ context.Context, id int) error {
    r.mu.Lock()
    defer r.mu.Unlock()
    for i := range r.users {
        if r.users[i].ID == id {
            r.users = append(r.users[:i], r.users[i+1:]...)
            return nil
        }
    }
    return ErrNotFound
}

func (r *MemoryRepo) ListConnectors(_ context.Context) ([]Connector, error) {
    r.mu.Lock()
    defer r.mu.Unlock()
    out := make([]Connector, len(r.connectors))
    copy(out, r.connectors)
    return out, nil
}

func (r *MemoryRepo) GetConnector(_ context.Context, id int) (*Connector, error) {
    r.mu.Lock()
    defer r.mu.Unlock()
    for i := range r.connectors {
        if r.connectors[i].ID == id {
            c := r.connectors[i]
            return &c, nil
        }
    }
    return nil, ErrNotFound
}

func (r *MemoryRepo) CreateConnector(_ context.Context, c *Connector) (int, error) {
    r.mu.Lock()
    defer r.mu.Unlock()
    r.nextConnectorID++
    c.ID = r.nextConnectorID
    r.connectors = append(r.connectors, *c)
    return c.ID, nil
}

func (r *MemoryRepo) UpdateConnector(_ context.Context, c *Connector) error {
    r.mu.Lock()
    defer r.mu.Unlock()
    for i := range r.connectors {
        if r.connectors[i].ID == c.ID {
            r.connectors[i] = *c
            return nil
        }
    }
    return ErrNotFound
}

func (r *MemoryRepo) DeleteConnector(_ context.Context, id int) error {
    r.mu.Lock()
    defer r.mu.Unlock()
    for i := range r.connectors {
        if r.connectors[i].ID == id {
            r.connectors = append(r.connectors[:i], r.connectors[i+1:]...)
            return nil
        }
    }
    return ErrNotFound
}

func (r *MemoryRepo) CreateGroup(_ context.Context, name string) (int, error) {
    r.mu.Lock()
    defer r.mu.Unlock()
    r.nextGroupID++
    r.groups = append(r.groups, router.Group{ID: r.nextGroupID, Name: name})
    return r.nextGroupID, nil
}

func (r *MemoryRepo) DeleteGroup(_ context.Context, id int) error {
    r.mu.Lock()
    defer r.mu.Unlock()
    for i := range r.groups {
        if r.groups[i].ID == id {
            r.groups = append(r.groups[:i], r.groups[i+1:]...)
            return nil
        }
    }
    return ErrNotFound
}

func (r *MemoryRepo) SetGroupMembers(_ context.Context, groupID int, members []router.GroupMember) error {
    r.mu.Lock()
    defer r.mu.Unlock()
    for i := range r.groups {
        if r.groups[i].ID == groupID {
            r.groups[i].Members = append([]router.GroupMember(nil), members...)
            return nil
        }
    }
    return ErrNotFound
}

func (r *MemoryRepo) CreateRoutingRule(_ context.Context, rule router.Rule) (int, error) {
    r.mu.Lock()
    defer r.mu.Unlock()
    r.nextRuleID++
    rule.ID = r.nextRuleID
    r.rules = append(r.rules, rule)
    return rule.ID, nil
}

func (r *MemoryRepo) DeleteRoutingRule(_ context.Context, id int) error {
    r.mu.Lock()
    defer r.mu.Unlock()
    for i := range r.rules {
        if r.rules[i].ID == id {
            r.rules = append(r.rules[:i], r.rules[i+1:]...)
            return nil
        }
    }
    return ErrNotFound
}

func (r *MemoryRepo) UpdateRoutingRulePriority(_ context.Context, id, priority int) error {
    r.mu.Lock()
    defer r.mu.Unlock()
    for i := range r.rules {
        if r.rules[i].ID == id {
            r.rules[i].Priority = priority
            return nil
        }
    }
    return ErrNotFound
}

func (r *MemoryRepo) ListMessages(_ context.Context, f MessageFilter) ([]Message, int, error) {
    r.mu.Lock()
    defer r.mu.Unlock()
    var matched []Message
    for _, m := range r.messages {
        if f.TenantID != "" && m.TenantID != f.TenantID {
            continue
        }
        if f.Msisdn != "" && !strings.Contains(m.Msisdn, f.Msisdn) {
            continue
        }
        if f.State != "" && m.State != f.State {
            continue
        }
        if f.ConnectorID != 0 && m.ConnectorID != f.ConnectorID {
            continue
        }
        matched = append(matched, m)
    }
    // ordenar por created_at desc
    sort.SliceStable(matched, func(i, j int) bool { return matched[i].CreatedAt.After(matched[j].CreatedAt) })
    total := len(matched)
    limit, offset := 50, 0
    if f.Limit > 0 {
        limit = f.Limit
    }
    if limit > 500 {
        limit = 500
    }
    if f.Offset > 0 {
        offset = f.Offset
    }
    if offset > total {
        offset = total
    }
    end := offset + limit
    if end > total {
        end = total
    }
    items := append([]Message(nil), matched[offset:end]...)
    return items, total, nil
}

func (r *MemoryRepo) CountByState(_ context.Context, tenantID string, since time.Time) (map[string]int, error) {
    r.mu.Lock()
    defer r.mu.Unlock()
    out := map[string]int{}
    for _, m := range r.messages {
        if tenantID != "" && m.TenantID != tenantID {
            continue
        }
        if !since.IsZero() && m.CreatedAt.Before(since) {
            continue
        }
        out[m.State]++
    }
    return out, nil
}

func (r *MemoryRepo) CountByConnector(_ context.Context, tenantID string, since time.Time) ([]ConnectorCount, error) {
    r.mu.Lock()
    defer r.mu.Unlock()
    agg := map[int]int{}
    for _, m := range r.messages {
        if tenantID != "" && m.TenantID != tenantID {
            continue
        }
        if !since.IsZero() && m.CreatedAt.Before(since) {
            continue
        }
        agg[m.ConnectorID]++
    }
    out := []ConnectorCount{}
    for id, n := range agg {
        out = append(out, ConnectorCount{ConnectorID: id, Count: n})
    }
    sort.Slice(out, func(i, j int) bool { return out[i].ConnectorID < out[j].ConnectorID })
    return out, nil
}

func (r *MemoryRepo) CountMessages(_ context.Context, tenantID string, since time.Time) (int, error) {
    r.mu.Lock()
    defer r.mu.Unlock()
    n := 0
    for _, m := range r.messages {
        if tenantID != "" && m.TenantID != tenantID {
            continue
        }
        if !since.IsZero() && m.CreatedAt.Before(since) {
            continue
        }
        n++
    }
    return n, nil
}
```

> `MemoryRepo.messages` y `r.mu` ya existen de M1/M3. `strings`/`sort` se añaden a imports de `memory.go` (revisar que no haya conflicto con `itoa` local).

- [ ] **Step 6: Run test to verify it passes**

Run: `go test ./internal/store/ -v`
Expected: PASS (tests previos del store + los nuevos Memory/List/Stats).

- [ ] **Step 7: Commit**

```bash
git add internal/store
git commit -m "feat: store repos admin (users, connectors, groups, rules, list, stats)"
```

---

### Task 3: Paquete `auth` — JWT HS256 + password KDF + RBAC

**Files:**
- Create: `internal/auth/jwt.go`, `internal/auth/jwt_test.go`
- Create: `internal/auth/passwd.go`, `internal/auth/passwd_test.go`
- Test: `go test ./internal/auth/ -v`

**Interfaces:**
- Consumes: stdlib `crypto/hmac`, `crypto/rand`, `crypto/sha256`, `crypto/subtle`, `encoding/base64`, `encoding/hex`, `encoding/json`, `errors`, `fmt`, `strings`, `time` (sin dependencias nuevas).
- Produces:
  - `auth.Claims{Username, Role string; Exp int64}`; `auth.Sign(secret []byte, c Claims) (string, error)`; `auth.Verify(secret []byte, token string) (Claims, error)` (errores `ErrUnauthorized` si token inválido, `ErrExpired` si `exp` pasó). JWT HS256: header `{"alg":"HS256","typ":"JWT"}`, payload CEJ-parity de `Claims`, firma HMAC-SHA256.
  - `auth.HashPassword(pw string) (string, error)` → `"iterations:salt_hex:hash_hex"` con HMAC-SHA256-iter (100000) y salt de 16 bytes; `auth.VerifyPassword(stored, pw string) bool`.
  - `auth.RoleRank` map y `auth.RequireRole(role, min string) bool` (`superadmin`> `admin`> `viewer`).

- [ ] **Step 1: Escribir los tests**

```go
// internal/auth/jwt_test.go
package auth

import (
    "testing"
    "time"
)

func TestSignVerify(t *testing.T) {
    secret := []byte("secret-para-tests")
    tok, err := Sign(secret, Claims{Username: "admin", Role: "superadmin", Exp: time.Now().Add(time.Hour).Unix()})
    if err != nil || tok == "" {
        t.Fatalf("tok=%q err=%v", tok, err)
    }
    c, err := Verify(secret, tok)
    if err != nil || c.Username != "admin" || c.Role != "superadmin" {
        t.Fatalf("c=%+v err=%v", c, err)
    }
}

func TestVerifyErrors(t *testing.T) {
    secret := []byte("s")
    tok, _ := Sign(secret, Claims{Username: "a", Role: "viewer", Exp: time.Now().Add(time.Hour).Unix()})
    if _, err := Verify([]byte("otro-secret"), tok); err == nil {
        t.Fatal("esperaba error por firma invalida")
    }
    bad, _ := Sign(secret, Claims{Username: "a", Role: "viewer", Exp: time.Now().Add(-time.Hour).Unix()})
    if _, err := Verify(secret, bad); err != ErrExpired {
        t.Fatalf("esperaba ErrExpired, got %v", err)
    }
    if _, err := Verify(secret, "no-es-un-jwt"); err != ErrUnauthorized {
        t.Fatalf("esperaba ErrUnauthorized, got %v", err)
    }
    if _, err := Verify(secret, tok+".extra"); err != ErrUnauthorized {
        t.Fatalf("esperaba ErrUnauthorized (3 partes), got %v", err)
    }
}

func TestRoleRank(t *testing.T) {
    if !RequireRole("superadmin", "admin") {
        t.Fatal("superadmin deberia pasar admin")
    }
    if RequireRole("viewer", "admin") {
        t.Fatal("viewer no deberia pasar admin")
    }
    if !RequireRole("admin", "admin") {
        t.Fatal("admin deberia pasar admin")
    }
    if !RequireRole("admin", "viewer") {
        t.Fatal("admin deberia pasar viewer")
    }
    if RequireRole("viewer", "superadmin") {
        t.Fatal("viewer no deberia pasar superadmin")
    }
}
```

```go
// internal/auth/passwd_test.go
package auth

import "testing"

func TestHashAndVerify(t *testing.T) {
    h, err := HashPassword("s3cret")
    if err != nil {
        t.Fatal(err)
    }
    if !VerifyPassword(h, "s3cret") {
        t.Fatal("password correcta deberia verificar")
    }
    if VerifyPassword(h, "incorrecta") {
        t.Fatal("password incorrecta no deberia verificar")
    }
    if VerifyPassword("no-valido", "x") {
        t.Fatal("hash corrupto no deberia verificar")
    }
    h1, _ := HashPassword("s3cret")
    h2, _ := HashPassword("s3cret")
    if h1 == h2 {
        t.Fatal("dos hashes de la misma password no deberian coincidir (salt aleatorio)")
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/auth/`
Expected: FAIL — package no existe.

- [ ] **Step 3: Implementación JWT**

```go
// internal/auth/jwt.go
package auth

import (
    "crypto/hmac"
    "crypto/sha256"
    "crypto/subtle"
    "encoding/base64"
    "encoding/json"
    "errors"
    "fmt"
    "strings"
    "time"
)

var (
    ErrUnauthorized = errors.New("token invalido")
    ErrExpired      = errors.New("token expirado")
)

type Claims struct {
    Username string `json:"username"`
    Role     string `json:"role"`
    Exp      int64  `json:"exp"`
}

func b64e(b []byte) string {
    return base64.RawURLEncoding.EncodeToString(b)
}

func b64d(s string) ([]byte, error) {
    return base64.RawURLEncoding.DecodeString(s)
}

func Sign(secret []byte, c Claims) (string, error) {
    header := b64e([]byte(`{"alg":"HS256","typ":"JWT"}`))
    payload, err := json.Marshal(c)
    if err != nil {
        return "", err
    }
    body := header + "." + b64e(payload)
    mac := hmac.New(sha256.New, secret)
    mac.Write([]byte(body))
    return body + "." + b64e(mac.Sum(nil)), nil
}

func Verify(secret []byte, token string) (Claims, error) {
    parts := strings.Split(token, ".")
    if len(parts) != 3 {
        return Claims{}, ErrUnauthorized
    }
    sig, err := b64d(parts[2])
    if err != nil {
        return Claims{}, ErrUnauthorized
    }
    mac := hmac.New(sha256.New, secret)
    mac.Write([]byte(parts[0] + "." + parts[1]))
    if subtle.ConstantTimeCompare(sig, mac.Sum(nil)) != 1 {
        return Claims{}, ErrUnauthorized
    }
    payload, err := b64d(parts[1])
    if err != nil {
        return Claims{}, ErrUnauthorized
    }
    var c Claims
    if err := json.Unmarshal(payload, &c); err != nil {
        return Claims{}, ErrUnauthorized
    }
    if c.Exp != 0 && time.Now().Unix() >= c.Exp {
        return Claims{}, ErrExpired
    }
    return c, nil
}

var RoleRank = map[string]int{"viewer": 1, "admin": 2, "superadmin": 3}

// RequireRole devuelve true si role tiene rango >= min.
func RequireRole(role, min string) bool {
    return RoleRank[role] >= RoleRank[min]
}
```

- [ ] **Step 4: Implementación password KDF**

```go
// internal/auth/passwd.go
package auth

import (
    "crypto/hmac"
    "crypto/rand"
    "crypto/sha256"
    "crypto/subtle"
    "encoding/hex"
    "fmt"
    "strconv"
    "strings"
)

const hashIterations = 100000

// HashPassword produce "iter:salt_hex:hash_hex". HMAC-SHA256 encadenado iter veces.
func HashPassword(pw string) (string, error) {
    salt := make([]byte, 16)
    if _, err := rand.Read(salt); err != nil {
        return "", err
    }
    h := hmac.New(sha256.New, salt)
    h.Write([]byte(pw))
    cur := h.Sum(nil)
    for i := 1; i < hashIterations; i++ {
        h = hmac.New(sha256.New, salt)
        h.Write(cur)
        cur = h.Sum(nil)
    }
    return fmt.Sprintf("%d:%s:%s", hashIterations, hex.EncodeToString(salt), hex.EncodeToString(cur)), nil
}

// VerifyPassword compara, ignorando el iter de almacenado (usa el del hash) para compatibilidad futura.
func VerifyPassword(stored, pw string) bool {
    parts := strings.Split(stored, ":")
    if len(parts) != 3 {
        return false
    }
    iters, err := strconv.Atoi(parts[0])
    if err != nil || iters < 1 {
        return false
    }
    salt, err := hex.DecodeString(parts[1])
    if err != nil || len(salt) == 0 {
        return false
    }
    want, err := hex.DecodeString(parts[2])
    if err != nil {
        return false
    }
    h := hmac.New(sha256.New, salt)
    h.Write([]byte(pw))
    cur := h.Sum(nil)
    for i := 1; i < iters; i++ {
        h = hmac.New(sha256.New, salt)
        h.Write(cur)
        cur = h.Sum(nil)
    }
    return subtle.ConstantTimeCompare(cur, want) == 1
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./internal/auth/ -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/auth
git commit -m "feat: auth package (jwt hs256, password kdf, roles)"
```

---

### Task 4: API admin — login, RBAC middleware, CRUD connectors/groups/routing-rules/users, búsqueda de mensajes y métricas (JSON + SSE)

**Files:**
- Modify: `internal/config/config.go` (`AdminUser`, `AdminPassword`, `JWTSecret`, `JWTTTL`, `MetricsInterval` + validación)
- Modify: `internal/api/server.go` (repos interface + campos del Server + mux con middleware + `handleLogin`, `withAuth`, `handleMetrics`, `handleMetricsStream`, `handleListMessages`)
- Create: `internal/api/admin_connectors.go`, `internal/api/admin_groups.go`, `internal/api/admin_rules.go`, `internal/api/admin_users.go`
- Modify: `internal/api/server_test.go` (tests de login/RBAC/métricas/búsqueda + CRUD)
- Modify: `cmd/smppgw/run.go` (único cambio: el api repite la firma; ver Task 5 para wiring completo)
- Test: `go test ./internal/api/ ./internal/config/ -v`

**Interfaces:**
- Consumes: `auth` (Task 3), `store` repos de Task 2, `config.Config` (M1 + este task), `api.repos` (M3/M4 con 5 interfaces — se amplía a 10).
- Produce:
  - `api.New(cfg, p, repo repos)` igual firma; `repos` = `{MessageRepo; WebhookRepo; TenantRepo; RateRepo; LedgerRepo; UserRepo; ConnectorRepo; GroupRepo; RuleRepo; StatsRepo}`.
  - Server con `jwtSecret []byte`, `jwtTTL time.Duration`, `metricsInterval time.Duration`, `users store.UserRepo`, `conns store.ConnectorRepo`, `groups store.GroupRepo`, `rules store.RuleRepo`, `stats store.StatsRepo`.
  - Nueva ruta `POST /api/v1/auth/login` `{username, password}` → `{token, username, role}` (401 si falla).
  - Middleware `(*Server).withAuth(min string, next http.HandlerFunc) http.HandlerFunc`: `Bearer` JWT válido, rol ≥ min (403 si no), `401` si sin token/vencido.
  - Rutas admin (envueltas con `withAuth`):
    - `GET /api/v1/admin/messages` (viewer) `?msisdn=&state=&tenant_id=&connector_id=&limit=&offset=` → `{total, items:[…]}` (JSON de message de M3 + `amount`, `route_id`).
    - `GET/POST /api/v1/admin/tenants`, `GET /{id}`, `POST /{id}/credit`, `GET /{id}/transactions` (viewer/admin, ya existentes → `withAuth`).
    - `GET/POST /api/v1/admin/rate-tables`, `GET/POST .../{id}/entries`, `DELETE /api/v1/admin/rate-entries/{id}` (viewer/admin → `withAuth`).
    - `GET/POST/PUT/DELETE /api/v1/admin/webhooks[ /{id}]` (viewer/admin → `withAuth`).
    - `GET /api/v1/admin/connectors` (viewer; password enmascarado `"********"`), `POST` (admin, `{name,type,host,port,system_id,password,bind_mode,...}`), `PUT /{id}` (admin; si `password` vacío o `"********"` conserva el actual), `DELETE /{id}` (admin), `POST /{id}/test` (admin; `{ok, detail}`).
    - `GET /api/v1/admin/groups` (viewer; con members), `POST` (admin `{name}` → `201 {id}`), `DELETE /{id}` (admin), `PUT /{id}/members` (admin `{members:[{connector_id,weight}]}`).
    - `GET /api/v1/admin/routing-rules` (viewer), `POST` (admin `{priority, tenant_id?, from?, prefix?, regex?, routing_tag?, connector_id?, group_id?}`), `DELETE /{id}` (admin), `PUT /{id}/priority` (admin `{priority}`).
    - `GET/POST /api/v1/admin/users` y `DELETE /{id}` (superadmin).
  - `GET /api/v1/metrics` (viewer) → `{by_state:{...}, by_connector:[{connector_id,count}], today, last_5min, generated_at}`; `GET /api/v1/metrics/stream` (viewer) SSE idéntico cada `metricsInterval` (default 2s), `data: {json}\n\n`.
  - `config.Config` nuevo: `AdminUser (SMG_ADMIN_USER, "admin")`, `AdminPassword (SMG_ADMIN_PASSWORD, "")`, `JWTSecret (SMG_JWT_SECRET, "")`, `JWTTTL (SMG_JWT_TTL, 24h)`, `MetricsInterval (SMG_METRICS_INTERVAL, 2s)`. `config.Load` falla con error si `Role=="server"` y `JWTSecret==""`.

- [ ] **Step 1: WIP tests springboard — config + jwt en server_test**

```go
// internal/config/config_test.go — añadir (ajustar a la estructura existente del archivo)
func TestServerRequieresJWTSecret(t *testing.T) {
    os.Setenv("SMG_ROLE", "server")
    os.Setenv("SMG_JWT_SECRET", "")
    defer os.Unsetenv("SMG_ROLE")
    defer os.Unsetenv("SMG_JWT_SECRET")
    if _, err := Load(); err == nil {
        t.Fatal("server sin SMG_JWT_SECRET deberia fallar")
    }
}
```

- [ ] **Step 2: Config — nuevos campos y validación**

```go
// internal/config/config.go — campos nuevos
    AdminUser        string
    AdminPassword    string
    JWTSecret        string
    JWTTTL           time.Duration
    MetricsInterval  time.Duration
```

```go
// internal/config/config.go — defaults
        JWTSecret:       os.Getenv("SMG_JWT_SECRET"),
        JWTTTL:          durOr("SMG_JWT_TTL", 24*time.Hour),
        MetricsInterval: durOr("SMG_METRICS_INTERVAL", 2*time.Second),
        AdminUser:       envOr("SMG_ADMIN_USER", "admin"),
        AdminPassword:   os.Getenv("SMG_ADMIN_PASSWORD"),
```

```go
// internal/config/config.go — en Load, tras validar SMG_ROLE
    if c.Role == "server" && c.JWTSecret == "" {
        return Config{}, fmt.Errorf("SMG_JWT_SECRET requerido para rol server")
    }
```

> `durOr` debe existir de M3 (duró load de `SMG_RECONCILE_*`); si se llama distinto (`envDuration`), usar el nombre real del archivo. Verificar con grep antes de aplicar.

- [ ] **Step 3: api.Server — repos, campos y mux con auth**

```go
// internal/api/server.go — repos interface (reemplazar)
type repos interface {
    store.MessageRepo
    store.WebhookRepo
    store.TenantRepo
    store.RateRepo
    store.LedgerRepo
    store.UserRepo
    store.ConnectorRepo
    store.GroupRepo
    store.RuleRepo
    store.StatsRepo
}
```

```go
// internal/api/server.go — struct Server: añadir campos
type Server struct {
    cfg     config.Config
    p       *pipeline.Pipeline
    repo    repos
    jwtSecret       []byte
    jwtTTL          time.Duration
    metricsInterval time.Duration
    users   store.UserRepo
    conns   store.ConnectorRepo
    groups  store.GroupRepo
    rules   store.RuleRepo
    stats   store.StatsRepo
}
```

```go
// internal/api/server.go — New (reemplazar el cuerpo asignado, manteniendo la firma)
func New(cfg config.Config, p *pipeline.Pipeline, repo repos) *Server {
    return &Server{
        cfg:             cfg,
        p:               p,
        repo:            repo,
        jwtSecret:       []byte(cfg.JWTSecret),
        jwtTTL:          cfg.JWTTTL,
        metricsInterval: cfg.MetricsInterval,
        users:   repo,
        conns:   repo,
        groups:  repo,
        rules:   repo,
        stats:   repo,
    }
}
```

```go
// internal/api/server.go — mux() (reemplazar): rutas admin con middleware y métricas
func (s *Server) mux() *http.ServeMux {
    mux := http.NewServeMux()
    mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
        w.WriteHeader(http.StatusOK)
        w.Write([]byte("ok"))
    })
    mux.HandleFunc("POST /api/v1/auth/login", s.handleLogin)
    mux.HandleFunc("POST /api/v1/messages", s.handleSubmit)
    mux.HandleFunc("GET /api/v1/messages/{id}", s.handleGetMessage)

    mux.HandleFunc("GET /api/v1/metrics", s.withAuth("viewer", s.handleMetrics))
    mux.HandleFunc("GET /api/v1/metrics/stream", s.withAuth("viewer", s.handleMetricsStream))

    mux.HandleFunc("GET /api/v1/admin/messages", s.withAuth("viewer", s.handleListMessages))

    mux.HandleFunc("GET /api/v1/admin/webhooks", s.withAuth("viewer", s.handleListWebhooks))
    mux.HandleFunc("POST /api/v1/admin/webhooks", s.withAuth("admin", s.handleCreateWebhook))
    mux.HandleFunc("PUT /api/v1/admin/webhooks/{id}", s.withAuth("admin", s.handleUpdateWebhook))
    mux.HandleFunc("DELETE /api/v1/admin/webhooks/{id}", s.withAuth("admin", s.handleDeleteWebhook))

    mux.HandleFunc("GET /api/v1/admin/tenants", s.withAuth("viewer", s.handleListTenants))
    mux.HandleFunc("POST /api/v1/admin/tenants", s.withAuth("admin", s.handleCreateTenant))
    mux.HandleFunc("GET /api/v1/admin/tenants/{id}", s.withAuth("viewer", s.handleGetTenant))
    mux.HandleFunc("POST /api/v1/admin/tenants/{id}/credit", s.withAuth("admin", s.handleCreditTenant))
    mux.HandleFunc("GET /api/v1/admin/tenants/{id}/transactions", s.withAuth("viewer", s.handleListTransactions))

    mux.HandleFunc("GET /api/v1/admin/rate-tables", s.withAuth("viewer", s.handleListRateTables))
    mux.HandleFunc("POST /api/v1/admin/rate-tables", s.withAuth("admin", s.handleCreateRateTable))
    mux.HandleFunc("GET /api/v1/admin/rate-tables/{id}/entries", s.withAuth("viewer", s.handleListRateEntries))
    mux.HandleFunc("POST /api/v1/admin/rate-tables/{id}/entries", s.withAuth("admin", s.handleCreateRateEntry))
    mux.HandleFunc("DELETE /api/v1/admin/rate-entries/{id}", s.withAuth("admin", s.handleDeleteRateEntry))

    mux.HandleFunc("GET /api/v1/admin/connectors", s.withAuth("viewer", s.handleListConnectors))
    mux.HandleFunc("POST /api/v1/admin/connectors", s.withAuth("admin", s.handleCreateConnector))
    mux.HandleFunc("PUT /api/v1/admin/connectors/{id}", s.withAuth("admin", s.handleUpdateConnector))
    mux.HandleFunc("DELETE /api/v1/admin/connectors/{id}", s.withAuth("admin", s.handleDeleteConnector))
    mux.HandleFunc("POST /api/v1/admin/connectors/{id}/test", s.withAuth("admin", s.handleTestConnector))

    mux.HandleFunc("GET /api/v1/admin/groups", s.withAuth("viewer", s.handleListGroups))
    mux.HandleFunc("POST /api/v1/admin/groups", s.withAuth("admin", s.handleCreateGroup))
    mux.HandleFunc("DELETE /api/v1/admin/groups/{id}", s.withAuth("admin", s.handleDeleteGroup))
    mux.HandleFunc("PUT /api/v1/admin/groups/{id}/members", s.withAuth("admin", s.handleSetGroupMembers))

    mux.HandleFunc("GET /api/v1/admin/routing-rules", s.withAuth("viewer", s.handleListRules))
    mux.HandleFunc("POST /api/v1/admin/routing-rules", s.withAuth("admin", s.handleCreateRule))
    mux.HandleFunc("DELETE /api/v1/admin/routing-rules/{id}", s.withAuth("admin", s.handleDeleteRule))
    mux.HandleFunc("PUT /api/v1/admin/routing-rules/{id}/priority", s.withAuth("admin", s.handleUpdateRulePriority))

    mux.HandleFunc("GET /api/v1/admin/users", s.withAuth("superadmin", s.handleListUsers))
    mux.HandleFunc("POST /api/v1/admin/users", s.withAuth("superadmin", s.handleCreateUser))
    mux.HandleFunc("DELETE /api/v1/admin/users/{id}", s.withAuth("superadmin", s.handleDeleteUser))
    return mux
}
```

- [ ] **Step 4: handleLogin, withAuth y utilidades**

```go
// internal/api/server.go — añadir
type loginReq struct {
    Username string `json:"username"`
    Password string `json:"password"`
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
    var req loginReq
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(w, "json invalido", http.StatusBadRequest)
        return
    }
    u, err := s.users.GetUserByUsername(r.Context(), req.Username)
    if err != nil || !auth.VerifyPassword(u.PasswordHash, req.Password) {
        http.Error(w, "credenciales invalidas", http.StatusUnauthorized)
        return
    }
    claims := auth.Claims{
        Username: u.Username,
        Role:     u.Role,
        Exp:      time.Now().Add(s.jwtTTL).Unix(),
    }
    token, err := auth.Sign(s.jwtSecret, claims)
    if err != nil {
        http.Error(w, "no se pudo generar token", http.StatusInternalServerError)
        return
    }
    writeJSON(w, 200, map[string]any{
        "token": token, "username": u.Username, "role": u.Role,
    })
}

func (s *Server) withAuth(minRole string, next http.HandlerFunc) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
        claims, err := auth.Verify(s.jwtSecret, token)
        if err != nil {
            http.Error(w, "no autorizado", http.StatusUnauthorized)
            return
        }
        if !auth.RequireRole(claims.Role, minRole) {
            http.Error(w, "permiso insuficiente", http.StatusForbidden)
            return
        }
        next(w, r)
    }
}

func writeJSON(w http.ResponseWriter, status int, v any) {
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(status)
    _ = json.NewEncoder(w).Encode(v)
}

func readID(r *http.Request) (int, error) {
    return strconv.Atoi(r.PathValue("id"))
}
```

- [ ] **Step 5: Búsqueda de mensajes + métricas + SSE**

```go
// internal/api/admin_messages.go — nuevo
package api

import (
    "fmt"
    "net/http"
    "strconv"
)

func (s *Server) handleListMessages(w http.ResponseWriter, r *http.Request) {
    q := r.URL.Query()
    limit, _ := strconv.Atoi(q.Get("limit"))
    offset, _ := strconv.Atoi(q.Get("offset"))
    f := store.MessageFilter{
        TenantID:    q.Get("tenant_id"),
        Msisdn:      q.Get("msisdn"),
        State:       q.Get("state"),
        ConnectorID: atoiOrZero(q.Get("connector_id")),
        Limit:       limit,
        Offset:      offset,
    }
    items, total, err := s.repo.ListMessages(r.Context(), f)
    if err != nil {
        http.Error(w, "no se pudo listar", http.StatusInternalServerError)
        return
    }
    writeJSON(w, 200, map[string]any{"total": total, "items": mapMessages(items)})
}

func atoiOrZero(v string) int {
    n, _ := strconv.Atoi(v)
    return n
}

func mapMessages(items []store.Message) []map[string]any {
    out := make([]map[string]any, 0, len(items))
    for _, m := range items {
        out = append(out, map[string]any{
            "id": m.ID, "tenant_id": m.TenantID, "source_addr": m.SourceAddr,
            "msisdn": m.Msisdn, "text": m.Text, "segments": m.Segments,
            "connector_id": m.ConnectorID, "route_id": m.RouteID, "state": m.State,
            "try_count": m.TryCount, "smsc_msgid": m.SmscMsgid, "amount": m.Amount,
            "created_at": m.CreatedAt, "updated_at": m.UpdatedAt,
        })
    }
    return out
}
```

```go
// internal/api/metrics.go — nuevo
package api

import (
    "context"
    "encoding/json"
    "fmt"
    "net/http"
    "time"

    "github.com/eskiconce/smpp-gateway/internal/store"
)

type metricsSnapshot struct {
    ByState      map[string]int      `json:"by_state"`
    ByConnector  []store.ConnectorCount `json:"by_connector"`
    Today        int                 `json:"today"`
    Last5Min     int                 `json:"last_5min"`
    GeneratedAt  time.Time           `json:"generated_at"`
}

func (s *Server) metricsSnapshot(ctx context.Context) (metricsSnapshot, error) {
    dayStart := time.Now().Truncate(24 * time.Hour)
    fiveMin := time.Now().Add(-5 * time.Minute)
    byState, err := s.stats.CountByState(ctx, "", dayStart)
    if err != nil {
        return metricsSnapshot{}, err
    }
    byConn, err := s.stats.CountByConnector(ctx, "", dayStart)
    if err != nil {
        return metricsSnapshot{}, err
    }
    today, err := s.stats.CountMessages(ctx, "", dayStart)
    if err != nil {
        return metricsSnapshot{}, err
    }
    last5, err := s.stats.CountMessages(ctx, "", fiveMin)
    if err != nil {
        return metricsSnapshot{}, err
    }
    return metricsSnapshot{
        ByState: byState, ByConnector: byConn,
        Today: today, Last5Min: last5, GeneratedAt: time.Now(),
    }, nil
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
    snap, err := s.metricsSnapshot(r.Context())
    if err != nil {
        http.Error(w, "no se pudieron calcular métricas", http.StatusInternalServerError)
        return
    }
    writeJSON(w, 200, snap)
}

func (s *Server) handleMetricsStream(w http.ResponseWriter, r *http.Request) {
    fl, ok := w.(http.Flusher)
    if !ok {
        http.Error(w, "streaming no soportado", http.StatusInternalServerError)
        return
    }
    w.Header().Set("Content-Type", "text/event-stream")
    w.Header().Set("Cache-Control", "no-cache")
    w.Header().Set("Connection", "keep-alive")
    ticker := time.NewTicker(s.metricsInterval)
    defer ticker.Stop()
    ctx := r.Context()
    for {
        snap, err := s.metricsSnapshot(ctx)
        if err != nil {
            fmt.Fprintf(w, "event: error\ndata: %v\n\n", err)
        } else {
            b, _ := json.Marshal(snap)
            fmt.Fprintf(w, "data: %s\n\n", b)
        }
        fl.Flush()
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
        }
    }
}
```

- [ ] **Step 6: CRUD connectors/groups/rules/users**

```go
// internal/api/admin_connectors.go — nuevo
package api

import (
    "encoding/json"
    "fmt"
    "net"
    "net/http"
    "strconv"
    "strings"
    "time"

    "github.com/eskiconce/smpp-gateway/internal/smpp"
    "github.com/eskiconce/smpp-gateway/internal/store"
)

const maskPassword = "********"

func (s *Server) handleListConnectors(w http.ResponseWriter, r *http.Request) {
    list, err := s.conns.ListConnectors(r.Context())
    if err != nil {
        http.Error(w, "no se pudo listar", http.StatusInternalServerError)
        return
    }
    out := make([]map[string]any, 0, len(list))
    for _, c := range list {
        c.Password = maskPassword
        out = append(out, connectorJSON(c))
    }
    writeJSON(w, 200, out)
}

func (s *Server) handleCreateConnector(w http.ResponseWriter, r *http.Request) {
    var c store.Connector
    if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
        http.Error(w, "json invalido", http.StatusBadRequest)
        return
    }
    if c.Name == "" || (c.Type != "smpp" && c.Type != "http") || c.Host == "" {
        http.Error(w, "name, type y host requeridos", http.StatusBadRequest)
        return
    }
    id, err := s.conns.CreateConnector(r.Context(), &c)
    if err != nil {
        http.Error(w, "no se pudo crear", http.StatusInternalServerError)
        return
    }
    writeJSON(w, 201, map[string]any{"id": id})
}

func (s *Server) handleUpdateConnector(w http.ResponseWriter, r *http.Request) {
    id, err := readID(r)
    if err != nil {
        http.Error(w, "id invalido", http.StatusBadRequest)
        return
    }
    current, err := s.conns.GetConnector(r.Context(), id)
    if err != nil {
        http.Error(w, "conector no encontrado", http.StatusNotFound)
        return
    }
    var c store.Connector
    if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
        http.Error(w, "json invalido", http.StatusBadRequest)
        return
    }
    if c.Password == "" || c.Password == maskPassword {
        c.Password = current.Password // conservar
    }
    c.ID = id
    if err := s.conns.UpdateConnector(r.Context(), &c); err != nil {
        http.Error(w, "no se pudo actualizar", http.StatusInternalServerError)
        return
    }
    writeJSON(w, 200, map[string]any{"id": id})
}

func (s *Server) handleDeleteConnector(w http.ResponseWriter, r *http.Request) {
    id, err := readID(r)
    if err != nil {
        http.Error(w, "id invalido", http.StatusBadRequest)
        return
    }
    if err := s.conns.DeleteConnector(r.Context(), id); err != nil {
        http.Error(w, "conector no encontrado", http.StatusNotFound)
        return
    }
    w.WriteHeader(204)
}

// handleTestConnector: smpp → dial + enquire_link; http → GET a la url con timeout 5s.
func (s *Server) handleTestConnector(w http.ResponseWriter, r *http.Request) {
    id, err := readID(r)
    if err != nil {
        http.Error(w, "id invalido", http.StatusBadRequest)
        return
    }
    c, err := s.conns.GetConnector(r.Context(), id)
    if err != nil {
        http.Error(w, "conector no encontrado", http.StatusNotFound)
        return
    }
    if !c.Enabled {
        writeJSON(w, 200, map[string]any{"ok": false, "detail": "conector deshabilitado"})
        return
    }
    var ok bool
    var detail string
    if c.Type == "http" {
        client := &http.Client{Timeout: 5 * time.Second}
        resp, err := client.Get("http://" + c.Host + "/")
        if err != nil {
            detail = "GET fallido: " + err.Error()
        } else {
            resp.Body.Close()
            ok = resp.StatusCode >= 200 && resp.StatusCode < 500
            detail = "HTTP " + resp.Status
        }
    } else {
        addr := net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
        conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
        if err != nil {
            detail = "dial fallido: " + err.Error()
        } else {
            defer conn.Close()
            _ = conn.SetDeadline(time.Now().Add(5 * time.Second))
            resp, err := testEnquire(conn)
            if err != nil {
                detail = "enquire_link fallido: " + err.Error()
            } else {
                ok = true
                detail = fmt.Sprintf("resp %d", resp)
            }
        }
    }
    writeJSON(w, 200, map[string]any{"ok": ok, "detail": detail})
}

func connectorJSON(c store.Connector) map[string]any {
    return map[string]any{
        "id": c.ID, "name": c.Name, "type": c.Type, "host": c.Host, "port": c.Port,
        "system_id": c.SystemID, "password": c.Password, "bind_mode": c.BindMode,
        "source_addr": c.SourceAddr, "source_ton": c.SourceTON, "source_npi": c.SourceNPI,
        "dest_ton": c.DestTON, "dest_npi": c.DestNPI, "concurrency": c.Concurrency,
        "max_msg_per_sec": c.MaxMsgPerSec, "enquire_link_interval": c.EnquireLinkInterval,
        "tls": c.TLS, "enabled": c.Enabled,
    }
}
```

```go
// internal/api/connector_test_helpers.go — dentro del paquete api (mismo archivo admin_connectors.go, añadir)
func testEnquire(nc net.Conn) (int, error) {
    pdu := smpp.PDU{Header: smpp.Head(1, smpp.EnquireLink)}
    if _, err := nc.Write(smpp.Encode(pdu)); err != nil {
        return 0, err
    }
    buf := make([]byte, 1024)
    n, err := nc.Read(buf)
    if err != nil {
        return 0, err
    }
    p, err := smpp.Decode(buf[:n])
    if err != nil {
        return 0, err
    }
    if p.Header.ID != smpp.EnquireLinkResp {
        return 0, fmt.Errorf("respuesta inesperada id=%d", p.Header.ID)
    }
    return int(p.Header.Status), nil
}
```

```go
// internal/api/admin_groups.go — nuevo
package api

import (
    "net/http"

    "github.com/eskiconce/smpp-gateway/internal/router"
)

func (s *Server) handleListGroups(w http.ResponseWriter, r *http.Request) {
    groups, err := s.groups.ListGroups(r.Context())
    if err != nil {
        http.Error(w, "no se pudo listar", http.StatusInternalServerError)
        return
    }
    out := make([]map[string]any, 0, len(groups))
    for _, g := range groups {
        members := make([]map[string]any, 0, len(g.Members))
        for _, m := range g.Members {
            members = append(members, map[string]any{"connector_id": m.ConnectorID, "weight": m.Weight})
        }
        out = append(out, map[string]any{"id": g.ID, "name": g.Name, "members": members})
    }
    writeJSON(w, 200, out)
}

func (s *Server) handleCreateGroup(w http.ResponseWriter, r *http.Request) {
    var req struct {
        Name string `json:"name"`
    }
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(w, "json invalido", http.StatusBadRequest)
        return
    }
    if req.Name == "" {
        http.Error(w, "name requerido", http.StatusBadRequest)
        return
    }
    id, err := s.groups.CreateGroup(r.Context(), req.Name)
    if err != nil {
        http.Error(w, "no se pudo crear", http.StatusInternalServerError)
        return
    }
    writeJSON(w, 201, map[string]any{"id": id})
}

func (s *Server) handleDeleteGroup(w http.ResponseWriter, r *http.Request) {
    id, err := readID(r)
    if err != nil {
        http.Error(w, "id invalido", http.StatusBadRequest)
        return
    }
    if err := s.groups.DeleteGroup(r.Context(), id); err != nil {
        http.Error(w, "grupo no encontrado", http.StatusNotFound)
        return
    }
    w.WriteHeader(204)
}

func (s *Server) handleSetGroupMembers(w http.ResponseWriter, r *http.Request) {
    id, err := readID(r)
    if err != nil {
        http.Error(w, "id invalido", http.StatusBadRequest)
        return
    }
    var req struct {
        Members []struct {
            ConnectorID int `json:"connector_id"`
            Weight      int `json:"weight"`
        } `json:"members"`
    }
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(w, "json invalido", http.StatusBadRequest)
        return
    }
    members := make([]router.GroupMember, 0, len(req.Members))
    for _, m := range req.Members {
        members = append(members, router.GroupMember{ConnectorID: m.ConnectorID, Weight: m.Weight})
    }
    if err := s.groups.SetGroupMembers(r.Context(), id, members); err != nil {
        http.Error(w, "grupo no encontrado", http.StatusNotFound)
        return
    }
    writeJSON(w, 200, map[string]any{"id": id})
}
```

```go
// internal/api/admin_rules.go — nuevo
package api

import (
    "net/http"

    "github.com/eskiconce/smpp-gateway/internal/router"
)

func (s *Server) handleListRules(w http.ResponseWriter, r *http.Request) {
    rules, err := s.rules.ListRoutingRules(r.Context())
    if err != nil {
        http.Error(w, "no se pudo listar", http.StatusInternalServerError)
        return
    }
    out := make([]map[string]any, 0, len(rules))
    for _, rule := range rules {
        out = append(out, ruleJSON(rule))
    }
    writeJSON(w, 200, out)
}

func (s *Server) handleCreateRule(w http.ResponseWriter, r *http.Request) {
    var req struct {
        Priority    int    `json:"priority"`
        TenantID    string `json:"tenant_id"`
        From        string `json:"from"`
        Prefix      string `json:"prefix"`
        Regex       string `json:"regex"`
        RoutingTag  string `json:"routing_tag"`
        ConnectorID int    `json:"connector_id"`
        GroupID     int    `json:"group_id"`
    }
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(w, "json invalido", http.StatusBadRequest)
        return
    }
    if req.ConnectorID == 0 && req.GroupID == 0 {
        http.Error(w, "se requiere connector_id o group_id", http.StatusBadRequest)
        return
    }
    id, err := s.rules.CreateRoutingRule(r.Context(), router.Rule{
        Priority: req.Priority, TenantID: req.TenantID, From: req.From,
        Prefix: req.Prefix, Regex: req.Regex, RoutingTag: req.RoutingTag,
        ConnectorID: req.ConnectorID, GroupID: req.GroupID,
    })
    if err != nil {
        http.Error(w, "no se pudo crear", http.StatusInternalServerError)
        return
    }
    writeJSON(w, 201, map[string]any{"id": id})
}

func (s *Server) handleDeleteRule(w http.ResponseWriter, r *http.Request) {
    id, err := readID(r)
    if err != nil {
        http.Error(w, "id invalido", http.StatusBadRequest)
        return
    }
    if err := s.rules.DeleteRoutingRule(r.Context(), id); err != nil {
        http.Error(w, "regla no encontrada", http.StatusNotFound)
        return
    }
    w.WriteHeader(204)
}

func (s *Server) handleUpdateRulePriority(w http.ResponseWriter, r *http.Request) {
    id, err := readID(r)
    if err != nil {
        http.Error(w, "id invalido", http.StatusBadRequest)
        return
    }
    var req struct {
        Priority int `json:"priority"`
    }
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(w, "json invalido", http.StatusBadRequest)
        return
    }
    if err := s.rules.UpdateRoutingRulePriority(r.Context(), id, req.Priority); err != nil {
        http.Error(w, "regla no encontrada", http.StatusNotFound)
        return
    }
    writeJSON(w, 200, map[string]any{"id": id, "priority": req.Priority})
}

func ruleJSON(rule router.Rule) map[string]any {
    return map[string]any{
        "id": rule.ID, "priority": rule.Priority,
        "tenant_id": rule.TenantID, "from": rule.From, "prefix": rule.Prefix,
        "regex": rule.Regex, "routing_tag": rule.RoutingTag,
        "connector_id": rule.ConnectorID, "group_id": rule.GroupID,
    }
}
```

```go
// internal/api/admin_users.go — nuevo
package api

import (
    "net/http"

    "github.com/eskiconce/smpp-gateway/internal/auth"
    "github.com/eskiconce/smpp-gateway/internal/store"
)

func (s *Server) handleListUsers(w http.ResponseWriter, r *http.Request) {
    list, err := s.users.ListUsers(r.Context())
    if err != nil {
        http.Error(w, "no se pudo listar", http.StatusInternalServerError)
        return
    }
    out := make([]map[string]any, 0, len(list))
    for _, u := range list {
        out = append(out, map[string]any{
            "id": u.ID, "username": u.Username, "role": u.Role,
            "tenant_id": u.TenantID, "created_at": u.CreatedAt,
        })
    }
    writeJSON(w, 200, out)
}

func (s *Server) handleCreateUser(w http.ResponseWriter, r *http.Request) {
    var req struct {
        Username string `json:"username"`
        Password string `json:"password"`
        Role     string `json:"role"`
        TenantID string `json:"tenant_id"`
    }
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(w, "json invalido", http.StatusBadRequest)
        return
    }
    if req.Username == "" || req.Password == "" {
        http.Error(w, "username y password requeridos", http.StatusBadRequest)
        return
    }
    if _, ok := auth.RoleRank[req.Role]; !ok {
        http.Error(w, "rol invalido (superadmin|admin|viewer)", http.StatusBadRequest)
        return
    }
    hash, err := auth.HashPassword(req.Password)
    if err != nil {
        http.Error(w, "no se pudo hashear", http.StatusInternalServerError)
        return
    }
    u := &store.User{Username: req.Username, PasswordHash: hash, Role: req.Role, TenantID: req.TenantID}
    if err := s.users.CreateUser(r.Context(), u); err != nil {
        http.Error(w, "no se pudo crear (username puede existir)", http.StatusConflict)
        return
    }
    writeJSON(w, 201, map[string]any{"id": u.ID})
}

func (s *Server) handleDeleteUser(w http.ResponseWriter, r *http.Request) {
    id, err := readID(r)
    if err != nil {
        http.Error(w, "id invalido", http.StatusBadRequest)
        return
    }
    if err := s.users.DeleteUser(r.Context(), id); err != nil {
        http.Error(w, "usuario no encontrado", http.StatusNotFound)
        return
    }
    w.WriteHeader(204)
}
```

- [ ] **Step 7: Tests API — login, RBAC, CRUD y métricas**

```go
// internal/api/server_test.go — añadir (el setup usa apiConfig() y store.NewMemory() de M3/M4)
func loginToken(t *testing.T, srv *Server, user, pass string) string {
    t.Helper()
    body := fmt.Sprintf(`{"username":%q,"password":%q}`, user, pass)
    req := httptest.NewRequest("POST", "/api/v1/auth/login", strings.NewReader(body))
    rr := httptest.NewRecorder()
    srv.ServeHTTP(rr, req)
    if rr.Code != 200 {
        t.Fatalf("login %d: %s", rr.Code, rr.Body.String())
    }
    var out struct {
        Token string `json:"token"`
    }
    if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
        t.Fatal(err)
    }
    return out.Token
}

func TestLogin(t *testing.T) {
    repo := store.NewMemory()
    _ = repo.CreateUser(context.Background(), &store.User{
        Username: "admin", PasswordHash: mustHash(t, "s3cret"), Role: "superadmin",
    })
    srv := apiFor(t, repo) // helper local: api.New(apiConfig(), pipelineStub(), repo)
    tok := loginToken(t, srv, "admin", "s3cret")
    if tok == "" {
        t.Fatal("token vacio")
    }
    req := httptest.NewRequest("POST", "/api/v1/auth/login", strings.NewReader(`{"username":"admin","password":"mala"}`))
    rr := httptest.NewRecorder()
    srv.ServeHTTP(rr, req)
    if rr.Code != 401 {
        t.Fatalf("login mal deberia dar 401, got %d", rr.Code)
    }
}

func TestRBACAdminEndpoints(t *testing.T) {
    repo := store.NewMemory()
    _ = repo.CreateUser(context.Background(), &store.User{
        Username: "viewer", PasswordHash: mustHash(t, "v"), Role: "viewer",
    })
    srv := apiFor(t, repo)

    // Sin token → 401
    req := httptest.NewRequest("GET", "/api/v1/admin/tenants", nil)
    rr := httptest.NewRecorder()
    srv.ServeHTTP(rr, req)
    if rr.Code != 401 {
        t.Fatalf("sin token deberia dar 401, got %d", rr.Code)
    }

    // Viewer en POST → 403
    body := strings.NewReader(`{"id":"t1","name":"T1"}`)
    req = httptest.NewRequest("POST", "/api/v1/admin/tenants", body)
    req.Header.Set("Authorization", "Bearer "+loginToken(t, srv, "viewer", "v"))
    rr = httptest.NewRecorder()
    srv.ServeHTTP(rr, req)
    if rr.Code != 403 {
        t.Fatalf("viewer escribiendo deberia dar 403, got %d", rr.Code)
    }

    // Viewer leyendo → 200
    req = httptest.NewRequest("GET", "/api/v1/admin/tenants", nil)
    req.Header.Set("Authorization", "Bearer "+loginToken(t, srv, "viewer", "v"))
    rr = httptest.NewRecorder()
    srv.ServeHTTP(rr, req)
    if rr.Code != 200 {
        t.Fatalf("viewer leyendo deberia dar 200, got %d", rr.Code)
    }
}

func TestConnectorsCRUD(t *testing.T) {
    repo := store.NewMemory()
    _ = repo.CreateUser(context.Background(), &store.User{
        Username: "admin", PasswordHash: mustHash(t, "a"), Role: "admin",
    })
    srv := apiFor(t, repo)
    tok := loginToken(t, srv, "admin", "a")

    post := func(path, body string) *httptest.ResponseRecorder {
        req := httptest.NewRequest("POST", path, strings.NewReader(body))
        req.Header.Set("Authorization", "Bearer "+tok)
        rr := httptest.NewRecorder()
        srv.ServeHTTP(rr, req)
        return rr
    }
    rr := post("/api/v1/admin/connectors", `{"name":"c1","type":"smpp","host":"h","port":2775}`)
    if rr.Code != 201 {
        t.Fatalf("create %d: %s", rr.Code, rr.Body.String())
    }
    req := httptest.NewRequest("GET", "/api/v1/admin/connectors", nil)
    req.Header.Set("Authorization", "Bearer "+tok)
    rr = httptest.NewRecorder()
    srv.ServeHTTP(rr, req)
    if rr.Code != 200 || !strings.Contains(rr.Body.String(), `"password":"`+maskPassword+`"`) {
        t.Fatalf("list %d: %s", rr.Code, rr.Body.String())
    }
    if rr := post("/api/v1/admin/connectors", `{"name":"x","type":"raro"}`); rr.Code != 400 {
        t.Fatalf("type invalido deberia dar 400, got %d", rr.Code)
    }
}

func TestRulesGroupsEndpoints(t *testing.T) {
    repo := store.NewMemory()
    _ = repo.CreateUser(context.Background(), &store.User{
        Username: "admin", PasswordHash: mustHash(t, "a"), Role: "admin",
    })
    srv := apiFor(t, repo)
    tok := loginToken(t, srv, "admin", "a")

    post := func(path, body string) *httptest.ResponseRecorder {
        req := httptest.NewRequest("POST", path, strings.NewReader(body))
        req.Header.Set("Authorization", "Bearer "+tok)
        rr := httptest.NewRecorder()
        srv.ServeHTTP(rr, req)
        return rr
    }
    rcg := post("/api/v1/admin/groups", `{"name":"g1"}`)
    if rcg.Code != 201 {
        t.Fatalf("group %d: %s", rcg.Code, rcg.Body.String())
    }
    var gid struct {
        ID int `json:"id"`
    }
    _ = json.Unmarshal(rcg.Body.Bytes(), &gid)

    cc := post("/api/v1/admin/connectors", `{"name":"c1","type":"smpp","host":"h","port":1}`)
    var cid struct {
        ID int `json:"id"`
    }
    _ = json.Unmarshal(cc.Body.Bytes(), &cid)

    rr := post(fmt.Sprintf("/api/v1/admin/groups/%d/members", gid.ID), `{"members":[{"connector_id":`+strconv.Itoa(cid.ID)+`,"weight":100}]}`)
    if rr.Code != 200 {
        t.Fatalf("members %d: %s", rr.Code, rr.Body.String())
    }

    body := `{"priority":1,"prefix":"569","group_id":` + strconv.Itoa(gid.ID) + `}`
    rc := post("/api/v1/admin/routing-rules", body)
    if rc.Code != 201 {
        t.Fatalf("rule %d: %s", rc.Code, rc.Body.String())
    }
    req := httptest.NewRequest("GET", "/api/v1/admin/routing-rules", nil)
    req.Header.Set("Authorization", "Bearer "+tok)
    rr = httptest.NewRecorder()
    srv.ServeHTTP(rr, req)
    if rr.Code != 200 || !strings.Contains(rr.Body.String(), `"prefix":"569"`) {
        t.Fatalf("rules %d: %s", rr.Code, rr.Body.String())
    }
}

func TestMetricsAndMessagesList(t *testing.T) {
    repo := store.NewMemory()
    _ = repo.CreateUser(context.Background(), &store.User{
        Username: "v", PasswordHash: mustHash(t, "v"), Role: "viewer",
    })
    for i := 0; i < 3; i++ {
        _ = repo.CreateMessage(context.Background(), &store.Message{
            ID: fmt.Sprintf("m%d", i), TenantID: "t1", Msisdn: "569x",
            State: "delivered", ConnectorID: 1, Segments: 1, Text: "hola",
        })
    }
    srv := apiFor(t, repo)
    tok := loginToken(t, srv, "v", "v")

    req := httptest.NewRequest("GET", "/api/v1/metrics", nil)
    req.Header.Set("Authorization", "Bearer "+tok)
    rr := httptest.NewRecorder()
    srv.ServeHTTP(rr, req)
    if rr.Code != 200 || !strings.Contains(rr.Body.String(), `"delivered":3`) {
        t.Fatalf("metrics %d: %s", rr.Code, rr.Body.String())
    }

    req = httptest.NewRequest("GET", "/api/v1/admin/messages?msisdn=569", nil)
    req.Header.Set("Authorization", "Bearer "+tok)
    rr = httptest.NewRecorder()
    srv.ServeHTTP(rr, req)
    if rr.Code != 200 || !strings.Contains(rr.Body.String(), `"total":3`) {
        t.Fatalf("messages %d: %s", rr.Code, rr.Body.String())
    }
}

func TestUsersEndpointsRequiereSuperadmin(t *testing.T) {
    repo := store.NewMemory()
    _ = repo.CreateUser(context.Background(), &store.User{
        Username: "admin", PasswordHash: mustHash(t, "a"), Role: "admin",
    })
    srv := apiFor(t, repo)
    tok := loginToken(t, srv, "admin", "a")

    req := httptest.NewRequest("GET", "/api/v1/admin/users", nil)
    req.Header.Set("Authorization", "Bearer "+tok)
    rr := httptest.NewRecorder()
    srv.ServeHTTP(rr, req)
    if rr.Code != 403 {
        t.Fatalf("admin pidiendo users deberia dar 403, got %d", rr.Code)
    }
}

func mustHash(t *testing.T, pw string) string {
    t.Helper()
    h, err := auth.HashPassword(pw)
    if err != nil {
        t.Fatal(err)
    }
    return h
}
```

- [ ] **Step 8: Test SSE (stream emite al menos un evento y termina con cancel)**

```go
// internal/api/metrics_test.go — nuevo
package api

import (
    "context"
    "net/http/httptest"
    "strings"
    "testing"
    "time"

    "github.com/eskiconce/smpp-gateway/internal/store"
)

func TestMetricsStreamEmiteEventos(t *testing.T) {
    repo := store.NewMemory()
    _ = repo.CreateUser(context.Background(), &store.User{
        Username: "v", PasswordHash: mustHash(t, "v"), Role: "viewer",
    })
    srv := apiFor(t, repo)
    srv.metricsInterval = 10 * time.Millisecond
    tok := loginToken(t, srv, "v", "v")

    req := httptest.NewRequest("GET", "/api/v1/metrics/stream", nil)
    req.Header.Set("Authorization", "Bearer "+tok)
    rr := httptest.NewRecorder()
    done := make(chan struct{})
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    req = req.WithContext(ctx)
    go func() {
        srv.ServeHTTP(rr, req)
        close(done)
    }()

    deadline := time.Now().Add(2 * time.Second)
    for time.Now().Before(deadline) {
        if strings.Contains(rr.Body.String(), "data:") {
            cancel()
            break
        }
        time.Sleep(5 * time.Millisecond)
    }
    <-done
    if !strings.Contains(rr.Body.String(), `"by_state"`) {
        t.Fatalf("SSE sin payload: %q", rr.Body.String())
    }
}
```

> Si el helper `apiFor` requiere un `pipeline` real, usar `newTestPipeline(t)` de M3/M4 o el `fakePipeline` de M5 — ver nota de la Task 4 Step 9. Si `api.New` pide `p *pipeline.Pipeline` no nulo para `handleSubmit`, usar un stub local que cumpla el campo (revisar tests M3/M4: todos crean `api.New` con un pipeline stub).

- [ ] **Step 9: Verificar tests previos y correr**

Run: `go test ./internal/api/ ./internal/config/ -v`
Expected: PASS. Los tests de M3/M4 de webhooks/tenants/rate-tables que invocaban rutas sin token ahora devolverán 401: **ajustar esos tests añadiendo login/Bearer** (helper `loginToken`). Igual para `handleSubmit` (sin token, sigue igual).

- [ ] **Step 10: Commit**

```bash
git add internal/api internal/config
git commit -m "feat: api admin (jwt rbac, crud connectors/groups/rules/users, messages list, metrics sse)"
```

---

### Task 5: Wiring `run.go` — repos, bootstrap superadmin, fix redis client M5

**Files:**
- Modify: `cmd/smppgw/run.go`
- Test: `go build ./...`, `make test`

**Interfaces:**
- Consumes: `api.New` (Task 4), `auth` (Task 3), `store.PGRepo` (Tasks 1-2), `config` (Task 4), M5 wiring (ESME).
- Produce:
  - `runServer` (firma ya actualizada desde M5: `runServer(ctx, cfg, repo, q) error`) hace:
    1. Bootstrap del superadmin: si `countUsers == 0` y `cfg.AdminPassword != ""`, `CreateUser{Username: cfg.AdminUser, PasswordHash: auth.HashPassword(cfg.AdminPassword), Role: "superadmin"}`; si no hay password, log warn y skip.
    2. Reemplaza el `redis.NewClient(&redis.Options{Addr: cfg.RedisURL})` de M5 por `redis.ParseURL` (el Addr no acepta URL completa; bug corregido).
    3. `api.New(cfg, p, repo)` ahora recibe el `repos` de 10 interfaces — `*store.PGRepo` lo satisface sin cambios en `run.go`.

- [ ] **Step 1: Modificar runServer**

```go
// cmd/smppgw/run.go — runServer (partes nuevas/resaltadas)
// 1) bootstrap superadmin, tras crear `repo`:
func ensureAdmin(ctx context.Context, cfg config.Config, repo *store.PGRepo) error {
    users, err := repo.ListUsers(ctx)
    if err != nil {
        return err
    }
    if len(users) > 0 {
        return nil
    }
    if cfg.AdminPassword == "" {
        slog.Warn("no hay usuarios y SMG_ADMIN_PASSWORD vacio; no se crea admin")
        return nil
    }
    hash, err := auth.HashPassword(cfg.AdminPassword)
    if err != nil {
        return err
    }
    return repo.CreateUser(ctx, &store.User{
        Username: cfg.AdminUser, PasswordHash: hash, Role: "superadmin",
    })
}
```

```go
// cmd/smppgw/run.go — dentro de runServer, tras `pg, err := store.NewPG(...)`:
    if err := ensureAdmin(ctx, cfg, pg); err != nil {
        slog.Error("admin bootstrap", "err", err)
        return err
    }
```

```go
// cmd/smppgw/run.go — reemplazar la construcción del cliente Redis de M5:
    opts, err := redis.ParseURL(cfg.RedisURL)
    if err != nil {
        return fmt.Errorf("redis url invalida: %w", err)
    }
    rdb := redis.NewClient(opts)
    defer rdb.Close()
```

> Imports nuevos en `run.go`: `github.com/eskiconce/smpp-gateway/internal/auth`, `github.com/eskiconce/smpp-gateway/internal/store` (si no estaban). Revisar que no se duplique `redis` (el `queue.NewRedis(cfg.RedisURL)` de M1 se mantiene).

- [ ] **Step 2: Verificar build**

Run: `go build ./...`
Expected: compila. El `repos` interface ya exige las 10 repos al llamar `api.New(cfg, p, repo)` — `*store.PGRepo` las implementa todas tras la Task 2. Si falla un test anterior de `cmd` que construía `api.New` con `store.NewMemory()`, ajustar con el helper de login (las rutas admin quedan detrás de auth).

- [ ] **Step 3: Correr tests**

Run: `make test`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add cmd/smppgw/run.go
git commit -m "feat: wire run.go admin bootstrap + fix redis parse url"
```

---

### Task 6: Frontend scaffold — Vite + React + TS + cliente API + login + hash routing

**Files:**
- Create: `web/package.json`, `web/tsconfig.json`, `web/vite.config.ts`, `web/index.html`, `web/src/main.tsx`, `web/src/api.ts`, `web/src/App.tsx`, `web/src/styles.css`, `web/src/vite-env.d.ts`
- Create: `web/src/app.test.ts` (vitest mínimo del router de hash)
- Test: `cd web && npm install && npm test && npm run build`

**Interfaces:**
- Consumes: API REST Task 4 (`/api/v1/auth/login`, `/api/v1/admin/*`, `/api/v1/metrics`). Base `"/api/v1"` (nginx proxya; en dev Vite usa proxy).
- Produce:
  - `web/src/api.ts`: `apiFetch(path, opts?)` wrapper con `Authorization: Bearer <token localStorage "smpp-token">`; en `401` borra token y redirige a `#/login`; expone `login(username,password)`.
  - `web/src/main.tsx`: monta `<App/>`.
  - `web/src/App.tsx`: hash router (`#/login`, `#/dashboard`, `#/messages`, `#/tenants`, `#/connectors`, `#/groups`, `#/rules`, `#/rates`, `#/webhooks`, `#/users`); sidebar; guard: sin token → redirige login.
  - `VITE_API_TARGET` (default `http://127.0.0.1:8080`) solo para el proxy de dev.

- [ ] **Step 1: package.json/tools**

```json
{
  "name": "smppgw-web",
  "private": true,
  "version": "1.0.0",
  "type": "module",
  "scripts": {
    "dev": "vite",
    "build": "tsc --noEmit && vite build",
    "test": "vitest run"
  },
  "dependencies": {
    "react": "^18.3.1",
    "react-dom": "^18.3.1"
  },
  "devDependencies": {
    "@types/react": "^18.3.3",
    "@types/react-dom": "^18.3.0",
    "@vitejs/plugin-react": "^4.3.1",
    "typescript": "^5.5.4",
    "vite": "^5.4.0",
    "vitest": "^2.0.5"
  }
}
```

```json
// web/tsconfig.json
{
  "compilerOptions": {
    "target": "ES2020",
    "lib": ["ES2020", "DOM", "DOM.Iterable"],
    "module": "ESNext",
    "moduleResolution": "bundler",
    "jsx": "react-jsx",
    "strict": true,
    "skipLibCheck": true,
    "noEmit": true,
    "types": ["vite/client"]
  },
  "include": ["src"]
}
```

```ts
// web/vite.config.ts
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

const proxyTarget = process.env.VITE_API_TARGET || 'http://127.0.0.1:8080';

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: { '/api': { target: proxyTarget, changeOrigin: true } },
  },
  build: { outDir: 'dist' },
  test: { environment: 'node' },
});
```

```html
<!-- web/index.html -->
<!doctype html>
<html lang="es">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>smppgw — Admin</title>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/main.tsx"></script>
  </body>
</html>
```

```ts
// web/src/vite-env.d.ts
/// <reference types="vite/client" />
```

- [ ] **Step 2: api.ts**

```ts
// web/src/api.ts
const BASE = '/api/v1';
const TOKEN_KEY = 'smpp-token';

export function getToken(): string {
  return localStorage.getItem(TOKEN_KEY) || '';
}

export function setToken(tok: string): void {
  localStorage.setItem(TOKEN_KEY, tok);
}

export function clearToken(): void {
  localStorage.removeItem(TOKEN_KEY);
}

export async function login(username: string, password: string): Promise<string> {
  const res = await fetch(`${BASE}/auth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username, password }),
  });
  if (!res.ok) throw new Error('Credenciales inválidas');
  const data = await res.json();
  setToken(data.token);
  return data.role as string;
}

export async function apiFetch<T = unknown>(path: string, opts: RequestInit = {}): Promise<T> {
  const headers: Record<string, string> = {
    ...(opts.headers as Record<string, string> | undefined),
  };
  const tok = getToken();
  if (tok) headers.Authorization = `Bearer ${tok}`;
  if (opts.body) headers['Content-Type'] = 'application/json';
  const res = await fetch(`${BASE}${path}`, { ...opts, headers });
  if (res.status === 401) {
    clearToken();
    location.hash = '#/login';
    throw new Error('Sesión expirada');
  }
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `Error ${res.status}`);
  }
  if (res.status === 204) return undefined as T;
  return (await res.json()) as T;
}
```

```ts
// web/src/main.tsx
import React from 'react';
import { createRoot } from 'react-dom/client';
import App from './App';
import './styles.css';

createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
```

```ts
// web/src/App.tsx
import { useEffect, useState } from 'react';
import { getToken } from './api';
import Login from './views/Login';
import Dashboard from './views/Dashboard';
import Messages from './views/Messages';
import Tenants from './views/Tenants';
import Connectors from './views/Connectors';
import Groups from './views/Groups';
import Rules from './views/Rules';
import Rates from './views/Rates';
import Webhooks from './views/Webhooks';
import Users from './views/Users';

const NAV = [
  { hash: '#/dashboard', label: 'Dashboard' },
  { hash: '#/messages', label: 'Mensajes' },
  { hash: '#/tenants', label: 'Tenants' },
  { hash: '#/connectors', label: 'Conectores' },
  { hash: '#/groups', label: 'Grupos' },
  { hash: '#/rules', label: 'Reglas de ruteo' },
  { hash: '#/rates', label: 'Tarifas' },
  { hash: '#/webhooks', label: 'Webhooks' },
  { hash: '#/users', label: 'Usuarios' },
];

export function currentRoute(): string {
  const h = location.hash || '';
  return h.startsWith('#/') ? h : '#/dashboard';
}

export default function App() {
  const [route, setRoute] = useState(currentRoute());

  useEffect(() => {
    const onHash = () => setRoute(currentRoute());
    window.addEventListener('hashchange', onHash);
    return () => window.removeEventListener('hashchange', onHash);
  }, []);

  if (!getToken()) return <Login />;

  let view;
  switch (route) {
    case '#/dashboard': view = <Dashboard />; break;
    case '#/messages': view = <Messages />; break;
    case '#/tenants': view = <Tenants />; break;
    case '#/connectors': view = <Connectors />; break;
    case '#/groups': view = <Groups />; break;
    case '#/rules': view = <Rules />; break;
    case '#/rates': view = <Rates />; break;
    case '#/webhooks': view = <Webhooks />; break;
    case '#/users': view = <Users />; break;
    default: view = <Dashboard />;
  }

  return (
    <div className="layout">
      <aside className="sidebar">
        <h1>smppgw</h1>
        <nav>
          {NAV.map((n) => (
            <a key={n.hash} href={n.hash} className={route === n.hash ? 'active' : ''}>
              {n.label}
            </a>
          ))}
          <a href="#/login" onClick={() => { clearToken(); }}>Salir</a>
        </nav>
      </aside>
      <main className="content">{view}</main>
    </div>
  );
}
```

> `clearToken` se importa desde `./api`. El `<a href="#/login">` de Salir limpia el token vía onClick.

- [ ] **Step 3: Login + test del router**

```tsx
// web/src/views/Login.tsx
import { useState } from 'react';
import { login } from '../api';

export default function Login() {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    try {
      await login(username, password);
      location.hash = '#/dashboard';
      location.reload(); // fuerza re-evaluacion del guard
    } catch (err) {
      setError((err as Error).message);
    }
  };

  return (
    <div className="login">
      <form onSubmit={submit}>
        <h1>smppgw — Admin</h1>
        <input value={username} onChange={(e) => setUsername(e.target.value)}
          placeholder="usuario" autoFocus />
        <input type="password" value={password} onChange={(e) => setPassword(e.target.value)}
          placeholder="contraseña" />
        {error && <p className="error">{error}</p>}
        <button type="submit">Entrar</button>
      </form>
    </div>
  );
}
```

```ts
// web/src/app.test.ts
import { describe, it, expect } from 'vitest';
import { currentRoute } from './App';

describe('hash routing', () => {
  it('defaults a dashboard', () => {
    expect(currentRoute()).toBe('#/dashboard');
  });
});
```

- [ ] **Step 4: CSS mínimo global**

```css
/* web/src/styles.css */
* { box-sizing: border-box; }
body { margin: 0; font-family: system-ui, sans-serif; color: #222; }
.layout { display: flex; min-height: 100vh; }
.sidebar { width: 220px; background: #1f2937; color: #fff; padding: 16px; }
.sidebar h1 { font-size: 18px; margin: 0 0 16px; }
.sidebar nav a { display: block; color: #d1d5db; text-decoration: none; padding: 8px; }
.sidebar nav a.active, .sidebar nav a:hover { background: #374151; color: #fff; }
.content { flex: 1; padding: 24px; }
.login { display: flex; height: 100vh; align-items: center; justify-content: center; }
.login form { display: flex; flex-direction: column; width: 280px; gap: 8px; }
input, button, select { padding: 8px; font-size: 14px; }
table { border-collapse: collapse; width: 100%; }
th, td { border: 1px solid #e5e7eb; padding: 6px 8px; text-align: left; }
.error { color: #dc2626; }
.card { border: 1px solid #e5e7eb; border-radius: 8px; padding: 12px; margin-bottom: 16px; }
.card h2 { margin-top: 0; }
.toolbar { display: flex; gap: 8px; margin-bottom: 12px; flex-wrap: wrap; }
.kpis { display: flex; gap: 16px; margin-bottom: 16px; flex-wrap: wrap; }
.kpi { flex: 1; min-width: 140px; background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 8px; padding: 12px; }
.kpi b { font-size: 24px; display: block; }
.drag-row { cursor: grab; }
```

- [ ] **Step 5: Correr tests + build**

Run: `cd web && npm install && npm test && npm run build`
Expected: 1 test PASS; build sin errores TS (`dist/index.html` generado).

- [ ] **Step 6: Commit**

```bash
git add web
git commit -m "feat: web scaffold (vite react ts, api client, login, hash router)"
```

---

### Task 7: Frontend — Dashboard (métricas + SSE) y búsqueda de mensajes

**Files:**
- Create: `web/src/views/Dashboard.tsx`, `web/src/views/Messages.tsx`
- Test: `cd web && npm run build`; revisión manual contra `curl` de Task 4

**Interfaces:**
- Consumes: `GET /api/v1/metrics` y `EventSource` (`/api/v1/metrics/stream`, JWT en query header no soportado por EventSource → añadir `?token=` fallback aceptado por backend como query param; si no se implementa, Dashboard usa polling de `GET /api/v1/metrics` cada 2s — **decisión tomada**: polling, sin SSE en el navegador; el SSE queda como endpoint de infraestructura). `GET /api/v1/admin/messages?...`.
- Produce: vistas `Dashboard` y `Messages` multilineales con estado.

- [ ] **Step 1: Dashboard**

```tsx
// web/src/views/Dashboard.tsx
import { useEffect, useState } from 'react';
import { apiFetch } from '../api';

interface Snapshot {
  by_state: Record<string, number>;
  by_connector: { connector_id: number; count: number }[];
  today: number;
  last_5min: number;
  generated_at: string;
}

export default function Dashboard() {
  const [snap, setSnap] = useState<Snapshot | null>(null);
  const [error, setError] = useState('');

  useEffect(() => {
    let alive = true;
    const load = async () => {
      try {
        const s = await apiFetch<Snapshot>('/metrics');
        if (alive) { setSnap(s); setError(''); }
      } catch (err) {
        if (alive) setError((err as Error).message);
      }
    };
    load();
    const id = window.setInterval(load, 2000);
    return () => { alive = false; window.clearInterval(id); };
  }, []);

  const stateOrder = ['delivered', 'undeliv', 'expired', 'rejected', 'buffered', 'accepted', 'failed', 'pending'];

  return (
    <div>
      <h1>Dashboard</h1>
      {error && <p className="error">{error}</p>}
      {snap && (
        <>
          <div className="kpis">
            <div className="kpi"><b>{snap.today}</b>Enviados hoy</div>
            <div className="kpi"><b>{snap.last_5min}</b>Últimos 5 min</div>
            <div className="kpi"><b>{snap.by_state['delivered'] ?? 0}</b>Entregados (DELIVRD)</div>
            <div className="kpi"><b>{snap.by_state['undeliv'] ?? 0}</b>No entregados</div>
          </div>
          <div className="card">
            <h2>Estado</h2>
            <table>
              <thead><tr><th>Estado</th><th>Cantidad</th></tr></thead>
              <tbody>
                {stateOrder.filter((s) => (snap.by_state[s] ?? 0) > 0).map((s) => (
                  <tr key={s}><td>{s}</td><td>{snap.by_state[s]}</td></tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="card">
            <h2>Por conector</h2>
            <table>
              <thead><tr><th>Conector</th><th>Enviados hoy</th></tr></thead>
              <tbody>
                {snap.by_connector.map((c) => (
                  <tr key={c.connector_id}><td>{c.connector_id}</td><td>{c.count}</td></tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}
```

> Decisión explicada: `EventSource` no admite headers; el SSE del backend se consume por infraestructura (jq/curl con token), y el Dashboard usa polling JSON cada 2s. Se evita exponer el token como query string.

- [ ] **Step 2: Búsqueda de mensajes**

```tsx
// web/src/views/Messages.tsx
import { useEffect, useState } from 'react';
import { apiFetch } from '../api';

interface Msg {
  id: string; tenant_id?: string; source_addr: string; msisdn: string;
  text: string; segments: number; connector_id?: number; state: string;
  try_count?: number; smsc_msgid?: string; amount?: number;
  created_at: string; updated_at?: string;
}

export default function Messages() {
  const [filters, setFilters] = useState({ msisdn: '', state: '', limit: '25' });
  const [total, setTotal] = useState(0);
  const [items, setItems] = useState<Msg[]>([]);
  const [error, setError] = useState('');

  const search = async (offset = 0) => {
    setError('');
    try {
      const params = new URLSearchParams();
      if (filters.msisdn) params.set('msisdn', filters.msisdn);
      if (filters.state) params.set('state', filters.state);
      params.set('limit', filters.limit || '25');
      params.set('offset', String(offset));
      const data = await apiFetch<{ total: number; items: Msg[] }>(`/admin/messages?${params}`);
      setItems(data.items);
      setTotal(data.total);
    } catch (err) {
      setError((err as Error).message);
    }
  };

  useEffect(() => { void search(0); }, []);

  return (
    <div>
      <h1>Mensajes</h1>
      <div className="toolbar">
        <input value={filters.msisdn} placeholder="msisdn"
          onChange={(e) => setFilters({ ...filters, msisdn: e.target.value })} />
        <input value={filters.state} placeholder="estado (ej: DELIVRD)"
          onChange={(e) => setFilters({ ...filters, state: e.target.value })} />
        <select value={filters.limit}
          onChange={(e) => setFilters({ ...filters, limit: e.target.value })}>
          <option value="10">10</option>
          <option value="25">25</option>
          <option value="50">50</option>
        </select>
        <button onClick={() => void search(0)}>Buscar</button>
      </div>
      <p>Total: {total}</p>
      {error && <p className="error">{error}</p>}
      <table>
        <thead>
          <tr><th>ID</th><th>Tenant</th><th>MSISDN</th><th>Estado</th>
              <th>Seg</th><th>Conector</th><th>SmscMsgid</th><th>Fecha</th></tr>
        </thead>
        <tbody>
          {items.map((m) => (
            <tr key={m.id}>
              <td>{m.id}</td><td>{m.tenant_id}</td><td>{m.msisdn}</td>
              <td>{m.state}</td><td>{m.segments}</td><td>{m.connector_id}</td>
              <td>{m.smsc_msgid}</td><td>{new Date(m.created_at).toLocaleString()}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {total > (filters.limit ? Number(filters.limit) : 25) && (
        <button onClick={() => void search(Number(filters.limit))}>Siguiente</button>
      )}
    </div>
  );
}
```

- [ ] **Step 3: Build**

Run: `cd web && npm run build`
Expected: sin errores TS.

- [ ] **Step 4: Commit**

```bash
git add web/src/views/Dashboard.tsx web/src/views/Messages.tsx
git commit -m "feat: web dashboard (polling metrics) + message search"
```

---

### Task 8: Frontend — Tenants, Connectors (con test), Groups y Routing rules (reorden por prioridad)

**Files:**
- Create: `web/src/views/Tenants.tsx`, `web/src/views/Connectors.tsx`, `web/src/views/Groups.tsx`, `web/src/views/Rules.tsx`
- Test: `cd web && npm run build`

**Interfaces:**
- Consumes: `GET/POST /api/v1/admin/tenants`, `POST /{id}/credit`, `GET /{id}/transactions`, `GET/POST/PUT/DELETE /api/v1/admin/connectors`, `POST /{id}/test`, `GET/POST/DELETE /api/v1/admin/groups`, `PUT /{id}/members`, `GET/POST/DELETE /api/v1/admin/routing-rules`, `PUT /{id}/priority`.
- Produce: CRUD simple con formularios inline (sin modal); Rules con reorden por prioridad usando drag&drop nativo HTML5 (sin librerías).

- [ ] **Step 1: Tenants + Cohetes de crédito**

```tsx
// web/src/views/Tenants.tsx
import { useEffect, useState } from 'react';
import { apiFetch } from '../api';

interface Tenant {
  id: string; name: string; status: string; routing_tag?: string;
  balance?: number; mode?: string; api_key?: string;
}

export default function Tenants() {
  const [items, setItems] = useState<Tenant[]>([]);
  const [form, setForm] = useState({ id: '', name: '', mode: 'prepaid', api_key: '' });
  const [credit, setCredit] = useState<Record<string, string>>({});
  const [msg, setMsg] = useState('');

  const load = async () => {
    const data = await apiFetch<Tenant[]>('/admin/tenants');
    setItems(data ?? []);
  };
  useEffect(() => { void load(); }, []);

  const create = async (e: React.FormEvent) => {
    e.preventDefault();
    await apiFetch('/admin/tenants', { method: 'POST', body: JSON.stringify(form) });
    setForm({ id: '', name: '', mode: 'prepaid', api_key: '' });
    await load();
  };

  const credit = async (id: string) => {
    const amount = Number(credit[id] || '0');
    if (!amount) return;
    await apiFetch(`/admin/tenants/${id}/credit`, {
      method: 'POST', body: JSON.stringify({ amount }),
    });
    setCredit({ ...credit, [id]: '' });
    await load();
  };

  return (
    <div>
      <h1>Tenants</h1>
      {msg && <p>{msg}</p>}
      <div className="card">
        <h2>Nuevo tenant</h2>
        <form onSubmit={create} className="toolbar">
          <input placeholder="id (slug)" value={form.id}
            onChange={(e) => setForm({ ...form, id: e.target.value })} required />
          <input placeholder="nombre" value={form.name}
            onChange={(e) => setForm({ ...form, name: e.target.value })} required />
          <select value={form.mode} onChange={(e) => setForm({ ...form, mode: e.target.value })}>
            <option value="prepaid">prepaid</option>
            <option value="postpaid">postpaid</option>
          </select>
          <input placeholder="api_key" value={form.api_key}
            onChange={(e) => setForm({ ...form, api_key: e.target.value })} />
          <button type="submit">Crear</button>
        </form>
      </div>
      <table>
        <thead><tr><th>ID</th><th>Nombre</th><th>Estado</th><th>Saldo</th>
          <th>Modo</th><th>Crédito</th><th>Api key</th></tr></thead>
        <tbody>
          {items.map((t) => (
            <tr key={t.id}>
              <td>{t.id}</td><td>{t.name}</td><td>{t.status}</td><td>{t.balance}</td>
              <td>{t.mode}</td>
              <td>
                <input value={credit[t.id] || ''} placeholder="monto"
                  onChange={(e) => setCredit({ ...credit, [t.id]: e.target.value })} />
                <button onClick={() => void credit(t.id)}>Abonar</button>
              </td>
              <td>{t.api_key}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
```

- [ ] **Step 2: Connectors + test**

```tsx
// web/src/views/Connectors.tsx
import { useEffect, useState } from 'react';
import { apiFetch } from '../api';

interface Connector {
  id: number; name: string; type: 'smpp' | 'http'; host: string; port: number;
  system_id?: string; password?: string; bind_mode?: string; concurrency?: number;
  max_msg_per_sec?: number; enquire_link_interval?: number; tls?: boolean; enabled?: boolean;
}

const EMPTY: Connector = {
  id: 0, name: '', type: 'smpp', host: '', port: 2775, system_id: '',
  password: '', bind_mode: 'transceiver', concurrency: 1, max_msg_per_sec: 0,
  enquire_link_interval: 30, tls: false, enabled: true,
};

export default function Connectors() {
  const [items, setItems] = useState<Connector[]>([]);
  const [form, setForm] = useState<Connector>({ ...EMPTY });
  const [tests, setTests] = useState<Record<string, string>>({});

  const load = async () => {
    const data = await apiFetch<Connector[]>('/admin/connectors');
    setItems(data ?? []);
  };
  useEffect(() => { void load(); }, []);

  const save = async (e: React.FormEvent) => {
    e.preventDefault();
    if (form.id) {
      await apiFetch(`/admin/connectors/${form.id}`, {
        method: 'PUT', body: JSON.stringify(form),
      });
    } else {
      await apiFetch('/admin/connectors', { method: 'POST', body: JSON.stringify(form) });
    }
    setForm({ ...EMPTY });
    await load();
  };

  const remove = async (id: number) => {
    await apiFetch(`/admin/connectors/${id}`, { method: 'DELETE' });
    await load();
  };

  const testConn = async (id: number) => {
    try {
      const r = await apiFetch<{ ok: boolean; detail: string }>(
        `/admin/connectors/${id}/test`, { method: 'POST' });
      setTests({ ...tests, [id]: `${r.ok ? 'OK' : 'FALLO'}: ${r.detail}` });
    } catch (err) {
      setTests({ ...tests, [id]: (err as Error).message });
    }
  };

  return (
    <div>
      <h1>Conectores</h1>
      <div className="card">
        <h2>{form.id ? 'Editar conector' : 'Nuevo conector'}</h2>
        <form onSubmit={save} className="toolbar">
          <input placeholder="nombre" value={form.name}
            onChange={(e) => setForm({ ...form, name: e.target.value })} required />
          <select value={form.type} onChange={(e) => setForm({ ...form, type: e.target.value as 'smpp' | 'http' })}>
            <option value="smpp">SMPP</option>
            <option value="http">HTTP</option>
          </select>
          <input placeholder="host" value={form.host}
            onChange={(e) => setForm({ ...form, host: e.target.value })} required />
          <input type="number" placeholder="port" value={form.port}
            onChange={(e) => setForm({ ...form, port: Number(e.target.value) })} />
          <input placeholder="system_id" value={form.system_id}
            onChange={(e) => setForm({ ...form, system_id: e.target.value })} />
          <input placeholder="password" type="password" value={form.password || ''}
            onChange={(e) => setForm({ ...form, password: e.target.value })} />
          <input type="number" placeholder="msg/s" value={form.max_msg_per_sec}
            onChange={(e) => setForm({ ...form, max_msg_per_sec: Number(e.target.value) })} />
          <button type="submit">Guardar</button>
          {form.id ? <button type="button" onClick={() => setForm({ ...EMPTY })}>Cancelar</button> : null}
        </form>
      </div>
      <table>
        <thead><tr><th>ID</th><th>Nombre</th><th>Tipo</th><th>Dirección</th>
          <th>System ID</th><th>msg/s</th><th>Activo</th><th>Acciones</th></tr></thead>
        <tbody>
          {items.map((c) => (
            <tr key={c.id}>
              <td>{c.id}</td><td>{c.name}</td><td>{c.type}</td>
              <td>{c.host}:{c.port}</td><td>{c.system_id}</td>
              <td>{c.max_msg_per_sec}</td><td>{c.enabled ? 'sí' : 'no'}</td>
              <td>
                <button onClick={() => { setForm({ ...c, password: '' }); }}>Editar</button>
                <button onClick={() => void testConn(c.id)}>Test</button>
                <button onClick={() => void remove(c.id)}>Eliminar</button>
                {tests[c.id] && <span> {tests[c.id]}</span>}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
```

- [ ] **Step 3: Groups**

```tsx
// web/src/views/Groups.tsx
import { useEffect, useState } from 'react';
import { apiFetch } from '../api';
import { ConnectorOption } from './shared';

interface Group {
  id: number; name: string; members: { connector_id: number; weight: number }[];
}

export default function Groups() {
  const [items, setItems] = useState<Group[]>([]);
  const [connectors, setConnectors] = useState<ConnectorOption[]>([]);
  const [name, setName] = useState('');

  const loadGroups = async () => {
    setItems(await apiFetch<Group[]>('/admin/groups') ?? []);
  };
  const loadConnectors = async () => {
    setConnectors(await apiFetch<ConnectorOption[]>('/admin/connectors') ?? []);
  };
  useEffect(() => { void loadGroups(); void loadConnectors(); }, []);

  const createGroup = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!name) return;
    await apiFetch('/admin/groups', { method: 'POST', body: JSON.stringify({ name }) });
    setName('');
    await loadGroups();
  };

  const removeGroup = async (id: number) => {
    await apiFetch(`/admin/groups/${id}`, { method: 'DELETE' });
    await loadGroups();
  };

  const saveMembers = async (g: Group, cid: number, weight: number) => {
    const members = g.members
      .filter((m) => m.connector_id !== cid || weight > 0)
      .concat(weight > 0 ? [{ connector_id: cid, weight }] : []);
    await apiFetch(`/admin/groups/${g.id}/members`, {
      method: 'PUT', body: JSON.stringify({ members }),
    });
    await loadGroups();
  };

  return (
    <div>
      <h1>Grupos</h1>
      <div className="card">
        <form onSubmit={createGroup} className="toolbar">
          <input placeholder="nombre del grupo" value={name}
            onChange={(e) => setName(e.target.value)} />
          <button type="submit">Crear</button>
        </form>
      </div>
      {items.length === 0 && <p>Sin grupos. Crea uno para balancear conectores.</p>}
      {items.map((g) => (
        <div className="card" key={g.id}>
          <h2>{g.name} <button onClick={() => void removeGroup(g.id)}>Eliminar</button></h2>
          {connectors.map((c) => {
            const m = g.members.find((x) => x.connector_id === c.id);
            return (
              <div key={c.id} className="toolbar">
                <span style={{ width: 200 }}>{c.name}</span>
                <input type="number" min={0} placeholder={m ? String(m.weight) : 'peso (0 = fuera)'}
                  onBlur={(e) => void saveMembers(g, c.id, Number(e.target.value) || 0)} />
              </div>
            );
          })}
        </div>
      ))}
    </div>
  );
}
```

```ts
// web/src/views/shared.ts
export interface ConnectorOption {
  id: number; name: string; type: string;
}
```

- [ ] **Step 4: Routing rules con drag&drop de prioridad**

```tsx
// web/src/views/Rules.tsx
import { useEffect, useState } from 'react';
import { apiFetch } from '../api';

interface Rule {
  id: number; priority: number; tenant_id?: string; from?: string;
  prefix?: string; regex?: string; routing_tag?: string;
  connector_id?: number; group_id?: number;
}

export default function Rules() {
  const [items, setItems] = useState<Rule[]>([]);
  const [form, setForm] = useState({
    priority: '', tenant_id: '', from: '', prefix: '', regex: '',
    routing_tag: '', connector_id: '', group_id: '',
  });
  const [dragId, setDragId] = useState<number | null>(null);

  const load = async () => {
    const data = await apiFetch<Rule[]>('/admin/routing-rules');
    setItems((data ?? []).sort((a, b) => a.priority - b.priority));
  };
  useEffect(() => { void load(); }, []);

  const create = async (e: React.FormEvent) => {
    e.preventDefault();
    await apiFetch('/admin/routing-rules', {
      method: 'POST',
      body: JSON.stringify({
        priority: Number(form.priority) || items.length + 1,
        tenant_id: form.tenant_id, from: form.from, prefix: form.prefix,
        regex: form.regex, routing_tag: form.routing_tag,
        connector_id: Number(form.connector_id) || 0,
        group_id: Number(form.group_id) || 0,
      }),
    });
    setForm({ priority: '', tenant_id: '', from: '', prefix: '', regex: '', routing_tag: '', connector_id: '', group_id: '' });
    await load();
  };

  const remove = async (id: number) => {
    await apiFetch(`/admin/routing-rules/${id}`, { method: 'DELETE' });
    await load();
  };

  // reordena: al soltar sobre otra fila, intercambia prioridades entre ambas
  const dropOn = async (targetId: number) => {
    if (dragId === null || dragId === targetId) return;
    const a = items.find((r) => r.id === dragId)!;
    const b = items.find((r) => r.id === targetId)!;
    await apiFetch(`/admin/routing-rules/${a.id}/priority`, {
      method: 'PUT', body: JSON.stringify({ priority: b.priority }),
    });
    await apiFetch(`/admin/routing-rules/${b.id}/priority`, {
      method: 'PUT', body: JSON.stringify({ priority: a.priority }),
    });
    setDragId(null);
    await load();
  };

  return (
    <div>
      <h1>Reglas de ruteo</h1>
      <p>Arrastra una fila sobre otra para reordenar por prioridad.</p>
      <div className="card">
        <form onSubmit={create} className="toolbar">
          <input placeholder="priority" value={form.priority}
            onChange={(e) => setForm({ ...form, priority: e.target.value })} />
          <input placeholder="tenant_id" value={form.tenant_id}
            onChange={(e) => setForm({ ...form, tenant_id: e.target.value })} />
          <input placeholder="from" value={form.from}
            onChange={(e) => setForm({ ...form, from: e.target.value })} />
          <input placeholder="prefix (msisdn)" value={form.prefix}
            onChange={(e) => setForm({ ...form, prefix: e.target.value })} />
          <input placeholder="regex" value={form.regex}
            onChange={(e) => setForm({ ...form, regex: e.target.value })} />
          <input placeholder="routing_tag" value={form.routing_tag}
            onChange={(e) => setForm({ ...form, routing_tag: e.target.value })} />
          <input placeholder="connector_id" value={form.connector_id}
            onChange={(e) => setForm({ ...form, connector_id: e.target.value })} />
          <input placeholder="group_id" value={form.group_id}
            onChange={(e) => setForm({ ...form, group_id: e.target.value })} />
          <button type="submit">Crear</button>
        </form>
      </div>
      <table>
        <thead>
          <tr><th>#</th><th>Tenant</th><th>From</th><th>Prefix</th><th>Regex</th>
              <th>Tag</th><th>Destino</th><th></th></tr>
        </thead>
        <tbody>
          {items.map((r) => (
            <tr key={r.id} className="drag-row" draggable
              onDragStart={() => setDragId(r.id)}
              onDragOver={(e) => e.preventDefault()}
              onDrop={() => void dropOn(r.id)}>
              <td>{r.priority}</td><td>{r.tenant_id || '-'}</td><td>{r.from || '-'}</td>
              <td>{r.prefix || '-'}</td><td>{r.regex || '-'}</td><td>{r.routing_tag || '-'}</td>
              <td>{r.group_id ? `grupo ${r.group_id}` : `conector ${r.connector_id}`}</td>
              <td><button onClick={() => void remove(r.id)}>Eliminar</button></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
```

- [ ] **Step 5: Build**

Run: `cd web && npm run build`
Expected: sin errores TS.

- [ ] **Step 6: Commit**

```bash
git add web/src/views/shared.ts web/src/views/Tenants.tsx web/src/views/Connectors.tsx web/src/views/Groups.tsx web/src/views/Rules.tsx
git commit -m "feat: web tenants, connectors (test), groups, routing rules dnd"
```

---

### Task 9: Frontend — Rate tables, Webhooks y Usuarios

**Files:**
- Create: `web/src/views/Rates.tsx`, `web/src/views/Webhooks.tsx`, `web/src/views/Users.tsx`
- Test: `cd web && npm run build`

**Interfaces:**
- Consumes: `GET/POST /api/v1/admin/rate-tables`, `GET/POST .../{id}/entries`, `DELETE /api/v1/admin/rate-entries/{id}`; webhooks CRUD de M3; `GET/POST/DELETE /api/v1/admin/users`. Rate tables requieren header `X-Tenant-ID` → el hook fetch de `apiFetch` necesita enviarlo: se añade un parámetro `X-Tenant-ID` desde un selector del tenant en la vista.
- Produce: vistas `Rates`, `Webhooks`, `Users` con CRUD inline.

- [ ] **Step 1: Mensaje de decisión — X-Tenant-ID en rate tables**

Las rutas de rate-tables (M4) leen `X-Tenant-ID`. `apiFetch` se amplía con un tercer argumento `extraHeaders?: Record<string,string>` para cubrirlo; la vista Rates muestra un selector de tenants (compartido con Tenants) y manda el header.

```ts
// web/src/api.ts — ampliar apiFetch (firma)
export async function apiFetch<T = unknown>(
  path: string,
  opts: RequestInit = {},
  extraHeaders: Record<string, string> = {}
): Promise<T> {
  const headers: Record<string, string> = {
    ...(opts.headers as Record<string, string> | undefined),
    ...extraHeaders,
  };
  // ...resto igual...
}
```

- [ ] **Step 2: Rates**

```tsx
// web/src/views/Rates.tsx
import { useEffect, useState } from 'react';
import { apiFetch } from '../api';

interface RateTable { id: number; name: string; active: boolean; }
interface RateEntry { id: number; prefix: string; price: number; connector_id?: number; valid_from?: string; valid_to?: string; }

export default function Rates() {
  const [tenants, setTenants] = useState<{ id: string; name: string }[]>([]);
  const [tenant, setTenant] = useState('t1');
  const [tables, setTables] = useState<RateTable[]>([]);
  const [selected, setSelected] = useState<number | null>(null);
  const [entries, setEntries] = useState<RateEntry[]>([]);
  const [newTable, setNewTable] = useState('');
  const [newEntry, setNewEntry] = useState({ prefix: '', price: '', connector_id: '' });

  const loadTenants = async () => {
    setTenants(await apiFetch<{ id: string; name: string }[]>('/admin/tenants') ?? []);
  };
  const loadTables = async (tid: string) => {
    const data = await apiFetch<RateTable[]>('/admin/rate-tables', {}, { 'X-Tenant-ID': tid });
    setTables(data ?? []);
  };
  const loadEntries = async (tableId: number) => {
    const data = await apiFetch<RateEntry[]>(`/admin/rate-tables/${tableId}/entries`, {}, { 'X-Tenant-ID': tenant });
    setEntries(data ?? []);
  };
  useEffect(() => { void loadTenants(); }, []);
  useEffect(() => { if (tenant) void loadTables(tenant); }, [tenant]);

  const createTable = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newTable) return;
    const r = await apiFetch<{ id: number }>('/admin/rate-tables',
      { method: 'POST', body: JSON.stringify({ name: newTable, active: true }) },
      { 'X-Tenant-ID': tenant });
    setSelected(r.id);
    setNewTable('');
    await loadTables(tenant);
    await loadEntries(r.id);
  };

  const createEntry = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!selected || !newEntry.prefix || !newEntry.price) return;
    await apiFetch(`/admin/rate-tables/${selected}/entries`,
      { method: 'POST', body: JSON.stringify({
        prefix: newEntry.prefix,
        price: Number(newEntry.price),
        connector_id: Number(newEntry.connector_id) || undefined,
      }) },
      { 'X-Tenant-ID': tenant });
    setNewEntry({ prefix: '', price: '', connector_id: '' });
    await loadEntries(selected);
  };

  const removeEntry = async (id: number) => {
    await apiFetch(`/admin/rate-entries/${id}`, { method: 'DELETE' });
    if (selected) await loadEntries(selected);
  };

  return (
    <div>
      <h1>Tarifas</h1>
      <div className="toolbar">
        <label>Tenant
          <select value={tenant} onChange={(e) => setTenant(e.target.value)}>
            {tenants.map((t) => <option key={t.id} value={t.id}>{t.name}</option>)}
          </select>
        </label>
      </div>
      <div className="card">
        <form onSubmit={createTable} className="toolbar">
          <input placeholder="nombre de tabla" value={newTable}
            onChange={(e) => setNewTable(e.target.value)} />
          <button type="submit">Nueva tabla</button>
        </form>
        {tables.map((t) => (
          <div key={t.id} className="toolbar">
            <button onClick={() => { setSelected(t.id); void loadEntries(t.id); }}>
              {t.name}{t.active ? '' : ' (inactiva)'}
            </button>
          </div>
        ))}
      </div>
      {selected && (
        <div className="card">
          <h2>Entradas de tabla {selected}</h2>
          <form onSubmit={createEntry} className="toolbar">
            <input placeholder="prefix msisdn" value={newEntry.prefix}
              onChange={(e) => setNewEntry({ ...newEntry, prefix: e.target.value })} required />
            <input placeholder="precio" value={newEntry.price}
              onChange={(e) => setNewEntry({ ...newEntry, price: e.target.value })} required />
            <input placeholder="connector_id" value={newEntry.connector_id}
              onChange={(e) => setNewEntry({ ...newEntry, connector_id: e.target.value })} />
            <button type="submit">Agregar</button>
          </form>
          <table>
            <thead><tr><th>Prefix</th><th>Precio</th><th>Conector</th><th>Vigencia</th><th></th></tr></thead>
            <tbody>
              {entries.map((en) => (
                <tr key={en.id}>
                  <td>{en.prefix}</td><td>{en.price}</td><td>{en.connector_id ?? '-'}</td>
                  <td>{en.valid_from ?? ''} → {en.valid_to ?? ''}</td>
                  <td><button onClick={() => void removeEntry(en.id)}>Eliminar</button></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
```

- [ ] **Step 3: Webhooks**

```tsx
// web/src/views/Webhooks.tsx
import { useEffect, useState } from 'react';
import { apiFetch } from '../api';

interface Webhook {
  id: number; tenant_id?: string; url: string; auth_token?: string;
  events?: string[]; active?: boolean;
}

export default function Webhooks() {
  const [items, setItems] = useState<Webhook[]>([]);
  const [form, setForm] = useState({ url: '', auth_token: '', events: 'DLR', active: true, tenant_id: '' });

  const load = async () => {
    const data = await apiFetch<Webhook[]>('/admin/webhooks');
    setItems(data ?? []);
  };
  useEffect(() => { void load(); }, []);

  const create = async (e: React.FormEvent) => {
    e.preventDefault();
    await apiFetch('/admin/webhooks', {
      method: 'POST',
      body: JSON.stringify({
        url: form.url,
        auth_token: form.auth_token,
        events: form.events.split(',').map((s) => s.trim()).filter(Boolean),
        active: form.active,
        tenant_id: form.tenant_id || undefined,
      }),
    });
    setForm({ url: '', auth_token: '', events: 'DLR', active: true, tenant_id: '' });
    await load();
  };

  const remove = async (id: number) => {
    await apiFetch(`/admin/webhooks/${id}`, { method: 'DELETE' });
    await load();
  };

  return (
    <div>
      <h1>Webhooks</h1>
      <div className="card">
        <form onSubmit={create} className="toolbar">
          <input placeholder="url" value={form.url}
            onChange={(e) => setForm({ ...form, url: e.target.value })} required />
          <input placeholder="auth_token" value={form.auth_token}
            onChange={(e) => setForm({ ...form, auth_token: e.target.value })} />
          <input placeholder="eventos (DLR)" value={form.events}
            onChange={(e) => setForm({ ...form, events: e.target.value })} />
          <button type="submit">Crear</button>
        </form>
      </div>
      <table>
        <thead><tr><th>ID</th><th>URL</th><th>Eventos</th><th>Activo</th><th></th></tr></thead>
        <tbody>
          {items.map((h) => (
            <tr key={h.id}>
              <td>{h.id}</td><td>{h.url}</td><td>{(h.events || []).join(', ')}</td>
              <td>{h.active ? 'sí' : 'no'}</td>
              <td><button onClick={() => void remove(h.id)}>Eliminar</button></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
```

- [ ] **Step 4: Usuarios**

```tsx
// web/src/views/Users.tsx
import { useEffect, useState } from 'react';
import { apiFetch } from '../api';

interface User {
  id: number; username: string; role: string; tenant_id?: string; created_at: string;
}

export default function Users() {
  const [items, setItems] = useState<User[]>([]);
  const [form, setForm] = useState({ username: '', password: '', role: 'viewer', tenant_id: '' });
  const [error, setError] = useState('');

  const load = async () => {
    setItems(await apiFetch<User[]>('/admin/users') ?? []);
  };
  useEffect(() => { void load(); }, []);

  const create = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    try {
      await apiFetch('/admin/users', { method: 'POST', body: JSON.stringify(form) });
      setForm({ username: '', password: '', role: 'viewer', tenant_id: '' });
      await load();
    } catch (err) {
      setError((err as Error).message);
    }
  };

  const remove = async (id: number) => {
    await apiFetch(`/admin/users/${id}`, { method: 'DELETE' });
    await load();
  };

  return (
    <div>
      <h1>Usuarios</h1>
      {error && <p className="error">{error}</p>}
      <div className="card">
        <form onSubmit={create} className="toolbar">
          <input placeholder="usuario" value={form.username}
            onChange={(e) => setForm({ ...form, username: e.target.value })} required />
          <input placeholder="contraseña" type="password" value={form.password}
            onChange={(e) => setForm({ ...form, password: e.target.value })} required />
          <select value={form.role} onChange={(e) => setForm({ ...form, role: e.target.value })}>
            <option value="superadmin">superadmin</option>
            <option value="admin">admin</option>
            <option value="viewer">viewer</option>
          </select>
          <input placeholder="tenant_id (opcional)" value={form.tenant_id}
            onChange={(e) => setForm({ ...form, tenant_id: e.target.value })} />
          <button type="submit">Crear</button>
        </form>
      </div>
      <table>
        <thead><tr><th>ID</th><th>Usuario</th><th>Rol</th><th>Tenant</th><th></th></tr></thead>
        <tbody>
          {items.map((u) => (
            <tr key={u.id}>
              <td>{u.id}</td><td>{u.username}</td><td>{u.role}</td>
              <td>{u.tenant_id || '-'}</td>
              <td><button onClick={() => void remove(u.id)}>Eliminar</button></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
```

- [ ] **Step 5: Build**

Run: `cd web && npm run build`
Expected: sin errores TS (atención a `apiFetch` ampliada en Task 9 — la firma es compatible con las llamadas previas).

- [ ] **Step 6: Commit**

```bash
git add web/src/api.ts web/src/views/Rates.tsx web/src/views/Webhooks.tsx web/src/views/Users.tsx
git commit -m "feat: web rates, webhooks, users"
```

---

### Task 10: Deploy — Makefile, systemd, nginx, README deploy

**Files:**
- Modify: `Makefile` (targets existentes + nuevos)
- Create: `deploy/smppgw-server.service`, `deploy/smppgw-connector@.service`, `deploy/nginx-smppgw.conf`
- Modify: `README.md` (sección Despliegue)
- Test: revisión manual de la VM (o checklist si no hay acceso)

**Interfaces:**
- Consumes: `web/dist` (Task 6-9), binario `smppgw` (M1-M5), migraciones golang-migrate, config env de M1/M5/Task 4.
- Produce:
  - `make build`, `make build-ui`, `make install-ui` (`/var/www/smppgw`), `make migrate-up` (golang-migrate), `make run-server`, `make run-connector`, `make deploy` (build + install-ui + `systemctl restart`).
  - Units systemd: `smppgw-server.service` (rol server, `After=postgresql redis`), `smppgw-connector@%i.service` (rol connector con `SMG_CONNECTOR_ID=%i`).
  - `deploy/nginx-smppgw.conf`: server 80 → root `/var/www/smppgw`, `location /api/` → proxy `http://127.0.0.1:8080` (bajar `proxy_buffering off` solo en `location /api/v1/metrics/stream`), headers CORS no necesarios (mismo origen).

- [ ] **Step 1: Makefile**

```make
# Makefile — añadir al bloque existente
BUILD_DIR := bin
UI_DIST := /var/www/smppgw

.PHONY: build build-ui install-ui migrate-down migrate-up deploy

build:
	go build -o $(BUILD_DIR)/smppgw ./cmd/smppgw

build-ui:
	cd web && npm ci && npm run build

install-ui:
	rm -rf $(UI_DIST)
	cp -r web/dist $(UI_DIST)

migrate-up:
	@test -n "$(SMG_DB_URL)" || (echo "SMG_DB_URL requerida"; exit 1)
	migrate -path db/migrations -database "$$SMG_DB_URL" up

migrate-down:
	@test -n "$(SMG_DB_URL)" || (echo "SMG_DB_URL requerida"; exit 1)
	migrate -path db/migrations -database "$$SMG_DB_URL" down 1

deploy: build build-ui install-ui
	systemctl restart smppgw-server smppgw-connector@1 smppgw-connector@2
```

> `make run-server`/`make run-connector` ya existen en M1; se dejan intactos.

- [ ] **Step 2: Units systemd**

```ini
# deploy/smppgw-server.service
[Unit]
Description=smppgw server (API + SMPP inbound)
After=network.target postgresql.service redis-server.service
Wants=network.target

[Service]
User=smpp
Group=smpp
EnvironmentFile=/etc/smppgw/server.env
ExecStart=/usr/local/bin/smppgw server
Restart=on-failure
RestartSec=3
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

> `/etc/smppgw/server.env` debe contener: `SMG_ROLE=server`, `SMG_DB_URL=...`, `SMG_REDIS_URL=...`, `SMG_JWT_SECRET=<generado>`, `SMG_ADMIN_USER=admin`, `SMG_ADMIN_PASSWORD=<elegida>`. Migrar con `SMG_DB_URL` igual.

```ini
# deploy/smppgw-connector@.service
[Unit]
Description=smppgw connector %i
After=network.target postgresql.service redis-server.service

[Service]
User=smpp
Group=smpp
EnvironmentFile=/etc/smppgw/connector.env
ExecStart=/usr/local/bin/smppgw connector
Restart=on-failure
RestartSec=3
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

> `/etc/smppgw/connector.env` contiene las mismas SMG_DB_URL/SMG_REDIS_URL; el `%i` se mapea a `SMG_CONNECTOR_ID` — el binary lo lee de env, así que instanciar con `systemctl start smppgw-connector@1` **requiere** una línea override: crear `/etc/systemd/system/smppgw-connector@.service.d/override.conf` con:

```ini
# /etc/systemd/system/smppgw-connector@.service.d/override.conf
[Service]
Environment="SMG_CONNECTOR_ID=%i"
```

> Alternativa equivalente: template con `ExecStart=/usr/local/bin/smppgw connector` y pasar el id vía `systemctl set-environment`; se elige el override (explícito y persistente).

- [ ] **Step 3: nginx**

```nginx
# deploy/nginx-smppgw.conf → /etc/nginx/sites-available/smppgw (symlink en sites-enabled)
server {
    listen 80;
    server_name smppgw.lan;

    root /var/www/smppgw;
    index index.html;

    location / {
        try_files $uri $uri/ /index.html;
    }

    location /api/v1/metrics/stream {
        proxy_pass http://127.0.0.1:8080;
        proxy_http_version 1.1;
        proxy_buffering off;
        proxy_cache off;
        proxy_read_timeout 1h;
    }

    location /api/ {
        proxy_pass http://127.0.0.1:8080;
        proxy_http_version 1.1;
    }
}

# TLS opcional (certbot): sudo certbot --nginx -d smppgw.lan
```

- [ ] **Step 4: README deploy**

```markdown
## Despliegue (VM sin Docker)

1. Instalar Go 1.22+, PostgreSQL 14+, Redis 7+, golang-migrate, nginx, node 20.
2. `sudo useradd -r -s /bin/false smpp`
3. `make build` y copiar `bin/smppgw` a `/usr/local/bin/smppgw`.
4. Crear `/etc/smppgw/` con los env de `deploy/smppgw-server.service` y `deploy/smppgw-connector@.service` (leyenda arriba). Generar SMG_JWT_SECRET con `openssl rand -hex 32`.
5. `make migrate-up SMG_DB_URL=postgres://smpp:smpp@localhost:5432/smpp?sslmode=disable`
6. Copiar units: `sudo cp deploy/smppgw-server.service deploy/smppgw-connector@.service /etc/systemd/system/` + override dir del %i; `sudo systemctl daemon-reload && sudo systemctl enable --now smppgw-server smppgw-connector@1 smppgw-connector@2`.
7. `make build-ui && sudo make install-ui`; copiar nginx conf y reload.
8. `curl -X POST localhost:8080/api/v1/auth/login -d '{"username":"admin","password":"..."}'` → token. Abrir `http://<VM>/`.
9. TLS: `sudo certbot --nginx -d smppgw.lan`.
```

- [ ] **Step 5: Verificación (VM)**

Run:
- `make deploy`
- `curl http://localhost/healthz` → `ok` (vía nginx).
- `curl -s http://localhost/api/v1/auth/login -d '{"username":"admin","password":"<elegida>"}'` → token.
- `curl -s -H "Authorization: Bearer <token>" http://localhost/api/v1/metrics` → JSON.
- `curl -N -H "Authorization: Bearer <token>" http://localhost/api/v1/metrics/stream | head -n 3` → `data: {...}`.
- Abrir `http://<VM>/` en navegador → login → dashboard.
Expected: todo 200; el stream emite eventos.

- [ ] **Step 6: Commit**

```bash
git add Makefile deploy README.md
git commit -m "feat: deploy systemd nginx make targets"
```

---

### Task 11: E2E admin + integración gated + docs finales

**Files:**
- Modify: `e2e/e2e_test.go` (admin CRUD + login contra PG)
- Modify: `test/integration_test.go` (login + rate-tables con X-Tenant-ID + métricas)
- Test: `make test`, `make test-integration`

**Interfaces:**
- Consumes: API completa de Task 4, store PG/Memory de Task 2.
- Produce: cobertura de humo del flujo admin completo en memoria (e2e) y contra PG+Redis (integración).

- [ ] **Step 1: e2e admin en memoria**

```go
// e2e/e2e_test.go — añadir
func TestAdminLoginYCRUD(t *testing.T) {
    repo := store.NewMemory()
    _ = repo.CreateUser(context.Background(), &store.User{
        Username: "admin", PasswordHash: mustHashE2E(t, "s3cret"), Role: "superadmin",
    })
    cfg := apiConfig()
    cfg.JWTSecret = "secreto-e2e"
    cfg.JWTTTL = time.Hour
    srv := api.New(cfg, e2ePipeline(t), repo)

    login := httptest.NewRequest("POST", "/api/v1/auth/login",
        strings.NewReader(`{"username":"admin","password":"s3cret"}`))
    rr := httptest.NewRecorder()
    srv.ServeHTTP(rr, login)
    if rr.Code != 200 {
        t.Fatalf("login %d: %s", rr.Code, rr.Body.String())
    }
    var tok struct{ Token string `json:"token"` }
    _ = json.Unmarshal(rr.Body.Bytes(), &tok)

    req := httptest.NewRequest("GET", "/api/v1/admin/messages", nil)
    req.Header.Set("Authorization", "Bearer "+tok.Token)
    rr = httptest.NewRecorder()
    srv.ServeHTTP(rr, req)
    if rr.Code != 200 || !strings.Contains(rr.Body.String(), `"total":`) {
        t.Fatalf("messages %d: %s", rr.Code, rr.Body.String())
    }
}

func mustHashE2E(t *testing.T, pw string) string {
    t.Helper()
    h, err := auth.HashPassword(pw)
    if err != nil {
        t.Fatal(err)
    }
    return h
}
```

> `apiConfig()` y `e2ePipeline(t)` existen en e2e (M3/M4). `cfg.JWTSecret`/`cfg.JWTTTL` se agregan como campos en Task 4. Si `apiConfig()` retorna un `config.Config` con los nuevos campos cero, se setean aquí.

- [ ] **Step 2: integración gated (PG+Redis)**

```go
// test/integration_test.go — añadir
func TestIntegrationAdminLoginVMetrics(t *testing.T) {
    if os.Getenv("SMG_TEST_INTEGRATION") != "1" {
        t.Skip("requiere SMG_TEST_INTEGRATION=1")
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

    users, _ := repo.ListUsers(ctx)
    adminUser := "it-admin"
    for _, u := range users {
        if u.Username == adminUser {
            _ = repo.DeleteUser(ctx, u.ID)
        }
    }
    hash, _ := auth.HashPassword("s3cret")
    if err := repo.CreateUser(ctx, &store.User{Username: adminUser, PasswordHash: hash, Role: "superadmin"}); err != nil {
        t.Fatal(err)
    }

    cfg := config.Config{Role: "server", JWTSecret: "it-secret", JWTTTL: time.Hour, MetricsInterval: time.Second}
    srv := api.New(cfg, newTestPipelineIntegration(t, repo), repo)

    login := httptest.NewRequest("POST", "/api/v1/auth/login",
        strings.NewReader(`{"username":"it-admin","password":"s3cret"}`))
    rr := httptest.NewRecorder()
    srv.ServeHTTP(rr, login)
    if rr.Code != 200 {
        t.Fatalf("login %d: %s", rr.Code, rr.Body.String())
    }
    var tok struct{ Token string `json:"token"` }
    _ = json.Unmarshal(rr.Body.Bytes(), &tok)

    req := httptest.NewRequest("GET", "/api/v1/metrics", nil)
    req.Header.Set("Authorization", "Bearer "+tok.Token)
    rr = httptest.NewRecorder()
    srv.ServeHTTP(rr, req)
    if rr.Code != 200 || !strings.Contains(rr.Body.String(), `"today"`) {
        t.Fatalf("metrics %d: %s", rr.Code, rr.Body.String())
    }
}

// helper en el mismo archivo — pipeline stub que no enruta
func newTestPipelineIntegration(t *testing.T, repo store.MessageRepo) *pipeline.Pipeline {
    t.Helper()
    return pipeline.NewPipeline(repo, nilQueue(), nilRouter)
}
```

> En integración no se necesita Redis para métricas: `nilQueue`/`nilRouter` son valores que no se tocan en estos tests (los handlers admin no usan cola ni router). Si `pipeline.NewPipeline` exige interfaces no nil, crear stubs mínimos como en M1/M2 (`type nullQueue struct{}` con métodos que tocan Redis solo si se usan). Ajustar según la firma real tras leer M2 Task 4.

- [ ] **Step 3: Correr tests**

Run: `make test && SMG_TEST_INTEGRATION=1 SMG_DB_URL=... make test-integration`
Expected: PASS (unitarios) y PASS (integración con PG local con `0007` aplicada).

- [ ] **Step 4: Commit**

```bash
git add e2e/e2e_test.go test/integration_test.go
git commit -m "feat: e2e/integration admin login metrics"
```

---

## Self-Review del plan

**1. Cobertura del spec:**
- "Auth JWT + RBAC (superadmin/admin/viewer)" → Tasks 3, 4 (login, `withAuth`, roles). ✓
- Tabla `users` (hash bcrypt) → Task 1 (`users`) + Task 2 (`UserRepo`); hash por KDF stdlib (constraint de dependencias, documentado en Global Constraints). ✓
- `/api/v1/admin/{tenants,users,connectors,groups,routing-rules,rate-tables,webhooks}` CRUD RBAC → Tasks 4 (rutas). Tenants/rate-tables/webhooks ya existían (M3/M4) y se envuelven con JWT. ✓
- `/api/v1/messages` (envío HTTP, API key) se mantiene sin JWT (Task 4 no lo toca). ✓
- `/api/v1/messages/{id}` estado/DLR → M3 + búsqueda paginada en Task 4 (`/api/v1/admin/messages`). ✓
- `/api/v1/metrics` + `/api/v1/metrics/stream` SSE (y `/metrics` Prometheus **opcional**) → Task 4 (`/metrics` Prometheus se omite en v1: el JSON + SSE cubren el dashboard; anotado). ✓
- Frontend: Login → Dashboard (TPS, por conector, estados) → Tenants → Connectors (+test) → Groups → Routing rules (drag&drop) → Rate tables → Búsqueda mensajes → Webhooks → Usuarios → Tasks 6-9. ✓ (TPS en vivo = polling 2s de `/metrics`; se documenta por qué no se usa SSE en el navegador).
- Despliegue: systemd `smppgw-server` + `smppgw-connector@` template, nginx (build + proxy `/api`, buffering off SSE), golang-migrate, certbot opcional → Task 10. ✓
- "go:embed opcional como fallback" — se omite (spec lo marca opcional y con nginx no hace falta). ✓

**2. Placeholder scan:**
- No hay TBD/TODO/"implementar más tarde". Todos los snippets tienen código completo.
- Notas de decisión explícitas: SSE en navegador → polling (EventSource sin headers); bcrypt → KDF stdlib; `/metrics` Prometheus omitido en v1; override systemd para `%i`.
- Riesgo acotado y señalado: en Task 4 Step 7/Step 8 los tests de M3/M4 de rutas admin sin token pasan a 401 → hay que tocarlos con `loginToken` (Step 9 lo indica). El helper `apiFor`/`newTestPipeline`/`nilQueue` depende de firmas reales de M2/M3/M4 → el Step 8 y Task 11 dejan márgenes verificables al ejecutar (`go test` los resuelve; se ajusta si la firma difiere).

**3. Type consistency:**
- `auth.Sign/Verify/Claims`, `auth.HashPassword/VerifyPassword`, `auth.RequireRole` (Task 3) usados en Task 4 (`handleLogin`, `withAuth`, `handleCreateUser`) y Task 5 (`ensureAdmin`). ✓
- `store.User` (id serial, username, password_hash, role, tenant_id) Tabla `users` 0007 ← `UserRepo` Task 2 ← handlers Task 4. ✓
- `store.Connector` campos = columnas 0001 (source_ton/source_npi/dest_ton/dest_npi como ints; `max_message_per_second` → `MaxMsgPerSec float64`). ✓
- `router.Group`/`router.GroupMember`/`router.Rule` (M2) reutilizados por `GroupRepo`/`RuleRepo` y DTOs JSON (`connector_id/weight`, `priority/tenant_id/from/prefix/regex/routing_tag/connector_id/group_id`). ✓
- `store.MessageFilter` y `ListMessages(ctx, f) ([]Message, int, error)` Firmado igual en PG y Memory; `MessageRepo` se amplía (M1/M3 métodos intactos). ✓
- `StatsRepo{CountByState, CountByConnector, CountMessages}` consumido por `metricsSnapshot` (Task 4). ✓
- `config.Config` nuevos campos consumidos por `api.New` (Task 4) y `run.go` (Task 5); `config.Load` valida `SMG_JWT_SECRET` en server. ✓
- `api.New(cfg, p, repo repos)` firma intacta (M3/M4) con `repos` ampliado a 10; `*store.MemoryRepo` y `*store.PGRepo` declarados conformes (`var _ XRepo = (*MemoryRepo)(nil)` Task 2). ✓
- Refs de rutas: mux Task 4={`/api/v1/auth/login`, `/api/v1/metrics(stream)`, `/api/v1/admin/{messages,connectors,groups,routing-rules,users,...}`} consumidas por `api.ts`/vistas Tasks 6-9 (paths idénticos). ✓
- REDIS: `redis.ParseURL` (fix M5) usado en `run.go` Task 5 — go-redis v9 lo exporta. ✓
- `smpp.PDU{Header: smpp.Head(1, smpp.EnquireLink)}`, `smpp.Encode`, `smpp.Decode`, `smpp.EnquireLinkResp` (M1) usados en `handleTestConnector`. ✓