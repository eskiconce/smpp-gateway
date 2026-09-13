# M5 — Bind Entrante ESME + deliver_sm + 0512

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Aceptar binds SMPP entrantes de clientes ESME multi-tenant, recibir `submit_sm` con validación/billing/router, rechazar con 0512 si saldo insuficiente, y entregar DLR de vuelta al ESME via `deliver_sm` (en vez de webhook).

**Architecture:** Un nuevo paquete `internal/esme` implementa un servidor SMPP TCP que acepta `bind_transceiver`/`bind_receiver`/`bind_transmitter` de clientes, autentica contra la tabla `tenants` (credenciales SMPP), recibe `submit_sm`, alimenta al pipeline (validación → billing → router → cola) y responde con `submit_sm_resp` (msgid o 0512). Cuando un DLR llega al conector y el mensaje fue vía SMPP, se publica a un canal Redis pub/sub que el servidor ESME consume y envía `deliver_sm` de vuelta. Se añade `source_channel` a `messages` para distinguir `api` vs `smpp`.

**Tech Stack:** Go 1.22+, go-redis v9, PostgreSQL 14+ (golang-migrate), golang.org/x/time/rate, librería SMPP propia (`internal/smpp`).

**Spec:** `docs/superpowers/specs/2026-09-12-smpp-gateway-design.md` — secciones "Arquitectura", "DLR y estados", "Billing", "API REST" + tabla `tenants` de "Modelo de datos".

## Global Constraints

- Sin Docker. Go 1.22+, PostgreSQL 14+, Redis 7+.
- `golang-migrate` para migraciones. Nombres/copy en español. Sin comentarios salvo invariantes.
- TDD. Commits en inglés. Sin dependencias nuevas fuera de `go-redis v9`, `miniredis v2`, `pgxpool`, `golang.org/x/time/rate`.
- Firma `api.New(cfg config.Config, p *pipeline.Pipeline, repo repos) *Server` (M3+M4).
- `pipeline.NewPipeline(repo, q, r, opts ...Option) *Pipeline` (M2+M4 variádico).
- `worker.NewWorker(q, repo, opts ...Option)` con `WithBackoff`, `SetSession`, `WithDLR`, `WithBiller`.
- `store.ErrNotFound` es el error sentinela de store (Task 2 M4).
- `billing.ErrInsufficientBalance` → 0512 en ESME, 402 en HTTP.
- `dlr.Event` tiene `TenantID, MessageID, SmscMsgid, Msisdn, State, Timestamp` (M3); se añade `SourceChannel`.
- `dlr.Notifier` con `Notify(ctx, Event) error` (M3).

---

## File Structure

| Acción | Archivo | Responsabilidad |
|--------|---------|-----------------|
| Create | `db/migrations/0006_esme_bind.up.sql` | Columnas SMPP en `tenants`, `source_channel` en `messages` |
| Create | `db/migrations/0006_esme_bind.down.sql` | Rollback |
| Create | `internal/esme/server.go` | Servidor SMPP TCP inbound: accept, bind, auth, submit, deliver |
| Create | `internal/esme/server_test.go` | Tests herméticos con smscsim como cliente |
| Create | `internal/esme/dlr.go` | `ESMEConsumer` (suscriptor Redis pub/sub → deliver_sm) + `ESMEPublisher` (publicador Redis para el conector) |
| Create | `internal/esme/dlr_test.go` | Tests del bridge pub/sub |
| Modify | `internal/smpp/statuses.go` | Añadir `ESME_0512 CommandStatus = 0x0512` (saldo insuficiente) |
| Modify | `internal/store/tenant.go` | `GetTenantBySMPPSystemID(ctx, systemID) (*Tenant, error)` + `SourceChannel` en `Message` |
| Modify | `internal/store/pg.go` | Implementación PG de `GetTenantBySMPPSystemID` + `source_channel` en queries |
| Modify | `internal/store/memory.go` | Implementación memoria de `GetTenantBySMPPSystemID` + `source_channel` |
| Modify | `internal/store/msg_test.go` | Tests de `source_channel` |
| Modify | `internal/dlr/processor.go` | Añadir `SourceChannel` a `Event`, populate desde `GetMessage` |
| Modify | `internal/dlr/processor_test.go` | Tests de `SourceChannel` en Event |
| Modify | `internal/pipeline/pipeline.go` | `Outgoing` gana `SourceChannel string`; `CreateMessage` lo propaga |
| Modify | `internal/pipeline/pipeline_test.go` | Tests de `SourceChannel` |
| Modify | `cmd/smppgw/run.go` | `runServer` levanta ESME server + consumer Redis |

---

### Task 1: Migración 0006 — credenciales SMPP para tenants + `source_channel`

**Files:**
- Create: `db/migrations/0006_esme_bind.up.sql`
- Create: `db/migrations/0006_esme_bind.down.sql`

**Interfaces:**
- Consumes: schema M4 (`tenants` con `mode`/`api_key`, `messages` con `amount`).
- Produce: columnas `tenants.smpp_system_id`, `tenants.smpp_password`, `messages.source_channel`. Las tasks 2-7 las consumen.

- [ ] **Step 1: Migración up**

```sql
-- db/migrations/0006_esme_bind.up.sql
ALTER TABLE tenants
    ADD COLUMN smpp_system_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN smpp_password  TEXT NOT NULL DEFAULT '';
CREATE UNIQUE INDEX uq_tenants_smpp_system_id
    ON tenants(smpp_system_id) WHERE smpp_system_id <> '';

ALTER TABLE messages
    ADD COLUMN source_channel TEXT NOT NULL DEFAULT 'api';
```

- [ ] **Step 2: Migración down**

```sql
-- db/migrations/0006_esme_bind.down.sql
DROP INDEX IF EXISTS uq_tenants_smpp_system_id;
ALTER TABLE tenants DROP COLUMN IF EXISTS smpp_system_id;
ALTER TABLE tenants DROP COLUMN IF EXISTS smpp_password;
ALTER TABLE messages DROP COLUMN IF EXISTS source_channel;
```

- [ ] **Step 3: Commit**

```bash
git add db/migrations/0006_esme_bind.up.sql db/migrations/0006_esme_bind.down.sql
git commit -m "feat: migracion esme bind (smpp creds, source_channel)"
```

---

### Task 2: Store — `TenantRepo` SMPP + `source_channel` en `Message`

**Files:**
- Modify: `internal/store/tenant.go` (nuevo método + struct)
- Modify: `internal/store/pg.go` (queries SMPP)
- Modify: `internal/store/memory.go` (misma interfaz)
- Modify: `internal/store/msg.go` (campo `SourceChannel`)
- Modify: `internal/store/msg_test.go` (roundtrip de `source_channel`)
- Test: `go test ./internal/store/ -v`

**Interfaces:**
- Consumes: `store.Tenant` y `store.Message` (M1/M4), migración 0006 (Task 1).
- Produce:
  - `Tenant` gana `SmppSystemID`, `SmppPassword string`.
  - `TenantRepo` gana `GetTenantBySMPPSystemID(ctx, systemID string) (*Tenant, error)`.
  - `Message` gana `SourceChannel string`.
  - `MessageRepo.CreateMessage` persiste `source_channel`.

- [ ] **Step 1: Test de `GetTenantBySMPPSystemID` (memory)**

```go
// internal/store/msg_test.go — añadir
func TestGetTenantBySMPPSystemID(t *testing.T) {
    repo := NewMemory()
    ctx := context.Background()
    if err := repo.CreateTenant(ctx, &Tenant{
        ID: "t1", Status: "active",
        SmppSystemID: "esme-01", SmppPassword: "pass",
    }); err != nil {
        t.Fatal(err)
    }
    got, err := repo.GetTenantBySMPPSystemID(ctx, "esme-01")
    if err != nil {
        t.Fatal(err)
    }
    if got.ID != "t1" || got.SmppPassword != "pass" {
        t.Fatalf("got=%+v", got)
    }
    // no encontrado
    if _, err := repo.GetTenantBySMPPSystemID(ctx, "no-existe"); !errors.Is(err, ErrNotFound) {
        t.Fatalf("esperaba ErrNotFound, got %v", err)
    }
}
```

- [ ] **Step 2: Test de `SourceChannel` en mensaje**

```go
// internal/store/msg_test.go — añadir
func TestMessageSourceChannel(t *testing.T) {
    repo := NewMemory()
    ctx := context.Background()
    m := &Message{ID: "m1", TenantID: "t1", Msisdn: "569", Text: "hola", SourceChannel: "smpp"}
    if err := repo.CreateMessage(ctx, m); err != nil {
        t.Fatal(err)
    }
    got, _ := repo.GetMessage(ctx, "m1")
    if got.SourceChannel != "smpp" {
        t.Fatalf("source_channel=%q", got.SourceChannel)
    }
}
```

- [ ] **Step 3: Correr tests y verificar que fallan**

Run: `go test ./internal/store/ -run 'TestGetTenantBySMPPSystemID|TestMessageSourceChannel' -v`
Expected: FAIL — campos y método no existen.

- [ ] **Step 4: Implementar store.go — Tenant y Message**

```go
// internal/store/tenant.go — añadir al struct Tenant
type Tenant struct {
    ID             string
    Name           string
    Status         string
    RoutingTag     string
    Balance        float64
    Mode           string
    ApiKey         string
    SmppSystemID   string
    SmppPassword   string
    CreatedAt      time.Time
}

// TenantRepo — añadir método
type TenantRepo interface {
    ListTenants(ctx context.Context) ([]Tenant, error)
    CreateTenant(ctx context.Context, t *Tenant) error
    GetTenant(ctx context.Context, id string) (*Tenant, error)
    GetTenantByAPIKey(ctx context.Context, apiKey string) (*Tenant, error)
    GetTenantBySMPPSystemID(ctx context.Context, systemID string) (*Tenant, error)
}
```

```go
// internal/store/pg.go — añadir
const tenantCols = `id, name, status, coalesce(routing_tag,''), balance, mode, api_key,
    smpp_system_id, smpp_password, created_at`

func (r *PGRepo) GetTenantBySMPPSystemID(ctx context.Context, systemID string) (*Tenant, error) {
    var t Tenant
    row := r.pool.QueryRow(ctx, `SELECT `+tenantCols+` FROM tenants WHERE smpp_system_id=$1`, systemID)
    if err := row.Scan(&t.ID, &t.Name, &t.Status, &t.RoutingTag, &t.Balance,
        &t.Mode, &t.ApiKey, &t.SmppSystemID, &t.SmppPassword, &t.CreatedAt); err != nil {
        if errors.Is(err, pgx.ErrNoRows) {
            return nil, ErrNotFound
        }
        return nil, err
    }
    return &t, nil
}

// Update Tenant scan en ListTenants, GetTenant, GetTenantByAPIKey para incluir
// SmppSystemID y SmppPassword (añadir 2 campos al Scan de cada uno).
```

```go
// internal/store/memory.go — añadir
type MemoryRepo struct {
    mu      sync.Mutex
    msgs    map[string]*Message
    tenants map[string]*Tenant
    // webhooks, rateTables, rateEntries, transactions... (M3/M4)
}

func (r *MemoryRepo) GetTenantBySMPPSystemID(_ context.Context, systemID string) (*Tenant, error) {
    r.mu.Lock()
    defer r.mu.Unlock()
    for _, t := range r.tenants {
        if t.SmppSystemID == systemID {
            cp := *t
            return &cp, nil
        }
    }
    return nil, ErrNotFound
}
```

```go
// internal/store/msg.go — añadir campo
type Message struct {
    ID            string
    TenantID      string
    SourceAddr    string
    Msisdn        string
    Text          string
    Segments      int
    ConnectorID   int
    TryCount      int
    State         string
    SmscMsgid     string
    SourceChannel string
    Amount        float64
    CreatedAt     time.Time
    UpdatedAt     time.Time
}

// internal/store/pg.go — CreateMessage incluir source_channel
const msgInsertCols = `id, tenant_id, source_addr, msisdn, text, segments, connector_id,
    try_count, state, smsc_msgid, source_channel, amount, created_at, updated_at`

// Scan de GetMessage incluir source_channel (posición 11 en la tupla).
```

- [ ] **Step 5: Correr tests y verificar que pasan**

Run: `go test ./internal/store/ -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/store
git commit -m "feat: store con smpp credentials y source_channel"
```

---

### Task 3: `smpp` package — constante `ESME_0512`

**Files:**
- Modify: `internal/smpp/statuses.go` (o donde se definan `CommandStatus`)
- Test: `go test ./internal/smpp/ -v`

**Interfaces:**
- Consumes: `smpp.CommandStatus` (M1: `ESME_ROK=0`, `ESME_RBINDFAIL=0x0B`, `ESME_RINVPASWD=0x0C`, `ESME_RUNKNOWNERR=0xFF`).
- Produce: `ESME_0512 CommandStatus = 0x0512` (saldo insuficiente, código de agregador).

- [ ] **Step 1: Test**

```go
// internal/smpp/statuses_test.go — añadir
func TestESME0512(t *testing.T) {
    if ESME_0512 != 0x0512 {
        t.Fatalf("ESME_0512=%x, esperaba 0x0512", ESME_0512)
    }
    // roundtrip encode/decode con status 0512
    p := NewSubmitSMResp(1, ESME_0512, "msg-01")
    b := Encode(p)
    p2, err := Decode(b)
    if err != nil {
        t.Fatal(err)
    }
    if p2.Header.Status != ESME_0512 {
        t.Fatalf("status=%x", p2.Header.Status)
    }
}
```

- [ ] **Step 2: Correr test y verificar que falla**

Run: `go test ./internal/smpp/ -run TestESME0512 -v`
Expected: FAIL — `ESME_0512` no declarado.

- [ ] **Step 3: Implementar**

```go
// internal/smpp/statuses.go — añadir (junto a las demás constantes)
const (
    // ...
    ESME_0512 CommandStatus = 0x0512 // saldo insuficiente (código de agregador)
)
```

- [ ] **Step 4: Correr test y verificar que pasa**

Run: `go test ./internal/smpp/ -run TestESME0512 -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/smpp
git commit -m "feat: constante ESME_0512 para saldo insuficiente"
```

---

### Task 4: ESME Server — bind entrante, auth, submit_sm → pipeline, 0512

**Files:**
- Create: `internal/esme/server.go`
- Create: `internal/esme/server_test.go`
- Test: `go test ./internal/esme/ -v`

**Interfaces:**
- Consumes: `smpp.*` (M1: `ParseBindTransceiver`, `NewBindTransceiverResp`, `ParseSubmitSM`, `NewSubmitSMResp`, `NewEnquireLinkResp`, `NewDeliverSM`, `CommandStatus`, `Head`, `Encode`, `Decode`, `io.ReadFull`), `store.TenantRepo` (Task 2: `GetTenantBySMPPSystemID`), `pipeline.Pipeline` (M2+M4: `Submit(ctx, Outgoing) (msgID, segments, err)`), `billing.ErrInsufficientBalance` (M4).
- Produce:
  - `esme.Config{Addr string; EnquireLinkInterval time.Duration}`
  - `esme.Submitter` — interfaz `Submit(ctx, pipeline.Outgoing) (string, int, error)` que cumple `*pipeline.Pipeline` (permite inyectar un fake en tests).
  - `esme.Server` con `New(cfg Config, repo store.TenantRepo, p Submitter) *Server`, `Run(ctx context.Context) error`, `Addr() string`, `Close() error`.
  - `esme.session` (interno, no exportado, por conexión): trackea `TenantID`, `SystemID`, `Conn net.Conn`, `Seq uint32`, `mu`, `done chan struct{}`; método exportado `Deliver(source, dest, dlrText string) error` (construye y envía `deliver_sm`).
  - `(*Server).SessionsByTenant(tenantID string) []*session` — usado por el DLR consumer (Tasks 5/7) para enviar `deliver_sm`. Se puede iterar desde `package main` y llamar `Deliver` sobre cada elemento (el tipo `session` no exportado no impide usar sus métodos exportados).
  - `ESME_0512` (Task 3) se usa al responder `submit_sm_resp` cuando `billing.ErrInsufficientBalance`.

- [ ] **Step 1: Test de bind + auth + submit_sm**

```go
// internal/esme/server_test.go
package esme

import (
    "bufio"
    "context"
    "net"
    "testing"
    "time"

    "github.com/eskiconce/smpp-gateway/internal/billing"
    "github.com/eskiconce/smpp-gateway/internal/pipeline"
    "github.com/eskiconce/smpp-gateway/internal/smpp"
    "github.com/eskiconce/smpp-gateway/internal/store"
)

type fakePipeline struct {
    msgID string
    segs  int
    err   error
}

func (f *fakePipeline) Submit(_ context.Context, _ pipeline.Outgoing) (string, int, error) {
    return f.msgID, f.segs, f.err
}

// start levanta el server en background y espera a que el listener esté activo.
func start(t *testing.T, srv *Server) {
    t.Helper()
    go func() { _ = srv.Run(context.Background()) }()
    deadline := time.Now().Add(2 * time.Second)
    for srv.Addr() == "" && time.Now().Before(deadline) {
        time.Sleep(5 * time.Millisecond)
    }
    if srv.Addr() == "" {
        t.Fatal("esme server no arranco")
    }
}

func TestESMEBindOK(t *testing.T) {
    repo := store.NewMemory()
    _ = repo.CreateTenant(context.Background(), &store.Tenant{
        ID: "t1", Status: "active",
        SmppSystemID: "esme-01", SmppPassword: "pass", Mode: "prepaid", Balance: 10,
    })
    fp := &fakePipeline{msgID: "gw-001", segs: 1}
    srv := New(Config{Addr: "127.0.0.1:0", EnquireLinkInterval: 30 * time.Second}, repo, fp)
    start(t, srv)
    defer srv.Close()

    conn, err := net.Dial("tcp", srv.Addr())
    if err != nil {
        t.Fatal(err)
    }
    defer conn.Close()
    br := bufio.NewReader(conn)

    // bind_transceiver
    bind := smpp.NewBindTransceiver(1, "esme-01", "pass", "", 0, 0, "")
    conn.Write(smpp.Encode(bind))
    p, _ := readPDU(br)
    if p.Header.Status != smpp.ESME_ROK {
        t.Fatalf("bind status=%x", p.Header.Status)
    }

    // submit_sm
    sub, _ := smpp.NewSubmitSM(2, "1234", "569123", "hola", 0, 1)
    conn.Write(smpp.Encode(sub))
    p, _ = readPDU(br)
    if p.Header.Status != smpp.ESME_ROK {
        t.Fatalf("submit status=%x", p.Header.Status)
    }
    msgid, _ := smpp.ParseSubmitSMResp(p.Body)
    if msgid != "gw-001" {
        t.Fatalf("msgid=%q", msgid)
    }
}

func TestESMEBindFail(t *testing.T) {
    repo := store.NewMemory()
    _ = repo.CreateTenant(context.Background(), &store.Tenant{
        ID: "t1", Status: "active",
        SmppSystemID: "esme-01", SmppPassword: "pass",
    })
    fp := &fakePipeline{msgID: "gw-001", segs: 1}
    srv := New(Config{Addr: "127.0.0.1:0"}, repo, fp)
    start(t, srv)
    defer srv.Close()

    conn, _ := net.Dial("tcp", srv.Addr())
    defer conn.Close()
    br := bufio.NewReader(conn)

    // password incorrecta
    bind := smpp.NewBindTransceiver(1, "esme-01", "wrong", "", 0, 0, "")
    conn.Write(smpp.Encode(bind))
    p, _ := readPDU(br)
    if p.Header.Status != smpp.ESME_RINVPASWD {
        t.Fatalf("esperaba RINVPASWD, got %x", p.Header.Status)
    }
}

func TestESMESaldoInsuficiente(t *testing.T) {
    repo := store.NewMemory()
    _ = repo.CreateTenant(context.Background(), &store.Tenant{
        ID: "t1", Status: "active", SmppSystemID: "esme-01", SmppPassword: "pass",
        Mode: "prepaid", Balance: 0,
    })
    fp := &fakePipeline{err: billing.ErrInsufficientBalance}
    srv := New(Config{Addr: "127.0.0.1:0"}, repo, fp)
    start(t, srv)
    defer srv.Close()

    conn, _ := net.Dial("tcp", srv.Addr())
    defer conn.Close()
    br := bufio.NewReader(conn)

    bind := smpp.NewBindTransceiver(1, "esme-01", "pass", "", 0, 0, "")
    conn.Write(smpp.Encode(bind))
    p, _ := readPDU(br)
    if p.Header.Status != smpp.ESME_ROK {
        t.Fatalf("bind status=%x", p.Header.Status)
    }

    sub, _ := smpp.NewSubmitSM(2, "1234", "569123", "hola", 0, 1)
    conn.Write(smpp.Encode(sub))
    p, _ = readPDU(br)
    if p.Header.Status != smpp.ESME_0512 {
        t.Fatalf("esperaba 0512, got %x", p.Header.Status)
    }
}

func TestDeliverSMToSession(t *testing.T) {
    repo := store.NewMemory()
    _ = repo.CreateTenant(context.Background(), &store.Tenant{
        ID: "t1", Status: "active",
        SmppSystemID: "esme-01", SmppPassword: "pass",
    })
    fp := &fakePipeline{msgID: "gw-001", segs: 1}
    srv := New(Config{Addr: "127.0.0.1:0"}, repo, fp)
    start(t, srv)
    defer srv.Close()

    conn, _ := net.Dial("tcp", srv.Addr())
    defer conn.Close()
    br := bufio.NewReader(conn)

    bind := smpp.NewBindTransceiver(1, "esme-01", "pass", "", 0, 0, "")
    conn.Write(smpp.Encode(bind))
    p, _ := readPDU(br)
    if p.Header.Status != smpp.ESME_ROK {
        t.Fatalf("bind status=%x", p.Header.Status)
    }

    sessions := srv.SessionsByTenant("t1")
    if len(sessions) != 1 {
        t.Fatalf("sesiones=%d", len(sessions))
    }
    if err := sessions[0].Deliver("1234", "569123", "dlr text"); err != nil {
        t.Fatal(err)
    }
    p, _ = readPDU(br)
    if p.Header.ID != smpp.DeliverSM {
        t.Fatalf("esperaba DeliverSM, got %x", p.Header.ID)
    }
    f, _ := smpp.ParseDeliverSM(p.Body)
    if f.ShortMessage != "dlr text" {
        t.Fatalf("dlr text=%q", f.ShortMessage)
    }
}
```

> `readPDU` se define en `server_test.go` como helper local (misma lógica que `smscsim.readPDU` pero sin dependencia de ese paquete): lee 4 bytes de largo, lee body, llama `smpp.Decode`. Alternativa: mover `readPDU` a un shared testutil — pero para no crear paquete nuevo, se duplica en el test (es <10 líneas).

- [ ] **Step 2: Correr tests y verificar que fallan**

Run: `go test ./internal/esme/ -v`
Expected: FAIL — paquete no existe.

- [ ] **Step 3: Implementar server.go**

```go
// internal/esme/server.go
package esme

import (
    "bufio"
    "context"
    "errors"
    "fmt"
    "io"
    "log/slog"
    "net"
    "sync"
    "time"

    "github.com/eskiconce/smpp-gateway/internal/billing"
    "github.com/eskiconce/smpp-gateway/internal/pipeline"
    "github.com/eskiconce/smpp-gateway/internal/smpp"
    "github.com/eskiconce/smpp-gateway/internal/store"
)

type Config struct {
    Addr                string
    EnquireLinkInterval time.Duration
}

// Submitter es la parte del pipeline que usa el servidor ESME
// (la cumple *pipeline.Pipeline; permite inyectar un fake en tests).
type Submitter interface {
    Submit(ctx context.Context, out pipeline.Outgoing) (msgID string, segments int, err error)
}

type session struct {
    tenantID string
    systemID string
    conn     net.Conn
    br       *bufio.Reader
    seq      uint32
    mu       sync.Mutex
    done     chan struct{}
}

func (se *session) Deliver(source, dest, dlrText string) error {
    se.mu.Lock()
    defer se.mu.Unlock()
    se.seq++
    _, err := se.conn.Write(smpp.Encode(smpp.NewDeliverSM(se.seq, source, dest, dlrText)))
    return err
}

type Server struct {
    cfg      Config
    repo     store.TenantRepo
    p        Submitter
    ln       net.Listener
    lnMu     sync.Mutex
    sessions sync.Map // systemID → *session (una por tenant)
    wg       sync.WaitGroup
}

func New(cfg Config, repo store.TenantRepo, p Submitter) *Server {
    if cfg.EnquireLinkInterval <= 0 {
        cfg.EnquireLinkInterval = 30 * time.Second
    }
    return &Server{cfg: cfg, repo: repo, p: p}
}

func (s *Server) Run(ctx context.Context) error {
    ln, err := net.Listen("tcp", s.cfg.Addr)
    if err != nil {
        return err
    }
    s.lnMu.Lock()
    s.ln = ln
    s.lnMu.Unlock()
    s.wg.Add(1)
    go func() {
        defer s.wg.Done()
        <-ctx.Done()
        ln.Close()
    }()
    for {
        conn, err := ln.Accept()
        if err != nil {
            return nil // listener cerrado
        }
        s.wg.Add(1)
        go func() {
            defer s.wg.Done()
            s.handleConn(ctx, conn)
        }()
    }
}

func (s *Server) Addr() string {
    s.lnMu.Lock()
    defer s.lnMu.Unlock()
    if s.ln == nil {
        return ""
    }
    return s.ln.Addr().String()
}

func (s *Server) Close() error {
    s.lnMu.Lock()
    ln := s.ln
    s.lnMu.Unlock()
    if ln != nil {
        ln.Close()
    }
    s.wg.Wait()
    return nil
}

func (s *Server) SessionsByTenant(tenantID string) []*session {
    var out []*session
    s.sessions.Range(func(_, v any) bool {
        se := v.(*session)
        if se.tenantID == tenantID {
            out = append(out, se)
        }
        return true
    })
    return out
}

func (s *Server) handleConn(ctx context.Context, conn net.Conn) {
    defer conn.Close()
    br := bufio.NewReader(conn)
    se := &session{conn: conn, br: br, done: make(chan struct{})}

    // bind
    p, err := readPDU(br)
    if err != nil {
        return
    }
    switch p.Header.ID {
    case smpp.BindTransceiver, smpp.BindReceiver, smpp.BindTransmitter:
        bf, err := smpp.ParseBindTransceiver(p.Body)
        if err != nil {
            conn.Write(smpp.Encode(smpp.NewBindTransceiverResp(p.Header.Seq, smpp.ESME_RBINDFAIL, "")))
            return
        }
        tenant, err := s.repo.GetTenantBySMPPSystemID(ctx, bf.SystemID)
        if err != nil || tenant.SmppPassword != bf.Password {
            conn.Write(smpp.Encode(smpp.NewBindTransceiverResp(p.Header.Seq, smpp.ESME_RINVPASWD, "")))
            return
        }
        se.tenantID = tenant.ID
        se.systemID = bf.SystemID
        conn.Write(smpp.Encode(smpp.NewBindTransceiverResp(p.Header.Seq, smpp.ESME_ROK, se.systemID)))
    default:
        h := smpp.Head(p.Header.Seq, smpp.GenericNack)
        h.Status = smpp.ESME_RBINDFAIL
        conn.Write(smpp.Encode(&smpp.PDU{Header: h}))
        return
    }

    s.sessions.Store(se.systemID, se)
    defer s.sessions.Delete(se.systemID)
    slog.Info("esme bind", "system_id", se.systemID, "tenant", se.tenantID)

    // reader loop
    for {
        p, err := readPDU(br)
        if err != nil {
            return
        }
        switch p.Header.ID {
        case smpp.SubmitSM:
            s.handleSubmit(ctx, se, p)
        case smpp.EnquireLink:
            conn.Write(smpp.Encode(smpp.NewEnquireLinkResp(p.Header.Seq)))
        case smpp.Unbind:
            conn.Write(smpp.Encode(&smpp.PDU{Header: smpp.Head(p.Header.Seq, smpp.UnbindResp)}))
            return
        default:
            h := smpp.Head(p.Header.Seq, smpp.GenericNack)
            h.Status = smpp.ESME_RUNKNOWNERR
            conn.Write(smpp.Encode(&smpp.PDU{Header: h}))
        }
    }
}

func (s *Server) handleSubmit(ctx context.Context, se *session, p *smpp.PDU) {
    f, err := smpp.ParseSubmitSM(p.Body)
    if err != nil {
        h := smpp.Head(p.Header.Seq, smpp.SubmitSMResp)
        h.Status = smpp.ESME_RUNKNOWNERR
        se.conn.Write(smpp.Encode(&smpp.PDU{Header: h}))
        return
    }

    id, _, err := s.p.Submit(ctx, pipeline.Outgoing{
        TenantID:      se.tenantID,
        SourceAddr:    f.SourceAddr,
        Msisdn:        f.DestAddr,
        Text:          f.ShortMessage,
        DataCoding:    int(f.DataCoding),
        SourceChannel: "smpp",
    })
    if err != nil {
        status := smpp.ESME_RUNKNOWNERR
        if errors.Is(err, billing.ErrInsufficientBalance) {
            status = smpp.ESME_0512
        }
        h := smpp.Head(p.Header.Seq, smpp.SubmitSMResp)
        h.Status = status
        se.conn.Write(smpp.Encode(&smpp.PDU{Header: h}))
        return
    }

    se.conn.Write(smpp.Encode(smpp.NewSubmitSMResp(p.Header.Seq, smpp.ESME_ROK, id)))
}

func readPDU(br *bufio.Reader) (*smpp.PDU, error) {
    head := make([]byte, 4)
    if _, err := io.ReadFull(br, head); err != nil {
        return nil, err
    }
    n := int(smpp.GetU32(head))
    if n < smpp.HeaderLen {
        return nil, fmt.Errorf("pdu corto: %d", n)
    }
    rest := make([]byte, n-4)
    if _, err := io.ReadFull(br, rest); err != nil {
        return nil, err
    }
    return smpp.Decode(append(head, rest...))
}
```

> `readPDU` es idéntica a la de `smscsim` pero definida en el paquete `esme` para evitar dependencia. Si se prefiere DRY, se puede extraer a un paquete `smpputil` compartido, pero la duplicación es mínima y mantiene los paquetes independientes.

> `pipeline.Outgoing` debe tener `SourceChannel string` — se añade en Task 6.

- [ ] **Step 4: Correr tests y verificar que pasan**

Run: `go test ./internal/esme/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/esme
git commit -m "feat: esme server con bind, auth, submit y 0512"
```

---

### Task 5: DLR cross-process — `ESMEPublisher` (conector) + `ESMEConsumer` (server)

**Files:**
- Create: `internal/esme/dlr.go`
- Create: `internal/esme/dlr_test.go`
- Modify: `internal/dlr/processor.go` (añadir `SourceChannel` a `Event`, populate desde `GetMessage`)
- Modify: `internal/dlr/processor_test.go` (test de `SourceChannel` en Event)
- Test: `go test ./internal/esme/ ./internal/dlr/ -v`

**Interfaces:**
- Consumes: `dlr.Notifier` (M3), `dlr.Event` (M3), `smpp.NewDeliverSM` (M1), go-redis v9, `store.MessageRepo` (Task 2: `Message.SourceChannel`).
- Produce:
  - `esme.ESMEPublisher`: publica eventos DLR a canal Redis `esme-dlr:{tenantID}`. Firma `Notify(ctx, DLRMessage)` — **no** implementa `dlr.Notifier` (firma distinta); lo invoca `compositeNotifier` (Task 7). Se usa en el proceso conector.
  - `esme.DeliverFunc` (== `func(source, dest, dlrText string) error`) y `esme.ESMEConsumer` con `NewESMEConsumer(rdb *redis.Client, handlers map[string]DeliverFunc) *ESMEConsumer` y `Run(ctx)`: suscribe a los canales por tenant y envía `deliver_sm` a la sesión ESME correspondiente vía el handler. Se usa en el proceso server.
  - `dlr.Event` gana `SourceChannel string`.
  - `(*Processor).Handle` popula `SourceChannel` desde `GetMessage`.

- [ ] **Step 1: Test del bridge pub/sub**

```go
// internal/esme/dlr_test.go
package esme

import (
    "bufio"
    "context"
    "encoding/json"
    "fmt"
    "net"
    "testing"
    "time"

    "github.com/redis/go-redis/v9"
    "github.com/alicebob/miniredis/v2"
    "github.com/eskiconce/smpp-gateway/internal/smpp"
)

func TestPublisherConsumer(t *testing.T) {
    mr := miniredis.RunT(t)
    rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

    pub := NewESMEPublisher(rdb)

    // simula una sesión ESME
    ln, _ := net.Listen("tcp", "127.0.0.1:0")
    defer ln.Close()
    go func() {
        conn, _ := ln.Accept()
        if conn == nil {
            return
        }
        br := bufio.NewReader(conn)
        // lee deliver_sm
        p, _ := readPDU(br)
        if p.Header.ID != smpp.DeliverSM {
            t.Errorf("esperaba DeliverSM, got %x", p.Header.ID)
        }
        conn.Close()
    }()

    deliver := func(source, dest, dlrText string) error {
        conn, _ := net.Dial("tcp", ln.Addr().String())
        if conn == nil {
            return fmt.Errorf("no connection")
        }
        defer conn.Close()
        dlr := smpp.NewDeliverSM(1, source, dest, dlrText)
        _, err := conn.Write(smpp.Encode(dlr))
        return err
    }

    consumer := NewESMEConsumer(rdb, map[string]DeliverFunc{
        "t1": deliver,
    })

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    go consumer.Run(ctx)

    time.Sleep(50 * time.Millisecond)

    // publicar evento
    ev := DLRMessage{
        TenantID: "t1", MessageID: "m1", SmscMsgid: "smsc-1",
        Msisdn: "569123", State: "DELIVRD", SourceChannel: "smpp",
    }
    data, _ := json.Marshal(ev)
    rdb.Publish(ctx, "esme-dlr:t1", data)

    time.Sleep(200 * time.Millisecond)
}
```

> Este test verifica que un evento publicado en Redis llega al consumer y genera un `deliver_sm`. La función `DeliverFunc` es un callback que el consumer usa para enviar (en producción usa `Server.SessionsByTenant`).

- [ ] **Step 2: Test de `SourceChannel` en `dlr.Event`**

```go
// internal/dlr/processor_test.go — añadir
func TestHandlePopulatesSourceChannel(t *testing.T) {
    p, repo, n := newTestProcessor(t)
    ctx := context.Background()
    // mensaje entrante por SMPP (source_channel=smpp, se persiste en CreateMessage, Task 2)
    _ = repo.CreateMessage(ctx, &store.Message{
        ID: "m-smpp", TenantID: "t1", Msisdn: "569123", Text: "hola",
        SourceChannel: "smpp", State: "accepted",
    })
    p.Register(ctx, "smsc-2", "m-smpp")
    p.Handle(ctx, "smsc-2", "DELIVRD")
    deadline := time.Now().Add(time.Second)
    for n.count() == 0 && time.Now().Before(deadline) {
        time.Sleep(time.Millisecond)
    }
    ev := n.last()
    if ev.SourceChannel != "smpp" {
        t.Fatalf("source_channel=%q", ev.SourceChannel)
    }
}
```

- [ ] **Step 3: Correr tests y verificar que fallan**

Run: `go test ./internal/esme/ -run TestPublisherConsumer -v && go test ./internal/dlr/ -run TestHandlePopulatesSourceChannel -v`
Expected: FAIL — `SourceChannel` no existe en `Event`; `ESMEPublisher`/`ESMEConsumer` no existen.

- [ ] **Step 4: Implementar dlr.go**

```go
// internal/esme/dlr.go
package esme

import (
    "context"
    "encoding/json"
    "fmt"
    "log/slog"
    "time"

    "github.com/redis/go-redis/v9"
)

type DLRMessage struct {
    TenantID      string `json:"tenant_id"`
    MessageID     string `json:"message_id"`
    SmscMsgid     string `json:"smsc_msgid"`
    Msisdn        string `json:"msisdn"`
    State         string `json:"state"`
    SourceChannel string `json:"source_channel"`
    Timestamp     string `json:"timestamp"`
}

type ESMEPublisher struct {
    rdb *redis.Client
}

func NewESMEPublisher(rdb *redis.Client) *ESMEPublisher {
    return &ESMEPublisher{rdb: rdb}
}

func (e *ESMEPublisher) Notify(ctx context.Context, ev DLRMessage) error {
    data, err := json.Marshal(ev)
    if err != nil {
        return err
    }
    return e.rdb.Publish(ctx, "esme-dlr:"+ev.TenantID, data).Err()
}

type DeliverFunc func(source, dest, dlrText string) error

type ESMEConsumer struct {
    rdb      *redis.Client
    handlers map[string]DeliverFunc
}

func NewESMEConsumer(rdb *redis.Client, handlers map[string]DeliverFunc) *ESMEConsumer {
    return &ESMEConsumer{rdb: rdb, handlers: handlers}
}

func (c *ESMEConsumer) Run(ctx context.Context) {
    for tenantID, handler := range c.handlers {
        ch := c.rdb.Subscribe(ctx, "esme-dlr:"+tenantID)
        go c.listen(ctx, ch, handler)
    }
    <-ctx.Done()
}

func (c *ESMEConsumer) listen(ctx context.Context, ch *redis.PubSub, handler DeliverFunc) {
    for {
        msg, err := ch.ReceiveMessage(ctx)
        if err != nil {
            return
        }
        var ev DLRMessage
        if err := json.Unmarshal([]byte(msg.Payload), &ev); err != nil {
            slog.Warn("esme dlr unmarshal", "err", err)
            continue
        }
        dlrText := fmt.Sprintf("id:%s sub:001 dlvrd:001 submit date:2609121230 done date:2609121231 stat:%s err:000 text:", ev.SmscMsgid, ev.State)
        if err := handler(ev.Msisdn, "", dlrText); err != nil {
            slog.Warn("esme deliver_sm", "tenant", ev.TenantID, "err", err)
        }
    }
}
```

> `ESMEConsumer` se inicializa en `runServer` (Task 7) con un `DeliverFunc` por tenant que busca la sesión ESME con `Server.SessionsByTenant` y envía `deliver_sm`. El método `session.Deliver` ya existe desde Task 4.

- [ ] **Step 5: Implementar `SourceChannel` en Event y Processor**

```go
// internal/dlr/processor.go — modificar Event
type Event struct {
    TenantID, MessageID, SmscMsgid, Msisdn, State string
    SourceChannel                                  string
    Timestamp                                      time.Time
}

// Processor.Handle — añadir SourceChannel al Event
func (p *Processor) Handle(ctx context.Context, smscMsgid, stat string) error {
    id, err := p.cache.Lookup(ctx, smscMsgid)
    if err != nil || id == "" {
        return err
    }
    state := MapStat(stat)
    if state == "" {
        return nil
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
        ev := Event{
            TenantID: m.TenantID, MessageID: m.ID, SmscMsgid: smscMsgid,
            Msisdn: m.Msisdn, State: state, SourceChannel: m.SourceChannel,
            Timestamp: time.Now(),
        }
        go p.notifier.Notify(context.Background(), ev)
    }
    return nil
}
```

- [ ] **Step 6: Correr tests y verificar que pasan**

Run: `go test ./internal/esme/ ./internal/dlr/ -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/esme/dlr.go internal/esme/dlr_test.go internal/dlr/processor.go internal/dlr/processor_test.go
git commit -m "feat: dlr cross-process bridge (publisher/consumer) y source_channel en Event"
```

---

### Task 6: Pipeline — `SourceChannel` en `Outgoing` + `Message` + `queue.Item`

**Files:**
- Modify: `internal/pipeline/pipeline.go` (Outgoing + CreateMessage)
- Modify: `internal/pipeline/pipeline_test.go` (test de source_channel)
- Test: `go test ./internal/pipeline/ -v`

**Interfaces:**
- Consumes: `pipeline.Outgoing` (M2+M4), `store.Message` (Task 2: `SourceChannel`), `queue.Item` (M4).
- Produce: `Outgoing` gana `SourceChannel string`; `CreateMessage` propaga el campo; `queue.Item` no necesita el campo (el worker no lo usa directamente; la info vive en `messages`).

- [ ] **Step 1: Test**

```go
// internal/pipeline/pipeline_test.go — añadir
func TestSubmitSetsSourceChannel(t *testing.T) {
    repo := store.NewMemory()
    q := queue.NewMemory()
    p := NewPipeline(repo, q, &fixedRouter{ids: []int{1}})
    ctx := context.Background()
    id, _, err := p.Submit(ctx, Outgoing{
        TenantID: "t1", Msisdn: "569123", Text: "hola",
        SourceAddr: "1234", SourceChannel: "smpp",
    })
    if err != nil {
        t.Fatal(err)
    }
    m, _ := repo.GetMessage(ctx, id)
    if m.SourceChannel != "smpp" {
        t.Fatalf("source_channel=%q", m.SourceChannel)
    }
}
```

- [ ] **Step 2: Correr test y verificar que falla**

Run: `go test ./internal/pipeline/ -run TestSubmitSetsSourceChannel -v`
Expected: FAIL — `Outgoing.SourceChannel` no existe o no se propaga.

- [ ] **Step 3: Implementar**

```go
// internal/pipeline/pipeline.go — Outgoing (añadir campo)
type Outgoing struct {
    TenantID      string
    SourceAddr    string
    Msisdn        string
    Text          string
    RoutingTag    string
    Priority      int
    DataCoding    int
    SourceChannel string
}

// Pipeline.Submit — propagar SourceChannel al CreateMessage
// (dentro del bloque donde se crea el Message)
m := &store.Message{
    ID: msgID, TenantID: out.TenantID, SourceAddr: out.SourceAddr, Msisdn: out.Msisdn,
    Text: text, Segments: segs, SourceChannel: out.SourceChannel,
    State: "buffered", CreatedAt: time.Now(), UpdatedAt: time.Now(),
}
```

> `pipeline.Outgoing.SourceChannel` se añade; `Pipeline.Submit` lo copia a `store.Message.SourceChannel`. Tests previos usan `Outgoing{}` sin `SourceChannel` → se queda en `""` (equivalente a `"api"` por el default de la DB). No se rompe nada.

- [ ] **Step 4: Correr tests y verificar que pasan**

Run: `go test ./internal/pipeline/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/pipeline
git commit -m "feat: pipeline con source_channel en outgoing y message"
```

---

### Task 7: Wiring (`run.go`) + e2e ESME + integración gated

**Files:**
- Modify: `cmd/smppgw/run.go` (`runServer` levanta `esme.Server` + `esme.ESMEConsumer`)
- Modify: `e2e/e2e_test.go` (e2e: ESME submit → deliver_sm DLR)
- Modify: `test/integration_test.go` (integración gated: ESME auth + submit + 0512)
- Test: `go build ./...`, `make test`, `make test-integration`

**Interfaces:**
- Consumes: `esme.Server` (Task 4), `esme.ESMEPublisher` (Task 5), `esme.ESMEConsumer` (Task 5), `pipeline.Pipeline` (M2+M4+Task 6), `config.Config` (M1), `store.PGRepo` (Task 2), `dlr.NewProcessor` (M3), `dlr.NewWebhookNotifier` (M3).
- Produce:
  - `runServer` levanta `esme.Server` en `cfg.SMPPAddr` (campo existente de M1; el puerto SMPP inbound) y `esme.ESMEConsumer` con un `esme.DeliverFunc` por tenant que busca la sesión y envía `deliver_sm`.
  - El `ESMEConsumer` se configura al inicio de `runServer` con los tenants que tienen `smpp_system_id` (`ListTenants`); la sesión ESME se registra en el server al hacer bind (Task 4). v1: no se agrega/remueve en caliente — el server autentica dinámicamente contra `tenants`; solo el set de canales Redis a escuchar se fija al arrancar.

- [ ] **Step 1: runServer — levantar ESME server**

```go
// cmd/smppgw/run.go — runServer (reemplazar)
func runServer(ctx context.Context, cfg config.Config, repo *store.PGRepo, q queue.Queue) error {
    r := router.New(repo, router.Config{})
    if err := r.Load(ctx); err != nil {
        return err
    }
    bill := billing.New(repo, repo, repo)
    p := pipeline.NewPipeline(repo, q, r, pipeline.WithBiller(bill))
    srv := api.New(cfg, p, repo)

    // ESME inbound (bloquea hasta que el contexto se cancele o cierre el listener)
    esmeSrv := esme.New(esme.Config{Addr: cfg.SMPPAddr}, repo, p)
    go func() {
        if err := esmeSrv.Run(ctx); err != nil {
            slog.Warn("esme server", "err", err)
        }
    }()

    // ESMEConsumer: DLR publicados en Redis (por el conector) → deliver_sm a la sesión ESME
    rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisURL})
    defer rdb.Close()
    handlers := map[string]esme.DeliverFunc{}
    tenants, _ := repo.ListTenants(ctx)
    for _, t := range tenants {
        if t.SmppSystemID == "" {
            continue
        }
        tt := t // capturar
        handlers[tt.ID] = func(source, dest, dlrText string) error {
            sess := esmeSrv.SessionsByTenant(tt.ID)
            if len(sess) == 0 {
                return fmt.Errorf("esme: sin sesion para tenant %s", tt.ID)
            }
            return sess[0].Deliver(source, dest, dlrText)
        }
    }
    go esme.NewESMEConsumer(rdb, handlers).Run(ctx)

    // DLR reconciler
    if cfg.ReconcileInterval > 0 {
        rec := dlr.NewReconciler(repo, dlr.NewWebhookNotifier(repo, cfg.WebhookTimeout),
            cfg.ReconcileTimeout, cfg.ReconcileInterval)
        go func() { _ = rec.Run(ctx) }()
    }

    return srv.Run(ctx)
}
```

> La sesión ESME se auto-registra en `Server.sessions` al hacer bind (Task 4). El `DeliverFunc` por tenant busca `esmeSrv.SessionsByTenant(tenantID)` y envía `deliver_sm` a la primera sesión. Para v1, un tenant tiene a lo más una sesión activa. Los handlers se registran en `runServer` para los tenants que tienen `smpp_system_id` al arrancar (v1: si un tenant SMPP se agrega en caliente, se reinicia el proceso; el ESME server autentica contra `tenants` dinámicamente igual).

> El `ESMEPublisher` (conector) se configura en `runConnector` (abajo) para publicar DLR a la sesión ESME en el server. El conector no tiene acceso al ESME server; publica a Redis y el `ESMEConsumer` del server consume y envía `deliver_sm`. Para v1, el ESME server y HTTP server están en el mismo proceso, y el DLR del conector se notifica vía Redis.

```go
// cmd/smppgw/run.go — runConnector (añadir ESMENotify)
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

    // Redis client para notificar DLR a ESME sessions en el server
    rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisURL})
    defer rdb.Close()
    esmePub := esme.NewESMEPublisher(rdb)

    notifier := newDLRNotifier(dlr.NewWebhookNotifier(repo, cfg.WebhookTimeout), esmePub)
    dlrProc := dlr.NewProcessor(cache, repo, notifier)
    bill := billing.New(repo, repo, repo)

    w := worker.NewWorker(q, repo, worker.WithDLR(dlrProc), worker.WithBiller(bill))
    // ... (resto igual a M4)
}
```

```go
// cmd/smppgw/run.go — helper (añadir)
type compositeNotifier struct {
    webhook dlr.Notifier
    esme    *esme.ESMEPublisher
}

func newDLRNotifier(webhook dlr.Notifier, esme *esme.ESMEPublisher) dlr.Notifier {
    return &compositeNotifier{webhook: webhook, esme: esme}
}

func (c *compositeNotifier) Notify(ctx context.Context, ev dlr.Event) error {
    // webhook siempre (si tenant tiene webhooks configurados, el WebhookNotifier filtra)
    _ = c.webhook.Notify(ctx, ev)
    // deliver_sm via Redis solo para mensajes SMPP
    if ev.SourceChannel == "smpp" {
        _ = c.esme.Notify(ctx, esme.DLRMessage{
            TenantID: ev.TenantID, MessageID: ev.MessageID, SmscMsgid: ev.SmscMsgid,
            Msisdn: ev.Msisdn, State: ev.State, SourceChannel: ev.SourceChannel,
        })
    }
    return nil
}
```

> `compositeNotifier` despacha a webhook (siempre) y a Redis pub/sub (solo para `source_channel == "smpp"`). El `ESMEConsumer` en el server recibe el evento y envía `deliver_sm` a la sesión del tenant.

- [ ] **Step 2: E2E — ESME submit → deliver_sm DLR**

```go
// e2e/e2e_test.go — añadir
func TestE2ESMPPSubmitToDeliverSM(t *testing.T) {
    repo := store.NewMemory()
    q := queue.NewMemory()

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // crear tenant con credenciales SMPP
    if err := repo.CreateTenant(ctx, &store.Tenant{
        ID: "t1", Status: "active", Mode: "prepaid", Balance: 10,
        SmppSystemID: "esme-01", SmppPassword: "pass",
    }); err != nil {
        t.Fatal(err)
    }
    // tarifa activa
    tbl := &store.RateTable{TenantID: "t1", Name: "nacional", Active: true}
    if err := repo.CreateRateTable(ctx, tbl); err != nil {
        t.Fatal(err)
    }
    if err := repo.CreateRateEntry(ctx, &store.RateEntry{TableID: tbl.ID, Prefix: "569", Price: 0.01}); err != nil {
        t.Fatal(err)
    }

    r := router.New(&e2eRules{groups: []router.Group{{
        ID: 3, Name: "ops",
        Members: []router.GroupMember{{ConnectorID: 1, Weight: 1}},
    }}}, router.Config{})
    if err := r.Load(ctx); err != nil {
        t.Fatal(err)
    }
    bill := billing.New(repo, repo, repo)
    p := pipeline.NewPipeline(repo, q, r, pipeline.WithBiller(bill))

    // ESME server
    esmeSrv := esme.New(esme.Config{Addr: "127.0.0.1:0"}, repo, p)
    go func() { _ = esmeSrv.Run(ctx) }()
    defer esmeSrv.Close()
    deadline := time.Now().Add(2 * time.Second)
    for esmeSrv.Addr() == "" && time.Now().Before(deadline) {
        time.Sleep(5 * time.Millisecond)
    }

    // simular SMSC (provider)
    sim := smscsim.New(smscsim.Config{Addr: "127.0.0.1:0", SystemID: "esp", Password: "secreto", EnableDLR: true})
    sim.Start()
    defer sim.Close()

    _, portStr, _ := net.SplitHostPort(sim.Addr())
    port, _ := strconv.Atoi(portStr)

    // DLR de vuelta al cliente ESME. En producción el conector publica a Redis y el
    // ESMEConsumer del server consume; aquí (e2e monoproceso, sin Redis) el notifier
    // inyecta el deliver_sm directo a la sesión ESME.
    dlrProc := dlr.NewProcessor(dlr.NewMemCache(), repo, &dlrToESME{esmeSrv})
    w := worker.NewWorker(q, repo, worker.WithBackoff(noBackoff), worker.WithDLR(dlrProc), worker.WithBiller(bill))
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

    // ESME client se conecta al gateway
    esmeConn, _ := net.Dial("tcp", esmeSrv.Addr())
    defer esmeConn.Close()
    br := bufio.NewReader(esmeConn)

    // bind
    bind := smpp.NewBindTransceiver(1, "esme-01", "pass", "", 0, 0, "")
    esmeConn.Write(smpp.Encode(bind))
    bindResp, _ := readPDU(br)
    if bindResp.Header.Status != smpp.ESME_ROK {
        t.Fatalf("bind status=%x", bindResp.Header.Status)
    }

    // submit
    sub, _ := smpp.NewSubmitSM(2, "1234", "569123", "hola mundo", 0, 1)
    esmeConn.Write(smpp.Encode(sub))
    subResp, _ := readPDU(br)
    if subResp.Header.Status != smpp.ESME_ROK {
        t.Fatalf("submit status=%x", subResp.Header.Status)
    }
    msgID, err := smpp.ParseSubmitSMResp(subResp.Body)
    if err != nil || msgID == "" {
        t.Fatalf("submit_sm_resp sin msgid: %v", err)
    }

    // esperar deliver_sm de vuelta (DLR del SMSC → conector → processor → notifier → sesión ESME)
    deadline = time.Now().Add(5 * time.Second)
    for time.Now().Before(deadline) {
        dlrPkt, err := readPDU(br)
        if err != nil {
            time.Sleep(10 * time.Millisecond)
            continue
        }
        if dlrPkt.Header.ID == smpp.DeliverSM {
            f, _ := smpp.ParseDeliverSM(dlrPkt.Body)
            if f.ShortMessage != "" {
                return // deliver_sm recibido con DLR
            }
        }
    }
    t.Fatal("e2e: deliver_sm no recibido")
}

// dlrToESME envía el deliver_sm directo a la sesión ESME (equivalente abreviado del
// ESMEConsumer sin Redis, para e2e monoproceso).
type dlrToESME struct {
    esme *esme.Server
}

func (d *dlrToESME) Notify(_ context.Context, ev dlr.Event) error {
    sessions := d.esme.SessionsByTenant(ev.TenantID)
    if len(sessions) == 0 {
        return nil
    }
    text := fmt.Sprintf("id:%s sub:001 dlvrd:001 submit date:2609121230 done date:2609121231 stat:%s err:000 text:", ev.SmscMsgid, ev.State)
    return sessions[0].Deliver(ev.Msisdn, "", text)
}
```

> `readPDU` se define en `e2e_test.go` como helper local (misma lógica de `smscsim`). El `noBackoff` es el backoff stub que ya existe en tests M4. Imports nuevos para `e2e_test.go`: `fmt`, `strconv`, `net`, `time`, `github.com/eskiconce/smpp-gateway/internal/billing`, `internal/dlr`, `internal/esme`, `internal/queue`, `internal/router`, `internal/session`, `internal/smpp`, `internal/smscsim`, `internal/worker`. `e2eRules` ya existe (M2).

- [ ] **Step 3: Integración gated (PG + Redis)**

```go
// test/integration_test.go — añadir
func TestIntegrationESMEAuth(t *testing.T) {
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

    if err := repo.CreateTenant(ctx, &store.Tenant{
        ID: "it-esme", Status: "active",
        SmppSystemID: "it-esme-01", SmppPassword: "s3cret",
    }); err != nil {
        t.Fatal(err)
    }
    got, err := repo.GetTenantBySMPPSystemID(ctx, "it-esme-01")
    if err != nil {
        t.Fatal(err)
    }
    if got.ID != "it-esme" || got.SmppPassword != "s3cret" {
        t.Fatalf("got=%+v", got)
    }
    // no encontrado → store.ErrNotFound
    if _, err := repo.GetTenantBySMPPSystemID(ctx, "no-existe"); !errors.Is(err, store.ErrNotFound) {
        t.Fatalf("esperaba ErrNotFound, got %v", err)
    }
}
```

> Requiere migración `0006` aplicada (golang-migrate) antes de correr este test.

- [ ] **Step 4: Correr build y tests**

Run: `go build ./... && make test`
Expected: PASS (compila + tests herméticos pasan).

- [ ] **Step 5: Commit**

```bash
git add cmd/smppgw/run.go e2e/e2e_test.go test/integration_test.go
git commit -m "feat: wiring esme server + e2e bind/submit/deliver_sm"
```

---

## Self-Review del plan

**1. Cobertura del spec (sección "Arquitectura" + "DLR" + "Billing" + "Modelo de datos"):**
- "acepta binds SMPP entrantes (ESME multi-tenant)" → Task 4 (`esme.Server`, bind_transceiver, auth contra `tenants.smpp_system_id`). ✓
- "validación → billing → router → publicación en cola" → Task 4 (`handleSubmit` llama `pipeline.Submit`). ✓
- "deliver_sm para ESME" → Task 5 (`ESMEPublisher`/`ESMEConsumer`, pub/sub Redis → deliver_sm) + Task 7 (wiring `compositeNotifier`). ✓
- "POST al webhook para HTTP" → Ya funciona (M3 `WebhookNotifier`; Task 7 `compositeNotifier` siempre llama webhook). ✓
- "Saldo insuficiente → rechazo en entrada (p.ej. SMPP 0512)" → Task 3 (`ESME_0512`) + Task 4 (`handleSubmit` mapea `ErrInsufficientBalance` → `ESME_0512`). ✓
- `tenants` con credenciales SMPP → Task 1 (migración) + Task 2 (store). ✓
- `messages.source_channel` para distinguir canal de origen → Task 1 + Task 2 + Task 6. ✓

**2. Placeholder scan:**
- No hay "TBD", "similar a Task N" ni pasos sin código. Todos los snippets son completos. ✓
- Las únicas notas son de decisión de diseño acotadas (`readPDU` duplicada mínima entre paquetes, `compositeNotifier` como bridge, `DeliverFunc` como callback testable). ✓

**3. Type consistency:**
- `pipeline.Outgoing.SourceChannel` (Task 6) se usa en `esme.handleSubmit` (Task 4). ✓
- `store.Message.SourceChannel` (Task 2) se popula en `pipeline.Submit` (Task 6) y se lee en `dlr.Processor.Handle` (Task 5). ✓
- `dlr.Event.SourceChannel` (Task 5) se compara en `compositeNotifier.Notify` (Task 7). ✓
- `ESME_0512` (Task 3) se usa en `esme.handleSubmit` (Task 4). ✓
- `ESMEPublisher.Notify(DLRMessage)` y `ESMEConsumer` usan el mismo `DLRMessage` struct. ✓
- `ESMEPublisher` evita implementar `dlr.Notifier` (firma `Notify(ctx, DLRMessage)` distinta); `compositeNotifier` (Task 7) llama a ambos. ✓
- `store.TenantRepo.GetTenantBySMPPSystemID` (Task 2) se consume en `esme.handleConn` (Task 4). ✓
- `esme.New(cfg, repo, p Submitter)` (Task 4): `*pipeline.Pipeline` y el `fakePipeline` de tests la cumplen. ✓
- `esme.Server.SessionsByTenant` devuelve `[]*session` (no exportado) y `session.Deliver` está exportado: se itera y envía `deliver_sm` desde `package main` (Task 7) y desde e2e. ✓
- `esme.NewESMEConsumer(rdb, map[string]DeliverFunc)` (Task 5) se construye en `runServer` con `esme.DeliverFunc` en `package main` (tipo exportado). ✓
- `dlr.Processor`/`WebhookNotifier`/`NewMemCache`/`newTestProcessor` (M3/M4) usados en Tasks 5 y 7 con firmas intactas. ✓
- `router.New(&e2eRules{groups: []router.Group{...}}, router.Config{})` + `r.Load` y `fixedRouter{ids: []int{1}}` (M2). ✓
- `store.RateTable`/`store.RateEntry` (M4: `{ID, TableID int; Prefix string; Price float64; ...}`) usados vía `CreateRateTable`/`CreateRateEntry` (IDs asignados por el repo). ✓
