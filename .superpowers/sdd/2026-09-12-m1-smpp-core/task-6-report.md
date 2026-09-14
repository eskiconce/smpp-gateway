# Task 6: Simulador SMSC (para tests) — Report

## What You Implemented

New package `internal/smscsim` providing a fake SMSC server for testing:

- **`smscsim.go`**: TCP server that accepts SMPP connections, reads PDUs with a buffered reader, and responds to:
  - `bind_transceiver` — validates system_id/password, returns OK or `ESME_RINVPASWD`
  - `submit_sm` — returns `submit_sm_resp` with deterministic msgid `"smsc-<seq>"`
  - `enquire_link` — returns `enquire_link_resp`
  - When `EnableDLR` is true, sends `deliver_sm` with `stat:DELIVRD` after each submit_sm
  - Unknown commands get `generic_nack` with `ESME_RUNKNOWNERR`

- **`smscsim_test.go`**: Integration test exercising bind → submit → DLR flow

## What You Tested and Test Results

**Test:** `TestSimHandshakeAndSubmit` — full lifecycle:
1. Connect to server, send `bind_transceiver` → verify `BindTransceiverResp` with `ESME_ROK`
2. Send `submit_sm` → verify `submit_sm_resp` with `msgid == "smsc-2"`
3. Read `deliver_sm` DLR → verify `stat == "DELIVRD"`

**Result:** PASS (0.05s)

## TDD Evidence

### RED (build failed — package doesn't exist)
```
undefined: New
undefined: Config
undefined: readPDU
FAIL  github.com/eskiconce/smpp-gateway/internal/smscsim [build failed]
```

### GREEN (test passes after implementation)
```
=== RUN   TestSimHandshakeAndSubmit
--- PASS: TestSimHandshakeAndSubmit (0.05s)
PASS
ok    github.com/eskiconce/smpp-gateway/internal/smscsim  0.054s
```

## Files Changed

| File | Action |
|------|--------|
| `internal/smscsim/smscsim.go` | Created (126 lines) |
| `internal/smscsim/smscsim_test.go` | Created (63 lines) |

## Self-Review Findings

### Deviation from plan's Step 3 code

1. **msgid generation uses client's seq, not server counter**: The plan's `smscsim.go` used `s.nextSeq()` for the msgid, producing `"smsc-1"`. But the test expects `"smsc-2"` (matching the client's `submit_sm` seq=2). Fixed by using `p.Header.Seq` directly: `fmt.Sprintf("smsc-%d", p.Header.Seq)`. The `nextSeq()` counter is retained only for the deliver_sm sequence number.

2. **Test Config needs `EnableDLR: true`**: The plan's test code omitted `EnableDLR: true` in the Config, but the test asserts DLR delivery. Fixed by adding the field. Without it, the server never sends deliver_sm and the test hangs until timeout.

3. **readPDU uses `binary.BigEndian.Uint32` directly** instead of `smpp.GetU32` — functionally identical, but avoids an import dependency for a trivially simple operation.

### Verification

- `go vet ./internal/smscsim/` — clean
- `gofmt -l internal/smscsim/` — clean (no formatting issues)

## Commit

```
2a799f5 feat: simulador smsc para tests
```

---

## Fix Report (Task Review — Critical/Important findings)

### What Was Changed

| Finding | Severity | Change |
|---------|----------|--------|
| Missing `Handler` callback | Critical | Added `Handler` struct with `OnSubmit func(msgid string, seq uint32) error` field on `Config`. Called before sending `submit_sm_resp`. If `OnSubmit` returns error, responds with `ESME_RUNKNOWNERR` and closes connection. If nil, default behavior preserved. |
| Missing `MsgidFor` function | Critical | Added `MsgidFor(seq uint32) string` returning `"smsc-<seq>"` — extracted from inline `fmt.Sprintf` for external test assertions. |
| Dead `conns` field | Important | Removed `conns sync.Map` from `Server` struct (was stored but never read or cleaned up). |
| No bind state enforcement | Important | Added `bound bool` flag per connection in `handleConn`. `submit_sm` from unbound connections receives `ESME_RBINDFAIL` and connection is closed. |
| `conn.Write` errors ignored | Important | Introduced `writePDU(conn, data)` helper that returns error. All write calls now check error and return (closing connection via `defer`) on write failure. |

### Tests & Verification

**Existing test:** `TestSimHandshakeAndSubmit` — PASS (0.05s)

```
=== RUN   TestSimHandshakeAndSubmit
--- PASS: TestSimHandshakeAndSubmit (0.05s)
PASS
ok  	github.com/eskiconce/smpp-gateway/internal/smscsim	0.056s
```

**`go vet ./internal/smscsim/`** — clean

**`gofmt -l internal/smscsim/`** — clean (no output)
