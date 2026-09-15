package store

import (
	"context"
	"errors"
	"time"

	"github.com/eskiconce/smpp-gateway/internal/router"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PGRepo struct {
	pool *pgxpool.Pool
}

func NewPG(ctx context.Context, dsn string) (*PGRepo, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, err
	}
	return &PGRepo{pool: pool}, nil
}

func (r *PGRepo) Close() { r.pool.Close() }

func (r *PGRepo) CreateMessage(ctx context.Context, m *Message) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO messages (id, tenant_id, source_addr, msisdn, text, segments, connector_id, route_id, state, try_count, source_channel)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		m.ID, m.TenantID, m.SourceAddr, m.Msisdn, m.Text, m.Segments, m.ConnectorID, m.RouteID, m.State, m.TryCount, m.SourceChannel)
	return err
}

func (r *PGRepo) UpdateState(ctx context.Context, id, state string) error {
	_, err := r.pool.Exec(ctx, `UPDATE messages SET state=$2, updated_at=now() WHERE id=$1`, id, state)
	return err
}

func (r *PGRepo) SetSmscMsgid(ctx context.Context, id, smscMsgid string) error {
	_, err := r.pool.Exec(ctx, `UPDATE messages SET smsc_msgid=$2, state='accepted', updated_at=now() WHERE id=$1`, id, smscMsgid)
	return err
}

func (r *PGRepo) GetMessage(ctx context.Context, id string) (*Message, error) {
	var m Message
	err := r.pool.QueryRow(ctx,
		`SELECT id, tenant_id, source_addr, msisdn, text, segments, connector_id, route_id, state, try_count, smsc_msgid, source_channel, created_at, updated_at
         FROM messages WHERE id=$1`, id).
		Scan(&m.ID, &m.TenantID, &m.SourceAddr, &m.Msisdn, &m.Text, &m.Segments,
			&m.ConnectorID, &m.RouteID, &m.State, &m.TryCount, &m.SmscMsgid, &m.SourceChannel, &m.CreatedAt, &m.UpdatedAt)
	return &m, err
}

func (r *PGRepo) IncrementTry(ctx context.Context, id string) error {
	_, err := r.pool.Exec(ctx, `UPDATE messages SET try_count = try_count + 1 WHERE id=$1`, id)
	return err
}

func (r *PGRepo) SetConnector(ctx context.Context, id string, connectorID int) error {
	_, err := r.pool.Exec(ctx, `UPDATE messages SET connector_id=$2 WHERE id=$1`, id, connectorID)
	return err
}

func (r *PGRepo) ListStaleAccepted(ctx context.Context, before time.Time) ([]Message, error) {
	rows, err := r.pool.Query(ctx, `
        SELECT id, tenant_id, source_addr, msisdn, text, segments, connector_id, route_id,
               state, try_count, smsc_msgid, source_channel, created_at, updated_at
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
			&m.SmscMsgid, &m.SourceChannel, &m.CreatedAt, &m.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

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

func (r *PGRepo) ListRoutingRules(ctx context.Context) ([]router.Rule, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, priority, COALESCE(tenant_id,''), COALESCE("from",''::text), prefix, regex, COALESCE(routing_tag,''), connector_id, COALESCE(group_id,0)
         FROM routing_rules ORDER BY priority`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var rules []router.Rule
	for rows.Next() {
		var rl router.Rule
		if err := rows.Scan(&rl.ID, &rl.Priority, &rl.TenantID, &rl.From, &rl.Prefix, &rl.Regex, &rl.RoutingTag, &rl.ConnectorID, &rl.GroupID); err != nil {
			return nil, err
		}
		rules = append(rules, rl)
	}
	return rules, rows.Err()
}

const tenantCols = `id, name, status, coalesce(routing_tag,''), balance, mode, api_key,
	smpp_system_id, smpp_password, created_at`

func scanTenants(rows pgx.Rows) ([]Tenant, error) {
	defer rows.Close()
	var out []Tenant
	for rows.Next() {
		var t Tenant
		rt := ""
		if err := rows.Scan(&t.ID, &t.Name, &t.Status, &rt, &t.Balance, &t.Mode, &t.ApiKey,
			&t.SmppSystemID, &t.SmppPassword, &t.CreatedAt); err != nil {
			return nil, err
		}
		t.RoutingTag = rt
		out = append(out, t)
	}
	return out, rows.Err()
}

func scanTenant(row pgx.Row) (*Tenant, error) {
	var t Tenant
	rt := ""
	if err := row.Scan(&t.ID, &t.Name, &t.Status, &rt, &t.Balance, &t.Mode, &t.ApiKey,
		&t.SmppSystemID, &t.SmppPassword, &t.CreatedAt); err != nil {
		return nil, err
	}
	t.RoutingTag = rt
	return &t, nil
}

func (r *PGRepo) ListTenants(ctx context.Context) ([]Tenant, error) {
	rows, err := r.pool.Query(ctx, `SELECT `+tenantCols+` FROM tenants ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	return scanTenants(rows)
}

func (r *PGRepo) GetTenant(ctx context.Context, id string) (*Tenant, error) {
	row := r.pool.QueryRow(ctx, `SELECT `+tenantCols+` FROM tenants WHERE id=$1`, id)
	t, err := scanTenant(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return t, err
}

func (r *PGRepo) GetTenantByAPIKey(ctx context.Context, apiKey string) (*Tenant, error) {
	row := r.pool.QueryRow(ctx, `SELECT `+tenantCols+` FROM tenants WHERE api_key=$1`, apiKey)
	t, err := scanTenant(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return t, err
}

func (r *PGRepo) GetTenantBySMPPSystemID(ctx context.Context, systemID string) (*Tenant, error) {
	row := r.pool.QueryRow(ctx, `SELECT `+tenantCols+` FROM tenants WHERE smpp_system_id=$1`, systemID)
	t, err := scanTenant(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return t, err
}

func (r *PGRepo) CreateTenant(ctx context.Context, t *Tenant) error {
	if t.Mode == "" {
		t.Mode = "prepaid"
	}
	if t.Status == "" {
		t.Status = "active"
	}
	_, err := r.pool.Exec(ctx, `INSERT INTO tenants (id, name, status, routing_tag, balance, mode, api_key, smpp_system_id, smpp_password)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		t.ID, t.Name, t.Status, t.RoutingTag, t.Balance, t.Mode, t.ApiKey, t.SmppSystemID, t.SmppPassword)
	return err
}

const rateTableCols = `id, tenant_id, name, active, created_at`

const rateEntryCols = `id, table_id, prefix, price, COALESCE(connector_id, 0), valid_from, valid_to`

func scanRateEntries(rows pgx.Rows) ([]RateEntry, error) {
	defer rows.Close()
	var out []RateEntry
	for rows.Next() {
		var e RateEntry
		if err := rows.Scan(&e.ID, &e.TableID, &e.Prefix, &e.Price, &e.ConnectorID, &e.ValidFrom, &e.ValidTo); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (r *PGRepo) GetActiveRateTable(ctx context.Context, tenantID string) (*RateTable, error) {
	var t RateTable
	err := r.pool.QueryRow(ctx, `SELECT `+rateTableCols+` FROM rate_tables
		WHERE tenant_id=$1 AND active ORDER BY id LIMIT 1`, tenantID).
		Scan(&t.ID, &t.TenantID, &t.Name, &t.Active, &t.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func (r *PGRepo) ListRateTables(ctx context.Context, tenantID string) ([]RateTable, error) {
	rows, err := r.pool.Query(ctx, `SELECT `+rateTableCols+` FROM rate_tables WHERE tenant_id=$1 ORDER BY id`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RateTable
	for rows.Next() {
		var t RateTable
		if err := rows.Scan(&t.ID, &t.TenantID, &t.Name, &t.Active, &t.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (r *PGRepo) ListRateEntries(ctx context.Context, tableID int) ([]RateEntry, error) {
	rows, err := r.pool.Query(ctx, `SELECT `+rateEntryCols+` FROM rate_entries WHERE table_id=$1 ORDER BY id`, tableID)
	if err != nil {
		return nil, err
	}
	return scanRateEntries(rows)
}

func (r *PGRepo) CreateRateTable(ctx context.Context, t *RateTable) error {
	return r.pool.QueryRow(ctx, `INSERT INTO rate_tables (tenant_id, name, active)
		VALUES ($1, $2, $3) RETURNING id, created_at`, t.TenantID, t.Name, t.Active).
		Scan(&t.ID, &t.CreatedAt)
}

func (r *PGRepo) CreateRateEntry(ctx context.Context, e *RateEntry) error {
	var connID any
	if e.ConnectorID != 0 {
		connID = e.ConnectorID
	}
	return r.pool.QueryRow(ctx, `INSERT INTO rate_entries (table_id, prefix, price, connector_id, valid_from, valid_to)
		VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
		e.TableID, e.Prefix, e.Price, connID, e.ValidFrom, e.ValidTo).Scan(&e.ID)
}

func (r *PGRepo) DeleteRateEntry(ctx context.Context, id int) error {
	_, err := r.pool.Exec(ctx, `DELETE FROM rate_entries WHERE id=$1`, id)
	return err
}

const txnCols = `id, tenant_id, message_id, type, amount, result_balance, created_at`

func scanTransactions(rows pgx.Rows) ([]Transaction, error) {
	defer rows.Close()
	var out []Transaction
	for rows.Next() {
		var tx Transaction
		if err := rows.Scan(&tx.ID, &tx.TenantID, &tx.MessageID, &tx.Type, &tx.Amount, &tx.ResultBalance, &tx.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, tx)
	}
	return out, rows.Err()
}

func (r *PGRepo) Debit(ctx context.Context, tenantID, messageID string, amount float64) (float64, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	var already int64
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM transactions
		WHERE message_id=$1 AND type='debit'`, messageID).Scan(&already); err != nil {
		return 0, err
	}
	if already > 0 {
		var bal float64
		if err := tx.QueryRow(ctx, `SELECT balance FROM tenants WHERE id=$1`, tenantID).Scan(&bal); err != nil {
			return 0, err
		}
		return bal, nil
	}

	var newBal float64
	if err := tx.QueryRow(ctx, `UPDATE tenants SET balance = balance - $2
		WHERE id=$1 RETURNING balance`, tenantID, amount).Scan(&newBal); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO transactions (tenant_id, message_id, type, amount, result_balance)
		VALUES ($1, $2, 'debit', $3, $4)`, tenantID, messageID, amount, newBal); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return newBal, nil
}

func (r *PGRepo) Credit(ctx context.Context, tenantID string, amount float64) (float64, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	var newBal float64
	if err := tx.QueryRow(ctx, `UPDATE tenants SET balance = balance + $2
		WHERE id=$1 RETURNING balance`, tenantID, amount).Scan(&newBal); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO transactions (tenant_id, message_id, type, amount, result_balance)
		VALUES ($1, NULL, 'credit', $2, $3)`, tenantID, amount, newBal); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return newBal, nil
}

func (r *PGRepo) ListTransactions(ctx context.Context, tenantID string, limit int) ([]Transaction, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := r.pool.Query(ctx, `SELECT `+txnCols+` FROM transactions
		WHERE tenant_id=$1 ORDER BY id DESC LIMIT $2`, tenantID, limit)
	if err != nil {
		return nil, err
	}
	return scanTransactions(rows)
}

func (r *PGRepo) ListGroups(ctx context.Context) ([]router.Group, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT g.id, g.name, COALESCE(gm.connector_id,0), COALESCE(gm.weight,1)
         FROM groups g
         LEFT JOIN group_members gm ON gm.group_id = g.id
         ORDER BY g.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	groupMap := map[int]*router.Group{}
	var order []int
	for rows.Next() {
		var gid int
		var name string
		var cid, weight int
		if err := rows.Scan(&gid, &name, &cid, &weight); err != nil {
			return nil, err
		}
		g, ok := groupMap[gid]
		if !ok {
			g = &router.Group{ID: gid, Name: name}
			groupMap[gid] = g
			order = append(order, gid)
		}
		if cid > 0 {
			g.Members = append(g.Members, router.GroupMember{ConnectorID: cid, Weight: weight})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var groups []router.Group
	for _, id := range order {
		groups = append(groups, *groupMap[id])
	}
	return groups, nil
}
