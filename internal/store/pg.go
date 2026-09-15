package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
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

func itoa(i int) string { return fmt.Sprintf("%d", i) }

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
	where := r.sinceAndTenant("", tenantID, since)
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
	where := r.sinceAndTenant("", tenantID, since)
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
	where := r.sinceAndTenant("", tenantID, since)
	var n int
	err := r.pool.QueryRow(ctx, `SELECT count(*) FROM messages`+where.cond, where.args...).Scan(&n)
	return n, err
}

type whereClause struct {
	cond string
	args []any
}

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
