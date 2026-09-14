package store

import (
	"context"
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
		`INSERT INTO messages (id, tenant_id, source_addr, msisdn, text, segments, connector_id, route_id, state, try_count)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		m.ID, m.TenantID, m.SourceAddr, m.Msisdn, m.Text, m.Segments, m.ConnectorID, m.RouteID, m.State, m.TryCount)
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
		`SELECT id, tenant_id, source_addr, msisdn, text, segments, connector_id, route_id, state, try_count, smsc_msgid, created_at, updated_at
         FROM messages WHERE id=$1`, id).
		Scan(&m.ID, &m.TenantID, &m.SourceAddr, &m.Msisdn, &m.Text, &m.Segments,
			&m.ConnectorID, &m.RouteID, &m.State, &m.TryCount, &m.SmscMsgid, &m.CreatedAt, &m.UpdatedAt)
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
		`SELECT id, priority, COALESCE(tenant_id,''), COALESCE(from_addr,''), prefix, regex, COALESCE(routing_tag,''), connector_id, COALESCE(group_id,0)
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

func (r *PGRepo) ListGroups(ctx context.Context) ([]router.Group, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT g.id, g.name, COALESCE(gm.connector_id,0), COALESCE(gm.weight,1)
         FROM routing_groups g
         LEFT JOIN routing_group_members gm ON gm.group_id = g.id
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
