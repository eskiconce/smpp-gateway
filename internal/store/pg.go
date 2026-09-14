package store

import (
	"context"

	"github.com/eskiconce/smpp-gateway/internal/router"
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
	_, err := r.pool.Exec(ctx, `UPDATE messages SET state=$2 WHERE id=$1`, id, state)
	return err
}

func (r *PGRepo) SetSmscMsgid(ctx context.Context, id, smscMsgid string) error {
	_, err := r.pool.Exec(ctx, `UPDATE messages SET smsc_msgid=$2, state='accepted' WHERE id=$1`, id, smscMsgid)
	return err
}

func (r *PGRepo) GetMessage(ctx context.Context, id string) (*Message, error) {
	var m Message
	err := r.pool.QueryRow(ctx,
		`SELECT id, tenant_id, source_addr, msisdn, text, segments, connector_id, route_id, state, try_count, smsc_msgid, created_at
         FROM messages WHERE id=$1`, id).
		Scan(&m.ID, &m.TenantID, &m.SourceAddr, &m.Msisdn, &m.Text, &m.Segments,
			&m.ConnectorID, &m.RouteID, &m.State, &m.TryCount, &m.SmscMsgid, &m.CreatedAt)
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
