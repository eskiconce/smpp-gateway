package store

import (
	"context"
	"time"
)

type Message struct {
	ID          string
	TenantID    string
	SourceAddr  string
	Msisdn      string
	Text        string
	Segments    int
	ConnectorID int
	RouteID     int
	TryCount    int
	State       string
	SmscMsgid   string
	CreatedAt   time.Time
	UpdatedAt   time.Time
	Amount      float64
}

type MessageRepo interface {
	CreateMessage(ctx context.Context, m *Message) error
	UpdateState(ctx context.Context, id, state string) error
	SetSmscMsgid(ctx context.Context, id, smscMsgid string) error
	GetMessage(ctx context.Context, id string) (*Message, error)
	IncrementTry(ctx context.Context, id string) error
	SetConnector(ctx context.Context, id string, connectorID int) error
	ListStaleAccepted(ctx context.Context, before time.Time) ([]Message, error)
}

type Webhook struct {
	ID        int       `json:"id"`
	TenantID  string    `json:"tenant_id"`
	URL       string    `json:"url"`
	AuthToken string    `json:"auth_token"`
	Events    []string  `json:"events"`
	Active    bool      `json:"active"`
	CreatedAt time.Time `json:"created_at"`
}

type Tenant struct {
	ID         string
	Name       string
	Status     string
	RoutingTag string
	Balance    float64
	Mode       string
	ApiKey     string
	CreatedAt  time.Time
}

type TenantRepo interface {
	ListTenants(ctx context.Context) ([]Tenant, error)
	GetTenant(ctx context.Context, id string) (*Tenant, error)
	GetTenantByAPIKey(ctx context.Context, apiKey string) (*Tenant, error)
	CreateTenant(ctx context.Context, t *Tenant) error
}

type RateTable struct {
	ID        int
	TenantID  string
	Name      string
	Active    bool
	CreatedAt time.Time
}

type RateEntry struct {
	ID          int
	TableID     int
	Prefix      string
	Price       float64
	ConnectorID int
	ValidFrom   *time.Time
	ValidTo     *time.Time
}

type RateRepo interface {
	GetActiveRateTable(ctx context.Context, tenantID string) (*RateTable, error)
	ListRateTables(ctx context.Context, tenantID string) ([]RateTable, error)
	ListRateEntries(ctx context.Context, tableID int) ([]RateEntry, error)
	CreateRateTable(ctx context.Context, t *RateTable) error
	CreateRateEntry(ctx context.Context, e *RateEntry) error
	DeleteRateEntry(ctx context.Context, id int) error
}

type Transaction struct {
	ID            int64
	TenantID      string
	MessageID     string
	Type          string
	Amount        float64
	ResultBalance float64
	CreatedAt     time.Time
}

type LedgerRepo interface {
	Debit(ctx context.Context, tenantID, messageID string, amount float64) (float64, error)
	Credit(ctx context.Context, tenantID string, amount float64) (float64, error)
	ListTransactions(ctx context.Context, tenantID string, limit int) ([]Transaction, error)
}

type WebhookRepo interface {
	ListWebhooks(ctx context.Context, tenantID string) ([]Webhook, error)
	ListActiveByEvent(ctx context.Context, tenantID, event string) ([]Webhook, error)
	CreateWebhook(ctx context.Context, w *Webhook) error
	UpdateWebhook(ctx context.Context, w Webhook) error
	DeleteWebhook(ctx context.Context, id int) error
}
