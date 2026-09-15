package store

import (
	"context"
	"time"
)

type Message struct {
	ID            string
	TenantID      string
	SourceAddr    string
	Msisdn        string
	Text          string
	Segments      int
	ConnectorID   int
	RouteID       int
	TryCount      int
	State         string
	SmscMsgid     string
	SourceChannel string
	Amount        float64
	CreatedAt     time.Time
	UpdatedAt     time.Time
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
	ID             string    `json:"id"`
	Name           string    `json:"name"`
	Status         string    `json:"status"`
	RoutingTag     string    `json:"routing_tag"`
	Balance        float64   `json:"balance"`
	Mode           string    `json:"mode"`
	ApiKey         string    `json:"api_key"`
	SmppSystemID   string    `json:"smpp_system_id"`
	SmppPassword   string    `json:"smpp_password"`
	CreatedAt      time.Time `json:"created_at"`
}

type TenantRepo interface {
	ListTenants(ctx context.Context) ([]Tenant, error)
	GetTenant(ctx context.Context, id string) (*Tenant, error)
	GetTenantByAPIKey(ctx context.Context, apiKey string) (*Tenant, error)
	GetTenantBySMPPSystemID(ctx context.Context, systemID string) (*Tenant, error)
	CreateTenant(ctx context.Context, t *Tenant) error
}

type RateTable struct {
	ID        int       `json:"id"`
	TenantID  string    `json:"tenant_id"`
	Name      string    `json:"name"`
	Active    bool      `json:"active"`
	CreatedAt time.Time `json:"created_at"`
}

type RateEntry struct {
	ID          int        `json:"id"`
	TableID     int        `json:"table_id"`
	Prefix      string     `json:"prefix"`
	Price       float64    `json:"price"`
	ConnectorID int        `json:"connector_id"`
	ValidFrom   *time.Time `json:"valid_from"`
	ValidTo     *time.Time `json:"valid_to"`
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
	ID            int64     `json:"id"`
	TenantID      string    `json:"tenant_id"`
	MessageID     string    `json:"message_id"`
	Type          string    `json:"type"`
	Amount        float64   `json:"amount"`
	ResultBalance float64   `json:"result_balance"`
	CreatedAt     time.Time `json:"created_at"`
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
