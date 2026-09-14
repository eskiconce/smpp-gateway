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

type WebhookRepo interface {
	ListWebhooks(ctx context.Context, tenantID string) ([]Webhook, error)
	ListActiveByEvent(ctx context.Context, tenantID, event string) ([]Webhook, error)
	CreateWebhook(ctx context.Context, w *Webhook) error
	UpdateWebhook(ctx context.Context, w Webhook) error
	DeleteWebhook(ctx context.Context, id int) error
}
