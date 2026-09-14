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
	ID        int
	TenantID  string
	URL       string
	AuthToken string
	Events    []string
	Active    bool
	CreatedAt time.Time
}

type WebhookRepo interface {
	ListWebhooks(ctx context.Context, tenantID string) ([]Webhook, error)
	ListActiveByEvent(ctx context.Context, tenantID, event string) ([]Webhook, error)
	CreateWebhook(ctx context.Context, w *Webhook) error
	UpdateWebhook(ctx context.Context, w Webhook) error
	DeleteWebhook(ctx context.Context, id int) error
}
