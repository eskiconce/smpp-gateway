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
	State       string
	TryCount    int
	SmscMsgid   string
	CreatedAt   time.Time
}

type MessageRepo interface {
	CreateMessage(ctx context.Context, m *Message) error
	UpdateState(ctx context.Context, id, state string) error
	SetSmscMsgid(ctx context.Context, id, smscMsgid string) error
	GetMessage(ctx context.Context, id string) (*Message, error)
}
