package queue

import (
	"context"
	"strconv"
)

type Item struct {
	ID          string `json:"id"`
	TenantID    string `json:"tenant_id"`
	ConnectorID int    `json:"connector_id"`
	Priority    int    `json:"priority"`
	DataCoding  int    `json:"data_coding"`
	Msisdn      string `json:"msisdn"`
	SourceAddr  string `json:"source_addr"`
	Text        string `json:"text"`
	Candidates  []int  `json:"candidates"`
	Try         int    `json:"try"`
}

type Queue interface {
	Enqueue(ctx context.Context, key string, it Item) error
	Consume(ctx context.Context, key, group string, fn func(Item) error) error
	Ack(ctx context.Context, key, group, streamID string) error
}

func Key(connectorID, priority int) string {
	return "stream:con:" + strconv.Itoa(connectorID) + ":" + strconv.Itoa(priority)
}
