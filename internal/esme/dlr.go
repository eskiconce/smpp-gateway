package esme

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/redis/go-redis/v9"
)

type DLRMessage struct {
	TenantID      string `json:"tenant_id"`
	MessageID     string `json:"message_id"`
	SmscMsgid     string `json:"smsc_msgid"`
	Msisdn        string `json:"msisdn"`
	State         string `json:"state"`
	SourceChannel string `json:"source_channel"`
	Timestamp     string `json:"timestamp"`
}

type ESMEPublisher struct {
	rdb *redis.Client
}

func NewESMEPublisher(rdb *redis.Client) *ESMEPublisher {
	return &ESMEPublisher{rdb: rdb}
}

func (e *ESMEPublisher) Notify(ctx context.Context, ev DLRMessage) error {
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	return e.rdb.Publish(ctx, "esme-dlr:"+ev.TenantID, data).Err()
}

type DeliverFunc func(source, dest, dlrText string) error

type ESMEConsumer struct {
	rdb      *redis.Client
	handlers map[string]DeliverFunc
}

func NewESMEConsumer(rdb *redis.Client, handlers map[string]DeliverFunc) *ESMEConsumer {
	return &ESMEConsumer{rdb: rdb, handlers: handlers}
}

func (c *ESMEConsumer) Run(ctx context.Context) {
	for tenantID, handler := range c.handlers {
		ch := c.rdb.Subscribe(ctx, "esme-dlr:"+tenantID)
		go c.listen(ctx, ch, handler)
	}
	<-ctx.Done()
}

func (c *ESMEConsumer) listen(ctx context.Context, ch *redis.PubSub, handler DeliverFunc) {
	for {
		msg, err := ch.ReceiveMessage(ctx)
		if err != nil {
			return
		}
		var ev DLRMessage
		if err := json.Unmarshal([]byte(msg.Payload), &ev); err != nil {
			slog.Warn("esme dlr unmarshal", "err", err)
			continue
		}
		dlrText := fmt.Sprintf("id:%s sub:001 dlvrd:001 submit date:2609121230 done date:2609121231 stat:%s err:000 text:", ev.SmscMsgid, ev.State)
		if err := handler(ev.Msisdn, "", dlrText); err != nil {
			slog.Warn("esme deliver_sm", "tenant", ev.TenantID, "err", err)
		}
	}
}
