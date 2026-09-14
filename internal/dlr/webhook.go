package dlr

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/eskiconce/smpp-gateway/internal/store"
)

type WebhookNotifier struct {
	repo   store.WebhookRepo
	client *http.Client
}

func NewWebhookNotifier(repo store.WebhookRepo, timeout time.Duration) *WebhookNotifier {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &WebhookNotifier{repo: repo, client: &http.Client{Timeout: timeout}}
}

func (n *WebhookNotifier) Notify(ctx context.Context, ev Event) error {
	ws, err := n.repo.ListActiveByEvent(ctx, ev.TenantID, ev.State)
	if err != nil {
		return err
	}
	body, err := json.Marshal(map[string]any{
		"event":      ev.State,
		"message_id": ev.MessageID,
		"smsc_msgid": ev.SmscMsgid,
		"msisdn":     ev.Msisdn,
		"state":      ev.State,
		"timestamp":  ev.Timestamp.Format(time.RFC3339),
	})
	if err != nil {
		return err
	}
	for _, w := range ws {
		if err := n.post(ctx, w, body); err != nil {
			slog.Warn("webhook fallo", "url", w.URL, "err", err)
		}
	}
	return nil
}

func (n *WebhookNotifier) post(ctx context.Context, w store.Webhook, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if w.AuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+w.AuthToken)
	}
	resp, err := n.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}
