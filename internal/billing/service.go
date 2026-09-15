package billing

import (
	"context"
	"errors"
	"math"
	"strings"
	"time"

	"github.com/eskiconce/smpp-gateway/internal/store"
)

var (
	ErrNoRate              = errors.New("billing: sin tarifa para el destino")
	ErrInsufficientBalance = errors.New("billing: saldo insuficiente")
)

type Service struct {
	rates   store.RateRepo
	tenants store.TenantRepo
	ledger  store.LedgerRepo
}

func New(rates store.RateRepo, tenants store.TenantRepo, ledger store.LedgerRepo) *Service {
	return &Service{rates: rates, tenants: tenants, ledger: ledger}
}

func (s *Service) Price(ctx context.Context, tenantID string, connectorID int, msisdn string, segments int) (float64, error) {
	table, err := s.rates.GetActiveRateTable(ctx, tenantID)
	if err != nil {
		return 0, err
	}
	if table == nil {
		return 0, ErrNoRate
	}
	entries, err := s.rates.ListRateEntries(ctx, table.ID)
	if err != nil {
		return 0, err
	}
	now := time.Now()
	var specificPrice, generalPrice float64
	specificLen, generalLen := -1, -1
	for _, e := range entries {
		if e.ValidFrom != nil && now.Before(*e.ValidFrom) {
			continue
		}
		if e.ValidTo != nil && now.After(*e.ValidTo) {
			continue
		}
		if !strings.HasPrefix(msisdn, e.Prefix) {
			continue
		}
		if e.ConnectorID == connectorID {
			if len(e.Prefix) > specificLen {
				specificPrice, specificLen = e.Price, len(e.Prefix)
			}
		} else if e.ConnectorID == 0 {
			if len(e.Prefix) > generalLen {
				generalPrice, generalLen = e.Price, len(e.Prefix)
			}
		}
	}
	if specificLen >= 0 {
		return round4(specificPrice * float64(segments)), nil
	}
	if generalLen >= 0 {
		return round4(generalPrice * float64(segments)), nil
	}
	return 0, ErrNoRate
}

func (s *Service) Reserve(ctx context.Context, tenantID string, amount float64) error {
	t, err := s.tenants.GetTenant(ctx, tenantID)
	if err != nil {
		return err
	}
	if t.Mode == "prepaid" && t.Balance < amount {
		return ErrInsufficientBalance
	}
	return nil
}

func (s *Service) Debit(ctx context.Context, tenantID, messageID string, amount float64) (float64, error) {
	return s.ledger.Debit(ctx, tenantID, messageID, amount)
}

func (s *Service) Credit(ctx context.Context, tenantID string, amount float64) (float64, error) {
	return s.ledger.Credit(ctx, tenantID, amount)
}

func round4(v float64) float64 {
	return math.Round(v*10000) / 10000
}
