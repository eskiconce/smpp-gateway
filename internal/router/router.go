// internal/router/router.go
package router

import (
	"context"
	"errors"
	"regexp"
	"strings"
)

type Rule struct {
	Priority    int
	TenantID    string
	Prefix      string
	Regex       string
	RoutingTag  string
	ConnectorID int
}

type Store interface {
	ListRoutingRules(ctx context.Context) ([]Rule, error)
}

type Config struct {
	DefaultConnectorID int
}

type Router struct {
	store   Store
	cfg     Config
	rules   []Rule
	defConn int
}

func New(s Store, cfg Config) *Router {
	return &Router{store: s, cfg: cfg, defConn: cfg.DefaultConnectorID}
}

func (r *Router) Load(ctx context.Context) error {
	rules, err := r.store.ListRoutingRules(ctx)
	if err != nil {
		return err
	}
	sortRules(rules)
	r.rules = rules
	for _, rule := range rules {
		if rule.TenantID == "" && rule.RoutingTag == "" && rule.ConnectorID > 0 {
			r.defConn = rule.ConnectorID // ultima = la de menor prioridad actúa como default
		}
	}
	return nil
}

var ErrNoRoute = errors.New("router: sin ruta")

func (r *Router) Route(ctx context.Context, tenantID, msisdn, routingTag string) (int, error) {
	for _, rule := range r.rules {
		if !matchTenant(rule, tenantID) {
			continue
		}
		if routingTag != "" && rule.RoutingTag != "" && rule.RoutingTag != routingTag {
			continue
		}
		if routingTag == "" && rule.RoutingTag != "" {
			continue
		}
		if routingTag != "" && rule.RoutingTag == "" {
			continue
		}
		if !matchMsisdn(rule, msisdn) {
			continue
		}
		return rule.ConnectorID, nil
	}
	if r.defConn > 0 {
		return r.defConn, nil
	}
	return 0, ErrNoRoute
}

func matchTenant(rule Rule, tenantID string) bool {
	return rule.TenantID == "" || rule.TenantID == tenantID
}

func matchMsisdn(rule Rule, msisdn string) bool {
	if rule.Regex != "" {
		ok, _ := regexp.MatchString(rule.Regex, msisdn)
		return ok
	}
	if rule.Prefix != "" {
		return strings.HasPrefix(msisdn, rule.Prefix)
	}
	return true
}

func sortRules(rules []Rule) {
	for i := 1; i < len(rules); i++ {
		for j := i; j > 0 && rules[j].Priority < rules[j-1].Priority; j-- {
			rules[j], rules[j-1] = rules[j-1], rules[j]
		}
	}
}
