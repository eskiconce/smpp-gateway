package router

import (
	"context"
	"errors"
	"math/rand"
	"regexp"
	"strings"
)

type Rule struct {
	ID          int
	Priority    int
	TenantID    string
	From        string
	Prefix      string
	Regex       string
	RoutingTag  string
	ConnectorID int
	GroupID     int
}

type GroupMember struct {
	ConnectorID int
	Weight      int
}

type Group struct {
	ID      int
	Name    string
	Members []GroupMember
}

type Store interface {
	ListRoutingRules(ctx context.Context) ([]Rule, error)
	ListGroups(ctx context.Context) ([]Group, error)
}

type Config struct {
	DefaultConnectorID int
	Rand               *rand.Rand
}

type RouteInput struct {
	TenantID   string
	SourceAddr string
	Msisdn     string
	RoutingTag string
}

type RouteResult struct {
	RuleID     int
	Connectors []int
}

var ErrNoRoute = errors.New("router: sin ruta")

type Router struct {
	store   Store
	cfg     Config
	rules   []Rule
	groups  map[int]Group
	defConn int
	rand    *rand.Rand
}

func New(s Store, cfg Config) *Router {
	r := &Router{store: s, cfg: cfg, defConn: cfg.DefaultConnectorID, groups: map[int]Group{}}
	if cfg.Rand != nil {
		r.rand = cfg.Rand
	}
	return r
}

func (r *Router) Load(ctx context.Context) error {
	rules, err := r.store.ListRoutingRules(ctx)
	if err != nil {
		return err
	}
	sortRules(rules)
	r.rules = rules
	groups, err := r.store.ListGroups(ctx)
	if err != nil {
		return err
	}
	for _, g := range groups {
		r.groups[g.ID] = g
	}
	for _, rule := range rules {
		if rule.ConnectorID > 0 {
			r.defConn = rule.ConnectorID
		}
	}
	return nil
}

func (r *Router) Route(ctx context.Context, in RouteInput) (RouteResult, error) {
	for _, rule := range r.rules {
		if !matchTenant(rule, in.TenantID) {
			continue
		}
		if !matchFrom(rule, in.SourceAddr) {
			continue
		}
		if in.RoutingTag != "" && rule.RoutingTag != "" && rule.RoutingTag != in.RoutingTag {
			continue
		}
		if in.RoutingTag == "" && rule.RoutingTag != "" {
			continue
		}
		if !matchMsisdn(rule, in.Msisdn) {
			continue
		}
		return r.candidates(rule)
	}
	if r.defConn > 0 {
		return RouteResult{Connectors: []int{r.defConn}}, nil
	}
	return RouteResult{}, ErrNoRoute
}

func (r *Router) candidates(rule Rule) (RouteResult, error) {
	if rule.GroupID == 0 {
		return RouteResult{RuleID: rule.ID, Connectors: []int{rule.ConnectorID}}, nil
	}
	g, ok := r.groups[rule.GroupID]
	if !ok || len(g.Members) == 0 {
		return RouteResult{}, ErrNoRoute
	}
	return RouteResult{RuleID: rule.ID, Connectors: weightedOrder(g.Members, r.rand)}, nil
}

func weightedOrder(members []GroupMember, rr *rand.Rand) []int {
	total := 0
	for _, m := range members {
		total += m.Weight
	}
	var choose float64
	if rr != nil {
		choose = rr.Float64()
	} else {
		choose = rand.Float64()
	}
	chosen := pickWeighted(members, total, choose)
	order := make([]int, 0, len(members))
	order = append(order, members[chosen].ConnectorID)
	for i, m := range members {
		if i != chosen {
			order = append(order, m.ConnectorID)
		}
	}
	return order
}

func pickWeighted(members []GroupMember, total int, u float64) int {
	t := u * float64(total)
	acc := 0
	for i, m := range members {
		acc += m.Weight
		if float64(acc) >= t {
			return i
		}
	}
	return len(members) - 1
}

func matchTenant(rule Rule, tenantID string) bool {
	return rule.TenantID == "" || rule.TenantID == tenantID
}

func matchFrom(rule Rule, sourceAddr string) bool {
	return rule.From == "" || rule.From == sourceAddr
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
