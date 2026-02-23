// Package policy provides a rule-based routing engine for GlobalFS.
//
// The Engine evaluates an ordered set of Rules to decide which SiteMounts
// should handle a given object operation.  Rules are matched by:
//
//   - Operation type (read, write, delete)
//   - Object key pattern (exact, glob, or recursive prefix)
//
// When a rule matches, its TargetRoles field restricts the returned sites to
// those with the listed roles.  If no rule matches, or TargetRoles is empty,
// all sites are returned in default priority order: primary → backup → burst.
//
// # Key pattern syntax
//
//   - Exact:    "data/genome.bam"    — matches only that key
//   - Glob:     "*.bam"             — standard path.Match glob (* does not cross /)
//   - Glob:     "data/*"            — matches any key directly under data/
//   - Prefix:   "data/genomes/"     — trailing / = recursive prefix (matches everything under)
//   - Wildcard: ""                  — empty pattern matches every key
//
// # Rule priority
//
// Lower Priority values are evaluated first.  When two rules share the same
// Priority, the one that appears earlier in the slice passed to New wins.
package policy

import (
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/scttfrdmn/globalfs/pkg/config"
	"github.com/scttfrdmn/globalfs/pkg/site"
	"github.com/scttfrdmn/globalfs/pkg/types"
)

// OperationType identifies the kind of object operation being routed.
type OperationType string

const (
	// OperationRead covers GET and HEAD requests.
	OperationRead OperationType = "read"
	// OperationWrite covers PUT requests.
	OperationWrite OperationType = "write"
	// OperationDelete covers DELETE requests.
	OperationDelete OperationType = "delete"
)

// Rule describes a placement/routing policy for a set of object operations on
// keys matching a given pattern.
//
// Rules with a lower Priority value are evaluated first (higher precedence).
// An empty Operations slice matches any operation.
// An empty TargetRoles slice returns all sites in default priority order.
type Rule struct {
	// Name is a human-readable identifier shown in logs.
	Name string `yaml:"name"`

	// KeyPattern is matched against object keys using the rules in the
	// package-level documentation.  An empty pattern matches all keys.
	KeyPattern string `yaml:"key_pattern"`

	// Operations restricts the rule to specific operation types.
	// An empty slice matches all operations.
	Operations []OperationType `yaml:"operations"`

	// TargetRoles restricts the result to sites with the listed roles.
	// Relative ordering within the returned set preserves the input order.
	// An empty slice returns all sites in default priority order.
	TargetRoles []types.SiteRole `yaml:"target_roles"`

	// Priority controls evaluation order.  Lower values are evaluated first.
	// Ties are broken by the order rules were passed to New.
	Priority int `yaml:"priority"`
}

// matchesKey reports whether the rule's KeyPattern matches key.
func (r *Rule) matchesKey(key string) bool {
	if r.KeyPattern == "" {
		return true
	}
	// Recursive prefix match: pattern ending with "/" matches every key that
	// starts with that prefix (e.g. "genomes/" matches "genomes/sample.bam").
	if strings.HasSuffix(r.KeyPattern, "/") {
		return strings.HasPrefix(key, r.KeyPattern)
	}
	matched, err := path.Match(r.KeyPattern, key)
	if err != nil {
		// Invalid pattern syntax — treat as no match rather than panic.
		return false
	}
	return matched
}

// matchesOperation reports whether op is covered by this rule.
func (r *Rule) matchesOperation(op OperationType) bool {
	if len(r.Operations) == 0 {
		return true
	}
	for _, o := range r.Operations {
		if o == op {
			return true
		}
	}
	return false
}

// Engine evaluates an ordered set of Rules to determine which sites should
// handle a given object operation.
//
// Engine is safe for concurrent use after construction.
type Engine struct {
	rules []Rule // sorted ascending by Priority
}

// New creates an Engine from the given rules.
//
// Rules are sorted by ascending Priority; within the same Priority, the
// input order is preserved (stable sort).
func New(rules ...Rule) *Engine {
	sorted := make([]Rule, len(rules))
	copy(sorted, rules)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].Priority < sorted[j].Priority
	})
	return &Engine{rules: sorted}
}

// NewFromConfig constructs an Engine from YAML-decoded policy rule configs.
//
// Returns an error if any rule references an unknown operation or role name.
func NewFromConfig(cfgRules []config.PolicyRuleConfig) (*Engine, error) {
	rules := make([]Rule, 0, len(cfgRules))
	for _, cr := range cfgRules {
		rule := Rule{
			Name:       cr.Name,
			KeyPattern: cr.KeyPattern,
			Priority:   cr.Priority,
		}
		for _, op := range cr.Operations {
			switch OperationType(op) {
			case OperationRead, OperationWrite, OperationDelete:
				rule.Operations = append(rule.Operations, OperationType(op))
			default:
				return nil, fmt.Errorf("policy: unknown operation %q in rule %q", op, cr.Name)
			}
		}
		for _, role := range cr.TargetRoles {
			switch types.SiteRole(role) {
			case types.SiteRolePrimary, types.SiteRoleBackup, types.SiteRoleBurst:
				rule.TargetRoles = append(rule.TargetRoles, types.SiteRole(role))
			default:
				return nil, fmt.Errorf("policy: unknown role %q in rule %q", role, cr.Name)
			}
		}
		rules = append(rules, rule)
	}
	return New(rules...), nil
}

// Rules returns the engine's rules in evaluation order (ascending Priority).
// The returned slice is a copy; modifying it does not affect the engine.
func (e *Engine) Rules() []Rule {
	cp := make([]Rule, len(e.rules))
	copy(cp, e.rules)
	return cp
}

// Route returns the ordered list of sites that should handle op on key.
//
// Evaluation proceeds as follows:
//  1. Iterate rules in ascending Priority order.
//  2. The first rule whose KeyPattern and Operations both match wins.
//  3. If the winning rule has TargetRoles, return only sites with those roles,
//     preserving their relative order from the input.
//  4. If no rule matches, return all sites in default order (primary → backup →
//     burst → other).
//
// The returned slice is a new allocation.
func (e *Engine) Route(op OperationType, key string, sites []*site.SiteMount) ([]*site.SiteMount, error) {
	for i := range e.rules {
		r := &e.rules[i]
		if !r.matchesOperation(op) {
			continue
		}
		if !r.matchesKey(key) {
			continue
		}
		// Rule matched.
		if len(r.TargetRoles) == 0 {
			return DefaultOrdering(sites), nil
		}
		return filterByRoles(sites, r.TargetRoles), nil
	}
	return DefaultOrdering(sites), nil
}

// DefaultOrdering returns a copy of sites sorted primary → backup → burst.
// Sites with unrecognised roles are appended at the end in their original
// relative order.
func DefaultOrdering(sites []*site.SiteMount) []*site.SiteMount {
	rolePriority := []types.SiteRole{
		types.SiteRolePrimary,
		types.SiteRoleBackup,
		types.SiteRoleBurst,
	}
	seen := make(map[string]struct{}, len(sites))
	result := make([]*site.SiteMount, 0, len(sites))
	for _, role := range rolePriority {
		for _, s := range sites {
			if s.Role() == role {
				result = append(result, s)
				seen[s.Name()] = struct{}{}
			}
		}
	}
	for _, s := range sites {
		if _, ok := seen[s.Name()]; !ok {
			result = append(result, s)
		}
	}
	return result
}

// filterByRoles returns sites whose role appears in the given list, preserving
// the relative order from sites.
func filterByRoles(sites []*site.SiteMount, roles []types.SiteRole) []*site.SiteMount {
	roleSet := make(map[types.SiteRole]struct{}, len(roles))
	for _, r := range roles {
		roleSet[r] = struct{}{}
	}
	result := make([]*site.SiteMount, 0, len(sites))
	for _, s := range sites {
		if _, ok := roleSet[s.Role()]; ok {
			result = append(result, s)
		}
	}
	return result
}
