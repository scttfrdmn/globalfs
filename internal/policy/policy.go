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
// Patterns are matched with [doublestar.Match], so a single * stops at a / and
// ** crosses them:
//
//   - Exact:     "data/genome.bam"  — matches only that key
//   - Glob:      "data/*"           — any key directly under data/, one level only
//   - Recursive: "data/**"          — any key under data/ at any depth, and "data" itself
//   - Recursive: "**/*.bam"         — any .bam at any depth, including the root
//   - Prefix:    "data/genomes/"    — trailing / = recursive prefix (matches everything under)
//   - Wildcard:  ""                 — empty pattern matches every key
//
// Note that "*.bam" matches only root-level keys, because one * does not cross a
// /. Write "**/*.bam" for the recursive form. This is the standard glob rule and
// not a GlobalFS quirk, but it is the one that surprises people.
//
// Matching used [path.Match] before v0.3.0, to which ** is just two stars and
// therefore still barred from crossing / (#100). Every recursive pattern the
// documentation and the shipped examples advertised — "/inputs/**" among them —
// matched nothing below the first level. The failure was silent in the worst way:
// a pattern that matches nothing is not an error, so the rule simply never fired
// and its objects were placed by whatever rule won instead.
//
// # Rule priority
//
// Lower Priority values are evaluated first.  When two rules share the same
// Priority, the one that appears earlier in the slice passed to New wins.
package policy

import (
	"fmt"
	"sort"
	"strings"

	"github.com/bmatcuk/doublestar/v4"

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
	// Kept as a plain string prefix rather than folded into the glob, because it
	// is exactly a prefix test and a pattern containing glob metacharacters
	// before its trailing / would otherwise change meaning.
	if strings.HasSuffix(r.KeyPattern, "/") {
		return strings.HasPrefix(key, r.KeyPattern)
	}
	matched, err := doublestar.Match(r.KeyPattern, key)
	if err != nil {
		// Invalid pattern syntax — treat as no match rather than panic.
		// ValidatePattern rejects these at construction, so reaching this is a
		// rule built by hand rather than through New/NewFromConfig.
		return false
	}
	return matched
}

// ValidateKeyPattern reports whether pattern is syntactically usable.
//
// It is [config.ValidateKeyPattern], re-exported here so that a caller working
// with policy.Rule does not have to reach into the config package to check one.
// The implementation lives there because pkg/config validates the same patterns
// during Load and cannot import this package — policy imports config, not the
// other way round.
func ValidateKeyPattern(pattern string) error {
	return config.ValidateKeyPattern(pattern)
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
// Returns an error if any rule references an unknown operation or role name, or
// carries a key pattern that cannot be parsed (#100).  [New] cannot make the
// pattern check — it takes already-typed rules and returns no error — so a rule
// assembled in Go is trusted; every rule that arrives from a config file is not.
func NewFromConfig(cfgRules []config.PolicyRuleConfig) (*Engine, error) {
	rules := make([]Rule, 0, len(cfgRules))
	for _, cr := range cfgRules {
		if err := ValidateKeyPattern(cr.KeyPattern); err != nil {
			return nil, fmt.Errorf("policy: rule %q: %w", cr.Name, err)
		}
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
