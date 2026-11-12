// Package catalogusage is the analytics of the use of catalog items
// (FTR.NAB.CHAT-0005): the daily and monthly aggregates, the hints and states
// of an item, the report of the administration, the goal metric and the
// actions of the tab «Использование».
//
// The mode api reads the aggregates only (the drill-down reads the raw audit
// within 31 days); the mode worker is the only writer of the aggregates and
// of the goal metric; the mode cleaner removes the expired rows
// (FTR.NAB.CHAT-0005 arch §2.1, tech §«Агрегация в worker»).
package catalogusage

import (
	"encoding/csv"
	"encoding/json"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// ZeroUUID is the sentinel of the item-level rows and of the rows «outside
// the catalog» (FTR.NAB.CHAT-0005 tech §«Схема БД», arch A6): PostgreSQL
// counts NULLs as distinct in a primary key.
const ZeroUUID = "00000000-0000-0000-0000-000000000000"

// Zero is ZeroUUID as a value.
var Zero = uuid.MustParse(ZeroUUID)

// Kinds of a caller (R19).
const (
	CallerPersonal = "personal"
	CallerService  = "service"
	CallerClient   = "client"
)

// States of a row of the table (R8–R11, R15, R23).
const (
	StateActive     = "active"
	StateUnused     = "unused"
	StateDormant    = "dormant"
	StateSingleUser = "single_user"
	StateErrorsOnly = "errors_only"
	StateService    = "service_only"
	StateNotGranted = "not_granted"
	StateYoung      = "young"
	StateUnpublish  = "unpublished"
	StateUnavail    = "unavailable"
	StatePinned     = "pinned"
)

// Hints of the table (R8–R11).
const (
	HintUnused     = "unused"
	HintSingleUser = "single_user"
	HintErrorsOnly = "errors_only"
	HintDormant    = "dormant"
)

// Action values of admin_log of this feature (R31).
const (
	ActionPublish     = "catalog.publish"
	ActionUnpublish   = "catalog.unpublish"
	ActionGrantRevoke = "catalog.grant.revoke"
	ActionDelete      = "catalog.delete"
)

// ResultCodes is the list of the audit result codes (R3), the same as the
// constraint audit_result_code_chk of migration 0020.
var ResultCodes = []string{"ok", "tool_error", "transport_error", "denied", "file_rejected", "file_too_large", "auth"}

// Config is the thresholds and windows of the report (tech §«Конфигурация»).
type Config struct {
	UnusedDays     int
	ReferenceDays  int
	PinDays        int
	SingleUserMax  int
	ErrorsShare    float64
	ErrorsMinCalls int
	AuthShare      float64
	AuthMinCalls   int
	QuickDays      int
	DrilldownDays  int
	ToolsCandidate int
	Tick           time.Duration
	CloseAfter     string
	RetentionDays  int
	MonthlyMonths  int
}

// DefaultConfig is the set of the documented defaults.
func DefaultConfig() Config {
	return Config{UnusedDays: 30, ReferenceDays: 90, PinDays: 90, SingleUserMax: 1, ErrorsShare: 0.7, ErrorsMinCalls: 20,
		AuthShare: 0.5, AuthMinCalls: 20, QuickDays: 7, DrilldownDays: 31, ToolsCandidate: 10, Tick: time.Hour,
		CloseAfter: "00:15", RetentionDays: 400, MonthlyMonths: 36}
}

// Thresholds is meta.thresholds of the contract.
type Thresholds struct {
	UnusedDays     int     `json:"unusedDays"`
	ReferenceDays  int     `json:"referenceDays"`
	PinDays        int     `json:"pinDays"`
	SingleUserMax  int     `json:"singleUserMax"`
	ErrorsShare    float64 `json:"errorsShare"`
	ErrorsMinCalls int     `json:"errorsMinCalls"`
	AuthShare      float64 `json:"authShare"`
	AuthMinCalls   int     `json:"authMinCalls"`
	ToolsCandidate int     `json:"toolsCandidate"`
}

// Windows is meta.windows of the contract.
type Windows struct {
	HintDays      int `json:"hintDays"`
	ReferenceDays int `json:"referenceDays"`
	GoalDays      int `json:"goalDays"`
	DrilldownDays int `json:"drilldownDays"`
}

// Ready is meta.ready: the readiness of the windows (R6).
type Ready struct {
	Quick           bool `json:"quick"`
	Hints           bool `json:"hints"`
	Goal            bool `json:"goal"`
	ObservationDays int  `json:"observationDays"`
	ObservationLeft int  `json:"observationLeft"`
}

// GoalMetric is meta.goalMetric: the precomputed row of the goal (A16).
type GoalMetric struct {
	Value              *float64 `json:"value"`
	Baseline           *float64 `json:"baseline"`
	Delta              *float64 `json:"delta"`
	VerifiedManuallyAt *string  `json:"verifiedManuallyAt"`
}

// Meta is the common envelope of the report (tech §«API»).
type Meta struct {
	DataFrom      string     `json:"dataFrom,omitempty"`
	UpdatedAt     *time.Time `json:"updatedAt,omitempty"`
	Ready         Ready      `json:"ready"`
	Windows       Windows    `json:"windows"`
	Thresholds    Thresholds `json:"thresholds"`
	GoalMetric    GoalMetric `json:"goalMetric"`
	WindowClamped bool       `json:"windowClamped"`
}

// Thresholds converts the configuration.
func (c Config) Thresholds() Thresholds {
	return Thresholds{UnusedDays: c.UnusedDays, ReferenceDays: c.ReferenceDays, PinDays: c.PinDays, SingleUserMax: c.SingleUserMax,
		ErrorsShare: c.ErrorsShare, ErrorsMinCalls: c.ErrorsMinCalls, AuthShare: c.AuthShare, AuthMinCalls: c.AuthMinCalls,
		ToolsCandidate: c.ToolsCandidate}
}

// Windows converts the configuration.
func (c Config) Windows() Windows {
	return Windows{HintDays: c.UnusedDays, ReferenceDays: c.ReferenceDays, GoalDays: c.ReferenceDays, DrilldownDays: c.DrilldownDays}
}

// UsageRow is a row of the flat table «item × period» (R17–R19, R23).
type UsageRow struct {
	ItemID        uuid.UUID        `json:"itemId"`
	ItemName      string           `json:"itemName"`
	ItemType      string           `json:"itemType"`
	Mode          *string          `json:"mode"`
	Published     bool             `json:"published"`
	PublishedAt   *time.Time       `json:"publishedAt"`
	UnpublishedAt *time.Time       `json:"unpublishedAt"`
	DeletedAt     *time.Time       `json:"deletedAt"`
	ToolCount     int              `json:"toolCount"`
	Calls         int64            `json:"calls"`
	AdminCalls    int64            `json:"adminCalls"`
	ServiceCalls  int64            `json:"serviceCalls"`
	Users         int64            `json:"users"`
	Sessions      int64            `json:"sessions"`
	OK            int64            `json:"ok"`
	ErrorsByCode  map[string]int64 `json:"errorsByCode"`
	AuthCalls     int64            `json:"authCalls"`
	DeniedCalls   int64            `json:"deniedCalls"`
	P50Ms         *int64           `json:"p50Ms"`
	P95Ms         *int64           `json:"p95Ms"`
	LastCallAt    *time.Time       `json:"lastCallAt"`
	Calls90       int64            `json:"calls90"`
	AvailableDays int              `json:"availableDays"`
	Hints         []string         `json:"hints"`
	State         string           `json:"state"`
	// ConfirmationsRejected — number of rejected confirmations or nil («нет данных», not 0) (R17).
	ConfirmationsRejected *int64     `json:"confirmationsRejected"`
	Groups                []GroupRef `json:"groups"`
	// HumanUsers is the number of distinct people, administrators aside (R9); not in the contract.
	HumanUsers int64 `json:"-"`
	// ExplicitGrant reports an explicit grant to a group, not only «admins» (R8, R18).
	ExplicitGrant bool `json:"-"`
	// ErrShare is the share of errors among actual calls; used to sort.
	ErrShare float64 `json:"-"`
}

// GroupRef is a row of the group slice of a table row.
type GroupRef struct {
	GroupID   uuid.UUID `json:"groupId"`
	GroupName string    `json:"groupName"`
	Builtin   *string   `json:"builtin"`
	Calls     int64     `json:"calls"`
}

// GroupSlice is a row of the group slice of an item card.
type GroupSlice struct {
	GroupID      uuid.UUID        `json:"groupId"`
	GroupName    string           `json:"groupName"`
	GroupBuiltin *string          `json:"groupBuiltin"`
	Calls        int64            `json:"calls"`
	Users        int64            `json:"users"`
	OK           int64            `json:"ok"`
	ErrorsByCode map[string]int64 `json:"errorsByCode"`
}

// Interval is a publication interval (R13).
type Interval struct {
	ID            int64      `json:"id"`
	PublishedAt   time.Time  `json:"publishedAt"`
	UnpublishedAt *time.Time `json:"unpublishedAt"`
	Estimated     bool       `json:"estimated"`
	AvailableDays int        `json:"availableDays"`
	RemovedDays   int        `json:"removedDays"`
}

// Pin is a pin of an item (R12).
type Pin struct {
	ID         uuid.UUID  `json:"id"`
	ItemID     uuid.UUID  `json:"itemId"`
	ItemName   string     `json:"itemName,omitempty"`
	Reason     string     `json:"reason"`
	PinnedBy   *uuid.UUID `json:"pinnedBy"`
	PinnedAt   time.Time  `json:"pinnedAt"`
	ReviewAt   time.Time  `json:"reviewAt"`
	ReleasedAt *time.Time `json:"releasedAt"`
	Effective  bool       `json:"effective"`
}

// Candidate is a row of the panel «кандидаты на разбор» (R20).
type Candidate struct {
	ItemID    uuid.UUID  `json:"itemId"`
	ItemName  string     `json:"itemName"`
	ItemType  string     `json:"itemType"`
	Reasons   []string   `json:"reasons"`
	LastOkAt  *time.Time `json:"lastOkAt"`
	Calls     int64      `json:"calls"`
	AuthShare float64    `json:"authShare"`
	Tools     int        `json:"tools"`
}

// ItemCard is the card of an item (tech §«API»).
type ItemCard struct {
	ItemID             uuid.UUID        `json:"itemId"`
	ItemName           string           `json:"itemName"`
	ItemType           string           `json:"itemType"`
	Mode               *string          `json:"mode"`
	Published          bool             `json:"published"`
	PublishedAt        *time.Time       `json:"publishedAt"`
	UnpublishedAt      *time.Time       `json:"unpublishedAt"`
	PublishedEstimated bool             `json:"publishedEstimated"`
	LastCheckAt        *time.Time       `json:"lastCheckAt"`
	LastOkAt           *time.Time       `json:"lastOkAt"`
	Status             *string          `json:"status"`
	ToolCount          int              `json:"toolCount"`
	Calls              int64            `json:"calls"`
	AdminCalls         int64            `json:"adminCalls"`
	ServiceCalls       int64            `json:"serviceCalls"`
	Users              int64            `json:"users"`
	Sessions           int64            `json:"sessions"`
	OK                 int64            `json:"ok"`
	ErrorsByCode       map[string]int64 `json:"errorsByCode"`
	P50Ms              *int64           `json:"p50Ms"`
	P95Ms              *int64           `json:"p95Ms"`
	LastCallAt         *time.Time       `json:"lastCallAt"`
	Calls90            int64            `json:"calls90"`
	AvailableDays      int              `json:"availableDays"`
	Hints              []string         `json:"hints"`
	States             []string         `json:"states"`
	SkillActivations   *int64           `json:"skillActivations"`
	SkillSessions      *int64           `json:"skillSessions"`
	UnresolvedTasks    []UnresolvedTask `json:"unresolvedTasks"`
}

// UnresolvedTask is a task that waits for the administrator after a return
// to publication (R29).
type UnresolvedTask struct {
	TaskID    uuid.UUID `json:"taskId"`
	Title     string    `json:"title"`
	PauseKind string    `json:"pauseKind"`
}

// Tiles is the summary of the report (R16, R14).
type Tiles struct {
	DeadShare          *DeadShare `json:"deadShare"`
	PublishedToolCount ToolCount  `json:"publishedToolCount"`
	PlatformSuccess    Success    `json:"platformSuccess"`
	ResolvedCandidates Resolved   `json:"resolvedCandidates"`
	TopCalls           []TopItem  `json:"topCalls"`
	TopErrors          []TopError `json:"topErrors"`
	UnusedCount        int        `json:"unusedCount"`
	PinnedUnused       int        `json:"pinnedUnused"`
}

// DeadShare is the tile of the goal metric (R14).
type DeadShare struct {
	Value              *float64 `json:"value"`
	Dead               int      `json:"dead"`
	Total              int      `json:"total"`
	Baseline           *float64 `json:"baseline"`
	Delta              *float64 `json:"delta"`
	VerifiedManuallyAt *string  `json:"verifiedManuallyAt"`
	DroppedOut         Dropped  `json:"droppedOut"`
}

// Dropped is the block «выбыло за окно» (R14).
type Dropped struct {
	Total     int `json:"total"`
	WithCalls int `json:"withCalls"`
}

// ToolCount is the tile «инструментов в опубликованных позициях» (R16).
type ToolCount struct {
	Value int        `json:"value"`
	Trend []TrendVal `json:"trend"`
}

// TrendVal is a point of a trend.
type TrendVal struct {
	Day   string  `json:"day"`
	Value float64 `json:"value"`
}

// Success is the tile «доля успешных по платформе» (R16).
type Success struct {
	Value  *float64   `json:"value"`
	Denied int64      `json:"denied"`
	Auth   int64      `json:"auth"`
	Trend  []TrendVal `json:"trend"`
}

// Resolved is the tile «разобрано кандидатов» (R16).
type Resolved struct {
	Value      float64 `json:"value"`
	Resolved   int     `json:"resolved"`
	Candidates int     `json:"candidates"`
	Target     float64 `json:"target"`
	Note       string  `json:"note"`
}

// TopItem is a row of the top of calls.
type TopItem struct {
	ItemID   uuid.UUID `json:"itemId"`
	ItemName string    `json:"itemName"`
	Calls    int64     `json:"calls"`
}

// TopError is a row of the top of errors.
type TopError struct {
	ItemID   uuid.UUID `json:"itemId"`
	ItemName string    `json:"itemName"`
	Code     string    `json:"code"`
	Calls    int64     `json:"calls"`
}

// Impact is the preview of an action (R27, R28).
type Impact struct {
	Item            ImpactItem     `json:"item"`
	Grants          []ImpactGrant  `json:"grants"`
	Users           int            `json:"users"`
	Connections     int            `json:"connections"`
	CollectionTasks []ImpactTask   `json:"collectionTasks"`
	ServiceAgents   []ImpactAgent  `json:"serviceAgents"`
	ServiceClients  []ImpactClient `json:"serviceClients"`
	Calls90         int64          `json:"calls90"`
	LastCallAt      *time.Time     `json:"lastCallAt"`
	Publication     *Interval      `json:"publication"`
}

// ImpactItem is the item of an impact.
type ImpactItem struct {
	ID        uuid.UUID `json:"id"`
	Name      string    `json:"name"`
	Type      string    `json:"type"`
	Mode      *string   `json:"mode"`
	Published bool      `json:"published"`
}

// ImpactGrant is a group that loses the item.
type ImpactGrant struct {
	GroupID   uuid.UUID `json:"groupId"`
	GroupName string    `json:"groupName"`
	Builtin   *string   `json:"builtin"`
	Users     int       `json:"users"`
}

// ImpactTask is a collection task of a user.
type ImpactTask struct {
	TaskID    uuid.UUID `json:"taskId"`
	UserEmail string    `json:"userEmail"`
	Title     string    `json:"title"`
}

// ImpactAgent is a service agent of the configuration of which the item is.
type ImpactAgent struct {
	Name  string          `json:"name"`
	Tasks []ImpactTaskRef `json:"tasks"`
}

// ImpactTaskRef is a task of a service agent. A service agent has no
// scheduled tasks of its own in this version: its runs are one-off; the list
// is empty and kept for the shape of the contract.
type ImpactTaskRef struct {
	TaskID uuid.UUID `json:"taskId"`
	Title  string    `json:"title"`
}

// ImpactClient is a service client that runs the affected agents.
type ImpactClient struct {
	ClientID string   `json:"clientId"`
	Name     string   `json:"name"`
	Agents   []string `json:"agents"`
}

// ToolRow is a row of the drill-down (R21).
type ToolRow struct {
	Tool      string `json:"tool"`
	Calls     int64  `json:"calls"`
	OK        int64  `json:"ok"`
	Errors    int64  `json:"errors"`
	AuthCalls int64  `json:"authCalls"`
	P50Ms     *int64 `json:"p50Ms"`
	P95Ms     *int64 `json:"p95Ms"`
}

// HintInput is what the evaluation of hints and states needs (R8–R11, R15, R23).
type HintInput struct {
	ItemType      string
	Published     bool
	InDev         bool
	Deleted       bool
	Status        string
	ExplicitGrant bool
	Pinned        bool
	PublishedAt   *time.Time
	Now           time.Time
	// HumanCalls — personal calls without the calls of administrators (R8, R14).
	HumanCalls int64
	// HumanUsers — distinct people, administrators aside (R9).
	HumanUsers int64
	// ActualCalls, ActualOK — the denominators of success: without auth and denied (R9).
	ActualCalls int64
	ActualOK    int64
	// AuthCalls — calls with the auth result (R10).
	AuthCalls int64
	// ServiceCalls — service agents and clients (R19).
	ServiceCalls int64
	// AvailableDays in the hint window (R13).
	AvailableDays int
	// Calls90 — calls in the reference window (R11).
	Calls90 int64
	// ReadyHints / ReadyQuick gate the hints by the days of data (R6).
	ReadyHints bool
	ReadyQuick bool
	Cfg        Config
}

// Evaluate returns the state and the hints of an item by the priorities of
// suppression: not_granted → unpublished → unavailable → young → pinned →
// service_only → dormant → unused (R8, R11, tech §«API»).
func Evaluate(in HintInput) (string, []string) {
	skill := in.ItemType == "skill" || in.ItemType == "unknown"
	switch {
	case in.Deleted:
		return StateUnpublish, []string{}
	case !in.ExplicitGrant && !skill:
		return StateNotGranted, []string{}
	case !in.Published || in.InDev:
		return StateUnpublish, []string{}
	case in.Status == "error":
		return StateUnavail, []string{}
	case in.PublishedAt != nil && in.Now.Sub(*in.PublishedAt) < time.Duration(in.Cfg.UnusedDays)*24*time.Hour:
		return StateYoung, []string{}
	case in.Pinned:
		return StatePinned, []string{}
	}
	// hints are computed for the states below the suppression list
	hints := []string{}
	if in.ReadyQuick && !skill {
		if in.ActualCalls >= int64(in.Cfg.ErrorsMinCalls) &&
			float64(in.ActualOK)/float64(in.ActualCalls) < in.Cfg.ErrorsShare {
			hints = append(hints, HintErrorsOnly)
		}
		if in.HumanCalls >= 1 && in.HumanUsers <= int64(in.Cfg.SingleUserMax) {
			hints = append(hints, HintSingleUser)
		}
	}
	unused := false
	if in.ReadyHints && !skill && in.HumanCalls == 0 && in.AvailableDays >= in.Cfg.UnusedDays {
		unused = true
		hints = append([]string{HintUnused}, hints...)
	}
	if !skill && in.HumanCalls == 0 && in.ServiceCalls > 0 {
		// R19: a service agent's calls are not a person's use, and the item gets no hint
		return StateService, []string{}
	}
	if unused && in.Calls90 > 0 {
		if !contains(hints, HintDormant) {
			hints = append(hints, HintDormant)
		}
		return StateDormant, hints
	}
	switch {
	case unused:
		return StateUnused, hints
	case contains(hints, HintErrorsOnly):
		return StateErrorsOnly, hints
	case contains(hints, HintSingleUser):
		return StateSingleUser, hints
	}
	return StateActive, hints
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// ObservationDays is the number of days of aggregates since dataFrom (R6).
func ObservationDays(dataFrom time.Time, now time.Time) int {
	if dataFrom.IsZero() {
		return 0
	}
	d := int(now.Truncate(24*time.Hour).Sub(dataFrom.Truncate(24*time.Hour)).Hours()/24) + 1
	if d < 0 {
		return 0
	}
	return d
}

// Readiness computes meta.ready (R6).
func Readiness(dataFrom time.Time, now time.Time, cfg Config) Ready {
	days := ObservationDays(dataFrom, now)
	left := cfg.ReferenceDays - days
	if left < 0 {
		left = 0
	}
	return Ready{Quick: days >= cfg.QuickDays, Hints: days >= cfg.UnusedDays, Goal: days >= cfg.ReferenceDays,
		ObservationDays: days, ObservationLeft: left}
}

// PeriodDays is the number of days of a period, both ends included.
func PeriodDays(from, to time.Time) int {
	return int(to.Sub(from).Hours()/24) + 1
}

// ClampPeriod trims the period by dataFrom (R6): from earlier than the
// rollout is not an error; it also reports whether the period was clamped.
func ClampPeriod(from, to time.Time, dataFrom *time.Time) (time.Time, time.Time, bool) {
	if from.After(to) {
		from, to = to, from
	}
	clamped := false
	if dataFrom != nil && from.Before(*dataFrom) {
		from = *dataFrom
		clamped = true
	}
	return from, to, clamped
}

// DrilldownWindow trims the window of the drill-down to the configured days
// (R21); it reports whether the window was clamped.
func DrilldownWindow(from, to time.Time, days int) (time.Time, time.Time, bool) {
	if days <= 0 {
		days = 31
	}
	if from.After(to) {
		from, to = to, from
	}
	if PeriodDays(from, to) > days {
		return to.AddDate(0, 0, -(days - 1)), to, true
	}
	return from, to, false
}

// PinReviewAt is the default date of the review of a pin (R12, PIN-04).
func PinReviewAt(now time.Time, cfg Config) time.Time {
	return now.AddDate(0, 0, cfg.PinDays)
}

// bucketBoundsMS are the upper bounds in milliseconds: the same list scaled
// to the boundaries of the proxy histogram (100 ms … 300 s).
var bucketBoundsMS = []int64{100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000, 120000, 300000}

// Bucket is the upper bound of the bucket of a duration in milliseconds: the
// boundaries of the proxy histogram (100 ms … 300 s); everything above 300 s
// gets a bucket of its own, so the last proxy bucket stays exact.
func Bucket(ms int64) int64 {
	for _, b := range bucketBoundsMS {
		if ms <= b {
			return b
		}
	}
	return 600000 // «above 300 s»: a bucket of its own, so the last proxy bucket stays exact
}

// Percentiles recovers p50 and p95 from the merged buckets of the proxy
// (R4, AGG-07).
func Percentiles(buckets map[string]int64) (*int64, *int64) {
	if len(buckets) == 0 {
		return nil, nil
	}
	type point struct {
		bound int64
		n     int64
	}
	var pts []point
	for k, v := range buckets {
		b, err := strconv.ParseInt(strings.TrimSpace(k), 10, 64)
		if err != nil || v <= 0 {
			continue
		}
		pts = append(pts, point{b, v})
	}
	if len(pts) == 0 {
		return nil, nil
	}
	sort.Slice(pts, func(i, j int) bool { return pts[i].bound < pts[j].bound })
	var total int64
	for _, p := range pts {
		total += p.n
	}
	if total == 0 {
		return nil, nil
	}
	at := func(q float64) *int64 {
		idx := int64(math.Ceil(q * float64(total)))
		if idx < 1 {
			idx = 1
		}
		var run int64
		for _, p := range pts {
			run += p.n
			if run >= idx {
				v := p.bound
				return &v
			}
		}
		v := pts[len(pts)-1].bound
		return &v
	}
	return at(0.5), at(0.95)
}

// CSV writes the rows of the table as the export of the screen: the same
// columns, no personalities, UTF-8 with BOM, comma (R25, R26).
func CSV(rows []UsageRow) ([]byte, error) {
	var b strings.Builder
	b.WriteString("\ufeff")
	w := csv.NewWriter(&b)
	header := []string{"itemId", "itemName", "itemType", "mode", "published", "publishedAt", "unpublishedAt", "deletedAt",
		"toolCount", "calls", "adminCalls", "serviceCalls", "users", "sessions", "ok", "errorsByCode", "authCalls", "deniedCalls",
		"p50Ms", "p95Ms", "lastCallAt", "calls90", "availableDays", "state", "hints", "confirmationsRejected"}
	if err := w.Write(header); err != nil {
		return nil, err
	}
	ts := func(t *time.Time) string {
		if t == nil {
			return ""
		}
		return t.UTC().Format(time.RFC3339)
	}
	num := func(v *int64) string {
		if v == nil {
			return ""
		}
		return strconv.FormatInt(*v, 10)
	}
	for _, r := range rows {
		mode := ""
		if r.Mode != nil {
			mode = *r.Mode
		}
		mode = strings.TrimSpace(mode)
		rec := []string{r.ItemID.String(), r.ItemName, r.ItemType, mode, strconv.FormatBool(r.Published), ts(r.PublishedAt),
			ts(r.UnpublishedAt), ts(r.DeletedAt), strconv.Itoa(r.ToolCount), strconv.FormatInt(r.Calls, 10),
			strconv.FormatInt(r.AdminCalls, 10), strconv.FormatInt(r.ServiceCalls, 10), strconv.FormatInt(r.Users, 10),
			strconv.FormatInt(r.Sessions, 10), strconv.FormatInt(r.OK, 10), errorCodes(r.ErrorsByCode),
			strconv.FormatInt(r.AuthCalls, 10), strconv.FormatInt(r.DeniedCalls, 10), num(r.P50Ms), num(r.P95Ms), ts(r.LastCallAt),
			strconv.FormatInt(r.Calls90, 10), strconv.Itoa(r.AvailableDays), r.State, strings.Join(r.Hints, " "),
			num(r.ConfirmationsRejected)}
		if err := w.Write(rec); err != nil {
			return nil, err
		}
	}
	w.Flush()
	return []byte(b.String()), w.Error()
}

// errorCodes renders the error counters of a row: "auth=4;tool_error=18".
func errorCodes(m map[string]int64) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+strconv.FormatInt(m[k], 10))
	}
	return strings.Join(parts, ";")
}

// MergeErrors adds the error counters of a day.
func MergeErrors(dst map[string]int64, raw []byte) {
	if len(raw) == 0 {
		return
	}
	var m map[string]int64
	if json.Unmarshal(raw, &m) != nil {
		return
	}
	for k, v := range m {
		dst[k] += v
	}
}

// MergeLatency adds the latency buckets of a day.
func MergeLatency(dst map[string]int64, raw []byte) {
	if len(raw) == 0 {
		return
	}
	var m map[string]int64
	if json.Unmarshal(raw, &m) != nil {
		return
	}
	for k, v := range m {
		dst[k] += v
	}
}

// SuccessShare is ok / actual calls, nil without actual calls (R16).
func SuccessShare(ok, calls, auth, denied int64) *float64 {
	actual := calls - auth - denied
	if actual <= 0 {
		return nil
	}
	v := float64(ok) / float64(actual)
	return &v
}

// AuthShareOf is the share of auth among actual calls (R10).
func AuthShareOf(auth, calls, denied int64) float64 {
	actual := calls - denied
	if actual <= 0 {
		return 0
	}
	return float64(auth) / float64(actual)
}

// ToolsOf counts the tools of an item: the allowed list, or the checked one
// when the list is not set (R16, R17).
func ToolsOf(allowed []byte, tools []byte) int {
	for _, raw := range [][]byte{allowed, tools} {
		if len(raw) == 0 {
			continue
		}
		var m []json.RawMessage
		if json.Unmarshal(raw, &m) == nil {
			return len(m)
		}
		var names []string
		if json.Unmarshal(raw, &names) == nil {
			return len(names)
		}
	}
	return 0
}
