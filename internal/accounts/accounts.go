// Package accounts archives, restores and purges the accounts of employees
// (FTR.NAB.CMN-0002 R18–R22; arch §7; tech §10). Archiving closes the account
// at once (step mark) and the worker finishes the idempotent steps from the
// last unfinished one; restoring is explicit only — by an administrator or
// a service client; a daily purge deletes accounts after the retention.
package accounts

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/domain"
	"github.com/GreenOnGrey/nabu-core/internal/ledger"
	"github.com/GreenOnGrey/nabu-core/internal/platform/events"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
	"github.com/GreenOnGrey/nabu-core/internal/platform/storage"
)

// Results per email (tech §2.3).
const (
	Archived        = "archived"
	Restored        = "restored"
	AlreadyArchived = "already_archived"
	AlreadyActive   = "already_active"
	NotFound        = "not_found"
	Purged          = "purged"
)

// Group agents of an archived owner (R18).
const (
	GroupsTransfer = "transfer"
	GroupsDisable  = "disable"
)

// Steps of archiving (tech §10), in order.
var Steps = []string{"sessions", "tasks", "sandbox", "connections", "telegram", "groups", "audit"}

// DefaultRetention is the retention of archived accounts (R20).
const DefaultRetention = 180

// Result is the outcome for one email.
type Result struct {
	Email  string     `json:"email"`
	Result string     `json:"result"`
	UserID *uuid.UUID `json:"userId,omitempty"`
}

// Actor is who archives or restores: an administrator or a service client.
type Actor struct {
	UserID    *uuid.UUID
	Email     string
	ClientID  *uuid.UUID
	Client    string
	Initiator string // the initiator passed by the client (R22)
}

// By is the archived_by text: the email of the administrator or "api:<client>".
func (a Actor) By() string {
	if a.Client != "" {
		return "api:" + a.Client
	}
	return a.Email
}

// Sandboxes stops sandboxes (worker).
type Sandboxes interface {
	Stop(ctx context.Context, uid uuid.UUID) error
}

// Groups hands over the group agents of an archived owner and purges expired ones.
type Groups interface {
	OwnerArchived(ctx context.Context, uid uuid.UUID, mode string, to *uuid.UUID) error
	Purge(ctx context.Context) (int, error)
	Retention(days int)
}

// Service archives, restores and purges.
type Service struct {
	Pool   *pgxpool.Pool
	S3     storage.Storage
	Events events.Publisher
	Ledger *ledger.Ledger
	// Worker dependencies.
	Sandboxes Sandboxes
	// CloseSessions closes the agent sessions of a user with snapshots.
	CloseSessions func(ctx context.Context, uid uuid.UUID) error
	Groups        Groups
	now           func() time.Time
}

func (s *Service) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

// Retention is the retention of archived accounts in days.
func (s *Service) Retention(ctx context.Context) int {
	var raw []byte
	if err := s.Pool.QueryRow(ctx, `SELECT value FROM settings WHERE key = 'archive'`).Scan(&raw); err != nil {
		return DefaultRetention
	}
	var v struct {
		RetentionDays int `json:"retentionDays"`
	}
	if json.Unmarshal(raw, &v) != nil || v.RetentionDays <= 0 {
		return DefaultRetention
	}
	return v.RetentionDays
}

// SetRetention changes the retention; dryRun only counts the accounts the
// next purge deletes (AR-09). Saving recalculates purge_after of all archived.
func (s *Service) SetRetention(ctx context.Context, days int, dryRun bool) (int, error) {
	if days < 1 || days > 3650 {
		return 0, apperr.Unprocessable("invalid_retention", "the retention is 1–3650 days").With("field", "retentionDays")
	}
	var n int
	if err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM users WHERE status = 'archived' AND archived_at + make_interval(days => $1::int) < now()`,
		days).Scan(&n); err != nil {
		return 0, err
	}
	if dryRun {
		return n, nil
	}
	err := postgres.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		b, _ := json.Marshal(map[string]int{"retentionDays": days})
		if _, err := tx.Exec(ctx, `INSERT INTO settings (key, value, updated_at) VALUES ('archive', $1, now())
			ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()`, b); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE users SET purge_after = archived_at + make_interval(days => $1::int) WHERE status = 'archived'`, days)
		return err
	})
	if s.Groups != nil {
		s.Groups.Retention(days)
	}
	return n, err
}

func normalize(emails []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, e := range emails {
		e = domain.NormalizeEmail(e)
		if e != "" && !seen[e] {
			seen[e] = true
			out = append(out, e)
		}
	}
	return out
}

func (s *Service) purgedEmail(ctx context.Context, email string) bool {
	var ok bool
	_ = s.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM purged_accounts WHERE email = $1)`, email).Scan(&ok)
	return ok
}

// ArchiveOptions are the options of archiving.
type ArchiveOptions struct {
	GroupAgents string     `json:"groupAgents"` // transfer | disable
	TransferTo  *uuid.UUID `json:"transferTo"`
}

func (s *Service) defaultAdmin(ctx context.Context, exclude []string) *uuid.UUID {
	var id uuid.UUID
	if err := s.Pool.QueryRow(ctx, `SELECT id FROM users WHERE is_admin AND status = 'active' AND NOT (email = ANY($1))
		ORDER BY created_at LIMIT 1`, exclude).Scan(&id); err != nil {
		return nil
	}
	return &id
}

// Archive archives accounts by email (R18, R22); a repeated call answers
// already_archived (AR-13).
func (s *Service) Archive(ctx context.Context, emails []string, opt ArchiveOptions, by Actor) ([]Result, error) {
	list := normalize(emails)
	if len(list) == 0 {
		return nil, apperr.Unprocessable("emails_required", "a list of emails is required").With("field", "emails")
	}
	if len(list) > 500 {
		return nil, apperr.Unprocessable("too_many_emails", "at most 500 emails per call").With("field", "emails")
	}
	switch opt.GroupAgents {
	case "":
		opt.GroupAgents = GroupsDisable
	case GroupsTransfer, GroupsDisable:
	default:
		return nil, apperr.Unprocessable("invalid_group_agents", "groupAgents is transfer or disable").With("field", "groupAgents")
	}
	if opt.GroupAgents == GroupsTransfer {
		if opt.TransferTo == nil {
			if by.UserID != nil && !contains(list, by.Email) {
				opt.TransferTo = by.UserID
			} else {
				opt.TransferTo = s.defaultAdmin(ctx, list)
			}
		}
		var ok bool
		if opt.TransferTo != nil {
			_ = s.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM users WHERE id = $1 AND is_admin AND status = 'active' AND NOT (email = ANY($2)))`,
				*opt.TransferTo, list).Scan(&ok)
		}
		if !ok {
			return nil, apperr.Unprocessable("invalid_transfer", "group agents are transferred to an active administrator").With("field", "transferTo")
		}
	}
	if by.UserID != nil && contains(list, by.Email) {
		return nil, apperr.Conflict("cannot_archive_self", "you cannot archive your own account")
	}
	retention := s.Retention(ctx)
	params, _ := json.Marshal(map[string]any{"groupAgents": opt.GroupAgents, "transferTo": opt.TransferTo, "by": by.By(),
		"clientId": by.ClientID, "initiator": by.Initiator, "actorId": by.UserID})
	out := make([]Result, 0, len(list))
	for _, email := range list {
		var r Result
		r.Email = email
		err := postgres.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
			var id uuid.UUID
			var status string
			err := tx.QueryRow(ctx, `SELECT id, status FROM users WHERE email = $1 AND created_via <> 'group' FOR UPDATE`, email).Scan(&id, &status)
			if postgres.IsNoRows(err) {
				r.Result = NotFound
				return nil
			}
			if err != nil {
				return err
			}
			r.UserID = &id
			if status == "archived" {
				r.Result = AlreadyArchived
				return nil
			}
			now := s.clock()
			if _, err := tx.Exec(ctx, `UPDATE users SET status = 'archived', archived_at = $2::timestamptz, archived_by = $3,
				purge_after = $2::timestamptz + make_interval(days => $4::int), link_identity_on_next_login = false WHERE id = $1`, id, now, by.By(), retention); err != nil {
				return err
			}
			// the site closes at once; the worker finishes the other steps
			if _, err := tx.Exec(ctx, `DELETE FROM user_sessions WHERE user_id = $1`, id); err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `INSERT INTO account_jobs (user_id, kind, step, params) VALUES ($1,'archive',$2,$3)`, id, Steps[0], params)
			r.Result = Archived
			return err
		})
		if err != nil {
			s.accessChanged(ctx) // the accounts archived before the failure are committed
			return nil, err
		}
		if r.Result == NotFound && s.purgedEmail(ctx, email) {
			r.Result = Purged
		}
		out = append(out, r)
	}
	s.accessChanged(ctx)
	return out, nil
}

func contains(list []string, e string) bool {
	e = domain.NormalizeEmail(e)
	for _, x := range list {
		if x == e {
			return true
		}
	}
	return false
}

func (s *Service) accessChanged(ctx context.Context) {
	if s.Events != nil {
		s.Events.Publish(ctx, events.Event{Type: "access.changed", Data: map[string]any{"kind": ""}})
	}
}

// ─── the worker steps ───────────────────────────────────────────────

// RunJobs performs the unfinished steps of archiving (AR-02: a failure
// resumes at the step that failed).
func (s *Service) RunJobs(ctx context.Context) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		s.runPending(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (s *Service) runPending(ctx context.Context) {
	// a job that failed waits for the next pass and does not hold back the others
	failed := []uuid.UUID{}
	for ctx.Err() == nil {
		id, err := s.runOne(ctx, failed)
		if err != nil {
			slog.WarnContext(ctx, "account job", "job", id, "err", err)
			if id == uuid.Nil {
				return
			}
			failed = append(failed, id)
			continue
		}
		if id == uuid.Nil {
			return
		}
	}
}

type jobParams struct {
	GroupAgents string     `json:"groupAgents"`
	TransferTo  *uuid.UUID `json:"transferTo"`
	By          string     `json:"by"`
	ClientID    *uuid.UUID `json:"clientId"`
	Initiator   string     `json:"initiator"`
	ActorID     *uuid.UUID `json:"actorId"`
}

// RunOne performs one running job to its end; false — none is waiting.
func (s *Service) RunOne(ctx context.Context) (bool, error) {
	id, err := s.runOne(ctx, []uuid.UUID{})
	return err == nil && id != uuid.Nil, err
}

// runOne performs the oldest running job not listed in skip and returns its
// ID; uuid.Nil — none is waiting. The job is held by a session advisory
// lock, not by a transaction: the steps call Kubernetes and S3.
func (s *Service) runOne(ctx context.Context, skip []uuid.UUID) (uuid.UUID, error) {
	conn, err := s.Pool.Acquire(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	defer conn.Release()
	rows, err := conn.Query(ctx, `SELECT id FROM account_jobs WHERE status = 'running' AND kind = 'archive' AND NOT (id = ANY($1::uuid[]))
		ORDER BY created_at`, skip)
	if err != nil {
		return uuid.Nil, err
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[uuid.UUID])
	if err != nil {
		return uuid.Nil, err
	}
	for _, id := range ids {
		key := "nabu:account-job:" + id.String()
		var ok bool
		if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtext($1))`, key).Scan(&ok); err != nil {
			return uuid.Nil, err
		}
		if !ok {
			continue // another worker performs it
		}
		ran, err := s.runJob(ctx, id)
		if _, uerr := conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock(hashtext($1))`, key); uerr != nil {
			_ = conn.Conn().Close(context.WithoutCancel(ctx)) // the lock ends with the session
		}
		if ran || err != nil {
			return id, err
		}
	}
	return uuid.Nil, nil
}

// runJob performs the steps of a job from the one it stopped at; false —
// another worker finished the job first.
func (s *Service) runJob(ctx context.Context, id uuid.UUID) (bool, error) {
	var uid uuid.UUID
	var step string
	var raw []byte
	err := s.Pool.QueryRow(ctx, `SELECT user_id, step, params FROM account_jobs WHERE id = $1 AND status = 'running'`, id).Scan(&uid, &step, &raw)
	if postgres.IsNoRows(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var p jobParams
	_ = json.Unmarshal(raw, &p)
	var status string
	_ = s.Pool.QueryRow(ctx, `SELECT status FROM users WHERE id = $1`, uid).Scan(&status)
	if status != "archived" { // restored before the job finished
		_, err := s.Pool.Exec(ctx, `UPDATE account_jobs SET status = 'cancelled', finished_at = now() WHERE id = $1`, id)
		return true, err
	}
	start := 0
	for i, st := range Steps {
		if st == step {
			start = i
		}
	}
	for _, st := range Steps[start:] {
		// the step is recorded before it runs: a failure resumes here (AR-02)
		if _, err := s.Pool.Exec(ctx, `UPDATE account_jobs SET step = $2 WHERE id = $1`, id, st); err != nil {
			return false, err
		}
		if err := s.step(ctx, st, uid, p); err != nil {
			return false, errors.Join(errors.New("archive step "+st), err)
		}
	}
	_, err = s.Pool.Exec(ctx, `UPDATE account_jobs SET step = 'done', status = 'done', finished_at = now() WHERE id = $1`, id)
	return true, err
}

func (s *Service) step(ctx context.Context, st string, uid uuid.UUID, p jobParams) error {
	switch st {
	case "sessions":
		if _, err := s.Pool.Exec(ctx, `DELETE FROM user_sessions WHERE user_id = $1`, uid); err != nil {
			return err
		}
		if s.CloseSessions != nil {
			return s.CloseSessions(ctx, uid)
		}
	case "tasks":
		_, err := s.Pool.Exec(ctx, `UPDATE scheduled_tasks SET status = 'paused', pause_reason = 'the account is archived', pause_kind = 'archived'
			WHERE user_id = $1 AND status = 'active'`, uid)
		return err
	case "sandbox":
		if s.Sandboxes != nil {
			return s.Sandboxes.Stop(ctx, uid)
		}
	case "connections":
		if _, err := s.Pool.Exec(ctx, `DELETE FROM oauth_states WHERE user_id = $1`, uid); err != nil {
			return err
		}
		_, err := s.Pool.Exec(ctx, `DELETE FROM user_connections WHERE user_id = $1`, uid)
		return err
	case "telegram":
		for _, q := range []string{`DELETE FROM telegram_bindings WHERE user_id = $1`, `DELETE FROM telegram_keys WHERE user_id = $1`,
			`DELETE FROM channel_contacts WHERE user_id = $1`} {
			if _, err := s.Pool.Exec(ctx, q, uid); err != nil {
				return err
			}
		}
	case "groups":
		if s.Groups != nil {
			return s.Groups.OwnerArchived(ctx, uid, p.GroupAgents, p.TransferTo)
		}
	case "audit":
		s.audit(ctx, "account.archive", uid, p.By, p.ClientID, p.Initiator)
	}
	return nil
}

func (s *Service) audit(ctx context.Context, tool string, uid uuid.UUID, by string, client *uuid.UUID, initiator string) {
	if s.Ledger == nil {
		return
	}
	ch := "admin"
	if strings.HasPrefix(by, "api:") {
		ch = "client:" + strings.TrimPrefix(by, "api:")
	}
	var ini *string
	switch {
	case initiator != "":
		ini = &initiator
	case !strings.HasPrefix(by, "api:") && by != "":
		ini = &by
	}
	args, _ := json.Marshal(map[string]string{"by": by})
	s.Ledger.Audit(ctx, ledger.Entry{AgentKind: "personal", Agent: "nabu", UserID: &uid, ClientID: client, InitiatorEmail: ini,
		Channel: ch, Tool: tool, Result: "ok"}, string(args), nil)
}

// ─── restoring (R19) ────────────────────────────────────────────────

// Request is a restore request of an archived user who signed in.
type Request struct {
	ID          uuid.UUID  `json:"id"`
	UserID      uuid.UUID  `json:"userId"`
	Email       string     `json:"email"`
	Name        string     `json:"name"`
	ArchivedAt  *time.Time `json:"archivedAt"`
	RequestedAt time.Time  `json:"requestedAt"`
	NewIdentity *Identity  `json:"newIdentity"`
}

// Identity is an identity of the sign-in provider.
type Identity struct {
	Issuer  string `json:"issuer"`
	Subject string `json:"subject"`
}

// RequestRestore records the sign-in of an archived user (AR-05); a new
// identity of the provider is kept for the administrator to confirm.
func (s *Service) RequestRestore(ctx context.Context, uid uuid.UUID, id *Identity) error {
	var iss, sub *string
	if id != nil {
		iss, sub = &id.Issuer, &id.Subject
	}
	_, err := s.Pool.Exec(ctx, `INSERT INTO restore_requests (user_id, issuer, subject) VALUES ($1,$2,$3)
		ON CONFLICT (user_id) WHERE status = 'open' DO UPDATE SET requested_at = now(),
		issuer = COALESCE(EXCLUDED.issuer, restore_requests.issuer), subject = COALESCE(EXCLUDED.subject, restore_requests.subject)`, uid, iss, sub)
	if err == nil && s.Events != nil {
		s.Events.Publish(ctx, events.Event{Type: "restore.requested", Data: map[string]any{"userId": uid}})
	}
	return err
}

// Requests lists open restore requests.
func (s *Service) Requests(ctx context.Context) ([]Request, error) {
	rows, err := s.Pool.Query(ctx, `SELECT r.id, r.user_id, u.email, COALESCE(u.name,''), u.archived_at, r.requested_at, r.issuer, r.subject
		FROM restore_requests r JOIN users u ON u.id = r.user_id WHERE r.status = 'open' ORDER BY r.requested_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Request{}
	for rows.Next() {
		var r Request
		var iss, sub *string
		if err := rows.Scan(&r.ID, &r.UserID, &r.Email, &r.Name, &r.ArchivedAt, &r.RequestedAt, &iss, &sub); err != nil {
			return nil, err
		}
		if iss != nil && sub != nil {
			r.NewIdentity = &Identity{Issuer: *iss, Subject: *sub}
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// RejectRequest rejects a restore request.
func (s *Service) RejectRequest(ctx context.Context, id uuid.UUID) error {
	tag, err := s.Pool.Exec(ctx, `UPDATE restore_requests SET status = 'rejected' WHERE id = $1 AND status = 'open'`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return apperr.NotFound("not_found", "restore request not found")
	}
	return nil
}

// Restore restores an archived account (AR-06): conversations, memory,
// space and the agent stay; tasks stay paused; with link the new identity
// of the request is linked to the account; byAPI links the identity of
// the next sign-in with the email (AR-07).
func (s *Service) Restore(ctx context.Context, uid uuid.UUID, link *uuid.UUID, byAPI bool, by Actor) (string, error) {
	res := Restored
	err := postgres.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		var status string
		err := tx.QueryRow(ctx, `SELECT status FROM users WHERE id = $1 AND created_via <> 'group' FOR UPDATE`, uid).Scan(&status)
		if postgres.IsNoRows(err) {
			res = NotFound
			return nil
		}
		if err != nil {
			return err
		}
		if status != "archived" {
			res = AlreadyActive
			return nil
		}
		if link != nil {
			var iss, sub *string
			err := tx.QueryRow(ctx, `SELECT issuer, subject FROM restore_requests WHERE id = $1 AND user_id = $2 AND status = 'open'`, *link, uid).Scan(&iss, &sub)
			if postgres.IsNoRows(err) {
				return apperr.NotFound("not_found", "restore request not found")
			}
			if err != nil {
				return err
			}
			if iss != nil && sub != nil {
				if _, err := tx.Exec(ctx, `INSERT INTO user_identities (issuer, subject, user_id) VALUES ($1,$2,$3)
					ON CONFLICT (issuer, subject) DO UPDATE SET user_id = EXCLUDED.user_id`, *iss, *sub, uid); err != nil {
					return err
				}
			}
		}
		if _, err := tx.Exec(ctx, `UPDATE users SET status = 'active', archived_at = NULL, archived_by = NULL, purge_after = NULL,
			link_identity_on_next_login = $2 WHERE id = $1`, uid, byAPI); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE restore_requests SET status = 'approved' WHERE user_id = $1 AND status = 'open'`, uid)
		return err
	})
	if err != nil {
		return "", err
	}
	if res == Restored {
		s.audit(ctx, "account.restore", uid, by.By(), by.ClientID, by.Initiator)
		s.accessChanged(ctx)
	}
	return res, nil
}

// RestoreEmails restores accounts by email through the API (R22).
func (s *Service) RestoreEmails(ctx context.Context, emails []string, by Actor) ([]Result, error) {
	list := normalize(emails)
	if len(list) == 0 {
		return nil, apperr.Unprocessable("emails_required", "a list of emails is required").With("field", "emails")
	}
	if len(list) > 500 {
		return nil, apperr.Unprocessable("too_many_emails", "at most 500 emails per call").With("field", "emails")
	}
	out := make([]Result, 0, len(list))
	for _, email := range list {
		r := Result{Email: email, Result: NotFound}
		var id uuid.UUID
		if err := s.Pool.QueryRow(ctx, `SELECT id FROM users WHERE email = $1 AND created_via <> 'group'`, email).Scan(&id); err == nil {
			r.UserID = &id
			res, err := s.Restore(ctx, id, nil, true, by)
			if err != nil {
				return nil, err
			}
			r.Result = res
		} else if s.purgedEmail(ctx, email) {
			r.Result = Purged
		}
		out = append(out, r)
	}
	return out, nil
}

// ─── the purge (tech §10) ───────────────────────────────────────────

// Purge deletes archived accounts after purge_after in batches of 50
// (AR-11): their objects in S3 and the rows (the database cascades);
// the audit stays. Group agents with an expired data_until go too.
func (s *Service) Purge(ctx context.Context) (int, error) {
	total := 0
	for ctx.Err() == nil {
		rows, err := s.Pool.Query(ctx, `SELECT id, email FROM users WHERE status = 'archived' AND purge_after < now() ORDER BY purge_after LIMIT 50`)
		if err != nil {
			return total, err
		}
		type acc struct {
			id    uuid.UUID
			email string
		}
		var list []acc
		for rows.Next() {
			var a acc
			if rows.Scan(&a.id, &a.email) == nil {
				list = append(list, a)
			}
		}
		rows.Close()
		if len(list) == 0 {
			break
		}
		for _, a := range list {
			if err := s.DeleteUserData(ctx, a.id); err != nil {
				return total, err
			}
			if _, err := s.Pool.Exec(ctx, `INSERT INTO purged_accounts (email) VALUES ($1) ON CONFLICT (email) DO UPDATE SET purged_at = now()`, a.email); err != nil {
				return total, err
			}
			if _, err := s.Pool.Exec(ctx, `DELETE FROM users WHERE id = $1`, a.id); err != nil {
				return total, err
			}
			metrics.PurgeDeleted.WithLabelValues("account").Inc()
			slog.InfoContext(ctx, "account purged", "user_id", a.id)
			total++
		}
	}
	if s.Groups != nil {
		n, err := s.Groups.Purge(ctx)
		if err != nil {
			return total, err
		}
		metrics.PurgeDeleted.WithLabelValues("group_agent").Add(float64(n))
	}
	return total, nil
}

// DeleteUserData deletes the objects of a user in S3: the space, the
// attachments and the session snapshots of the conversations.
func (s *Service) DeleteUserData(ctx context.Context, uid uuid.UUID) error {
	if s.S3 == nil {
		return nil
	}
	prefixes := []string{"spaces/" + uid.String() + "/", "attachments/" + uid.String() + "/"}
	for _, p := range prefixes {
		objs, err := s.S3.List(ctx, p)
		if err != nil {
			return err
		}
		for _, o := range objs {
			if err := s.S3.Delete(ctx, o.Key); err != nil && !errors.Is(err, storage.ErrNotFound) {
				return err
			}
		}
	}
	rows, err := s.Pool.Query(ctx, `SELECT id FROM conversations WHERE user_id = $1`, uid)
	if err != nil {
		return err
	}
	var convs []uuid.UUID
	for rows.Next() {
		var c uuid.UUID
		if rows.Scan(&c) == nil {
			convs = append(convs, c)
		}
	}
	rows.Close()
	for _, c := range convs {
		if err := s.S3.Delete(ctx, "sessions/"+c.String()+".jsonl"); err != nil && !errors.Is(err, storage.ErrNotFound) {
			return err
		}
	}
	return nil
}

// CountUsers refreshes nabu_users{status}.
func (s *Service) CountUsers(ctx context.Context) {
	rows, err := s.Pool.Query(ctx, `SELECT status, count(*) FROM users WHERE created_via <> 'group' GROUP BY status`)
	if err != nil {
		return
	}
	defer rows.Close()
	for _, st := range []string{"invited", "active", "blocked", "archived"} {
		metrics.Users.WithLabelValues(st).Set(0)
	}
	for rows.Next() {
		var st string
		var n int
		if rows.Scan(&st, &n) == nil {
			metrics.Users.WithLabelValues(st).Set(float64(n))
		}
	}
}
