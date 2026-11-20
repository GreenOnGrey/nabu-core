package comm

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/collect"
	"github.com/GreenOnGrey/nabu-core/internal/platform/events"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
)

// Tag is a person or a topic of a note (R8).
type Tag struct {
	ID    uuid.UUID `json:"id"`
	Kind  string    `json:"kind"`
	Label string    `json:"label"`
	Email string    `json:"email,omitempty"`
	// Days are the days of communication with the person or on the topic (FTR.NAB.MEM-0001 R20); Notes repeats
	// them for the clients of the version before and goes in the next one.
	Days  int `json:"days,omitempty"`
	Notes int `json:"notes,omitempty"`
}

// Appended is what the collector added to a note the user had edited (R10).
type Appended struct {
	At   time.Time `json:"at"`
	Text string    `json:"text"`
}

// Message is a source message of a note: never its text (R7).
type Message struct {
	ID     string     `json:"id"`
	Author string     `json:"author"`
	SentAt *time.Time `json:"sentAt"`
	Title  string     `json:"title"`
	URL    string     `json:"url"`
	// Key: the message is one of the key ones of a compacted record (FTR.NAB.MEM-0001 R6).
	Key bool `json:"key"`
}

// Note is a note of a mail thread or of a chat for a day (R6).
type Note struct {
	ID              uuid.UUID `json:"id"`
	Title           string    `json:"title"`
	Excerpt         string    `json:"excerpt"`
	Source          string    `json:"source"`
	ItemID          uuid.UUID `json:"itemId"`
	FirstAt         time.Time `json:"firstAt"`
	LastAt          time.Time `json:"lastAt"`
	TimePrecision   string    `json:"timePrecision"`
	MessageCount    int       `json:"messageCount"`
	People          []Tag     `json:"people"`
	Topics          []Tag     `json:"topics"`
	OpenCommitments int       `json:"openCommitments"`
	UserEdited      bool      `json:"userEdited"`
	// Level and period of the record, its days of communication, the link to its thread or chat or else to its
	// last message (FTR.NAB.MEM-0001 R1, R17, R23).
	Level         string     `json:"level"`
	PeriodFrom    string     `json:"periodFrom"`
	PeriodTo      string     `json:"periodTo"`
	Days          int        `json:"days"`
	SourceURL     string     `json:"sourceUrl"`
	CompactedFrom *int       `json:"compactedFrom"`
	CompactedAt   *time.Time `json:"compactedAt"`
	threadKey     string
	lastURL       string
	// The fields of the full note (GET /comm/notes/{id}).
	Summary     *string      `json:"summary,omitempty"`
	Appended    []Appended   `json:"appended,omitempty"`
	Messages    []Message    `json:"messages,omitempty"`
	Commitments []Commitment `json:"commitments,omitempty"`
}

// Filter narrows the notes of the table, the graph and the search (R18–R20).
type Filter struct {
	Query  string
	Person *uuid.UUID
	Topic  *uuid.UUID
	Source string
	From   *time.Time
	To     *time.Time
}

// where builds the conditions of a filter over comm_notes n; args[0] is the user.
func (f Filter) where(args *[]any) string {
	var b strings.Builder
	add := func(cond string, v any) {
		*args = append(*args, v)
		b.WriteString(" AND " + strings.ReplaceAll(cond, "?", fmt.Sprintf("$%d", len(*args))))
	}
	for _, t := range []struct {
		id   *uuid.UUID
		kind string
	}{{f.Person, KindPerson}, {f.Topic, KindTopic}} {
		if t.id != nil {
			add(`EXISTS (SELECT 1 FROM comm_note_tags t WHERE t.user_id = n.user_id AND t.note_id = n.id AND t.kind = '`+t.kind+`' AND t.tag_id = ?)`, *t.id)
		}
	}
	if f.Source != "" {
		add(`n.source = ?`, f.Source)
	}
	// the period of a record that overlaps the filter (FTR.NAB.MEM-0001 R18): a month is found by a week inside it;
	// the dates of the periods are in the time zone of the user, so are the bounds
	if f.From != nil {
		add(`n.period_to >= `+localDay("?::timestamptz"), *f.From)
	}
	if f.To != nil {
		add(`n.period_from <= `+localDay("(?::timestamptz - interval '1 microsecond')"), *f.To)
	}
	return b.String()
}

const noteCols = `n.id, n.title, left(n.summary, 240), n.source, n.item_id, n.first_at, n.last_at, n.time_precision, n.message_count, n.user_edited,
	(SELECT count(*) FROM commitments c WHERE c.user_id = n.user_id AND c.note_id = n.id AND c.state = 'open'),
	n.level, COALESCE(to_char(n.period_from, 'YYYY-MM-DD'), ''), COALESCE(to_char(n.period_to, 'YYYY-MM-DD'), ''), n.days,
	n.compacted_from, n.compacted_at, n.thread_key,
	COALESCE((SELECT m.url FROM comm_note_messages m WHERE m.user_id = n.user_id AND m.note_id = n.id AND m.url IS NOT NULL
		ORDER BY m.sent_at DESC NULLS LAST LIMIT 1), '')`

func scanNote(row pgx.Row) (*Note, error) {
	var n Note
	if err := row.Scan(&n.ID, &n.Title, &n.Excerpt, &n.Source, &n.ItemID, &n.FirstAt, &n.LastAt, &n.TimePrecision, &n.MessageCount, &n.UserEdited, &n.OpenCommitments,
		&n.Level, &n.PeriodFrom, &n.PeriodTo, &n.Days, &n.CompactedFrom, &n.CompactedAt, &n.threadKey, &n.lastURL); err != nil {
		return nil, err
	}
	n.People, n.Topics = []Tag{}, []Tag{}
	n.SourceURL = n.lastURL
	return &n, nil
}

// linkNotes is linkThreads over a list.
func (s *Service) linkNotes(ctx context.Context, notes []Note) {
	ptrs := make([]*Note, len(notes))
	for i := range notes {
		ptrs[i] = &notes[i]
	}
	s.linkThreads(ctx, ptrs)
}

// linkThreads gives the records the link to their thread or chat when the adapter of the source has one
// (FTR.NAB.MEM-0001 R23); the others keep the link to their last message. It reads the catalog, so it runs after
// the transaction of the user, not inside it.
func (s *Service) linkThreads(ctx context.Context, notes []*Note) {
	ads := map[uuid.UUID]collect.Adapter{}
	for _, n := range notes {
		ad, ok := ads[n.ItemID]
		if !ok {
			ad, _ = s.AdapterOf(ctx, n.ItemID)
			ads[n.ItemID] = ad
		}
		if ad == nil {
			continue
		}
		if link := ad.Link(n.Source, collect.Thread{Key: n.threadKey, DayKey: ""}, ""); link != "" {
			n.SourceURL = link
		}
	}
}

// loadTags fills the people and the topics of notes.
func loadTags(ctx context.Context, tx pgx.Tx, uid uuid.UUID, notes []*Note) error {
	if len(notes) == 0 {
		return nil
	}
	ids := make([]uuid.UUID, len(notes))
	by := map[uuid.UUID]*Note{}
	for i, n := range notes {
		ids[i], by[n.ID] = n.ID, n
	}
	rows, err := tx.Query(ctx, `SELECT t.note_id, t.kind, t.tag_id, COALESCE(NULLIF(p.name,''), p.email, tp.name, ''), COALESCE(p.email, '')
		FROM comm_note_tags t
		LEFT JOIN comm_people p ON t.kind = 'person' AND p.id = t.tag_id
		LEFT JOIN comm_topics tp ON t.kind = 'topic' AND tp.id = t.tag_id
		WHERE t.user_id = $1 AND t.note_id = ANY($2) ORDER BY 4`, uid, ids)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var nid uuid.UUID
		var t Tag
		if err := rows.Scan(&nid, &t.Kind, &t.ID, &t.Label, &t.Email); err != nil {
			return err
		}
		if n := by[nid]; n != nil {
			if t.Kind == KindPerson {
				n.People = append(n.People, t)
			} else {
				n.Topics = append(n.Topics, t)
			}
		}
	}
	return rows.Err()
}

// List lists the notes of the table (R18). With a query the notes come by
// relevance without a cursor; without it — the latest first, page by page.
func (s *Service) List(ctx context.Context, uid uuid.UUID, f Filter, page httpx.Page) (httpx.List[Note], error) {
	defer observe("table")()
	if q := strings.TrimSpace(f.Query); q != "" {
		notes, err := s.Search(ctx, uid, f, page.Limit)
		return httpx.List[Note]{Items: notes}, err
	}
	var out []Note
	err := s.as(ctx, uid, func(tx pgx.Tx) error {
		args := []any{uid}
		cond := f.where(&args)
		if page.Cursor != nil {
			id, err := uuid.Parse(page.Cursor.ID)
			if err != nil {
				return apperr.BadRequest("invalid_cursor", "invalid cursor")
			}
			args = append(args, page.Cursor.T, id)
			cond += fmt.Sprintf(` AND (n.last_at, n.id) < ($%d, $%d)`, len(args)-1, len(args))
		}
		args = append(args, page.Limit+1)
		rows, err := tx.Query(ctx, `SELECT `+noteCols+` FROM comm_notes n WHERE n.user_id = $1`+cond+
			fmt.Sprintf(` ORDER BY n.last_at DESC, n.id DESC LIMIT $%d`, len(args)), args...)
		if err != nil {
			return err
		}
		ptrs, err := collectNotes(rows)
		if err != nil {
			return err
		}
		if err := loadTags(ctx, tx, uid, ptrs); err != nil {
			return err
		}
		for _, n := range ptrs {
			out = append(out, *n)
		}
		return nil
	})
	if err != nil {
		return httpx.List[Note]{}, err
	}
	s.linkNotes(ctx, out)
	return httpx.NewList(out, page.Limit, func(n Note) (time.Time, string) { return n.LastAt, n.ID.String() }), nil
}

func collectNotes(rows pgx.Rows) ([]*Note, error) {
	defer rows.Close()
	var out []*Note
	for rows.Next() {
		n, err := scanNote(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

func observe(kind string) func() {
	start := time.Now()
	return func() { metrics.MemorySearch.WithLabelValues(kind).Observe(time.Since(start).Seconds()) }
}

// Search finds notes by words (tech §8): the full-text index and a substring
// match, the best first. The vector branch joins when embeddings are
// configured.
func (s *Service) Search(ctx context.Context, uid uuid.UUID, f Filter, limit int) ([]Note, error) {
	defer observe("search")()
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	q := strings.TrimSpace(f.Query)
	out := []Note{}
	// The vector of the query is computed before the transaction; when the
	// model does not answer the search goes by words (VIEW-10).
	var vec []float32
	var model string
	if e := s.embedder(ctx); e != nil && q != "" {
		if v, err := e.Embed(ctx, []string{q}); err == nil {
			vec, model = v[0], e.Model
		}
	}
	err := s.as(ctx, uid, func(tx pgx.Tx) error {
		if vec != nil {
			ids, err := s.searchHybrid(ctx, tx, uid, f, q, vec, model, limit)
			if err != nil {
				return err
			}
			rows, err := tx.Query(ctx, `SELECT `+noteCols+` FROM comm_notes n WHERE n.user_id = $1 AND n.id = ANY($2)`, uid, ids)
			if err != nil {
				return err
			}
			ptrs, err := collectNotes(rows)
			if err != nil {
				return err
			}
			if err := loadTags(ctx, tx, uid, ptrs); err != nil {
				return err
			}
			by := map[uuid.UUID]*Note{}
			for _, n := range ptrs {
				by[n.ID] = n
			}
			for _, id := range ids { // in the order of the merged rank
				if n := by[id]; n != nil {
					out = append(out, *n)
				}
			}
			return nil
		}
		args := []any{uid}
		cond := f.where(&args)
		args = append(args, q, limit)
		qi, li := len(args)-1, len(args)
		rows, err := tx.Query(ctx, `SELECT `+noteCols+` FROM comm_notes n, plainto_tsquery('simple', `+fmt.Sprintf("$%d", qi)+`) tq
			WHERE n.user_id = $1`+cond+fmt.Sprintf(` AND (n.search_vector @@ tq OR `+postgres.Fold("n.title")+` ILIKE '%%' || $%d || '%%' OR `+postgres.Fold("n.summary")+` ILIKE '%%' || $%d || '%%')
			ORDER BY ts_rank(n.search_vector, tq) DESC, n.last_at DESC, n.id DESC LIMIT $%d`, qi, qi, li), args...)
		if err != nil {
			return err
		}
		ptrs, err := collectNotes(rows)
		if err != nil {
			return err
		}
		if err := loadTags(ctx, tx, uid, ptrs); err != nil {
			return err
		}
		for _, n := range ptrs {
			out = append(out, *n)
		}
		return nil
	})
	if err == nil {
		s.linkNotes(ctx, out)
	}
	return out, err
}

// Get loads a note with its summary, sources and commitments.
func (s *Service) Get(ctx context.Context, uid, id uuid.UUID) (*Note, error) {
	var n *Note
	err := s.as(ctx, uid, func(tx pgx.Tx) error {
		var err error
		n, err = getNote(ctx, tx, uid, id)
		return err
	})
	if n != nil {
		s.linkThreads(ctx, []*Note{n})
	}
	return n, err
}

func getNote(ctx context.Context, tx pgx.Tx, uid, id uuid.UUID) (*Note, error) {
	n, err := scanNote(tx.QueryRow(ctx, `SELECT `+noteCols+` FROM comm_notes n WHERE n.user_id = $1 AND n.id = $2`, uid, id))
	if postgres.IsNoRows(err) {
		return nil, apperr.NotFound("not_found", "note not found")
	}
	if err != nil {
		return nil, err
	}
	var summary string
	var appended []byte
	if err := tx.QueryRow(ctx, `SELECT summary, appended FROM comm_notes WHERE user_id = $1 AND id = $2`, uid, id).Scan(&summary, &appended); err != nil {
		return nil, err
	}
	n.Summary = &summary
	_ = json.Unmarshal(appended, &n.Appended)
	if err := loadTags(ctx, tx, uid, []*Note{n}); err != nil {
		return nil, err
	}
	// the key messages of a compacted record first, then the rest by time (FTR.NAB.MEM-0001 R6)
	rows, err := tx.Query(ctx, `SELECT message_id, COALESCE(author,''), sent_at, COALESCE(title,''), COALESCE(url,''), key FROM comm_note_messages
		WHERE user_id = $1 AND note_id = $2 ORDER BY key DESC, sent_at NULLS LAST, message_id`, uid, id)
	if err != nil {
		return nil, err
	}
	n.Messages = []Message{}
	for rows.Next() {
		var m Message
		if err := rows.Scan(&m.ID, &m.Author, &m.SentAt, &m.Title, &m.URL, &m.Key); err != nil {
			rows.Close()
			return nil, err
		}
		n.Messages = append(n.Messages, m)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	n.Commitments, err = listCommitments(ctx, tx, uid, CommitmentFilter{Note: &id, State: "all"}, 200)
	return n, err
}

// ─── saving by the collector (tech §5.1) ────────────────────────────

// PersonIn is a participant the collector names.
type PersonIn struct {
	Email string `json:"email"`
	Name  string `json:"name"`
}

// SaveNote is what the collector saves for a thread or a chat day.
type SaveNote struct {
	ItemID        uuid.UUID
	Source        string
	ThreadKey     string
	DayKey        string
	Title         string
	Summary       string
	People        []PersonIn
	Topics        []string
	Messages      []Message
	TimePrecision string
	// UserEmail is left out of the people: a tag is a participant other than the user (R8).
	UserEmail string
}

// Saved is the outcome of SaveNote.
type Saved struct {
	ID          uuid.UUID
	Created     bool
	NewMessages int
	// Skipped: the user deleted the note of this thread, it is not collected again (R10).
	Skipped bool
}

// SaveNote creates the note of a thread or adds to it (R6): the key is the
// source, the thread and the day, so a repeated batch makes no duplicates
// (NOTE-03). A note the user edited keeps its text — the news go to a
// separate block (R10). Secrets are masked before writing (R7).
func (s *Service) SaveNote(ctx context.Context, uid uuid.UUID, in SaveNote) (*Saved, error) {
	in.Title = clipRunes(masked(strings.TrimSpace(in.Title)), MaxTitle)
	in.Summary = masked(strings.TrimSpace(in.Summary))
	if in.Title == "" || in.Summary == "" {
		return nil, apperr.Unprocessable("invalid_note", "title and summary are required")
	}
	if len([]rune(in.Summary)) > MaxSummary {
		return nil, apperr.Unprocessable("invalid_note", fmt.Sprintf("the summary is longer than %d characters", MaxSummary))
	}
	// FTR.NAB.CMN-0003 R8 (v1.5): an automated notice or a one-off message has no topic
	topics := uniqueTopics(in.Topics)
	if len(topics) > MaxTopics {
		return nil, apperr.Unprocessable("invalid_topics", fmt.Sprintf("give at most %d topics", MaxTopics))
	}
	if in.TimePrecision != "day" {
		in.TimePrecision = "minute"
	}
	res := &Saved{}
	err := s.as(ctx, uid, func(tx pgx.Tx) error {
		var gone bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM comm_tombstones WHERE user_id = $1 AND item_id = $2 AND source = $3 AND thread_key = $4 AND day_key = $5)`,
			uid, in.ItemID, in.Source, in.ThreadKey, in.DayKey).Scan(&gone); err != nil {
			return err
		}
		if gone {
			res.Skipped = true
			return nil
		}
		first, last := span(in.Messages, s.clock())
		var edited bool
		err := tx.QueryRow(ctx, `SELECT id, user_edited FROM comm_notes WHERE user_id = $1 AND item_id = $2 AND source = $3 AND thread_key = $4 AND day_key = $5 FOR UPDATE`,
			uid, in.ItemID, in.Source, in.ThreadKey, in.DayKey).Scan(&res.ID, &edited)
		switch {
		case postgres.IsNoRows(err):
			res.Created = true
			// a mail thread is one record of the level thread, a chat a record of a day (FTR.NAB.MEM-0001 R1)
			if err := tx.QueryRow(ctx, `INSERT INTO comm_notes (user_id, item_id, source, thread_key, day_key, title, summary, first_at, last_at, time_precision, level, chars_in)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10, CASE WHEN $5 = '' THEN 'thread' ELSE 'day' END, char_length($7)) RETURNING id`,
				uid, in.ItemID, in.Source, in.ThreadKey, in.DayKey, in.Title, in.Summary, first, last, in.TimePrecision).Scan(&res.ID); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO comm_settings (user_id, notes_count) VALUES ($1, 1)
				ON CONFLICT (user_id) DO UPDATE SET notes_count = comm_settings.notes_count + 1`, uid); err != nil {
				return err
			}
		case err != nil:
			return err
		}
		for _, m := range in.Messages {
			if strings.TrimSpace(m.ID) == "" {
				continue
			}
			tag, err := tx.Exec(ctx, `INSERT INTO comm_note_messages (user_id, note_id, message_id, author, sent_at, title, url)
				VALUES ($1,$2,$3,NULLIF($4,''),$5,NULLIF($6,''),NULLIF($7,'')) ON CONFLICT DO NOTHING`,
				uid, res.ID, m.ID, clipRunes(m.Author, 200), m.SentAt, clipRunes(masked(m.Title), MaxTitle), m.URL)
			if err != nil {
				return err
			}
			res.NewMessages += int(tag.RowsAffected())
		}
		if !res.Created {
			switch {
			case edited && res.NewMessages > 0:
				b, _ := json.Marshal([]Appended{{At: s.clock().UTC(), Text: in.Summary}})
				if _, err := tx.Exec(ctx, `UPDATE comm_notes SET appended = appended || $3::jsonb, updated_at = now() WHERE user_id = $1 AND id = $2`, uid, res.ID, b); err != nil {
					return err
				}
			case !edited:
				if _, err := tx.Exec(ctx, `UPDATE comm_notes SET title = $3, summary = $4, updated_at = now() WHERE user_id = $1 AND id = $2`, uid, res.ID, in.Title, in.Summary); err != nil {
					return err
				}
			}
		}
		if _, err := tx.Exec(ctx, `UPDATE comm_notes n SET
				message_count = (SELECT count(*) FROM comm_note_messages m WHERE m.user_id = n.user_id AND m.note_id = n.id),
				first_at = LEAST(n.first_at, $3), last_at = GREATEST(n.last_at, $4)
			WHERE n.user_id = $1 AND n.id = $2`, uid, res.ID, first, last); err != nil {
			return err
		}
		// Tags: the collector's set replaces the tags of a note the user did
		// not touch; an edited note only gains tags.
		if !edited {
			if _, err := tx.Exec(ctx, `DELETE FROM comm_note_tags WHERE user_id = $1 AND note_id = $2`, uid, res.ID); err != nil {
				return err
			}
		}
		for _, p := range in.People {
			email := strings.ToLower(strings.TrimSpace(p.Email))
			if email == "" || !strings.Contains(email, "@") || email == strings.ToLower(in.UserEmail) {
				continue
			}
			id, err := upsertPerson(ctx, tx, uid, email, p.Name)
			if err != nil {
				return err
			}
			if err := tagNote(ctx, tx, uid, res.ID, KindPerson, id); err != nil {
				return err
			}
		}
		var keys topicKeys
		if len(topics) > 0 {
			if keys, err = loadTopicKeys(ctx, tx, uid); err != nil {
				return err
			}
		}
		for _, name := range topics {
			id, err := upsertTopic(ctx, tx, uid, name, keys)
			if err != nil {
				return err
			}
			if err := tagNote(ctx, tx, uid, res.ID, KindTopic, id); err != nil {
				return err
			}
		}
		return refreshNotes(ctx, tx, uid, res.ID)
	})
	if err != nil {
		return nil, err
	}
	if !res.Skipped {
		s.publish(ctx, events.CommChanged, uid)
	}
	return res, nil
}

func span(ms []Message, now time.Time) (time.Time, time.Time) {
	var first, last time.Time
	for _, m := range ms {
		if m.SentAt == nil {
			continue
		}
		if first.IsZero() || m.SentAt.Before(first) {
			first = *m.SentAt
		}
		if m.SentAt.After(last) {
			last = *m.SentAt
		}
	}
	if first.IsZero() {
		return now, now
	}
	return first, last
}

func uniqueTopics(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, t := range in {
		t = clipRunes(strings.Join(strings.Fields(masked(t)), " "), 80)
		if k := looseKey(t); k != "" && !seen[k] {
			seen[k] = true
			out = append(out, t)
		}
	}
	return out
}

func upsertPerson(ctx context.Context, tx pgx.Tx, uid uuid.UUID, email, name string) (uuid.UUID, error) {
	var id uuid.UUID
	name = clipRunes(strings.TrimSpace(name), 120)
	err := tx.QueryRow(ctx, `INSERT INTO comm_people (user_id, email, name) VALUES ($1, $2, NULLIF($3,''))
		ON CONFLICT (user_id, email) DO UPDATE SET name = COALESCE(NULLIF(comm_people.name,''), EXCLUDED.name) RETURNING id`, uid, email, name).Scan(&id)
	return id, err
}

// upsertTopic finds the topic or creates it. A name that differs from an
// existing topic only by case, punctuation or ё is that topic
// (FTR.NAB.CMN-0003 R8): «ИТ-стратегия» is «ИТ Стратегия».
func upsertTopic(ctx context.Context, tx pgx.Tx, uid uuid.UUID, name string, keys topicKeys) (uuid.UUID, error) {
	if id, ok := keys[looseKey(name)]; ok {
		return id, nil
	}
	var id uuid.UUID
	err := tx.QueryRow(ctx, `INSERT INTO comm_topics (user_id, name, name_key) VALUES ($1, $2, $3)
		ON CONFLICT (user_id, name_key) DO UPDATE SET name_key = EXCLUDED.name_key RETURNING id`, uid, name, nameKey(name)).Scan(&id)
	if err == nil && keys != nil {
		keys[looseKey(name)] = id
	}
	return id, err
}

func tagNote(ctx context.Context, tx pgx.Tx, uid, note uuid.UUID, kind string, tag uuid.UUID) error {
	_, err := tx.Exec(ctx, `INSERT INTO comm_note_tags (user_id, note_id, kind, tag_id) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING`, uid, note, kind, tag)
	return err
}

// ─── editing by the user (R10) ──────────────────────────────────────

// TagRef names a tag to add or remove: an id, or a name to find or create.
type TagRef struct {
	Kind string     `json:"kind"`
	ID   *uuid.UUID `json:"id"`
	Name string     `json:"name"`
}

// NotePatch edits a note.
type NotePatch struct {
	Title      *string  `json:"title"`
	Summary    *string  `json:"summary"`
	AddTags    []TagRef `json:"addTags"`
	RemoveTags []TagRef `json:"removeTags"`
}

// Update edits a note of the user: a changed summary marks the note as
// edited, so the collector appends instead of rewriting (R10).
func (s *Service) Update(ctx context.Context, uid, id uuid.UUID, p NotePatch) (*Note, error) {
	var n *Note
	err := s.as(ctx, uid, func(tx pgx.Tx) error {
		if _, err := getNote(ctx, tx, uid, id); err != nil {
			return err
		}
		if p.Title != nil {
			t := clipRunes(strings.TrimSpace(*p.Title), MaxTitle)
			if t == "" {
				return apperr.Unprocessable("invalid_note", "the title is required").With("field", "title")
			}
			if _, err := tx.Exec(ctx, `UPDATE comm_notes SET title = $3, updated_at = now() WHERE user_id = $1 AND id = $2`, uid, id, t); err != nil {
				return err
			}
		}
		if p.Summary != nil {
			t := strings.TrimSpace(*p.Summary)
			if t == "" || len([]rune(t)) > MaxSummary {
				return apperr.Unprocessable("invalid_note", fmt.Sprintf("the summary is required, up to %d characters", MaxSummary)).With("field", "summary")
			}
			if _, err := tx.Exec(ctx, `UPDATE comm_notes SET summary = $3, user_edited = true, updated_at = now() WHERE user_id = $1 AND id = $2`, uid, id, t); err != nil {
				return err
			}
		}
		for _, r := range p.RemoveTags {
			if r.ID == nil || (r.Kind != KindPerson && r.Kind != KindTopic) {
				return apperr.Unprocessable("invalid_tag", "a tag to remove needs kind and id")
			}
			if _, err := tx.Exec(ctx, `DELETE FROM comm_note_tags WHERE user_id = $1 AND note_id = $2 AND kind = $3 AND tag_id = $4`, uid, id, r.Kind, *r.ID); err != nil {
				return err
			}
			// a tag taken off a conversation stays off when its records are compacted (FTR.NAB.MEM-0001 R10)
			if _, err := tx.Exec(ctx, `INSERT INTO comm_tag_removals (user_id, item_id, source, thread_key, kind, tag_id)
				SELECT user_id, item_id, source, thread_key, $3, $4 FROM comm_notes WHERE user_id = $1 AND id = $2 ON CONFLICT DO NOTHING`, uid, id, r.Kind, *r.ID); err != nil {
				return err
			}
		}
		for _, a := range p.AddTags {
			tag, err := resolveTag(ctx, tx, uid, a)
			if err != nil {
				return err
			}
			if err := tagNote(ctx, tx, uid, id, a.Kind, tag); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `DELETE FROM comm_tag_removals r USING comm_notes n WHERE n.user_id = $1 AND n.id = $2
				AND r.user_id = n.user_id AND r.item_id = n.item_id AND r.source = n.source AND r.thread_key = n.thread_key AND r.kind = $3 AND r.tag_id = $4`,
				uid, id, a.Kind, tag); err != nil {
				return err
			}
		}
		if len(p.AddTags)+len(p.RemoveTags) > 0 {
			if err := refreshNotes(ctx, tx, uid, id); err != nil {
				return err
			}
			// tags changed by hand stay: the collector only adds to this note
			if _, err := tx.Exec(ctx, `UPDATE comm_notes SET user_edited = true, updated_at = now() WHERE user_id = $1 AND id = $2`, uid, id); err != nil {
				return err
			}
		}
		var err error
		n, err = getNote(ctx, tx, uid, id)
		return err
	})
	if err == nil {
		s.linkThreads(ctx, []*Note{n})
		s.publish(ctx, events.CommChanged, uid)
	}
	return n, err
}

func resolveTag(ctx context.Context, tx pgx.Tx, uid uuid.UUID, r TagRef) (uuid.UUID, error) {
	switch r.Kind {
	case KindTopic:
		if r.ID != nil {
			var ok bool
			_ = tx.QueryRow(ctx, `SELECT true FROM comm_topics WHERE user_id = $1 AND id = $2`, uid, *r.ID).Scan(&ok)
			if !ok {
				return uuid.Nil, apperr.NotFound("not_found", "topic not found")
			}
			return *r.ID, nil
		}
		name := clipRunes(strings.Join(strings.Fields(r.Name), " "), 80)
		if name == "" {
			return uuid.Nil, apperr.Unprocessable("invalid_tag", "a topic needs id or name")
		}
		keys, err := loadTopicKeys(ctx, tx, uid)
		if err != nil {
			return uuid.Nil, err
		}
		return upsertTopic(ctx, tx, uid, name, keys)
	case KindPerson:
		if r.ID != nil {
			var ok bool
			_ = tx.QueryRow(ctx, `SELECT true FROM comm_people WHERE user_id = $1 AND id = $2`, uid, *r.ID).Scan(&ok)
			if !ok {
				return uuid.Nil, apperr.NotFound("not_found", "person not found")
			}
			return *r.ID, nil
		}
		email := strings.ToLower(strings.TrimSpace(r.Name))
		if !strings.Contains(email, "@") {
			return uuid.Nil, apperr.Unprocessable("invalid_tag", "a person is added by id or email")
		}
		return upsertPerson(ctx, tx, uid, email, "")
	}
	return uuid.Nil, apperr.Unprocessable("invalid_tag", "kind is person or topic")
}

// deleteNotes deletes notes and remembers their keys: a deleted note is not
// collected again (R10).
func deleteNotes(ctx context.Context, tx pgx.Tx, uid uuid.UUID, cond string, args ...any) (int, error) {
	all := append([]any{uid}, args...)
	days, err := daysOf(ctx, tx, uid, cond, args...)
	if err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO comm_tombstones (user_id, item_id, source, thread_key, day_key)
		SELECT n.user_id, n.item_id, n.source, n.thread_key, n.day_key FROM comm_notes n WHERE n.user_id = $1 AND `+cond+` ON CONFLICT DO NOTHING`, all...); err != nil {
		return 0, err
	}
	tag, err := tx.Exec(ctx, `DELETE FROM comm_notes n WHERE n.user_id = $1 AND `+cond, all...)
	if err != nil {
		return 0, err
	}
	if err := refreshDays(ctx, tx, uid, days); err != nil {
		return 0, err
	}
	n := int(tag.RowsAffected())
	if n > 0 {
		if _, err := tx.Exec(ctx, `UPDATE comm_settings SET notes_count = GREATEST(notes_count - $2, 0) WHERE user_id = $1`, uid, n); err != nil {
			return 0, err
		}
	}
	return n, nil
}

// Delete deletes a note of the user.
func (s *Service) Delete(ctx context.Context, uid, id uuid.UUID) error {
	err := s.as(ctx, uid, func(tx pgx.Tx) error {
		n, err := deleteNotes(ctx, tx, uid, `n.id = $2`, id)
		if err == nil && n == 0 {
			return apperr.NotFound("not_found", "note not found")
		}
		return err
	})
	if err == nil {
		s.publish(ctx, events.CommChanged, uid)
		s.publish(ctx, events.CommitmentsChanged, uid)
	}
	return err
}

// DeleteByTag deletes every note with a person or a topic; confirm must be
// the id of the tag (R10, NOTE-16).
func (s *Service) DeleteByTag(ctx context.Context, uid uuid.UUID, kind string, tag uuid.UUID, confirm string) (int, error) {
	if confirm != tag.String() {
		return 0, apperr.BadRequest("confirmation_required", `send {"confirm":"<tag id>"} to delete every note with the tag`)
	}
	var n int
	err := s.as(ctx, uid, func(tx pgx.Tx) error {
		var err error
		n, err = deleteNotes(ctx, tx, uid, `EXISTS (SELECT 1 FROM comm_note_tags t WHERE t.user_id = n.user_id AND t.note_id = n.id AND t.kind = $2 AND t.tag_id = $3)`, kind, tag)
		return err
	})
	if err == nil && n > 0 {
		s.publish(ctx, events.CommChanged, uid)
		s.publish(ctx, events.CommitmentsChanged, uid)
	}
	return n, err
}

// ─── people and topics (R8, R9) ─────────────────────────────────────

// Tags lists the people or the topics of the user with their days of communication, the most used first
// (FTR.NAB.MEM-0001 R20, R22); q narrows by a part of the name or the email, regardless of case and «ё»; id gives one
// tag.
func (s *Service) Tags(ctx context.Context, uid uuid.UUID, kind, q string, limit int, id *uuid.UUID) ([]Tag, error) {
	if limit <= 0 || limit > 500 {
		limit = 200
	}
	out := []Tag{}
	err := s.as(ctx, uid, func(tx pgx.Tx) error {
		days := `(SELECT count(DISTINCT d.day) FROM comm_tag_days d WHERE d.user_id = p.user_id AND d.kind = '` + kind + `' AND d.tag_id = p.id)`
		sql := `SELECT p.id, COALESCE(NULLIF(p.name,''), p.email), p.email, ` + days + `
			FROM comm_people p WHERE p.user_id = $1 AND ($2 = '' OR ` + foldE("p.email") + ` LIKE '%' || ` + foldE("$2") + ` || '%' OR ` + foldE("p.name") + ` LIKE '%' || ` + foldE("$2") + ` || '%')`
		if kind == KindTopic {
			sql = `SELECT p.id, p.name, '', ` + days + `
			FROM comm_topics p WHERE p.user_id = $1 AND ($2 = '' OR ` + foldE("p.name") + ` LIKE '%' || ` + foldE("$2") + ` || '%')`
		}
		sql += ` AND ($4::uuid IS NULL OR p.id = $4)`
		rows, err := tx.Query(ctx, sql+` ORDER BY 4 DESC, 2 LIMIT $3`, uid, strings.TrimSpace(q), limit, id)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			t := Tag{Kind: kind}
			if err := rows.Scan(&t.ID, &t.Label, &t.Email, &t.Days); err != nil {
				return err
			}
			t.Notes = t.Days
			out = append(out, t)
		}
		return rows.Err()
	})
	return out, err
}

// RenameTopic renames a topic (R9).
func (s *Service) RenameTopic(ctx context.Context, uid, id uuid.UUID, name string) error {
	name = clipRunes(strings.Join(strings.Fields(name), " "), 80)
	if name == "" {
		return apperr.Unprocessable("invalid_topic", "the name is required").With("field", "name")
	}
	err := s.as(ctx, uid, func(tx pgx.Tx) error {
		keys, err := loadTopicKeys(ctx, tx, uid)
		if err != nil {
			return err
		}
		if other, ok := keys[looseKey(name)]; ok && other != id { // R8: «ИТ-стратегия» next to «ИТ Стратегия» is one topic
			return apperr.Conflict("topic_exists", "a topic with this name exists: merge the topics instead")
		}
		tag, err := tx.Exec(ctx, `UPDATE comm_topics SET name = $3, name_key = $4 WHERE user_id = $1 AND id = $2`, uid, id, name, nameKey(name))
		if postgres.IsUniqueViolation(err) {
			return apperr.Conflict("topic_exists", "a topic with this name exists: merge the topics instead")
		}
		if err == nil && tag.RowsAffected() == 0 {
			return apperr.NotFound("not_found", "topic not found")
		}
		return err
	})
	if err == nil {
		s.publish(ctx, events.CommChanged, uid)
	}
	return err
}

// MergeTopic moves the notes of a topic to another one and deletes it (R9).
func (s *Service) MergeTopic(ctx context.Context, uid, id, into uuid.UUID) error {
	if id == into {
		return apperr.Unprocessable("invalid_topic", "a topic cannot be merged into itself")
	}
	err := s.as(ctx, uid, func(tx pgx.Tx) error {
		var n int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM comm_topics WHERE user_id = $1 AND id IN ($2, $3)`, uid, id, into).Scan(&n); err != nil {
			return err
		}
		if n != 2 {
			return apperr.NotFound("not_found", "topic not found")
		}
		ids, err := notesWithTag(ctx, tx, uid, KindTopic, id)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO comm_note_tags (user_id, note_id, kind, tag_id)
			SELECT user_id, note_id, kind, $3 FROM comm_note_tags WHERE user_id = $1 AND kind = 'topic' AND tag_id = $2 ON CONFLICT DO NOTHING`, uid, id, into); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM comm_note_tags WHERE user_id = $1 AND kind = 'topic' AND tag_id = $2`, uid, id); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM comm_topics WHERE user_id = $1 AND id = $2`, uid, id); err != nil {
			return err
		}
		return refreshNotes(ctx, tx, uid, ids...)
	})
	if err == nil {
		s.publish(ctx, events.CommChanged, uid)
	}
	return err
}

// DeleteTopic deletes a topic; its notes stay without it (R9).
func (s *Service) DeleteTopic(ctx context.Context, uid, id uuid.UUID) error {
	err := s.as(ctx, uid, func(tx pgx.Tx) error {
		ids, err := notesWithTag(ctx, tx, uid, KindTopic, id)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM comm_note_tags WHERE user_id = $1 AND kind = 'topic' AND tag_id = $2`, uid, id); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `DELETE FROM comm_topics WHERE user_id = $1 AND id = $2`, uid, id)
		if err == nil && tag.RowsAffected() == 0 {
			return apperr.NotFound("not_found", "topic not found")
		}
		if err != nil {
			return err
		}
		return refreshNotes(ctx, tx, uid, ids...)
	})
	if err == nil {
		s.publish(ctx, events.CommChanged, uid)
	}
	return err
}

// ─── graph (R19, tech §9) ───────────────────────────────────────────

// Node is a person or a topic of the graph; Days — the days of communication (FTR.NAB.MEM-0001 R20), Notes
// repeats them for the clients of the version before.
type Node struct {
	ID    uuid.UUID `json:"id"`
	Kind  string    `json:"kind"`
	Label string    `json:"label"`
	Days  int       `json:"days"`
	Notes int       `json:"notes"`
	Links int       `json:"links"`
}

// Edge links two tags that meet in a record; Days — the days they meet on.
type Edge struct {
	A     uuid.UUID `json:"a"`
	B     uuid.UUID `json:"b"`
	Days  int       `json:"days"`
	Notes int       `json:"notes"`
}

// Graph is the graph of the filtered records.
type Graph struct {
	Nodes      []Node `json:"nodes"`
	Edges      []Edge `json:"edges"`
	TotalNodes int    `json:"totalNodes"`
	TotalEdges int    `json:"totalEdges"`
}

// GraphLimits are MEMORY_GRAPH_MAX_NODES, MEMORY_GRAPH_NODE_EDGES and MEMORY_GRAPH_MAX_EDGES.
type GraphLimits struct {
	MaxNodes, NodeEdges, MaxEdges int
}

// Graph builds the graph of people and topics over the filtered records (FTR.NAB.CMN-0003 R19, FTR.NAB.MEM-0001
// R20, R24): a node per tag, an edge per pair of tags that meet in a record; the measure is the days of
// communication. Without a filter by person, topic or text the graph is read from the aggregate by day (arch A6);
// with one it is counted over the records. Nodes with fewer than minLinks links are left out, of the rest the
// MaxNodes most linked stay, and of their links the strongest NodeEdges of each node, MaxEdges in all.
func (s *Service) Graph(ctx context.Context, uid uuid.UUID, f Filter, minLinks int, lim GraphLimits) (*Graph, error) {
	path := "live"
	if f.Person == nil && f.Topic == nil && strings.TrimSpace(f.Query) == "" {
		path = "aggregate"
	}
	defer observe("graph")()
	if lim.MaxNodes <= 0 {
		lim.MaxNodes = 150
	}
	type pair struct{ a, b uuid.UUID }
	nodes := map[uuid.UUID]*Node{}
	edges := map[pair]int{}
	err := s.as(ctx, uid, func(tx pgx.Tx) error {
		args := []any{uid}
		var tagSQL, pairSQL string
		if path == "aggregate" {
			cond := ""
			add := func(c string, v any) {
				args = append(args, v)
				cond += fmt.Sprintf(" AND "+c, len(args))
			}
			if f.Source != "" {
				add("d.source = $%d", f.Source)
			}
			if f.From != nil {
				add("d.day >= "+localDay("$%d::timestamptz"), *f.From)
			}
			if f.To != nil {
				add("d.day <= "+localDay("($%d::timestamptz - interval '1 microsecond')"), *f.To)
			}
			tagSQL = `SELECT d.kind, d.tag_id, count(DISTINCT d.day) FROM comm_tag_days d WHERE d.user_id = $1` + cond + ` GROUP BY 1, 2`
			pairSQL = `SELECT d.a, d.b, count(DISTINCT d.day) FROM comm_pair_days d WHERE d.user_id = $1` + cond + ` GROUP BY 1, 2`
		} else {
			cond := f.where(&args)
			if q := strings.TrimSpace(f.Query); q != "" {
				args = append(args, q)
				cond += fmt.Sprintf(` AND (n.search_vector @@ plainto_tsquery('simple', $%[1]d) OR `+postgres.Fold("n.title")+` ILIKE '%%' || `+postgres.Fold(fmt.Sprintf("$%d", len(args)))+` || '%%' OR `+postgres.Fold("n.summary")+` ILIKE '%%' || `+postgres.Fold(fmt.Sprintf("$%d", len(args)))+` || '%%')`, len(args))
			}
			// the days of the records inside the period only, as the aggregate counts them
			days := ""
			if f.From != nil {
				args = append(args, *f.From)
				days += fmt.Sprintf(" AND day >= "+localDay("$%d::timestamptz"), len(args))
			}
			if f.To != nil {
				args = append(args, *f.To)
				days += fmt.Sprintf(" AND day <= "+localDay("($%d::timestamptz - interval '1 microsecond')"), len(args))
			}
			scope := `WITH t AS (SELECT t.kind, t.tag_id, t.note_id FROM comm_note_tags t JOIN comm_notes n ON n.user_id = t.user_id AND n.id = t.note_id
				WHERE t.user_id = $1` + cond + `), d AS (SELECT note_id, day FROM comm_note_days WHERE user_id = $1 AND note_id IN (SELECT note_id FROM t)` + days + `) `
			tagSQL = scope + `SELECT t.kind, t.tag_id, count(DISTINCT d.day) FROM t JOIN d USING (note_id) GROUP BY 1, 2`
			pairSQL = scope + `SELECT a.tag_id, b.tag_id, count(DISTINCT d.day) FROM t a JOIN t b ON a.note_id = b.note_id AND (a.kind, a.tag_id) < (b.kind, b.tag_id)
				JOIN d ON d.note_id = a.note_id GROUP BY 1, 2`
		}
		rows, err := tx.Query(ctx, tagSQL, args...)
		if err != nil {
			return err
		}
		for rows.Next() {
			var n Node
			if err := rows.Scan(&n.Kind, &n.ID, &n.Days); err != nil {
				rows.Close()
				return err
			}
			n.Notes = n.Days
			nodes[n.ID] = &n
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		rows, err = tx.Query(ctx, pairSQL, args...)
		if err != nil {
			return err
		}
		for rows.Next() {
			var p pair
			var n int
			if err := rows.Scan(&p.a, &p.b, &n); err != nil {
				rows.Close()
				return err
			}
			edges[p] = n
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		// the labels of the nodes
		ids := make([]uuid.UUID, 0, len(nodes))
		for id := range nodes {
			ids = append(ids, id)
		}
		rows, err = tx.Query(ctx, `SELECT id, COALESCE(NULLIF(name,''), email) FROM comm_people WHERE user_id = $1 AND id = ANY($2)
			UNION ALL SELECT id, name FROM comm_topics WHERE user_id = $1 AND id = ANY($2)`, uid, ids)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id uuid.UUID
			var label string
			if err := rows.Scan(&id, &label); err != nil {
				return err
			}
			if n := nodes[id]; n != nil {
				n.Label = label
			}
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	for p := range edges {
		if a, b := nodes[p.a], nodes[p.b]; a != nil && b != nil {
			a.Links++
			b.Links++
		}
	}
	g := &Graph{Nodes: []Node{}, Edges: []Edge{}, TotalNodes: len(nodes)}
	for _, n := range nodes {
		if n.Links >= minLinks {
			g.Nodes = append(g.Nodes, *n)
		}
	}
	sort.Slice(g.Nodes, func(i, j int) bool {
		a, b := g.Nodes[i], g.Nodes[j]
		if a.Links != b.Links {
			return a.Links > b.Links
		}
		if a.Days != b.Days {
			return a.Days > b.Days
		}
		return a.ID.String() < b.ID.String()
	})
	if len(g.Nodes) > lim.MaxNodes {
		g.Nodes = g.Nodes[:lim.MaxNodes]
	}
	keep := map[uuid.UUID]bool{}
	for _, n := range g.Nodes {
		keep[n.ID] = true
	}
	var between []Edge
	for p, n := range edges {
		if keep[p.a] && keep[p.b] {
			between = append(between, Edge{A: p.a, B: p.b, Days: n, Notes: n})
		}
	}
	g.TotalEdges = len(between)
	g.Edges = capEdges(between, lim.NodeEdges, lim.MaxEdges)
	if g.Edges == nil {
		g.Edges = []Edge{}
	}
	return g, nil
}
