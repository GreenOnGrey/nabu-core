package comm

import (
	"context"
	"math"
	"sort"
	"strings"
	"unicode"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// The matching of topics (FTR.NAB.CMN-0003 R8, decision 19): the collector
// names the key subject of a thread itself, then looks for the same subject
// among ALL the user's topics; the same subject in other words is the same
// topic.

// looseKey is the name of a topic without case, punctuation and the letter ё:
// «ИТ-стратегия», «ИТ стратегия» and «ит стратегия» are one key. It is looser
// than nameKey, which backs the unique index of comm_topics: names that only
// looseKey joins are joined by the code (topicKeys), so the topics stored
// before the rule stay valid.
func looseKey(s string) string {
	var b strings.Builder
	space := true
	for _, r := range strings.ToLower(s) {
		switch {
		case r == 'ё':
			r = 'е'
		case !unicode.IsLetter(r) && !unicode.IsDigit(r):
			if !space {
				b.WriteByte(' ')
				space = true
			}
			continue
		}
		b.WriteRune(r)
		space = false
	}
	return strings.TrimSpace(b.String())
}

// stemRunes is the length of the rough stem: «стратегия», «стратегии» and
// «стратегию» are one word.
const stemRunes = 5

func stem(w string) string {
	if r := []rune(w); len(r) > stemRunes {
		return string(r[:stemRunes])
	}
	return w
}

// filler holds the stems of the words that do not tell one subject from
// another, so «проекте» and «проектом» are filler too.
var filler = func() map[string]bool {
	out := map[string]bool{}
	for _, w := range strings.Fields("и в на по для с о об the of and for проект задача вопрос project task") {
		out[stem(w)] = true
	}
	return out
}()

// stems are the meaningful words of a name cut to their stems.
func stems(s string) map[string]bool {
	out := map[string]bool{}
	for _, w := range strings.Fields(looseKey(s)) {
		if st := stem(w); !filler[st] {
			out[st] = true
		}
	}
	return out
}

// Weights and thresholds of matching.
const (
	containedWeight = 0.9  // «ИТ стратегия» inside «ИТ стратегия 2027»
	oneWordWeight   = 0.6  // a one-word name inside a longer one: shown, never suggested
	topicCandidate  = 0.5  // shown to the collector to judge
	topicSuggested  = 0.75 // suggested as the same topic
	topicMatches    = 5    // matches shown for a name
)

// topicScore is how likely two names are one subject, 0…1: the larger of the
// share of common stems (Jaccard) and how much of the shorter name the longer
// one contains. A one-word name contained in a longer one — «Стратегия» in
// «ИТ Стратегия» — is left to the collector to judge.
func topicScore(a, b string) float64 {
	if looseKey(a) == looseKey(b) {
		return 1
	}
	sa, sb := stems(a), stems(b)
	shared := 0
	for w := range sa {
		if sb[w] {
			shared++
		}
	}
	if shared == 0 {
		return 0
	}
	union := len(sa) + len(sb) - shared
	shorter := min(len(sa), len(sb))
	jaccard := float64(shared) / float64(union)
	contained := containedWeight * float64(shared) / float64(shorter)
	if shorter == 1 && union > 1 {
		contained = oneWordWeight
	}
	return max(jaccard, contained)
}

// TopicMatch is an existing topic like a name the collector formed.
type TopicMatch struct {
	Name  string  `json:"name"`
	Notes int     `json:"notes"`
	Score float64 `json:"score"`
}

// TopicFound is the answer for one name: the matching topics, the best first,
// and the suggestion — the topic to use, or nil when the name is new.
type TopicFound struct {
	Query      string       `json:"query"`
	Matches    []TopicMatch `json:"matches"`
	Suggestion *string      `json:"suggestion"`
}

// FindTopics looks for every name among all the topics of the user.
func (s *Service) FindTopics(ctx context.Context, uid uuid.UUID, names []string) ([]TopicFound, error) {
	var all []topicRow
	err := s.as(ctx, uid, func(tx pgx.Tx) (err error) {
		all, err = allTopics(ctx, tx, uid)
		return err
	})
	if err != nil {
		return nil, err
	}
	out := []TopicFound{}
	for _, name := range uniqueTopics(names) {
		found := TopicFound{Query: name, Matches: []TopicMatch{}}
		var best float64
		for _, t := range all {
			if score := topicScore(name, t.Name); score >= topicCandidate {
				found.Matches = append(found.Matches, TopicMatch{Name: t.Name, Notes: t.Notes, Score: math.Floor(score*100) / 100})
				if score > best {
					best, found.Suggestion = score, &t.Name
				}
			}
		}
		if best < topicSuggested {
			found.Suggestion = nil
		}
		sort.SliceStable(found.Matches, func(i, j int) bool { return found.Matches[i].Score > found.Matches[j].Score })
		if len(found.Matches) > topicMatches {
			found.Matches = found.Matches[:topicMatches]
		}
		out = append(out, found)
	}
	return out, nil
}

type topicRow struct {
	ID    uuid.UUID
	Name  string
	Notes int
}

// allTopics lists every topic of the user, the most used first.
func allTopics(ctx context.Context, tx pgx.Tx, uid uuid.UUID) ([]topicRow, error) {
	rows, err := tx.Query(ctx, `SELECT p.id, p.name,
			(SELECT count(*) FROM comm_note_tags t WHERE t.user_id = p.user_id AND t.kind = 'topic' AND t.tag_id = p.id)
		FROM comm_topics p WHERE p.user_id = $1 ORDER BY 3 DESC, p.name, p.id`, uid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []topicRow{}
	for rows.Next() {
		var t topicRow
		if err := rows.Scan(&t.ID, &t.Name, &t.Notes); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// topicKeys maps the loose keys of the user's topics to their ids. When topics
// stored before the rule share a key, the most used one wins.
type topicKeys map[string]uuid.UUID

func loadTopicKeys(ctx context.Context, tx pgx.Tx, uid uuid.UUID) (topicKeys, error) {
	all, err := allTopics(ctx, tx, uid)
	if err != nil {
		return nil, err
	}
	out := topicKeys{}
	for _, t := range all {
		if k := looseKey(t.Name); k != "" {
			if _, ok := out[k]; !ok {
				out[k] = t.ID
			}
		}
	}
	return out, nil
}
