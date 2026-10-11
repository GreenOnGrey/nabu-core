package quality

import (
	"strings"
	"testing"
)

// perfect answers the corpus exactly as it is labelled.
func perfect(c *Corpus) Run {
	run := Run{Threads: map[string]Result{}}
	for _, th := range c.Threads {
		res := Result{Saved: true, Texts: []string{"Заголовок", "Выжимка без секретов"}}
		if th.Expect.Topic != nil {
			res.Topics = []string{*th.Expect.Topic}
		}
		for _, w := range th.Expect.Commitments {
			f := Found{Text: "Сделать: " + w.Keywords[0], Direction: w.Direction, Person: w.Person}
			if w.Due != nil {
				f.Due = *w.Due
			}
			res.Commitments = append(res.Commitments, f)
		}
		run.Threads[th.ID] = res
		run.Closed = append(run.Closed, th.Expect.Closes...)
	}
	return run
}

// The corpus is what qa §12 asks for, and the scoring tells a good run from bad ones.
func TestCorpusAndScore(t *testing.T) {
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	langs := map[string]int{}
	var wanted, secrets, closes, copyOnly int
	ids := map[string]bool{}
	for _, th := range c.Threads {
		langs[th.Lang]++
		wanted += len(th.Expect.Commitments)
		secrets += len(th.Expect.Secrets)
		closes += len(th.Expect.Closes)
		if strings.Contains(th.Note, "not named") {
			copyOnly++
		}
		if ids[th.ID] || len(th.Messages) == 0 || len(th.Expect.People) == 0 {
			t.Fatalf("thread %s is malformed", th.ID)
		}
		ids[th.ID] = true
		for _, m := range th.Messages {
			if m.ID == "" || m.Sender == "" || m.Date == "" || m.Body == "" {
				t.Fatalf("message of %s is malformed: %+v", th.ID, m)
			}
		}
		for _, w := range th.Expect.Commitments {
			if w.Direction != "mine" && w.Direction != "theirs" || w.Person == "" || len(w.Keywords) == 0 {
				t.Fatalf("expected commitment of %s is malformed: %+v", th.ID, w)
			}
		}
	}
	if len(c.Threads) != 50 || langs["ru"] < 30 || langs["en"] < 10 || wanted < 20 || secrets < 4 || closes != 3 || copyOnly != 4 || len(c.SeedCommitments) != 6 {
		t.Fatalf("corpus: %d threads %v, %d commitments, %d secrets, %d closings, %d copy-only, %d seeds",
			len(c.Threads), langs, wanted, secrets, closes, copyOnly, len(c.SeedCommitments))
	}

	good := Score(c, perfect(c))
	if f := good.Failures(); len(f) != 0 || good.Recall != 1 || good.Extra != 0 || good.MissedClosings != 0 {
		t.Fatalf("a perfect run fails: %v — %s", f, good.Summary())
	}

	// QLT-01: a third of the commitments is missed.
	bad := perfect(c)
	n := 0
	for id, res := range bad.Threads {
		if len(res.Commitments) > 0 && n%3 == 0 {
			res.Commitments = nil
			bad.Threads[id] = res
		}
		if len(res.Commitments) > 0 || n%3 == 0 {
			n++
		}
	}
	if r := Score(c, bad); r.Recall >= MinRecall || !has(r.Failures(), "QLT-01") {
		t.Fatalf("missed commitments pass: %s", r.Summary())
	}
	// QLT-02: a commitment invented in every thread.
	bad = perfect(c)
	for id, res := range bad.Threads {
		res.Commitments = append(res.Commitments, Found{Text: "Подумать о будущем", Direction: "mine", Person: "nobody@corp.example"})
		bad.Threads[id] = res
	}
	if r := Score(c, bad); !has(r.Failures(), "QLT-02") {
		t.Fatalf("invented commitments pass: %s", r.Summary())
	}
	// QLT-03: the directions are swapped.
	bad = perfect(c)
	for id, res := range bad.Threads {
		for i := range res.Commitments {
			res.Commitments[i].Direction = map[string]string{"mine": "theirs", "theirs": "mine"}[res.Commitments[i].Direction]
		}
		bad.Threads[id] = res
	}
	if r := Score(c, bad); r.Recall != 1 || !has(r.Failures(), "QLT-03") {
		t.Fatalf("swapped directions pass: %s", r.Summary())
	}
	// QLT-04: a new topic every time.
	bad = perfect(c)
	for id, res := range bad.Threads {
		res.Topics = []string{"Новая тема " + id}
		bad.Threads[id] = res
	}
	if r := Score(c, bad); !has(r.Failures(), "QLT-04") {
		t.Fatalf("new topics pass: %s", r.Summary())
	}
	// QLT-05: a reminder closed a commitment.
	bad = perfect(c)
	bad.Closed = append(bad.Closed, "s5")
	if r := Score(c, bad); r.FalseClosings != 1 || !has(r.Failures(), "QLT-05") {
		t.Fatalf("a false closing passes: %s", r.Summary())
	}
	// QLT-06: a secret copied to the summary; a refused call.
	bad = perfect(c)
	for _, th := range c.Threads {
		if len(th.Expect.Secrets) > 0 {
			res := bad.Threads[th.ID]
			res.Texts = append(res.Texts, "Передали доступ: "+th.Expect.Secrets[0])
			bad.Threads[th.ID] = res
			break
		}
	}
	if r := Score(c, bad); r.Leaks != 1 || !has(r.Failures(), "QLT-06") {
		t.Fatalf("a leaked secret passes: %s", r.Summary())
	}
	bad = perfect(c)
	bad.Denied = 2
	if r := Score(c, bad); !has(r.Failures(), "QLT-06") {
		t.Fatalf("refused calls pass: %s", r.Summary())
	}
}

func has(list []string, id string) bool {
	for _, s := range list {
		if strings.HasPrefix(s, id) {
			return true
		}
	}
	return false
}
