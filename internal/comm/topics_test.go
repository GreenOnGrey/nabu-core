package comm

import "testing"

// NOTE-18: names that differ only by case, punctuation or ё are one key.
func TestLooseKey(t *testing.T) {
	for in, want := range map[string]string{
		"ИТ-стратегия":    "ит стратегия",
		" ИТ  Стратегия ": "ит стратегия",
		"Ёлка, проект!":   "елка проект",
	} {
		if got := looseKey(in); got != want {
			t.Errorf("looseKey(%q) = %q, want %q", in, got, want)
		}
	}
	if got := uniqueTopics([]string{"ИТ-стратегия", "ИТ Стратегия", "Релиз"}); len(got) != 2 || got[0] != "ИТ-стратегия" {
		t.Errorf("uniqueTopics = %q", got)
	}
}

// NOTE-10, NOTE-17
func TestTopicScore(t *testing.T) {
	cases := []struct {
		a, b      string
		suggested bool // ≥ topicSuggested
		candidate bool // ≥ topicCandidate: shown
	}{
		{"ИТ-стратегия", "ИТ Стратегия", true, true},
		{"стратегия ИТ 2027", "ИТ Стратегия", true, true},
		{"ИТ стратегии", "ИТ Стратегия", true, true},
		{"Альфа", "Проект Альфа", true, true},
		{"выпуск версии 2.3", "Релиз 2.3", false, true}, // NOTE-10: shown, the collector judges
		{"StarLine", "Миграция StarLine", false, true},  // one word inside a longer name: shown only
		{"Стратегия", "ИТ Стратегия", false, true},
		{"Проект Альфа", "Проект Бета", false, false},
		{"Проекте Альфа", "Проектом Бета", false, false}, // inflected filler words do not count
		{"Стратегия продаж", "ИТ Стратегия", false, false},
		{"StarLine", "ИТ Стратегия", false, false},
	}
	for _, c := range cases {
		score := topicScore(c.a, c.b)
		if (score >= topicSuggested) != c.suggested || (score >= topicCandidate) != c.candidate {
			t.Errorf("topicScore(%q, %q) = %.3f, want suggested=%v candidate=%v", c.a, c.b, score, c.suggested, c.candidate)
		}
	}
}
