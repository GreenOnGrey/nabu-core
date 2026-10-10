package channels

import (
	"encoding/json"
	"strings"
	"testing"
)

// TG-01, TG-04: the format of the key and its normalization.
func TestKeyFormat(t *testing.T) {
	k := NewKey()
	if len(k) != 17 || !strings.HasPrefix(k, "NB-") || strings.ContainsAny(k[3:], "0O1IL") {
		t.Fatal(k)
	}
	for _, v := range []string{k, strings.ToLower(k), strings.ReplaceAll(k, "-", ""), " " + strings.ReplaceAll(k, "-", " ") + "\n", k[3:]} {
		if !LooksLikeKey(v) || NormalizeKey(v) != NormalizeKey(k) {
			t.Fatalf("%q", v)
		}
	}
	for _, v := range []string{"привет", "NB-AAAA-AAAA", "what is the status of the release", "NB-AAAA-AAAA-AAA0"} {
		if LooksLikeKey(v) {
			t.Fatalf("%q looks like a key", v)
		}
	}
	a, b := &Keys{Pepper: []byte("one")}, &Keys{Pepper: []byte("two")}
	if string(a.Hash(k)) == string(b.Hash(k)) || string(a.Hash(k)) != string(a.Hash(strings.ToLower(k))) {
		t.Fatal("TG-02: the hash depends on the pepper only")
	}
	if string(DerivePepper([]byte("0123456789abcdef0123456789abcdef"))) == "" {
		t.Fatal("pepper")
	}
}

// tech §7: a group message addresses the bot by a mention or a reply to it.
func TestMentions(t *testing.T) {
	parse := func(s string) *TGMessage {
		var m TGMessage
		if err := json.Unmarshal([]byte(s), &m); err != nil {
			t.Fatal(err)
		}
		return &m
	}
	if !parse(`{"text":"😀 @Nabu_Bot привет","entities":[{"type":"mention","offset":3,"length":9}]}`).Mentions("nabu_bot", 7) {
		t.Fatal("a mention after an emoji (UTF-16 offsets)")
	}
	if parse(`{"text":"@other_bot привет","entities":[{"type":"mention","offset":0,"length":10}]}`).Mentions("nabu_bot", 7) {
		t.Fatal("a mention of another bot")
	}
	if !parse(`{"text":"а подробнее?","reply_to_message":{"from":{"id":7}}}`).Mentions("nabu_bot", 7) {
		t.Fatal("a reply to the bot")
	}
	if parse(`{"text":"обсуждаем релиз","reply_to_message":{"from":{"id":8}}}`).Mentions("nabu_bot", 7) {
		t.Fatal("GR-03: a message not for the bot")
	}
}

func TestTexts(t *testing.T) {
	for k := range texts["en"] {
		if _, ok := texts["ru"][k]; !ok {
			t.Fatalf("no Russian text for %s", k)
		}
	}
	if !strings.Contains(T("ru-RU", "tg.blocked"), "час") || !strings.Contains(T("de", "tg.blocked"), "hour") {
		t.Fatal("language choice")
	}
}
