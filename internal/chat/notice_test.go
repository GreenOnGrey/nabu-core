package chat

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
)

// FTR.NAB.CHAT-0002 NC-05: the bounds of a notice name the field.
func TestValidateNotice(t *testing.T) {
	ok := Notice{Text: " **Discovery обновлён** ", Payload: json.RawMessage(`{"kind":"discovery.revision"}`), IdempotencyKey: "k-1"}
	if err := ValidateNotice(&ok); err != nil || ok.Text != "**Discovery обновлён**" {
		t.Fatalf("valid: %v %q", err, ok.Text)
	}
	noPayload := Notice{Text: "t", Payload: json.RawMessage(`null`), IdempotencyKey: "k"}
	if err := ValidateNotice(&noPayload); err != nil || noPayload.Payload != nil {
		t.Fatalf("null payload: %v %s", err, noPayload.Payload)
	}
	for _, c := range []struct {
		n     Notice
		field string
	}{
		{Notice{Text: "  ", IdempotencyKey: "k"}, "text"},
		{Notice{Text: strings.Repeat("я", MaxNoticeText+1), IdempotencyKey: "k"}, "text"},
		{Notice{Text: "t"}, "idempotencyKey"},
		{Notice{Text: "t", IdempotencyKey: strings.Repeat("k", MaxNoticeKey+1)}, "idempotencyKey"},
		{Notice{Text: "t", IdempotencyKey: "k", Payload: json.RawMessage(`[1]`)}, "payload"},
		{Notice{Text: "t", IdempotencyKey: "k", Payload: json.RawMessage(`{"a":"` + strings.Repeat("x", MaxNoticePayload) + `"}`)}, "payload"},
	} {
		err := ValidateNotice(&c.n)
		e, isApp := apperr.As(err)
		if !isApp || e.Code != "invalid_notice" || e.Details["field"] != c.field {
			t.Fatalf("NC-05 %s: %v", c.field, err)
		}
	}
}
