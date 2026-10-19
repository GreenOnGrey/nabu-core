package channels

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func file(name, mime, body string) OutFile {
	return OutFile{Name: name, MimeType: mime, Size: int64(len(body)),
		Open: func(context.Context) (io.ReadCloser, error) { return io.NopCloser(strings.NewReader(body)), nil }}
}

type tgCall struct {
	method, chat, media string
	files               map[string]string // field → name:body
}

func tgServer(t *testing.T) (*Telegram, *[]tgCall) {
	t.Helper()
	var calls []tgCall
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Error(err)
		}
		c := tgCall{method: r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:], chat: r.FormValue("chat_id"), media: r.FormValue("media"), files: map[string]string{}}
		for field, hs := range r.MultipartForm.File {
			f, _ := hs[0].Open()
			b, _ := io.ReadAll(f)
			c.files[field] = hs[0].Filename + ":" + string(b)
		}
		calls = append(calls, c)
		_, _ = w.Write([]byte(`{"ok":true,"result":{}}`))
	}))
	t.Cleanup(srv.Close)
	return NewTelegram(srv.URL, "123:abc", "s"), &calls
}

// FTR.NAB.CHAT-0001 ATT-02: a document goes by sendDocument, a photo by sendPhoto.
func TestTelegramSendFile(t *testing.T) {
	tg, calls := tgServer(t)
	ctx := context.Background()
	if err := tg.SendFile(ctx, "42", file("summary.md", "text/markdown", "# Итоги")); err != nil {
		t.Fatal(err)
	}
	if err := tg.SendFile(ctx, "42", file("chart.png", "image/png", "png")); err != nil {
		t.Fatal(err)
	}
	if err := tg.SendFile(ctx, "42", file("anim.gif", "image/gif", "gif")); err != nil {
		t.Fatal(err)
	}
	c := *calls
	if len(c) != 3 || c[0].method != "sendDocument" || c[0].chat != "42" || c[0].files["document"] != "summary.md:# Итоги" ||
		c[1].method != "sendPhoto" || c[1].files["photo"] != "chart.png:png" || c[2].method != "sendDocument" {
		t.Fatalf("sent %+v", c)
	}
}

// ATT-12: several photos are one album, several documents another.
func TestTelegramSendAlbum(t *testing.T) {
	tg, calls := tgServer(t)
	if err := tg.SendAlbum(context.Background(), "42", true, []OutFile{file("a.png", "image/png", "1"), file("b.jpg", "image/jpeg", "2")}); err != nil {
		t.Fatal(err)
	}
	c := (*calls)[0]
	var media []map[string]string
	_ = json.Unmarshal([]byte(c.media), &media)
	if c.method != "sendMediaGroup" || len(media) != 2 || media[0]["type"] != "photo" || media[1]["media"] != "attach://f1" ||
		c.files["f0"] != "a.png:1" || c.files["f1"] != "b.jpg:2" {
		t.Fatalf("album %+v %v", c, media)
	}
}

func TestPhoto(t *testing.T) {
	if !Photo("image/jpeg", 10<<20) || Photo("image/jpeg", 10<<20+1) || Photo("image/gif", 10) || Photo("image/svg+xml", 10) {
		t.Fatal("photos are JPEG, PNG and WebP up to 10 MB")
	}
}
