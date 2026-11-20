package preview

import (
	"archive/zip"
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestKind(t *testing.T) {
	for name, want := range map[string]string{"offer.DOCX": "doc", "a.ppt": "doc", "x.odp": "doc", "macro.docm": "doc",
		"budget.xlsx": "sheet", "old.xls": "sheet", "t.ods": "sheet", "a.pdf": "", "noext": ""} {
		if got := Kind(name); got != want {
			t.Errorf("%s: %q", name, got)
		}
	}
}

// PRV-02: the pages Word wrote and the slides PowerPoint wrote.
func TestDocPages(t *testing.T) {
	mk := func(app string) []byte {
		var b bytes.Buffer
		zw := zip.NewWriter(&b)
		w, _ := zw.Create("docProps/app.xml")
		_, _ = w.Write([]byte(app))
		_ = zw.Close()
		return b.Bytes()
	}
	if n := docPages("a.docx", mk(`<Properties><Pages>120</Pages></Properties>`)); n == nil || *n != 120 {
		t.Fatalf("docx: %v", n)
	}
	if n := docPages("a.pptx", mk(`<Properties><Slides>80</Slides><Pages>1</Pages></Properties>`)); n == nil || *n != 80 {
		t.Fatalf("pptx: %v", n)
	}
	if n := docPages("a.doc", []byte("x")); n != nil {
		t.Fatal("PRV-03: no total for DOC")
	}
}

func TestCountPages(t *testing.T) {
	pdf := []byte("<< /Type /Pages /Count 2 >> << /Type /Page /Parent 1 0 R >> << /Type/Page >>")
	if n := countPages(pdf); n != 2 {
		t.Fatalf("pages %d", n)
	}
}

// PRV-18 (FTR.NAB.CHAT-0003 tech, decision 32): Gotenberg 9 takes the pages of LibreOffice as nativePages and answers 400
// to the field nativePageRanges of Gotenberg 8.
func TestToPDFFields(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if u, p, _ := r.BasicAuth(); u != "u" || p != "p" || r.URL.Path != "/forms/libreoffice/convert" {
			http.Error(w, "bad", http.StatusNotFound)
			return
		}
		if err := r.ParseMultipartForm(1 << 20); err != nil || r.FormValue("nativePageRanges") != "" {
			http.Error(w, "a Gotenberg 8 field", http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte(r.FormValue("nativePages")))
	}))
	defer srv.Close()
	s := &Service{Cfg: Config{ConvertURL: srv.URL + "/", ConvertUser: "u", ConvertPassword: "p"}}
	got, err := s.toPDF(context.Background(), "a.docx", []byte("DATA"))
	if err != nil || string(got) != "1-50" {
		t.Fatalf("%q %v", got, err)
	}
}
