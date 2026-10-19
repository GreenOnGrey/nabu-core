package chat

import "testing"

// FTR.NAB.CHAT-0001 R33: the type of a file the agent sends decides its preview.
func TestFileType(t *testing.T) {
	for name, want := range map[string]string{
		"report.md":       "text/markdown",
		"README.MARKDOWN": "text/markdown",
		"scan.pdf":        "application/pdf",
		"chart.png":       "image/png",
		"noext":           "text/plain",
	} {
		if got := FileType(name, []byte("plain words")); got != want {
			t.Errorf("FileType(%q) = %q, want %q", name, got, want)
		}
	}
}
