package workspace

import (
	"regexp"
	"sync"
)

// Missing is a module or a command a command could not find
// (FTR.NAB.CMN-0008 R11, tech §6).
type Missing struct {
	Ecosystem string `json:"ecosystem"` // python-module | command
	Name      string `json:"name"`
}

// missingTail is how much of the end of the output is searched.
const missingTail = 64 << 10

var (
	reMissingModule  = regexp.MustCompile(`ModuleNotFoundError: No module named '([A-Za-z_][A-Za-z0-9_]*)`)
	reMissingCommand = regexp.MustCompile(`(?m)(?:^|: )([A-Za-z0-9_][A-Za-z0-9_.+-]{0,63}): (?:command )?not found\s*$`)
	reMissingZsh     = regexp.MustCompile(`(?m)command not found: ([A-Za-z0-9_][A-Za-z0-9_.+-]{0,63})\s*$`)
	ignoredCommands  = map[string]bool{"bash": true, "sh": true, "line": true, "dash": true, "zsh": true}
)

// FindMissing reads the modules and commands that were not found: at most
// five, each once.
func FindMissing(out []byte) []Missing {
	var items []Missing
	seen := map[Missing]bool{}
	add := func(m Missing) {
		if !seen[m] && len(items) < 5 {
			seen[m] = true
			items = append(items, m)
		}
	}
	for _, m := range reMissingModule.FindAllSubmatch(out, -1) {
		add(Missing{Ecosystem: "python-module", Name: string(m[1])})
	}
	for _, re := range []*regexp.Regexp{reMissingCommand, reMissingZsh} {
		for _, m := range re.FindAllSubmatch(out, -1) {
			if name := string(m[1]); !ignoredCommands[name] {
				add(Missing{Ecosystem: "command", Name: name})
			}
		}
	}
	return items
}

// tailBuffer keeps the last max bytes written.
type tailBuffer struct {
	mu  sync.Mutex
	max int
	buf []byte
}

func (t *tailBuffer) Write(p []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buf = append(t.buf, p...)
	if over := len(t.buf) - t.max; over > 0 {
		t.buf = append(t.buf[:0], t.buf[over:]...)
	}
}

func (t *tailBuffer) Bytes() []byte {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]byte(nil), t.buf...)
}
