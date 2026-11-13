// Package collect holds the adapters of the sources of communications
// (FTR.NAB.CMN-0003 arch §3.2, tech §6.1): how the worker learns what
// changed in a source through its MCP server, which read tools the collector
// agent gets and how a message is linked. An adapter never returns the text
// of messages — only identifiers and metadata.
package collect

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
)

// ErrAccess is returned by a Caller when the server refused the credentials
// of the user: the access was revoked or expired (R17).
var ErrAccess = errors.New("no access to the source")

// ErrRateLimited is returned by a Caller when the server asked to slow down
// (HTTP 429): the tick is repeated later, it is not a failure
// (FTR.NAB.ADM-0001 tech §4.3).
var ErrRateLimited = errors.New("the source limits the rate of requests")

// Caller calls a tool of the MCP server of the source with the personal
// access of the user and returns the text of its result.
type Caller func(ctx context.Context, tool string, args any) (json.RawMessage, error)

// Source is a source an adapter gives.
type Source struct {
	Key   string `json:"key"`
	Title string `json:"title"`
}

// Msg is a message of a thread: its id and metadata.
type Msg struct {
	ID     string     `json:"id"`
	Author string     `json:"author,omitempty"`
	SentAt *time.Time `json:"sentAt,omitempty"`
	// URL opens the message when the server gives the link itself
	// (FTR.NAB.CMN-0006: the mail adapter); else Adapter.Link builds it.
	URL string `json:"url,omitempty"`
}

// Thread is a mail thread or a chat for a day that changed.
type Thread struct {
	Key          string   `json:"key"`
	DayKey       string   `json:"dayKey,omitempty"`
	Participants []string `json:"participants,omitempty"`
	Messages     []Msg    `json:"messages"`
	// Parts are more keys the agent reads for this note with the read tool:
	// the discussions of the messages of a chat day (FTR.NAB.ADM-0001 arch §3.3).
	Parts []string `json:"parts,omitempty"`
	// ReadFrom is where the read call starts: the message after the day
	// (FTR.NAB.ADM-0001 tech §4.2); empty — from the latest messages.
	ReadFrom string `json:"readFrom,omitempty"`
}

// Exclusions are what is never collected for the user (R4): the senders,
// folders and chats the user excluded and the addresses of the Nabu bot.
type Exclusions struct {
	Senders, Folders, Chats []string
	BotAddresses            []string
	// IncludeMuted are the muted groups the user returned to the collection
	// (FTR.NAB.ADM-0001 R8).
	IncludeMuted []string
	// Location is the time zone of the user: a chat note is a chat for a day
	// of the user. nil — UTC.
	Location *time.Location
}

// ChangeSet is what changed in a source since a cursor.
type ChangeSet struct {
	Threads []Thread
	// Cursor is the cursor to store when every thread of the set is processed.
	Cursor string
	// Skipped counts the messages left out as automatic.
	Skipped int
	// Truncated names the chats where the run stopped at the limit of
	// messages, with how many it read (FTR.NAB.ADM-0001 R10).
	Truncated map[string]int
}

// ToolCall is how the collector agent reads a thread.
type ToolCall struct {
	Tool string         `json:"tool"`
	Args map[string]any `json:"args"`
}

// Adapter knows one kind of MCP server.
type Adapter interface {
	// Kind is the value of catalog_items.adapter.
	Kind() string
	// Sources are the sources the server gives.
	Sources() []Source
	// Changes lists the threads with messages since the cursor; an empty
	// cursor starts at since (the depth of the first collection, R15).
	Changes(ctx context.Context, call Caller, source, cursor string, since, now time.Time, ex Exclusions) (ChangeSet, error)
	// ReadTools are the read tools of the server the collector agent may call.
	ReadTools() []string
	// ReadCall is the call that reads a thread.
	ReadCall(source string, t Thread) ToolCall
	// ThreadOf names the thread a call of a read tool is for: the proxy lets
	// through only the threads of the batch (tech §6.4).
	ThreadOf(tool string, args json.RawMessage) (string, bool)
	// Link opens a message in the source.
	Link(source string, t Thread, messageID string) string
	// TimePrecision is "minute", or "day" when the server gives only dates.
	TimePrecision(source string) string
	// CheckTools fails when the server lacks a tool the adapter needs.
	CheckTools(tools []string) error
}

// PartReader is an adapter whose notes have parts: the calls that read them
// (FTR.NAB.ADM-0001 arch §3.3).
type PartReader interface {
	ReadPartCalls(source string, t Thread) []ToolCall
}

// SourceChecker is an adapter of several sources, each with its own tools:
// a source is offered only when the server has its tools (FTR.NAB.ADM-0001 R4).
type SourceChecker interface {
	CheckSourceTools(source string, tools []string) error
}

var registry = map[string]Adapter{}

// Register adds an adapter.
func Register(a Adapter) { registry[a.Kind()] = a }

// Get finds the adapter of a kind.
func Get(kind string) (Adapter, bool) {
	a, ok := registry[kind]
	return a, ok
}

// Kinds lists the adapters.
func Kinds() []string {
	out := make([]string, 0, len(registry))
	for k := range registry {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func missing(have, need []string) error {
	set := map[string]bool{}
	for _, t := range have {
		set[t] = true
	}
	for _, n := range need {
		if !set[n] {
			return fmt.Errorf("the server has no tool %s", n)
		}
	}
	return nil
}
