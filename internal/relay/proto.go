// Package relay is the channel between the agent and workspaces
// (FTR.NAB.CMN-0001 arch §5.1, tech §6). A workspace — a personal sandbox or
// the runner of a calling service — opens a WebSocket to the relay and keeps
// it; the agent's tool calls come to the relay over HTTP and travel to the
// workspace as frames. No inbound access to workspaces is needed, so the
// scheme works across clusters and networks.
//
// The tunnel carries the operations of the workspace server (fs/read,
// fs/write, exec, …, the protocol of FTR.HMR.CMN-0004 tech §6): the workspace
// side serves a call frame with its own HTTP handler and sends the response
// back as result frames, so exec output streams through unchanged.
package relay

import (
	"encoding/json"
)

// Protocol is the version in hello.
const Protocol = 1

// MaxFrame bounds a frame; larger bodies travel in chunks (RLY-04).
const MaxFrame = 1 << 20

// chunkSize keeps a base64-encoded chunk with its envelope under MaxFrame.
const chunkSize = 512 << 10

// Frame types.
const (
	FrameHello  = "hello"
	FrameCall   = "call"
	FrameResult = "result"
	FrameAbort  = "abort"
	FramePing   = "ping"
	FramePong   = "pong"
)

// Frame is one message of the channel.
type Frame struct {
	Type string `json:"type"`
	ID   string `json:"id,omitempty"`
	// hello
	WorkspaceID string `json:"workspaceId,omitempty"`
	Kind        string `json:"kind,omitempty"` // sandbox | external
	Protocol    int    `json:"protocol,omitempty"`
	// call: Tool is the operation of the workspace server (fs/read, exec, …).
	Tool   string          `json:"tool,omitempty"`
	Params json.RawMessage `json:"params,omitempty"`
	// result: Status is set in the first frame; Chunk carries body bytes
	// (base64 in JSON); Final ends the response.
	Seq    int    `json:"seq,omitempty"`
	Status int    `json:"status,omitempty"`
	Chunk  []byte `json:"chunk,omitempty"`
	Final  bool   `json:"final,omitempty"`
	Error  string `json:"error,omitempty"`
}

// Ops are the operations a call may name.
var Ops = map[string]bool{
	"fs/read": true, "fs/access": true, "fs/stat": true, "fs/readdir": true, "fs/write": true,
	"fs/mkdir": true, "fs/glob": true, "grep": true, "exec": true, "abort": true,
}

// Streaming reports operations whose interruption is not retried: their
// effects cannot be repeated safely (tech §6: bash returns workspace_disconnected).
func Streaming(op string) bool { return op == "exec" }
