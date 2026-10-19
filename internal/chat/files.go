package chat

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"path"
	"strings"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/platform/mcp"
)

// Files the agent sends to the chat (FTR.NAB.CHAT-0001 R33): the agent names a
// file of its space, the file is copied to the attachments and shown under
// the answer being written; a messenger gets it after the text.

// ReadFile reads the current version of a file of the user's space, at most
// max bytes. Its *mcp.ToolError is told to the agent as it is.
type ReadFile func(ctx context.Context, uid uuid.UUID, p string, max int64) ([]byte, error)

// AttachTool is chat_attach of the built-in MCP.
func (a *API) AttachTool(read ReadFile) mcp.Tool {
	return mcp.Tool{Name: "chat_attach",
		Description: "Send a file of your space to the user: it is attached to your current answer (in a task — to its result) " +
			"(the site previews images, PDF, Markdown, text and tables; messengers and mail get the files after the text). " +
			fmt.Sprintf("Write the file first. At most %d files per answer, each up to %d MB. ", MaxAgentFiles, a.MaxFile>>20) +
			"Mention the file in your answer instead of pasting its content.",
		InputSchema: mcp.Schema(map[string]any{
			"path": map[string]any{"type": "string", "description": "Path of the file in your space, e.g. reports/summary.pdf"},
		}, "path"),
		// a conversation, or a run of a task: its files go with its result
		Available: func(_ context.Context, g mcp.Grant) bool { return g.Conversation != uuid.Nil || g.TaskID != uuid.Nil },
		Handler: func(ctx context.Context, g mcp.Grant, args json.RawMessage) (string, error) {
			var in struct {
				Path string `json:"path"`
			}
			if err := json.Unmarshal(args, &in); err != nil || strings.TrimSpace(in.Path) == "" {
				return "", &mcp.ToolError{Msg: "path is required"}
			}
			data, err := read(ctx, g.UserID, in.Path, a.MaxFile)
			if err != nil {
				var te *mcp.ToolError
				if errors.As(err, &te) {
					return "", te
				}
				return "", &mcp.ToolError{Msg: "the file could not be read"}
			}
			name := path.Base(strings.ReplaceAll(in.Path, "\\", "/"))
			mt := FileType(name, data)
			id, err := a.storeAttachment(ctx, g.UserID, name, mt, bytes.NewReader(data), int64(len(data)))
			if err != nil {
				return "", err
			}
			at := Attachment{ID: id, FileName: name, MimeType: mt, Size: int64(len(data))}
			if err := a.attach(ctx, g, at); err != nil {
				a.dropAttachment(ctx, g.UserID, id)
				switch {
				case errors.Is(err, ErrNoAnswer):
					return "", &mcp.ToolError{Msg: "no answer is being written in this conversation or run"}
				case errors.Is(err, ErrTooManyFiles):
					return "", &mcp.ToolError{Msg: fmt.Sprintf("an answer or a run carries at most %d files", MaxAgentFiles)}
				}
				return "", err
			}
			return fmt.Sprintf("Sent %s (%s, %d bytes): it is shown under your answer.", name, mt, at.Size), nil
		}}
}

func (a *API) attach(ctx context.Context, g mcp.Grant, at Attachment) error {
	if g.Conversation == uuid.Nil {
		return a.Store.AttachToRun(ctx, g.TaskID, at)
	}
	return a.Store.AttachToAnswer(ctx, g.Conversation, at)
}

// FileType is the media type of a file the agent sends: by its extension,
// else by its content. Markdown is text/markdown, whatever the system knows.
func FileType(name string, data []byte) string {
	switch strings.ToLower(path.Ext(name)) {
	case ".md", ".markdown":
		return "text/markdown"
	}
	if t := mime.TypeByExtension(path.Ext(name)); t != "" {
		if mt, _, err := mime.ParseMediaType(t); err == nil {
			return mt
		}
	}
	mt, _, _ := mime.ParseMediaType(http.DetectContentType(data))
	return mt
}

// dropAttachment removes an attachment that did not reach a message.
func (a *API) dropAttachment(ctx context.Context, uid, id uuid.UUID) {
	if key, err := a.Store.DeleteAttachment(ctx, uid, id); err == nil {
		_ = a.S3.Delete(ctx, key)
	}
}
