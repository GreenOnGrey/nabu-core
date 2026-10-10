// Package engine runs agents in the worker (FTR.NAB.CMN-0001 arch §4, §6,
// §9a; tech §8–9a): it answers user messages from every channel in the
// conversation's harness session, performs the runs of scheduled tasks and
// of service agents, records usage and audit, and delivers answers to
// messengers.
package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/GreenOnGrey/nabu-core/internal/domain"
	"github.com/GreenOnGrey/nabu-core/internal/memory"
	"github.com/GreenOnGrey/nabu-core/internal/users"
)

var tones = map[string]string{
	"business": "business-like: precise, polite, to the point",
	"friendly": "friendly: warm and informal, but still precise",
	"brief":    "brief: the shortest useful answer, no small talk",
	"mentor":   "mentor: explain the reasoning and teach, suggest next steps",
}

// Persona is what the instructions of a personal session are built from
// (arch §4.2): the name and tone, the memory, the user's profile.
type Persona struct {
	User    *users.User
	Agent   users.Agent
	Memory  []memory.Record
	Spaces  bool
	Now     time.Time
	Catalog []string // titles of the connected tools
	// Group is set for a group agent (FTR.NAB.CMN-0002 R16).
	Group *GroupInfo
}

// GroupInfo describes the chat of a group agent.
type GroupInfo struct {
	Title, Channel, Owner string
}

// Hash changes when the instructions must change: the profile or the memory.
func (p Persona) Hash() string {
	h := sha256.New()
	fmt.Fprintf(h, "%s|%s|%s|%s|%s|%v", p.Agent.Name, p.Agent.Tone, p.User.Timezone, p.User.Language, memory.Hash(p.Memory), p.Catalog)
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// Instructions are APPEND_SYSTEM.md of the session.
func (p Persona) Instructions() string {
	if p.Group != nil {
		return p.groupInstructions()
	}
	var b strings.Builder
	name := p.User.Name
	if name == "" {
		name = p.User.Email
	}
	fmt.Fprintf(&b, "You are %s, the personal AI agent of %s (%s) on Nabu, the corporate agent platform of the company.\n", p.Agent.Name, name, p.User.Email)
	fmt.Fprintf(&b, "Your tone is %s.\n", tones[p.Agent.Tone])
	b.WriteString("Answer in the language the user writes in. You talk to the same user in several channels (the website, messengers, products of the company); it is one continuous conversation.\n")
	fmt.Fprintf(&b, "The user's time zone is %s.\n", p.User.Timezone)
	b.WriteString("\nYour platform tools (mcp__nabu__*): memory_save, memory_search and memory_delete for the user's long-term memory — save durable facts and preferences only; " +
		"space_info for the personal space; task_create, task_list and task_cancel for scheduled tasks — reminders and regular jobs that run even when the chat is closed. " +
		"When the user asks to do something later or regularly, create a task: ask for a missing time or channel, then confirm the schedule, the first run and the channel.\n")
	if p.Spaces {
		b.WriteString("Your file and shell tools (read, write, edit, bash, ls, find, grep) work in the user's personal space: an isolated workspace whose files persist between conversations.\n")
	} else {
		b.WriteString("You have no file system or shell in this deployment.\n")
	}
	if len(p.Catalog) > 0 {
		b.WriteString("Tools the user connected: " + strings.Join(p.Catalog, "; ") + ". You act in these systems on behalf of the user, with the user's rights.\n")
	}
	b.WriteString(profileBlock(p))
	return b.String()
}

// groupInstructions are the instructions of a group agent (arch §6).
func (p Persona) groupInstructions() string {
	var b strings.Builder
	fmt.Fprintf(&b, "You are %s, the group AI agent of the %s chat «%s» on Nabu, the corporate agent platform of the company. "+
		"You are an agent of the chat, not a personal agent of any member; the owner of the agent is %s.\n", p.Agent.Name, p.Group.Channel, p.Group.Title, p.Group.Owner)
	fmt.Fprintf(&b, "Your tone is %s.\n", tones[p.Agent.Tone])
	b.WriteString("Answer in the language of the message. You see only messages addressed to you (a mention or a reply) and their quotes; " +
		"every message names its author. Keep answers compact: one message for the whole chat.\n")
	b.WriteString("You have no access to the personal data, memory, space, mail or personal tools of the members. " +
		"When a member asks for something personal (their mail, their tasks, their files), suggest writing to you in a private chat with the bot.\n")
	b.WriteString("\nYour platform tools (mcp__nabu__*): memory_save, memory_search and memory_delete keep the memory of the GROUP — save facts useful to the whole chat only; " +
		"space_info shows the space of the group. Do not create scheduled tasks.\n")
	if p.Spaces {
		b.WriteString("Your file and shell tools work in the space of the group: files persist between conversations of the chat.\n")
	}
	if len(p.Catalog) > 0 {
		b.WriteString("Platform tools: " + strings.Join(p.Catalog, "; ") + ". They work with platform credentials, the same for every member.\n")
	}
	b.WriteString(strings.Replace(profileBlock(p), "The user's memory", "The memory of the group", 2))
	return b.String()
}

func profileBlock(p Persona) string {
	var b strings.Builder
	if len(p.Memory) == 0 {
		b.WriteString("\nThe user's memory is empty.\n")
		return b.String()
	}
	b.WriteString("\nThe user's memory (facts saved earlier; use them, do not repeat them back unless asked):\n")
	for _, r := range p.Memory {
		pin := ""
		if r.Pinned {
			pin = " (pinned)"
		}
		fmt.Fprintf(&b, "- [%s]%s %s\n", r.ID, pin, oneLine(r.Text))
	}
	return b.String()
}

// Update is prepended to the next prompt of an open session when the
// profile or the memory changed (memory deleted on the site stops being used
// from the next message: MEM-02).
func (p Persona) Update() string {
	return "[Nabu: your profile or the user's memory changed. From now on your name is " + p.Agent.Name + " and your tone is " +
		tones[p.Agent.Tone] + ". Forget memory records not listed below." + profileBlock(p) + "]"
}

func oneLine(s string) string { return strings.Join(strings.Fields(s), " ") }

// channelRules are the rules of the channel of a message (R9).
func channelRules(ch string, now time.Time, tz string) string {
	loc, err := time.LoadLocation(tz)
	if err != nil {
		loc = time.UTC
	}
	t := now.In(loc).Format("Monday 2006-01-02 15:04")
	switch {
	case ch == domain.ChannelTelegram:
		return "[Channel: Telegram, " + t + ". Keep answers compact; Markdown with headings, lists, code and tables is rendered; no HTML.]"
	case ch == domain.ChannelVKTeams || ch == "vkws":
		return "[Channel: VK Teams, " + t + ". Keep answers compact; Markdown is rendered, tables are shown as monospace text.]"
	case ch == domain.ChannelEmail:
		return "[Channel: mail, " + t + ". Markdown with headings, lists, code and tables is rendered in the letter.]"
	case strings.HasPrefix(ch, "client:"):
		return "[Channel: " + strings.TrimPrefix(ch, "client:") + " (embedded), " + t + ".]"
	default:
		return "[Channel: Nabu website, " + t + ". Markdown is rendered.]"
	}
}
