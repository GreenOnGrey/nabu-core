// Package metrics declares the Prometheus metrics exported on :9100/metrics.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Registry is the process-wide registry.
var Registry = prometheus.NewRegistry()

var (
	HTTPRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nabu_http_requests_total", Help: "HTTP requests by route, method and status.",
	}, []string{"route", "method", "status"})
	HTTPDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "nabu_http_request_duration_seconds", Help: "HTTP request latency.", Buckets: prometheus.DefBuckets,
	}, []string{"route", "method"})
	KafkaLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "nabu_kafka_consumer_lag", Help: "Kafka consumer lag by topic.",
	}, []string{"topic"})
	AgentSessions = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "nabu_agent_sessions_active", Help: "Active harness sessions of the agent operator by kind.",
	}, []string{"kind"})
	AgentProcessStarts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nabu_agent_process_starts_total", Help: "Pi process starts by reason (new, restore, crash, check, subagent).",
	}, []string{"reason"})
	// FTR.NAB.CMN-0009 arch §6: subagents of the turns.
	SubagentRuns = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nabu_subagent_runs_total", Help: "Subagents by session kind and result (done, failed, timeout, aborted).",
	}, []string{"kind", "result"})
	SubagentDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "nabu_subagent_duration_seconds", Help: "Run time of subagents.", Buckets: []float64{5, 15, 30, 60, 120, 300, 600, 900},
	}, []string{"kind"})
	SubagentRejected = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nabu_subagent_rejected_total", Help: "Refused calls of subagents by reason (turn_limit, call_limit, no_slot, disabled).",
	}, []string{"reason"})
	LLMRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nabu_llm_requests_total", Help: "Agent runs (prompts) by connection, model and kind.",
	}, []string{"connection", "model", "kind"})
	LLMErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nabu_llm_errors_total", Help: "LLM errors by class and connection.",
	}, []string{"class", "connection"})
	LLMTokens = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nabu_llm_tokens_total", Help: "LLM tokens by direction (in, out, cache_read, cache_write).",
	}, []string{"direction"})
	LLMCost = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nabu_llm_cost_usd_total", Help: "LLM cost in US dollars by connection and model.",
	}, []string{"connection", "model"})
	Turns = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nabu_turns_total", Help: "Agent turns by channel and result.",
	}, []string{"channel", "result"})
	WorkspaceConnections = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "nabu_relay_workspace_connections", Help: "Workspaces connected to this relay pod by kind.",
	}, []string{"kind"})
	WorkspaceCalls = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nabu_relay_calls_total", Help: "Workspace tool calls by operation and result.",
	}, []string{"op", "result"})
	Sandboxes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "nabu_sandboxes_running", Help: "Personal sandboxes not sleeping.",
	})
	TaskRuns = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nabu_task_runs_total", Help: "Scheduled task runs by result.",
	}, []string{"result"})
	// FTR.NAB.CMN-0003 arch §9: the memory of communications.
	MemoryMaskedSecrets = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "nabu_memory_masked_secrets_total", Help: "Secrets masked in the notes and commitments the collector saved.",
	})
	MemoryDeniedTools = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "nabu_memory_denied_tools_total", Help: "Tool calls of collector runs refused by the built-in MCP or the MCP proxy.",
	})
	MemoryCollect = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nabu_memory_collect_runs_total", Help: "Runs of the collection of communications by result.",
	}, []string{"result"})
	MemoryCollectSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "nabu_memory_collect_seconds", Help: "Duration of a run of the collector agent over a batch.",
		Buckets: []float64{5, 15, 30, 60, 120, 300, 600},
	})
	MemoryEmbedPending = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "nabu_memory_embed_pending", Help: "Notes waiting for a vector of the current model of embeddings.",
	})
	MemoryDigests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nabu_memory_digests_total", Help: "Digests of commitments by period and result.",
	}, []string{"period", "result"})
	MemorySearch = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "nabu_memory_search_seconds", Help: "Duration of the search, the table and the graph of communications.",
		Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2, 3, 5},
	}, []string{"kind"})
	ServiceRuns = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nabu_service_agent_runs_total", Help: "Service agent runs by agent and result.",
	}, []string{"agent", "result"})
	ChannelMessages = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nabu_channel_messages_total", Help: "Messenger messages by channel and direction.",
	}, []string{"channel", "direction"})

	// FTR.NAB.CMN-0002 arch §10.
	ChannelInbound = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nabu_channel_inbound_total", Help: "Incoming channel messages by channel and result (accepted, rejected, unavailable, automatic).",
	}, []string{"channel", "result"})
	ChannelOutbound = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nabu_channel_outbound_total", Help: "Outgoing channel messages by channel and result.",
	}, []string{"channel", "result"})
	EmailAuthRejected = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nabu_email_auth_rejected_total", Help: "Letters rejected by the authenticity check, by reason.",
	}, []string{"reason"})
	IMAPConnected = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "nabu_imap_connected", Help: "1 while the mail receiver holds the IMAP connection.",
	})
	VKTeamsPollLag = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "nabu_vkteams_poll_lag_seconds", Help: "Seconds since the last successful VK Teams poll.",
	})
	PendingConfirmations = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "nabu_pending_confirmations", Help: "Tool calls waiting for the confirmation of users.",
	})
	Users = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "nabu_users", Help: "Users by status.",
	}, []string{"status"})
	PurgeDeleted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nabu_purge_deleted_total", Help: "Accounts and group agents deleted by the purge.",
	}, []string{"kind"})
)

// FTR.NAB.CMN-0004 arch §9: the pods of agents and the queue of turns.
var (
	AgentPods = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "nabu_agent_pods", Help: "Agent pods by state.",
	}, []string{"state"})
	AgentPodsCapacity = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "nabu_agent_pods_capacity", Help: "The ceiling of agent pods by its source; 0 without a ceiling.",
	}, []string{"source"})
	AgentPodsWarm = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "nabu_agent_pods_warm", Help: "Agent pods kept warm.",
	})
	AgentPodsWarmTarget = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "nabu_agent_pods_warm_target", Help: "The size of the warm reserve the manager aims at.",
	})
	AgentPodStart = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "nabu_agent_pod_start_seconds", Help: "From creating an agent pod to its readiness.",
		Buckets: []float64{1, 2, 3, 5, 8, 12, 20, 30, 60},
	})
	AgentPodStops = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nabu_agent_pod_stops_total", Help: "Stopped agent pods by reason.",
	}, []string{"reason"})
	AgentQueueLength = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "nabu_agent_queue_length", Help: "Turns waiting for an agent pod by reason.",
	}, []string{"reason"})
	AgentQueueEnqueued = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nabu_agent_queue_enqueued_total", Help: "Turns put into the queue by reason.",
	}, []string{"reason"})
	AgentQueueWait = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "nabu_agent_queue_wait_seconds", Help: "The time a turn waited for its agent pod.",
		Buckets: []float64{1, 2, 5, 10, 20, 30, 60, 120, 300, 600},
	})
	AgentQueueExpired = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "nabu_agent_queue_expired_total", Help: "Turns that did not get a pod within the queue timeout.",
	})
	AgentManagerLoop = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "nabu_agent_manager_last_loop_timestamp_seconds", Help: "The time of the last loop of the pod manager.",
	})
	TurnFirstToken = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "nabu_turn_first_token_seconds", Help: "From a message to the first piece of the answer by start (hot, warm, cold).",
		Buckets: []float64{0.5, 1, 2, 3, 5, 8, 12, 20, 30, 45, 60, 120},
	}, []string{"start"})
)

func init() {
	Registry.MustRegister(AgentPods, AgentPodsCapacity, AgentPodsWarm, AgentPodsWarmTarget, AgentPodStart, AgentPodStops,
		AgentQueueLength, AgentQueueEnqueued, AgentQueueWait, AgentQueueExpired, AgentManagerLoop, TurnFirstToken)
}

func init() {
	Registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		HTTPRequests, HTTPDuration, KafkaLag, AgentSessions, AgentProcessStarts, SubagentRuns, SubagentDuration, SubagentRejected, LLMRequests, LLMErrors, LLMTokens, LLMCost,
		Turns, WorkspaceConnections, WorkspaceCalls, Sandboxes, TaskRuns, ServiceRuns, ChannelMessages,
		ChannelInbound, ChannelOutbound, EmailAuthRejected, IMAPConnected, VKTeamsPollLag, PendingConfirmations, Users, PurgeDeleted,
		MemoryMaskedSecrets, MemoryDeniedTools, MemoryDigests, MemorySearch, MemoryCollect, MemoryCollectSeconds, MemoryEmbedPending)
}

// FTR.NAB.CMN-0006 tech §3.7: calls of tools of catalog MCP servers through
// the proxy of api. tool is a name from the stored list of the item, else
// "other": the labels do not grow with the names an agent sends.
var (
	MCPProxyCalls = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nabu_mcp_proxy_calls_total", Help: "Calls of tools of catalog MCP servers through the proxy by item, tool and result.",
	}, []string{"item", "tool", "result"})
	MCPProxyCallSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "nabu_mcp_proxy_call_seconds", Help: "Duration of calls of tools of catalog MCP servers through the proxy.",
		Buckets: []float64{.1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300},
	}, []string{"item", "tool"})
)

func init() {
	Registry.MustRegister(MCPProxyCalls, MCPProxyCallSeconds)
}

// FTR.NAB.CHAT-0001 arch §7: the context of conversations, skills, images,
// models of connections, the GigaChat bridge and voice.
var (
	ContextEvents = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nabu_context_events_total", Help: "Resets and compactions of the context of conversations by kind and auto.",
	}, []string{"kind", "auto"})
	SkillInvocations = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nabu_skill_invocations_total", Help: "Explicit calls of skills by result (ok, unavailable).",
	}, []string{"result"})
	// FTR.NAB.CHAT-0002 arch §7.
	Notices = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nabu_notices_total", Help: "Notices of products in topics by client and result (created, duplicate, not_found, invalid).",
	}, []string{"client", "result"})
	ProductMCPCalls = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nabu_product_mcp_calls_total", Help: "Delegated calls of the MCP of products by item and whether they carry the context of a turn (yes, no).",
	}, []string{"item", "turn_context"})
	PromptImages = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nabu_prompt_images_total", Help: "Images of user messages by where they went (model, space).",
	}, []string{"delivered"})
	ModelsRefresh = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nabu_models_refresh_total", Help: "Refreshes of the models of connections by type and result.",
	}, []string{"type", "result"})
	GigaChatBridge = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nabu_gigachat_bridge_requests_total", Help: "Requests through the GigaChat bridge by status.",
	}, []string{"status"})
	GigaChatTokens = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nabu_gigachat_token_refresh_total", Help: "Access tokens of GigaChat obtained by result.",
	}, []string{"result"})
	Transcriptions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nabu_transcriptions_total", Help: "Voice recognitions by channel and result.",
	}, []string{"channel", "result"})
	TranscriptionSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "nabu_transcription_seconds", Help: "Duration of a voice recognition.",
		Buckets: []float64{0.5, 1, 2, 3, 5, 8, 12, 20, 30, 60, 120},
	})
)

func init() {
	Registry.MustRegister(ContextEvents, SkillInvocations, Notices, ProductMCPCalls, PromptImages, ModelsRefresh, GigaChatBridge, GigaChatTokens,
		Transcriptions, TranscriptionSeconds)
}

// FTR.NAB.CMN-0008 tech §7.10: sandbox profiles and requests of libraries.
// profile is the name of a profile: a stand has a handful of them.
var (
	SandboxStartSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "nabu_sandbox_start_seconds", Help: "From the creation of a sandbox pod to its connection to the relay.",
		Buckets: []float64{1, 2, 5, 10, 15, 20, 30, 45, 60, 120},
	}, []string{"profile", "profile_kind", "kind"})
	SandboxStartFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nabu_sandbox_start_failures_total", Help: "Sandbox pods that did not start by profile and reason (pull, crash, timeout).",
	}, []string{"profile", "profile_kind", "reason"})
	SandboxPrepullNodes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "nabu_sandbox_prepull_nodes", Help: "Nodes where the image of a profile is preloaded (ready) or not yet (pending).",
	}, []string{"profile", "state"})
	SandboxProbes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nabu_sandbox_probe_total", Help: "Checks of custom sandbox images by result.",
	}, []string{"result"})
	SandboxPackageSignals = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nabu_sandbox_package_signals_total", Help: "Signals of missing packages by ecosystem, source, profile and result (ok, duplicate, rate_limited, invalid).",
	}, []string{"ecosystem", "source", "profile", "result"})
	SandboxImageRestarts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nabu_sandbox_image_restarts_total", Help: "Idle sandboxes restarted on a new image of their profile.",
	}, []string{"profile"})
)

func init() {
	Registry.MustRegister(SandboxStartSeconds, SandboxStartFailures, SandboxPrepullNodes, SandboxProbes,
		SandboxPackageSignals, SandboxImageRestarts)
}

// FTR.NAB.CHAT-0003 tech §6.4: previews of office files.
var (
	PreviewRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nabu_preview_requests_total", Help: "Requests of previews by kind (doc, sheet) and result (ok, pending, too_large, timeout, broken, busy, disabled).",
	}, []string{"kind", "result"})
	PreviewSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "nabu_preview_seconds", Help: "Time to a ready preview: the conversion (miss) or the cache (hit).",
		Buckets: []float64{.1, .5, 1, 2, 5, 10, 20, 30},
	}, []string{"kind", "cache"})
)

func init() {
	Registry.MustRegister(PreviewRequests, PreviewSeconds)
}
