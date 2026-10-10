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
		Name: "nabu_agent_process_starts_total", Help: "Pi process starts by reason (new, restore, crash, check).",
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
		HTTPRequests, HTTPDuration, KafkaLag, AgentSessions, AgentProcessStarts, LLMRequests, LLMErrors, LLMTokens, LLMCost,
		Turns, WorkspaceConnections, WorkspaceCalls, Sandboxes, TaskRuns, ServiceRuns, ChannelMessages,
		ChannelInbound, ChannelOutbound, EmailAuthRejected, IMAPConnected, VKTeamsPollLag, PendingConfirmations, Users, PurgeDeleted)
}
