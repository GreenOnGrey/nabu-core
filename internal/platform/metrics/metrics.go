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
)

func init() {
	Registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		HTTPRequests, HTTPDuration, KafkaLag, AgentSessions, AgentProcessStarts, LLMRequests, LLMErrors, LLMTokens, LLMCost,
		Turns, WorkspaceConnections, WorkspaceCalls, Sandboxes, TaskRuns, ServiceRuns, ChannelMessages)
}
