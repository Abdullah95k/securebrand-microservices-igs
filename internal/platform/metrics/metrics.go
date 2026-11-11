// Package metrics holds the Prometheus registry and the service metrics
// shared across binaries. Metric names are prefixed with igs_, never fbs_:
// fbs's alert rules select by metric name alone, so an igs pod exporting
// fbs_ series would feed fbs's alerts (ADR 0004 §5).
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Registry is the process registry exposed on /metrics.
var Registry = prometheus.NewRegistry()

func init() {
	Registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
}

func counterVec(name, help string, labels ...string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "igs", Name: name, Help: help}, labels)
	Registry.MustRegister(c)
	return c
}

func counter(name, help string) prometheus.Counter {
	c := prometheus.NewCounter(prometheus.CounterOpts{Namespace: "igs", Name: name, Help: help})
	Registry.MustRegister(c)
	return c
}

func histVec(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
	h := prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "igs", Name: name, Help: help, Buckets: buckets}, labels)
	Registry.MustRegister(h)
	return h
}

func gauge(name, help string) prometheus.Gauge {
	g := prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "igs", Name: name, Help: help})
	Registry.MustRegister(g)
	return g
}

func gaugeVec(name, help string, labels ...string) *prometheus.GaugeVec {
	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "igs", Name: name, Help: help}, labels)
	Registry.MustRegister(g)
	return g
}

var (
	// Requests to the scraped platform.
	Requests       = counterVec("ig_requests_total", "Platform requests by query and response class.", "query", "class", "tier")
	RequestSeconds = histVec("ig_request_seconds", "Platform request latency.", prometheus.ExponentialBuckets(0.05, 2, 10), "query", "tier")
	ResponseBytes  = histVec("ig_response_bytes", "Decoded platform response size.", prometheus.ExponentialBuckets(1024, 2, 12), "query")
	ProxyBytes     = counterVec("proxy_bytes_total", "Wire bytes through each proxy tier.", "tier", "dir")

	// Parsing.
	ParseTotal        = counterVec("parse_total", "Parser runs by outcome.", "query", "parser_version", "outcome")
	ParseMissingField = counterVec("parse_missing_field_total", "Optional or required fields missing from responses.", "query", "field")

	// Identities.
	IdentityActive  = gaugeVec("identity_active", "Live identities in this pod.", "tier")
	IdentityRetired = counterVec("identity_retired_total", "Retired identities by reason.", "tier", "reason")
	// Identities and IdentityShortfall show the identity pool: what a pod
	// holds, and the tasks that got no identity.
	Identities        = gaugeVec("identities", "Identities in this pod by state: idle, leased (in use by a task), waiting (tasks waiting at the pod's cap).", "tier", "state")
	IdentityShortfall = counterVec("identity_shortfall_total", "Identity requests a pod could not meet, by reason (exits_cooling, no_other_exit, at_cap).", "tier", "reason")
	// ExitPageLoads counts page loads per exit: a static exit by name
	// (isp#3), a gateway's sessions by bucket.
	ExitPageLoads = counterVec("exit_page_loads_total", "Page loads by exit (static exits by name, gateway sessions by bucket).", "tier", "exit")
	// IdentityUnparked counts identities a pod resumed from another
	// process's park (session.Pool.Park) instead of bootstrapping anew.
	IdentityUnparked = counterVec("identity_unparked_total", "Identities resumed from the store after a restart.", "tier")
	// IdentityAdopted counts identities taken from the checkpoint of a
	// crawler that died without parking, and parked for the others.
	IdentityAdopted = counterVec("identity_adopted_total", "Identities taken from a dead crawler's checkpoint and parked for reuse.")
	IdentityHops    = counterVec("identity_hops_total", "Blocked identities a task replaced with one on another exit and carried on, by stage and block class.", "stage", "class")
	Exits           = gaugeVec("exits", "Proxy exits configured, by static tier.", "tier")
	ExitsResting    = gaugeVec("exits_resting", "Proxy exits resting after a rate limit, checkpoint or login page, by tier.", "tier")
	ExitRests       = counterVec("exit_rests_total", "Times an exit was rested, by tier, reason (rate_limited, checkpoint, page_wall, page_load_limit; probe_failed: a probe after a rest was blocked, so the exit rests again) and exit (tier#n).", "tier", "reason", "exit")
	Bootstraps      = counterVec("bootstrap_total", "Identity bootstraps.", "method", "outcome")

	// Tasks.
	Tasks       = counterVec("tasks_total", "Crawl tasks by stage, lane and outcome.", "stage", "lane", "outcome")
	TaskSeconds = histVec("task_seconds", "Crawl task duration.", prometheus.ExponentialBuckets(0.1, 2, 12), "stage")
	TaskYields  = counterVec("task_yields_total", "Tasks that yielded a continuation.", "stage")
	DLQ         = counterVec("dlq_total", "Tasks sent to the dead letter queue.", "stage", "reason")
	Retries     = counterVec("retries_total", "Tasks sent to a retry topic.", "stage", "tier")

	// Media.
	MediaDiscoveryToStored = histVec("media_discovery_to_stored_seconds", "Time from media discovery to object stored.", prometheus.ExponentialBuckets(1, 2, 14), "kind")
	MediaOutcome           = counterVec("media_download_total", "Media downloads by outcome.", "kind", "outcome")
	MediaDeleted           = counterVec("media_deleted_total", "Media objects deleted by reason.", "reason")

	// Persistence and jobs.
	PersistRows        = counterVec("persist_rows_total", "Rows written by the persister.", "entity", "op")
	PersistQuarantined = counterVec("persist_quarantined_total", "Records the persister could not write and moved to its DLQ.", "topic", "reason")
	PersistSuppressed  = counterVec("persist_suppressed_total", "Records the persister dropped or redacted for an erased subject.", "entity", "action")
	DeletionChecks     = counterVec("deletion_checks_total", "Posts read in full that the finalizer checked for deleted comments, by outcome (checked, skipped_partial: the read's count did not match the post's).", "outcome")
	CommentChanges     = counterVec("comment_changes_total", "Stored comments edited, marked deleted or seen again after a deletion, by kind (edited, deleted, restored).", "kind")
	PersistBatch       = histVec("persist_batch_seconds", "Persister batch flush duration.", prometheus.ExponentialBuckets(0.005, 2, 12), "entity")
	Jobs               = counterVec("jobs_total", "Jobs reaching a terminal state.", "status", "lane")
	ScheduleLag        = gaugeVec("schedule_lag_seconds", "Age of the oldest due profile in the schedule.", "tier")
	// ProfilesByTier counts the enabled profiles on the schedule per tier
	// (hot, warm, long_tail, quiet, custom).
	ProfilesByTier = gaugeVec("profiles_scheduled", "Enabled profiles on the schedule, by tier.", "tier")
	// Normalized counts ig.* records mapped to content.* by
	// igs-normalizer (outcome ok or undecodable).
	Normalized   = counterVec("normalized_total", "ig.* records mapped to the content contract, by topic and outcome.", "topic", "outcome")
	BreakerState = gaugeVec("breaker_state", "Circuit breaker state (0 closed, 1 half-open, 2 open).", "name")
	// RateLimitStoreErrors and RateLimitWaiting: the fleet-wide rate limit
	// fails closed, so a request waits while Valkey does not answer.
	RateLimitStoreErrors = counterVec("ratelimit_store_errors_total", "Fleet-wide rate limit checks Valkey did not answer (the request waited and tried again), by key kind.", "kind")
	RateLimitWaiting     = gauge("ratelimit_waiting_for_store", "Requests in this pod waiting for Valkey before the fleet-wide rate limit lets them go.")
	// ExitShareRequests and ExitShareWait: requests of a class capped at a
	// share of each exit's rate (ig.discovery.exit_share), and the time
	// they waited for it.
	ExitShareRequests = counterVec("exit_share_requests_total", "Requests of a class capped at a share of each proxy exit's rate (discovery: ig.discovery.exit_share), by class.", "class")
	ExitShareWait     = counterVec("exit_share_wait_seconds_total", "Seconds requests of a capped class waited for their share of the exit's rate, by class.", "class")
	// Raw responses (internal/crawl/rawsink): lines kept, by kind (raw,
	// failure, sample); responses not kept for want of room (backlog) and
	// lines whose object was never written (lost); failed uploads, tried
	// again on the next flush; bytes waiting for upload.
	RawKept              = counterVec("raw_kept_total", "Raw responses kept, by kind (raw: expires with its prefix; failure and sample: kept for good).", "kind")
	RawNotKept           = counterVec("raw_not_kept_total", "Raw responses not kept: backlog (uploads failing, no reference handed out) or lost (a stopping pod could not write an object; its references dangle).", "reason")
	RawUploadFailures    = counter("raw_upload_failures_total", "Raw object uploads that failed; each is tried again on the next flush.")
	RawBacklog           = gauge("raw_backlog_bytes", "Raw response bytes in this pod waiting for upload.")
	QueryShortCircuit    = counterVec("ig_query_short_circuit_total", "Requests not sent because the query's breaker is open.", "query")
	SchedulerLeader      = gauge("scheduler_leader", "1 on the scheduler pod that holds the leader lock.")
	AIMDCeiling          = gauge("aimd_ceiling_rps", "Fleet-wide platform request ceiling set by AIMD.")
	AIMDAdjustments      = counterVec("aimd_adjustments_total", "AIMD ceiling changes by direction.", "direction")
	FleetRPS             = gauge("fleet_rps", "GraphQL responses per second across the fleet (last AIMD window).")
	BudgetUsed           = gaugeVec("budget_used", "Platform traffic used today (UTC), by kind (requests, bytes) and lane.", "kind", "lane")
	BudgetProjected      = gaugeVec("budget_projected", "Platform traffic projected by the end of the day, by kind.", "kind")
	BudgetLimit          = gaugeVec("budget_limit", "Daily traffic budget by kind (0 = none).", "kind")
	BudgetBackfillPaused = gauge("budget_backfill_paused", "1 while the budget holds backfill (p2) back.")
	BudgetStretch        = gauge("budget_stretch", "Interval factor the budget applies to the slow tiers (1 = none).")
	FleetBlockRate       = gauge("fleet_block_rate", "Share of fleet GraphQL responses that were blocks (last AIMD window).")
	LaneSlots            = gaugeVec("lane_slots", "Shared crawl slots per stage and lane: used, or tasks waiting for one.", "stage", "lane", "state")
	APIRequests          = counterVec("api_requests_total", "API requests.", "route", "code")
	KafkaProduce         = counterVec("kafka_produce_total", "Produced records.", "topic", "outcome")
	KafkaCommitted       = counterVec("kafka_commits_total", "Offset commits.", "group", "outcome")
	KafkaHandlerPanics   = counterVec("kafka_handler_panics_total", "Record handlers that panicked (the record is retried; its partition waits).", "group")
	ConsumerLag          = gaugeVec("kafka_consumer_lag", "Records behind the end of each topic, per consumer group (leader scheduler).", "group", "topic")

	// Profile discovery (M11).
	DiscoverySightings = counterVec("discovery_sightings_total", "Accounts met in the posts read, by source (tags, coauthors, mentions, commenters).", "source")
	DiscoveryVets      = counterVec("discovery_vets_total", "Discovery candidates vetted, by outcome (registered, below, private, gone, failed, already_registered).", "outcome")
	// DiscoveryAudience counts the commenter test's results on decided
	// candidates (D-055, ig.discovery.audience): in_scope (the Iraqi share
	// reached), below_share, too_few (fewer commenters than
	// min_commenters) and none (no commenter read).
	DiscoveryAudience = counterVec("discovery_audience_checks_total", "Discovery's Iraqi-commenter test on decided candidates, by result (in_scope, below_share, too_few, none).", "result")
	DiscoveryQueue     = gaugeVec("discovery_candidates", "Discovery candidates by status (new counts those at ig.discovery.min_priority or above).", "status")
	DiscoveryHeld      = gaugeVec("discovery_held", "1 while discovery starts no vets, by reason (schedule_behind, exits_resting, rests_unknown).", "reason")

	// Registry sync (igs-registry-sync).
	RegistrySyncRuns        = counterVec("registry_sync_runs_total", "Registry sync runs by status (ok, degraded, error).", "status")
	RegistrySyncLastSuccess = gauge("registry_sync_last_success_timestamp_seconds", "When the last registry sync run that gave every query a verdict completed.")
	RegistryChecks          = counterVec("registry_checks_total", "Registry entry checks on canary profiles by entry status and verdict.", "query", "status", "verdict")
	RegistryPromotions      = counterVec("registry_promotions_total", "doc_ids promoted to active.", "query")
	RegistryQueryHealthy    = gaugeVec("registry_query_healthy", "1 when the query's active doc_id parsed on a canary profile (or a replacement was promoted), 0 when it failed with none to promote.", "query")
	RegistryDiscovered      = gauge("registry_discovered_operations", "Relay operations found in the canary profiles' bundles in the last run.")
)
