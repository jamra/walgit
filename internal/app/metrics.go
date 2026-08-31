package app

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type serverMetrics struct {
	activeRequests atomic.Int64
	lastActivity   atomic.Int64

	mu                  sync.Mutex
	requests            map[string]uint64
	authFailures        uint64
	reconcileCount      uint64
	reconcileFailures   uint64
	reconcileSeconds    float64
	backendCount        uint64
	backendFailures     uint64
	backendSeconds      float64
	maintenanceRuns     uint64
	maintenanceFailures uint64
	compactions         uint64
	garbageCollections  uint64
	evictions           uint64
}

func newServerMetrics() *serverMetrics {
	m := &serverMetrics{requests: make(map[string]uint64)}
	m.lastActivity.Store(time.Now().UnixNano())
	return m
}

func (m *serverMetrics) recordRequest(service string, write bool, status int) {
	operation := "read"
	if write {
		operation = "write"
	}
	key := service + "\x00" + operation + "\x00" + strconv.Itoa(status)
	m.mu.Lock()
	m.requests[key]++
	m.mu.Unlock()
}

func (m *serverMetrics) recordAuthFailure() {
	m.mu.Lock()
	m.authFailures++
	m.mu.Unlock()
}

func (m *serverMetrics) recordReconcile(elapsed time.Duration, err error) {
	m.mu.Lock()
	m.reconcileCount++
	m.reconcileSeconds += elapsed.Seconds()
	if err != nil {
		m.reconcileFailures++
	}
	m.mu.Unlock()
}

func (m *serverMetrics) recordBackend(elapsed time.Duration, err error) {
	m.mu.Lock()
	m.backendCount++
	m.backendSeconds += elapsed.Seconds()
	if err != nil {
		m.backendFailures++
	}
	m.mu.Unlock()
}

func (m *serverMetrics) render(w io.Writer, repository string, includeMetadata bool) {
	m.mu.Lock()
	requests := make(map[string]uint64, len(m.requests))
	for key, value := range m.requests {
		requests[key] = value
	}
	authFailures := m.authFailures
	reconcileCount, reconcileFailures, reconcileSeconds := m.reconcileCount, m.reconcileFailures, m.reconcileSeconds
	backendCount, backendFailures, backendSeconds := m.backendCount, m.backendFailures, m.backendSeconds
	maintenanceRuns, maintenanceFailures := m.maintenanceRuns, m.maintenanceFailures
	compactions, garbageCollections, evictions := m.compactions, m.garbageCollections, m.evictions
	m.mu.Unlock()

	if includeMetadata {
		writeMetricMetadata(w)
	}
	keys := make([]string, 0, len(requests))
	for key := range requests {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		parts := splitMetricKey(key)
		fmt.Fprintf(w, "walgit_http_requests_total{repository=%q,service=%q,operation=%q,status=%q} %d\n", repository, parts[0], parts[1], parts[2], requests[key])
	}
	fmt.Fprintf(w, "walgit_http_active_requests{repository=%q} %d\n", repository, m.activeRequests.Load())
	fmt.Fprintf(w, "walgit_http_auth_failures_total{repository=%q} %d\n", repository, authFailures)
	fmt.Fprintf(w, "walgit_reconcile_total{repository=%q} %d\n", repository, reconcileCount)
	fmt.Fprintf(w, "walgit_reconcile_failures_total{repository=%q} %d\n", repository, reconcileFailures)
	fmt.Fprintf(w, "walgit_reconcile_seconds_total{repository=%q} %.9f\n", repository, reconcileSeconds)
	fmt.Fprintf(w, "walgit_git_backend_total{repository=%q} %d\n", repository, backendCount)
	fmt.Fprintf(w, "walgit_git_backend_failures_total{repository=%q} %d\n", repository, backendFailures)
	fmt.Fprintf(w, "walgit_git_backend_seconds_total{repository=%q} %.9f\n", repository, backendSeconds)
	fmt.Fprintf(w, "walgit_maintenance_runs_total{repository=%q} %d\n", repository, maintenanceRuns)
	fmt.Fprintf(w, "walgit_maintenance_failures_total{repository=%q} %d\n", repository, maintenanceFailures)
	fmt.Fprintf(w, "walgit_compactions_total{repository=%q} %d\n", repository, compactions)
	fmt.Fprintf(w, "walgit_garbage_collections_total{repository=%q} %d\n", repository, garbageCollections)
	fmt.Fprintf(w, "walgit_cache_evictions_total{repository=%q} %d\n", repository, evictions)
	fmt.Fprintf(w, "walgit_last_activity_timestamp_seconds{repository=%q} %.3f\n", repository, float64(m.lastActivity.Load())/float64(time.Second))
}

func writeMetricMetadata(w io.Writer) {
	for _, metric := range []struct {
		name, kind, help string
	}{
		{"walgit_http_requests_total", "counter", "Git smart HTTP requests."},
		{"walgit_http_active_requests", "gauge", "Active Git smart HTTP requests."},
		{"walgit_http_auth_failures_total", "counter", "Rejected authentication attempts."},
		{"walgit_reconcile_total", "counter", "Repository reconciliation attempts."},
		{"walgit_reconcile_failures_total", "counter", "Failed repository reconciliations."},
		{"walgit_reconcile_seconds_total", "counter", "Time spent reconciling repository caches."},
		{"walgit_git_backend_total", "counter", "Git backend invocations."},
		{"walgit_git_backend_failures_total", "counter", "Failed Git backend invocations."},
		{"walgit_git_backend_seconds_total", "counter", "Time spent in Git backend processes."},
		{"walgit_maintenance_runs_total", "counter", "Repository maintenance attempts."},
		{"walgit_maintenance_failures_total", "counter", "Failed repository maintenance attempts."},
		{"walgit_compactions_total", "counter", "Completed repository checkpoints."},
		{"walgit_garbage_collections_total", "counter", "Completed garbage-collection passes."},
		{"walgit_cache_evictions_total", "counter", "Completed local cache evictions."},
		{"walgit_last_activity_timestamp_seconds", "gauge", "Unix timestamp of the last authorized Git request."},
	} {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", metric.name, metric.help, metric.name, metric.kind)
	}
}

func splitMetricKey(key string) [3]string {
	var result [3]string
	part := 0
	start := 0
	for i := 0; i <= len(key) && part < len(result); i++ {
		if i == len(key) || key[i] == 0 {
			result[part] = key[start:i]
			part++
			start = i + 1
		}
	}
	return result
}
