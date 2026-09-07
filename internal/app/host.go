package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"walgit/internal/wal"
)

type HostConfig struct {
	Version           int                    `json:"version"`
	AuthorizationFile string                 `json:"authorization_file,omitempty"`
	Repositories      []HostRepositoryConfig `json:"repositories"`
}

type HostRepositoryConfig struct {
	ID                  string `json:"id"`
	Repository          string `json:"repository"`
	Store               string `json:"store"`
	AnonymousRead       bool   `json:"anonymous_read,omitempty"`
	MaximumRequestSize  *int64 `json:"maximum_request_bytes,omitempty"`
	MaintenanceInterval string `json:"maintenance_interval,omitempty"`
	ScrubInterval       string `json:"scrub_interval,omitempty"`
	CompactAfterEntries *int   `json:"compact_after_entries,omitempty"`
	CompactAfterBytes   *int64 `json:"compact_after_bytes,omitempty"`
	GCGrace             string `json:"gc_grace,omitempty"`
	IdleCacheAfter      string `json:"idle_cache_after,omitempty"`
}

type gitHTTPHost struct {
	repositories map[string]*gitHTTPHandler
	ids          []string
}

func LoadHostConfig(path, authorizationOverride, sharedToken string) (*gitHTTPHost, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open host config: %w", err)
	}
	defer f.Close()
	decoder := json.NewDecoder(f)
	decoder.DisallowUnknownFields()
	var config HostConfig
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("decode host config: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("host config contains trailing JSON values")
		}
		return nil, fmt.Errorf("decode host config trailing data: %w", err)
	}
	if config.Version != 1 {
		return nil, fmt.Errorf("unsupported host config version %d", config.Version)
	}
	if len(config.Repositories) == 0 {
		return nil, errors.New("host config requires at least one repository")
	}
	configDirectory, err := filepath.Abs(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	authFile := authorizationOverride
	if authFile == "" {
		authFile = config.AuthorizationFile
		if authFile != "" && !filepath.IsAbs(authFile) {
			authFile = filepath.Join(configDirectory, authFile)
		}
	}
	host := &gitHTTPHost{repositories: make(map[string]*gitHTTPHandler)}
	for index, repository := range config.Repositories {
		if repository.ID == "" || strings.ContainsAny(repository.ID, "/\\") || repository.ID == "." || repository.ID == ".." {
			return nil, fmt.Errorf("repository %d has invalid ID %q", index, repository.ID)
		}
		if _, exists := host.repositories[repository.ID]; exists {
			return nil, fmt.Errorf("duplicate repository ID %q", repository.ID)
		}
		repoPath := repository.Repository
		if repoPath != "" && !filepath.IsAbs(repoPath) {
			repoPath = filepath.Join(configDirectory, repoPath)
		}
		store := repository.Store
		if store != "" && !strings.Contains(store, "://") && !filepath.IsAbs(store) {
			store = filepath.Join(configDirectory, store)
		}
		options, err := repository.httpOptions(repoPath, store, authFile, sharedToken)
		if err != nil {
			return nil, fmt.Errorf("repository %q: %w", repository.ID, err)
		}
		handler, err := newGitHTTPHandler(options)
		if err != nil {
			return nil, fmt.Errorf("repository %q: %w", repository.ID, err)
		}
		host.repositories[repository.ID] = handler
		host.ids = append(host.ids, repository.ID)
	}
	sort.Strings(host.ids)
	return host, nil
}

func (c HostRepositoryConfig) httpOptions(repo, store, authFile, token string) (HTTPOptions, error) {
	maintenanceInterval, err := parseConfigDuration(c.MaintenanceInterval, 5*time.Minute)
	if err != nil {
		return HTTPOptions{}, fmt.Errorf("maintenance interval: %w", err)
	}
	scrubInterval, err := parseConfigDuration(c.ScrubInterval, 0)
	if err != nil {
		return HTTPOptions{}, fmt.Errorf("scrub interval: %w", err)
	}
	gcGrace, err := parseConfigDuration(c.GCGrace, 24*time.Hour)
	if err != nil {
		return HTTPOptions{}, fmt.Errorf("GC grace: %w", err)
	}
	idleCacheAfter, err := parseConfigDuration(c.IdleCacheAfter, 0)
	if err != nil {
		return HTTPOptions{}, fmt.Errorf("idle cache duration: %w", err)
	}
	maxRequest := int64(1 << 30)
	if c.MaximumRequestSize != nil {
		maxRequest = *c.MaximumRequestSize
	}
	compactEntries := 100
	if c.CompactAfterEntries != nil {
		compactEntries = *c.CompactAfterEntries
	}
	compactBytes := int64(1 << 30)
	if c.CompactAfterBytes != nil {
		compactBytes = *c.CompactAfterBytes
	}
	return HTTPOptions{
		Repository: repo, Store: store, RepositoryID: c.ID,
		Token: token, AuthorizationFile: authFile, AnonymousRead: c.AnonymousRead,
		MaximumRequestSize: maxRequest, MaintenanceInterval: maintenanceInterval,
		ScrubInterval:       scrubInterval,
		CompactAfterEntries: compactEntries, CompactAfterBytes: compactBytes,
		GCGrace: gcGrace, IdleCacheAfter: idleCacheAfter,
	}, nil
}

func parseConfigDuration(value string, fallback time.Duration) (time.Duration, error) {
	if value == "" {
		return fallback, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, err
	}
	if duration < 0 {
		return 0, errors.New("duration cannot be negative")
	}
	return duration, nil
}

func (h *gitHTTPHost) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.serveOperationalEndpoint(w, r) {
		return
	}
	id, ok := repositoryIDFromPath(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	handler, ok := h.repositories[id]
	if !ok {
		http.NotFound(w, r)
		return
	}
	handler.ServeHTTP(w, r)
}

func repositoryIDFromPath(path string) (string, bool) {
	if !strings.HasPrefix(path, "/") {
		return "", false
	}
	end := strings.Index(path, ".git/")
	if end < 1 || end+5 > len(path) || strings.Contains(path[1:end], "/") {
		return "", false
	}
	return path[1:end], true
}

func (h *gitHTTPHost) serveOperationalEndpoint(w http.ResponseWriter, r *http.Request) bool {
	switch r.URL.Path {
	case "/_walgit/health/live":
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return true
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "repositories": len(h.ids)})
		return true
	case "/_walgit/health/ready":
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return true
		}
		unavailable := 0
		for _, id := range h.ids {
			if h.repositories[id].readinessError() != nil {
				unavailable++
			}
		}
		status := http.StatusOK
		state := "ready"
		if unavailable > 0 {
			status = http.StatusServiceUnavailable
			state = "unavailable"
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": state, "repositories": len(h.ids), "unavailable": unavailable})
		return true
	case "/metrics":
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return true
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		included := false
		for _, id := range h.ids {
			handler := h.repositories[id]
			if !handler.authenticated(r, false) {
				continue
			}
			handler.metrics.render(w, id, !included)
			included = true
		}
		if !included {
			w.Header().Set("WWW-Authenticate", `Basic realm="walgit"`)
			http.Error(w, "authentication required", http.StatusUnauthorized)
		}
		return true
	default:
		return false
	}
}

func (h *gitHTTPHandler) readinessError() error {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if err := h.metrics.scrubHealthError(); err != nil {
		return err
	}
	backend, err := wal.Open(h.options.Store)
	if err == nil {
		_, err = backend.Load(h.options.RepositoryID)
	}
	if err == nil {
		_, err = os.Stat(filepath.Join(h.options.Repository, "HEAD"))
	}
	return err
}

func ServeGitHTTPHost(listen, configPath, authorizationOverride, sharedToken string) error {
	host, err := LoadHostConfig(configPath, authorizationOverride, sharedToken)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var maintenance sync.WaitGroup
	for _, id := range host.ids {
		handler := host.repositories[id]
		if handler.options.ScrubInterval > 0 {
			_, _ = handler.runScrub()
			maintenance.Add(1)
			go handler.runScrubLoop(ctx, &maintenance)
		}
		if handler.options.MaintenanceInterval > 0 {
			maintenance.Add(1)
			go func() {
				defer maintenance.Done()
				ticker := time.NewTicker(handler.options.MaintenanceInterval)
				defer ticker.Stop()
				for {
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
						_, _ = handler.runMaintenance()
					}
				}
			}()
		}
	}
	server := &http.Server{Addr: listen, Handler: host, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 2 * time.Minute}
	serverError := make(chan error, 1)
	go func() { serverError <- server.ListenAndServe() }()
	select {
	case err := <-serverError:
		stop()
		if !waitGroupWithin(&maintenance, 30*time.Second) {
			return errors.New("timed out waiting for maintenance shutdown")
		}
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		shutdownErr := server.Shutdown(shutdownContext)
		serverErr := <-serverError
		if !waitGroupWithin(&maintenance, 30*time.Second) {
			return errors.New("timed out waiting for maintenance shutdown")
		}
		if shutdownErr != nil {
			return shutdownErr
		}
		if !errors.Is(serverErr, http.ErrServerClosed) {
			return serverErr
		}
		return nil
	}
}

func waitGroupWithin(group *sync.WaitGroup, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		group.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}
