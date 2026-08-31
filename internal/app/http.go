package app

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/textproto"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type HTTPOptions struct {
	Repository          string
	Store               string
	RepositoryID        string
	Token               string
	AuthorizationFile   string
	AnonymousRead       bool
	MaximumRequestSize  int64
	MaintenanceInterval time.Duration
	CompactAfterEntries int
	CompactAfterBytes   int64
	GCGrace             time.Duration
	IdleCacheAfter      time.Duration
}

type gitHTTPHandler struct {
	options    HTTPOptions
	authorizer *authorizer
	metrics    *serverMetrics
	mu         sync.RWMutex
}

func NewGitHTTPHandler(options HTTPOptions) (http.Handler, error) {
	return newGitHTTPHandler(options)
}

func newGitHTTPHandler(options HTTPOptions) (*gitHTTPHandler, error) {
	if options.Repository == "" || options.Store == "" || options.RepositoryID == "" {
		return nil, errors.New("HTTP server requires repository, store, and repository ID")
	}
	repo, err := filepath.Abs(options.Repository)
	if err != nil {
		return nil, err
	}
	options.Repository = repo
	if options.MaximumRequestSize == 0 {
		options.MaximumRequestSize = 1 << 30
	}
	if options.MaximumRequestSize < 0 {
		return nil, errors.New("maximum request size cannot be negative")
	}
	if options.GCGrace < 0 {
		return nil, errors.New("garbage-collection grace period cannot be negative")
	}
	authorizer, err := loadAuthorizer(options.RepositoryID, options.Token, options.AuthorizationFile)
	if err != nil {
		return nil, err
	}
	if err := recoverCacheEviction(options.Repository, options.Store, options.RepositoryID); err != nil {
		return nil, fmt.Errorf("recover repository cache: %w", err)
	}
	return &gitHTTPHandler{options: options, authorizer: authorizer, metrics: newServerMetrics()}, nil
}

func ServeGitHTTP(listen string, options HTTPOptions) error {
	handler, err := newGitHTTPHandler(options)
	if err != nil {
		return err
	}
	server := &http.Server{
		Addr: listen, Handler: handler,
		ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 2 * time.Minute,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	maintenanceDone := make(chan struct{})
	if options.MaintenanceInterval > 0 {
		go func() {
			defer close(maintenanceDone)
			ticker := time.NewTicker(options.MaintenanceInterval)
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
	} else {
		close(maintenanceDone)
	}
	serverError := make(chan error, 1)
	go func() { serverError <- server.ListenAndServe() }()
	select {
	case err := <-serverError:
		stop()
		select {
		case <-maintenanceDone:
		case <-time.After(30 * time.Second):
			return errors.New("timed out waiting for maintenance shutdown")
		}
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		err := server.Shutdown(shutdownCtx)
		serverErr := <-serverError
		select {
		case <-maintenanceDone:
		case <-shutdownCtx.Done():
			return errors.New("timed out waiting for maintenance shutdown")
		}
		if err != nil {
			return err
		}
		if !errors.Is(serverErr, http.ErrServerClosed) {
			return serverErr
		}
		return nil
	}
}

func (h *gitHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.serveOperationalEndpoint(w, r) {
		return
	}
	service, write, ok := h.route(r)
	if !ok {
		http.NotFound(w, r)
		return
	}
	tracked := &trackingResponseWriter{ResponseWriter: w, status: http.StatusOK}
	h.metrics.activeRequests.Add(1)
	defer h.metrics.activeRequests.Add(-1)
	defer func() { h.metrics.recordRequest(service, write, tracked.status) }()
	authenticated := h.authenticated(r, write)
	if !authenticated && (write || !h.options.AnonymousRead) {
		h.metrics.recordAuthFailure()
		tracked.Header().Set("WWW-Authenticate", `Basic realm="walgit"`)
		http.Error(tracked, "authentication required", http.StatusUnauthorized)
		return
	}
	h.metrics.lastActivity.Store(time.Now().UnixNano())
	if r.ContentLength > h.options.MaximumRequestSize {
		http.Error(tracked, "request is too large", http.StatusRequestEntityTooLarge)
		return
	}
	if r.Body != nil {
		r.Body = http.MaxBytesReader(tracked, r.Body, h.options.MaximumRequestSize)
	}
	if write {
		h.mu.Lock()
		defer h.mu.Unlock()
	} else {
		h.mu.RLock()
		defer h.mu.RUnlock()
	}
	reconcileStart := time.Now()
	reconcileErr := Reconcile(h.options.Repository, h.options.Store, h.options.RepositoryID)
	h.metrics.recordReconcile(time.Since(reconcileStart), reconcileErr)
	if reconcileErr != nil {
		http.Error(tracked, "repository reconciliation failed", http.StatusServiceUnavailable)
		return
	}
	backendStart := time.Now()
	backendErr := h.runBackend(tracked, r, service, authenticated)
	h.metrics.recordBackend(time.Since(backendStart), backendErr)
	if backendErr != nil {
		if !tracked.wroteHeader {
			http.Error(tracked, "Git backend failed", http.StatusInternalServerError)
		}
	}
}

func (h *gitHTTPHandler) serveOperationalEndpoint(w http.ResponseWriter, r *http.Request) bool {
	switch r.URL.Path {
	case "/_walgit/health/live":
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return true
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		return true
	case "/_walgit/health/ready":
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return true
		}
		err := h.readinessError()
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "unavailable"})
		} else {
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
		}
		return true
	case "/metrics":
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return true
		}
		if !h.authenticated(r, false) {
			h.metrics.recordAuthFailure()
			w.Header().Set("WWW-Authenticate", `Basic realm="walgit"`)
			http.Error(w, "authentication required", http.StatusUnauthorized)
			return true
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		h.metrics.render(w, h.options.RepositoryID, true)
		return true
	default:
		return false
	}
}

func (h *gitHTTPHandler) route(r *http.Request) (service string, write, ok bool) {
	base := "/" + h.options.RepositoryID + ".git"
	if !strings.HasPrefix(r.URL.Path, base+"/") {
		return "", false, false
	}
	suffix := strings.TrimPrefix(r.URL.Path, base)
	switch {
	case r.Method == http.MethodGet && suffix == "/info/refs":
		service = r.URL.Query().Get("service")
		if service == "git-upload-pack" {
			return service, false, true
		}
		if service == "git-receive-pack" {
			return service, true, true
		}
	case r.Method == http.MethodPost && suffix == "/git-upload-pack":
		return "git-upload-pack", false, true
	case r.Method == http.MethodPost && suffix == "/git-receive-pack":
		return "git-receive-pack", true, true
	}
	return "", false, false
}

func (h *gitHTTPHandler) authenticated(r *http.Request, write bool) bool {
	header := r.Header.Get("Authorization")
	_, password, hasBasic := r.BasicAuth()
	return h.authorizer.authorize(requestToken(header, password, hasBasic), write)
}

func (h *gitHTTPHandler) runBackend(w http.ResponseWriter, r *http.Request, service string, authenticated bool) error {
	repoParent := filepath.Dir(h.options.Repository)
	repoName := filepath.Base(h.options.Repository)
	pathInfo := "/" + repoName + strings.TrimPrefix(r.URL.Path, "/"+h.options.RepositoryID+".git")
	env := append(os.Environ(),
		"GIT_PROJECT_ROOT="+repoParent,
		"GIT_HTTP_EXPORT_ALL=1",
		"PATH_INFO="+pathInfo,
		"REQUEST_METHOD="+r.Method,
		"QUERY_STRING="+r.URL.RawQuery,
		"CONTENT_TYPE="+r.Header.Get("Content-Type"),
		"CONTENT_LENGTH="+strconv.FormatInt(r.ContentLength, 10),
		"REMOTE_ADDR="+remoteAddress(r.RemoteAddr),
	)
	if authenticated {
		env = append(env, "REMOTE_USER=walgit")
	}
	if protocol := r.Header.Get("Git-Protocol"); protocol != "" {
		env = append(env, "HTTP_GIT_PROTOCOL="+protocol)
	}
	cmd := exec.Command("git", "http-backend")
	cmd.Env = env
	cmd.Stdin = r.Body
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	reader := bufio.NewReader(stdout)
	headers, err := textproto.NewReader(reader).ReadMIMEHeader()
	if err != nil {
		_ = cmd.Wait()
		return fmt.Errorf("read CGI headers: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	status := http.StatusOK
	if value := headers.Get("Status"); value != "" {
		parts := strings.Fields(value)
		if len(parts) > 0 {
			if parsed, parseErr := strconv.Atoi(parts[0]); parseErr == nil {
				status = parsed
			}
		}
		headers.Del("Status")
	}
	for key, values := range headers {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(status)
	if _, err := io.Copy(w, reader); err != nil {
		_ = cmd.Wait()
		return err
	}
	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("git http-backend: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

func remoteAddress(value string) string {
	host, _, err := net.SplitHostPort(value)
	if err == nil {
		return host
	}
	return value
}

type trackingResponseWriter struct {
	http.ResponseWriter
	wroteHeader bool
	status      int
}

func (w *trackingResponseWriter) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *trackingResponseWriter) Write(data []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(data)
}
