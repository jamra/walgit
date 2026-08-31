package app

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"walgit/internal/wal"
)

func TestRepositoryScopedHashedAuthorizationAndOperationalEndpoints(t *testing.T) {
	root := t.TempDir()
	repo := filepath.Join(root, "repo.git")
	store := filepath.Join(root, "store")
	if err := Init(repo, store, "repo"); err != nil {
		t.Fatal(err)
	}
	policyPath := filepath.Join(root, "auth.json")
	policy := AuthorizationPolicy{
		Version: 1,
		Repositories: map[string][]TokenGrant{
			"repo": {
				{Name: "reader", SHA256: HashToken("read-secret"), Read: true},
				{Name: "writer", SHA256: HashToken("write-secret"), Read: true, Write: true},
			},
			"different-repo": {{Name: "other", SHA256: HashToken("other"), Read: true, Write: true}},
		},
	}
	data, err := json.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(policyPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	handler, err := NewGitHTTPHandler(HTTPOptions{
		Repository: repo, Store: store, RepositoryID: "repo", AuthorizationFile: policyPath,
	})
	if err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodGet, "http://walgit.test/repo.git/info/refs?service=git-receive-pack", nil)
	request.Header.Set("Authorization", "Bearer read-secret")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("read token receive-pack status %d, want 401", response.Code)
	}

	request = httptest.NewRequest(http.MethodGet, "http://walgit.test/repo.git/info/refs?service=git-upload-pack", nil)
	request.Header.Set("Authorization", "Bearer read-secret")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("read token upload-pack status %d, want 200: %s", response.Code, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodGet, "http://walgit.test/repo.git/info/refs?service=git-receive-pack", nil)
	request.SetBasicAuth("git", "write-secret")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("write token receive-pack status %d, want 200: %s", response.Code, response.Body.String())
	}

	request = httptest.NewRequest(http.MethodGet, "http://walgit.test/_walgit/health/live", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"status":"ok"`) {
		t.Fatalf("unexpected liveness response %d: %s", response.Code, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodGet, "http://walgit.test/_walgit/health/ready", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"status":"ready"`) {
		t.Fatalf("unexpected readiness response %d: %s", response.Code, response.Body.String())
	}

	request = httptest.NewRequest(http.MethodGet, "http://walgit.test/metrics", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated metrics status %d, want 401", response.Code)
	}
	request = httptest.NewRequest(http.MethodGet, "http://walgit.test/metrics", nil)
	request.SetBasicAuth("metrics", "read-secret")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("authenticated metrics status %d, want 200", response.Code)
	}
	for _, metric := range []string{"walgit_http_requests_total", "walgit_http_auth_failures_total", "walgit_reconcile_seconds_total"} {
		if !strings.Contains(response.Body.String(), metric) {
			t.Fatalf("metrics response does not contain %s: %s", metric, response.Body.String())
		}
	}
}

func TestInterruptedCacheEvictionIsRecoveredAtStartup(t *testing.T) {
	root := t.TempDir()
	repo := filepath.Join(root, "repo.git")
	store := filepath.Join(root, "store")
	if err := Init(repo, store, "repo"); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(repo, "old-cache-marker")
	if err := os.WriteFile(marker, []byte("preserve me"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(repo, evictionBackupPath(repo)); err != nil {
		t.Fatal(err)
	}
	if _, err := newGitHTTPHandler(HTTPOptions{Repository: repo, Store: store, RepositoryID: "repo", Token: "secret"}); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(marker); err != nil || string(data) != "preserve me" {
		t.Fatalf("old cache was not restored: data=%q err=%v", data, err)
	}
	if _, err := os.Stat(evictionBackupPath(repo)); !os.IsNotExist(err) {
		t.Fatalf("eviction recovery path still exists: %v", err)
	}
}

func TestMultiRepositoryHostRoutesAndScopesMetrics(t *testing.T) {
	root := t.TempDir()
	store := filepath.Join(root, "store")
	for _, id := range []string{"alpha", "beta"} {
		if err := Init(filepath.Join(root, id+".git"), store, id); err != nil {
			t.Fatal(err)
		}
	}
	policy := AuthorizationPolicy{Version: 1, Repositories: map[string][]TokenGrant{
		"alpha": {{Name: "alpha-reader", SHA256: HashToken("alpha-secret"), Read: true, Write: true}},
		"beta":  {{Name: "beta-reader", SHA256: HashToken("beta-secret"), Read: true, Write: true}},
	}}
	policyData, err := json.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "auth.json"), policyData, 0o600); err != nil {
		t.Fatal(err)
	}
	zero := 0
	config := HostConfig{
		Version: 1, AuthorizationFile: "auth.json",
		Repositories: []HostRepositoryConfig{
			{ID: "alpha", Repository: "alpha.git", Store: "store", MaintenanceInterval: "0s", CompactAfterEntries: &zero},
			{ID: "beta", Repository: "beta.git", Store: "store", MaintenanceInterval: "0s", CompactAfterEntries: &zero},
		},
	}
	configData, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(root, "host.json")
	if err := os.WriteFile(configPath, configData, 0o600); err != nil {
		t.Fatal(err)
	}
	host, err := LoadHostConfig(configPath, "", "")
	if err != nil {
		t.Fatal(err)
	}
	for id, token := range map[string]string{"alpha": "alpha-secret", "beta": "beta-secret"} {
		request := httptest.NewRequest(http.MethodGet, "http://walgit.test/"+id+".git/info/refs?service=git-upload-pack", nil)
		request.Header.Set("Authorization", "Bearer "+token)
		response := httptest.NewRecorder()
		host.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("%s advertisement status %d: %s", id, response.Code, response.Body.String())
		}
	}
	request := httptest.NewRequest(http.MethodGet, "http://walgit.test/_walgit/health/ready", nil)
	response := httptest.NewRecorder()
	host.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"repositories":2`) {
		t.Fatalf("unexpected host readiness response %d: %s", response.Code, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodGet, "http://walgit.test/metrics", nil)
	request.Header.Set("Authorization", "Bearer alpha-secret")
	response = httptest.NewRecorder()
	host.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("host metrics status %d: %s", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), `repository="alpha"`) || strings.Contains(response.Body.String(), `repository="beta"`) {
		t.Fatalf("metrics were not scoped to alpha: %s", response.Body.String())
	}
}

func TestMaintenanceCompactsEvictsAndReconciles(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	root := t.TempDir()
	exe := filepath.Join(root, "walgit")
	cmd := exec.Command("go", "build", "-o", exe, "./cmd/walgit")
	cmd.Dir = filepath.Join("..", "..")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build hook executable: %v: %s", err, out)
	}
	t.Setenv("WALGIT_EXECUTABLE", exe)
	repo := filepath.Join(root, "repo.git")
	storeLocation := filepath.Join(root, "store")
	work := filepath.Join(root, "work")
	if err := Init(repo, storeLocation, "repo"); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"init", "-b", "main", work},
		{"-C", work, "config", "user.name", "test"},
		{"-C", work, "config", "user.email", "test@example.invalid"},
	} {
		if err := run("", "git", args...); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(work, "file"), []byte("durable"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"-C", work, "add", "file"},
		{"-C", work, "commit", "-m", "initial"},
		{"-C", work, "push", repo, "HEAD:refs/heads/main"},
	} {
		if err := run("", "git", args...); err != nil {
			t.Fatal(err)
		}
	}
	expected, err := commandOutput("", "git", "-C", repo, "rev-parse", "refs/heads/main")
	if err != nil {
		t.Fatal(err)
	}
	handler, err := newGitHTTPHandler(HTTPOptions{
		Repository: repo, Store: storeLocation, RepositoryID: "repo", Token: "secret",
		CompactAfterEntries: 1, GCGrace: 0, IdleCacheAfter: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	handler.metrics.lastActivity.Store(time.Now().Add(-time.Hour).UnixNano())
	result, err := handler.runMaintenance()
	if err != nil {
		t.Fatal(err)
	}
	if !result.Compacted || !result.Evicted || result.Checkpoint.Generation != 1 {
		t.Fatalf("unexpected maintenance result: %#v", result)
	}
	backend, err := wal.Open(storeLocation)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := backend.Load("repo")
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Checkpoint == nil || manifest.Checkpoint.Generation != 1 || len(manifest.Entries) != 0 {
		t.Fatalf("unexpected compacted manifest: %#v", manifest)
	}
	if _, err := commandOutput("", "git", "-C", repo, "rev-parse", "--verify", "refs/heads/main"); err == nil {
		t.Fatal("evicted cache unexpectedly retained refs/heads/main")
	}
	if err := Reconcile(repo, storeLocation, "repo"); err != nil {
		t.Fatal(err)
	}
	actual, err := commandOutput("", "git", "-C", repo, "rev-parse", "refs/heads/main")
	if err != nil {
		t.Fatal(err)
	}
	if actual != expected {
		t.Fatalf("reconciled head %s, want %s", actual, expected)
	}
}

func TestGitHookCrashMatrixPreservesFailedPushSemantics(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	root := t.TempDir()
	exe := filepath.Join(root, "walgit")
	cmd := exec.Command("go", "build", "-o", exe, "./cmd/walgit")
	cmd.Dir = filepath.Join("..", "..")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build hook executable: %v: %s", err, out)
	}
	t.Setenv("WALGIT_EXECUTABLE", exe)
	cases := []struct {
		name                string
		failpoints          string
		generationAfterFail uint64
		preparedAfterFail   int
	}{
		{name: "stage after sync", failpoints: "stage.after_sync", generationAfterFail: 0},
		{name: "prepared entry rename", failpoints: "commit.after_entry_rename", generationAfterFail: 0},
		{name: "prepared manifest publish", failpoints: "commit.after_manifest", generationAfterFail: 2},
		{name: "rollback entry rename", failpoints: "commit.after_manifest,rollback.after_entry_rename", generationAfterFail: 1, preparedAfterFail: 1},
		{name: "rollback manifest publish", failpoints: "commit.after_manifest,rollback.after_manifest", generationAfterFail: 2},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			caseRoot := t.TempDir()
			remote := filepath.Join(caseRoot, "remote.git")
			storeLocation := filepath.Join(caseRoot, "store")
			work := filepath.Join(caseRoot, "work")
			if err := Init(remote, storeLocation, "repo"); err != nil {
				t.Fatal(err)
			}
			for _, args := range [][]string{
				{"init", "-b", "main", work},
				{"-C", work, "config", "user.name", "test"},
				{"-C", work, "config", "user.email", "test@example.invalid"},
			} {
				if err := run("", "git", args...); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filepath.Join(work, "file"), []byte(testCase.name), 0o644); err != nil {
				t.Fatal(err)
			}
			for _, args := range [][]string{{"-C", work, "add", "file"}, {"-C", work, "commit", "-m", "initial"}} {
				if err := run("", "git", args...); err != nil {
					t.Fatal(err)
				}
			}
			newOID, err := commandOutput("", "git", "-C", work, "rev-parse", "HEAD")
			if err != nil {
				t.Fatal(err)
			}
			t.Setenv("WALGIT_FAILPOINT", testCase.failpoints)
			if err := run("", "git", "-C", work, "push", remote, "HEAD:refs/heads/main"); err == nil {
				t.Fatal("push unexpectedly succeeded through injected failure")
			}
			t.Setenv("WALGIT_FAILPOINT", "")
			backend, err := wal.Open(storeLocation)
			if err != nil {
				t.Fatal(err)
			}
			manifest, err := backend.Load("repo")
			if err != nil {
				t.Fatal(err)
			}
			if manifest.Generation != testCase.generationAfterFail || len(manifest.Prepared) != testCase.preparedAfterFail {
				t.Fatalf("unexpected state after failure: generation=%d prepared=%d manifest=%#v", manifest.Generation, len(manifest.Prepared), manifest)
			}
			if _, err := commandOutput("", "git", "-C", remote, "rev-parse", "--verify", "refs/heads/main"); err == nil {
				t.Fatal("failed push changed the local ref")
			}
			updates := []wal.RefUpdate{{Old: strings.Repeat("0", len(newOID)), New: newOID, Ref: "refs/heads/main"}}
			if testCase.preparedAfterFail > 0 {
				if err := backend.Abort("repo", updates); err != nil {
					t.Fatalf("resume interrupted rollback: %v", err)
				}
				manifest, err = backend.Load("repo")
				if err != nil {
					t.Fatal(err)
				}
			}
			if len(manifest.Prepared) != 0 || len(manifest.Refs) != 0 {
				t.Fatalf("failed push remained authoritative: %#v", manifest)
			}
			if err := run("", "git", "-C", work, "push", remote, "HEAD:refs/heads/main"); err != nil {
				t.Fatalf("retry push failed: %v", err)
			}
			manifest, err = backend.Load("repo")
			if err != nil {
				t.Fatal(err)
			}
			if manifest.Refs["refs/heads/main"] != newOID || len(manifest.Prepared) != 0 {
				t.Fatalf("retry was not finalized: %#v", manifest)
			}
		})
	}
}

func TestCommittedHookFailureLeavesDurablePreparedTransaction(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	root := t.TempDir()
	exe := filepath.Join(root, "walgit")
	cmd := exec.Command("go", "build", "-o", exe, "./cmd/walgit")
	cmd.Dir = filepath.Join("..", "..")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build hook executable: %v: %s", err, out)
	}
	t.Setenv("WALGIT_EXECUTABLE", exe)
	remote := filepath.Join(root, "remote.git")
	storeLocation := filepath.Join(root, "store")
	work := filepath.Join(root, "work")
	if err := Init(remote, storeLocation, "repo"); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"init", "-b", "main", work},
		{"-C", work, "config", "user.name", "test"},
		{"-C", work, "config", "user.email", "test@example.invalid"},
	} {
		if err := run("", "git", args...); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(work, "file"), []byte("committed"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"-C", work, "add", "file"}, {"-C", work, "commit", "-m", "initial"}} {
		if err := run("", "git", args...); err != nil {
			t.Fatal(err)
		}
	}
	expected, err := commandOutput("", "git", "-C", work, "rev-parse", "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("WALGIT_FAILPOINT", "finalize.before_manifest")
	// Git has already committed its refs when the committed hook runs, so its
	// exit status is advisory. The durable prepared record must survive either
	// client-visible outcome.
	_ = run("", "git", "-C", work, "push", remote, "HEAD:refs/heads/main")
	t.Setenv("WALGIT_FAILPOINT", "")
	local, err := commandOutput("", "git", "-C", remote, "rev-parse", "refs/heads/main")
	if err != nil || local != expected {
		t.Fatalf("local committed ref=%s err=%v, want %s", local, err, expected)
	}
	backend, err := wal.Open(storeLocation)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := backend.Load("repo")
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Generation != 1 || manifest.Refs["refs/heads/main"] != expected || len(manifest.Prepared) != 1 {
		t.Fatalf("committed failure was not durably recoverable: %#v", manifest)
	}
	updates := []wal.RefUpdate{{Old: strings.Repeat("0", len(expected)), New: expected, Ref: "refs/heads/main"}}
	if err := backend.Finalize("repo", updates); err != nil {
		t.Fatal(err)
	}
	manifest, err = backend.Load("repo")
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Prepared) != 0 || manifest.Refs["refs/heads/main"] != expected {
		t.Fatalf("prepared commit did not finalize cleanly: %#v", manifest)
	}
}
