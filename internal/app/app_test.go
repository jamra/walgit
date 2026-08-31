package app

import (
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadUpdates(t *testing.T) {
	old := strings.Repeat("0", 40)
	newID := strings.Repeat("a", 40)
	updates, err := readUpdates(strings.NewReader(old + " " + newID + " refs/heads/main\n"))
	if err != nil {
		t.Fatal(err)
	}
	if len(updates) != 1 || updates[0].New != newID || updates[0].Ref != "refs/heads/main" {
		t.Fatalf("unexpected updates: %#v", updates)
	}
}

func TestBenchmarkRestoresRepository(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	exe := filepath.Join(t.TempDir(), "walgit")
	cmd := exec.Command("go", "build", "-o", exe, "./cmd/walgit")
	cmd.Dir = filepath.Join("..", "..")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build hook executable: %v: %s", err, out)
	}
	t.Setenv("WALGIT_EXECUTABLE", exe)
	var out strings.Builder
	if err := Benchmark(3, 1024, false, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `"pushes": 3`) {
		t.Fatalf("unexpected benchmark output: %s", out.String())
	}
	out.Reset()
	if err := BenchmarkNodes(2, 3, 1024, false, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `"nodes": 2`) || !strings.Contains(out.String(), `"final_generation": 5`) {
		t.Fatalf("unexpected multi-node benchmark output: %s", out.String())
	}
}

func TestReconcileRepairsMissingLocalRefAfterManifestCommit(t *testing.T) {
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
	store := filepath.Join(root, "store")
	work := filepath.Join(root, "work")
	if err := Init(remote, store, "repo"); err != nil {
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
	if err := os.WriteFile(filepath.Join(work, "file"), []byte("content"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"-C", work, "add", "file"},
		{"-C", work, "commit", "-m", "initial"},
	} {
		if err := run("", "git", args...); err != nil {
			t.Fatal(err)
		}
	}
	receiveGateway := shellQuote(exe) + " gateway -store " + shellQuote(store) + " -id repo -service receive-pack"
	if err := run("", "git", "-C", work, "push", "--receive-pack="+receiveGateway, remote, "HEAD:refs/heads/main"); err != nil {
		t.Fatal(err)
	}
	expected, err := commandOutput("", "git", "-C", remote, "rev-parse", "refs/heads/main")
	if err != nil {
		t.Fatal(err)
	}
	emptyHooks := filepath.Join(root, "empty-hooks")
	if err := os.MkdirAll(emptyHooks, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := run("", "git", "-c", "core.hooksPath="+emptyHooks, "-C", remote, "update-ref", "-d", "refs/heads/main"); err != nil {
		t.Fatal(err)
	}
	if err := writeLocalState(remote, localState{RepositoryID: "repo", Generation: 0}); err != nil {
		t.Fatal(err)
	}
	gateway := shellQuote(exe) + " gateway -store " + shellQuote(store) + " -id repo -service upload-pack"
	cmd = exec.Command("git", "ls-remote", "--upload-pack="+gateway, remote)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("gateway read failed: %v: %s", err, out)
	} else if !strings.Contains(string(out), expected+"\trefs/heads/main") {
		t.Fatalf("gateway returned unexpected refs: %s", out)
	}
	actual, err := commandOutput("", "git", "-C", remote, "rev-parse", "refs/heads/main")
	if err != nil {
		t.Fatal(err)
	}
	if actual != expected {
		t.Fatalf("reconciled ref %s, want %s", actual, expected)
	}
	state, err := readLocalState(remote, "repo")
	if err != nil {
		t.Fatal(err)
	}
	if state.Generation != 1 {
		t.Fatalf("reconciled generation %d, want 1", state.Generation)
	}
}

func TestSmartHTTPRequiresAuthAndServesGitAdvertisement(t *testing.T) {
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
	store := filepath.Join(root, "store")
	if err := Init(remote, store, "repo"); err != nil {
		t.Fatal(err)
	}
	handler, err := NewGitHTTPHandler(HTTPOptions{Repository: remote, Store: store, RepositoryID: "repo", Token: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "http://walgit.test/repo.git/info/refs?service=git-upload-pack", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated response status %d, want 401", response.Code)
	}
	work := filepath.Join(root, "work")
	for _, args := range [][]string{
		{"init", "-b", "main", work},
		{"-C", work, "config", "user.name", "test"},
		{"-C", work, "config", "user.email", "test@example.invalid"},
	} {
		if err := run("", "git", args...); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(work, "file"), []byte("over smart HTTP"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"-C", work, "add", "file"},
		{"-C", work, "commit", "-m", "initial"},
		{"-C", work, "push", remote, "HEAD:refs/heads/main"},
	} {
		if err := run("", "git", args...); err != nil {
			t.Fatal(err)
		}
	}
	request = httptest.NewRequest(http.MethodGet, "http://walgit.test/repo.git/info/refs?service=git-upload-pack", nil)
	request.SetBasicAuth("git", "secret")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("authenticated response status %d, want 200: %s", response.Code, response.Body.String())
	}
	if contentType := response.Header().Get("Content-Type"); !strings.Contains(contentType, "application/x-git-upload-pack-advertisement") {
		t.Fatalf("unexpected Git advertisement content type %q", contentType)
	}
	if !strings.Contains(response.Body.String(), "refs/heads/main") {
		t.Fatalf("Git advertisement is missing main: %q", response.Body.String())
	}
}
