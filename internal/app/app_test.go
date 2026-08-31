package app

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"walgit/internal/wal"
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

func TestReadReferenceUpdatesIgnoresSymbolicHEADNotification(t *testing.T) {
	old := strings.Repeat("0", 40)
	newID := strings.Repeat("a", 40)
	input := old + " " + newID + " HEAD\n" + old + " " + newID + " refs/heads/main\n"
	updates, err := readReferenceUpdates(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	if len(updates) != 1 || updates[0].Ref != "refs/heads/main" {
		t.Fatalf("unexpected filtered updates: %#v", updates)
	}
	if _, err := readUpdates(strings.NewReader(old + " " + newID + " HEAD\n")); err == nil {
		t.Fatal("pre-receive parser accepted a pseudo-ref")
	}
}

func TestPersistentWriterGroupsConcurrentCommits(t *testing.T) {
	root := t.TempDir()
	storePath := filepath.Join(root, "store")
	backend, err := wal.Open(storePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Initialize("repo", "refs/heads/main", "sha1"); err != nil {
		t.Fatal(err)
	}
	socketRoot, err := os.MkdirTemp("/tmp", "walgit-writer-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(socketRoot)
	socket := filepath.Join(socketRoot, "writer.sock")
	handle, err := StartWriter(socket, storePath, "repo", 50*time.Millisecond, 64)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(socket)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("writer socket permissions are %o", info.Mode().Perm())
	}

	const count = 8
	updates := make([][]wal.RefUpdate, count)
	objects := filepath.Join(root, "objects")
	if err := os.MkdirAll(objects, 0o755); err != nil {
		t.Fatal(err)
	}
	for i := range updates {
		updates[i] = []wal.RefUpdate{{
			Old: strings.Repeat("0", 40), New: fmt.Sprintf("%040x", i+1), Ref: fmt.Sprintf("refs/heads/batch-%02d", i),
		}}
		if _, err := callWriter(socket, writerRequest{Operation: "stage", ObjectDir: objects, Updates: updates[i]}); err != nil {
			t.Fatal(err)
		}
	}
	start := make(chan struct{})
	errorsByWriter := make([]error, count)
	var group sync.WaitGroup
	for i := range updates {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			<-start
			_, errorsByWriter[i] = callWriter(socket, writerRequest{Operation: "commit", Updates: updates[i]})
		}(i)
	}
	close(start)
	group.Wait()
	for i, err := range errorsByWriter {
		if err != nil {
			t.Fatalf("writer %d: %v", i, err)
		}
	}
	response, err := callWriter(socket, writerRequest{Operation: "load"})
	if err != nil {
		t.Fatal(err)
	}
	if response.Manifest.Generation != count || len(response.Manifest.Refs) != count {
		t.Fatalf("unexpected grouped manifest: %#v", response.Manifest)
	}
	stats := handle.Stats()
	if stats.CommitRequests != count || stats.CommitBatches >= count || stats.MaximumBatch < 2 {
		t.Fatalf("commits were not grouped: %#v", stats)
	}
	if err := handle.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(socket); !os.IsNotExist(err) {
		t.Fatalf("writer socket survived shutdown: %v", err)
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
