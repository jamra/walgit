package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"walgit/internal/app"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	var err error
	switch os.Args[1] {
	case "init":
		fs := flag.NewFlagSet("init", flag.ExitOnError)
		repo := fs.String("repo", "", "path to the bare repository")
		store := fs.String("store", "", "path to the durable WAL store")
		id := fs.String("id", "", "stable repository ID")
		_ = fs.Parse(os.Args[2:])
		err = app.Init(*repo, *store, *id)
	case "hook":
		if len(os.Args) < 3 {
			err = fmt.Errorf("hook requires a hook name")
		} else if os.Args[2] == "pre-receive" {
			err = app.PreReceive(os.Stdin)
		} else if os.Args[2] == "reference-transaction" && len(os.Args) == 4 {
			err = app.ReferenceTransaction(os.Args[3], os.Stdin)
		} else {
			err = fmt.Errorf("invalid hook invocation")
		}
	case "restore":
		fs := flag.NewFlagSet("restore", flag.ExitOnError)
		repo := fs.String("repo", "", "destination bare repository")
		store := fs.String("store", "", "path to the durable WAL store")
		id := fs.String("id", "", "repository ID")
		_ = fs.Parse(os.Args[2:])
		err = app.Restore(*repo, *store, *id)
	case "reconcile":
		fs := flag.NewFlagSet("reconcile", flag.ExitOnError)
		repo := fs.String("repo", "", "path to the bare repository cache")
		store := fs.String("store", "", "path to the durable WAL store")
		id := fs.String("id", "", "repository ID")
		_ = fs.Parse(os.Args[2:])
		err = app.Reconcile(*repo, *store, *id)
	case "gateway":
		fs := flag.NewFlagSet("gateway", flag.ExitOnError)
		store := fs.String("store", "", "path to the durable WAL store")
		id := fs.String("id", "", "repository ID")
		service := fs.String("service", "", "Git service: upload-pack or receive-pack")
		_ = fs.Parse(os.Args[2:])
		if len(fs.Args()) != 1 {
			err = fmt.Errorf("gateway requires exactly one repository path")
		} else {
			err = app.Gateway(fs.Args()[0], *store, *id, *service, os.Stdin, os.Stdout, os.Stderr)
		}
	case "compact":
		fs := flag.NewFlagSet("compact", flag.ExitOnError)
		repo := fs.String("repo", "", "path to a reconciled bare repository")
		store := fs.String("store", "", "path to the durable WAL store")
		id := fs.String("id", "", "repository ID")
		_ = fs.Parse(os.Args[2:])
		var checkpoint any
		checkpoint, err = app.Compact(*repo, *store, *id)
		if err == nil {
			err = json.NewEncoder(os.Stdout).Encode(checkpoint)
		}
	case "gc":
		fs := flag.NewFlagSet("gc", flag.ExitOnError)
		store := fs.String("store", "", "path to the durable WAL store")
		id := fs.String("id", "", "repository ID")
		grace := fs.Duration("grace", 24*time.Hour, "minimum age for unreferenced objects")
		_ = fs.Parse(os.Args[2:])
		var result any
		result, err = app.GarbageCollect(*store, *id, *grace)
		if err == nil {
			err = json.NewEncoder(os.Stdout).Encode(result)
		}
	case "scrub":
		fs := flag.NewFlagSet("scrub", flag.ExitOnError)
		store := fs.String("store", "", "path or s3:// URI for the primary durable authority")
		id := fs.String("id", "", "repository ID")
		_ = fs.Parse(os.Args[2:])
		var report any
		report, err = app.Scrub(*store, *id)
		if encodeErr := json.NewEncoder(os.Stdout).Encode(report); encodeErr != nil && err == nil {
			err = encodeErr
		}
	case "repair":
		fs := flag.NewFlagSet("repair", flag.ExitOnError)
		store := fs.String("store", "", "path or s3:// URI for the primary durable authority")
		id := fs.String("id", "", "repository ID")
		source := fs.String("source", "", "verified source authority: primary or secondary")
		_ = fs.Parse(os.Args[2:])
		var report any
		report, err = app.Repair(*store, *id, *source)
		if encodeErr := json.NewEncoder(os.Stdout).Encode(report); encodeErr != nil && err == nil {
			err = encodeErr
		}
	case "retention-check":
		fs := flag.NewFlagSet("retention-check", flag.ExitOnError)
		store := fs.String("store", "", "s3:// URI for the primary durable authority")
		minimum := fs.Duration("minimum", 30*24*time.Hour, "minimum default retention period")
		mode := fs.String("mode", "compliance", "required Object Lock mode: compliance or governance")
		_ = fs.Parse(os.Args[2:])
		var report any
		report, err = app.CheckRetention(*store, *minimum, *mode)
		if encodeErr := json.NewEncoder(os.Stdout).Encode(report); encodeErr != nil && err == nil {
			err = encodeErr
		}
	case "drill":
		fs := flag.NewFlagSet("drill", flag.ExitOnError)
		store := fs.String("store", "", "path or s3:// URI for the primary durable authority")
		id := fs.String("id", "", "repository ID")
		source := fs.String("source", "", "authority to restore independently: primary or secondary")
		keep := fs.Bool("keep", false, "keep the restored repository")
		_ = fs.Parse(os.Args[2:])
		var report any
		report, err = app.DisasterRestoreDrill(*store, *id, *source, *keep)
		if encodeErr := json.NewEncoder(os.Stdout).Encode(report); encodeErr != nil && err == nil {
			err = encodeErr
		}
	case "serve":
		fs := flag.NewFlagSet("serve", flag.ExitOnError)
		listen := fs.String("listen", "127.0.0.1:8080", "HTTP listen address")
		config := fs.String("config", "", "multi-repository host configuration file")
		repo := fs.String("repo", "", "path to the bare repository cache")
		store := fs.String("store", "", "durable storage path or s3:// URI")
		id := fs.String("id", "", "repository ID and HTTP path")
		authFile := fs.String("auth-file", "", "JSON authorization policy containing hashed tokens")
		anonymousRead := fs.Bool("anonymous-read", false, "allow unauthenticated clone and fetch")
		maxRequest := fs.Int64("max-request-bytes", 1<<30, "maximum Git HTTP request size")
		maintenanceInterval := fs.Duration("maintenance-interval", 5*time.Minute, "checkpoint, garbage collection, and eviction check interval; 0 disables")
		scrubInterval := fs.Duration("scrub-interval", 0, "dual-authority integrity scrub interval; 0 disables")
		compactEntries := fs.Int("compact-after-entries", 100, "checkpoint after this many WAL entries; 0 disables this threshold")
		compactBytes := fs.Int64("compact-after-bytes", 1<<30, "checkpoint after this many WAL bytes; 0 disables this threshold")
		gcGrace := fs.Duration("gc-grace", 24*time.Hour, "minimum age for unreferenced durable objects")
		idleCacheAfter := fs.Duration("idle-cache-after", 0, "evict an idle local cache after this duration; 0 disables")
		_ = fs.Parse(os.Args[2:])
		if *config != "" {
			err = app.ServeGitHTTPHost(*listen, *config, *authFile, os.Getenv("WALGIT_HTTP_TOKEN"))
		} else {
			err = app.ServeGitHTTP(*listen, app.HTTPOptions{
				Repository: *repo, Store: *store, RepositoryID: *id,
				Token: os.Getenv("WALGIT_HTTP_TOKEN"), AuthorizationFile: *authFile,
				AnonymousRead: *anonymousRead, MaximumRequestSize: *maxRequest,
				MaintenanceInterval: *maintenanceInterval, CompactAfterEntries: *compactEntries,
				ScrubInterval:     *scrubInterval,
				CompactAfterBytes: *compactBytes, GCGrace: *gcGrace, IdleCacheAfter: *idleCacheAfter,
			})
		}
	case "writer":
		fs := flag.NewFlagSet("writer", flag.ExitOnError)
		socket := fs.String("socket", "", "absolute Unix socket path")
		store := fs.String("store", "", "durable storage path or s3:// URI")
		id := fs.String("id", "", "repository ID")
		batchWindow := fs.Duration("batch-window", 5*time.Millisecond, "maximum group-commit collection window")
		batchMaximum := fs.Int("batch-maximum", 64, "maximum transactions per manifest commit")
		_ = fs.Parse(os.Args[2:])
		err = app.ServeWriter(*socket, *store, *id, *batchWindow, *batchMaximum)
	case "bench":
		fs := flag.NewFlagSet("bench", flag.ExitOnError)
		pushes := fs.Int("pushes", 100, "number of pushes")
		bytes := fs.Int("blob-bytes", 64*1024, "bytes changed per push")
		nodes := fs.Int("nodes", 1, "serving cache nodes; values above 1 run the shared-store benchmark")
		benchmarkStore := fs.String("store", "", "base s3:// URI for an isolated live-store benchmark")
		s3SingleWriter := fs.Bool("s3-single-writer", false, "disable manifest CAS for a store with externally serialized writes")
		persistentWriter := fs.Bool("persistent-writer", false, "route pushes through a persistent group-commit coordinator")
		keep := fs.Bool("keep", false, "keep benchmark files")
		_ = fs.Parse(os.Args[2:])
		if *nodes > 1 {
			if *s3SingleWriter {
				_ = os.Setenv("WALGIT_S3_DISABLE_CONDITIONAL_WRITES", "true")
			}
			if *persistentWriter {
				_ = os.Setenv("WALGIT_BENCH_PERSISTENT_WRITER", "true")
			}
			err = app.BenchmarkNodesAtStore(*nodes, *pushes, *bytes, *benchmarkStore, *keep, os.Stdout)
		} else {
			err = app.BenchmarkWithWriter(*pushes, *bytes, *keep, *persistentWriter, os.Stdout)
		}
	case "bench-dual":
		fs := flag.NewFlagSet("bench-dual", flag.ExitOnError)
		pushes := fs.Int("pushes", 100, "number of pushes")
		bytes := fs.Int("blob-bytes", 64*1024, "bytes changed per push")
		primary := fs.String("primary", "", "primary base s3:// URI")
		secondary := fs.String("secondary", "", "independent secondary base s3:// URI")
		allowUnprotected := fs.Bool("allow-unprotected", false, "benchmark without Object Lock; invalidates durability claims")
		keep := fs.Bool("keep", false, "keep isolated remote objects and local benchmark files")
		_ = fs.Parse(os.Args[2:])
		err = app.BenchmarkDualAuthorities(*primary, *secondary, *pushes, *bytes, *allowUnprotected, *keep, os.Stdout)
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "walgit:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: walgit <init|hook|restore|reconcile|gateway|compact|gc|scrub|repair|retention-check|drill|serve|writer|bench|bench-dual> [options]")
}
