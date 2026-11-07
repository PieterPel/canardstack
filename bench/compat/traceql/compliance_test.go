// Runs TraceQL queries against a real Tempo (the reference) and against canardstack, feeding both
// the same generated traces, and compares the search results: the queries in queries.yml plus
// every non-metrics query from Tempo's own list of valid examples.
//
//	nix-shell -p go --run 'go test -run TestCompliance -timeout 30m -count=1 -json ./... > results.json'
//
// Needs `cargo build` (target/debug/canardstack, or CANARDSTACK_BINARY) and `uv`. The Tempo binary
// is downloaded into ./bin (TEMPO_VERSION, default 3.1.1). DEBUG=1 streams the child processes;
// KEEP_DIR=1 keeps their temp directories.
package traceqlcompat_test

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	apiKey      = "compliance-key"
	concurrency = 8
	traceCount  = 60
)

var tempoConfig = `stream_over_http_enabled: true
server:
  http_listen_port: %[1]d
  grpc_listen_port: %[2]d
  log_level: warn
distributor:
  receivers:
    otlp:
      protocols:
        http:
          endpoint: 127.0.0.1:%[3]d
storage:
  trace:
    backend: local
    wal:
      path: %[4]s/wal
    local:
      path: %[4]s/blocks
live_store:
  wal:
    path: %[4]s/live-store/traces
  shutdown_marker_dir: %[4]s/live-store/shutdown-marker
block_builder:
  wal:
    path: %[4]s/block-builder/traces
backend_scheduler:
  local_work_path: %[4]s/scheduler
usage_report:
  reporting_enabled: false
`

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func tempDir(t *testing.T, name string) string {
	t.Helper()
	dir, err := os.MkdirTemp("", name+"-*")
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv("KEEP_DIR") == "" {
		t.Cleanup(func() { os.RemoveAll(dir) })
	} else {
		fmt.Fprintln(os.Stderr, "keeping", dir)
	}
	return dir
}

func start(t *testing.T, dir string, env []string, prog string, args ...string) {
	t.Helper()
	cmd := exec.Command(prog, args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), env...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if os.Getenv("DEBUG") != "" {
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", prog, err)
	}
	t.Cleanup(func() {
		if pgid, err := syscall.Getpgid(cmd.Process.Pid); err == nil {
			_ = syscall.Kill(-pgid, syscall.SIGTERM)
		}
		_ = cmd.Wait()
	})
}

func waitFor(t *testing.T, what string, timeout time.Duration, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("%s was not ready after %s", what, timeout)
}

func httpOK(target string, header http.Header) bool {
	req, _ := http.NewRequest(http.MethodGet, target, nil)
	for k, v := range header {
		req.Header[k] = v
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode == 200
}

func downloadTempo(t *testing.T) string {
	t.Helper()
	version := os.Getenv("TEMPO_VERSION")
	if version == "" {
		version = "3.1.1"
	}
	path, _ := filepath.Abs(filepath.Join("bin", fmt.Sprintf("tempo-%s-%s-%s", runtime.GOOS, runtime.GOARCH, version)))
	if _, err := os.Stat(path); err == nil {
		return path
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	u := fmt.Sprintf("https://github.com/grafana/tempo/releases/download/v%s/tempo_%s_%s_%s.tar.gz", version, version, runtime.GOOS, runtime.GOARCH)
	t.Logf("downloading %s", u)
	resp, err := http.Get(u)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("download %s: %v (%v)", u, err, resp)
	}
	defer resp.Body.Close()
	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(gz)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			t.Fatalf("tempo binary not found in %s", u)
		}
		if err != nil {
			t.Fatal(err)
		}
		if filepath.Base(h.Name) != "tempo" {
			continue
		}
		out, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
		if err != nil {
			t.Fatal(err)
		}
		_, err = io.Copy(out, tr)
		out.Close()
		if err != nil {
			t.Fatal(err)
		}
		return path
	}
}

func startTempo(t *testing.T, traces []trace) string {
	t.Helper()
	binary := downloadTempo(t)
	dir := tempDir(t, "tempo")
	httpPort, grpcPort, otlpPort := freePort(t), freePort(t), freePort(t)
	config := filepath.Join(dir, "tempo.yml")
	if err := os.WriteFile(config, []byte(fmt.Sprintf(tempoConfig, httpPort, grpcPort, otlpPort, dir)), 0o644); err != nil {
		t.Fatal(err)
	}
	start(t, dir, nil, binary, "-config.file="+config)
	base := fmt.Sprintf("http://127.0.0.1:%d", httpPort)
	waitFor(t, "Tempo", 2*time.Minute, func() bool { return httpOK(base+"/ready", nil) })
	// Tempo 3 reports ready before its live-store partition turns active (about 15s in), and
	// traces pushed earlier are not searchable.
	time.Sleep(25 * time.Second)
	for _, body := range otlpTraces(traces) {
		resp, err := http.Post(fmt.Sprintf("http://127.0.0.1:%d/v1/traces", otlpPort), "application/json", strings.NewReader(body))
		if err != nil || resp.StatusCode >= 300 {
			msg := []byte{}
			if resp != nil {
				msg, _ = io.ReadAll(resp.Body)
			}
			t.Fatalf("push to Tempo: %v %s", err, msg)
		}
		resp.Body.Close()
	}
	return base
}

func startCanardstack(t *testing.T, repo string, traces []trace) string {
	t.Helper()
	dir := tempDir(t, "traceql-canardstack")
	catalog, data := filepath.Join(dir, "catalog.sqlite"), filepath.Join(dir, "data")
	if err := os.MkdirAll(data, 0o755); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(repo, "bench/compat/traceql/ingest_traces.py")
	uv := func(args ...string) *exec.Cmd {
		return exec.Command("uv", append([]string{"run", "--project", filepath.Join(repo, "bench"), script}, args...)...)
	}
	if out, err := uv("init", catalog).CombinedOutput(); err != nil {
		t.Fatalf("init catalog: %v: %s", err, out)
	}
	binary := os.Getenv("CANARDSTACK_BINARY")
	if binary == "" {
		binary = filepath.Join(repo, "target/debug/canardstack")
	}
	port := freePort(t)
	start(t, dir, []string{
		fmt.Sprintf("CANARDSTACK_BIND=127.0.0.1:%d", port),
		"CANARDSTACK_API_KEY=" + apiKey,
		"CANARDSTACK_ADMIN_API_KEY=" + apiKey + "-admin",
		"CANARDSTACK_DATA_DIR=" + filepath.Join(dir, "canardstack"),
		"CANARDSTACK_DUCKLAKE_ATTACH_URI=ducklake:sqlite:" + catalog,
		"CANARDSTACK_DUCKLAKE_DATA_PATH=" + data + "/",
		"CANARDSTACK_QUERY_CONCURRENCY=16",
		"CANARDSTACK_QUERY_ADMISSION_WAIT_MS=120000",
		"CANARDSTACK_QUERY_TIMEOUT_SECS=60",
	}, binary, "serve")
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitFor(t, "canardstack", 2*time.Minute, func() bool { return httpOK(base+"/healthz", nil) })
	file := filepath.Join(dir, "traces.ndjson")
	if err := os.WriteFile(file, []byte(strings.Join(otlpTraces(traces), "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if out, err := uv("run", catalog, data, file).CombinedOutput(); err != nil {
		t.Fatalf("ingest: %v: %s", err, out)
	}
	return base
}

type target struct {
	base   string
	header http.Header
}

type window struct{ start, end time.Time }

func (tg target) search(query string, w window) (map[string]traceSummary, error) {
	params := url.Values{
		"q":     {query},
		"start": {fmt.Sprint(w.start.Unix())},
		"end":   {fmt.Sprint(w.end.Unix())},
		"limit": {"1000"},
		"spss":  {"100"},
	}
	req, _ := http.NewRequest(http.MethodGet, tg.base+"/api/search?"+params.Encode(), nil)
	for k, v := range tg.header {
		req.Header[k] = v
	}
	client := http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("%d: %s", resp.StatusCode, truncate(string(body), 200))
	}
	return summarise(body)
}

func truncate(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

var metricsQuery = regexp.MustCompile(`rate\(\)|_over_time|compare\(|topk|bottomk`)

func loadQueries(t *testing.T) []string {
	t.Helper()
	var ours struct {
		Queries []string `yaml:"queries"`
	}
	raw, err := os.ReadFile("queries.yml")
	if err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(raw, &ours); err != nil {
		t.Fatal(err)
	}
	var tempo struct {
		Valid []string `yaml:"valid"`
	}
	raw, err = os.ReadFile("../../../crates/traceql-syntax/tests/test_examples.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(raw, &tempo); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	var out []string
	for _, q := range append(ours.Queries, tempo.Valid...) {
		if metricsQuery.MatchString(q) || seen[q] {
			continue
		}
		seen[q] = true
		out = append(out, q)
	}
	return out
}

var spaces = regexp.MustCompile(`\s+`)

func TestCompliance(t *testing.T) {
	repo, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	queries := loadQueries(t)
	// Whole seconds, so Tempo and canardstack see the same timestamps.
	// Tempo's live-store only serves recent traces, so keep the data fresh and dense.
	base := time.Now().UTC().Truncate(time.Second).Add(-70 * time.Second)
	traces := generate(base, time.Second, traceCount)
	w := window{start: base.Add(-time.Minute), end: base.Add(10 * time.Minute)}

	ref := target{base: startTempo(t, traces)}
	got := target{base: startCanardstack(t, repo, traces), header: http.Header{"Authorization": {"Bearer " + apiKey}}}

	waitFor(t, "Tempo to index the traces", 3*time.Minute, func() bool {
		res, err := ref.search("{ }", w)
		return err == nil && len(res) == traceCount
	})
	t.Logf("%d traces loaded, %d queries", traceCount, len(queries))

	sem := make(chan struct{}, concurrency)
	t.Run("queries", func(t *testing.T) {
		for _, q := range queries {
			q := q
			t.Run(spaces.ReplaceAllString(strings.TrimSpace(q), " "), func(t *testing.T) {
				t.Parallel()
				sem <- struct{}{}
				defer func() { <-sem }()
				want, refErr := ref.search(q, w)
				if refErr != nil {
					t.Skipf("reference rejects the query: %v", refErr)
				}
				have, err := got.search(q, w)
				if err != nil {
					t.Fatalf("unexpected failure: %v", err)
				}
				if d := diff(want, have); d != "" {
					t.Fatalf("different results: %s", d)
				}
			})
		}
	})
}
