// Runs oteldb's LogQL query templates against a real Loki (the reference) and against canardstack,
// feeding both the same generated log lines, and compares every response.
//
//	nix-shell -p go --run 'go test -run TestCompliance -timeout 30m -json ./... > results.json'
//
// Needs `cargo build` (target/debug/canardstack, or CANARDSTACK_BINARY) and `uv`. The Loki binary
// is downloaded into ./bin (LOKI_VERSION, default 3.0.0, the reference oteldb's suite targets).
// DEBUG=1 streams the child processes' output; KEEP_DIR=1 keeps their temp directories.
package logqlcompat_test

import (
	"archive/zip"
	"bytes"
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
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	apiKey      = "compliance-key"
	queryLimit  = "1000"
	queryStep   = "10"
	concurrency = 8
)

var lokiConfig = `auth_enabled: false
server:
  http_listen_port: %d
  grpc_listen_port: %d
common:
  instance_addr: 127.0.0.1
  path_prefix: %s
  storage:
    filesystem:
      chunks_directory: %s/chunks
      rules_directory: %s/rules
  replication_factor: 1
  ring:
    kvstore:
      store: inmemory
querier:
  max_concurrent: 4096
schema_config:
  configs:
    - from: 2024-04-01
      store: tsdb
      object_store: filesystem
      schema: v13
      index:
        prefix: index_
        period: 24h
limits_config:
  max_entries_limit_per_query: 50000000000
  # Attribute discovery is implementation-defined, so disable it.
  discover_service_name: []
analytics:
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

// start runs a child process until the test ends.
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

func waitFor(t *testing.T, what, target string, header http.Header) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequest(http.MethodGet, target, nil)
		for k, v := range header {
			req.Header[k] = v
		}
		if resp, err := http.DefaultClient.Do(req); err == nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == 200 {
				_ = body
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("%s did not become ready at %s", what, target)
}

func downloadLoki(t *testing.T) string {
	t.Helper()
	version := os.Getenv("LOKI_VERSION")
	if version == "" {
		version = "3.0.0"
	}
	name := fmt.Sprintf("loki-%s-%s", runtime.GOOS, runtime.GOARCH)
	path := filepath.Join("bin", name+"-"+version)
	if abs, err := filepath.Abs(path); err == nil {
		path = abs
	}
	if _, err := os.Stat(path); err == nil {
		return path
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	u := fmt.Sprintf("https://github.com/grafana/loki/releases/download/v%s/%s.zip", version, name)
	t.Logf("downloading %s", u)
	resp, err := http.Get(u)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("download %s: %v (status %v)", u, err, resp)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range zr.File {
		if f.Name != name {
			continue
		}
		rc, _ := f.Open()
		out, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
		if err != nil {
			t.Fatal(err)
		}
		_, err = io.Copy(out, rc)
		out.Close()
		rc.Close()
		if err != nil {
			t.Fatal(err)
		}
		return path
	}
	t.Fatalf("%s not found in %s", name, u)
	return ""
}

func startLoki(t *testing.T) string {
	t.Helper()
	binary := downloadLoki(t)
	dir := tempDir(t, "loki")
	httpPort, grpcPort := freePort(t), freePort(t)
	config := filepath.Join(dir, "loki.yml")
	if err := os.WriteFile(config, []byte(fmt.Sprintf(lokiConfig, httpPort, grpcPort, dir, dir, dir)), 0o644); err != nil {
		t.Fatal(err)
	}
	// UTC, so `__timestamp__` prints the zone canardstack uses (Loki prints the server's local zone).
	start(t, dir, []string{"TZ=UTC"}, binary, "-config.file="+config)
	base := fmt.Sprintf("http://127.0.0.1:%d", httpPort)
	waitFor(t, "Loki", base+"/ready", nil)
	return base
}

func startCanardstack(t *testing.T, repo string, lines []logLine) string {
	t.Helper()
	dir := tempDir(t, "logql-canardstack")
	catalog, data := filepath.Join(dir, "catalog.sqlite"), filepath.Join(dir, "data")
	if err := os.MkdirAll(data, 0o755); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(repo, "bench/compat/logql/ingest_logs.py")
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
	waitFor(t, "canardstack", base+"/healthz", nil)

	file := filepath.Join(dir, "logs.ndjson")
	if err := os.WriteFile(file, []byte(strings.Join(otlpLogs(lines), "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if out, err := uv("run", catalog, data, file).CombinedOutput(); err != nil {
		t.Fatalf("ingest: %v: %s", err, out)
	}
	return base
}

type target struct {
	name   string
	base   string
	header http.Header
}

type window struct{ start, end time.Time }

func (tg target) queryRange(query string, w window) (*envelope, error) {
	params := url.Values{
		"query":     {query},
		"start":     {fmt.Sprint(w.start.UnixNano())},
		"end":       {fmt.Sprint(w.end.UnixNano())},
		"step":      {queryStep},
		"direction": {"forward"},
		"limit":     {queryLimit},
	}
	req, _ := http.NewRequest(http.MethodGet, tg.base+"/loki/api/v1/query_range?"+params.Encode(), nil)
	for k, v := range tg.header {
		req.Header[k] = v
	}
	var status int
	var body []byte
	if err := measure("logql", tg.name, query, func() (bool, error) {
		client := http.Client{Timeout: 60 * time.Second}
		resp, err := client.Do(req.Clone(req.Context()))
		if err != nil {
			return false, err
		}
		defer resp.Body.Close()
		body, _ = io.ReadAll(resp.Body)
		status = resp.StatusCode
		return status == 200, nil
	}); err != nil {
		return nil, err
	}
	e := &envelope{}
	if err := jsonUnmarshal(body, e); err != nil {
		return nil, fmt.Errorf("status %d: %s", status, truncate(string(body), 200))
	}
	if status != 200 || e.Status != "success" {
		message := e.Error
		if message == "" {
			message = string(body)
		}
		return nil, fmt.Errorf("%d %s: %s", status, e.ErrorType, truncate(message, 160))
	}
	return e, nil
}

func truncate(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

// scale multiplies the number of generated log lines (SCALE=10 is ten times as many).
func scale() int {
	n, err := strconv.Atoi(os.Getenv("SCALE"))
	if err != nil || n < 1 {
		return 1
	}
	return n
}

var spaces = regexp.MustCompile(`\s+`)

func TestCompliance(t *testing.T) {
	repo, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile("queries.yml")
	if err != nil {
		t.Fatal(err)
	}
	var cfg struct {
		TestCases []pattern `yaml:"test_cases"`
	}
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	cases, err := expand(cfg.TestCases)
	if err != nil {
		t.Fatal(err)
	}

	end := time.Now().UTC().Truncate(time.Second).Add(-5 * time.Second)
	lines := generate(end.Add(-2*time.Minute), end, time.Second, 4*scale(), []string{"log.json", "other.json", "third.json"})
	w := window{start: end.Add(-time.Minute), end: end}

	loki := startLoki(t)
	resp, err := http.Post(loki+"/loki/api/v1/push", "application/json", bytes.NewReader(lokiPush(lines)))
	if err != nil || resp.StatusCode >= 300 {
		body := []byte{}
		if resp != nil {
			body, _ = io.ReadAll(resp.Body)
		}
		t.Fatalf("push to Loki: %v %s", err, body)
	}
	resp.Body.Close()
	ref := target{name: "loki", base: loki}
	got := target{name: "canardstack", base: startCanardstack(t, repo, lines), header: http.Header{"Authorization": {"Bearer " + apiKey}}}
	t.Logf("%d lines pushed, %d queries", len(lines), len(cases))

	sem := make(chan struct{}, concurrency)
	t.Run("queries", func(t *testing.T) {
		for _, tc := range cases {
			tc := tc
			t.Run(spaces.ReplaceAllString(strings.TrimSpace(tc.Query), " "), func(t *testing.T) {
				t.Parallel()
				sem <- struct{}{}
				defer func() { <-sem }()
				compare(t, tc, ref, got, w)
			})
		}
	})
}

func compare(t *testing.T, tc testCase, ref, got target, w window) {
	refResp, refErr := ref.queryRange(tc.Query, w)
	gotResp, gotErr := got.queryRange(tc.Query, w)
	if (refErr != nil) != tc.ShouldFail {
		if refErr != nil {
			t.Skipf("reference rejects the query: %v", refErr)
		}
		t.Skip("reference accepted a query that should fail")
	}
	if (gotErr != nil) != tc.ShouldFail {
		if gotErr != nil {
			t.Fatalf("unexpected failure: %v", gotErr)
		}
		t.Fatal("query succeeded, but should have failed")
	}
	if tc.ShouldFail || tc.Skip {
		return
	}
	refKind, refSeries, err := normalise(refResp)
	if err != nil {
		t.Fatalf("reference response: %v", err)
	}
	gotKind, gotSeries, err := normalise(gotResp)
	if err != nil {
		t.Fatalf("response: %v", err)
	}
	if refKind != gotKind {
		t.Fatalf("different results: result type %s, want %s", gotKind, refKind)
	}
	if isEmpty(refKind, refSeries) != tc.ShouldBeEmpty {
		t.Skipf("reference gave an unexpected %s result", map[bool]string{true: "empty", false: "non-empty"}[isEmpty(refKind, refSeries)])
	}
	if isEmpty(gotKind, gotSeries) != tc.ShouldBeEmpty {
		t.Fatalf("unexpected %s result", map[bool]string{true: "empty", false: "non-empty"}[isEmpty(gotKind, gotSeries)])
	}
	if d := diff(refKind, refSeries, gotSeries); d != "" {
		t.Fatalf("different results: %s", d)
	}
}
