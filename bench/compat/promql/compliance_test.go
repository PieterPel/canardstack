// Runs the upstream PromQL compliance suite (github.com/prometheus/compliance) with canardstack as
// the target and Prometheus as the reference.
//
//	nix-shell -p go --run 'go test -run TestCompliance -timeout 30m ./...'   # from this directory
//
// Needs `cargo build` (target/debug/canardstack, or CANARDSTACK_BINARY) and `uv`. The reference
// Prometheus binary is downloaded by the suite unless PROMETHEUS_COMPLIANCE_PROMQL_BINARY is set.
// DEBUG=1 streams the child processes' output.
//
// The Prometheus reference target below is adapted from prometheus/compliance (Apache-2.0).
package promqlcompat_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"text/template"

	"github.com/prometheus/common/model"
	"github.com/prometheus/compliance"
	"github.com/prometheus/compliance/promql"
)

const (
	prometheusDownloadURL = "https://github.com/prometheus/prometheus/releases/download/v3.13.5/prometheus-3.13.5.{{.OS}}-{{.Arch}}.tar.gz"
	scrapeConfigTemplate  = `
global:
  scrape_interval: {{.ScrapeInterval}}

scrape_configs:
  - job_name: "{{.ScrapeTargetJobName}}"
    scrape_interval: {{.ScrapeInterval}}
    honor_labels: true
    honor_timestamps: true
    scrape_native_histograms: true
    scrape_protocols:
      - PrometheusProto
      - OpenMetricsText1.0.0
      - PrometheusText0.0.4
    static_configs:
      - targets: ["{{.ScrapeTargetHostPort}}"]
`
	apiKey = "compliance-key"
)

var scrapeConfigTmpl = template.Must(template.New("config").Parse(scrapeConfigTemplate))

type prometheus struct{ name string }

func (p prometheus) Name() string { return p.name }

func (p prometheus) Run(ctx context.Context, opts promql.Options) error {
	binary := os.Getenv("PROMETHEUS_COMPLIANCE_PROMQL_BINARY")
	if binary == "" {
		var err error
		if binary, err = compliance.DownloadBinary(prometheusDownloadURL, "prometheus"); err != nil {
			return err
		}
	}
	var buf bytes.Buffer
	if err := scrapeConfigTmpl.Execute(&buf, struct {
		ScrapeTargetJobName, ScrapeTargetHostPort, ScrapeInterval string
	}{opts.ScrapeTargetJobName, opts.ScrapeTargetHostPort, model.Duration(opts.ScrapeInterval).String()}); err != nil {
		return err
	}
	dir, err := os.MkdirTemp("", "promql-ref-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	configFile := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configFile, buf.Bytes(), 0o600); err != nil {
		return err
	}
	return promql.RunCommand(ctx, ".", nil, binary,
		fmt.Sprintf("--web.listen-address=%s", opts.QueryHostPort),
		fmt.Sprintf("--storage.tsdb.path=%v", dir),
		fmt.Sprintf("--config.file=%s", configFile))
}

// canardstack scrapes through bridge.py into a local SQLite-catalog DuckLake that a native
// canardstack serves. A reverse proxy in front adds the API key, which the suite's client cannot.
type canardstack struct{ repo string }

func (c canardstack) Name() string { return "canardstack" }

func (c canardstack) Run(ctx context.Context, opts promql.Options) error {
	dir, err := os.MkdirTemp("", "promql-canardstack-*")
	if err != nil {
		return err
	}
	if os.Getenv("KEEP_DIR") == "" {
		defer os.RemoveAll(dir)
	} else {
		fmt.Fprintln(os.Stderr, "keeping", dir)
	}
	catalog, data := filepath.Join(dir, "catalog.sqlite"), filepath.Join(dir, "data")
	if err := os.MkdirAll(data, 0o755); err != nil {
		return err
	}
	bridgePath := filepath.Join(c.repo, "bench/compat/promql/bridge.py")
	uv := func(args ...string) []string {
		return append([]string{"run", "--project", filepath.Join(c.repo, "bench"), bridgePath}, args...)
	}
	if out, err := exec.CommandContext(ctx, "uv", uv("init", catalog)...).CombinedOutput(); err != nil {
		return fmt.Errorf("bridge init: %w: %s", err, out)
	}

	binary := os.Getenv("CANARDSTACK_BINARY")
	if binary == "" {
		binary = filepath.Join(c.repo, "target/debug/canardstack")
	}
	private, err := compliance.AllocateFreeLocalHostPort()
	if err != nil {
		return err
	}
	errCh := make(chan error, 3)
	go func() {
		errCh <- promql.RunCommand(ctx, dir, []string{
			"CANARDSTACK_BIND=" + private,
			"CANARDSTACK_API_KEY=" + apiKey,
			"CANARDSTACK_ADMIN_API_KEY=" + apiKey + "-admin",
			"CANARDSTACK_DATA_DIR=" + filepath.Join(dir, "canardstack"),
			"CANARDSTACK_DUCKLAKE_ATTACH_URI=ducklake:sqlite:" + catalog,
			"CANARDSTACK_DUCKLAKE_DATA_PATH=" + data + "/",
		}, binary, "serve")
	}()
	if err := compliance.WaitForHTTPReady(ctx, "http://"+private+"/healthz", errCh, "canardstack"); err != nil {
		return err
	}

	target, _ := url.Parse("http://" + private)
	proxy := httputil.NewSingleHostReverseProxy(target)
	director := proxy.Director
	proxy.Director = func(r *http.Request) {
		director(r)
		r.Header.Set("Authorization", "Bearer "+apiKey)
	}
	server := &http.Server{Addr: opts.QueryHostPort, Handler: proxy}
	go func() { errCh <- server.ListenAndServe() }()
	defer server.Close()

	go func() {
		errCh <- promql.RunCommand(ctx, dir, nil, "uv", uv("run", catalog, data,
			"--scrape", opts.ScrapeTargetHostPort,
			"--interval", fmt.Sprintf("%f", opts.ScrapeInterval.Seconds()),
			"--job", opts.ScrapeTargetJobName)...)
	}()

	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("a canardstack component exited: %w", err)
	}
}

func TestCompliance(t *testing.T) {
	repo, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	promql.RunTests(t, prometheus{name: "reference-prometheus"}, canardstack{repo: repo}, promql.ComplianceTests())
}
