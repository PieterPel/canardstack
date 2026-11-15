// A deterministic trace dataset: checkout-style traces across four services, with typed span and
// resource attributes, errors, events and a few links, so TraceQL filters, aggregates and
// structural operators have something to find.
package traceqlcompat_test

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"
)

type attrs map[string]any

type span struct {
	TraceID, SpanID, ParentID string
	Name                      string
	Kind                      int // OTLP: 1 internal, 2 server, 3 client, 4 producer, 5 consumer
	Start                     time.Time
	Duration                  time.Duration
	StatusCode                int // 0 unset, 1 ok, 2 error
	StatusMessage             string
	Service                   string
	Attrs                     attrs
	Events                    []event
	Links                     []link
}

type event struct {
	Name  string
	At    time.Time
	Attrs attrs
}

type link struct{ TraceID, SpanID string }

type trace struct {
	ID    string
	Spans []span
}

func id(r *rand.Rand, bytes int) string {
	b := make([]byte, bytes)
	r.Read(b)
	return hex.EncodeToString(b)
}

func pick[T any](r *rand.Rand, items []T) T { return items[r.Intn(len(items))] }

// generate makes n traces, one every step starting at start.
func generate(start time.Time, step time.Duration, n int) []trace {
	r := rand.New(rand.NewSource(7))
	methods := []string{"GET", "POST", "PUT", "DELETE"}
	routes := []string{"/api/orders", "/api/cart", "/api/users", "/healthz"}
	statuses := []int{200, 200, 200, 201, 404, 500, 503}
	var traces []trace
	for i := 0; i < n; i++ {
		t0 := start.Add(time.Duration(i) * step)
		tid := id(r, 16)
		method, route, status := pick(r, methods), pick(r, routes), pick(r, statuses)
		rootDur := time.Duration(50+r.Intn(900)) * time.Millisecond
		root := span{TraceID: tid, SpanID: id(r, 8), Name: method + " " + route, Kind: 2, Start: t0, Duration: rootDur, Service: "frontend",
			Attrs: attrs{"http.method": method, "http.route": route, "http.status_code": int64(status), "user.id": fmt.Sprintf("u%d", r.Intn(5)), "cache.hit": r.Intn(2) == 0}}
		if status >= 500 {
			root.StatusCode, root.StatusMessage = 2, "upstream failed"
		} else if status == 200 {
			root.StatusCode = 1
		}
		spans := []span{root}
		if route != "/healthz" {
			co := span{TraceID: tid, SpanID: id(r, 8), ParentID: root.SpanID, Name: "checkout.process", Kind: 3, Start: t0.Add(5 * time.Millisecond), Duration: rootDur * 8 / 10, Service: "frontend",
				Attrs: attrs{"peer.service": "checkout", "retry.count": int64(r.Intn(3))}}
			cs := span{TraceID: tid, SpanID: id(r, 8), ParentID: co.SpanID, Name: "ProcessOrder", Kind: 2, Start: t0.Add(10 * time.Millisecond), Duration: rootDur * 7 / 10, Service: "checkout",
				Attrs: attrs{"order.items": int64(1 + r.Intn(9)), "order.total": float64(r.Intn(50000)) / 100}}
			pay := span{TraceID: tid, SpanID: id(r, 8), ParentID: cs.SpanID, Name: "payment.charge", Kind: 1, Start: t0.Add(20 * time.Millisecond), Duration: rootDur * 3 / 10, Service: "payment",
				Attrs: attrs{"payment.provider": pick(r, []string{"stripe", "adyen", "paypal"}), "amount": float64(r.Intn(50000)) / 100}}
			if status >= 500 {
				pay.StatusCode, pay.StatusMessage = 2, "card declined"
				pay.Events = []event{{Name: "exception", At: pay.Start.Add(pay.Duration / 2), Attrs: attrs{"exception.type": "CardDeclined", "exception.message": "insufficient funds"}}}
			}
			db1 := span{TraceID: tid, SpanID: id(r, 8), ParentID: cs.SpanID, Name: "SELECT orders", Kind: 3, Start: t0.Add(15 * time.Millisecond), Duration: time.Duration(1+r.Intn(40)) * time.Millisecond, Service: "checkout",
				Attrs: attrs{"db.system": "postgresql", "db.statement": "SELECT * FROM orders WHERE id = $1"}}
			db2 := span{TraceID: tid, SpanID: id(r, 8), ParentID: cs.SpanID, Name: "INSERT audit", Kind: 3, Start: t0.Add(60 * time.Millisecond), Duration: time.Duration(1+r.Intn(20)) * time.Millisecond, Service: "checkout",
				Attrs: attrs{"db.system": "postgresql", "db.statement": "INSERT INTO audit VALUES ($1)"}}
			if i%7 == 0 && i > 0 {
				db2.Links = []link{{TraceID: traces[i-1].ID, SpanID: traces[i-1].Spans[0].SpanID}}
			}
			spans = append(spans, co, cs, pay, db1, db2)
		}
		traces = append(traces, trace{ID: tid, Spans: spans})
	}
	return traces
}

func otlpValue(v any) map[string]any {
	switch x := v.(type) {
	case string:
		return map[string]any{"stringValue": x}
	case int64:
		return map[string]any{"intValue": fmt.Sprint(x)}
	case float64:
		return map[string]any{"doubleValue": x}
	case bool:
		return map[string]any{"boolValue": x}
	}
	panic(fmt.Sprintf("unsupported attribute value %T", v))
}

func otlpAttrs(a attrs) []map[string]any {
	out := []map[string]any{}
	for k, v := range a {
		out = append(out, map[string]any{"key": k, "value": otlpValue(v)})
	}
	return out
}

func serviceAttrs(service string) []map[string]any {
	return otlpAttrs(attrs{"service.name": service, "service.version": "1." + fmt.Sprint(len(service)), "deployment.environment": "test"})
}

// otlpTraces renders one OTLP/JSON export request per trace (resource spans grouped by service).
func otlpTraces(traces []trace) []string {
	var out []string
	for _, t := range traces {
		byService := map[string][]span{}
		var order []string
		for _, s := range t.Spans {
			if _, ok := byService[s.Service]; !ok {
				order = append(order, s.Service)
			}
			byService[s.Service] = append(byService[s.Service], s)
		}
		var resources []any
		for _, svc := range order {
			var spans []any
			for _, s := range byService[svc] {
				m := map[string]any{
					"traceId": s.TraceID, "spanId": s.SpanID, "name": s.Name, "kind": s.Kind,
					"startTimeUnixNano": fmt.Sprint(s.Start.UnixNano()), "endTimeUnixNano": fmt.Sprint(s.Start.Add(s.Duration).UnixNano()),
					"attributes": otlpAttrs(s.Attrs),
				}
				if s.ParentID != "" {
					m["parentSpanId"] = s.ParentID
				}
				if s.StatusCode != 0 {
					m["status"] = map[string]any{"code": s.StatusCode, "message": s.StatusMessage}
				}
				var events []any
				for _, e := range s.Events {
					events = append(events, map[string]any{"name": e.Name, "timeUnixNano": fmt.Sprint(e.At.UnixNano()), "attributes": otlpAttrs(e.Attrs)})
				}
				if events != nil {
					m["events"] = events
				}
				var links []any
				for _, l := range s.Links {
					links = append(links, map[string]any{"traceId": l.TraceID, "spanId": l.SpanID})
				}
				if links != nil {
					m["links"] = links
				}
				spans = append(spans, m)
			}
			resources = append(resources, map[string]any{
				"resource":   map[string]any{"attributes": serviceAttrs(svc)},
				"scopeSpans": []any{map[string]any{"scope": map[string]any{"name": "compat", "version": "1.0"}, "spans": spans}},
			})
		}
		body, _ := json.Marshal(map[string]any{"resourceSpans": resources})
		out = append(out, string(body))
	}
	return out
}
