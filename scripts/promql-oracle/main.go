// promql-oracle answers "what does Prometheus return for this load block
// and query" over a newline-delimited JSON protocol on stdin/stdout.
//
// It exists so the Rust conformance suite can be differential the way
// promql-engine's is: TestQueriesAgainstOldEngine runs every case
// against both the Thanos engine and Prometheus's own and asserts they
// match, which is why testcases/range_queries.yaml carries no expected
// values. Recording Go's current output as fixtures would rot silently
// as upstream evolves; asking the reference implementation on every run
// cannot.
//
// Usage:
//
//	promql-oracle              # serve: read requests until stdin EOF
//	promql-oracle -once        # read exactly one request, then exit
//
// The oracle deliberately does not interpret the load block itself. It
// hands the raw text to promqltest, so the sample sequences behind
// `46.00+13.00x40`, `_` and `stale` are materialised by upstream's own
// code. An oracle that reimplemented those rules could be confidently
// wrong, which is the one failure mode that would make the whole suite
// worthless.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/teststorage"
)

func main() {
	once := flag.Bool("once", false, "read a single request, answer it, and exit")
	flag.Parse()

	// The corpus uses experimental functions, and this is a package
	// global rather than an engine option. testcases_test.go in
	// promql-engine sets the same flag before parsing corpus queries;
	// without it a handful of queries fail to parse for the wrong
	// reason.
	parser.EnableExperimentalFunctions = true

	in := bufio.NewReaderSize(os.Stdin, 1<<20)
	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()

	for {
		line, err := in.ReadString('\n')
		if len(strings.TrimSpace(line)) > 0 {
			resp := answer(line)
			buf, mErr := json.Marshal(resp)
			if mErr != nil {
				// Cannot serialise the answer: fail loudly rather than
				// emit a line the client will misread.
				fmt.Fprintf(os.Stderr, "promql-oracle: marshal: %v\n", mErr)
				os.Exit(1)
			}
			out.Write(buf)
			out.WriteByte('\n')
			// Flush per response: the client blocks reading this line
			// before it sends the next request.
			if fErr := out.Flush(); fErr != nil {
				os.Exit(1)
			}
			if *once {
				return
			}
		}
		if err != nil {
			// EOF is the normal shutdown path. The client closing its
			// pipe -- including by dying -- ends the process, so no
			// oracle is ever orphaned.
			if err == io.EOF {
				return
			}
			fmt.Fprintf(os.Stderr, "promql-oracle: read: %v\n", err)
			os.Exit(1)
		}
	}
}

// answer handles one request line. It never panics: a malformed request
// or a panicking engine becomes an error Response, so one bad case
// cannot take down the serve loop.
func answer(line string) (resp Response) {
	var req Request
	if err := json.Unmarshal([]byte(line), &req); err != nil {
		return Response{Kind: "error", Err: fmt.Sprintf("decode request: %v", err)}
	}

	defer func() {
		if r := recover(); r != nil {
			resp = Response{ID: req.ID, Kind: "error", Err: fmt.Sprintf("panic: %v", r)}
		}
	}()

	res, err := execute(req)
	if err != nil {
		return Response{ID: req.ID, Kind: "error", Err: err.Error()}
	}
	res.ID = req.ID
	return res
}

func execute(req Request) (Response, error) {
	if req.StepMS <= 0 {
		return Response{}, fmt.Errorf("step_ms must be positive, got %d", req.StepMS)
	}
	if req.EndMS < req.StartMS {
		return Response{}, fmt.Errorf("end_ms %d is before start_ms %d", req.EndMS, req.StartMS)
	}
	if err := checkLoadSupported(req.Load); err != nil {
		return Response{}, err
	}

	end := time.UnixMilli(req.EndMS)

	db, closer, err := seed(req.Load, end)
	if err != nil {
		return Response{}, err
	}
	defer closer()

	// Our own engine rather than ll.QueryEngine(): the loader's internal
	// engine hardcodes MaxSamples 10000 and offers no lookback control.
	// These options mirror promql-engine's differential test
	// (engine_test.go:212-218) so that a divergence is ours and not a
	// configuration difference.
	eng := promql.NewEngine(promql.EngineOpts{
		Timeout:                  time.Hour,
		MaxSamples:               1e10,
		EnableAtModifier:         true,
		EnableNegativeOffset:     true,
		LookbackDelta:            time.Duration(req.LookbackMS) * time.Millisecond,
		NoStepSubqueryIntervalFn: func(int64) int64 { return time.Minute.Milliseconds() },
	})

	ctx := context.Background()
	q, err := eng.NewRangeQuery(
		ctx, db, nil, req.Query,
		time.UnixMilli(req.StartMS), end,
		time.Duration(req.StepMS)*time.Millisecond,
	)
	if err != nil {
		// A query that does not build is a legitimate result, not a
		// harness failure: some cases assert that both engines reject
		// the same expression.
		return Response{Kind: "error", Err: err.Error()}, nil
	}
	defer q.Close()

	return encode(q.Exec(ctx)), nil
}

// seed builds the storage a query runs against, materialising every
// sample up to `end`.
//
// Many corpus cases have no load block at all -- `pi`, `vector(1)`,
// `scalar binary op == true` -- and those must run against empty
// storage and return a real result. Handing an empty string to the lazy
// loader instead fails with `no "load" command found`, which would be
// reported as a query error; since comparison counts two errors as a
// match, such a case could then pass without either implementation
// having computed anything.
func seed(load string, end time.Time) (storage.Queryable, func(), error) {
	if !hasLoadDirective(load) {
		st, err := teststorage.NewWithError()
		if err != nil {
			return nil, nil, fmt.Errorf("empty storage: %w", err)
		}
		return st, func() { st.Close() }, nil
	}

	// promqltest.LoadedStorage -- the function the Go tests use -- takes
	// a testing.TB, and testing.TB has an unexported method that blocks
	// implementations outside package testing, so a main package cannot
	// call it. NewLazyLoader is the exported, error-returning door to
	// the same machinery: it runs the same parseLoad and builds the same
	// loadCmd, and appendTill calls the same appendSample.
	ll, err := promqltest.NewLazyLoader(load, promqltest.LazyLoaderOpts{
		EnableAtModifier:     true,
		EnableNegativeOffset: true,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("load: %w", err)
	}

	// The loader is lazy: samples exist only up to the timestamp asked
	// for here, so materialise the whole query range before querying.
	var appendErr error
	ll.WithSamplesTill(end, func(err error) { appendErr = err })
	if appendErr != nil {
		ll.Close()
		return nil, nil, fmt.Errorf("append samples: %w", appendErr)
	}

	return ll.Storage(), func() { ll.Close() }, nil
}

// hasLoadDirective reports whether the block opens with a `load`
// directive. The directive is always the first non-empty line.
func hasLoadDirective(load string) bool {
	for _, line := range strings.Split(load, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		return strings.HasPrefix(strings.ToLower(line), "load")
	}
	return false
}

// checkLoadSupported rejects load directives the lazy loader would
// mishandle.
//
// loadCmd.append converts classic histograms to native ones when the
// block says `load_with_nhcb`; appendTill, which is what the lazy loader
// uses, has no such branch and would seed only the raw series. The
// corpus contains no such block today, so erroring keeps a future case
// from quietly receiving a wrong answer instead of a loud one.
func checkLoadSupported(load string) error {
	for _, line := range strings.Split(load, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.HasPrefix(strings.ToLower(line), "load_with_nhcb") {
			return fmt.Errorf("load_with_nhcb is not supported: the lazy loader skips " +
				"the NHCB conversion that loadCmd.append performs")
		}
		// Only the directive line matters; it is the first non-empty one.
		return nil
	}
	return nil
}

func encode(res *promql.Result) Response {
	if res.Err != nil {
		return Response{Kind: "error", Err: res.Err.Error(), Warnings: warnings(res)}
	}

	out := Response{Warnings: warnings(res)}
	switch v := res.Value.(type) {
	case promql.Matrix:
		out.Kind = "matrix"
		out.Series = make([]Series, 0, len(v))
		for _, s := range v {
			row := Series{Labels: labelMap(s.Metric), Histograms: len(s.Histograms)}
			for _, p := range s.Floats {
				row.Floats = append(row.Floats, Point{T: p.T, V: formatFloat(p.F)})
			}
			out.Series = append(out.Series, row)
		}
		// Deterministic order so the Rust side never has to guess, and
		// so two runs are byte-identical.
		sort.Slice(out.Series, func(i, j int) bool {
			return labelKey(out.Series[i].Labels) < labelKey(out.Series[j].Labels)
		})
	case promql.Vector:
		out.Kind = "vector"
		out.Samples = make([]Sample, 0, len(v))
		for _, s := range v {
			out.Samples = append(out.Samples, Sample{
				Labels:    labelMap(s.Metric),
				T:         s.T,
				V:         formatFloat(s.F),
				Histogram: s.H != nil,
			})
		}
		sort.Slice(out.Samples, func(i, j int) bool {
			return labelKey(out.Samples[i].Labels) < labelKey(out.Samples[j].Labels)
		})
	case promql.Scalar:
		out.Kind = "scalar"
		out.Value = formatFloat(v.V)
		out.T = v.T
	case promql.String:
		out.Kind = "string"
		out.Value = v.V
		out.T = v.T
	default:
		out.Kind = "error"
		out.Err = fmt.Sprintf("unhandled result type %T", res.Value)
	}
	return out
}

func warnings(res *promql.Result) []string {
	if len(res.Warnings) == 0 {
		return nil
	}
	_, ws := res.Warnings.AsStrings("", 0, 0)
	if len(ws) == 0 {
		return nil
	}
	sort.Strings(ws)
	return ws
}

func labelMap(ls labels.Labels) map[string]string {
	m := make(map[string]string, ls.Len())
	ls.Range(func(l labels.Label) { m[l.Name] = l.Value })
	return m
}

// labelKey is a stable sort key for a label set. encoding/json emits map
// keys sorted, so building the key the same way keeps the JSON order and
// the sort order consistent.
func labelKey(m map[string]string) string {
	names := make([]string, 0, len(m))
	for n := range m {
		names = append(names, n)
	}
	sort.Strings(names)
	var b strings.Builder
	for _, n := range names {
		b.WriteString(n)
		b.WriteByte('=')
		b.WriteString(m[n])
		b.WriteByte(',')
	}
	return b.String()
}

// formatFloat emits the shortest decimal that round-trips to the same
// f64. NaN and the infinities come out as "NaN", "+Inf" and "-Inf",
// all of which Rust's f64::from_str accepts.
func formatFloat(f float64) string {
	return strconv.FormatFloat(f, 'g', -1, 64)
}
