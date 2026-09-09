package main

// Wire types for the newline-delimited JSON protocol. One Request per
// input line, one Response per output line, strictly in order.
//
// Floats are carried as strings throughout. JSON numbers cannot
// represent NaN, +Inf or -Inf, and the test corpus produces all three.
// Go's strconv.FormatFloat(v, 'g', -1, 64) emits the shortest decimal
// that round-trips, so Rust's f64::from_str recovers the identical bit
// pattern; Rust also accepts Go's "NaN"/"+Inf"/"-Inf" spellings.

// Request asks for one range query over one load block.
type Request struct {
	// ID is echoed back untouched. The protocol is strictly
	// request/response so it is not needed to correlate, but it turns a
	// stream desync into a loud failure instead of silently comparing
	// one case against another's answer.
	ID int64 `json:"id"`
	// Load is a Prometheus test-script `load` block, passed through
	// verbatim so that Go does its own parsing. That is the whole point
	// of the oracle: we compare against upstream's reading of the
	// input, not our own.
	Load  string `json:"load"`
	Query string `json:"query"`
	// Query range. StartMS and EndMS are Unix timestamps in
	// milliseconds and may be negative; StepMS is a duration.
	StartMS int64 `json:"start_ms"`
	EndMS   int64 `json:"end_ms"`
	StepMS  int64 `json:"step_ms"`
	// LookbackMS is the engine's lookback delta. Zero means the
	// Prometheus default (5m).
	LookbackMS int64 `json:"lookback_ms"`
}

// Response is the result of one Request.
//
// Kind distinguishes the payload field that is populated:
//
//	"matrix" -> Series
//	"vector" -> Samples
//	"scalar" -> Value, T
//	"string" -> Value, T
//	"error"  -> Err
type Response struct {
	ID   int64  `json:"id"`
	Kind string `json:"kind"`

	Series  []Series `json:"series,omitempty"`
	Samples []Sample `json:"samples,omitempty"`

	// Value holds a scalar or string result; T its timestamp.
	Value string `json:"value,omitempty"`
	T     int64  `json:"t,omitempty"`

	// Err is set when the query failed. Comparison treats "both
	// implementations errored" as a match, so the message text is
	// informational.
	Err      string   `json:"err,omitempty"`
	Warnings []string `json:"warnings,omitempty"`
}

// Series is one matrix row. Emitted sorted by label set.
type Series struct {
	Labels map[string]string `json:"labels"`
	Floats []Point           `json:"floats,omitempty"`
	// Histograms counts native-histogram points, which this protocol
	// does not encode yet. A non-zero count tells the Rust side to
	// treat the case as unsupported rather than compare a truncated
	// result and call it a mismatch.
	Histograms int `json:"histograms,omitempty"`
}

// Sample is one vector element.
type Sample struct {
	Labels    map[string]string `json:"labels"`
	T         int64             `json:"t"`
	V         string            `json:"v"`
	Histogram bool              `json:"histogram,omitempty"`
}

// Point is one float sample of a series.
type Point struct {
	T int64  `json:"t"`
	V string `json:"v"`
}
