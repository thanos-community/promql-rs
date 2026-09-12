// Command gen-xor-fixtures writes XOR chunks produced by Prometheus's own
// chunkenc package together with the samples that went in, so the Rust
// decoder in thanos-store is checked against the reference encoder.
//
//	cd scripts/gen-xor-fixtures && go run . > ../../thanos-store/testdata/xor_chunks.json
//
// Values are written as the 16 hex digits of their float64 bits, because
// JSON has no NaN or infinity and the stale marker is a NaN payload.
package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"

	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type sample struct {
	T int64  `json:"t"`
	V string `json:"v"`
}

type fixture struct {
	Name    string   `json:"name"`
	Hex     string   `json:"hex"`
	Samples []sample `json:"samples"`
}

type point struct {
	t int64
	v float64
}

func build(name string, pts []point) fixture {
	c := chunkenc.NewXORChunk()
	app, err := c.Appender()
	if err != nil {
		panic(err)
	}
	samples := make([]sample, 0, len(pts))
	for _, p := range pts {
		app.Append(p.t, p.v)
		samples = append(samples, sample{T: p.t, V: fmt.Sprintf("%016x", math.Float64bits(p.v))})
	}
	return fixture{Name: name, Hex: hex.EncodeToString(c.Bytes()), Samples: samples}
}

// regular is n samples starting at t0, step apart, valued by f.
func regular(n int, t0, step int64, f func(i int) float64) []point {
	pts := make([]point, 0, n)
	for i := 0; i < n; i++ {
		pts = append(pts, point{t0 + int64(i)*step, f(i)})
	}
	return pts
}

// fromDeltas is one sample at t0 and one more per delta, all valued 1.
func fromDeltas(t0 int64, deltas ...int64) []point {
	pts := []point{{t0, 1}}
	t := t0
	for _, d := range deltas {
		t += d
		pts = append(pts, point{t, 1})
	}
	return pts
}

func main() {
	fixtures := []fixture{
		build("empty", nil),
		build("one", []point{{1000, 1}}),
		build("two", []point{{1000, 1}, {16000, 2}}),
		build("three_same_delta", []point{{1000, 1}, {16000, 2}, {31000, 3}}),
		build("regular_120_counter", regular(120, 1_700_000_000_000, 15000, func(i int) float64 { return float64(i*i) * 0.5 })),
		build("constant_values", regular(50, 0, 15000, func(int) float64 { return 42.5 })),
		build("gauge_drift", regular(64, 0, 15000, func(i int) float64 { return 100 + math.Sin(float64(i)/7)*3 })),
		// Every delta-of-delta prefix code, both signs, plus a negative start.
		build("dod_buckets", fromDeltas(-5000,
			15000,
			15100,      // +100      -> 14 bits
			14900,      // -200      -> 14 bits
			8192+14900, // +8192     -> top of the 14-bit range
			55000,      // +31908    -> 17 bits
			14900,      // -40100    -> 17 bits
			415000,     // +400100   -> 20 bits
			14900,      // -400100   -> 20 bits
			1<<40,      // huge      -> 64 bits
			15000,      // -(1<<40)  -> 64 bits
			15000,      // 0
			15000,      // 0
		)),
		// XOR with 64 significant bits is written as a sigbits of 0.
		build("sigbits_64", []point{{0, 0}, {1000, math.Float64frombits(0x8000000000000001)}, {2000, 0}}),
		// XOR of 1: 63 leading zeros, clamped to 31 by the encoder.
		build("leading_clamp", []point{{0, 1}, {1000, math.Float64frombits(math.Float64bits(1) ^ 1)}, {2000, 1}}),
		// Shrinking XORs reuse the previous leading/trailing counts.
		build("reuse_leading_trailing", []point{{0, 1}, {1000, 1.5}, {2000, 1.25}, {3000, 1.125}, {4000, 1.0625}, {5000, 1.0625}}),
		build("special_values", []point{
			{0, math.NaN()},
			{1000, math.Float64frombits(0x7ff8000000000001)},
			{2000, math.Float64frombits(value.StaleNaN)},
			{3000, math.Inf(1)},
			{4000, math.Inf(-1)},
			{5000, 0},
			{6000, math.Copysign(0, -1)},
			{7000, 1},
			{8000, math.MaxFloat64},
			{9000, math.SmallestNonzeroFloat64},
		}),
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(fixtures); err != nil {
		panic(err)
	}
}
