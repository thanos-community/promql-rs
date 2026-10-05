# promql-rs: PromQL in Rust with Arrow & DataFusion

- **Event.** PromCon 2026, lightning talk.
- **Speaker.** Matthias Loibl.
- **Length.** 6:25 at 140 wpm, to be trimmed after a test run; ten slides. Each slide's budget is its Say at 140 words per minute, rounded up to 5 s.
- **Headline.** A series is an Arrow row, and the engine is DataFusion aggregates over that row.
- **Second beat.** The engine never holds a whole series, only the window a step reaches, so memory is bounded by the block.

Each slide has three parts. "On screen" is what the slide generator builds. "Say" is the transcript, and its heading carries its word count. Cues in square brackets, such as `[click 1]`, mark when to click and are neither spoken nor counted. "Notes" holds constraints and open questions for that slide.

## 0. Opener · 0:00, 15 s

### On screen

- A fake title slide, same typography as slide 1: "(Vibe) Rewriting Prometheus in Rust", Matthias Loibl, Dash0.
- One press: the fake title is struck through and the real title of slide 1, promql-rs: PromQL in Rust with Arrow & DataFusion, slides up in its place. This is the one GSAP transition that does narrative work; it is the only animation on slides 0 and 1.

### Say (26 words)

Hi, I'm Matthias. The Prometheus community has joked about rewriting it in Rust for years. So here is my talk on vibe rewriting Prometheus in Rust.

### Notes

- The rewrite-in-Rust joke is a long-running one inside the Prometheus community; the room owns it, so it needs no defusing. Slide 2 opens with the serious turn: the grammar is Prometheus's own and the spec is Prometheus's own test corpus.
- The fake title echoes the Bun post "Bun in Rust" (https://bun.com/blog/bun-in-rust), from the trend of rewriting Zig projects in Rust, and the years-old Prometheus community joke about rewriting Prometheus in Rust.
- The affiliation here follows the same title-slide exception as slide 1.

## 1. Title · 0:15, 15 s

### On screen

- Title: promql-rs: PromQL in Rust with Arrow & DataFusion
- Matthias Loibl, Dash0
- github.com/thanos-community/promql-rs

### Say (27 words)

I have five minutes and one sentence for you. A series is an Arrow row, and the engine is DataFusion aggregates over that row. This is promql-rs.

### Notes

- The repo URL is confirmed as github.com/thanos-community/promql-rs.
- The affiliation is the one place Dash0 appears outside slides 2 and 9. The brief asks for it on the title, and the Decisions rule below names the exception.

## 2. Origin and why · 0:30, 50 s

### On screen

Seven steps. The current step is large; the steps before it in the same group shrink into a compact list above it. Steps 1 to 5 bring one point each under the heading "Origin"; at step 6 the heading "Why" replaces the Origin list. Steps 6 and 7 bring several points at once.

- 2.1 Started at Polar Signals
- 2.2 Great Lakes, a columnar observability store
- 2.3 Dash0 acquired Polar Signals; we need a PromQL engine on Great Lakes at Dash0
- 2.4 Parser grammar generated from the Prometheus yacc grammar, not hand-rolled
- 2.5 Talked to the community and got in contact with people from Cloudflare, Reddit, Shopify and Roku for a first meeting. Two rows of circular avatars appear, each with the GitHub login beneath and no company labels anywhere. The main row is the first community meeting (`deck/src/data/meeting.json`): Ben Kochie, Filip Petkovski, Frederic Branczyk, Matthias Loibl, Mengnan, Michael Hoffmann, Neven Miculinic, Thor Hansen and Wiard van Rij. Mengnan has no verified GitHub login and is left off the row. Beneath it, a smaller row titled "contributing since" shows the repo contributors who were not in the meeting (`deck/src/data/contributors.json`).
- 2.6 Two points:
  - Great Lakes is a column store: Vortex in object storage
  - The Go engine decodes every sample into a point struct
- 2.7 Three points:
  - An engine that speaks Arrow reads the store's format directly
  - DataFusion brings the planner, parallelism and memory accounting
  - Goal: Rust and performance

### Say (114 words)

[click 1] Polar Signals started this engine. [click 2] We built it for Great Lakes, a columnar observability store. [click 3] Dash0 acquired Polar Signals, and we need a PromQL engine on Great Lakes. [click 4] The parser isn't hand-rolled. Its grammar is generated from the Prometheus yacc grammar, so it parses what Prometheus parses, and a tool flags the rules it misses. [click 5] We talked to the community, and had first meetings with people from Cloudflare, Reddit, Shopify and Roku.

[click 6] Great Lakes stores Vortex in object storage. The Go engine still decodes every sample into a point struct. [click 7] An engine that speaks Arrow reads the store's format directly. It borrows DataFusion's planner, parallelism and memory accounting. Rust and performance were the goal.

### Notes

- The parser claim stays honest because of the second half of the sentence. README.md says the parser still misses native-histogram descriptors, duration arithmetic, and the anchored and smoothed selectors, and `promql-sync generate-grammar` prints every upstream alternative without a Rust action.
- The sources do not say what the conversations with Cloudflare, Reddit, Shopify and Roku were about, so the transcript states only that they happened. The speaker may add one clause.
- No Thanos lineage and no other engine by name, on this slide or any other.
- Performance appears only as a goal.
- The Why points come in two steps, the problem and then the answer, so the Why half takes two clicks instead of five.

## 3. Two architectures · 1:20, 55 s

### On screen

- Two columns of boxes, top to bottom, rows aligned so the two parsers sit side by side. No code in the diagram itself.
- Left, "Prometheus (Go)": PromQL text, parser, `promql.Engine`, `rangeEval` looping over the steps, `storage.Querier` handing out series iterators. Beside the bottom box: a series is a `promql.Series`, labels and a slice of points.
- Right, "promql-rs": PromQL text, parser from the same grammar, planner, DataFusion logical plan with its aggregates drawn stacked, physical plan, `SeriesSource::select` returning a plan that streams Arrow `RecordBatch`es. Beside the bottom box: a series is one Arrow row.
- Arrows point down for calls and up for data. On the right every upward arrow is labelled `RecordBatch`.
- One press: the `SeriesSource::select` box gets an orange ring and the rest of the diagram dims. A callout appears beside the box, over the dimmed Go column: `trait SeriesSource` and its method `select` in three lines of monospace, the line "one trait, your store", and two implementations, "in-memory store (the test suite)" and "Thanos Store API client: queries a real Thanos today".

### Say (122 words)

On the left is the Go engine you know. The parser hands an AST to promql.Engine. rangeEval walks the steps, and the selectors pull samples through storage.Querier iterators. A series is labels and a slice of points.

On the right is promql-rs. Same grammar. A planner turns the AST into a DataFusion logical plan of stacked aggregates, and DataFusion makes the physical plan. At the bottom, a store implements one trait, SeriesSource, and returns a plan that streams Arrow RecordBatches. A series is one row, from the store to the result.

[click 1] The seam is one trait, SeriesSource. Implement it, like a Go interface, and your store runs PromQL. I have a Thanos Store API client doing that against a real Thanos today.

### Notes

- Check the Go names against the pinned Prometheus commit in `promql-conformance/testdata/prometheus/UPSTREAM.md`, not a local checkout's HEAD.
- The right column's middle boxes reuse the plan shapes of slide 5, so the two slides read as one picture.

## 4. A series is an Arrow row · 2:15, 30 s

### On screen

- The schema, copied from `docs/engine-blocks.md` section 1:

  ```
  labels       Struct<__name__: Utf8View, ...>
  samples      List<Struct<timestamp: Timestamp(Millisecond), value: Float64>>
  block_start  Timestamp(Millisecond)
  block_end    Timestamp(Millisecond)
  ```

- Below it, the four rows of `x` from the block-edge fixture in `docs/engine-blocks.md` section 4, in seconds as on the walkthrough page. The third row is highlighted.

  | labels | samples | block_start | block_end |
  |---|---|---|---|
  | `{pod="a"}` | 300 330 … 420 450 (6) | 240 | 480 |
  | `{pod="b"}` | 150 180 … 420 450 (11) | 240 | 480 |
  | **`{pod="a"}`** | **300 330 … 570 600 (11)** | **480** | **720** |
  | `{pod="b"}` | 180 210 … 570 600 (15) | 480 | 720 |

### Say (60 words)

This is the whole data model, four columns. Labels are a struct with one field per label name. Samples are a list of timestamp and value. block_start and block_end name the block, and so the steps this row answers. The highlighted row is one chunk of series a in block two. In two minutes you'll watch the engine fold it.

### Notes

- `series_id`, the optional fifth column of keyed mode (`docs/series-source.md`, *The series batch*), stays off the slide. The talk shows sorted mode only.
- The highlighted row is the row `2·a` that slide 6 folds, so the audience sees it twice.

## 5. Plan explorer demo · 2:45, 55 s

### On screen

- A query field with three preset buttons, a plan pane below it, and a Logical / Physical toggle that appears with the third query only. Plan text comes from the real planner at build time; the shapes below are the current pins.
- Click 1, `http_requests_total`. Shape from `promql-engine/tests/testdata/plans/selectors.yaml`:

  ```
  Projection: labels, samples, block_start, block_end
    Aggregate: vector_selector(samples, 600000..1200000 step 30s, lookback 5m)
      TableScan: http_requests_total
  ```

- Click 2, `rate(http_requests_total[5m])`. Shape from `functions.yaml` and `aggregators.yaml`:

  ```
  Projection: labels{job, pod}
    Aggregate: rate(samples[5m], 600000..1200000 step 30s)
      TableScan: http_requests_total
  ```

- Click 3, `sum by (job) (rate(http_requests_total[5m]))`. Pinned in `aggregators.yaml` as "grouped aggregation over a range function":

  ```
  Projection: labels{job: __group__job}
    Aggregate: sum by (job) (samples, 600000..1200000 step 30s)
      Projection: labels{job, pod}
        Aggregate: rate(samples[5m], 600000..1200000 step 30s)
          TableScan: http_requests_total
  ```

- Click 4, the Physical toggle. Abridged, shape from the four-partition pins in `aggregators.yaml`, except the scan line, which shows a Vortex file read and wraps under its node; hovering a line shows it in full. The three `AggregateExec` lines and the `RepartitionExec` hash key are highlighted:

  ```
  ProjectionExec
    AggregateExec: mode=FinalPartitioned, gby=[block_start, block_end, __group__job]
      RepartitionExec: partitioning=Hash([block_start, block_end, __group__job]), preserve_order=true
        AggregateExec: mode=Partial, gby=[block_start, block_end, __group__job]
          ProjectionExec
            AggregateExec: mode=SinglePartitioned, gby=[block_start, block_end, labels], ordering_mode=Sorted
              SeriesSetExec
                DataSourceExec: file_groups={1 group: [[dataset=default/…/test.vortex]]}, file_type=vortex, predicate: timestamp >= … AND timestamp < …
  ```

### Say (123 words)

[before click 1] These plans come from the real planner.

[click 1] A bare selector. One scan, and above it one aggregate, vector_selector, grouped per series and block. Picking the last sample in the lookback is an aggregate over the samples list.

[click 2] Now rate. The range selector has no node of its own. It's rate's input, and rate is the aggregate over the same scan.

[click 3] Now sum by job. A second aggregate stacks on top, grouped by block and job. Every PromQL operator here is a DataFusion aggregate, and the query is those aggregates stacked.

[click 4] And the physical plan. DataFusion's optimizer made this split, not my planner. A Partial in each store partition, a repartition on the block columns and job that keeps block order, and a FinalPartitioned.

### Notes

- The brief's line to land was "three PromQL layers became three stacked DataFusion aggregates". The pins disagree. The range selector is rate's input, so click 3 shows two `Aggregate` nodes over one `TableScan`. The physical plan has three `AggregateExec`s, but two of them are DataFusion's split of `sum`. The transcript lands "every PromQL operator is a DataFusion aggregate" and "DataFusion split the sum on its own" instead.
- No pin holds the physical plan of this exact query. The closest is "grouped aggregation over chunks in four partitions", `sum by (pod) (rate(x[5m]))`. The deck must plan against a source with several partitions, or the toggle shows no split.
- The range `600000..1200000 step 30s` is the pin files' default. The deck's build picks its own and prints whatever the planner returns.
- Plan text is generated by `promql-engine/tests/talk_plans.rs` into `deck/src/data/plans.json` against a four-partition source, so the physical toggle shows the split.
- Everything above the scan line is generated by the planner. The scan line is taken from a Vortex-backed store's plan on a test file, kept verbatim in `deck/src/data/vortex-scan.json`, and substituted because the in-memory test store reads no files: its own leaf says only `DataSourceExec: partitions=4`.

## 6. Block-edge step-through demo · 3:40, 60 s

### On screen

- The step-through from `docs/engine-blocks.html`, section "Watch one query cross a block edge": the fold animation (`#fold-svg`) on top, the Arrow buffer inspector (`#arrow-svg`) below, the frame counter and both captions visible.
- `sum(rate(x[5m]))` at step 30 s. Blocks every 240 s: block 1 answers 420 s and 450 s, block 2 answers 480 s to 600 s.
- Play is hidden. Step → is the only control used. ← Step and Restart stay for recovery.
- Opens on frame 5 / 19, "push 1·b to 420 s".
- Press 1, frame 6 / 19, "push 1·b to 450 s": block 1's last step is final.
- Press 2, frame 7 / 19, "block edge": `sum` emits 420 s and 450 s, drops their partials, opens five for block 2.
- Press 3, frame 8 / 19, "push 2·a to 480 s": the inspector reads cells 17 to 23 of batch C, offsets `[0, 6, 17, 28]`, and copies them into the window buffer. Samples before 480 s are reach-back.
- Press 4, frame 9 / 19, "push 2·a to 510 s": the value falls from 16 to 3.

### Say (136 words)

[before press 1] Now at runtime. sum of rate over two series. The fold is on top, the Arrow buffers below.

[press 1] Series b reaches 450 seconds, block one's last step. sum holds two partials, one per step of this block.

[press 2] Block edge. Series a arrives with block_start 480, so sum emits block one's two steps and the engine drops b's window buffer. Nothing per series crosses.

[press 3] Series a starts from an empty buffer. The inspector follows the offsets to cells 17 to 23 of the timestamp and value arrays. They're copied into the window buffer, so a series can continue into the next batch.

[press 4] The value drops from 16 to 3, a counter reset. rate keeps the 16 for reset_sum. The engine never held a whole series, only the window a step reaches. Memory is bounded by the block.

### Notes

- Status: the step-through port is deferred; the first deck build ships this slide as a static placeholder that names the demo. The batch-crossing question stays open until the author overhauls this slide.
- Frame numbers are replayed from the page's own event list: 19 frames, the block edge at 7, batch D's arrival at 13.
- The page cuts the batch edge before `2·b` on purpose, so at frame 13 no series continues across it, and four presses from frame 5 do not reach it. Press 3 therefore states the copy point from the inspector caption instead of showing a window buffer crossing a batch. If a visible crossing is wanted, the deck's copy of the fixture would split `2·a` across batches C and D. That makes the deck diverge from the page and is an open question.
- The rates the page shows come from its own port of `extrapolatedRate`, not from the engine (`docs/engine-blocks.md`, *Notes on the numbers*). The transcript quotes no rate.

## 7. Five gates · 4:40, 50 s

### On screen

- Left, a ring that builds up clockwise from the top, one gate per press. Each gate appears with the arrow leading into it; the newest gate is filled orange. Conformance is drawn about 1.3 times the size of the others. Gates 4 and 5 are optional: their circles and incoming arrows are dashed and carry a small "optional" tag.
- Right, the heading "Five gates per engine feature" over a detail panel that always describes the gate that just appeared: what the gate checks and its "green:" condition, both quoted from AGENTS.md, *Five gates per engine feature*. Gates 1 and 3 open with a line of the talk's own.
- 7.0 Empty: no gates, no caption, no panel. The centre of the ring stays empty while the ring builds.
- 7.1 Gate 1, Unit. As the circle appears it pulses red, green, red, green, about half a second per colour, a failing unit test fixed twice over, then settles to the orange current-gate fill. Panel, leading in bold: "Test first: the test fails, then the code makes it green." Under it the AGENTS.md quote that backs it, "every new kernel or planner branch has a test that fails without it", then "The steps of `.github/workflows/ci.yml` before the promqltest allowlist check, run locally." green: "all pass". The green line stops there because the rest of AGENTS.md's green sentence is the quote above it.
- 7.2 Gate 2, Conformance, and the arrow from Unit. Panel: "2,098 evals" in large type, read from `total` in `deck/src/data/scoreboard.json`, and the line "Prometheus's own promqltest corpus, the spec the agents check their work against". Then "A lower count is a regression to fix." green: "the gate passes without `BLESS`, and the pass count rose by the evals the feature targets."
- 7.3 Gate 3, Plan pins, and the arrow from Conformance. Panel: "DataFusion logical plans, pinned as text in `tests/testdata/plans/*.yaml`; small cases pin the physical plan too." Then "A changed pin is a design change: re-bless it in its own commit whose message says why the shape moved." green: "every changed pin is explained in that commit."
- 7.4 Gate 4, Benchmarks, dashed and tagged optional, and its dashed arrow from Plan pins. Panel, also tagged optional: "`promql-engine/benches` (`kernels`, `engine`, `memory`)" green: "no move beyond the five percent noise floor in the wrong direction, and the feature has a bench at realistic size."
- 7.5 Gate 5, Profile, dashed and tagged optional, and its dashed arrow from Benchmarks. Panel, also tagged optional: "Run the feature's bench with `--profile-time` (`promql-engine/benches/README.md`) and read the profile." green: "the hot path is the one `docs/engine.md` predicts."
- 7.6 The closing arrow from Profile back to Unit, solid. All five gates turn green (`--ok` on `--ok-bg`; 4 and 5 stay dashed), and as the arrow lands the centre caption "one feature, one loop" fades in. The panel shows one short line under the heading: "a human at the merge". Stepping back to 7.5 hides both.

### Say (110 words)

The opener was half true: the agents write most of the code. [click 1] It holds because every feature walks these five gates in order, each green before the next. Unit is test-first: a failing test before the code. Each gate catches a different failure, so a skipped gate hides which layer broke.

[click 2] Conformance is the gate that makes this work for agents: 2,098 evals from Prometheus itself. An agent checks its work against the real spec, not against its own opinion. [click 3] Gate three pins the DataFusion plans you saw, logical and physical. [click 4] Benchmarks, [click 5] then the profile. Those two are optional; the first three are not. [click 6] Then a human reviews and merges.

### Notes

- AGENTS.md, *Five gates per engine feature*, at the repository root, is the source of the gate order and of every quoted line; keep the quotes verbatim when AGENTS.md changes. It gives no separate "what it catches" line per gate, so each panel quotes the sentence of that gate's entry that says what it checks.
- The 7.1 pulse is the only colour animation before the closing frame. It settles back to the current-gate fill so that 7.2 onward look as they would without it and the ring's first green is the one at 7.6. It plays when a step lands on 7.1, forward or back; a jump to 7.1 shows the settled state.
- Gates 4 and 5 being optional is the author's decision for this talk, not what AGENTS.md says: there every gate applies, and "A pull request that stops before gate 5 names the gate it reached and stays a draft." That line stays off the slide because it contradicts the optional marking.
- The numbers come from `deck/src/data/scoreboard.json`: the 2,098 on screen is its `total`, the field slide 8 shows, so the two slides cannot disagree. The Say quotes the current value and needs the same edit when the corpus changes.
- Every plan pin holds the logical plan; only small cases also hold the physical plan, because partition counts and repartitioning make larger physical plans too fragile to hold as text (`promql-engine/tests/plan.rs`). The 7.3 panel says so; the Say's "logical and physical" rests on those small cases.
- "The opener" in the Say is slide 0's struck title, "(Vibe) Rewriting Prometheus in Rust". The plan pins of gate three are the pins whose shapes slide 5 shows.

## 8. Scoreboard · 5:30, 30 s

### On screen

- A chart of conformance progress, captioned "promqltest evals passing, per commit" (source: `deck/src/data/conformance-history.json`, from the committed history of `promql-conformance/testdata/prometheus/UNSUPPORTED.md`). The 2,098 evals come from 20 vendored `.test` files (source: `promql-conformance/tests/promqltest.rs`).
  - X axis: the commits that re-blessed the allowlist, oldest on the left, evenly spaced, with a date tick at each day's first commit, not one per commit.
  - Y axis: 0 to the corpus total, with a dashed line at the total, 2,098. The line steps if the total changes between commits; so far it never has.
  - The passing count in orange, one point per commit, from 267 at the first gated run to 559 at the last. The last point carries "559 of 2,098" in large type.
  - On entering the slide the line draws left to right; jumping straight to #8 shows the finished chart.
- Beside the chart, missing features that block the most evals (source: `UNSUPPORTED.md`, *Missing features*):
  - `histogram_quantile`, 86 evals
  - subqueries, 57 evals
  - `histogram_fraction`, 56 evals
- Below that, the CI rule from the allowlist `promql-conformance/testdata/prometheus/SUPPORTED.toml` (source: `README.md`, *Status*): a listed eval that stops passing turns CI red, and so does an unlisted eval that starts passing.

### Say (64 words)

When CI started gating on promqltest, 267 of its 2,098 evals passed; today 559 do. UNSUPPORTED.md, generated from a real run, names what blocks the most: histogram_quantile with 86 evals, subqueries with 57, histogram_fraction with 56. CI gates on an allowlist, SUPPORTED.toml. A listed eval that stops passing turns CI red. So does an unlisted eval that starts passing, so new coverage gets declared.

### Notes

- Every number on this slide is generated at build time from `UNSUPPORTED.md` and its git history. The figures above are the current values: 559 of 2,098 passing, and 465 evals blocked on missing features (source: `UNSUPPORTED.md`).
- The chart's points come from `deck/scripts/conformance-history.py`. It walks every commit from HEAD that touched `SUPPORTED.toml` or `UNSUPPORTED.md`, and reads the count that commit's `UNSUPPORTED.md` states, cross-checked against its tables. It walks every commit, not `--first-parent`: the blessing commits form one ancestry chain across the stacked PRs #15, #38, #33, #35, #36 and #41, while a merge-only walk would fold several blessings into one point. That gives ten points from 2026-09-22 to 2026-10-04, dated by commit date, with none left out.
- The first point, 267, is the commit that added the promqltest allowlist check to CI, which is what the Say means by "when CI started gating".
- The scoreboard numbers are produced by `deck/scripts/scoreboard.py` into `deck/src/data/scoreboard.json`. Slide 7's eval count reads the same `total`.
- `native_histograms` alone is 521 evals with 0 passing (source: `UNSUPPORTED.md`, *By file*), and native histograms are a stated non-goal (`docs/engine.md`). It explains a quarter of the gap if a question comes up. It stays off the slide to keep the time.

## 9. Close · 6:00, 25 s

### On screen

- github.com/thanos-community/promql-rs
- `docs/engine-blocks.html`: the walkthrough with the step-through, opens from disk
- `docs/talks/promcon-2026-lightning/`: these slides
- Try it: clone, then `cargo test -p promql-conformance --test promqltest` prints the scoreboard
- Runs at Dash0 as the PromQL layer on Great Lakes
- Thanks

### Say (56 words)

The repo is on GitHub. The walkthrough you just saw and these slides live in docs, and they open from disk. To try it, clone it and run the promqltest suite, which prints the scoreboard. Dash0 runs this engine on top of Great Lakes. If you run PromQL over a columnar store, find me afterwards. Thanks.

### Notes

- The repository has no binary that runs a query, so "try it" means the conformance suite and the walkthrough page.
- The repo URL is confirmed as github.com/thanos-community/promql-rs.

## Decisions

- The deck is committed under `docs/talks/promcon-2026-lightning/`: `TALK.md` is the source of structure and transcript; `deck/` holds a Vite + GSAP build with a hand-written slide controller (no reveal.js, no Lenis); `deck/dist/index.html` is the single-file build, committed, and opens from file://.
- A local server (`deck/serve.sh`) is used on stage for the speaker window only; the slides themselves need no server.
- No CDN or network fetch at runtime; fonts and libraries are vendored and inlined.
- Branding follows prometheus.io: Lato (SIL OFL, vendored) and the Prometheus palette, defined once in `deck/src/theme.css`.
- Demo code is copied from `docs/engine-blocks.html`, not shared with it; the step-through port is deferred.
- Polar Signals, Dash0 and Great Lakes appear only on slides 2 and 9, plus the speaker affiliation on the title slide.
- No performance numbers anywhere; Rust and performance appear only as a goal.
- Plan text and scoreboard numbers are generated at build time by `deck/scripts/gen-data.sh` from the real planner and `UNSUPPORTED.md`, except the physical scan line on slide 5, which is the Vortex line kept in `deck/src/data/vortex-scan.json`.
