# promql-rs: PromQL in Rust with Arrow & DataFusion

- **Event.** PromCon 2026, lightning talk.
- **Speaker.** Matthias Loibl.
- **Length.** 4:50; ten slides, slide 4 hidden. A timed run of the ten-slide draft took 6:50 for 845 spoken words, about 124 words per minute with clicks and pauses, so each slide's budget is its Say at 124 words per minute, rounded up to 5 s.
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

## 1. Title · 0:15, 10 s

### On screen

- Title: promql-rs: PromQL in Rust with Arrow & DataFusion
- Matthias Loibl, Dash0
- github.com/thanos-community/promql-rs

### Say (15 words)

I have five minutes for you. This is promql-rs: PromQL built on Arrow and DataFusion.

### Notes

- The repo URL is confirmed as github.com/thanos-community/promql-rs.
- The affiliation is the one place Dash0 appears outside slide 2. The brief asks for it on the title, and the Decisions rule below names the exception.
- The headline sentence at the top of this file is not said here: it is too deep for a cold audience. Slide 3's "a series is one row" and slide 5's "every PromQL operator here is a DataFusion aggregate" carry its two halves.

## 2. Origin and why · 0:25, 40 s

### On screen

Nine steps, one point each. The current point is large; the points before it in the same group shrink into a compact list above it. Steps 1 to 5 bring the points under the heading "Origin"; at step 6 the heading "Why" replaces the Origin list, and steps 6 to 9 bring the Why points the same way.

- 2.1 Started at Polar Signals
- 2.2 Great Lakes, a columnar observability store written in Rust
- 2.3 Dash0 acquired Polar Signals; we need a PromQL engine on Great Lakes at Dash0
- 2.4 Parser grammar generated from the Prometheus yacc grammar, not hand-rolled
- 2.5 Talked to the community and got in contact with people from Cloudflare, Reddit, Shopify and Roku for a first meeting. Two rows of circular avatars appear, each with the GitHub login beneath and no company labels anywhere. The main row is the first community meeting (`deck/src/data/meeting.json`): Ben Kochie, Filip Petkovski, Frederic Branczyk, Matthias Loibl, Mengnan, Michael Hoffmann, Neven Miculinic, Thor Hansen and Wiard van Rij. Mengnan has no verified GitHub login and is left off the row. Beneath it, a smaller row titled "contributing since" shows the repo contributors who were not in the meeting (`deck/src/data/contributors.json`).
- 2.6 Great Lakes is a column store: Vortex in object storage, Arrow in memory
- 2.7 The Go engine decodes every sample into a point struct
- 2.8 An engine that speaks Arrow reads the store's format directly
- 2.9 DataFusion brings the planner, parallelism and memory accounting

### Say (78 words)

[click 1] Polar Signals started this engine. [click 2] We built it for Great Lakes, a columnar store in Rust. [click 3] Dash0 acquired Polar Signals; we need PromQL on Great Lakes. [click 4] Our parser is generated from Prometheus's yacc grammar. [click 5] We met people from Cloudflare, Reddit, Shopify and Roku.

[click 6] Great Lakes stores Vortex in object storage, Arrow in memory. [click 7] Prometheus decodes every sample into a point struct. [click 8] An engine that speaks Arrow reads Great Lakes directly. [click 9] DataFusion brings the planner, parallelism and memory accounting.

### Notes

- The Say claims only where the grammar comes from, not that the parser handles everything Prometheus parses. README.md says the parser still misses native-histogram descriptors, duration arithmetic, and the anchored and smoothed selectors, and `promql-sync generate-grammar` prints every upstream alternative without a Rust action.
- The sources do not say what the conversations with Cloudflare, Reddit, Shopify and Roku were about, so the transcript states only that they happened. The speaker may add one clause.
- No Thanos lineage and no other engine by name, on this slide or any other.
- Each Why point is its own press, as each Origin point is; two or three landing at once looked broken next to the Origin half.

## 3. Two architectures · 1:05, 40 s

### On screen

- Two columns of boxes, top to bottom, rows aligned so the two parsers sit side by side. No code in the diagram itself.
- Left, "Prometheus (Go)": PromQL text, parser, `promql.Engine`, `rangeEval` looping over the steps, `storage.Querier` handing out series iterators. Beside the bottom box: a series is a `promql.Series`, labels and a slice of points.
- Right, "promql-rs": PromQL text, parser from the same grammar, planner, DataFusion logical plan with its aggregates drawn stacked, physical plan, `SeriesSource::select` returning a plan that streams Arrow `RecordBatch`es. Beside the bottom box: a series is one Arrow row.
- Arrows point down for calls and up for data. On the right every upward arrow is labelled `RecordBatch`.
- One press: the `SeriesSource::select` box gets an orange ring and the rest of the diagram dims. A callout appears beside the box, over the dimmed Go column: `trait SeriesSource` and its method `select` in three lines of monospace, the line "one trait, your store", and two implementations, "in-memory store (the test suite)" and "Thanos Store API client: queries a real Thanos today".

### Say (81 words)

On the left, the Go engine you know. A series is labels and a slice of points.

On the right, promql-rs. Same grammar, then a DataFusion plan of stacked aggregates over a store that streams Arrow RecordBatches. A series is one row, from the store to the result.

[click 1] The seam is SeriesSource: one trait, your store. Implement it, like a Go interface, and your store runs PromQL. I have a Thanos Store API client doing that against a real Thanos today.

### Notes

- Check the Go names against the pinned Prometheus commit in `promql-conformance/testdata/prometheus/UPSTREAM.md`, not a local checkout's HEAD.
- The right column's middle boxes reuse the plan shapes of slide 5, so the two slides read as one picture.

## 4. Blocks in Arrow · 1:45, 0 s

Hidden: skipped in the talk for now; the step-through carries the block columns. The slide stays in the file and the deck, at 0 s, and its words are left out of the total.

### On screen

- Headline "Blocks in Arrow", top left, styled and placed as slide 2's "Origin".
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

## 5. Plan explorer demo · 1:45, 45 s

### On screen

- Headline "PromQL as DataFusion plans", top left, styled and placed as slide 2's "Origin".
- Below it, a query field with three preset buttons, a plan pane below it, and a Logical / Physical toggle that appears with the third query only. Plan text comes from the real planner at build time; the shapes below are the current pins. The pane takes the height left under the headline, and the longest plan, the abridged physical plan of click 4 with its wrapped scan line, fits without scrolling.
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

### Say (83 words)

[before click 1] These plans come from the real planner.

[click 1] A bare selector: one scan, and one aggregate, vector_selector, over each series' samples.

[click 2] Now rate. The range selector has no node of its own; rate is the aggregate over the same scan.

[click 3] Now sum by job. A second aggregate stacks on top: every PromQL operator here is a DataFusion aggregate.

[click 4] And the physical plan. DataFusion's optimizer made this split, not my planner: a Partial per store partition, a repartition on the highlighted key, and a FinalPartitioned.

### Notes

- The brief's line to land was "three PromQL layers became three stacked DataFusion aggregates". The pins disagree. The range selector is rate's input, so click 3 shows two `Aggregate` nodes over one `TableScan`. The physical plan has three `AggregateExec`s, but two of them are DataFusion's split of `sum`. The transcript lands "every PromQL operator is a DataFusion aggregate" and "DataFusion split the sum on its own" instead.
- No pin holds the physical plan of this exact query. The closest is "grouped aggregation over chunks in four partitions", `sum by (pod) (rate(x[5m]))`. The deck must plan against a source with several partitions, or the toggle shows no split.
- The range `600000..1200000 step 30s` is the pin files' default. The deck's build picks its own and prints whatever the planner returns.
- Plan text is generated by `promql-engine/tests/talk_plans.rs` into `deck/src/data/plans.json` against a four-partition source, so the physical toggle shows the split.
- The Say never says "block": with slide 4 hidden, slide 6 introduces the block columns, and here the physical plan shows them in the highlighted key.
- Everything above the scan line is generated by the planner. The scan line is taken from a Vortex-backed store's plan on a test file, kept verbatim in `deck/src/data/vortex-scan.json`, and substituted because the in-memory test store reads no files: its own leaf says only `DataSourceExec: partitions=4`.

## 6. Block-edge step-through demo · 2:30, 55 s

### On screen

- Headline "Row by row, block by block", top left, styled and placed as slide 2's "Origin". Top right, `sum(rate(x[5m]))` and "step 30 s · one store partition".
- Below it, one drawing at stage scale. It runs wider than the other slides' text column, so the 28 timestamp cells of batch C fit at 28 px.
  - Top: RecordBatch C and RecordBatch D as Arrow arrays. `labels.pod`, `block_start` and `block_end` hold one cell per row. `offsets` sit on the boundaries they define, `[0, 6, 17, 28]` in C and `[0, 15]` in D, over the flat `timestamp` and `value` child buffers, 28 cells in C and 15 in D. The rows 1·a, 1·b and 2·a in C and 2·b in D are slide 4's four rows. Until click 4, D is a dashed outline, "not arrived yet". A caption box right of D names what the current click shows.
  - Bottom left, `rate`, "AggregateExec · SinglePartitioned · Sorted": the seven steps 420 s to 600 s under two brackets, block 1 240–480 and block 2 480–720. Below them, the open series' window buffer as timestamp and value cells, with the reach-back cells before `block_start` grey and dashed, and the output row `rate` builds: labels, the block's step values, `block_start` and `block_end`.
  - Bottom right, slide 5's operators top to bottom: `sum` Partial with one slot per step of the open block, the repartition on `Hash(block_start, block_end)`, `sum` FinalPartitioned, and seven output cells.
- 6.0 Nothing read yet. The buffer is empty, the Partial says "no block open", and the output cells are empty.
- 6.1 Row 1·a. Its slice of C, cells 0 to 5, is outlined, and the cells fly down into the window buffer. An orange window sweeps 420 s and 450 s, each step lights up for `a`, and a's output row fills with 0.015 and 0.018.
- 6.2 Row 1·b. Its other labels close `a`: a's output row moves into the Partial, which opens block 1's two slots, 0.015 and 0.018. a's buffer goes. Cells 6 to 16 copy in, 150 s to 210 s grey, 420 s and 450 s sweep, and b's output row fills.
- 6.3 Row 2·a, slide 4's highlighted row. Its `block_start` 480 closes `b`, whose row moves up: the slots read 0.048 and 0.052. Cells 17 to 27 copy into an empty buffer, 300 s to 450 s grey. The sweep runs 480 s to 600 s; at 510 s the value cell 3 turns red and "reset_sum = 16" appears.
- 6.4 RecordBatch D replaces its outline, offsets `[0, 15]`; C dims and is tagged "released". 2·b closes `a`: a's block-2 row moves into the Partial, which emits block 1. The repartition and the Final light in turn, and the output cells for 420 s and 450 s fill with 0.048 and 0.052. Block 2's five slots open with a's rates, and the buffer is empty.
- 6.5 Cells 0 to 14 of D copy in, 180 s to 450 s grey, and 480 s to 600 s sweep. At the end of the stream, b's row moves up, the Partial emits block 2 through the repartition and the Final, and the output completes: 0.055, 0.065, 0.069, 0.074, 0.074. The buffer and the slots end empty.

### Say (108 words)

[before click 1] Now that plan runs over four Arrow rows, each one series in one block, from block_start to block_end.

[click 1] rate copies row one into its buffer and answers 420 and 450, block one's steps.

[click 2] Row two has other labels, so a is done; its answer moves up into sum as one row.

[click 3] Row three is a again, in block two. The drop from 16 to 3 is a reset.

[click 4] Batch D brings its own offsets. When a's block-two row reaches sum, block one is done and goes to the Final.

[click 5] The engine never held a whole series, only the window a step reaches. Memory is bounded by the block.

### Notes

- Ported from `docs/engine-blocks.html`, section "Watch one query cross a block edge": its fixture, its two batches and the order of its events. `deck/src/slides/stepthrough.js` draws them again at stage scale, because the page's text would render at 13 to 18 stage px. The page's one frame per step becomes the sweep inside a click. Its controls, per-step table, lane bars and long captions are not ported.
- The numbers come from the engine. `promql-engine/tests/talk_stepthrough.rs` runs `rate(x[5m])` and `sum(rate(x[5m]))` over the fixture and writes `deck/src/data/stepthrough.json`, run by `deck/scripts/gen-data.sh`. It asserts all 14 rates and 7 sums against `docs/engine-blocks.md` sections 4 and 5 to 1e-10, and that both queries emit one row per block. The slide rounds to three decimals; the transcript quotes no rate.
- The deck differs from the page in when `sum` emits block 1. The page emits it when the store's 2·a arrives. The deck follows slide 5's plan: the Partial sees only rate's output rows, and `rate` emits a's block-2 row only when 2·b closes it, so block 1 leaves `sum` at click 4.
- Block 1 starts at 240 s here and on slide 4, as in a store with fixed 240 s ranges. `MemorySeriesSource`, which the export runs on, starts its first block at the first window end, 420 s. Both blocks answer 420 s and 450 s from the same samples, so the numbers agree, and the export does not compare the block start.
- No series continues across a batch, because the fixture cuts D before 2·b. Click 4 shows D's offsets starting again at 0 instead, and the transcript makes no claim about a crossing.
- The slide shows one store partition's stream. With several, each runs its own `rate` and Partial, and the Final adds their partials per block, as slide 5's physical plan shows.
- With slide 4 hidden, no slide before this one shows the block columns, so the Say's first sentence names them: each row is one series in one block, from `block_start` to `block_end`.
- The headline wording is a proposal.

## 7. Five gates · 3:25, 40 s

### On screen

- Left, a ring that builds up clockwise from the top, one gate per press. Each gate appears with the arrow leading into it; the newest gate is filled orange. Conformance is drawn about 1.3 times the size of the others. Gates 4 and 5 are optional: their circles and incoming arrows are dashed and carry a small "optional" tag.
- Right, from 7.1 on, the heading "Vibe coding, with guardrails" over a detail panel that always describes the gate that just appeared: what the gate checks and its "green:" condition, both quoted from AGENTS.md, *Five gates per engine feature*. Gates 1 and 3 open with a line of the talk's own.
- 7.0 The heading "Vibe coding, with guardrails" alone, super big and centred on the stage, horizontally and vertically: one line, as wide as the slide's padding allows, Lato bold in the accent colour. No gates, no caption, no panel. The centre of the ring stays empty while the ring builds.
- 7.1 Gate 1, Unit. On the press the heading shrinks and moves from the centre to its place top right, over the panel, in one smooth move of about 0.7 s; the panel fades in as the heading lands. Meanwhile the circle appears and pulses red, green, red, green, about half a second per colour, a failing unit test fixed twice over, then settles to the orange current-gate fill. Panel, leading in bold: "Test first: the test fails, then the code makes it green." Under it the AGENTS.md quote that backs it, "every new kernel or planner branch has a test that fails without it", then "The steps of `.github/workflows/ci.yml` before the promqltest allowlist check, run locally." green: "all pass". The green line stops there because the rest of AGENTS.md's green sentence is the quote above it.
- 7.2 Gate 2, Conformance, and the arrow from Unit. Panel: "2,098 evals" in large type, read from `total` in `deck/src/data/scoreboard.json`, and the line "Prometheus's own promqltest corpus, the spec the agents check their work against". Then "A lower count is a regression to fix." green: "the gate passes without `BLESS`, and the pass count rose by the evals the feature targets."
- 7.3 Gate 3, Plan pins, and the arrow from Conformance. Panel: "DataFusion logical plans, pinned as text in `tests/testdata/plans/*.yaml`; small cases pin the physical plan too." Then "A changed pin is a design change: re-bless it in its own commit whose message says why the shape moved." green: "every changed pin is explained in that commit."
- 7.4 Gate 4, Benchmarks, dashed and tagged optional, and its dashed arrow from Plan pins. Panel, also tagged optional: "`promql-engine/benches` (`kernels`, `engine`, `memory`)" green: "no move beyond the five percent noise floor in the wrong direction, and the feature has a bench at realistic size."
- 7.5 Gate 5, Profile, dashed and tagged optional, and its dashed arrow from Benchmarks. Panel, also tagged optional: "Run the feature's bench with `--profile-time` (`promql-engine/benches/README.md`) and read the profile." green: "the hot path is the one `docs/engine.md` predicts."
- 7.6 The closing arrow from Profile back to Unit, solid. All five gates turn green (`--ok` on `--ok-bg`; 4 and 5 stay dashed), and as the arrow lands the centre caption "one feature, one loop" fades in. The panel shows one short line under the heading: "a human at the merge". Stepping back to 7.5 hides both.

### Say (80 words)

The opener was half true: the agents write most of the code. [click 1] Every feature walks five gates in order, each green before the next. Unit is test-first: a failing test before the code.

[click 2] Conformance is what makes this work for agents: 2,098 evals from Prometheus itself. An agent checks its work against the real spec, not its own opinion. [click 3] Plan pins hold the DataFusion plans you saw. [click 4] Benchmarks [click 5] and the profile are optional. [click 6] And there's a human at the merge.

### Notes

- AGENTS.md, *Five gates per engine feature*, at the repository root, is the source of the gate order and of every quoted line; keep the quotes verbatim when AGENTS.md changes. It gives no separate "what it catches" line per gate, so each panel quotes the sentence of that gate's entry that says what it checks.
- Stepping back from 7.1 to 7.0 grows the heading back to the centre; a jump shows it where the steps leave it, big on 7.0 and small from 7.1 on. Its big size is measured from the text, so a reworded heading still fits on one line.
- The 7.1 pulse is the only colour animation before the closing frame. It settles back to the current-gate fill so that 7.2 onward look as they would without it and the ring's first green is the one at 7.6. It plays when a step lands on 7.1, forward or back; a jump to 7.1 shows the settled state.
- Gates 4 and 5 being optional is the author's decision for this talk, not what AGENTS.md says: there every gate applies, and "A pull request that stops before gate 5 names the gate it reached and stays a draft." That line stays off the slide because it contradicts the optional marking.
- The numbers come from `deck/src/data/scoreboard.json`: the 2,098 on screen is its `total`, the field slide 8 shows, so the two slides cannot disagree. The Say quotes the current value and needs the same edit when the corpus changes.
- Every plan pin holds the logical plan; only small cases also hold the physical plan, because partition counts and repartitioning make larger physical plans too fragile to hold as text (`promql-engine/tests/plan.rs`). The 7.3 panel says so.
- "The opener" in the Say is slide 0's struck title, "(Vibe) Rewriting Prometheus in Rust". The plan pins of gate three are the pins whose shapes slide 5 shows.

## 8. Scoreboard · 4:05, 25 s

### On screen

- A chart of conformance progress, captioned "promqltest evals passing, per merged pull request" (source: `deck/src/data/conformance-history.json`, from the committed history of `promql-conformance/testdata/prometheus/UNSUPPORTED.md`). The 2,098 evals come from 20 vendored `.test` files (source: `promql-conformance/tests/promqltest.rs`).
  - X axis, titled "commits": one evenly spaced slot per merge commit on main that changed the allowlist, oldest on the left, each labelled in small type with the number of the PR it merged (`#15` to `#56`), or its short sha when it names none. Labels too wide for their slot alternate between two rows, and past that only every nth slot and the last are labelled. No dates on the axis: a day of many merges would bunch them.
  - Y axis: 0 to the corpus total, with a dashed line at the total, 2,098. The line steps if the total changes between commits; so far it never has.
  - The passing count in orange, one point per merge, from 268 at the first gated merge to 707 at the last. The last point carries "707 of 2,098" in large type.
  - On entering the slide the line draws left to right; jumping straight to #8 shows the finished chart.
- Beside the chart, missing features that block the most evals (source: `UNSUPPORTED.md`, *Missing features*):
  - `histogram_quantile`, 86 evals
  - anchored and smoothed modifiers, 80 evals
  - subqueries, 60 evals
- Below that, the CI rule from the allowlist `promql-conformance/testdata/prometheus/SUPPORTED.toml` (source: `README.md`, *Status*): a listed eval that stops passing turns CI red, and so does an unlisted eval that starts passing.

### Say (46 words)

When CI started gating on promqltest, 268 of its 2,098 evals passed; today 707 do. The biggest blocker is histogram_quantile, with 86 evals. CI gates on an allowlist: a listed eval that stops passing turns CI red, and so does an unlisted eval that starts passing.

### Notes

- Every number on this slide is generated at build time from `UNSUPPORTED.md` and its git history. The figures above are the current values: 707 of 2,098 passing, and 492 evals blocked on missing features (source: `UNSUPPORTED.md`).
- The chart's points come from `deck/scripts/conformance-history.py`. It walks `--first-parent` from HEAD over the commits that changed `SUPPORTED.toml` or `UNSUPPORTED.md`, and reads the count that commit's `UNSUPPORTED.md` states, cross-checked against its tables. Every such commit on main is a pull request's merge, which carries the state it merged, so each point is a state main actually held. A PR's own blessing commits are not points: parallel PRs re-bless after rebasing onto each other, so a branch commit's count can describe a tree main never had. That gives twelve points, PRs #15 to #56, from 2026-09-22 to 2026-10-05, dated by commit date, with none left out. #53 re-blessed without changing the count, so it shows as a flat step.
- The first point, 268, is the merge of #15, which added the promqltest allowlist check to CI; that is what the Say means by "when CI started gating".
- The scoreboard numbers are produced by `deck/scripts/scoreboard.py` into `deck/src/data/scoreboard.json`. Slide 7's eval count reads the same `total`.
- `native_histograms` alone is 521 evals with 0 passing (source: `UNSUPPORTED.md`, *By file*), and native histograms are a stated non-goal (`docs/engine.md`). It is over a third of the 1,391 evals still failing, if a question comes up. It stays off the slide to keep the time.

## 9. Close · 4:30, 20 s

### On screen

- github.com/thanos-community/promql-rs
- `docs/engine-blocks.html`: the walkthrough with the step-through, opens from disk
- `docs/talks/promcon-2026-lightning/`: these slides
- Try it: clone, then `cargo test -p promql-conformance --test promqltest` prints the scoreboard
- Thanks

### Say (34 words)

It's all on GitHub: the code, the walkthrough you just saw, and these slides. Clone it; the promqltest suite prints the scoreboard. If you run PromQL over a columnar store, find me afterwards. Thanks.

### Notes

- The repository has no binary that runs a query, so "try it" means the conformance suite and the walkthrough page.
- The repo URL is confirmed as github.com/thanos-community/promql-rs.

## Decisions

- The deck is committed under `docs/talks/promcon-2026-lightning/`: `TALK.md` is the source of structure and transcript; `deck/` holds a Vite + GSAP build with a hand-written slide controller (no reveal.js, no Lenis); `deck/dist/index.html` is the single-file build, committed, and opens from file://.
- A local server (`deck/serve.sh`) is used on stage for the speaker window only; the slides themselves need no server.
- No CDN or network fetch at runtime; fonts and libraries are vendored and inlined.
- Branding follows prometheus.io: Lato (SIL OFL, vendored) and the Prometheus palette, defined once in `deck/src/theme.css`.
- Demo code is copied from `docs/engine-blocks.html`, not shared with it. Slide 6 copies the page's fixture and event order and redraws them at stage scale in `deck/src/slides/stepthrough.js`, one click per Arrow row.
- Polar Signals, Dash0 and Great Lakes appear only on slide 2, plus the speaker affiliation on the title slides 0 and 1. The close names neither on screen nor in its Say, because the engine does not run at Dash0 yet.
- No performance numbers anywhere.
- Plan text, slide 6's rates and the scoreboard numbers are generated at build time by `deck/scripts/gen-data.sh` from the real planner and engine and `UNSUPPORTED.md`, except the physical scan line on slide 5, which is the Vortex line kept in `deck/src/data/vortex-scan.json`.
