//! How a store hands series to the engine.
//!
//! [`SeriesSource`] mirrors Prometheus's `storage.Querier.Select`, which
//! every PromQL-on-something implementation has ended up writing. The
//! engine asks in PromQL terms — label matchers and a millisecond range —
//! and the store answers with a DataFusion [`ExecutionPlan`] in the
//! [canonical schema](crate::series). What happens inside `select` is the
//! store's business: a parquet scan, a vortex scan, a gRPC call, a
//! `GROUP BY` that nests samples into lists. This crate never looks.
//!
//! Four obligations come with the answer, `docs/series-source.md` being
//! the contract:
//!
//! 1. **Filter.** Every series matches every matcher, and a series whose
//!    whole span misses the range is not sent.
//! 2. **Series.** A series holds one chunk of exactly one label set, in
//!    one block, with at least one sample, samples ascending. A label set
//!    with several chunks in a block arrives once per chunk.
//! 3. **Order.** Over a partition's stream of series, never over a batch,
//!    which may be cut anywhere. All of a block's series come before the
//!    next block's; blocks ascend and do not overlap. Inside a block,
//!    series are sorted by label set in DataFusion's struct order: label
//!    fields by name, compared one at a time, an absent label as `""`.
//!    That is not Prometheus's `labels.Compare` (`{b="1"}` precedes
//!    `{a="1"}` here), because `labels ASC` is only a true declaration in
//!    the order DataFusion itself compares in. The chunks of one label set
//!    are consecutive, never cross a partition, and ascend by first sample
//!    timestamp.
//! 4. **Blocks.** The store cuts time. Every series carries its block's
//!    `block_start` and `block_end`; a block answers the steps whose
//!    window end lies in `[block_start, block_end)`, and its series hold
//!    every sample those windows reach, which the store learns from
//!    `SelectHints::window_ms`. Blocks are contiguous over the select's
//!    window ends.
//!
//! The engine trusts the first two and checks the rest, one label
//! comparison per series, in [`SeriesSetExec`]: an operator that closes a
//! series at the next label set or block would otherwise answer wrong
//! without noticing. Keyed mode, where a `series_id` column replaces the
//! label order, is not implemented yet: the schema check refuses it.
//!
//! [`SelectorTable`] then makes a `select` result look like an ordinary
//! table to DataFusion, so a selector is a `TableScan` leaf and everything
//! above it — `EXPLAIN`, the optimizer, later operators — is stock.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use datafusion::arrow::array::{make_comparator, Array, AsArray, RecordBatch, StructArray};
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{SchemaRef, TimestampMillisecondType};
use datafusion::arrow::row::{OwnedRow, RowConverter, SortField};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::DFSchemaRef;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionState;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{
    Expr, Extension, LogicalPlan, TableType, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, Statistics,
};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use futures::{Stream, StreamExt};
use promql_parser::ast::LabelMatcher;

use crate::error::EngineError;
use crate::series::{self, Block, BLOCK_END, BLOCK_START, LABELS, SAMPLES, TIMESTAMP};

/// What the engine wants from a scan, beyond the matchers: Prometheus's
/// `storage.SelectHints`, typed, plus the window.
///
/// The range and the window are binding. Both bounds are inclusive
/// milliseconds and already account for lookback, `offset`, `@` and range
/// windows, so the store does not need to know any PromQL to honour them.
/// Everything else is advisory: a store may use it to read less or to lay
/// its output out better, and may ignore it without changing the result.
///
/// Prometheus's `Limit` and `DisableTrimming` are left out: the first
/// serves its label-values API, the second its own chunk trimming.
///
/// `#[non_exhaustive]`: the engine adds hints over time, and a struct
/// literal in a store's own code would stop compiling with each one. A
/// store reads the fields; tests that need a hint build it with
/// [`Self::range`] and the `with_` methods.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct SelectHints {
    pub start_ms: i64,
    pub end_ms: i64,
    /// How far back of each window end the query reads: the lookback
    /// delta for an instant selector, the `[5m]` for a range one. The
    /// range starts a window before the first window end, so the blocks
    /// run from `start_ms + window_ms` to `end_ms`, and each reaches back
    /// by this much so that it answers its steps alone.
    pub window_ms: i64,
    /// Step of the enclosing range query; `None` when the query evaluates at
    /// a single timestamp (start_ms == end_ms). A store may read downsampled
    /// or step-aligned data.
    pub step_ms: Option<i64>,
    /// Window of a range selector such as `[5m]`; `None` for an instant
    /// selector. A store may prune chunks per window.
    pub range_ms: Option<i64>,
    /// The function or aggregation directly above the selector, by its
    /// PromQL name (`rate`, `sum`); `None` when there is none.
    /// `count_over_time` needs no values; `rate` needs whole windows.
    pub func: Option<String>,
    /// `by` of the aggregation directly above the selector. With
    /// `sum by (route)` only `route` decides the output, so a store may
    /// drop the other labels, and if it partitions its plan by the
    /// grouping labels the engine aggregates without a shuffle. Absent as
    /// soon as anything sits in between, as in `sum by (route)
    /// (rate(x[5m]))`, where the selector's own parent is `rate`.
    pub grouping: Option<Grouping>,
    /// Return only shard `index` of `count` of the matching series, for
    /// scanning the series space in parallel; `None` for all of them.
    pub shard: Option<Shard>,
}

impl SelectHints {
    /// Just the range, with no window to reach back by; every advisory
    /// hint absent.
    pub fn range(start_ms: i64, end_ms: i64) -> Self {
        Self {
            start_ms,
            end_ms,
            window_ms: 0,
            step_ms: None,
            range_ms: None,
            func: None,
            grouping: None,
            shard: None,
        }
    }

    pub fn with_window_ms(mut self, window_ms: i64) -> Self {
        self.window_ms = window_ms;
        self
    }

    pub fn with_step_ms(mut self, step_ms: i64) -> Self {
        self.step_ms = Some(step_ms);
        self
    }

    pub fn with_range_ms(mut self, range_ms: i64) -> Self {
        self.range_ms = Some(range_ms);
        self
    }

    pub fn with_func(mut self, func: impl Into<String>) -> Self {
        self.func = Some(func.into());
        self
    }

    pub fn with_grouping(mut self, grouping: Grouping) -> Self {
        self.grouping = Some(grouping);
        self
    }

    pub fn with_shard(mut self, shard: Shard) -> Self {
        self.shard = Some(shard);
        self
    }
}

/// `by (labels…)` or `without (labels…)` of an enclosing aggregation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Grouping {
    pub labels: Vec<String>,
    /// `true` for `by`, `false` for `without`. Carried for parity with
    /// upstream's `SelectHints.By`, which is likewise only ever set for
    /// `by`: `without` tells a store nothing it can act on.
    pub by: bool,
}

impl Grouping {
    pub fn new(labels: Vec<String>, by: bool) -> Self {
        Self { labels, by }
    }
}

/// One of `count` equal parts of the series space, by series identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct Shard {
    pub index: u64,
    pub count: u64,
}

impl Shard {
    pub fn new(index: u64, count: u64) -> Self {
        Self { index, count }
    }
}

/// A store of series, as the engine sees it.
#[async_trait]
pub trait SeriesSource: fmt::Debug + Send + Sync {
    /// Every series matching all of `matchers`, carrying only the samples
    /// within `hints`, as a plan whose schema passes [`series::validate`].
    ///
    /// `matchers` arrive verbatim from the parser, including `__name__`
    /// (synthesized from a bare metric name). An absent label compares as
    /// `""`, and regexes are anchored to the whole value; see
    /// [`crate::CompiledMatcher`] for the reference semantics a store is held to.
    async fn select(
        &self,
        state: &dyn Session,
        matchers: &[LabelMatcher],
        hints: SelectHints,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// Puts a source-owned node on top of a selector's `TableScan`, by
    /// returning [`SelectorExtension::plan`] over `scan`. Called once per
    /// selector, instant, range or inside a range function, before any
    /// aggregate goes above it. `hints` is what `select` received, for a
    /// source that only wants the node for some functions.
    ///
    /// The default returns `scan`, which leaves plans as they were.
    fn scan_node(&self, scan: LogicalPlan, _hints: &SelectHints) -> Result<LogicalPlan> {
        Ok(scan)
    }
}

/// What a source puts on top of a selector's scan; see
/// [`SeriesSource::scan_node`].
///
/// `wrap` runs beneath [`SeriesSetExec`], not above it. The engine owns that
/// operator and `check_selector_plans` refuses any node between a selector
/// aggregate and it, so an operator that sat above would be refused. The
/// extension planner therefore rebuilds the scan as
/// `SeriesSetExec -> wrap(store plan)`, and the order check still covers
/// whatever `wrap` emits.
pub trait SelectorNode: fmt::Debug + Send + Sync {
    /// The exec to run between the store's plan and [`SeriesSetExec`]. It
    /// must keep the scan's schema: the logical plan above was built from it.
    fn wrap(&self, input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>>;

    /// The node's line in logical `EXPLAIN`, e.g. `Dedup: replica_labels=[r]`.
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self:?}")
    }

    /// Identity for plan equality and hashing, which DataFusion needs of
    /// every logical node. Must differ whenever `wrap` would behave
    /// differently; a type name alone would equate two differently
    /// configured nodes, so there is no default.
    fn node_key(&self) -> String;
}

/// The logical node a [`SelectorNode`] travels in, with the scan as its
/// one input.
#[derive(Debug, Clone)]
pub struct SelectorExtension {
    node: Arc<dyn SelectorNode>,
    scan: LogicalPlan,
}

impl SelectorExtension {
    /// `node` over `scan`, as the plan [`SeriesSource::scan_node`] returns.
    pub fn plan(node: Arc<dyn SelectorNode>, scan: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(Self { node, scan }),
        })
    }
}

impl PartialEq for SelectorExtension {
    fn eq(&self, other: &Self) -> bool {
        self.node.node_key() == other.node.node_key() && self.scan == other.scan
    }
}

impl Eq for SelectorExtension {}

impl Hash for SelectorExtension {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.node.node_key().hash(state);
        self.scan.hash(state);
    }
}

impl PartialOrd for SelectorExtension {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.node.node_key().cmp(&other.node.node_key()) {
            Ordering::Equal => self.scan.partial_cmp(&other.scan),
            ord => Some(ord),
        }
    }
}

impl UserDefinedLogicalNodeCore for SelectorExtension {
    fn name(&self) -> &str {
        "SelectorExtension"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.scan]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.scan.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.node.fmt_for_explain(f)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        match (exprs.is_empty(), inputs.pop(), inputs.is_empty()) {
            (true, Some(scan), true) => Ok(Self {
                node: Arc::clone(&self.node),
                scan,
            }),
            _ => Err(DataFusionError::Internal(
                "SelectorExtension takes one input and no expressions".into(),
            )),
        }
    }
}

/// Lowers a [`SelectorExtension`] to `SeriesSetExec -> wrap(store plan)`.
///
/// The input arrives already planned by [`SelectorTable::scan`] as
/// `SeriesSetExec`, possibly under a `ProjectionExec` for pruned columns or
/// a `CooperativeExec` that DataFusion adds around leaves. Those wrappers
/// are rebuilt around the new `SeriesSetExec` rather than dropped, so the
/// output keeps the column order the plan above was built against.
pub(crate) struct SelectorExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for SelectorExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(ext) = node.as_any().downcast_ref::<SelectorExtension>() else {
            return Ok(None);
        };
        let [input] = physical_inputs else {
            return Err(DataFusionError::Internal(
                "SelectorExtension takes exactly one input".into(),
            ));
        };
        lower_beneath_series_set(input, ext.node.as_ref()).map(Some)
    }
}

fn lower_beneath_series_set(
    plan: &Arc<dyn ExecutionPlan>,
    node: &dyn SelectorNode,
) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(set) = plan.downcast_ref::<SeriesSetExec>() {
        let input = Arc::clone(&set.input);
        let wrapped = node.wrap(Arc::clone(&input))?;
        // `SeriesSetExec::new` indexes the canonical columns, and the plan
        // above is bound to the scan's column order; a wrapper that changes
        // either is its bug and must not become a panic or a silent
        // misread further up.
        if wrapped.schema().fields() != input.schema().fields() {
            return Err(DataFusionError::Plan(format!(
                "{} changed the scan's schema; a selector node must keep its input's columns",
                wrapped.name()
            )));
        }
        return Ok(Arc::new(SeriesSetExec::new(wrapped)));
    }
    if plan.is::<ProjectionExec>() || plan.is::<CooperativeExec>() {
        if let [child] = plan.children().as_slice() {
            let lowered = lower_beneath_series_set(child, node)?;
            return Arc::clone(plan).with_new_children(vec![lowered]);
        }
    }
    Err(DataFusionError::Plan(format!(
        "a selector node found {} where it expected a SeriesSetExec",
        plan.name()
    )))
}

/// One selector's scan, as a DataFusion table.
///
/// Built eagerly: `select` is called once at plan time and its result
/// held, because a [`TableProvider`] must report its schema before any
/// scan and the schema — the label names — is only known once the store
/// has found the matching series. That is also how Prometheus works: it
/// expands every selector's series before evaluating anything. A
/// consequence worth knowing is that the store's own planning happens
/// while the engine is planning, not while it is executing.
///
/// Public so that a store which implements [`SeriesSource`] can register
/// itself as a SQL table for debugging: `SELECT labels, samples FROM …`.
///
/// Nothing in this crate constructs one yet. The planner that stacks on
/// this PR is the first caller; it lives here because it is the other half
/// of what the trait promises — the shape a store returns, and how the
/// engine mounts that shape into a plan.
#[derive(Debug)]
pub struct SelectorTable {
    plan: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    /// The selector's bare metric name (`vs.name`), `""` when it had
    /// none. Kept alongside `matchers` because Go `VectorSelector.String()`
    /// (`promql/parser/printer.go`) only folds a `__name__` matcher into
    /// the bare-name prefix when its value equals this name — not for any
    /// `__name__` matcher — so rendering the scan needs the name on its
    /// own, not just recovered from the matcher list.
    name: String,
    /// The matchers `select` was called with. `select` only takes them,
    /// never returns them, and a `TableScan` node has no other way to say
    /// what it scans — kept here so `explain` can render `TableScan:
    /// selector_0 [http_requests_total{job="api"}]` instead of a bare
    /// table name.
    matchers: Vec<LabelMatcher>,
}

impl SelectorTable {
    /// Ask `source` for the selection and validate what comes back.
    pub async fn try_new(
        state: &dyn Session,
        source: &dyn SeriesSource,
        name: &str,
        matchers: &[LabelMatcher],
        hints: SelectHints,
    ) -> std::result::Result<Self, EngineError> {
        let plan = source.select(state, matchers, hints).await?;
        let schema = plan.schema();
        series::validate(&schema)?;
        Ok(Self {
            plan,
            schema,
            name: name.to_string(),
            matchers: matchers.to_vec(),
        })
    }

    /// The label names this selection carries.
    pub fn label_names(&self) -> Vec<String> {
        series::label_names(&self.schema)
    }

    /// The selector's bare metric name, for `explain`; see the field doc.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// What this table scans, for `explain` to render on the `TableScan` line.
    pub fn matchers(&self) -> &[LabelMatcher] {
        &self.matchers
    }
}

#[async_trait]
impl TableProvider for SelectorTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan: Arc<dyn ExecutionPlan> = Arc::new(SeriesSetExec::new(Arc::clone(&self.plan)));
        let Some(cols) = projection else {
            return Ok(plan);
        };
        let exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = cols
            .iter()
            .map(|&i| {
                let name = self.schema.field(i).name().clone();
                (
                    Arc::new(Column::new(&name, i)) as Arc<dyn PhysicalExpr>,
                    name,
                )
            })
            .collect();
        Ok(Arc::new(ProjectionExec::try_new(exprs, plan)?))
    }
}

/// A store's plan, declared ordered by `(block_start, block_end, labels)`
/// and checked to be so: Prometheus's `storage.SeriesSet`, the stream of
/// series a `Select` returns.
///
/// The declaration is what lets an aggregate grouped by the block and
/// `labels` run in `InputOrderMode::Sorted`, holding one open series per
/// partition instead of every series of the scan, and closing it at the
/// block's edge as well as at the next label set. A store cannot be
/// trusted with that on its word, because a wrong declaration does not
/// fail, it splits a series in two and answers twice. So every series,
/// empty ones included, is compared with the one before it in its
/// partition: within a block labels must not descend, which also catches
/// a closed series reappearing, and a new block must start at or after
/// the end of the one before. An empty series still carries a label set
/// and still closes the series before it, even though it has nothing to
/// fold; skipping its label check would let DataFusion's grouped
/// aggregate close a series on one this check never looked at. Only the
/// first-sample-timestamp check is skipped for an empty series, since it
/// has no first sample: within one label set the first sample timestamp
/// of the next non-empty chunk must not go backwards. Nothing is sorted
/// or buffered to repair a violation; that would be the whole-series
/// concatenation the contract exists to avoid. It is an
/// [`EngineError::Source`] instead.
///
/// The check is per partition. Keeping a series inside one partition is
/// the plan's concern, not something a per-partition stream can see.
#[derive(Debug)]
pub struct SeriesSetExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl SeriesSetExec {
    /// `input` must have a schema that passed [`series::validate`].
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let schema = input.schema();
        let column = |name: &str| -> Arc<dyn PhysicalExpr> {
            Arc::new(Column::new(
                name,
                schema.index_of(name).expect("a canonical schema"),
            ))
        };
        let labels = column(LABELS);
        let ordering = [column(BLOCK_START), column(BLOCK_END), Arc::clone(&labels)]
            .map(PhysicalSortExpr::new_default);
        let eq = EquivalenceProperties::new_with_orderings(schema, [ordering]);
        // Every label set whole in one partition is hash partitioning on
        // `labels` in DataFusion's sense. The selector groups by
        // (block_start, block_end, labels), which Hash(labels) satisfies as
        // a subset, so it needs no shuffle and plans SinglePartitioned. The
        // store's hash is not DataFusion's; see docs for which operators
        // that fools.
        let n = input.properties().partitioning.partition_count();
        let properties = PlanProperties::clone(input.properties())
            .with_eq_properties(eq)
            .with_partitioning(Partitioning::Hash(vec![labels], n))
            .into();
        Self { input, properties }
    }
}

impl DisplayAs for SeriesSetExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SeriesSetExec")
    }
}

impl ExecutionPlan for SeriesSetExec {
    fn name(&self) -> &str {
        "SeriesSetExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    /// Round-robin beneath this node would deal one label set's chunks to
    /// several partitions, each of which would then pass the check.
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "SeriesSetExec takes exactly one child".into(),
            ));
        }
        Ok(Arc::new(Self::new(children.swap_remove(0))))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let labels = input.schema().field_with_name(LABELS)?.data_type().clone();
        Ok(Box::pin(SeriesSetStream {
            input,
            converter: RowConverter::new(vec![SortField::new(labels)])?,
            prev: None,
            prev_first_t: None,
            prev_block: None,
        }))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        self.input.partition_statistics(partition)
    }
}

struct SeriesSetStream {
    input: SendableRecordBatchStream,
    /// One converter for the whole stream, so the first series of a batch
    /// compares with the last of the batch before.
    converter: RowConverter,
    prev: Option<OwnedRow>,
    /// The last non-empty chunk's first sample timestamp within the
    /// current label set, `None` until one has been seen.
    prev_first_t: Option<i64>,
    /// The block of the previous series, `None` before the first.
    prev_block: Option<Block>,
}

impl SeriesSetStream {
    /// Adjacent series of one batch are compared on the label columns as
    /// they are. Converting every series, as the boundary one is, copies
    /// each label set into row format and allocates one per series; with
    /// a sample walk that could not vectorise, that was the 8 to 15% of a
    /// 10,000-series query that switching the check off saved.
    fn check(&mut self, batch: &RecordBatch) -> Result<()> {
        let n = batch.num_rows();
        if n == 0 {
            return Ok(());
        }
        let labels = batch.column_by_name(LABELS).expect("canonical");
        let samples = batch
            .column_by_name(SAMPLES)
            .expect("canonical")
            .as_list::<i32>();
        let timestamps = samples
            .values()
            .as_struct()
            .column_by_name(TIMESTAMP)
            .expect("canonical")
            .as_primitive::<TimestampMillisecondType>()
            .values();
        let offsets = samples.value_offsets();
        let block_column = |name: &str| {
            batch
                .column_by_name(name)
                .expect("canonical")
                .as_primitive::<TimestampMillisecondType>()
                .values()
        };
        let (block_starts, block_ends) = (block_column(BLOCK_START), block_column(BLOCK_END));
        let cmp = make_comparator(labels, labels, SortOptions::default())?;
        let first = self.converter.convert_columns(&[labels.slice(0, 1)])?;
        for r in 0..n {
            let (start, end) = (offsets[r] as usize, offsets[r + 1] as usize);
            // A whole chunk at a time, so the scan vectorises; only a
            // chunk that fails is walked again for the message.
            if !timestamps[start..end].is_sorted() {
                let i = (start + 1..end)
                    .find(|&i| timestamps[i] < timestamps[i - 1])
                    .expect("an unsorted chunk has a descent");
                return Err(source_error(format!(
                    "series {}: sample at {} follows one at {}; samples within a chunk \
                     must ascend by timestamp",
                    format_labels(labels.as_struct(), r),
                    timestamps[i],
                    timestamps[i - 1],
                )));
            }
            let block = Block {
                start_ms: block_starts[r],
                end_ms: block_ends[r],
            };
            if block.end_ms < block.start_ms {
                return Err(source_error(format!(
                    "series {} is in block [{}, {}), which ends before it starts",
                    format_labels(labels.as_struct(), r),
                    block.start_ms,
                    block.end_ms,
                )));
            }
            // A new block starts the label order afresh; the blocks
            // themselves must ascend and not overlap. The columns are
            // constant within a block by construction: a block that shares
            // its start with the previous one but not its end is neither
            // the same block nor a later one.
            let same_block = match self.prev_block {
                Some(prev) if prev == block => true,
                Some(prev) if block.start_ms >= prev.end_ms && block.start_ms > prev.start_ms => {
                    false
                }
                Some(prev) => {
                    return Err(source_error(format!(
                        "series {} in block [{}, {}) follows one in block [{}, {}); blocks \
                         must ascend and not overlap, and a series' block columns must \
                         not change within the block",
                        format_labels(labels.as_struct(), r),
                        block.start_ms,
                        block.end_ms,
                        prev.start_ms,
                        prev.end_ms,
                    )))
                }
                None => false,
            };
            self.prev_block = Some(block);
            let order = match (same_block, r) {
                (false, _) => None,
                (true, 0) => self.prev.as_ref().map(|p| p.row().cmp(&first.row(0))),
                (true, _) => Some(cmp(r - 1, r)),
            };
            match order {
                Some(Ordering::Greater) => {
                    let prev = match r {
                        0 => self.prev_labels()?,
                        _ => format_labels(labels.as_struct(), r - 1),
                    };
                    return Err(source_error(format!(
                        "series {} arrived after {prev}; within a block series must be \
                         sorted by labels in struct order, and the chunks of a label set \
                         consecutive",
                        format_labels(labels.as_struct(), r),
                    )));
                }
                Some(Ordering::Equal) => {
                    if start != end {
                        let first_t = timestamps[start];
                        if let Some(prev_first_t) = self.prev_first_t {
                            if first_t < prev_first_t {
                                return Err(source_error(format!(
                                    "series {}: a chunk starting at {first_t} follows one \
                                     starting at {prev_first_t}; the chunks of a label set \
                                     must ascend by first sample timestamp",
                                    format_labels(labels.as_struct(), r),
                                )));
                            }
                        }
                        self.prev_first_t = Some(first_t);
                    }
                }
                _ => self.prev_first_t = (start != end).then(|| timestamps[start]),
            }
        }
        let last = self.converter.convert_columns(&[labels.slice(n - 1, 1)])?;
        self.prev = Some(last.row(0).owned());
        Ok(())
    }

    /// The previous series' labels, decoded only for an error message.
    fn prev_labels(&self) -> Result<String> {
        let prev = self.prev.as_ref().expect("compared against");
        let columns = self.converter.convert_rows(std::iter::once(prev.row()))?;
        Ok(format_labels(columns[0].as_struct(), 0))
    }
}

impl Stream for SeriesSetStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => Poll::Ready(Some(self.check(&batch).map(|()| batch))),
            other => other,
        }
    }
}

impl RecordBatchStream for SeriesSetStream {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}

pub(crate) fn source_error(msg: String) -> DataFusionError {
    DataFusionError::External(Box::new(EngineError::Source(msg)))
}

/// `{name="value", …}` without the absent labels, as Prometheus prints a
/// label set.
fn format_labels(labels: &StructArray, row: usize) -> String {
    let pairs: Vec<String> = labels
        .fields()
        .iter()
        .zip(labels.columns())
        .filter_map(|(f, c)| {
            let v = c.as_string_view().value(row);
            (!v.is_empty()).then(|| format!("{}={v:?}", f.name()))
        })
        .collect();
    format!("{{{}}}", pairs.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        ArrayRef, Float64Array, ListArray, RecordBatch, StringViewArray, StructArray,
        TimestampMillisecondArray,
    };
    use datafusion::arrow::buffer::OffsetBuffer;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;

    use crate::series::{sample_fields, sample_item, schema};

    #[test]
    fn hints_built_by_the_builders_carry_what_was_set() {
        let hints = SelectHints::range(10, 20)
            .with_window_ms(5)
            .with_step_ms(2)
            .with_range_ms(5)
            .with_func("rate")
            .with_grouping(Grouping::new(vec!["route".into()], true))
            .with_shard(Shard::new(1, 4));
        assert_eq!(
            hints,
            SelectHints {
                start_ms: 10,
                end_ms: 20,
                window_ms: 5,
                step_ms: Some(2),
                range_ms: Some(5),
                func: Some("rate".into()),
                grouping: Some(Grouping {
                    labels: vec!["route".into()],
                    by: true
                }),
                shard: Some(Shard { index: 1, count: 4 }),
            }
        );
    }

    /// `(labels, timestamps)`; a label missing from a series is `""`, as
    /// in the canonical shape.
    type Row<'a> = (&'a [(&'a str, &'a str)], &'a [i64]);

    /// One block over everything, so the block columns stay out of the
    /// way of the label and timestamp checks.
    const ONE_BLOCK: Block = Block {
        start_ms: 0,
        end_ms: i64::MAX,
    };

    /// One batch of `rows` over the label `names`, all in one block.
    fn batch(names: &[&str], rows: &[Row]) -> RecordBatch {
        batch_in(ONE_BLOCK, names, rows)
    }

    /// One batch of `rows` over the label `names`, all in `block`.
    fn batch_in(block: Block, names: &[&str], rows: &[Row]) -> RecordBatch {
        let names: Vec<String> = names.iter().map(|n| n.to_string()).collect();
        let schema = schema(&names);
        let columns: Vec<ArrayRef> = names
            .iter()
            .map(|n| {
                let values = rows
                    .iter()
                    .map(|(labels, _)| labels.iter().find(|(k, _)| k == n).map_or("", |(_, v)| *v));
                Arc::new(StringViewArray::from_iter_values(values)) as ArrayRef
            })
            .collect();
        let labels = match schema.field(0).data_type() {
            datafusion::arrow::datatypes::DataType::Struct(f) if f.is_empty() => {
                StructArray::new_empty_fields(rows.len(), None)
            }
            datafusion::arrow::datatypes::DataType::Struct(f) => {
                StructArray::new(f.clone(), columns, None)
            }
            _ => unreachable!("canonical"),
        };
        let ts: Vec<i64> = rows.iter().flat_map(|(_, t)| t.iter().copied()).collect();
        let entries = StructArray::new(
            sample_fields(),
            vec![
                Arc::new(TimestampMillisecondArray::from(ts.clone())),
                Arc::new(Float64Array::from(vec![1.0; ts.len()])),
            ],
            None,
        );
        let offsets = OffsetBuffer::from_lengths(rows.iter().map(|(_, t)| t.len()));
        let samples = ListArray::new(sample_item(), offsets, Arc::new(entries), None);
        let [block_start, block_end] = block.columns(rows.len());
        RecordBatch::try_new(
            schema,
            vec![Arc::new(labels), Arc::new(samples), block_start, block_end],
        )
        .unwrap()
    }

    fn block(start_ms: i64, end_ms: i64) -> Block {
        Block { start_ms, end_ms }
    }

    fn series_set(partitions: Vec<Vec<RecordBatch>>) -> Arc<SeriesSetExec> {
        let schema = partitions
            .iter()
            .flatten()
            .next()
            .expect("at least one batch")
            .schema();
        let child = MemorySourceConfig::try_new_exec(&partitions, schema, None).unwrap();
        Arc::new(SeriesSetExec::new(child))
    }

    async fn run(
        partitions: Vec<Vec<RecordBatch>>,
    ) -> std::result::Result<Vec<RecordBatch>, EngineError> {
        let ctx = SessionContext::new();
        Ok(collect(series_set(partitions), ctx.task_ctx()).await?)
    }

    async fn refused(partitions: Vec<Vec<RecordBatch>>) -> String {
        match run(partitions).await {
            Err(EngineError::Source(msg)) => msg,
            other => panic!("expected EngineError::Source, got {other:?}"),
        }
    }

    const A: &[(&str, &str)] = &[("pod", "a")];
    const B: &[(&str, &str)] = &[("pod", "b")];
    const C: &[(&str, &str)] = &[("pod", "c")];

    #[tokio::test]
    async fn descending_labels_are_refused() {
        let msg = refused(vec![vec![batch(&["pod"], &[(B, &[1]), (A, &[1])])]]).await;
        assert!(msg.contains(r#"pod="a""#), "{msg}");
    }

    #[tokio::test]
    async fn a_closed_series_reappearing_is_refused() {
        let msg = refused(vec![vec![
            batch(&["pod"], &[(A, &[1, 2])]),
            batch(&["pod"], &[(B, &[1])]),
            batch(&["pod"], &[(A, &[3])]),
        ]])
        .await;
        assert!(msg.contains(r#"pod="a""#), "{msg}");
    }

    #[tokio::test]
    async fn a_first_timestamp_going_backwards_is_refused() {
        let msg = refused(vec![vec![
            batch(&["pod"], &[(A, &[100, 200])]),
            batch(&["pod"], &[(A, &[50])]),
        ]])
        .await;
        assert!(msg.contains(r#"pod="a""#) && msg.contains("50"), "{msg}");
    }

    #[tokio::test]
    async fn a_series_split_across_three_batches_passes_untouched() {
        let input = vec![
            batch(&["pod"], &[(A, &[1, 2])]),
            batch(&["pod"], &[(A, &[3])]),
            batch(&["pod"], &[(A, &[3, 4]), (B, &[1])]),
        ];
        let out = run(vec![input.clone()]).await.unwrap();
        assert_eq!(out, input);
    }

    /// An empty row has no first timestamp to check, so a run of empty
    /// rows inside one series never trips the "ascend by first sample
    /// timestamp" check.
    #[tokio::test]
    async fn empty_rows_skip_only_the_timestamp_check() {
        run(vec![vec![batch(
            &["pod"],
            &[(A, &[100, 200]), (A, &[]), (A, &[300])],
        )]])
        .await
        .unwrap();
    }

    /// An empty row still carries a label set and still closes the series
    /// before it: `c{}` closes `b`, so `b` reappearing in the next batch is
    /// a closed series reappearing, same as if `c{}` had samples.
    #[tokio::test]
    async fn an_empty_row_closes_the_series_before_it() {
        let msg = refused(vec![vec![
            batch(&["pod"], &[(B, &[0, 30_000]), (C, &[])]),
            batch(&["pod"], &[(B, &[60_000, 90_000])]),
        ]])
        .await;
        assert!(msg.contains(r#"pod="b""#), "{msg}");
    }

    /// The doc promises samples ascend within a row is checked, not just
    /// assumed; a row whose samples go backwards must be refused.
    #[tokio::test]
    async fn samples_out_of_order_within_a_row_are_refused() {
        let msg = refused(vec![vec![batch(&["pod"], &[(A, &[200, 100])])]]).await;
        assert!(msg.contains(r#"pod="a""#), "{msg}");
    }

    /// The check runs a row at a time, not the batch's samples as one
    /// sequence: a descent at a row boundary is two rows, one deep inside a
    /// long row of a later series is an error.
    #[tokio::test]
    async fn a_descent_deep_in_a_later_row_is_refused() {
        let long: Vec<i64> = (0..40).map(|i| if i == 37 { 0 } else { i * 10 }).collect();
        let msg = refused(vec![vec![batch(&["pod"], &[(A, &[500, 600]), (B, &long)])]]).await;
        assert!(
            msg.contains(r#"pod="b""#) && msg.contains("follows one at 360"),
            "{msg}"
        );
    }

    /// A batch with no rows carries no series and must not reset what the
    /// next batch is compared against.
    #[tokio::test]
    async fn an_empty_batch_between_series_changes_nothing() {
        run(vec![vec![
            batch(&["pod"], &[(A, &[1])]),
            batch(&["pod"], &[]),
            batch(&["pod"], &[(A, &[2]), (B, &[1])]),
        ]])
        .await
        .unwrap();
        let msg = refused(vec![vec![
            batch(&["pod"], &[(B, &[1])]),
            batch(&["pod"], &[]),
            batch(&["pod"], &[(A, &[1])]),
        ]])
        .await;
        assert!(
            msg.contains(r#"pod="a""#) && msg.contains(r#"pod="b""#),
            "{msg}"
        );
    }

    /// The first row of a batch is compared with the last of the one
    /// before, not with the first series of the stream.
    #[tokio::test]
    async fn a_batch_boundary_compares_against_the_last_series() {
        run(vec![vec![
            batch(&["pod"], &[(A, &[1]), (B, &[1])]),
            batch(&["pod"], &[(B, &[2]), (C, &[1])]),
        ]])
        .await
        .unwrap();
        let msg = refused(vec![vec![
            batch(&["pod"], &[(A, &[1]), (C, &[1])]),
            batch(&["pod"], &[(B, &[1])]),
        ]])
        .await;
        assert!(
            msg.contains(r#"pod="b""#) && msg.contains(r#"pod="c""#),
            "{msg}"
        );
    }

    /// `labels ASC` is DataFusion's struct order: fields by name, compared
    /// one at a time, an absent label as `""`. `{b="1"}` is `("", "1")`
    /// and sorts before `{a="1"}`, `("1", "")`, the reverse of Prometheus's
    /// `labels.Compare`; the declaration can only be true in the order
    /// DataFusion compares in.
    #[tokio::test]
    async fn sparse_label_sets_pass_in_struct_order() {
        run(vec![vec![batch(
            &["a", "b"],
            &[
                (&[("b", "1")], &[1]),
                (&[("a", "1")], &[1]),
                (&[("a", "1"), ("b", "1")], &[1]),
            ],
        )]])
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn prometheus_order_is_refused_where_it_differs() {
        refused(vec![vec![batch(
            &["a", "b"],
            &[(&[("a", "1")], &[1]), (&[("b", "1")], &[1])],
        )]])
        .await;
    }

    #[tokio::test]
    async fn a_label_set_without_labels_is_one_series() {
        run(vec![vec![
            batch(&[], &[(&[], &[1, 2])]),
            batch(&[], &[(&[], &[3])]),
        ]])
        .await
        .unwrap();
        refused(vec![vec![batch(&[], &[(&[], &[2]), (&[], &[1])])]]).await;
    }

    /// Each partition is its own stream of series; the same label set in
    /// two partitions is the plan's problem, not the order check's.
    #[tokio::test]
    async fn partitions_are_checked_independently() {
        let out = run(vec![
            vec![batch(&["pod"], &[(B, &[1])])],
            vec![batch(&["pod"], &[(A, &[1])])],
        ])
        .await
        .unwrap();
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn declares_block_then_labels_ascending_and_keeps_the_partitioning() {
        let exec = series_set(vec![
            vec![batch(&["pod"], &[(A, &[1])])],
            vec![batch(&["pod"], &[(B, &[1])])],
        ]);
        assert_eq!(exec.properties().output_partitioning().partition_count(), 2);
        let ordering = exec.properties().output_ordering().expect("an ordering");
        assert_eq!(
            ordering.to_string(),
            "block_start@2 ASC, block_end@3 ASC, labels@0 ASC"
        );
        assert_eq!(exec.maintains_input_order(), [true]);
    }

    /// A new block starts the label order over: `b` then `a` is fine
    /// across a block edge, and so is `a` again, since each block folds
    /// its series from nothing.
    #[test]
    fn a_new_block_restarts_the_label_order() {
        run(vec![vec![
            batch_in(block(0, 100), &["pod"], &[(A, &[1]), (B, &[1])]),
            batch_in(block(100, 200), &["pod"], &[(A, &[150]), (B, &[150])]),
        ]])
        .await_ok();
        run(vec![vec![batch_in(block(0, 100), &["pod"], &[(B, &[1])])]
            .into_iter()
            .chain([batch_in(block(100, 200), &["pod"], &[(A, &[1])])])
            .collect()])
        .await_ok();
        // The first sample timestamp check starts over too: the second
        // block's reach-back repeats what the first block held.
        run(vec![vec![
            batch_in(block(0, 100), &["pod"], &[(A, &[50, 90])]),
            batch_in(block(100, 200), &["pod"], &[(A, &[40])]),
        ]])
        .await_ok();
    }

    #[tokio::test]
    async fn blocks_out_of_order_or_overlapping_are_refused() {
        let msg = refused(vec![vec![
            batch_in(block(100, 200), &["pod"], &[(A, &[150])]),
            batch_in(block(0, 100), &["pod"], &[(A, &[50])]),
        ]])
        .await;
        assert!(msg.contains("blocks must ascend"), "{msg}");

        let msg = refused(vec![vec![batch_in(block(0, 100), &["pod"], &[(A, &[50])])]
            .into_iter()
            .chain([batch_in(block(50, 150), &["pod"], &[(B, &[60])])])
            .collect()])
        .await;
        assert!(msg.contains("not overlap"), "{msg}");

        // Within one batch as well: the check does not need a batch edge.
        let mut b = batch_in(block(0, 100), &["pod"], &[(A, &[50]), (B, &[50])]);
        let ends = Arc::new(TimestampMillisecondArray::from(vec![100, 90])) as ArrayRef;
        let mut columns = b.columns().to_vec();
        columns[3] = ends;
        b = RecordBatch::try_new(b.schema(), columns).unwrap();
        let msg = refused(vec![vec![b]]).await;
        assert!(msg.contains("must not change within the block"), "{msg}");

        let msg = refused(vec![vec![batch_in(
            block(100, 50),
            &["pod"],
            &[(A, &[50])],
        )]])
        .await;
        assert!(msg.contains("ends before it starts"), "{msg}");
    }

    /// Inside a block the label order still holds, whatever the blocks
    /// before it did.
    #[tokio::test]
    async fn the_label_order_is_checked_within_the_new_block() {
        let msg = refused(vec![vec![
            batch_in(block(0, 100), &["pod"], &[(A, &[1])]),
            batch_in(block(100, 200), &["pod"], &[(B, &[150]), (A, &[150])]),
        ]])
        .await;
        assert!(msg.contains(r#"pod="a""#), "{msg}");
    }

    /// `run` for the tests above that only want it to succeed, on a
    /// runtime of their own so a `#[test]` can chain several.
    trait AwaitOk {
        fn await_ok(self);
    }

    impl<F: std::future::Future<Output = std::result::Result<Vec<RecordBatch>, EngineError>>>
        AwaitOk for F
    {
        fn await_ok(self) {
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(self)
                .unwrap();
        }
    }
}
