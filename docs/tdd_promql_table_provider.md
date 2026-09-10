# Datafusion PromQL: Table Providers

# Goals & background

## Background

DataFusion is an extensible query engine written in Rust that uses Apache Arrow as its in-memory format. It has broad community adoption, and it’s optimised for various analytical use cases beyond just querying metrics.

Currently PromQL implementation is heavily tied to the Prometheus TSDB storage format and observability ecosystem. It’s a home-grown database from scratch, used in popular, but niche context, with poor interop with existing OLAP ecosystem. Its developer community is small compared to general OLAPs like Clickhouse or Datafusion; while having to solve similar operational concerns (federation, sharding, query execution, quotas, etc.). To minimise this maintenance burden, and duplicate work, various observability teams are looking for a unified solution. 

Due to DataFusion OSS nature, true multi-vendor governance model, and great extensibility we’re focusing on building PromQL engine on top of datafusion. However this presents a problem this document aims to solve:

Each organization's metrics data store and layout will be different. From using thanos v1 TSDB block format, to early Parquet gateway, leveraging internal data lake teams (e.g. Iceberg), or something else entirely. 

For us to cooperate in OSS on PromQL query engine, thus, it’s important to define a “least common denominator” data layout definition on top of which PromQL will execute. Logical data definition via [Custom Table Format](https://datafusion.apache.org/library-user-guide/custom-table-providers.html) data fusion abstraction. 

## Goal

Defining minimal metric virtual table abstraction that can be used with our PromQL engine. Shared definitions we can all work on, and optimize together, vs. having non interoperable implementations. 

## Non-goal

- Specifying exact physical storage format (e.g. parquet/vortex)  
- Specifying physical storage format encoding or compression  
- Specifying exact catalog (e.g. Iceberg, etc.)

Focus is on abstraction, a virtual table from which initial logical query can be constructed. It’ll later be optimised with respect to actual storage design, and executed. 

# High level design

It’ll consist of virtual table per series `__name__`. For nameless queries such as `count({job=”example”})` we'd implement [UDTF](https://datafusion.apache.org/python/user-guide/common-operations/udf-and-udfa.html#table-functions) such as `all_series('job=”example”')`.

Logically each table has fully expanded columns for each label; that is, it’s column per label. One sample per row

```sql
TABLE {{ metric name }} (
    label_a    STRING  -- it should only use labels this metric has
	label_b    STRING
timestamp TIMESTAMP
	value_f   float64 -- for counters / gauges
	value_nh  NATIVE_HISTOGRAM -- for native histogram. TBD how we encode this
)
```

To efficiently support window functions like `rate` we'll implement them via [UDWF](https://datafusion.apache.org/python/user-guide/common-operations/udf-and-udfa.html#window-functions). Thus Table provider should within each timestamp [Range Partition](https://datafusion.apache.org/blog/output/2026/08/25/datafusion-55.0.0/#range-partitioning) be ordered by labelset and timestamp. Thus when window function ([df blogpost](https://datafusion.apache.org/blog/2025/04/19/user-defined-window-functions/)) are partitioned by lableset, and ordered by timestamp we'll avoid unnecessary sorting step due to presorted data. 

Exact ordering wrt labels is depending on usecase, and up to the operator. 

This doesn’t preclude underlying tables storing timeseries batches (e.g. 2h samples per row). This is optimisable during logical query optimization, and it’ll be common for `rate` like operations.

For now downsampling and preaggregations are excluded from the spec.

# Alternatives considered

Having batched rows, where multiple samples for same labelset per row, like:

```sql
TABLE {{ metric name }} (
    label_a    STRING  -- it should only use labels this metric has
	label_b    STRING
	min_time   TIMESTAMP
	max_time   TIMESTAMP
timestamps []TIMESTAMP
	values_f   []float64 -- for counters / gauges
	values_nh  []NATIVE_HISTOGRAM -- for native histogram. TBD how we encode this
)
```

Conversion between this format, and the flat version is straightforward. Ideally we’d test both versions, and based on benchmarking determine which one is easier to optimise for. It’s kinda CISC vs. RISC. 

It’s open-ended how should native histograms should be represented.
