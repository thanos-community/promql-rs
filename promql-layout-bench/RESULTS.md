# promql-layout-bench results

DataFusion 55.0.0. 24 target partitions, each holding a contiguous run of whole series. Both layouts arrive in batches of about 8,192 samples: that many rows when rows are samples, as many whole series as fit when rows are series. Scrape interval 30s. Every cell is the median of 5 runs after one warmup.

Both candidates run the same series-aware `RangeVectorExec`: `struct_ree · operator` feeds it rows as samples, `list · operator` rows as series. `rate`, `+unnest` and `full` are cumulative cuts of the same plan: the range vector alone, then unnested to one row per (series, step), then the whole query. Peak alloc is allocation above the level before the query, from a counting allocator.

## rate[5m], instant · 708 series × 10 samples = 7,080 samples

### sum by (code)

| candidate | df rows | batches | labels | rate | full | peak alloc | out rows |
|---|---:|---:|---:|---:|---:|---:|---:|
| struct_ree · operator | 7,080 | 24 | 212.8 KB | 2.5 ms | 4.8 ms | 3.8 MB | 7 |
| list · operator | 708 | 24 | 189.7 KB | 0.7 ms | 3.1 ms | 2.9 MB | 7 |

## rate[5m], 1h range at 15s · 708 series × 130 samples = 92,040 samples

### sum by (code), range

| candidate | df rows | batches | labels | rate | +unnest | full | peak alloc | out rows |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| struct_ree · operator | 92,040 | 24 | 212.8 KB | 2.8 ms | 4.0 ms | 8.0 ms | 10.1 MB | 1,680 |
| list · operator | 708 | 24 | 189.7 KB | 1.2 ms | 2.1 ms | 7.0 ms | 11.4 MB | 1,680 |

### topk(3), range

| candidate | df rows | batches | labels | rate | +unnest | full | peak alloc | out rows |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| struct_ree · operator | 92,040 | 24 | 212.8 KB | 2.6 ms | 4.1 ms | 12.9 ms | 8.2 MB | 720 |
| list · operator | 708 | 24 | 189.7 KB | 1.3 ms | 2.1 ms | 11.6 ms | 8.8 MB | 720 |

## rate[5m], 1d range at 1m · 708 series × 2,888 samples = 2,044,704 samples

### sum by (code), range

| candidate | df rows | batches | labels | rate | +unnest | full | peak alloc | out rows |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| struct_ree · operator | 2,044,704 | 264 | 212.8 KB | 11.6 ms | 11.7 ms | 16.7 ms | 16.0 MB | 10,080 |
| list · operator | 708 | 360 | 189.7 KB | 8.5 ms | 8.4 ms | 14.0 ms | 13.4 MB | 10,080 |

### topk(3), range

| candidate | df rows | batches | labels | rate | +unnest | full | peak alloc | out rows |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| struct_ree · operator | 2,044,704 | 264 | 212.8 KB | 10.8 ms | 10.8 ms | 44.9 ms | 57.5 MB | 4,320 |
| list · operator | 708 | 360 | 189.7 KB | 8.7 ms | 8.5 ms | 46.8 ms | 62.5 MB | 4,320 |

## rate[1d], instant · 708 series × 2,880 samples = 2,039,040 samples

### sum by (code)

| candidate | df rows | batches | labels | rate | full | peak alloc | out rows |
|---|---:|---:|---:|---:|---:|---:|---:|
| struct_ree · operator | 2,039,040 | 264 | 212.8 KB | 12.2 ms | 9.9 ms | 5.3 MB | 7 |
| list · operator | 708 | 360 | 189.7 KB | 7.5 ms | 5.8 ms | 4.3 MB | 7 |

## increase[2w], instant · 177 series × 40,320 samples = 7,136,640 samples

### sum by (code)

| candidate | df rows | batches | labels | rate | full | peak alloc | out rows |
|---|---:|---:|---:|---:|---:|---:|---:|
| struct_ree · operator | 7,136,640 | 885 | 177.2 KB | 19.4 ms | 17.6 ms | 17.1 MB | 7 |
| list · operator | 177 | 177 | 141.8 KB | 4.8 ms | 5.1 ms | 3.5 MB | 7 |
