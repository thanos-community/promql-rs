# Vendored Thanos protos

Copies of the Store API and Info API definitions from
[thanos-io/thanos](https://github.com/thanos-io/thanos) at commit
`35b8b991177def87ed52dcf10f9b6d87f07282c8` (main, 2026-09-03), laid out
as in upstream's `pkg/` directory so the `import` lines resolve unchanged:

| here | upstream |
|---|---|
| `store/storepb/rpc.proto` | `pkg/store/storepb/rpc.proto` |
| `store/storepb/types.proto` | `pkg/store/storepb/types.proto` |
| `store/labelpb/types.proto` | `pkg/store/labelpb/types.proto` |
| `info/infopb/rpc.proto` | `pkg/info/infopb/rpc.proto` |

## Edits

None of these change the wire format.

- Removed `import "gogoproto/gogo.proto"`, every file-level
  `option (gogoproto.*)` and every field option such as
  `[(gogoproto.nullable) = false]`. They only steer Go code generation.
  `(gogoproto.customtype) = "labelpb.ZLabel"` on `Series.labels` and
  `ZLabelSet.labels` is a zero-copy Go alias of `Label`; on the wire it
  is `Label { string name = 1; string value = 2; }`, which is what the
  vendored files say.
- Removed from `rpc.proto` the write path this client never uses:
  `service WriteableStore`, `WriteRequest`, `WriteResponse`,
  `TimeSeriesTenantTuple`, and with them the
  `import "store/storepb/prompb/types.proto"`. That keeps the Prometheus
  remote-write protos out of this crate.
- Kept `google.protobuf.Any hints`; prost maps it to `prost_types::Any`.

## Updating

Copy the four files from a newer upstream commit, reapply the edits
above, update the commit hash here, and run `cargo build -p thanos-store`.
`build.rs` compiles them with protox, so no `protoc` is needed.
