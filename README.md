# Quilt OSS

`quilt-core` is the open-source runtime: container lifecycle, networking, oci image workflow, sync/state engine, and local operational tooling.


## Scope

Included in OSS:
- gRPC daemon (`quilt`) and CLI (`cli`)
- Linux container runtime primitives (namespaces, cgroups, process supervision)
- SQLite-backed sync/state engine
- ICC networking stack (bridge, veth, DNS manager, firewall/netlink path)
- Image/registry workflows and runtime support modules
- Volume and metrics paths
- Fozzy scenarios and verification tooling
- NOTE: For native Kubernetes-like cluster management, use [quiltc](https://github.com/ariacomputecompany/quiltc)

## Requirements

- Linux host (cgroups + namespaces enabled)
- Rust toolchain (edition 2021; recent stable recommended)
- `protoc` (for gRPC/protobuf codegen)
- common build tools (`gcc`, `pkg-config`)

## Build

```bash
cargo build --release
```

Binaries:
- `target/release/quilt`
- `target/release/cli`
- `target/release/minit`

## Run

Start daemon:

```bash
./target/release/quilt
```

Basic CLI flow:

```bash
./target/release/cli --help
./target/release/cli list
./target/release/cli images
```

Optional local stack:

```bash
docker compose up --build
```

## Testing

[Fozzy](https://github.com/ariacomputecompany/fozzy) is the primary verification path in this repo.

Recommended full gate:

```bash
fozzy full \
  --scenario-root tests \
  --seed 1337 \
  --doctor-runs 5 \
  --fuzz-time 2s \
  --explore-steps 200 \
  --explore-nodes 3 \
  --allow-expected-failures \
  --require-topology-coverage . \
  --topology-min-risk 60 \
  --topology-profile pedantic \
  --json
```

Additional script tests are in `tests/` (container, ICC, sync, volume, stress, diagnostics).

## Project Layout

```text
src/
  main.rs            # gRPC daemon
  cli/               # CLI client
  daemon/            # runtime + system integration
  sync/              # SQLite state/scheduling/orchestration
  icc/               # networking + messaging
  image/             # OCI image handling
  registry/          # registry client/auth flows
  usage/             # usage event tracking
  utils/             # shared runtime utilities

proto/               # gRPC proto definitions
scripts/             # install/migration/dev helpers
tests/               # shell + fozzy test scenarios
```

## Contributing

- Keep changes scoped and production-safe for self-hosting.
- Validate with Fozzy full gate before PR.
- Avoid introducing cloud-only HTTP/auth/subscription/serverless surface into `quilt-core`.

## License

MIT OR Apache-2.0
