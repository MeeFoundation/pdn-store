# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

`iroh-docs` implements multi-dimensional key-value *documents* (called **Replicas**) that synchronize between peers using **range-based set reconciliation** (Aljoscha Meyer's algorithm, [paper](https://arxiv.org/abs/2212.13567)).

Two non-obvious facts shape the whole design:

- **Documents store hashes, not content.** Each *Entry* maps a key to the BLAKE3 hash, size, and timestamp of some content — the content bytes themselves are never stored in or transferred through a replica. Actual blob transfer is delegated to `iroh-blobs`.
- **`Docs` is a "meta-protocol."** It composes `iroh-blobs` (content) and `iroh-gossip` (live notification) on top of an `iroh` endpoint. Setting up `Docs` requires wiring up `Blobs` and `Gossip` too (see `examples/setup.rs` and the README).

Entries are signed by two keypairs: a **Namespace** key (write capability; its public `NamespaceId` is the replica's unique id) and an **Author** key (proof of authorship; any number of `AuthorId`s, with app-specific meaning).

## Common commands

This is a single crate, but commands use `--workspace` to match CI.

- **Format** (do NOT use plain `cargo fmt` — a custom nightly config is required): `cargo make format` / check with `cargo make format-check`. Uses `imports_granularity=Crate,group_imports=StdExternalCrate`.
- **Test** (CI uses [nextest](https://nexte.st/)): `cargo nextest run --all-features`
  - Single test: `cargo nextest run test_sync_via_relay` (substring filter) or `-E 'test(test_name)'`.
  - **Doctests are not run by nextest** — run them separately: `cargo test --all-features --doc`.
  - Some tests are `#[ignore]` (flaky); CI runs them in a separate job. `.config/nextest.toml` defines a `run-in-isolation` test group + `ci` profile for tests that must not run concurrently.
- **Lint**: `cargo clippy --workspace --all-features --all-targets`. CI treats warnings as errors (`RUSTFLAGS=-Dwarnings`) and also lints `--no-default-features` and default features.
- **Docs**: `cargo docs-rs` (nightly; gates feature-docs behind the crate-specific `--cfg iroh_docsrs`, not the usual `docsrs`).
- **wasm**: builds for `wasm32-unknown-unknown` with `--no-default-features`. CI asserts the output contains no `import "env"` (i.e. no non-wasm-compatible code leaked in).

CI also runs `cargo deny` (`deny.toml`), `cargo-semver-checks`, and `codespell` (with a custom ignore-words list, skipping `CHANGELOG.md`).

## Pre-push checklist

Run ALL of this locally before the change is committed or pushed — CI (`.github/workflows/ci.yaml`) treats warnings as errors and every item below is a blocking check. One-time setup: `rustup toolchain install nightly`, `rustup target add wasm32-unknown-unknown`, `cargo install cargo-docs-rs`, `brew install cargo-deny` (or `cargo install cargo-deny`).

1. **fmt**: `cargo make format-check`, or without cargo-make: `cargo fmt --all --check -- --config unstable_features=true --config "imports_granularity=Crate,group_imports=StdExternalCrate,reorder_imports=true"`
2. **clippy** — all three feature sets CI checks, each with `RUSTFLAGS=-Dwarnings`:
   - `cargo clippy --workspace --all-features --all-targets --bins --tests --benches`
   - `cargo clippy --workspace --no-default-features --lib --bins --tests`
   - `cargo clippy --workspace --all-targets`
3. **docs**: `RUSTDOCFLAGS=-Dwarnings cargo +nightly docs-rs` — catches what plain `cargo doc` does not (e.g. intra-doc links to private items).
4. **deny**: `cargo deny --workspace --all-features check -Dwarnings`. The advisory database is fetched at run time, so this can turn red with no code change (new advisories, yanked releases). Fix with targeted `cargo update -p <crate>` bumps — check the parent chain on crates.io when the patched version is a semver break (precedent: quick-xml 0.39→0.41 arrived via `cargo update -p plist`).
5. **tests**: `RUSTFLAGS=-Dwarnings cargo test --workspace --all-features --lib --bins --tests`, then doctests: `cargo test --workspace --all-features --doc`. CI runs the same via nextest for all three feature sets (`all` / `none` / `default`) on linux, macOS, and windows.
6. **wasm**: `RUSTFLAGS='--cfg getrandom_backend="wasm_js"' cargo build --target wasm32-unknown-unknown --no-default-features`
7. **codespell** (if installed): `codespell --ignore-words-list=ans,atmost,crate,inout,ratatui,ser,stayin,swarmin,worl --skip=CHANGELOG.md`

Not reproducible locally on macOS and safe to leave to CI: cross builds (freebsd, i686-linux, android) — linux-runner jobs; `cargo-semver-checks` and the MSRV job are `continue-on-error` and cannot redden main.

## Feature flags

`default = ["metrics", "rpc", "fs-store", "redb-v2-migration"]`

- `rpc` — exposes the API over the network (via `noq` + `irpc/rpc`); without it, the API is in-process only.
- `fs-store` — persistent redb file storage; without it, only the in-memory backend is available.
- `redb-v2-migration` — pulls in `redb_v3` to migrate stores written by older redb major versions on open.
- `metrics` — `iroh-metrics` counters.

The test matrix exercises `all` / `none` / `default` feature sets — changes must compile and pass under all three.

## Architecture

Requests flow top-to-bottom; each layer is in its own module. The two key indirections are that **all store access is serialized through a dedicated actor thread**, and **live networking is coordinated by a separate async actor**.

```
Docs (protocol.rs)          ── iroh ProtocolHandler; entry point. Builder: Docs::memory()/persistent(path).spawn(endpoint, blobs, gossip)
  └─ DocsApi (api.rs)       ── irpc client API; derefs from Docs
       └─ RpcActor          ── tokio task; translates DocsProtocol messages → Engine calls (api/actor.rs)
            └─ Engine (engine.rs)        ── coordinates everything below; holds Endpoint, blob store, downloader, default author
                 ├─ SyncHandle (actor.rs)        ── store/replica operations
                 └─ LiveActor (engine/live.rs)   ── live sync coordination
```

**Data model & reconciliation core** (no I/O, no networking):
- `sync.rs` — the big one. `Replica`/`ReplicaInfo`, `SignedEntry`/`Entry`/`Record`/`RecordIdentifier`, `Capability`/`CapabilityKind`. `ProtocolMessage = ranger::Message<SignedEntry>` is what goes on the wire.
- `ranger.rs` — generic range-based set reconciliation. Defines the `RangeEntry`/`RangeKey`/`RangeValue`/`Store` traits and the `Message` exchange. The doc types in `sync.rs` implement these traits.
- `keys.rs` — `Author`/`AuthorId`, `NamespaceSecret`/`NamespaceId`, wrapping `iroh::SecretKey`/`PublicKey`.
- `heads.rs` — `AuthorHeads` (latest timestamp per author), used in sync reports for cheap "are we in sync?" checks.

**Storage** (`store.rs` + `store/fs/`):
- `store::Store` (re-export of `store::fs::Store`) is the *only* store implementation, always backed by [`redb`]. "In-memory" = redb on a `Vec<u8>` backend; "persistent" = redb on a single file. It implements `ranger::Store`, so reconciliation runs directly against redb.
- `store/fs/tables.rs` — redb table layout (records, the `records_by_key` index, namespaces, authors, `latest_per_author`, `namespace_peers`, `download_policy`).
- `store/fs/migrations.rs` runs in-place schema migrations (001–004) automatically on open. `migrate_v1_v2.rs` / `migrate_redb_v2_tuples.rs` handle redb *major-version* upgrades and are gated behind `redb-v2-migration`.
- `DownloadPolicy` / `FilterKind` decide which entries' blobs get downloaded.

**The sync actor** (`actor.rs`): `SyncHandle` is a cheaply-cloneable handle to a dedicated **`std::thread`** (`"sync-actor"`) that owns the `Store` and processes `Action` messages sequentially. It is a thread, not a tokio task, because **redb is blocking** — but on `wasm_browser` it falls back to a tokio task. All replica mutation goes through here; the last handle drop joins the thread. Prefer `SyncHandle::shutdown().await` over relying on drop to avoid blocking an async context.

**The live engine** (`engine/`): `Engine` ties the `SyncHandle`, a `LiveActor`, and a per-document gossip swarm together. `live.rs` is the coordinator — it accepts connections, drives syncs, and reacts to gossip `Op`s (`Put` / `ContentReady` / `SyncReport`) by triggering blob downloads. `state.rs` tracks per-namespace sync state (`Origin`, `SyncReason`); `gossip.rs` manages the swarm. The engine also installs a GC-protect callback so blobs referenced by docs aren't garbage-collected by `iroh-blobs`.

**Networking** (`net.rs` + `net/codec.rs`): ALPN is `/iroh-sync/1`. `connect_and_sync` is the initiator ("Alice"), `handle_connection` the responder ("Bob"); `codec.rs` holds the wire state machines (`run_alice`, `BobState`) that exchange `ranger::Message`s over an iroh QUIC bi-stream.

**RPC layer** (`api/`): `api/protocol.rs` defines `DocsProtocol` via the `irpc::rpc_requests` macro (each variant has `#[rpc(tx = ...)]` reply channels, gated by `rpc_feature = "rpc"`). The same `DocsApi` works in-process (`LocalSender`) or, with the `rpc` feature, over the network.

## Conventions & gotchas

- **MSRV is 1.91** and is duplicated in `.github/workflows/ci.yaml` (`MSRV`) and `Cargo.toml` (`rust-version`) — update both together (there's a comment in `Cargo.toml` noting this).
- `#![deny(missing_docs, rustdoc::broken_intra_doc_links)]` at the crate root: every public item needs a doc comment and intra-doc links must resolve. Some internal modules opt out with `#![allow(missing_docs)]`. `missing_debug_implementations` is also warned.
- `EntrySignature` deliberately wraps `iroh::Signature` (not the raw `ed25519_dalek` type) to keep the on-wire `SignedEntry` format independent of upstream ed25519 serde changes — don't "simplify" this.
- `wasm_browser` is a `cfg` alias defined in `build.rs` (`all(target_family = "wasm", target_os = "unknown")`); use it to gate browser-specific code paths (notably the actor-as-task fallback).
- Property tests use `proptest` + `test-strategy`; regression seeds are checked in under `proptest-regressions/`.
- Releases use `cargo-release` + `git-cliff` (see `release.toml`, `cliff.toml`) to generate `CHANGELOG.md` from **conventional commits** — write commit messages accordingly (`feat!:`, `fix(store):`, `chore:`, `refactor!:`).
