# Project Guidelines

## Code Style
- Use the repo lint baseline from `analysis_options.yaml` (`flutter_lints`) and keep Dart style consistent with `example/lib/main.dart` and `example/integration_test/simple_test.dart`.
- Follow Rust naming and error conventions used in `rust/src/api/*.rs`: `snake_case` functions, `UpperCamelCase` types, and `Result<_, LanceError>` return patterns.
- Preserve `flutter_rust_bridge` attributes (`#[flutter_rust_bridge::frb(...)]`) in `rust/src/api/*` when exposing APIs.

## Architecture
- Public Dart package surface is `lib/flutter_lancedb.dart` (exports generated API wrappers and `RustLib`).
- Bridge contract is defined by `flutter_rust_bridge.yaml` (`rust_input: crate::api`, `dart_output: lib/src/rust`).
- Rust API modules exposed to Dart are in `rust/src/api/` (`connection.rs`, `table.rs`, `query.rs`, `types.rs`, `simple.rs`).
- Memory-sensitive opaque holder logic is isolated in `rust/src/internal.rs`.

## Build and Test
Run commands from repo root unless noted.

- Install deps: `flutter pub get`
- Analyze plugin: `flutter analyze`
- Rust compile check: `cd rust && cargo check -q`
- Rust tests/sanity: `cd rust && cargo test -q`
- Example deps/analyze: `cd example && flutter pub get && flutter analyze`
- Example integration test file: `cd example && flutter test integration_test/simple_test.dart`
- Driver integration run (desktop): `cd example && flutter drive --driver ../test_driver/integration_test.dart --target integration_test/simple_test.dart -d macos`
- Example macOS build: `cd example && flutter build macos --debug`
- Regenerate FRB bindings after Rust API changes: `dart run flutter_rust_bridge_codegen generate --config-file flutter_rust_bridge.yaml`

## Project Conventions
- Do not hand-edit generated bridge files. Regenerate instead:
  - `rust/src/frb_generated.rs`
  - `lib/src/rust/frb_generated*.dart`
  - `lib/src/rust/api/*.dart` (including `types.freezed.dart`)
- Make behavioral changes in handwritten Rust sources under `rust/src/api/` and expose them through FRB.
- Ensure app/test startup initializes bridge first (`await RustLib.init();`) as in `example/lib/main.dart` and `example/integration_test/simple_test.dart`.

## Integration Points
- Flutter ↔ Rust: calls flow through `RustLib.instance.api...` in `lib/src/rust/frb_generated.dart`.
- Rust ↔ LanceDB: database/query/index operations are implemented with `lancedb` crate in `rust/src/api/connection.rs`, `rust/src/api/table.rs`, and `rust/src/api/query.rs`.
- Native build tooling:
  - Android via Cargokit Gradle (`android/build.gradle`, `cargokit/gradle/plugin.gradle`)
  - iOS/macOS via podspec + `cargokit/build_pod.sh`
  - Linux/Windows via CMake + `cargokit/cmake/cargokit.cmake`

## Security
- Treat `rust/src/internal.rs` as high-risk: it contains `unsafe` pointer erasure and manual memory ownership.
- Treat query strings/predicates (`filter`, `predicate`, `only_if`) and update payloads as untrusted input in `rust/src/api/table.rs` and `rust/src/api/query.rs`.
- `connect(uri)` in `rust/src/api/connection.rs` accepts caller-provided URIs; validate sources at call sites.
- Be careful sharing build logs: `cargokit/build_pod.sh` prints environment variables during some build phases.