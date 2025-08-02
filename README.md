# Regexp Extract DataFusion Implementation

Implementation of Spark's `regexp_extract` function for Apache DataFusion, built as a User Defined Function (UDF).

## Project Status

- ✅ **Phase 1: Project Setup**: Complete
- ✅ **Phase 2: Research**: Complete
- ✅ **Phase 3: Implementation**: Complete
- ✅ **Phase 4: Comprehensive Testing**: Complete
- 🚀 **Phase 6: Spark GroupBy Analysis**: In Progress

The core function is implemented, robustly tested, and benchmarked. Phase 5 (Advanced Features & Polish) has been skipped to focus on comparative analysis.

## Project Structure

```
regexp_extract_datafusion/
├── Cargo.toml              # Project dependencies and metadata
├── README.md               # This file
├── benches/
│   └── regexp_benchmark.rs  # Performance benchmarks
├── src/
│   ├── main.rs            # Main entry point for live demos
│   ├── lib.rs             # Library exports
│   └── regexp_extract.rs  # Core `regexp_extract` implementation
└── tests/
    └── integration_tests.rs # SQL and DataFrame API integration tests
```

## Dependencies

- **DataFusion 49.0.0**: Query engine for implementing the UDF.
- **Arrow 55.2.0**: Columnar in-memory analytics.
- **Regex 1.10**: Core regular expression matching.
- **Tokio 1.0**: Asynchronous runtime.
- **Criterion 0.5**: Performance benchmarking.

## Development Tools (via Nix Flake)

- **rustc**: Rust compiler
- **cargo**: Package manager and build tool
- **rustfmt**: Code formatter (`cargo fmt`)
- **rust-analyzer**: Language server for IDE support
- **clippy**: Linter (`cargo clippy`)
- **cargo-machete**: Remove unused dependencies (`cargo machete`)
- **cargo-edit**: Add/remove dependencies (`cargo add`, `cargo rm`)

## Goal

Implement Spark's `regexp_extract(str, pattern, idx)` function as a User Defined Function (UDF) in DataFusion that:

- Takes a string, regex pattern, and group index as parameters.
- Returns the specified capture group from the regex match.
- Handles edge cases (invalid regex, missing groups, null inputs) correctly.
- Provides equivalent functionality to Spark's implementation.

## Next Steps

The implementation is complete and ready for comparative analysis with Spark's GroupBy performance.

## Quick Start

### Using Nix Flake (Recommended)

```bash
# Enter development environment with all tools
nix develop

# Verify tools are available
rustc --version
cargo --version
```

### Manual Setup

```bash
# Navigate to project directory
cd regexp_extract_datafusion

# Check to ensure dependencies resolve
cargo check

# Run the unit and integration tests
cargo test

# Run the performance benchmarks
cargo bench
```
