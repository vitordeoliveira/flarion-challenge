# Regexp Extract DataFusion Implementation

Implementation of Spark's `regexp_extract` function for Apache DataFusion.

## Project Status

✅ **Phase 1 Complete**: Project Setup & Environment

- [x] Rust environment verified (v1.87.0)
- [x] Project structure created
- [x] Dependencies configured and verified

## Project Structure

```
regexp_extract_datafusion/
├── Cargo.toml              # Project dependencies and metadata
├── README.md               # This file
├── src/
│   ├── main.rs            # Main entry point
│   ├── lib.rs             # Library exports
│   └── regexp_extract.rs  # Core implementation (TODO)
└── tests/
    └── integration_tests.rs # Integration tests (TODO)
```

## Dependencies

- **DataFusion 49.0.0**: Query engine for implementing the UDF
- **Arrow 56.0.0**: Columnar in-memory analytics
- **Regex 1.10**: Regular expression matching
- **Tokio 1.0**: Async runtime

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

- Takes a string, regex pattern, and group index as parameters
- Returns the specified capture group from the regex match
- Handles edge cases (invalid regex, missing groups, null inputs)
- Provides equivalent functionality to Spark's implementation

## Next Steps

- [ ] Phase 2: Research DataFusion UDF system and Spark behavior
- [ ] Phase 3: Implement the core function logic
- [ ] Phase 4: Comprehensive testing
- [ ] Phase 5: Performance optimization and documentation

## Quick Start

### Using Nix Flake (Recommended)

```bash
# Enter development environment with all tools
nix develop

# Verify tools are available
rustc --version
cargo --version
cargo clippy --version
cargo machete --version
```

### Manual Setup

```bash
# Verify setup
cd regexp_extract_datafusion
cargo check

# Run tests (when implemented)
cargo test
```

## Build Info

- **Rust Version**: 1.87.0
- **Target**: Native compilation
- **Dependencies**: Successfully resolved and compiled
