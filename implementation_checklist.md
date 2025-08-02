# Implementation Behavior Checklist

This document verifies the `regexp_extract` implementation against Spark's expected behavior.

### 1. `NULL` Input Handling

- [x] **Status**: Implemented.
- **Behavior**: Returns `NULL` if the input expression is `NULL`.
- **Verification**: The main loop explicitly checks `input_array.is_null(i)` and appends `NULL` to the result.

### 2. Empty String Returns

- [x] **Status**: Implemented.
- **Behavior**: Returns an empty string (`""`) for:
  - Inputs that do not match the regex pattern.
  - A `groupIndex` that is out of the bounds of the actual matches.
- **Verification**: The `else` blocks for both the pattern match (`if let Some(captures)`) and the index check (`if idx < captures.len()`) correctly append an empty string.

### 3. Group Index `0`

- [x] **Status**: Implemented.
- **Behavior**: Correctly handles group index `0` to mean the entire match.
- **Verification**: The code uses `captures.get(idx as usize)`, which correctly returns the full match when `idx` is `0`.

### 4. Positive Group Indices

- [x] **Status**: Implemented.
- **Behavior**: Correctly maps positive group indices to their corresponding capture groups.
- **Verification**: `captures.get(idx as usize)` correctly maps the index to the capture group.

### 5. Error Handling

- [x] **Status**: Implemented.
- **Behavior**:
  1.  **Invalid Regex Pattern**: Produces an error at execution time.
  2.  **Negative Group Index**: Produces an error.
- **Verification**:
  - **Invalid Regex**: An error is correctly returned at execution time, which is standard for a UDF where the pattern may be dynamic (i.e., come from a column).
  - **Negative Group Index**: A check `if idx < 0` has been added, and a unit test confirms it returns an `Execution` error instead of panicking.
