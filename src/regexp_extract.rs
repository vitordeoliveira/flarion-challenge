use std::any::Any;
use std::sync::Arc;

use arrow_array::builder::StringBuilder;
use arrow_array::{Array, ArrayRef, StringArray};
use datafusion_common::arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use regex::Regex;

fn extract_input_and_pattern(
    arg1: &ColumnarValue,
    arg2: &ColumnarValue,
    num_rows: usize,
) -> Result<(ArrayRef, ArrayRef)> {
    // looks like we need to check the first argument to check if
    // it is a scalar because
    // of the case SELECT regexp_extract('100-200', '(\\d+)', 1) FROM my_table;
    // in this case optimer will convert the first option to a scalar
    let input_array = match arg1 {
        ColumnarValue::Array(array) => array.clone(),
        ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(num_rows)?,
    };
    let pattern_array = match arg2 {
        ColumnarValue::Array(array) => array.clone(),
        ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(num_rows)?,
    };
    Ok((input_array, pattern_array))
}

#[derive(Debug, Clone)]
pub struct RegexpExtract {
    signature: Signature,
}

impl Default for RegexpExtract {
    fn default() -> Self {
        Self::new()
    }
}

impl RegexpExtract {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8, DataType::Int64],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RegexpExtract {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "regexp_extract"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // --- Step 1: Get Batch Size ---
        // Let's trace a query: SELECT regexp_extract(http_log, '(\\d+\\.\\d+\\.\\d+\\.\\d+)', 1) FROM logs
        // If our current batch has 1 row, num_rows will be 1.
        let num_rows = args.number_rows;

        // --- Step 2: Get Raw Arguments ---
        // These are the inputs exactly as the engine provides them.
        // http_log_col:  ColumnarValue::Array(["1.2.3.4 - GET /index.html 200"])
        // pattern_col:   ColumnarValue::Scalar("(\\d+\\.\\d+\\.\\d+\\.\\d+)")
        // idx_col:       ColumnarValue::Scalar(1)
        let http_log_col = &args.args[0];
        let pattern_col = &args.args[1];
        let idx_col = &args.args[2];

        // --- Step 3: Normalize Inputs to Arrays ---
        // Our helper function ensures everything is an array of `num_rows`.
        // Scalars are broadcast into arrays.
        // input_array:   ["1.2.3.4 - GET /index.html 200"]
        // pattern_array: ["(\\d+\\.\\d+\\.\\d+\\.\\d+)", "(\\d+\\.\\d+\\.\\d+\\.\\d+)", ...]
        let (input_array, pattern_array) =
            extract_input_and_pattern(http_log_col, pattern_col, num_rows)?;

        // --- Step 4: Downcast to Specific Array Types ---
        // We convert the generic `ArrayRef` to the concrete `StringArray` we need.
        let input_array = input_array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal("Expected a StringArray".to_string())
            })?;
        let pattern_array = pattern_array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "Expected a StringArray for pattern".to_string(),
                )
            })?;

        // --- Step 5: Extract Scalar Index ---
        // We get the single integer value for the group index.
        // idx -> 1
        let idx = match idx_col {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(idx))) => *idx,
            _ => {
                return Err(datafusion_common::DataFusionError::Internal(
                    "Expected a single Int64 for the index".to_string(),
                ));
            }
        };

        // --- Step 6: Prepare Output Builder ---
        // An Arrow builder for efficiently creating the output `StringArray`.
        let mut string_builder = StringBuilder::new();

        // --- Step 7: Iterate and Process Each Row ---
        for i in 0..num_rows {
            if input_array.is_null(i) {
                string_builder.append_null();
                continue;
            }

            // --- Step 7a: Get Row-Specific Values ---
            // For our example row (i=0):
            // input_val -> "1.2.3.4 - GET /index.html 200"
            // pattern   -> "(\\d+\\.\\d+\\.\\d+\\.\\d+)"
            let input_val = input_array.value(i);
            let pattern = pattern_array.value(i);

            // --- Step 7b: Compile Regex ---
            let compiled_regex = match Regex::new(pattern) {
                Ok(re) => re,
                Err(e) => {
                    return Err(datafusion_common::DataFusionError::Execution(format!(
                        "Error compiling regex: {e}"
                    )));
                }
            };

            // --- Step 7c: Execute Regex and Append Result ---
            if let Some(captures) = compiled_regex.captures(input_val) {
                // The pattern matches "1.2.3.4".
                // captures[0] -> "1.2.3.4" (the full match)
                // captures[1] -> "1.2.3.4" (the first and only group)
                if idx < captures.len() as i64 {
                    // Our index is 1, which is valid. We get the string "1.2.3.4".
                    string_builder.append_value(captures.get(idx as usize).unwrap().as_str());
                } else {
                    string_builder.append_value("");
                }
            } else {
                string_builder.append_value("");
            }
        }

        // --- Step 8: Finalize and Return Result Array ---
        // The builder is finalized into a new Arrow Array.
        // For our example, this will be a StringArray containing ["1.2.3.4"].
        Ok(ColumnarValue::Array(Arc::new(string_builder.finish())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::StringArray;
    use datafusion_common::ScalarValue;
    use datafusion_common::arrow::datatypes::Field;
    use datafusion_expr::ColumnarValue;
    use std::sync::Arc;

    // Helper to run a test and assert the output
    fn run_test(
        input: ColumnarValue,
        pattern: ColumnarValue,
        index: ColumnarValue,
        expected_values: Vec<Option<&str>>,
        num_rows: usize,
    ) {
        let args = ScalarFunctionArgs {
            args: vec![input, pattern, index],
            number_rows: num_rows,
            arg_fields: vec![], // Not used in this UDF
            return_field: Arc::new(Field::new("result", DataType::Utf8, true)),
        };

        let result = RegexpExtract::new().invoke_with_args(args).unwrap();

        let expected = StringArray::from(expected_values);

        match result {
            ColumnarValue::Array(array) => {
                let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(string_array, &expected);
            }
            _ => panic!("Expected an array result"),
        }
    }

    // Helper to run a test that is expected to fail
    fn run_test_error(
        input: ColumnarValue,
        pattern: ColumnarValue,
        index: ColumnarValue,
        num_rows: usize,
        expected_error_msg: &str,
    ) {
        let args = ScalarFunctionArgs {
            args: vec![input, pattern, index],
            number_rows: num_rows,
            arg_fields: vec![],
            return_field: Arc::new(Field::new("result", DataType::Utf8, true)),
        };

        let result = RegexpExtract::new().invoke_with_args(args);
        match result {
            Ok(_) => panic!("Expected an error but got Ok"),
            Err(e) => {
                assert!(
                    e.to_string().contains(expected_error_msg),
                    "Error message '{e}' did not contain expected substring '{expected_error_msg}'"
                );
            }
        }
    }

    #[test]
    fn test_regexp_extract_basic() {
        run_test(
            ColumnarValue::Array(Arc::new(StringArray::from(vec![
                "100-200", "300-400", "500-600",
            ]))),
            ColumnarValue::Scalar(ScalarValue::from(r"(\d+)-(\d+)")),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            vec![Some("100"), Some("300"), Some("500")],
            3,
        );
    }

    #[test]
    fn test_no_match() {
        run_test(
            ColumnarValue::Array(Arc::new(StringArray::from(vec!["abc", "def-ghi"]))),
            ColumnarValue::Scalar(ScalarValue::from(r"(\d+)")),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            vec![Some(""), Some("")],
            2,
        );
    }

    #[test]
    fn test_group_index_out_of_bounds() {
        run_test(
            ColumnarValue::Array(Arc::new(StringArray::from(vec!["100-200"]))),
            ColumnarValue::Scalar(ScalarValue::from(r"(\d+)-(\d+)")), // 2 capture groups
            ColumnarValue::Scalar(ScalarValue::Int64(Some(3))),       // Requesting 3rd group
            vec![Some("")],
            1,
        );
    }

    #[test]
    fn test_null_input_string() {
        run_test(
            ColumnarValue::Array(Arc::new(StringArray::from(vec![
                Some("100-200"),
                None,
                Some("500-600"),
            ]))),
            ColumnarValue::Scalar(ScalarValue::from(r"(\d+)-(\d+)")),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            vec![Some("100"), None, Some("500")],
            3,
        );
    }

    #[test]
    fn test_group_zero_for_full_match() {
        run_test(
            ColumnarValue::Array(Arc::new(StringArray::from(vec!["foo-bar", "baz-qux"]))),
            ColumnarValue::Scalar(ScalarValue::from(r"([a-z]+)-([a-z]+)")),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(0))), // Group 0 for the whole match
            vec![Some("foo-bar"), Some("baz-qux")],
            2,
        );
    }

    #[test]
    fn test_scalar_input_string_with_array_pattern() {
        run_test(
            ColumnarValue::Scalar(ScalarValue::from("data-fusion")),
            ColumnarValue::Array(Arc::new(StringArray::from(vec![
                r"([a-z]+)-",
                r"-([a-z]+)",
            ]))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            vec![Some("data"), Some("fusion")],
            2,
        );
    }

    #[test]
    fn test_invalid_regex_pattern() {
        run_test_error(
            ColumnarValue::Array(Arc::new(StringArray::from(vec!["a"]))),
            ColumnarValue::Scalar(ScalarValue::from("[invalid-regex")),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            1,
            "Error compiling regex",
        );
    }

    #[test]
    fn test_unicode_characters() {
        run_test(
            ColumnarValue::Array(Arc::new(StringArray::from(vec!["Gö-del", "你好-世界"]))),
            ColumnarValue::Scalar(ScalarValue::from(r"(.+)-(.+)")),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
            vec![Some("del"), Some("世界")],
            2,
        );
    }

    #[test]
    fn test_group_two_extraction() {
        run_test(
            ColumnarValue::Array(Arc::new(StringArray::from(vec!["100-200", "300-400"]))),
            ColumnarValue::Scalar(ScalarValue::from(r"(\d+)-(\d+)")),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(2))), // Requesting group 2
            vec![Some("200"), Some("400")],
            2,
        );
    }
}
