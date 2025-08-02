use std::any::Any;
use std::sync::Arc;

use arrow_array::builder::StringBuilder;
use arrow_array::{Array, ArrayRef, OffsetSizeTrait, StringArray};
use datafusion_common::arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use regex::Regex;

fn extract_input_and_pattern(
    arg1: &ColumnarValue,
    arg2: &ColumnarValue,
    num_rows: usize,
) -> Result<(ArrayRef, ArrayRef)> {
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
        let num_rows = args.number_rows;

        let str_arr_col = &args.args[0];
        let pattern_arr_col = &args.args[1];
        let idx_col = &args.args[2];

        let (str_array, pattern_array) =
            extract_input_and_pattern(str_arr_col, pattern_arr_col, num_rows)?;

        let str_array = str_array
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

        let idx = match idx_col {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(idx))) => *idx,
            _ => {
                return Err(datafusion_common::DataFusionError::Internal(
                    "Expected a single Int64 for the index".to_string(),
                ))
            }
        };

        let mut string_builder = StringBuilder::new();
        for i in 0..num_rows {
            if str_array.is_null(i) {
                string_builder.append_null();
                continue;
            }
            let str_val = str_array.value(i);
            let pattern = pattern_array.value(i);

            let compiled_regex = match Regex::new(pattern) {
                Ok(re) => re,
                Err(e) => {
                    return Err(datafusion_common::DataFusionError::Execution(format!(
                        "Error compiling regex: {}",
                        e
                    )))
                }
            };

            if let Some(captures) = compiled_regex.captures(str_val) {
                if idx < captures.len() as i64 {
                    string_builder.append_value(captures.get(idx as usize).unwrap().as_str());
                } else {
                    string_builder.append_value("");
                }
            } else {
                string_builder.append_value("");
            }
        }
        Ok(ColumnarValue::Array(Arc::new(string_builder.finish())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, StringArray};
    use datafusion_common::arrow::datatypes::Field;
    use datafusion_common::ScalarValue;
    use datafusion_expr::ColumnarValue;
    use std::sync::Arc;

    #[test]
    fn test_regexp_extract_basic() {
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(StringArray::from(vec![
                    "100-200", "300-400", "500-600",
                ]))),
                ColumnarValue::Array(Arc::new(StringArray::from(vec![
                    r"(\d+)-(\d+)",
                    r"(\d+)-(\d+)",
                    r"(\d+)-(\d+)",
                ]))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            ],
            number_rows: 3,
            arg_fields: vec![],
            return_field: Arc::new(Field::new("result", DataType::Utf8, true)),
        };

        let result = RegexpExtract::new().invoke_with_args(args).unwrap();

        match result {
            ColumnarValue::Array(array) => {
                let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(string_array.value(0), "100");
                assert_eq!(string_array.value(1), "300");
                assert_eq!(string_array.value(2), "500");
            }
            _ => panic!("Expected an array"),
        }
    }
}
