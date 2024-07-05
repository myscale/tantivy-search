use crate::logger::logger_bridge::TantivySearchLogger;
use crate::CXX_STRING_CONERTER;
use crate::{common::constants::LOG_CALLBACK, ERROR};
use cxx::CxxString;
use crate::tokenizer::parser::TantivySearchTokenizerUtils;

pub fn ffi_varify_index_parameter(index_json_parameter: &CxxString) -> bool {
    match CXX_STRING_CONERTER.convert(index_json_parameter) {
        Ok(json_parameter) => {
            TantivySearchTokenizerUtils::verify_index_json_parameter(json_parameter.as_str())
                .unwrap_or_else(|e| {
                    ERROR!(function: "ffi_varify_index_parameter", "{}", e);
                    false
                })
        }
        Err(e) => {
            ERROR!(function: "ffi_varify_index_parameter", "{}", e);
            false
        }
    }
}
