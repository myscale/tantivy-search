use crate::logger::logger_bridge::TantivySearchLogger;
use crate::CXX_STRING_CONVERTER;
use crate::{common::constants::LOG_CALLBACK, ERROR};
use cxx::CxxString;
use crate::ffi::{FFIBoolResult, FFIError};
use crate::tokenizer::parser::TokenizerUtils;

pub fn ffi_verify_index_parameter(index_json_parameter: &CxxString) -> FFIBoolResult {
    static FUNC_NAME: &str = "ffi_verify_index_parameter";

    let result = CXX_STRING_CONVERTER
        .convert(index_json_parameter)
        .map_err(|e|e.to_string())
        .and_then(|json_parameter| {
            TokenizerUtils::verify_index_json_parameter(json_parameter.as_str())
                .map_err(|e|e.to_string())
        });

    match result {
        Ok(value) => {
            FFIBoolResult {
                result: value,
                error: FFIError { is_error: false, message: String::new() },
            }
        }
        Err(e) => {
            ERROR!(function: FUNC_NAME, "{}", e);
            FFIBoolResult {
                result: false,
                error: FFIError { is_error: true, message: e },
            }
        }
    }
}
