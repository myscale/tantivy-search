use crate::logger::logger_bridge::TantivySearchLogger;
use crate::CXX_STRING_CONVERTER;
use crate::{common::constants::LOG_CALLBACK, ERROR};
use cxx::CxxString;
use crate::tokenizer::parser::TokenizerUtils;

pub fn ffi_verify_index_parameter(index_json_parameter: &CxxString) -> bool {
    static FUNC_NAME: &str = "ffi_verify_index_parameter";

    match CXX_STRING_CONVERTER.convert(index_json_parameter) {
        Ok(json_parameter) => {
            TokenizerUtils::verify_index_json_parameter(json_parameter.as_str())
                .unwrap_or_else(|e| {
                    ERROR!(function: FUNC_NAME, "{}", e);
                    false
                })
        }
        Err(e) => {
            ERROR!(function: FUNC_NAME, "{}", e);
            false
        }
    }
}
