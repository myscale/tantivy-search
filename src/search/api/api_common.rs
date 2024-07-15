use crate::search::implements::free_index_reader;
use crate::search::implements::get_indexed_doc_counts;
use crate::search::implements::load_index_reader;
use crate::CXX_STRING_CONVERTER;
use cxx::CxxString;
use crate::ffi::{FFIBoolResult, FFIError, FFIU64Result};
use crate::utils::api_utils::ApiUtils;

pub fn ffi_load_index_reader(index_path: &CxxString) -> FFIBoolResult {
    static FUNC_NAME: &str = "ffi_load_index_reader";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't convert 'index_path'", e.to_string());
        }
    };

    match load_index_reader(&index_path) {
        Ok(result) => FFIBoolResult {
            result,
            error: FFIError {
                is_error: false,
                message: String::new(),
            },
        },
        Err(e) => {
            ApiUtils::handle_error(FUNC_NAME, "Error loading index reader", e.to_string())
        }
    }
}

pub fn ffi_free_index_reader(index_path: &CxxString) -> FFIBoolResult {
    static FUNC_NAME: &str = "ffi_free_index_reader";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't convert 'index_path'", e.to_string());
        }
    };

    FFIBoolResult {
        result: free_index_reader(&index_path),
        error: FFIError {
            is_error: false,
            message: String::new(),
        },
    }
}

pub fn ffi_get_indexed_doc_counts(index_path: &CxxString) -> FFIU64Result {
    static FUNC_NAME: &str = "ffi_get_indexed_doc_counts";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't convert 'index_path'", e.to_string());
        }
    };

    match get_indexed_doc_counts(&index_path) {
        Ok(result) => FFIU64Result {
            result,
            error: FFIError {
                is_error: false,
                message: String::new(),
            },
        },
        Err(e) => {
            ApiUtils::handle_error(FUNC_NAME, "Error getting indexed doc counts", e.to_string())
        }
    }
}
