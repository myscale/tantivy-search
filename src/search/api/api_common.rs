use crate::logger::logger_bridge::TantivySearchLogger;
use crate::search::implements::free_index_reader;
use crate::search::implements::get_indexed_doc_counts;
use crate::search::implements::load_index_reader;
use crate::CXX_STRING_CONVERTER;
use crate::{common::constants::LOG_CALLBACK, ERROR};
use cxx::CxxString;

pub fn ffi_load_index_reader(index_path: &CxxString) -> bool {
    static FUNC_NAME: &str = "ffi_load_index_reader";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: FUNC_NAME, "Can't convert 'index_path', message: {}", e);
            return false;
        }
    };

    load_index_reader(&index_path).unwrap_or_else(|e| {
        ERROR!(function: FUNC_NAME, "Error loading index reader: {}", e);
        false
    })
}

pub fn ffi_free_index_reader(index_path: &CxxString) -> bool {
    static FUNC_NAME: &str = "ffi_free_index_reader";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: FUNC_NAME, "Can't convert 'index_path', message: {}", e);
            return false;
        }
    };

    free_index_reader(&index_path)
}

pub fn ffi_get_indexed_doc_counts(index_path: &CxxString) -> u64 {
    static FUNC_NAME: &str = "ffi_get_indexed_doc_counts";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: FUNC_NAME, "Can't convert 'index_path', message: {}", e);
            return 0;
        }
    };

    get_indexed_doc_counts(&index_path).unwrap_or_else(|e| {
        ERROR!(function: FUNC_NAME, "Error getting indexed doc counts: {}", e);
        0
    })
}
