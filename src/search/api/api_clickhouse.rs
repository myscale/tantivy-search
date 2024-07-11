use crate::logger::logger_bridge::TantivySearchLogger;
use crate::search::implements::query_sentence_bitmap;
use crate::search::implements::query_term_bitmap;
use crate::search::implements::query_terms_bitmap;
use crate::search::implements::regex_term_bitmap;
use crate::CXX_STRING_CONVERTER;
use crate::CXX_VECTOR_STRING_CONVERTER;
use crate::{common::constants::LOG_CALLBACK, ERROR};
use cxx::CxxString;
use cxx::CxxVector;

pub fn ffi_query_term_bitmap(
    index_path: &CxxString,
    column_name: &CxxString,
    term: &CxxString,
) -> Vec<u8> {
    static FUNC_NAME: &str = "ffi_query_term_bitmap";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: FUNC_NAME, "Can't convert 'index_path', message: {}", e);
            return Vec::new();
        }
    };
    let column_name: String = match CXX_STRING_CONVERTER.convert(column_name) {
        Ok(name) => name,
        Err(e) => {
            ERROR!(function: FUNC_NAME, "Can't convert 'column_name', message: {}", e);
            return Vec::new();
        }
    };
    let term: String = match CXX_STRING_CONVERTER.convert(term) {
        Ok(q) => q,
        Err(e) => {
            ERROR!(function: FUNC_NAME, "Can't convert 'term', message: {}", e);
            return Vec::new();
        }
    };

    query_term_bitmap(&index_path, &column_name, &term).unwrap_or_else(|e| {
        ERROR!(function: FUNC_NAME, "Error happened. {}", e);
        Vec::new()
    })
}

pub fn ffi_query_terms_bitmap(
    index_path: &CxxString,
    column_name: &CxxString,
    terms: &CxxVector<CxxString>,
) -> Vec<u8> {
    static FUNC_NAME: &str = "ffi_query_terms_bitmap";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: FUNC_NAME, "Can't convert 'index_path', message: {}", e);
            return Vec::new();
        }
    };
    let column_name: String = match CXX_STRING_CONVERTER.convert(column_name) {
        Ok(name) => name,
        Err(e) => {
            ERROR!(function: FUNC_NAME, "Can't convert 'column_name', message: {}", e);
            return Vec::new();
        }
    };
    let terms: Vec<String> = match CXX_VECTOR_STRING_CONVERTER.convert(terms) {
        Ok(ts) => ts,
        Err(e) => {
            ERROR!(function: FUNC_NAME, "Can't convert 'terms', message: {}", e);
            return Vec::new();
        }
    };

    query_terms_bitmap(&index_path, &column_name, &terms).unwrap_or_else(|e| {
        ERROR!(function: FUNC_NAME, "Error happened. {}", e);
        Vec::new()
    })
}

pub fn ffi_query_sentence_bitmap(
    index_path: &CxxString,
    column_name: &CxxString,
    sentence: &CxxString,
) -> Vec<u8> {
    static FUNC_NAME: &str = "ffi_query_sentence_bitmap";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: FUNC_NAME, "Can't convert 'index_path', message: {}", e);
            return Vec::new();
        }
    };
    let column_name: String = match CXX_STRING_CONVERTER.convert(column_name) {
        Ok(name) => name,
        Err(e) => {
            ERROR!(function: FUNC_NAME, "Can't convert 'column_name', message: {}", e);
            return Vec::new();
        }
    };
    let sentence: String = match CXX_STRING_CONVERTER.convert(sentence) {
        Ok(se) => se,
        Err(e) => {
            ERROR!(function: FUNC_NAME, "Can't convert 'sentence', message: {}", e);
            return Vec::new();
        }
    };

    query_sentence_bitmap(&index_path, &column_name, &sentence).unwrap_or_else(|e| {
        ERROR!(function: FUNC_NAME, "Error happened. {}", e);
        Vec::new()
    })
}

pub fn ffi_regex_term_bitmap(
    index_path: &CxxString,
    column_name: &CxxString,
    pattern: &CxxString,
) -> Vec<u8> {
    static FUNC_NAME: &str = "ffi_regex_term_bitmap";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: FUNC_NAME, "Can't convert 'index_path', message: {}", e);
            return Vec::new();
        }
    };
    let column_name: String = match CXX_STRING_CONVERTER.convert(column_name) {
        Ok(name) => name,
        Err(e) => {
            ERROR!(function: FUNC_NAME, "Can't convert 'column_name', message: {}", e);
            return Vec::new();
        }
    };
    let pattern: String = match CXX_STRING_CONVERTER.convert(pattern) {
        Ok(se) => se,
        Err(e) => {
            ERROR!(function: FUNC_NAME, "Can't convert 'pattern', message: {}", e);
            return Vec::new();
        }
    };

    regex_term_bitmap(&index_path, &column_name, &pattern).unwrap_or_else(|e| {
        ERROR!(function: FUNC_NAME, "Error happened. {}", e);
        Vec::new()
    })
}
