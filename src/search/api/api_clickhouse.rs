use crate::search::implements::query_sentence_bitmap;
use crate::search::implements::query_term_bitmap;
use crate::search::implements::query_terms_bitmap;
use crate::search::implements::regex_term_bitmap;
use crate::CXX_STRING_CONVERTER;
use crate::CXX_VECTOR_STRING_CONVERTER;
use cxx::CxxString;
use cxx::CxxVector;
use crate::ffi::{FFIError, FFIVecU8Result};
use crate::utils::api_utils::ApiUtils;

pub fn ffi_query_term_bitmap(
    index_path: &CxxString,
    column_name: &CxxString,
    term: &CxxString,
) -> FFIVecU8Result {
    static FUNC_NAME: &str = "ffi_query_term_bitmap";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't convert 'index_path'", e.to_string());
        }
    };
    let column_name: String = match CXX_STRING_CONVERTER.convert(column_name) {
        Ok(name) => name,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't convert 'column_name'", e.to_string());
        }
    };
    let term: String = match CXX_STRING_CONVERTER.convert(term) {
        Ok(q) => q,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't convert 'term'", e.to_string());
        }
    };

    match query_term_bitmap(&index_path, &column_name, &term) {
        Ok(result) => FFIVecU8Result {
            result,
            error: FFIError {
                is_error: false,
                message: String::new(),
            },
        },
        Err(e) => {
            ApiUtils::handle_error(FUNC_NAME, "Error happened when execute `query_term_bitmap`", e.to_string())
        }
    }
}

pub fn ffi_query_terms_bitmap(
    index_path: &CxxString,
    column_name: &CxxString,
    terms: &CxxVector<CxxString>,
) -> FFIVecU8Result {
    static FUNC_NAME: &str = "ffi_query_terms_bitmap";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't convert 'index_path'", e.to_string());
        }
    };
    let column_name: String = match CXX_STRING_CONVERTER.convert(column_name) {
        Ok(name) => name,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't convert 'column_name'", e.to_string());
        }
    };
    let terms: Vec<String> = match CXX_VECTOR_STRING_CONVERTER.convert(terms) {
        Ok(ts) => ts,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't convert 'terms'", e.to_string());
        }
    };

    match query_terms_bitmap(&index_path, &column_name, &terms) {
        Ok(result) => FFIVecU8Result {
            result,
            error: FFIError {
                is_error: false,
                message: String::new(),
            },
        },
        Err(e) => {
            ApiUtils::handle_error(FUNC_NAME, "Error happened when execute `query_terms_bitmap`", e.to_string())
        }
    }
}

pub fn ffi_query_sentence_bitmap(
    index_path: &CxxString,
    column_name: &CxxString,
    sentence: &CxxString,
) -> FFIVecU8Result {
    static FUNC_NAME: &str = "ffi_query_sentence_bitmap";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't convert 'index_path'", e.to_string());
        }
    };
    let column_name: String = match CXX_STRING_CONVERTER.convert(column_name) {
        Ok(name) => name,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't convert 'column_name'", e.to_string());
        }
    };
    let sentence: String = match CXX_STRING_CONVERTER.convert(sentence) {
        Ok(se) => se,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't convert 'sentence'", e.to_string());
        }
    };

    match query_sentence_bitmap(&index_path, &column_name, &sentence) {
        Ok(result) => FFIVecU8Result {
            result,
            error: FFIError {
                is_error: false,
                message: String::new(),
            },
        },
        Err(e) => {
            ApiUtils::handle_error(FUNC_NAME, "Error happened when execute `query_sentence_bitmap`", e.to_string())
        }
    }
}

pub fn ffi_regex_term_bitmap(
    index_path: &CxxString,
    column_name: &CxxString,
    pattern: &CxxString,
) -> FFIVecU8Result {
    static FUNC_NAME: &str = "ffi_regex_term_bitmap";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't convert 'index_path'", e.to_string());
        }
    };
    let column_name: String = match CXX_STRING_CONVERTER.convert(column_name) {
        Ok(name) => name,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't convert 'column_name'", e.to_string());
        }
    };
    let pattern: String = match CXX_STRING_CONVERTER.convert(pattern) {
        Ok(se) => se,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't convert 'pattern'", e.to_string());
        }
    };

    match regex_term_bitmap(&index_path, &column_name, &pattern) {
        Ok(result) => FFIVecU8Result {
            result,
            error: FFIError {
                is_error: false,
                message: String::new(),
            },
        },
        Err(e) => {
            ApiUtils::handle_error(FUNC_NAME, "Error happened when execute `regex_term_bitmap`", e.to_string())
        }
    }
}
