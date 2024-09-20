use crate::{cxx_vector_converter, CXX_VECTOR_STRING_CONVERTER};
use crate::ffi::{FFIError, FFIFieldTokenNumsResult, FFIU64Result, FFIVecDocWithFreqResult, FFIVecRowIdWithScoreResult, Statistics};
use crate::search::implements::{bm25_natural_language_search, bm25_standard_search, get_doc_freq,
                                get_total_num_docs, get_total_num_tokens};
use crate::CXX_STRING_CONVERTER;
use cxx::CxxString;
use cxx::CxxVector;
use crate::utils::api_utils::ApiUtils;

pub fn ffi_bm25_search(
    index_path: &CxxString,
    sentence: &CxxString,
    column_names: &CxxVector<CxxString>,
    top_k: u32,
    u8_alive_bitmap: &CxxVector<u8>,
    query_with_filter: bool,
    enable_nlq: bool,
    operator_or: bool,
    statistics: &Statistics,
) -> FFIVecRowIdWithScoreResult {
    static FUNC_NAME: &str = "ffi_bm25_search";
    static FUNC_NAME_WITH_NLQ: &str = "ffi_bm25_natural_language_search";
    static FUNC_NAME_WITHOUT_NLQ: &str = "ffi_bm25_standard_search";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't convert 'index_path'", e.to_string());
        }
    };

    let sentence: String = match CXX_STRING_CONVERTER.convert(sentence) {
        Ok(q) => q,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't convert 'sentence'", e.to_string());

        }
    };

    let column_names: Vec<String> = match CXX_VECTOR_STRING_CONVERTER.convert(column_names) {
        Ok(ts) => ts,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't convert 'terms'", e.to_string());
        }
    };

    let u8_alive_bitmap: Vec<u8> = match cxx_vector_converter::<u8>().convert(u8_alive_bitmap) {
        Ok(bitmap) => bitmap,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't convert 'u8_alive_bitmap'", e.to_string());

        }
    };

    if enable_nlq {
        match bm25_natural_language_search(
            &index_path,
            &sentence,
            &column_names,
            top_k,
            &u8_alive_bitmap,
            query_with_filter,
            operator_or,
            statistics,
            false,
        ) {
            Ok(result) => FFIVecRowIdWithScoreResult {
                result,
                error: FFIError {
                    is_error: false,
                    message: String::new(),
                },
            },
            Err(e) => {
                ApiUtils::handle_error(FUNC_NAME_WITH_NLQ, "Error performing BM25 natural language search with statistics", e.to_string())
            }
        }
    } else {
        match bm25_standard_search(
            &index_path,
            &sentence,
            &column_names,
            top_k,
            &u8_alive_bitmap,
            query_with_filter,
            operator_or,
            statistics,
            false,
        ) {
            Ok(result) => FFIVecRowIdWithScoreResult {
                result,
                error: FFIError {
                    is_error: false,
                    message: String::new(),
                },
            },
            Err(e) => {
                ApiUtils::handle_error(FUNC_NAME_WITHOUT_NLQ, "Error performing BM25 standard search with statistics", e.to_string())
            }
        }
    }
}

pub fn ffi_get_doc_freq(index_path: &CxxString, sentence: &CxxString) -> FFIVecDocWithFreqResult {
    static FUNC_NAME: &str = "ffi_get_doc_freq";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't convert 'index_path'", e.to_string());
        }
    };

    let sentence: String = match CXX_STRING_CONVERTER.convert(sentence) {
        Ok(q) => q,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't convert 'sentence'", e.to_string());
        }
    };

    match get_doc_freq(&index_path, &sentence) {
        Ok(result) => FFIVecDocWithFreqResult {
            result,
            error: FFIError {
                is_error: false,
                message: String::new(),
            },
        },
        Err(e) => {
            ApiUtils::handle_error(FUNC_NAME, "Error happened when execute `get_doc_freq`", e.to_string())
        }
    }
}

pub fn ffi_get_total_num_docs(index_path: &CxxString) -> FFIU64Result {
    static FUNC_NAME: &str = "ffi_get_total_num_docs";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't convert 'index_path'", e.to_string());
        }
    };
    match get_total_num_docs(&index_path) {
        Ok(result) => FFIU64Result {
            result,
            error: FFIError {
                is_error: false,
                message: String::new(),
            },
        },
        Err(e) => {
            ApiUtils::handle_error(FUNC_NAME, "Error happened when execute `get_total_num_docs`", e.to_string())

        }
    }
}

pub fn ffi_get_total_num_tokens(index_path: &CxxString) -> FFIFieldTokenNumsResult {
    static FUNC_NAME: &str = "ffi_get_total_num_tokens";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't convert 'index_path'", e.to_string());
        }
    };

    match get_total_num_tokens(&index_path) {
        Ok(result) => FFIFieldTokenNumsResult {
            result,
            error: FFIError {
                is_error: false,
                message: String::new(),
            },
        },
        Err(e) => {
            ApiUtils::handle_error(FUNC_NAME, "Error happened when execute `get_total_num_docs`", e.to_string())

        }
    }
}
