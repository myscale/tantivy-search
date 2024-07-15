use crate::cxx_vector_converter;
use crate::ffi::{FieldTokenNums, Statistics};
use crate::search::implements::{bm25_natural_language_search, bm25_standard_search, get_doc_freq,
                                get_total_num_docs, get_total_num_tokens};
use crate::{DocWithFreq, RowIdWithScore, CXX_STRING_CONVERTER};
use crate::{common::constants::LOG_CALLBACK, ERROR};
use crate::logger::logger_bridge::TantivySearchLogger;

use cxx::CxxString;
use cxx::CxxVector;

pub fn ffi_bm25_search(
    index_path: &CxxString,
    sentence: &CxxString,
    top_k: u32,
    u8_alive_bitmap: &CxxVector<u8>,
    query_with_filter: bool,
    enable_nlq: bool,
    operator_or: bool,
    statistics: &Statistics,
) -> Vec<RowIdWithScore> {
    static FUNC_NAME: &str = "ffi_bm25_search";
    static FUNC_NAME_WITH_NLQ: &str = "ffi_bm25_natural_language_search";
    static FUNC_NAME_WITHOUT_NLQ: &str = "ffi_bm25_standard_search";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: FUNC_NAME, "Can't convert 'index_path', message: {}", e);
            return Vec::new();
        }
    };

    let sentence: String = match CXX_STRING_CONVERTER.convert(sentence) {
        Ok(q) => q,
        Err(e) => {
            ERROR!(function: FUNC_NAME, "Can't convert 'sentence', message: {}", e);
            return Vec::new();
        }
    };

    let u8_alive_bitmap: Vec<u8> = match cxx_vector_converter::<u8>().convert(u8_alive_bitmap) {
        Ok(bitmap) => bitmap,
        Err(e) => {
            ERROR!(function: FUNC_NAME, "Can't convert vector 'u8_alive_bitmap', message: {}", e);
            return Vec::new();
        }
    };

    if enable_nlq {
        bm25_natural_language_search(
            &index_path,
            &sentence,
            top_k,
            &u8_alive_bitmap,
            query_with_filter,
            operator_or,
            statistics,
            false,
        ).unwrap_or_else(|e| {
            ERROR!(function: FUNC_NAME_WITH_NLQ, "Error performing BM25 natural language search with statistics: {}", e);
            Vec::new()
        })
    } else {
        bm25_standard_search(
            &index_path,
            &sentence,
            top_k,
            &u8_alive_bitmap,
            query_with_filter,
            operator_or,
            statistics,
            false,
        ).unwrap_or_else(|e| {
            ERROR!(function: FUNC_NAME_WITHOUT_NLQ, "Error performing BM25 standard search with statistics: {}", e);
            Vec::new()
        })
    }
}

pub fn ffi_get_doc_freq(index_path: &CxxString, sentence: &CxxString) -> Vec<DocWithFreq> {
    static FUNC_NAME: &str = "ffi_get_doc_freq";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: FUNC_NAME, "Can't convert 'index_path', message: {}", e);
            return Vec::new();
        }
    };

    let sentence: String = match CXX_STRING_CONVERTER.convert(sentence) {
        Ok(q) => q,
        Err(e) => {
            ERROR!(function: FUNC_NAME, "Can't convert 'sentence', message: {}", e);
            return Vec::new();
        }
    };

    get_doc_freq(&index_path, &sentence).unwrap_or_else(|e| {
        ERROR!(function: FUNC_NAME, "Error performing get_doc_freq: {}", e);
        Vec::new()
    })
}

pub fn ffi_get_total_num_docs(index_path: &CxxString) -> u64 {
    static FUNC_NAME: &str = "ffi_get_total_num_docs";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: FUNC_NAME, "Can't convert 'index_path', message: {}", e);
            return 0u64;
        }
    };
    get_total_num_docs(&index_path).unwrap_or_else(|e| {
        ERROR!(function: FUNC_NAME, "Error performing get_total_num_docs: {}", e);
        0u64
    })
}

pub fn ffi_get_total_num_tokens(index_path: &CxxString) -> Vec<FieldTokenNums> {
    static FUNC_NAME: &str = "ffi_get_total_num_tokens";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: FUNC_NAME, "Can't convert 'index_path', message: {}", e);
            return vec![];
        }
    };

    get_total_num_tokens(&index_path).unwrap_or_else(|e| {
        ERROR!(function: FUNC_NAME, "Error performing get_total_num_tokens: {}", e);
        vec![]
    })
}
