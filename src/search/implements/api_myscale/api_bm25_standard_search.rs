use crate::common::errors::TantivySearchError;
use crate::ffi::{RowIdWithScore, Statistics};
use crate::search::implements::api_myscale::bm25_inner_search::bm25_inner_search;
use crate::search::implements::strategy::BM25StandardQueryStrategy;

pub fn bm25_standard_search(
    index_path: &str,
    sentence: &str,
    top_k: u32,
    u8_alive_bitmap: &Vec<u8>,
    query_with_filter: bool,
    operation_or: bool,
    statistics: &Statistics,
    need_doc: bool,
) -> Result<Vec<RowIdWithScore>, TantivySearchError> {
    // Choose query strategy to construct query executor.
    let bm25_standard_query: BM25StandardQueryStrategy<'_> = BM25StandardQueryStrategy {
        sentence,
        top_k: &top_k,
        query_with_filter: &query_with_filter,
        u8_alive_bitmap,
        need_doc: &need_doc,
        operation_or: &operation_or,
    };

    bm25_inner_search(index_path, statistics, &bm25_standard_query)
}