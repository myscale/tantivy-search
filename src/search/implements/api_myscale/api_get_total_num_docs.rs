use tantivy::query::Bm25StatisticsProvider;
use crate::common::errors::TantivySearchError;
use crate::{common::constants::LOG_CALLBACK, DEBUG};
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::search::implements::search_utils::SearchUtils;

pub fn get_total_num_docs(index_path: &str) -> Result<u64, TantivySearchError> {
    static FUNC_NAME: &str = "get_total_num_docs";

    let bridge = SearchUtils::get_index_reader_bridge(FUNC_NAME, index_path)?;
    let searcher = bridge.reader.searcher();
    let total_num_docs = searcher.total_num_docs()?;

    DEBUG!(function: FUNC_NAME, "index_path:[{:?}], total_num_docs is:[{:?}]", index_path, total_num_docs);

    Ok(total_num_docs)
}
