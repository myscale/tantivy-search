use crate::common::errors::TantivySearchError;
use crate::search::implements::SearchUtils;

pub fn get_indexed_doc_counts(index_path: &str) -> Result<u64, TantivySearchError> {
    static FUNC_NAME: &str = "get_indexed_doc_counts";
    let bridge = SearchUtils::get_index_reader_bridge(FUNC_NAME, index_path)?;

    let num_docs: u64 = bridge.reader.searcher().num_docs();
    Ok(num_docs)
}
