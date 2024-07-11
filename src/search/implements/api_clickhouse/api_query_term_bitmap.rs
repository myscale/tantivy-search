use std::sync::Arc;
use roaring::RoaringBitmap;
use crate::common::errors::TantivySearchError;
use crate::search::implements::SearchUtils;
use crate::search::implements::strategy::QueryExecutor;
use crate::search::implements::strategy::SingleTermQueryStrategy;
use crate::search::utils::convert_utils::ConvertUtils;

pub fn query_term_bitmap(
    index_path: &str,
    column_name: &str,
    term: &str,
) -> Result<Vec<u8>, TantivySearchError> {
    static FUNC_NAME: &str = "query_term_bitmap";
    let bridge = SearchUtils::get_index_reader_bridge(FUNC_NAME, index_path)?;

    let strategy: SingleTermQueryStrategy<'_> = SingleTermQueryStrategy { column_name, term };
    let executor: QueryExecutor<'_, Arc<RoaringBitmap>> = QueryExecutor::new(&strategy);

    let bitmap = SearchUtils::execute_query(FUNC_NAME, executor, &bridge.reader.searcher())?;
    let row_ids: Vec<u32> = bitmap.iter().collect();
    Ok(ConvertUtils::row_ids_to_u8_bitmap(&row_ids))
}