use std::sync::Arc;
use roaring::RoaringBitmap;
use crate::common::errors::TantivySearchError;
use crate::search::implements::SearchUtils;
use crate::search::implements::strategy::QueryExecutor;
use crate::search::implements::strategy::TermSetQueryStrategy;
use crate::search::utils::convert_utils::ConvertUtils;

pub fn query_terms_bitmap(
    index_path: &str,
    column_name: &str,
    terms: &Vec<String>,
) -> Result<Vec<u8>, TantivySearchError> {
    static FUNC_NAME: &str = "query_terms_bitmap";
    let bridge = SearchUtils::get_index_reader_bridge(FUNC_NAME, index_path)?;

    let strategy: TermSetQueryStrategy<'_> = TermSetQueryStrategy { column_name, terms };
    let executor: QueryExecutor<'_, Arc<RoaringBitmap>> = QueryExecutor::new(&strategy);

    let bitmap = SearchUtils::execute_query(FUNC_NAME, executor, &bridge.reader.searcher())?;
    let row_ids: Vec<u32> = bitmap.iter().collect();
    Ok(ConvertUtils::row_ids_to_u8_bitmap(&row_ids))
}