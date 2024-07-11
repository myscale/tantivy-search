use std::path::Path;
use std::sync::Arc;
use roaring::RoaringBitmap;
use crate::common::errors::TantivySearchError;
use crate::{common::constants::LOG_CALLBACK, ERROR};
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::search::implements::SearchUtils;
use crate::search::implements::strategy::QueryExecutor;
use crate::search::implements::strategy::RegexQueryStrategy;
use crate::search::utils::convert_utils::ConvertUtils;
use crate::tokenizer::ingredient::Config;
use crate::tokenizer::parser::TokenizerWrapper;
use crate::utils::index_utils::IndexUtils;

pub fn regex_term_bitmap(
    index_path: &str,
    column_name: &str,
    pattern: &str,
) -> Result<Vec<u8>, TantivySearchError> {
    static FUNC_NAME: &str = "query_terms_bitmap";
    let bridge = SearchUtils::get_index_reader_bridge(FUNC_NAME, index_path)?;

    let config: Config =
        IndexUtils::load_tokenizer_config(Path::new(index_path)).map_err(|e| {
            ERROR!(function: FUNC_NAME, "{}", e);
            TantivySearchError::IndexUtilsError(e)
        })?;

    let lower_case = config
        .get_columns()
        .get(column_name)
        .map(|column|{
            let tokenizer = column.get_tokenizer();
            TokenizerWrapper::new(tokenizer).is_lower_case()
        }).unwrap_or(true);

    let mut pattern_handled = pattern.to_string();
    if lower_case {
        pattern_handled.make_ascii_lowercase(); // Keep same with Lower Case Tokenizer.
    }

    let strategy: RegexQueryStrategy<'_> = RegexQueryStrategy {column_name, pattern: &pattern_handled};
    let executor: QueryExecutor<'_, Arc<RoaringBitmap>> = QueryExecutor::new(&strategy);

    let bitmap = SearchUtils::execute_query(FUNC_NAME, executor, &bridge.reader.searcher())?;
    let row_ids: Vec<u32> = bitmap.iter().collect();
    Ok(ConvertUtils::row_ids_to_u8_bitmap(&row_ids))

}
