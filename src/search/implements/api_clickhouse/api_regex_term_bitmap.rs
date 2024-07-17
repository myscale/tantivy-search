use std::path::Path;
use std::sync::Arc;
use roaring::RoaringBitmap;
use crate::common::errors::TantivySearchError;
use crate::{common::constants::LOG_CALLBACK, ERROR};
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::search::implements::SearchUtils;
use crate::search::implements::strategy::QueryExecutor;
use crate::search::implements::strategy::RegexQueryStrategy;
use crate::search::utils::ConvertUtils;
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


#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use crate::common::{SinglePartTest, TEST_MUTEX};
    use crate::search::implements::api_common::load_index_reader;
    use crate::search::implements::regex_term_bitmap;

    #[test]
    fn normal_test() {
        let _guard = TEST_MUTEX.lock().unwrap();
        let tmp_dir = TempDir::new().unwrap();
        let tmp_dir = tmp_dir.path().to_str().unwrap();

        let _ = SinglePartTest::index_docs_and_get_reader_bridge(tmp_dir, true, true, true);
        assert!(load_index_reader(tmp_dir).unwrap());

        let res = regex_term_bitmap(tmp_dir, "col1", "An%ent").unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0], 17);

        let res = regex_term_bitmap(tmp_dir, "col1", "an%ent").unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0], 17);

        let res = regex_term_bitmap(tmp_dir, "col1", "%ncient%empir%").unwrap();
        assert_eq!(res.len(), 0);

        // Economic
        let res = regex_term_bitmap(tmp_dir, "col2", "E%no__c").unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0], 4);
    }
}
