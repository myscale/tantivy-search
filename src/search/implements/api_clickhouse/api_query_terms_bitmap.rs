use std::sync::Arc;
use roaring::RoaringBitmap;
use crate::common::errors::TantivySearchError;
use crate::search::implements::SearchUtils;
use crate::search::implements::strategy::QueryExecutor;
use crate::search::implements::strategy::TermSetQueryStrategy;
use crate::search::utils::ConvertUtils;

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

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use crate::common::{SinglePartTest, TEST_MUTEX};
    use crate::search::implements::api_common::load_index_reader;
    use crate::search::implements::query_terms_bitmap;

    #[test]
    fn normal_test() {
        let _guard = TEST_MUTEX.lock().unwrap();
        let tmp_dir = TempDir::new().unwrap();
        let tmp_dir = tmp_dir.path().to_str().unwrap();

        let _ = SinglePartTest::index_docs_and_get_reader_bridge(tmp_dir, true, true, true);
        assert!(load_index_reader(tmp_dir).unwrap());

        let res = query_terms_bitmap(
            tmp_dir, "col1", &vec!["ancient".to_string(), "reflect".to_string()]
        ).unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0], 19);

        let res = query_terms_bitmap(
            tmp_dir, "col2", &vec!["Nature".to_string(), "Moral".to_string()]
        ).unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0], 18);
    }
}