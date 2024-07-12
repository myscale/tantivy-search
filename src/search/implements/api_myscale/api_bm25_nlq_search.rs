use crate::common::errors::TantivySearchError;
use crate::ffi::{RowIdWithScore, Statistics};
use crate::search::implements::api_myscale::bm25_inner_search;
use crate::search::implements::strategy::BM25NaturalLanguageStrategy;

pub fn bm25_natural_language_search(
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
    let bm25_natural_language_query: BM25NaturalLanguageStrategy<'_> =
        BM25NaturalLanguageStrategy {
            sentence,
            top_k: &top_k,
            u8_alive_bitmap,
            query_with_filter: &query_with_filter,
            need_doc: &need_doc,
            operation_or: &operation_or,
        };

    bm25_inner_search(index_path, statistics, &bm25_natural_language_query)
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use crate::common::{SinglePartTest, TEST_MUTEX};
    use crate::ffi::Statistics;
    use crate::search::implements::api_common::load_index_reader;
    use crate::search::implements::{bm25_natural_language_search};

    #[test]
    fn normal_test_single_part() {
        let _guard = TEST_MUTEX.lock().unwrap();
        let tmp_dir = TempDir::new().unwrap();
        let tmp_dir = tmp_dir.path().to_str().unwrap();

        let _ = SinglePartTest::index_docs_and_get_reader_bridge(tmp_dir, true, true, true);
        assert!(load_index_reader(tmp_dir).unwrap());

        let res = bm25_natural_language_search(
            tmp_dir,
            "ancient OR (social military)",
            10,
            &vec![],
            false,
            true,
            &Statistics::default(),
            true
        );
        assert!(res.is_ok());
        for row in res.unwrap() {
            println!("{:?}", row);
        }
    }
}