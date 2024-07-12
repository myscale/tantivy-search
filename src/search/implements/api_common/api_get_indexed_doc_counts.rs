use crate::common::errors::TantivySearchError;
use crate::search::implements::SearchUtils;

pub fn get_indexed_doc_counts(index_path: &str) -> Result<u64, TantivySearchError> {
    static FUNC_NAME: &str = "get_indexed_doc_counts";
    let bridge = SearchUtils::get_index_reader_bridge(FUNC_NAME, index_path)?;

    let num_docs: u64 = bridge.reader.searcher().num_docs();
    Ok(num_docs)
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use crate::common::{SinglePartTest, TEST_MUTEX};
    use crate::search::implements::api_common::load_index_reader;
    use crate::search::implements::get_indexed_doc_counts;

    #[test]
    fn normal_test() {
        let _guard = TEST_MUTEX.lock().unwrap();
        let tmp_dir = TempDir::new().unwrap();
        let tmp_dir = tmp_dir.path().to_str().unwrap();

        let _ = SinglePartTest::index_docs_and_get_reader_bridge(tmp_dir, true, true, true);

        assert!(get_indexed_doc_counts(tmp_dir).is_err());
        assert!(load_index_reader(tmp_dir).unwrap());
        assert_eq!(
            get_indexed_doc_counts(tmp_dir).unwrap(),
            SinglePartTest::get_mocked_docs().iter().map(|col|col.len()).min().unwrap() as u64
        );
    }
}