use crate::common::constants::FFI_INDEX_SEARCHER_CACHE;

pub fn free_index_reader(index_path: &str) -> bool {
    if let Err(_) = FFI_INDEX_SEARCHER_CACHE.remove_index_reader_bridge(index_path.to_string()) {
        return false;
    }
    // key exists and success removed.
    true
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use crate::common::{SinglePartTest, TEST_MUTEX};
    use crate::search::implements::api_common::load_index_reader;
    use crate::search::implements::{free_index_reader, query_term_bitmap};

    #[test]
    fn normal_test() {
        let _guard = TEST_MUTEX.lock().unwrap();
        let tmp_dir = TempDir::new().unwrap();
        let tmp_dir = tmp_dir.path().to_str().unwrap();

        let _ = SinglePartTest::index_docs_and_get_reader_bridge(tmp_dir, true, true, true);

        assert_eq!(free_index_reader(tmp_dir), false);
        assert!(load_index_reader(tmp_dir).unwrap());
        assert_eq!(free_index_reader(tmp_dir), true);
        assert_eq!(free_index_reader(tmp_dir), false);

        let res = query_term_bitmap(tmp_dir, "col1", "Ancient");
        assert!(res.is_err());
    }
}