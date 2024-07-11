use crate::common::errors::TantivySearchError;
use crate::{common::constants::LOG_CALLBACK, ERROR};
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::index::implements::index_manager::IndexManager;

pub fn commit_index(index_path: &str) -> Result<bool, TantivySearchError> {
    static FUNC_NAME: &str = "commit_index";

    let bridge = IndexManager::get_index_writer_bridge(FUNC_NAME, index_path)?;
    bridge.commit().map_err(|e| {
        let error_info = format!("Failed to commit index writer: {}", e.to_string());
        ERROR!(function: FUNC_NAME, "{}", error_info);
        TantivySearchError::InternalError(e)
    })?;
    // Reload, not need handle error.
    let _ = IndexManager::reload_index_reader(index_path);

    Ok(true)
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use crate::common::{SinglePartTest, TEST_MUTEX};
    use crate::index::implements::commit_index;

    #[test]
    pub fn boundary_test_with_empty_directory() {
        let _guard = TEST_MUTEX.lock().unwrap();
        let tmp_dir = TempDir::new().unwrap();
        let tmp_dir = tmp_dir.path().to_str().unwrap();

        assert!(commit_index(tmp_dir).is_err());
    }

    #[test]
    pub fn normal_test_commit_index() {
        let _guard = TEST_MUTEX.lock().unwrap();
        let tmp_dir = TempDir::new().unwrap();
        let tmp_dir = tmp_dir.path().to_str().unwrap();

        let index_writer_bridge =
            SinglePartTest::index_docs_and_get_writer_bridge(tmp_dir, true,false, false);

        assert!(commit_index(tmp_dir).is_ok());

        SinglePartTest::search_with_index_writer_bridge(index_writer_bridge)
    }
}