use crate::common::constants::FFI_INDEX_WRITER_CACHE;
use crate::common::errors::TantivySearchError;
use crate::{common::constants::LOG_CALLBACK, DEBUG, ERROR};
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::index::implements::index_manager::IndexManager;

pub fn free_index_writer(index_path: &str) -> Result<bool, TantivySearchError> {
    static FUNC_NAME: &str = "free_index_writer";

    // get index writer bridge from CACHE
    let bridge_res = IndexManager::get_index_writer_bridge(FUNC_NAME, index_path);
    if bridge_res.is_err() {
        return Ok(false);
    }
    let bridge = bridge_res.unwrap();

    bridge.wait_merging_threads().map_err(|e| {
        let error_info = format!("Can't wait merging threads, exception: {}", e);
        ERROR!(function: FUNC_NAME, "{}", error_info);
        TantivySearchError::InternalError(error_info)
    })?;

    // Remove index writer from CACHE
    FFI_INDEX_WRITER_CACHE
        .remove_index_writer_bridge(index_path.to_string())
        .map_err(|e| {
            ERROR!(function: FUNC_NAME, "{}", e);
            TantivySearchError::InternalError(e)
        })?;

    DEBUG!(function: FUNC_NAME, "Index writer has been freed:[{}]", index_path);
    Ok(true)
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use crate::common::{SinglePartTest, TEST_MUTEX};
    use crate::common::constants::FFI_INDEX_WRITER_CACHE;
    use crate::index::implements::{create_index_with_parameter, free_index_writer};

    #[test]
    pub fn test_free_index_writer() {
        let _guard = TEST_MUTEX.lock().unwrap();
        let tmp_dir = TempDir::new().unwrap();
        let tmp_dir = tmp_dir.path().to_str().unwrap();

        let res = create_index_with_parameter(
            tmp_dir, SinglePartTest::get_column_names(), "{}"
        );
        assert!(res.is_ok());

        // Get index writer from CACHE
        let bridge = FFI_INDEX_WRITER_CACHE
            .get_index_writer_bridge(tmp_dir.to_string())
            .unwrap();
        assert!(bridge.commit().is_ok());

        // Test whether index_writer is present after `tantivy_writer_free`.
        assert!(
            bridge.writer
                .try_lock()
                .unwrap()
                .as_mut()
                .is_some()
        );

        assert!(free_index_writer(tmp_dir).is_ok());

        assert!(
            bridge.writer
                .try_lock()
                .unwrap()
                .as_mut()
                .is_none()
        );

        assert!(FFI_INDEX_WRITER_CACHE.get_index_writer_bridge(tmp_dir.to_string()).is_err());
    }
}