use std::path::Path;
use std::sync::Arc;
use tantivy::{Index, IndexReader, ReloadPolicy};
use crate::common::constants::FFI_INDEX_SEARCHER_CACHE;
use crate::common::errors::TantivySearchError;
use crate::{common::constants::LOG_CALLBACK, DEBUG, ERROR};
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::search::bridge::index_reader_bridge::IndexReaderBridge;
use crate::search::implements::api_common::free_index_reader;
use crate::utils::index_utils::IndexUtils;

pub fn load_index_reader(index_path: &str) -> Result<bool, TantivySearchError> {
    static FUNC_NAME: &str = "load_index_reader";

    // Boundary.
    let index_files_directory = Path::new(index_path);
    if !index_files_directory.exists() || !index_files_directory.is_dir() {
        let error_info: String = format!("index_path not exists: {:?}", index_path);
        let error: TantivySearchError = TantivySearchError::IndexNotExists(error_info);
        ERROR!(function: FUNC_NAME, "{}", error.to_string());
        return Err(error);
    }
    let index_path = index_path.trim_end_matches('/');

    // Free old reader bridge.
    let bridge = FFI_INDEX_SEARCHER_CACHE.get_index_reader_bridge(index_path.to_string());
    if bridge.is_ok() {
        let _ = free_index_reader(index_path);
    }

    // Load tantivy index and register tokenizer with given directory.
    let mut index: Index = Index::open_in_dir(index_files_directory).map_err(|e| {
        ERROR!(function: FUNC_NAME, "{}", e.to_string());
        e
    })?;
    IndexUtils::register_tokenizers_from_disk(index_files_directory, &index)?;

    #[cfg(feature = "use-shared-search-pool")]
    {
        // Set the multithreaded executor for search.
        match FFI_INDEX_SEARCHER_CACHE.get_shared_multi_thread_executor(num_cpus::get()) {
            Ok(shared_thread_pool) => {
                index.set_shared_multithread_executor(shared_thread_pool).map_err(|e| TantivySearchError::TantivyError(e))?;
                DEBUG!(function: FUNC_NAME, "Using shared multi thread with index_path: [{}]", index_path);
            }
            Err(e) => {
                ERROR!(function: FUNC_NAME, "Failed to use shared multi thread executor, due to: {}", e);
                index.set_default_multithread_executor().map_err(|e| {
                    ERROR!(function: FUNC_NAME, "Failed fall back to default multi thread executor, due to: {}", e);
                    TantivySearchError::TantivyError(e)
                })?;
            }
        }
    }
    #[cfg(not(feature = "use-shared-search-pool"))]
    {
        index.set_default_multithread_executor().map_err(|e| {
            ERROR!(function:"load_index_reader", "Failed to set default multi thread executor, due to: {}", e);
            TantivySearchError::TantivyError(e)
        })?;
    }

    // Create a reader for the index with an appropriate reload policy.
    // OnCommit: reload when commiting; Manual: developer need call IndexReader::reload() to reload.
    let reader: IndexReader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommit)
        .try_into()
        .map_err(|e| {
            ERROR!(function: FUNC_NAME, "Failed to create tantivy index reader: {}", e);
            TantivySearchError::TantivyError(e)
        })?;

    // Save IndexReaderBridge to cache.
    let index_reader_bridge: IndexReaderBridge = IndexReaderBridge {
        index,
        reader,
        path: index_path.trim_end_matches('/').to_string(),
    };

    FFI_INDEX_SEARCHER_CACHE
        .set_index_reader_bridge(index_path.to_string(), Arc::new(index_reader_bridge))
        .map_err(|e| {
            ERROR!(function: FUNC_NAME, "{}", e);
            TantivySearchError::InternalError(e)
        })?;

    Ok(true)
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use crate::common::{SinglePartTest, TEST_MUTEX};
    use crate::common::constants::FFI_INDEX_SEARCHER_CACHE;
    use crate::search::implements::api_common::load_index_reader;

    #[test]
    fn normal_test() {
        let _guard = TEST_MUTEX.lock().unwrap();
        let tmp_dir = TempDir::new().unwrap();
        let tmp_dir = tmp_dir.path().to_str().unwrap();

        let _ = SinglePartTest::index_docs_and_get_reader_bridge(tmp_dir, true, true, true);

        let res = FFI_INDEX_SEARCHER_CACHE.get_index_reader_bridge(tmp_dir.to_string());
        assert!(res.is_err());

        assert!(load_index_reader(tmp_dir).unwrap());

        let res = FFI_INDEX_SEARCHER_CACHE.get_index_reader_bridge(tmp_dir.to_string());
        assert!(res.is_ok());

        assert!(load_index_reader(tmp_dir).unwrap());
    }
}