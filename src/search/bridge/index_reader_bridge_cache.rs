use std::sync::Arc;
use flurry::HashMap;
use once_cell::sync::OnceCell;
use tantivy::Executor;
use crate::{common::constants::LOG_CALLBACK, DEBUG, WARNING};
use crate::logger::logger_bridge::TantivySearchLogger;
use super::index_reader_bridge::IndexReaderBridge;

pub struct IndexReaderBridgeCache {
    // pub path_locks: HashMap<String, Mutex<()>>,
    cache: HashMap<String, Arc<IndexReaderBridge>>,
    shared_thread_pool: OnceCell<Arc<Executor>>,
}

impl IndexReaderBridgeCache {
    pub fn new() -> Self {
        Self {
            // path_locks: HashMap::new(),
            cache: HashMap::new(),
            shared_thread_pool: OnceCell::new(),
        }
    }

    pub fn set_index_reader_bridge(
        &self,
        key: String,
        value: Arc<IndexReaderBridge>,
    ) -> Result<(), String> {
        let trimmed_key: String = key.trim_end_matches('/').to_string();
        let pinned = self.cache.pin();
        if pinned.contains_key(&trimmed_key) {
            pinned.insert(trimmed_key.clone(), value.clone());
            let message = format!("IndexReaderBridge already exists with given key: [{}], it has been overwritten.", trimmed_key);
            WARNING!("{}", message)
        } else {
            pinned.insert(trimmed_key, value.clone());
        }
        Ok(())
    }

    pub fn get_index_reader_bridge(&self, key: String) -> Result<Arc<IndexReaderBridge>, String> {
        let pinned = self.cache.pin();
        let trimmed_key: String = key.trim_end_matches('/').to_string();
        match pinned.get(&trimmed_key) {
            Some(result) => Ok(result.clone()),
            None => Err(format!("IndexReaderBridge doesn't exist with given key: [{}]", trimmed_key)),
        }
    }

    pub fn remove_index_reader_bridge(&self, key: String) -> Result<(), String> {
        let pinned = self.cache.pin();
        let trimmed_key: String = key.trim_end_matches('/').to_string();
        if pinned.contains_key(&trimmed_key) {
            pinned.remove(&trimmed_key);
        } else {
            let message: String = format!("IndexReaderBridge doesn't exist, can't remove it with given key [{}]", trimmed_key);
            DEBUG!("{}", message);
            return Err(message);
        }
        Ok(())
    }

    // shared thread pool for index searcher.
    pub fn get_shared_multi_thread_executor(
        &self,
        num_threads: usize,
    ) -> Result<Arc<Executor>, String> {
        if num_threads <= 0 {
            return Err("threads number minimum is 1".to_string());
        }
        let res: Result<&Arc<Executor>, String> = self.shared_thread_pool.get_or_try_init(|| {
            Executor::multi_thread(num_threads, "tantivy-search-")
                .map(Arc::new)
                .map_err(|e| e.to_string())
        });

        res.map(|executor| executor.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use tempfile::TempDir;
    use crate::common::SinglePartTest;
    use crate::search::bridge::index_reader_bridge_cache::IndexReaderBridgeCache;

    #[test]
    fn normal_test_insert() {
        let tmp_dir = TempDir::new().expect("");
        let tmp_dir = tmp_dir.path().to_str().unwrap();

        let cache = IndexReaderBridgeCache::new();
        let value = SinglePartTest::create_index_and_get_reader_bridge(tmp_dir, true);
        let res = cache.set_index_reader_bridge(tmp_dir.to_string(), value);
        assert!(res.is_ok());
    }

    #[test]
    fn normal_test_get() {
        let tmp_dir = TempDir::new().expect("");
        let tmp_dir = tmp_dir.path().to_str().unwrap();

        let cache = IndexReaderBridgeCache::new();
        let bridge = SinglePartTest::create_index_and_get_reader_bridge(tmp_dir, true);
        let set_res = cache.set_index_reader_bridge(tmp_dir.to_string(), bridge);
        assert!(set_res.is_ok());

        let get_res = cache.get_index_reader_bridge(tmp_dir.to_string());
        assert!(get_res.is_ok());
        assert_eq!(get_res.unwrap().path, tmp_dir.to_string());

    }

    #[test]
    fn boundary_test_get() {
        let cache = IndexReaderBridgeCache::new();
        let err = cache.get_index_reader_bridge("not_exists".to_string());
        assert!(err.is_err());
    }

    #[test]
    fn normal_test_update() {
        let tmp_dir_1 = TempDir::new().expect("");
        let tmp_dir_1 = tmp_dir_1.path().to_str().unwrap();
        let tmp_dir_2 = TempDir::new().expect("");
        let tmp_dir_2 = tmp_dir_2.path().to_str().unwrap();

        let cache = IndexReaderBridgeCache::new();

        // First insert.
        let value1 = SinglePartTest::create_index_and_get_reader_bridge(tmp_dir_1, true);
        let res1 = cache.set_index_reader_bridge(tmp_dir_1.to_string(), value1);
        assert!(res1.is_ok());

        // Second insert.
        let value2 = SinglePartTest::create_index_and_get_reader_bridge(tmp_dir_2, true);
        let res2 = cache.set_index_reader_bridge(tmp_dir_1.to_string(), value2);
        assert!(res2.is_ok());

        // Get after update.
        let res_get = cache.get_index_reader_bridge(tmp_dir_1.to_string());
        assert!(res_get.is_ok());
        assert_eq!(res_get.unwrap().path, tmp_dir_2.to_string());
    }

    #[test]
    fn test_remove_index_reader_bridge() {
        let tmp_dir = TempDir::new().expect("");
        let tmp_dir = tmp_dir.path().to_str().unwrap();

        let cache = IndexReaderBridgeCache::new();

        // insert
        let value = SinglePartTest::create_index_and_get_reader_bridge(tmp_dir, true);
        let res = cache.set_index_reader_bridge(tmp_dir.to_string(), value);
        assert!(res.is_ok());

        // get
        let res_get = cache.get_index_reader_bridge(tmp_dir.to_string());
        assert!(res_get.is_ok());
        assert_eq!(res_get.unwrap().path, tmp_dir.to_string());

        // remove
        let first_removed = cache.remove_index_reader_bridge(tmp_dir.to_string());
        assert!(first_removed.is_ok());

        // get
        let get_after_remove = cache.get_index_reader_bridge(tmp_dir.to_string());
        assert!(get_after_remove.is_err());

        // Remove a not exist `IndexReaderBridge` will trigger an error.
        let second_removed = cache.remove_index_reader_bridge(tmp_dir.to_string());
        assert!(second_removed.is_err());
    }

    #[test]
    fn test_shared_multi_thread_executor() {
        let cache = IndexReaderBridgeCache::new();

        assert!(cache.get_shared_multi_thread_executor(0).is_err());
        let executor1 = cache
            .get_shared_multi_thread_executor(2)
            .expect("Failed to get executor for the first time");
        let executor2 = cache
            .get_shared_multi_thread_executor(4)
            .expect("Failed to get executor for the second time");

        assert!(
            Arc::ptr_eq(&executor1, &executor2),
            "Executors should be the same instance"
        );
    }
}
