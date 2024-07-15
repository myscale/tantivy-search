use std::sync::Arc;

use flurry::HashMap;

use crate::{common::constants::LOG_CALLBACK, DEBUG, WARNING};
use crate::logger::logger_bridge::TantivySearchLogger;

use super::index_writer_bridge::IndexWriterBridge;

pub struct IndexWriterBridgeCache {
    cache: HashMap<String, Arc<IndexWriterBridge>>,
}

impl IndexWriterBridgeCache {
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }

    pub fn get_index_writer_bridge(&self, key: String) -> Result<Arc<IndexWriterBridge>, String> {
        let pinned = self.cache.pin();
        let trimmed_key: String = key.trim_end_matches('/').to_string();
        match pinned.get(&trimmed_key) {
            Some(result) => Ok(result.clone()),
            None => Err(format!(
                "Index Writer doesn't exist with given key: [{}]",
                trimmed_key
            )),
        }
    }

    pub fn set_index_writer_bridge(
        &self,
        key: String,
        value: Arc<IndexWriterBridge>,
    ) -> Result<(), String> {
        let pinned = self.cache.pin();
        let trimmed_key: String = key.trim_end_matches('/').to_string();
        if pinned.contains_key(&trimmed_key) {
            pinned.insert(trimmed_key.clone(), value.clone());
            WARNING!(
                "{}",
                format!(
                    "Index writer already exists with given key: [{}], it has been overwritten.",
                    trimmed_key
                )
            )
        } else {
            pinned.insert(trimmed_key, value.clone());
        }
        Ok(())
    }
    pub fn remove_index_writer_bridge(&self, key: String) -> Result<(), String> {
        let pinned = self.cache.pin();
        let trimmed_key: String = key.trim_end_matches('/').to_string();
        if pinned.contains_key(&trimmed_key) {
            pinned.remove(&trimmed_key);
        } else {
            let message = format!(
                "IndexWriterBridge doesn't exist, can't remove it with given key: [{}]",
                trimmed_key
            );
            DEBUG!("{}", message)
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use crate::common::SinglePartTest;
    use crate::index::bridge::IndexWriterBridgeCache;

    #[test]
    fn normal_test_insert() {
        let tmp_dir = TempDir::new().expect("");
        let tmp_dir = tmp_dir.path().to_str().unwrap();

        let cache = IndexWriterBridgeCache::new();
        let value = SinglePartTest::create_index_and_get_writer_bridge(tmp_dir, true);
        let res = cache.set_index_writer_bridge(tmp_dir.to_string(), value);
        assert!(res.is_ok());
    }

    #[test]
    fn normal_test_get() {
        let tmp_dir = TempDir::new().expect("");
        let tmp_dir = tmp_dir.path().to_str().unwrap();

        let cache = IndexWriterBridgeCache::new();
        let value = SinglePartTest::create_index_and_get_writer_bridge(tmp_dir, true);
        let res = cache.set_index_writer_bridge(tmp_dir.to_string(), value);
        assert!(res.is_ok());

        let res_get = cache.get_index_writer_bridge(tmp_dir.to_string());
        assert!(res_get.is_ok());
        assert_eq!(res_get.unwrap().path, tmp_dir.to_string());
    }

    #[test]
    fn boundary_test_get() {
        let cache = IndexWriterBridgeCache::new();
        let expect_error_result = cache.get_index_writer_bridge("not_exists".to_string());
        assert!(expect_error_result.is_err());
    }

    #[test]
    fn normal_test_update() {
        let tmp_dir = TempDir::new().expect("");
        let tmp_dir = tmp_dir.path().to_str().unwrap();
        let tmp_dir2 = TempDir::new().expect("");
        let tmp_dir2 = tmp_dir2.path().to_str().unwrap();

        let cache = IndexWriterBridgeCache::new();

        // First insert.
        let value1 = SinglePartTest::create_index_and_get_writer_bridge(tmp_dir, true);
        let res1 = cache.set_index_writer_bridge(tmp_dir.to_string(), value1);
        assert!(res1.is_ok());

        // Second insert.
        let value2 = SinglePartTest::create_index_and_get_writer_bridge(tmp_dir2, true);
        let res2 = cache.set_index_writer_bridge(tmp_dir.to_string(), value2);
        assert!(res2.is_ok());

        // Get after update.
        let res_get = cache.get_index_writer_bridge(tmp_dir.to_string());
        assert!(res_get.is_ok());
        assert_eq!(res_get.unwrap().path, tmp_dir2.to_string());
    }

    #[test]
    fn test_remove_index_writer_bridge() {
        let tmp_dir = TempDir::new().expect("");
        let tmp_dir = tmp_dir.path().to_str().unwrap();

        let cache = IndexWriterBridgeCache::new();

        // insert
        let value = SinglePartTest::create_index_and_get_writer_bridge(tmp_dir, true);
        let res = cache.set_index_writer_bridge(tmp_dir.to_string(), value);
        assert!(res.is_ok());

        // get
        let res_get = cache.get_index_writer_bridge(tmp_dir.to_string());
        assert!(res_get.is_ok());
        assert_eq!(res_get.unwrap().path, tmp_dir.to_string());

        // remove
        let first_removed = cache.remove_index_writer_bridge(tmp_dir.to_string());
        assert!(first_removed.is_ok());

        // get
        let get_after_remove = cache.get_index_writer_bridge(tmp_dir.to_string());
        assert!(get_after_remove.is_err());

        // Remove a not exist `IndexWriterBridge` will not trigger error.
        let second_removed = cache.remove_index_writer_bridge(tmp_dir.to_string());
        assert!(second_removed.is_ok());
    }
}
