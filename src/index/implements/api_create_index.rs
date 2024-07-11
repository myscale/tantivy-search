use std::path::Path;
use std::sync::Arc;
use tantivy::Index;
use crate::common::constants::FFI_INDEX_WRITER_CACHE;
use crate::common::errors::TantivySearchError;
use crate::{common::constants::LOG_CALLBACK, ERROR};
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::index::implements::index_manager::IndexManager;
use crate::utils::index_utils::IndexUtils;

pub fn create_index_with_parameter(
    index_path: &str,
    column_names: &Vec<String>,
    index_json_parameter: &str,
) -> Result<bool, TantivySearchError>
{
    static FUNC_NAME: &str = "create_index_with_parameter";

    IndexManager::prepare_directory(FUNC_NAME, index_path)?;
    IndexManager::persist_index_params(index_path, index_json_parameter)?;

    let (schema, config_map) =
        IndexManager::build_schema(FUNC_NAME, column_names, index_json_parameter)?;

    // Create the index in the specified directory.
    let index = Index::create_in_dir(Path::new(index_path), schema).map_err(|e| {
        let error_info = format!(
            "Failed to create index in directory:{}, message:{}",
            index_path,
            e.to_string()
        );
        ERROR!(function: FUNC_NAME, "{}", error_info);
        TantivySearchError::TantivyError(e)
    })?;

    IndexUtils::register_tokenizers_from_config_map(&index, &config_map);

    let bridge = IndexManager::create_writer(FUNC_NAME, &index, index_path)?;

    FFI_INDEX_WRITER_CACHE
        .set_index_writer_bridge(index_path.to_string(), Arc::new(bridge))
        .map_err(|e| {
            ERROR!(function: FUNC_NAME, "{}", e);
            TantivySearchError::InternalError(e)
        })?;

    Ok(true)
}

pub fn create_index(
    index_path: &str,
    column_names: &Vec<String>,
) -> Result<bool, TantivySearchError>
{
    create_index_with_parameter(index_path, column_names, "{}")
}


#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use crate::common::{SinglePartTest, TEST_MUTEX};
    use crate::index::implements::{create_index, create_index_with_parameter};


    // TODO 在创建完索引之后可以检查索引目录中是否存在自定义的配置文件

    #[test]
    pub fn normal_test_create_index_with_parameter() {
        let _guard = TEST_MUTEX.lock().unwrap();
        let tmp_dir = TempDir::new().unwrap();
        let tmp_dir = tmp_dir.path().to_str().unwrap();

        let result = create_index_with_parameter(
            tmp_dir, SinglePartTest::get_column_names(), "{}"
        );
        assert!(result.is_ok());
        let index_writer_bridge =
            SinglePartTest::index_docs_and_get_writer_bridge(tmp_dir, false, true, true);
        SinglePartTest::search_with_index_writer_bridge(index_writer_bridge)
    }

    #[test]
    pub fn normal_test_create_index() {
        let _guard = TEST_MUTEX.lock().unwrap();
        let tmp_dir = TempDir::new().unwrap();
        let tmp_dir = tmp_dir.path().to_str().unwrap();

        assert!(create_index(tmp_dir, SinglePartTest::get_column_names()).is_ok());
        let index_writer_bridge =
            SinglePartTest::index_docs_and_get_writer_bridge(tmp_dir, false, true, true);
        SinglePartTest::search_with_index_writer_bridge(index_writer_bridge)
    }

    #[test]
    pub fn boundary_test_with_invalid_parameter() {
        let _guard = TEST_MUTEX.lock().unwrap();
        let tmp_dir = TempDir::new().unwrap();
        let tmp_dir = tmp_dir.path().to_str().unwrap();

        let result = create_index_with_parameter(
            tmp_dir, SinglePartTest::get_column_names(), "{abc}"
        );
        assert!(result.is_err());
    }

    #[test]
    pub fn boundary_test_with_not_empty_directory() {
        let _guard = TEST_MUTEX.lock().unwrap();
        let tmp_dir = TempDir::new().unwrap();
        let tmp_dir = tmp_dir.path().to_str().unwrap();

        assert!(create_index_with_parameter(
            tmp_dir, SinglePartTest::get_column_names(), "{}"
        ).is_ok());
        assert!(create_index_with_parameter(
            tmp_dir, SinglePartTest::get_column_names(), "{}"
        ).is_ok());

    }
}