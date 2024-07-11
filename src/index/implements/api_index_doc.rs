use tantivy::Document;
use crate::common::errors::TantivySearchError;
use crate::{common::constants::LOG_CALLBACK, ERROR};
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::index::implements::index_manager::IndexManager;

pub fn index_multi_column_docs(
    index_path: &str,
    row_id: u64,
    column_names: &Vec<String>,
    column_docs: &Vec<String>,
) -> Result<bool, TantivySearchError> {
    static FUNC_NAME: &str = "index_multi_column_docs";

    let bridge = IndexManager::get_index_writer_bridge(FUNC_NAME, index_path)?;

    // Alias: Field::from_field_id
    let schema = bridge.index.schema();
    let row_id_field = schema.get_field("row_id").map_err(|e| {
        ERROR!(function: FUNC_NAME, "Failed to get row_id field: {}", e.to_string());
        TantivySearchError::TantivyError(e)
    })?;

    let mut doc = Document::default();
    doc.add_u64(row_id_field, row_id);

    for (col_name, column_doc) in column_names.iter().zip(column_docs.iter()) {
        let column_field = schema.get_field(col_name).map_err(|e| {
            ERROR!(function: FUNC_NAME, "Failed to get {} field in schema: {}", col_name, e.to_string());
            TantivySearchError::TantivyError(e)
        })?;
        doc.add_text(column_field, column_doc.clone());
    }

    match bridge.add_document(doc) {
        Ok(_) => Ok(true),
        Err(e) => {
            let error_info = format!("Failed to index doc:{}", e);
            ERROR!(function: FUNC_NAME, "{}", error_info);
            Err(TantivySearchError::InternalError(e))
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use crate::common::constants::FFI_INDEX_WRITER_CACHE;
    use crate::common::{SinglePartTest, TEST_MUTEX};
    use crate::index::implements::index_multi_column_docs;

    #[test]
    pub fn normal_test_index_multi_column_docs() {
        let _guard = TEST_MUTEX.lock().unwrap();
        let tmp_dir = TempDir::new().unwrap();
        let tmp_dir = tmp_dir.path().to_str().unwrap();

        // assert!(create_index(tmp_dir, SinglePartTest::get_column_names()).is_ok());
        let bridge = SinglePartTest::create_index_and_get_writer_bridge(tmp_dir, false);
        // Index some documents.
        let mocked_docs:Vec<Vec<&str>> = SinglePartTest::get_mocked_docs();
        let min_rows = mocked_docs.iter().map(|item| item.len()).min().unwrap();
        for row_id in 0..min_rows{
            let column_docs: Vec<String> = mocked_docs.iter()
                .map(|item| item[row_id].to_string())
                .collect();
            assert!(index_multi_column_docs(tmp_dir, row_id as u64, SinglePartTest::get_column_names(), &column_docs).is_ok());
        }
        assert!(bridge.commit().is_ok());

        // get index writer from CACHE
        let bridge = FFI_INDEX_WRITER_CACHE
            .get_index_writer_bridge(tmp_dir.to_string())
            .unwrap();

        SinglePartTest::search_with_index_writer_bridge(bridge)
    }


    #[test]
    pub fn boundary_test_without_create_index() {
        let _guard = TEST_MUTEX.lock().unwrap();
        let tmp_dir = TempDir::new().unwrap();
        let tmp_dir = tmp_dir.path().to_str().unwrap();

        // Index some documents.
        let mocked_docs:Vec<Vec<&str>> = SinglePartTest::get_mocked_docs();
        let min_rows = mocked_docs.iter().map(|item| item.len()).min().unwrap();
        for row_id in 0..min_rows{
            let column_docs: Vec<String> = mocked_docs.iter()
                .map(|item| item[row_id].to_string())
                .collect();
            assert!(index_multi_column_docs(tmp_dir, row_id as u64, SinglePartTest::get_column_names(), &column_docs).is_err());
        }
    }

}