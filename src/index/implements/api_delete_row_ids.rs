use tantivy::Term;
use crate::common::errors::TantivySearchError;
use crate::{common::constants::LOG_CALLBACK, ERROR};
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::index::implements::index_manager::IndexManager;

// FixMe: If all writer threads were killed, this func won't delete any rows.
pub fn delete_row_ids(index_path: &str, row_ids: &Vec<u32>) -> Result<bool, TantivySearchError> {
    static FUNC_NAME: &str = "delete_row_ids";

    let bridge = IndexManager::get_index_writer_bridge(FUNC_NAME, index_path)?;

    // Alias: Field::from_field_id
    let schema = bridge.index.schema();
    let row_id_field = schema.get_field("row_id").map_err(|e| {
        ERROR!(function: FUNC_NAME, "Failed to get row_id field: {}", e.to_string());
        TantivySearchError::TantivyError(e)
    })?;

    let terms = row_ids
        .iter()
        .map(|&row_id| Term::from_field_u64(row_id_field, row_id as u64))
        .collect();

    // Delete row_id terms.
    bridge.delete_terms(terms).map_err(|e| {
        ERROR!(function: FUNC_NAME, "{}", e);
        TantivySearchError::InternalError(e)
    })?;

    // After delete_term, commit index writer.
    bridge.commit().map_err(|e| {
        let error_info = format!("Failed to commit index writer: {}", e.to_string());
        ERROR!(function: FUNC_NAME, "{}", error_info);
        TantivySearchError::InternalError(error_info)
    })?;

    // Try reload index reader from CACHE
    let reload_status = IndexManager::reload_index_reader(index_path)?;

    Ok(reload_status)
}

#[cfg(test)]
mod tests {
    use tantivy::collector::Count;
    use tantivy::query::QueryParser;
    use tantivy::schema::Field;
    use tempfile::TempDir;
    use crate::common::{SinglePartTest, TEST_MUTEX};
    use crate::index::implements::delete_row_ids;

    #[test]
    pub fn test_delete_row_ids() {
        let _guard = TEST_MUTEX.lock().unwrap();
        let tmp_dir = TempDir::new().unwrap();
        let tmp_dir = tmp_dir.path().to_str().unwrap();

        // Shouldn't kill any writer threads.
        let index_writer_bridge =
            SinglePartTest::index_docs_and_get_writer_bridge(tmp_dir, true, true, false);

        let col1 = Field::from_field_id(1);
        let query_parser = QueryParser::for_index(&index_writer_bridge.index, vec![col1]);
        let text_query = query_parser.parse_query("Ancient").unwrap();

        // Check searched count before execute delete.
        let reader = index_writer_bridge.index.reader().unwrap();
        let count_col1 = reader.searcher().search(&text_query, &Count).unwrap();
        assert_eq!(count_col1, 2);

        assert!(delete_row_ids(tmp_dir, &vec![0, 1, 2, 3]).is_ok());
        assert!(reader.reload().is_ok());

        // Check searched count after execute delete.
        let count_col1 = reader.searcher().search(&text_query, &Count).unwrap();
        assert_eq!(count_col1, 1);
    }
}