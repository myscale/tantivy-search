use tantivy::{Index, IndexReader};
use crate::{common::constants::LOG_CALLBACK, INFO};
use crate::logger::logger_bridge::TantivySearchLogger;

pub struct IndexReaderBridge {
    pub path: String,
    pub index: Index,
    pub reader: IndexReader,
}

impl Drop for IndexReaderBridge {
    fn drop(&mut self) {
        // FixMe: release memory map manually
        INFO!(
            "IndexReaderBridge has been dropped. index_path:[{}]",
            self.path
        );
    }
}

impl IndexReaderBridge {
    #[allow(dead_code)]
    pub fn reader_address(&self) -> usize {
        &self.reader as *const IndexReader as usize
    }
    pub fn reload(&self) -> Result<(), String> {
        self.reader.reload().map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use tantivy::{query::QueryParser, Term};
    use tantivy::schema::Field;
    use tempfile::TempDir;
    use crate::common::SinglePartTest;
    use crate::search::collector::row_id_bitmap_collector::RowIdRoaringCollector;

    #[test]
    fn test_get_reader_address() {
        let tmp_dir_1 = TempDir::new().expect("");
        let tmp_dir_2 = TempDir::new().expect("");
        let tmp_dir_1 = tmp_dir_1.path().to_str().unwrap();
        let tmp_dir_2 = tmp_dir_2.path().to_str().unwrap();

        let reader_bridge_1 = SinglePartTest::index_docs_and_get_reader_bridge(tmp_dir_1,  true, true, true);
        let reader_bridge_2 = SinglePartTest::index_docs_and_get_reader_bridge(tmp_dir_2, true, true, true);
        assert_ne!(reader_bridge_1.reader_address(), reader_bridge_2.reader_address());
    }

    #[test]
    fn test_reload_reader() {
        let tmp_dir = TempDir::new().expect("");
        let tmp_dir = tmp_dir.path().to_str().unwrap();

        let (writer_bridge, reader_bridge) = SinglePartTest::index_docs_and_get_all_bridge(tmp_dir, true, true, false);

        let query_parser = QueryParser::for_index(&reader_bridge.index, vec![Field::from_field_id(1)]);
        let query = query_parser.parse_query("Ancient").expect("");
        let collector = RowIdRoaringCollector::with_field("row_id".to_string());

        // Execute search before delete.
        let reader = reader_bridge.index.reader().expect("");
        let res = reader.searcher().search(&query, &collector).expect("");
        assert_eq!(res.len(), 2);

        // Delete row_id terms.
        let term = Term::from_field_u64(Field::from_field_id(0), 0);
        let _ = writer_bridge.delete_term(term);
        assert!(writer_bridge.commit().is_ok());

        // Execute search after delete.
        let res_delete = reader.searcher().search(&query, &collector).expect("");
        assert_eq!(res_delete.len(), 2);

        // Execute search after reader reload.
        assert!(reader.reload().is_ok());
        let res_reload = reader.searcher().search(&query, &collector).expect("");
        assert_eq!(res_reload.len(), 1);
    }
}
