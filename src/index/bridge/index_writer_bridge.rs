use std::sync::Mutex;

use tantivy::{Document, Index, IndexWriter, Opstamp, Term};

use crate::{common::constants::LOG_CALLBACK, INFO};
use crate::logger::logger_bridge::TantivySearchLogger;

pub struct IndexWriterBridge {
    pub path: String,
    pub index: Index,
    pub writer: Mutex<Option<IndexWriter>>,
}

impl IndexWriterBridge {
    pub fn commit(&self) -> Result<Opstamp, String> {
        match self.writer.lock() {
            Ok(mut writer) => {
                if let Some(writer) = writer.as_mut() {
                    writer.commit().map_err(|e| e.to_string())
                } else {
                    Err("IndexWriterBridge is not available for commit".to_string())
                }
            }
            Err(e) => Err(format!("Lock error: {}", e)),
        }
    }

    pub fn add_document(&self, document: Document) -> Result<Opstamp, String> {
        match self.writer.lock() {
            Ok(mut writer) => {
                if let Some(writer) = writer.as_mut() {
                    writer.add_document(document).map_err(|e| e.to_string())
                } else {
                    Err("IndexWriterBridge is not available for add_document".to_string())
                }
            }
            Err(e) => Err(format!("Lock error: {}", e)),
        }
    }

    #[allow(dead_code)]
    pub fn delete_term(&self, term: Term) -> Result<Opstamp, String> {
        match self.writer.lock() {
            Ok(mut writer) => {
                if let Some(writer) = writer.as_mut() {
                    Ok(writer.delete_term(term))
                } else {
                    Err("IndexWriterBridge is not available for delete_term".to_string())
                }
            }
            Err(e) => Err(format!("Lock error: {}", e)),
        }
    }

    // Delete a group of terms.
    pub fn delete_terms(&self, terms: Vec<Term>) -> Result<Opstamp, String> {
        match self.writer.lock() {
            Ok(mut writer) => {
                if let Some(writer) = writer.as_mut() {
                    let mut opstamp: Opstamp = 0;
                    for term in terms {
                        opstamp = writer.delete_term(term)
                    }
                    Ok(opstamp)
                } else {
                    Err("IndexWriterBridge is not available for delete_term".to_string())
                }
            }
            Err(e) => Err(format!("Lock error: {}", e)),
        }
    }

    pub fn wait_merging_threads(&self) -> Result<(), String> {
        match self.writer.lock() {
            Ok(mut writer) => {
                if let Some(writer) = writer.take() {
                    let _ = writer.wait_merging_threads();
                };
                Ok(())
            }
            Err(e) => Err(format!("Lock error: {}", e)),
        }
    }
}

impl Drop for IndexWriterBridge {
    fn drop(&mut self) {
        INFO!("IndexW has been dropped. index_path:[{}]", self.path);
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
    pub fn normal_test_bridge_commit() {
        let tmp_dir = TempDir::new().expect("");
        let tmp_dir = tmp_dir.path().to_str().unwrap();

        let bridge = SinglePartTest::index_docs_and_get_writer_bridge(tmp_dir, true, false, false);
        let query_parser = QueryParser::for_index(&bridge.index, vec![Field::from_field_id(1)]);
        let text_query = query_parser.parse_query("Ancient").expect("");
        let collector = RowIdRoaringCollector::with_field("row_id".to_string());

        // Execute a query before commit.
        let searcher_1 = bridge.index.reader().expect("").searcher();
        let res_1 = searcher_1.search(&text_query, &collector).expect("");
        assert_eq!(res_1.len(), 0);

        assert!(bridge.commit().is_ok());

        // Execute a query after commit.
        let searcher_2 = bridge.index.reader().expect("").searcher();
        let res_2 = searcher_2.search(&text_query, &collector).expect("");
        assert_eq!(res_2.len(), 2);

        // commit multi times is fine
        assert!(bridge.commit().is_ok());
    }

    #[test]
    pub fn normal_test_bridge_wait_merge() {
        let tmp_dir = TempDir::new().expect("");
        let tmp_dir = tmp_dir.path().to_str().unwrap();

        let bridge = SinglePartTest::index_docs_and_get_writer_bridge(tmp_dir, true, true, false);
        // Now all merging threads are living.
        {
            let lock = bridge.writer.lock().unwrap();
            let index_writer_some = &*lock;
            assert!(index_writer_some.is_some());
        }
        assert!(bridge.wait_merging_threads().is_ok());
        // After wait all merging threads were killed.
        {
            let lock = bridge.writer.lock().unwrap();
            let index_writer_some = &*lock;
            assert!(index_writer_some.is_none());
        }
        // Waiting a not exist IndexWriterBridge merging threads is also fine.
        assert!(bridge.wait_merging_threads().is_ok());
    }

    #[test]
    pub fn test_delete_terms() {
        let tmp_dir = TempDir::new().expect("");
        let tmp_dir = tmp_dir.path().to_str().unwrap();

        let bridge = SinglePartTest::index_docs_and_get_writer_bridge(tmp_dir, true, true, false);
        let query_parser = QueryParser::for_index(&bridge.index, vec![Field::from_field_id(1)]);
        let collector = RowIdRoaringCollector::with_field("row_id".to_string());
        let text_query = query_parser.parse_query("Ancient").expect("Can't parse query");
        // Execute a query before delete a group of terms.
        let searcher_1 = bridge.index.reader().expect("").searcher();
        let res_1 = searcher_1.search(&text_query, &collector).expect("");
        assert_eq!(res_1.len(), 2);

        // Delete a group of terms.
        let row_ids = vec![0, 1, 2];
        let terms: Vec<Term> = row_ids
            .iter()
            .map(|row_id| Term::from_field_u64(Field::from_field_id(0), *row_id as u64))
            .collect();
        assert!(bridge.delete_terms(terms).is_ok());
        assert!(bridge.commit().is_ok());

        // Execute a query after delete a group of terms.
        let searcher_2 = bridge.index.reader().expect("").searcher();
        let res_2 = searcher_2.search(&text_query, &collector).expect("");
        assert_eq!(res_2.len(), 1);

        // Delete a specific term.
        let term = Term::from_field_u64(Field::from_field_id(0), 4);
        assert!(bridge.delete_term(term).is_ok());
        assert!(bridge.commit().is_ok());

        // Execute a query after delete the specific term.
        let searcher_3 = bridge.index.reader().expect("").searcher();
        let res_3 = searcher_3.search(&text_query, &collector).expect("");
        assert_eq!(res_3.len(), 0);
    }
}
