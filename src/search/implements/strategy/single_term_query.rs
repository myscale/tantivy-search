use std::sync::Arc;
use roaring::RoaringBitmap;
use tantivy::schema::{Field, FieldType, IndexRecordOption, Schema, TextFieldIndexing};
use tantivy::{Searcher, Term};
use tantivy::query::{TermQuery, TermSetQuery};
use tantivy::tokenizer::{BoxTokenStream, TextAnalyzer};
use crate::common::errors::IndexSearcherError;
use crate::common::constants::LOG_CALLBACK;
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::ERROR;
use crate::search::collector::row_id_bitmap_collector::RowIdRoaringCollector;
use crate::search::implements::strategy::query_strategy::QueryStrategy;

/// Execute query for one term.
///
/// Params:
/// - `column_name`: Execute query in which column.
/// - `term`: Term need to be queried.
///
pub struct SingleTermQueryStrategy<'a> {
    pub column_name: &'a str,
    pub term: &'a str,
}

impl<'a> QueryStrategy<Arc<RoaringBitmap>> for SingleTermQueryStrategy<'a> {
    fn execute(&self, searcher: &Searcher) -> Result<Arc<RoaringBitmap>, IndexSearcherError> {
        let schema: Schema = searcher.index().schema();

        let col_field: Field = schema.get_field(self.column_name).map_err(|e| {
            let error: IndexSearcherError = IndexSearcherError::TantivyError(e);
            ERROR!(function:"SingleTermQueryStrategy", "{}", error);
            error
        })?;

        let field_type: &FieldType = schema.get_field_entry(col_field).field_type();
        if !field_type.is_indexed() {
            let error_msg: String = format!("column field:{} not indexed.", self.column_name);
            ERROR!(function:"SingleTermQueryStrategy", "{}", error_msg);
            return Err(IndexSearcherError::InternalError(error_msg));
        }

        if let FieldType::Str(ref str_options) = field_type {
            let indexing_options: &TextFieldIndexing =
                str_options.get_indexing_options().ok_or_else(|| {
                    let error_msg: String = format!(
                        "column field:{} not indexed, but this error msg shouldn't display",
                        self.column_name
                    );
                    ERROR!(function:"SingleTermQueryStrategy", "{}", error_msg);
                    IndexSearcherError::InternalError(error_msg)
                })?;
            let mut terms: Vec<Term> = Vec::new();
            let mut text_analyzer: TextAnalyzer = searcher
                .index()
                .tokenizers()
                .get(indexing_options.tokenizer())
                .unwrap();
            let mut token_stream: BoxTokenStream<'_> = text_analyzer.token_stream(self.term);
            token_stream.process(&mut |token| {
                let term: Term = Term::from_field_text(col_field, &token.text);
                terms.push(term);
            });

            let ter_set_query: TermSetQuery = TermSetQuery::new(terms);
            let row_id_collector: RowIdRoaringCollector =
                RowIdRoaringCollector::with_field("row_id".to_string());

            searcher
                .search(&ter_set_query, &row_id_collector)
                .map_err(|e| {
                    ERROR!(function:"SingleTermQueryStrategy", "{}", e);
                    IndexSearcherError::TantivyError(e)
                })
        } else {
            // Not Expected.
            let term: Term = Term::from_field_text(col_field, self.term);
            let term_query: TermQuery = TermQuery::new(term, IndexRecordOption::WithFreqs);
            let row_id_collector: RowIdRoaringCollector =
                RowIdRoaringCollector::with_field("row_id".to_string());
            println!("for not str");
            searcher
                .search(&term_query, &row_id_collector)
                .map_err(|e| {
                    ERROR!(function:"SingleTermQueryStrategy", "{}", e);
                    IndexSearcherError::TantivyError(e)
                })
        }
    }
}