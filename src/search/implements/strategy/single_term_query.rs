use crate::common::errors::IndexSearcherError;
use crate::search::collector::row_id_bitmap_collector::RowIdRoaringCollector;
use crate::search::implements::strategy::query_strategy::QueryStrategy;
use crate::{common::constants::LOG_CALLBACK, ERROR};
use crate::logger::logger_bridge::TantivySearchLogger;
use roaring::RoaringBitmap;
use std::sync::Arc;
use tantivy::query::{TermQuery, TermSetQuery};
use tantivy::schema::{FieldType, IndexRecordOption, Schema, TextFieldIndexing};
use tantivy::tokenizer::{BoxTokenStream, TextAnalyzer};
use tantivy::{Searcher, Term};
use crate::search::implements::strategy::utils::StrategyUtils;

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
        static FUNC_NAME: &str = "SingleTermQueryStrategy";

        let schema: Schema = searcher.index().schema();
        let field = StrategyUtils::get_field_with_column(&schema, self.column_name)?;
        let field_type: &FieldType = schema.get_field_entry(field).field_type();
        if !field_type.is_indexed() {
            let error_msg: String = format!("column field:{} not indexed.", self.column_name);
            ERROR!(function: FUNC_NAME, "{}", error_msg);
            return Err(IndexSearcherError::InternalError(error_msg));
        }

        if let FieldType::Str(ref str_options) = field_type {
            let indexing_options: &TextFieldIndexing =
                str_options.get_indexing_options().ok_or_else(|| {
                    let error_msg: String = format!(
                        "column field:{} not indexed, but this error msg shouldn't display",
                        self.column_name
                    );
                    ERROR!(function: FUNC_NAME, "{}", error_msg);
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
                let term: Term = Term::from_field_text(field, &token.text);
                terms.push(term);
            });

            let ter_set_query: TermSetQuery = TermSetQuery::new(terms);
            let row_id_collector: RowIdRoaringCollector =
                RowIdRoaringCollector::with_field("row_id".to_string());

            searcher
                .search(&ter_set_query, &row_id_collector)
                .map_err(|e| {
                    ERROR!(function: FUNC_NAME, "{}", e);
                    IndexSearcherError::TantivyError(e)
                })
        } else {
            // FixMe: Not Expected.
            let term: Term = Term::from_field_text(field, self.term);
            let term_query: TermQuery = TermQuery::new(term, IndexRecordOption::WithFreqs);
            let row_id_collector: RowIdRoaringCollector =
                RowIdRoaringCollector::with_field("row_id".to_string());

            searcher
                .search(&term_query, &row_id_collector)
                .map_err(|e| {
                    ERROR!(function: FUNC_NAME, "{}", e);
                    IndexSearcherError::TantivyError(e)
                })
        }
    }
}
