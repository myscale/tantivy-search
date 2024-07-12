use crate::common::errors::IndexSearcherError;
use crate::search::collector::row_id_bitmap_collector::RowIdRoaringCollector;
use crate::search::implements::strategy::query_strategy::QueryStrategy;
use crate::{common::constants::LOG_CALLBACK, ERROR};
use crate::logger::logger_bridge::TantivySearchLogger;
use roaring::RoaringBitmap;
use std::sync::Arc;
use tantivy::query::TermSetQuery;
use tantivy::schema::{Field, FieldType, Schema, TextFieldIndexing};
use tantivy::tokenizer::{BoxTokenStream, TextAnalyzer};
use tantivy::{Searcher, Term};

/// Execute query for a group of terms.
///
/// Params:
/// - `column_name`: Execute query in which column.
/// - `terms`: A group of terms.
///
pub struct TermSetQueryStrategy<'a> {
    pub column_name: &'a str,
    pub terms: &'a Vec<String>,
}

impl<'a> QueryStrategy<Arc<RoaringBitmap>> for TermSetQueryStrategy<'a> {
    fn execute(&self, searcher: &Searcher) -> Result<Arc<RoaringBitmap>, IndexSearcherError> {
        static FUNC_NAME: &str = "TermSetQueryStrategy";

        let schema: Schema = searcher.index().schema();

        let col_field: Field = schema.get_field(self.column_name).map_err(|e| {
            let error: IndexSearcherError = IndexSearcherError::TantivyError(e);
            ERROR!(function: FUNC_NAME, "{}", error);
            error
        })?;

        let field_type: &FieldType = schema.get_field_entry(col_field).field_type();
        if !field_type.is_indexed() {
            let error_msg: String = format!("column field:{} not indexed.", self.column_name);
            ERROR!(function: FUNC_NAME, "{}", error_msg);
            return Err(IndexSearcherError::InternalError(error_msg));
        }

        let mut terms: Vec<Term> = Vec::new();

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
            let mut text_analyzer: TextAnalyzer = searcher
                .index()
                .tokenizers()
                .get(indexing_options.tokenizer())
                .unwrap();

            for term in self.terms {
                let mut token_stream: BoxTokenStream<'_> = text_analyzer.token_stream(term);
                token_stream.process(&mut |token| {
                    terms.push(Term::from_field_text(col_field, &token.text));
                });
            }
        } else {
            // Not Expected.
            for term in self.terms {
                terms.push(Term::from_field_text(col_field, &term));
            }
        }

        let ter_set_query: TermSetQuery = TermSetQuery::new(terms);
        let row_id_collector: RowIdRoaringCollector =
            RowIdRoaringCollector::with_field("row_id".to_string());

        searcher
            .search(&ter_set_query, &row_id_collector)
            .map_err(|e| {
                ERROR!(function: FUNC_NAME, "{}", e);
                IndexSearcherError::TantivyError(e)
            })
    }
}
