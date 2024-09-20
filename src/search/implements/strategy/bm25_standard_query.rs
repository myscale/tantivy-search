use crate::common::errors::IndexSearcherError;
use crate::ffi::RowIdWithScore;
use crate::search::collector::top_dos_with_bitmap_collector::TopDocsWithFilter;
use crate::search::implements::strategy::query_strategy::QueryStrategy;
use crate::{common::constants::LOG_CALLBACK, ERROR};
use crate::logger::logger_bridge::TantivySearchLogger;
use tantivy::query::{BooleanQuery, Occur, Query};
use tantivy::schema::{FieldType, Schema, TextFieldIndexing};
use tantivy::tokenizer::{BoxTokenStream, TextAnalyzer};
use tantivy::{Searcher, Term};
use crate::search::implements::strategy::utils::StrategyUtils;

pub struct BM25StandardQueryStrategy<'a> {
    pub sentence: &'a str,
    pub column_names: &'a Vec<String>,
    pub top_k: &'a u32,
    pub u8_alive_bitmap: &'a Vec<u8>,
    pub query_with_filter: &'a bool,
    pub need_doc: &'a bool,
    pub operation_or: &'a bool,
}

impl<'a> QueryStrategy<Vec<RowIdWithScore>> for BM25StandardQueryStrategy<'a> {
    fn execute(&self, searcher: &Searcher) -> Result<Vec<RowIdWithScore>, IndexSearcherError> {
        static FUNC_NAME: &str = "BM25StandardQueryStrategy";

        let schema: Schema = searcher.index().schema();
        let fields = if self.column_names.is_empty() {
            StrategyUtils::get_fields_without_row_id(&schema)
        } else {
            StrategyUtils::get_fileds_with_columns(&schema, self.column_names)?
        };

        for col_field in &fields {
            let field_type: &FieldType = schema.get_field_entry(*col_field).field_type();
            if !field_type.is_indexed() {
                let error_msg: String = format!(
                    "column field:{} not indexed.",
                    schema.get_field_name(*col_field)
                );
                ERROR!(function: FUNC_NAME, "{}", error_msg);
                return Err(IndexSearcherError::InternalError(error_msg));
            }
        }

        let mut top_docs_collector: TopDocsWithFilter =
            TopDocsWithFilter::with_limit(*self.top_k as usize)
                .with_searcher(searcher.clone())
                .with_text_fields(fields.clone())
                .with_stored_text(*self.need_doc);

        if *self.query_with_filter {
            top_docs_collector = top_docs_collector.with_alive_u8(self.u8_alive_bitmap.clone());
        }

        let mut subqueries: Vec<(Occur, Box<dyn Query>)> = Vec::new();

        for col_field in &fields {
            let mut terms: Vec<Term> = Vec::new();
            let field_type: &FieldType = schema.get_field_entry(*col_field).field_type();
            if let FieldType::Str(ref str_options) = field_type {
                let indexing_options: &TextFieldIndexing =
                    str_options.get_indexing_options().ok_or_else(|| {
                        let error_msg: String = format!(
                            "column field:{} not indexed, but this error msg shouldn't display",
                            schema.get_field_name(*col_field)
                        );
                        ERROR!(function: FUNC_NAME, "{}", error_msg);
                        IndexSearcherError::InternalError(error_msg)
                    })?;

                let mut text_analyzer: TextAnalyzer = searcher
                    .index()
                    .tokenizers()
                    .get(indexing_options.tokenizer())
                    .unwrap();

                let mut token_stream: BoxTokenStream<'_> = text_analyzer.token_stream(self.sentence);
                token_stream.process(&mut |token| {
                    terms.push(Term::from_field_text(*col_field, &token.text));
                });

                if *self.operation_or {
                    let subquery: Box<dyn Query> = Box::new(BooleanQuery::new_multiterms_query(terms));
                    subqueries.push((Occur::Should, subquery));
                } else {
                    let subquery: Box<dyn Query> = Box::new(BooleanQuery::new_multiterms_and_query(terms));
                    subqueries.push((Occur::Should, subquery));
                }
            } else {
                let error_msg = "Not expected, column field type must be str type.";
                ERROR!(function: FUNC_NAME, "{}", error_msg);
                return Err(IndexSearcherError::InternalError(error_msg.to_string()));
            }
        }

        searcher
            .search(&BooleanQuery::new(subqueries), &top_docs_collector)
            .map_err(|e| {
                ERROR!(function: FUNC_NAME, "{}", e);
                IndexSearcherError::TantivyError(e)
            })
    }
}
