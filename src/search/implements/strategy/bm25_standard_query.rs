use tantivy::schema::{Field, FieldType, Schema, TextFieldIndexing};
use tantivy::{Searcher, Term};
use tantivy::query::BooleanQuery;
use tantivy::tokenizer::{BoxTokenStream, TextAnalyzer};
use crate::common::errors::IndexSearcherError;
use crate::ERROR;
use crate::ffi::RowIdWithScore;
use crate::search::collector::top_dos_with_bitmap_collector::TopDocsWithFilter;
use crate::search::implements::strategy::query_strategy::QueryStrategy;
use crate::common::constants::LOG_CALLBACK;
use crate::logger::logger_bridge::TantivySearchLogger;
/// TODO Need Support Multi Column and BM25 Score.
pub struct BM25StandardQueryStrategy<'a> {
    // pub column_names: &'a Vec<String>,
    pub sentence: &'a str,
    pub topk: &'a u32,
    pub u8_aived_bitmap: &'a Vec<u8>,
    pub query_with_filter: &'a bool,
    pub need_doc: &'a bool,
    pub operation_or: &'a bool,
}

impl<'a> QueryStrategy<Vec<RowIdWithScore>> for BM25StandardQueryStrategy<'a> {
    fn execute(&self, searcher: &Searcher) -> Result<Vec<RowIdWithScore>, IndexSearcherError> {
        let schema: Schema = searcher.index().schema();

        let fields: Vec<Field> = schema
            .fields()
            .filter(|(field, _)|
                schema.get_field_name(*field) != "row_id" 
                // && self.column_names
                //     .iter()
                //     .map(|s| s.as_str()).collect::<Vec<&str>>()
                //     .contains(&schema.get_field_name(*field))
            )
            .map(|(field, _)| field)
            .collect();

        for col_field in &fields {
            let field_type: &FieldType = schema.get_field_entry(*col_field).field_type();
            if !field_type.is_indexed() {
                let error_msg: String = format!("column field:{} not indexed.", schema.get_field_name(*col_field));
                ERROR!(function:"BM25StandardQueryStrategy", "{}", error_msg);
                return Err(IndexSearcherError::InternalError(error_msg));
            }
        }

        let mut top_docs_collector: TopDocsWithFilter =
            TopDocsWithFilter::with_limit(*self.topk as usize)
                .with_searcher(searcher.clone())
                .with_text_fields(fields.clone())
                .with_stored_text(*self.need_doc);

        // If query_with_filter is false, we regards that don't use alive_bitmap.
        if *self.query_with_filter {
            // let mut alive_bitmap: RoaringBitmap = RoaringBitmap::new();
            // alive_bitmap.extend(ConvertUtils::u8_bitmap_to_row_ids(self.u8_aived_bitmap));
            // top_docs_collector = top_docs_collector.with_alive(Arc::new(alive_bitmap));
            top_docs_collector = top_docs_collector.with_alive_u8(self.u8_aived_bitmap.clone());
        }

        let mut terms: Vec<Term> = Vec::new();
        for col_field in &fields {
            let field_type: &FieldType = schema.get_field_entry(*col_field).field_type();
            if let FieldType::Str(ref str_options) = field_type {
                let indexing_options: &TextFieldIndexing =
                    str_options.get_indexing_options().ok_or_else(|| {
                        let error_msg: String = format!(
                            "column field:{} not indexed, but this error msg shouldn't display",
                            schema.get_field_name(*col_field)
                        );
                        ERROR!(function:"BM25StandardQueryStrategy", "{}", error_msg);
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
            } else {
                let error_msg = "Not expected, column field type must be str type.";
                ERROR!(function:"BM25StandardQueryStrategy", "{}", error_msg);
                return Err(IndexSearcherError::InternalError(error_msg.to_string()));
            }
        }

        if *self.operation_or {
            let boolean_query = BooleanQuery::new_multiterms_query(terms);
            searcher
                .search(&boolean_query, &top_docs_collector)
                .map_err(|e| {
                    ERROR!(function:"BM25StandardQueryStrategy", "{}", e);
                    IndexSearcherError::TantivyError(e)
                })
        }else {
            let boolean_query = BooleanQuery::new_multiterms_and_query(terms);
            searcher
                .search(&boolean_query, &top_docs_collector)
                .map_err(|e| {
                    ERROR!(function:"BM25StandardQueryStrategy", "{}", e);
                    IndexSearcherError::TantivyError(e)
                })
        }
    }
}
