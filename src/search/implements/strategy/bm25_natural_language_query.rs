use std::sync::Arc;
use roaring::RoaringBitmap;
use tantivy::query::{Query, QueryParser, QueryParserError};
use tantivy::schema::{Field, Schema};
use tantivy::{Searcher, TantivyError};
use crate::common::errors::IndexSearcherError;
use crate::ERROR;
use crate::ffi::RowIdWithScore;
use crate::search::collector::top_dos_with_bitmap_collector::TopDocsWithFilter;
use crate::search::implements::strategy::query_strategy::QueryStrategy;
use crate::search::utils::convert_utils::ConvertUtils;
use crate::common::constants::LOG_CALLBACK;
use crate::logger::logger_bridge::TantivySearchLogger;
/// Execute query for a sentence and get bm25 score.
/// Query will be run in all schema fields but `row_id`.
/// This sentence may be written by natural language, or just simple terms.
/// If `query_with_filter` is true, when calculating bm25 score, only in `alive_row_ids` will be recorded.
///
/// Params:
/// - `sentence`: Sentence need to be parsed and query.
/// - `topk`: max-heap build with topK
/// - `u8_aived_bitmap`: Represent row_ids who are alived.
/// - `query_with_filter`: Whether collect row_ids with `u8_alived_bitmap`
///
pub struct BM25NaturalLanguageStrategy<'a> {
    pub sentence: &'a str,
    pub topk: &'a u32,
    pub u8_aived_bitmap: &'a Vec<u8>,
    pub query_with_filter: &'a bool,
    pub need_doc: &'a bool,
    pub operation_or: &'a bool,
}

impl<'a> QueryStrategy<Vec<RowIdWithScore>> for BM25NaturalLanguageStrategy<'a> {
    fn execute(&self, searcher: &Searcher) -> Result<Vec<RowIdWithScore>, IndexSearcherError> {
        let schema: Schema = searcher.index().schema();

        let fields: Vec<Field> = schema
            .fields()
            .filter(|(field, _)| schema.get_field_name(*field) != "row_id")
            .map(|(field, _)| field)
            .collect();

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

        let mut query_parser: QueryParser = QueryParser::for_index(searcher.index(), fields);
        if !*self.operation_or {
            query_parser.set_conjunction_by_default();
        }
        let text_query: Box<dyn Query> = query_parser.parse_query(self.sentence).map_err(
            |e: QueryParserError| {
                ERROR!(function:"BM25NaturalLanguageStrategy", "Error when parse: {}. {}", self.sentence, e);
                IndexSearcherError::QueryParserError(e.to_string())
            },
        )?;

        searcher.search(&text_query, &top_docs_collector).map_err(|e: TantivyError|{
            ERROR!(function:"BM25NaturalLanguageStrategy", "Error when execute: {}. {}", self.sentence, e);
            IndexSearcherError::TantivyError(e)
        })
    }
}