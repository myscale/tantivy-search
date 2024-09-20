use crate::common::errors::IndexSearcherError;
use crate::ffi::RowIdWithScore;
use crate::search::collector::top_dos_with_bitmap_collector::TopDocsWithFilter;
use crate::search::implements::strategy::query_strategy::QueryStrategy;
use crate::{common::constants::LOG_CALLBACK, ERROR};
use crate::logger::logger_bridge::TantivySearchLogger;
use tantivy::query::{Query, QueryParser, QueryParserError};
use tantivy::schema::Schema;
use tantivy::{Searcher, TantivyError};
use crate::search::implements::strategy::utils::StrategyUtils;

/// Execute query for a sentence and get bm25 score.
/// Query will be run in all schema fields but `row_id`.
/// This sentence may be written by natural language, or just simple terms.
/// If `query_with_filter` is true, when calculating bm25 score, only in `alive_row_ids` will be recorded.
///
/// Params:
/// - `sentence`: Sentence need to be parsed and query.
/// - `top_k`: max-heap build with top_k
/// - `u8_alive_bitmap`: Represent row_ids who are alive.
/// - `query_with_filter`: Whether collect row_ids with `u8_alive_bitmap`
///
pub struct BM25NaturalLanguageStrategy<'a> {
    pub sentence: &'a str,
    pub column_names: &'a Vec<String>,
    pub top_k: &'a u32,
    pub u8_alive_bitmap: &'a Vec<u8>,
    pub query_with_filter: &'a bool,
    pub need_doc: &'a bool,
    pub operation_or: &'a bool,
}

impl<'a> QueryStrategy<Vec<RowIdWithScore>> for BM25NaturalLanguageStrategy<'a> {
    fn execute(&self, searcher: &Searcher) -> Result<Vec<RowIdWithScore>, IndexSearcherError> {
        static FUNC_NAME: &str = "BM25NaturalLanguageStrategy";

        let schema: Schema = searcher.index().schema();

        let fields = if self.column_names.is_empty() {
            StrategyUtils::get_fields_without_row_id(&schema)
        } else {
            StrategyUtils::get_fileds_with_columns(&schema, self.column_names)?
        };

        let mut top_docs_collector: TopDocsWithFilter =
            TopDocsWithFilter::with_limit(*self.top_k as usize)
                .with_searcher(searcher.clone())
                .with_text_fields(fields.clone())
                .with_stored_text(*self.need_doc);

        if *self.query_with_filter {
            top_docs_collector = top_docs_collector.with_alive_u8(self.u8_alive_bitmap.clone());
        }

        let mut query_parser: QueryParser = QueryParser::for_index(searcher.index(), fields);
        if !*self.operation_or {
            query_parser.set_conjunction_by_default();
        }

        let text_query: Box<dyn Query> = query_parser.parse_query(self.sentence).map_err(
            |e: QueryParserError| {
                ERROR!(function: FUNC_NAME, "Error when parse: {}. {}", self.sentence, e);
                IndexSearcherError::QueryParserError(e.to_string())
            },
        )?;

        searcher.search(&text_query, &top_docs_collector).map_err(|e: TantivyError|{
            ERROR!(function: FUNC_NAME, "Error when execute: {}. {}", self.sentence, e);
            IndexSearcherError::TantivyError(e)
        })
    }
}
