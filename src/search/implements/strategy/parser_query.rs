use std::sync::Arc;
use roaring::RoaringBitmap;
use tantivy::query::QueryParser;
use tantivy::schema::{Field, Schema};
use tantivy::Searcher;
use crate::common::errors::IndexSearcherError;
use crate::common::constants::LOG_CALLBACK;
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::ERROR;
use crate::search::collector::row_id_bitmap_collector::RowIdRoaringCollector;
use crate::search::implements::strategy::query_strategy::QueryStrategy;

/// Execute query for a sentence.
/// This sentence may be written by natural language, or just simple terms.
///
/// Params:
/// - `column_name`: Execute query in which column.
/// - `sentence`: Sentence need to be parsed and query.
///
pub struct ParserQueryStrategy<'a> {
    pub column_name: &'a str,
    pub sentence: &'a str,
}

impl<'a> QueryStrategy<Arc<RoaringBitmap>> for ParserQueryStrategy<'a> {
    fn execute(&self, searcher: &Searcher) -> Result<Arc<RoaringBitmap>, IndexSearcherError> {
        let schema: Schema = searcher.index().schema();

        let col_field: Field = schema.get_field(self.column_name).map_err(|e| {
            let error: IndexSearcherError = IndexSearcherError::TantivyError(e);
            ERROR!(function:"SingleTermQueryStrategy", "{}", error);
            error
        })?;

        let row_id_collector: RowIdRoaringCollector =
            RowIdRoaringCollector::with_field("row_id".to_string());
        let query_parser: QueryParser =
            QueryParser::for_index(searcher.index(), [col_field].to_vec());

        let text_query = query_parser.parse_query(self.sentence).map_err(|e| {
            ERROR!(function:"ParserQueryStrategy", "Error when parse: {}. {}", self.sentence, e);
            IndexSearcherError::QueryParserError(e.to_string())
        })?;

        searcher.search(&text_query, &row_id_collector).map_err(|e|{
            ERROR!(function:"ParserQueryStrategy", "Error when execute: {}. {}", self.sentence, e);
            IndexSearcherError::TantivyError(e)
        })
    }
}