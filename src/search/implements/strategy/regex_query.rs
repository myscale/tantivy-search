use std::sync::Arc;
use roaring::RoaringBitmap;
use tantivy::query::RegexQuery;
use tantivy::schema::{Field, Schema};
use tantivy::Searcher;
use crate::common::errors::IndexSearcherError;
use crate::common::constants::LOG_CALLBACK;
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::ERROR;
use crate::search::collector::row_id_bitmap_collector::RowIdRoaringCollector;
use crate::search::implements::strategy::query_strategy::QueryStrategy;
use crate::search::utils::convert_utils::ConvertUtils;

/// Execute regex query for a given pattern.
///
/// Params:
/// - `column_name`: Execute query in which column.
/// - `pattern`: Regex query will execute with given pattern str.
///
pub struct RegexQueryStrategy<'a> {
    pub column_name: &'a str,
    pub pattern: &'a str,
}

impl<'a> QueryStrategy<Arc<RoaringBitmap>> for RegexQueryStrategy<'a> {
    fn execute(&self, searcher: &Searcher) -> Result<Arc<RoaringBitmap>, IndexSearcherError> {
        let schema: Schema = searcher.index().schema();

        let col_field: Field = schema.get_field(self.column_name).map_err(|e| {
            let error: IndexSearcherError = IndexSearcherError::TantivyError(e);
            ERROR!(function:"RegexQueryStrategy", "{}", error);
            error
        })?;

        let row_id_collector: RowIdRoaringCollector =
            RowIdRoaringCollector::with_field("row_id".to_string());
        let regex_query: RegexQuery = RegexQuery::from_pattern(&ConvertUtils::like_to_regex(self.pattern), col_field).map_err(|e|{
            ERROR!(function:"RegexQueryStrategy", "Error when parse regex query:{}. {}", ConvertUtils::like_to_regex(self.pattern), e);
            IndexSearcherError::TantivyError(e)
        })?;

        searcher.search(&regex_query, &row_id_collector).map_err(|e|{
            ERROR!(function:"RegexQueryStrategy", "Error when execute regex query:{}. {}", ConvertUtils::like_to_regex(self.pattern), e);
            IndexSearcherError::TantivyError(e)
        })
    }
}
