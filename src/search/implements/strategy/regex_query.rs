use crate::common::errors::IndexSearcherError;
use crate::search::collector::row_id_bitmap_collector::RowIdRoaringCollector;
use crate::search::implements::strategy::query_strategy::QueryStrategy;
use crate::search::utils::ConvertUtils;
use crate::{common::constants::LOG_CALLBACK, ERROR};
use crate::logger::logger_bridge::TantivySearchLogger;
use roaring::RoaringBitmap;
use std::sync::Arc;
use tantivy::query::RegexQuery;
use tantivy::schema::Schema;
use tantivy::Searcher;
use crate::search::implements::strategy::utils::StrategyUtils;

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
        static FUNC_NAME: &str = "RegexQueryStrategy";

        let schema: Schema = searcher.index().schema();
        let field = StrategyUtils::get_field_with_column(&schema, self.column_name)?;

        let row_id_collector: RowIdRoaringCollector = RowIdRoaringCollector::with_field("row_id".to_string());
        let regex_query: RegexQuery = RegexQuery::from_pattern(&ConvertUtils::like_to_regex(self.pattern), field).map_err(|e|{
            ERROR!(function: FUNC_NAME, "Error when parse regex query:{}. {}", ConvertUtils::like_to_regex(self.pattern), e);
            IndexSearcherError::TantivyError(e)
        })?;

        searcher.search(&regex_query, &row_id_collector).map_err(|e|{
            ERROR!(function: FUNC_NAME, "Error when execute regex query:{}. {}", ConvertUtils::like_to_regex(self.pattern), e);
            IndexSearcherError::TantivyError(e)
        })
    }
}
