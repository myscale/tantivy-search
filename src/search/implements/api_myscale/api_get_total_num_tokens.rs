use tantivy::query::Bm25StatisticsProvider;
use tantivy::schema::{FieldType, Schema};
use crate::common::errors::TantivySearchError;
use crate::{common::constants::LOG_CALLBACK, DEBUG};
use crate::ffi::FieldTokenNums;
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::search::implements::search_utils::SearchUtils;

pub fn get_total_num_tokens(index_path: &str) -> Result<Vec<FieldTokenNums>, TantivySearchError> {
    static FUNC_NAME: &str = "get_total_num_tokens";

    let bridge = SearchUtils::get_index_reader_bridge(FUNC_NAME, index_path)?;
    let schema: Schema = bridge.index.schema();
    let searcher = bridge.reader.searcher();
    let mut result: Vec<FieldTokenNums> = vec![];

    for (col_field, col_field_entry) in schema.fields() {
        let field_type = col_field_entry.field_type();
        if !field_type.is_indexed() {
            continue;
        }
        if let FieldType::Str(_) = field_type {
            let field_total_num_tokens = searcher.total_num_tokens(col_field)?;
            DEBUG!(function: FUNC_NAME, "total_num_tokens for field-id:{} is {}", col_field.field_id(), field_total_num_tokens);
            result.push(FieldTokenNums::new(col_field.field_id(), field_total_num_tokens));
        }
    }
    Ok(result)
}
