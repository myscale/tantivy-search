use tantivy::schema::{Field, Schema};
use crate::common::errors::IndexSearcherError;
use crate::{common::constants::LOG_CALLBACK, ERROR};
use crate::logger::logger_bridge::TantivySearchLogger;
pub(super) struct StrategyUtils;

impl StrategyUtils {
    pub(super) fn get_fields_without_row_id(schema: &Schema) -> Vec<Field> {
        let fields: Vec<Field> = schema
            .fields()
            .filter(|(field, _)| schema.get_field_name(*field) != "row_id")
            .map(|(field, _field_entry)| field)
            .collect();
        return fields;
    }

    pub(super) fn get_field_with_column(schema: &Schema, column_name: &str) -> Result<Field, IndexSearcherError> {
        schema.get_field(column_name).map_err(|e| {
            let error: IndexSearcherError = IndexSearcherError::TantivyError(e);
            ERROR!("{}", error);
            error
        })
    }

    pub(super) fn get_fileds_with_columns(schema: &Schema, column_names: &Vec<String>) -> Result<Vec<Field>, IndexSearcherError> {
        column_names.iter().map(|column_name| {
            schema.get_field(column_name).map_err(|e| {
                let error: IndexSearcherError = IndexSearcherError::TantivyError(e);
                ERROR!("{}", error);
                error
            })
        }).collect()
    }
}