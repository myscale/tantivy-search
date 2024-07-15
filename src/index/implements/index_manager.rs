use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tantivy::Index;
use tantivy::schema::{FAST, INDEXED, IndexRecordOption, Schema, TEXT, TextFieldIndexing, TextOptions};
use crate::common::errors::TantivySearchError;
use crate::{common::constants::LOG_CALLBACK, ERROR, INFO};
use crate::logger::logger_bridge::TantivySearchLogger;

use crate::common::constants::{FFI_INDEX_SEARCHER_CACHE, FFI_INDEX_WRITER_CACHE};
use crate::index::bridge::IndexWriterBridge;
use crate::index::implements::api_free_index_writer::free_index_writer;
use crate::search::implements::free_index_reader;
use crate::tokenizer::parser::{TokenizerConfig, TokenizerUtils};
use crate::utils::index_utils::IndexUtils;

const MEMORY_64MB: usize = 1024 * 1024 * 64;
const BUILD_THREADS: usize = 2;

pub struct IndexManager;

impl IndexManager {
    pub(crate) fn prepare_directory(func_name: &str, index_path: &str) -> Result<(), TantivySearchError> {
        free_index_reader(index_path);
        free_index_writer(index_path).map_err(|e| {
            ERROR!(function: func_name, "{}", e);
            e
        })?;
        Ok(())
    }

    pub(crate) fn persist_index_params(index_path: &str, index_json_parameter: &str) -> Result<(), TantivySearchError> {
        let index_files_directory: &Path = Path::new(index_path);
        IndexUtils::save_index_parameter_to_disk(
            index_files_directory,
            index_json_parameter.to_string(),
            true,
        )?;
        Ok(())
    }

    pub(crate) fn build_schema(
        func_name: &str,
        column_names: &Vec<String>,
        index_json_parameter: &str
    ) -> Result<(Schema, HashMap<String, TokenizerConfig>), TantivySearchError> {
        let mut schema_builder = Schema::builder();
        schema_builder.add_u64_field("row_id", FAST | INDEXED);
        let combine = |left:&str, right:&str| {format!("{}_{}", left, right)};

        let config_map: HashMap<String, TokenizerConfig> =
            TokenizerUtils::parser_index_json_parameter(index_json_parameter)
                .map_err(|e| {
                    ERROR!(function: func_name, "{}", e.to_string());
                    TantivySearchError::TokenizerError(e)
                })?;

        for col_name in column_names {
            if let Some(config) = config_map.get(col_name) {
                let tokenizer_name = combine(col_name, config.tokenizer_name.as_str());
                let mut text_options = TextOptions::default().set_indexing_options(
                    TextFieldIndexing::default()
                        .set_tokenizer(tokenizer_name.as_str())
                        .set_index_option(IndexRecordOption::WithFreqsAndPositions),
                );
                if config.doc_store {
                    text_options = text_options.set_stored();
                }
                INFO!(function: func_name, "col_name:{}, tokenizer name: {}", col_name, tokenizer_name);
                schema_builder.add_text_field(col_name, text_options);
            } else {
                INFO!(function: func_name, "col_name:{}, tokenizer name: {}", col_name, "TEXT");
                schema_builder.add_text_field(col_name, TEXT);
            }
        }
        Ok((schema_builder.build(), config_map))
    }

    pub(crate) fn create_writer(
        func_name: &str,
        index: &Index,
        index_path: &str
    ) -> Result<IndexWriterBridge, TantivySearchError> {
        let writer = index.writer_with_num_threads(BUILD_THREADS, MEMORY_64MB).map_err(|e| {
            let error_info = format!("Failed to create tantivy writer: {}", e);
            ERROR!(function: func_name, "{}", error_info);
            TantivySearchError::TantivyError(e)
        })?;

        let mut merge_policy = tantivy::merge_policy::LogMergePolicy::default();
        merge_policy.set_min_num_segments(5);
        writer.set_merge_policy(Box::new(merge_policy));

        Ok(IndexWriterBridge {
            index: index.clone(),
            path: index_path.trim_end_matches('/').to_string(),
            writer: Mutex::new(Some(writer)),
        })
    }


    pub(crate) fn get_index_writer_bridge(func_name: &str, index_path: &str) -> Result<Arc<IndexWriterBridge>, TantivySearchError> {
        FFI_INDEX_WRITER_CACHE
            .get_index_writer_bridge(index_path.to_string())
            .map_err(|e| {
                INFO!(function: func_name, "{}", e);
                TantivySearchError::InternalError(e)
            })
    }
    pub(crate) fn reload_index_reader(index_path: &str) -> Result<bool, TantivySearchError> {
        let reload_status = match FFI_INDEX_SEARCHER_CACHE
            .get_index_reader_bridge(index_path.to_string())
        {
            Ok(current_index_reader) => match current_index_reader.reload() {
                Ok(_) => true,
                Err(e) => {
                    return Err(TantivySearchError::InternalError(e));
                }
            },
            Err(_) => {
                true
            }
        };
        return Ok(reload_status);
    }
}
