use std::sync::Arc;
use roaring::RoaringBitmap;
use tantivy::Searcher;
use crate::common::constants::FFI_INDEX_SEARCHER_CACHE;
use crate::search::bridge::index_reader_bridge::IndexReaderBridge;
use crate::{common::constants::LOG_CALLBACK, ERROR};
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::common::errors::TantivySearchError;
use crate::search::implements::strategy::QueryExecutor;

pub struct SearchUtils;

impl SearchUtils {
    pub(super) fn get_index_reader_bridge(
        func_name: &str, index_path: &str
    ) -> Result<Arc<IndexReaderBridge>, TantivySearchError> {
        let reader_bridge: Arc<IndexReaderBridge> = FFI_INDEX_SEARCHER_CACHE
            .get_index_reader_bridge(index_path.to_string())
            .map_err(|e| {
                ERROR!(function: func_name, "{}", e);
                TantivySearchError::InternalError(e)
            })?;
        return Ok(reader_bridge);
    }

    pub(super) fn execute_query(
        func_name: &str, query_executor: QueryExecutor<'_, Arc<RoaringBitmap>>, searcher: &Searcher
    ) -> Result<Arc<RoaringBitmap>, TantivySearchError> {
        let result: Arc<RoaringBitmap> = query_executor
            .execute(searcher)
            .map_err(|e| {
                ERROR!(function: func_name, "{}", e);
                TantivySearchError::IndexSearcherError(e)
            })?;
        Ok(result)
    }

}