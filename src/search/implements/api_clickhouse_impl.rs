use std::path::Path;
use crate::common::errors::{TantivySearchError, TokenizerUtilsError};
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::search::bridge::index_reader_bridge::IndexReaderBridge;
use crate::search::implements::strategy::query_strategy::QueryExecutor;
use crate::search::utils::convert_utils::ConvertUtils;
use crate::search::utils::index_searcher_utils::FFiIndexSearcherUtils;
use crate::FFI_INDEX_SEARCHER_CACHE;
use crate::{common::constants::LOG_CALLBACK, ERROR};
use roaring::RoaringBitmap;
use std::sync::Arc;
use crate::search::implements::strategy::regex_query::RegexQueryStrategy;
use crate::search::implements::strategy::sentence_query::SentenceQueryStrategy;
use crate::search::implements::strategy::single_term_query::SingleTermQueryStrategy;
use crate::search::implements::strategy::term_set_query::TermSetQueryStrategy;
use crate::tokenizer::dto::index_parameter_dto::IndexParameterDTO;
use crate::tokenizer::vo::tokenizer_json_vo::{ColumnTokenizer, Config};
use crate::utils::index_utils::IndexUtils;


/// Execute Term Query in specific rowid range.
pub fn query_term_with_range(
    index_path: &str,
    column_name: &str,
    term: &str,
    lrange: u64,
    rrange: u64,
) -> Result<bool, TantivySearchError> {
    // Get index_reader_bridge from CACHE
    let index_reader_bridge: Arc<IndexReaderBridge> = FFI_INDEX_SEARCHER_CACHE
        .get_index_reader_bridge(index_path.to_string())
        .map_err(|e| {
            ERROR!(function:"query_term_with_range", "{}", e);
            TantivySearchError::InternalError(e)
        })?;

    // Choose query strategy to construct query executor.
    let term_query: SingleTermQueryStrategy<'_> = SingleTermQueryStrategy { column_name, term };
    let query_executor: QueryExecutor<'_, Arc<RoaringBitmap>> = QueryExecutor::new(&term_query);

    // Compute query results.
    let result: Arc<RoaringBitmap> = query_executor
        .execute(&index_reader_bridge.reader.searcher())
        .map_err(|e| {
            ERROR!(function:"query_term_with_range", "{}", e);
            TantivySearchError::IndexSearcherError(e)
        })?;

    // Intersect query results with range.
    let intersected =
        FFiIndexSearcherUtils::intersect_with_range(result, lrange, rrange).map_err(|e| {
            ERROR!(function:"query_term_with_range", "{}", e);
            TantivySearchError::IndexSearcherError(e)
        })?;
    Ok(!intersected.is_empty())
}

/// Execute Terms Query in specific rowid range.
pub fn query_terms_with_range(
    index_path: &str,
    column_name: &str,
    terms: &Vec<String>,
    lrange: u64,
    rrange: u64,
) -> Result<bool, TantivySearchError> {
    // Get index_reader_bridge from CACHE
    let index_reader_bridge: Arc<IndexReaderBridge> = FFI_INDEX_SEARCHER_CACHE
        .get_index_reader_bridge(index_path.to_string())
        .map_err(|e| {
            ERROR!(function:"query_terms_with_range", "{}", e);
            TantivySearchError::InternalError(e)
        })?;

    // Choose query strategy to construct query executor.
    let terms_query: TermSetQueryStrategy<'_> = TermSetQueryStrategy { column_name, terms };
    let query_executor: QueryExecutor<'_, Arc<RoaringBitmap>> = QueryExecutor::new(&terms_query);

    // Compute query results.
    let result: Arc<RoaringBitmap> = query_executor
        .execute(&index_reader_bridge.reader.searcher())
        .map_err(|e| {
            ERROR!(function:"query_terms_with_range", "{}", e);
            TantivySearchError::IndexSearcherError(e)
        })?;

    // Intersect query results with range.
    let intersected =
        FFiIndexSearcherUtils::intersect_with_range(result, lrange, rrange).map_err(|e| {
            ERROR!(function:"query_terms_with_range", "{}", e);
            TantivySearchError::IndexSearcherError(e)
        })?;
    Ok(!intersected.is_empty())
}

/// Execute Sentence Query in specific rowid range.
pub fn query_sentence_with_range(
    index_path: &str,
    column_name: &str,
    sentence: &str,
    lrange: u64,
    rrange: u64,
) -> Result<bool, TantivySearchError> {
    // Get index_reader_bridge from CACHE
    let index_reader_bridge: Arc<IndexReaderBridge> = FFI_INDEX_SEARCHER_CACHE
        .get_index_reader_bridge(index_path.to_string())
        .map_err(|e| {
            ERROR!(function:"query_sentence_with_range", "{}", e);
            TantivySearchError::InternalError(e)
        })?;

    // Choose query strategy to construct query executor.
    let sentence_query: SentenceQueryStrategy<'_> = SentenceQueryStrategy {
        column_name,
        sentence,
    };
    let query_executor: QueryExecutor<'_, Arc<RoaringBitmap>> = QueryExecutor::new(&sentence_query);

    // Compute query results.
    let result: Arc<RoaringBitmap> = query_executor
        .execute(&index_reader_bridge.reader.searcher())
        .map_err(|e| {
            ERROR!(function:"query_sentence_with_range", "{}", e);
            TantivySearchError::IndexSearcherError(e)
        })?;

    // Intersect query results with range.
    let intersected =
        FFiIndexSearcherUtils::intersect_with_range(result, lrange, rrange).map_err(|e| {
            ERROR!(function:"query_sentence_with_range", "{}", e);
            TantivySearchError::IndexSearcherError(e)
        })?;
    Ok(!intersected.is_empty())
}

/// Execute Regex Query in specific rowid range.
pub fn regex_term_with_range(
    index_path: &str,
    column_name: &str,
    pattern: &str,
    lrange: u64,
    rrange: u64,
) -> Result<bool, TantivySearchError> {
    // Get index_reader_bridge from CACHE
    let index_reader_bridge: Arc<IndexReaderBridge> = FFI_INDEX_SEARCHER_CACHE
        .get_index_reader_bridge(index_path.to_string())
        .map_err(|e| {
            ERROR!(function:"regex_term_with_range", "{}", e);
            TantivySearchError::InternalError(e)
        })?;

    // Choose query strategy to construct query executor.
    let sentence_query: RegexQueryStrategy<'_> = RegexQueryStrategy {
        column_name,
        pattern,
    };
    let query_executor: QueryExecutor<'_, Arc<RoaringBitmap>> = QueryExecutor::new(&sentence_query);

    // Compute query results.
    let result: Arc<RoaringBitmap> = query_executor
        .execute(&index_reader_bridge.reader.searcher())
        .map_err(|e| {
            ERROR!(function:"regex_term_with_range", "{}", e);
            TantivySearchError::IndexSearcherError(e)
        })?;

    // Intersect query results with range.
    let intersected =
        FFiIndexSearcherUtils::intersect_with_range(result, lrange, rrange).map_err(|e| {
            ERROR!(function:"regex_term_with_range", "{}", e);
            TantivySearchError::IndexSearcherError(e)
        })?;
    Ok(!intersected.is_empty())
}

/// Execute Term Query.
pub fn query_term_bitmap(
    index_path: &str,
    column_name: &str,
    term: &str,
) -> Result<Vec<u8>, TantivySearchError> {
    // Get index_reader_bridge from CACHE
    let index_reader_bridge: Arc<IndexReaderBridge> = FFI_INDEX_SEARCHER_CACHE
        .get_index_reader_bridge(index_path.to_string())
        .map_err(|e| {
            ERROR!(function:"query_term_bitmap", "{}", e);
            TantivySearchError::InternalError(e)
        })?;

    // Choose query strategy to construct query executor.
    let term_query: SingleTermQueryStrategy<'_> = SingleTermQueryStrategy { column_name, term };
    let query_executor: QueryExecutor<'_, Arc<RoaringBitmap>> = QueryExecutor::new(&term_query);

    // Compute query results.
    let result: Arc<RoaringBitmap> = query_executor
        .execute(&index_reader_bridge.reader.searcher())
        .map_err(|e| {
            ERROR!(function:"query_term_bitmap", "{}", e);
            TantivySearchError::IndexSearcherError(e)
        })?;

    // Convert results to u8 bitmap.
    let row_ids_number: Vec<u32> = result.iter().collect();
    let u8_bitmap: Vec<u8> = ConvertUtils::row_ids_to_u8_bitmap(&row_ids_number);

    Ok(u8_bitmap)
}

/// Execute Terms Query.
pub fn query_terms_bitmap(
    index_path: &str,
    column_name: &str,
    terms: &Vec<String>,
) -> Result<Vec<u8>, TantivySearchError> {
    // Get index_reader_bridge from CACHE
    let index_reader_bridge: Arc<IndexReaderBridge> = FFI_INDEX_SEARCHER_CACHE
        .get_index_reader_bridge(index_path.to_string())
        .map_err(|e| {
            ERROR!(function:"query_terms_bitmap", "{}", e);
            TantivySearchError::InternalError(e)
        })?;

    // Choose query strategy to construct query executor.
    let sentence_query: TermSetQueryStrategy<'_> = TermSetQueryStrategy { column_name, terms };
    let query_executor: QueryExecutor<'_, Arc<RoaringBitmap>> = QueryExecutor::new(&sentence_query);

    // Compute query results.
    let result: Arc<RoaringBitmap> = query_executor
        .execute(&index_reader_bridge.reader.searcher())
        .map_err(|e| {
            ERROR!(function:"query_terms_bitmap", "{}", e);
            TantivySearchError::IndexSearcherError(e)
        })?;

    // Convert results to u8 bitmap.
    let row_ids_number: Vec<u32> = result.iter().collect();
    let u8_bitmap: Vec<u8> = ConvertUtils::row_ids_to_u8_bitmap(&row_ids_number);

    Ok(u8_bitmap)
}

/// Execute Sentence Query.
pub fn query_sentence_bitmap(
    index_path: &str,
    column_name: &str,
    sentence: &str,
) -> Result<Vec<u8>, TantivySearchError> {
    // Get index_reader_bridge from CACHE
    let index_reader_bridge: Arc<IndexReaderBridge> = FFI_INDEX_SEARCHER_CACHE
        .get_index_reader_bridge(index_path.to_string())
        .map_err(|e| {
            ERROR!(function:"query_sentence_bitmap", "{}", e);
            TantivySearchError::InternalError(e)
        })?;

    // Choose query strategy to construct query executor.
    let sentence_query: SentenceQueryStrategy<'_> = SentenceQueryStrategy {
        column_name,
        sentence,
    };
    let query_executor: QueryExecutor<'_, Arc<RoaringBitmap>> = QueryExecutor::new(&sentence_query);

    // Compute query results.
    let result: Arc<RoaringBitmap> = query_executor
        .execute(&index_reader_bridge.reader.searcher())
        .map_err(|e| {
            ERROR!(function:"query_sentence_bitmap", "{}", e);
            TantivySearchError::IndexSearcherError(e)
        })?;

    // Convert results to u8 bitmap.
    let row_ids_number: Vec<u32> = result.iter().collect();
    let u8_bitmap: Vec<u8> = ConvertUtils::row_ids_to_u8_bitmap(&row_ids_number);

    Ok(u8_bitmap)
}

/// Execute Regex Query.
pub fn regex_term_bitmap(
    index_path: &str,
    column_name: &str,
    pattern: &str,
) -> Result<Vec<u8>, TantivySearchError> {
    // Get index_reader_bridge from CACHE
    let index_reader_bridge: Arc<IndexReaderBridge> = FFI_INDEX_SEARCHER_CACHE
        .get_index_reader_bridge(index_path.to_string())
        .map_err(|e| {
            ERROR!(function:"regex_term_bitmap", "{}", e);
            TantivySearchError::InternalError(e)
        })?;

    // TODO: refine code, refine tokenizer utils code.
    let index_parameter: IndexParameterDTO =
        IndexUtils::load_custom_index_setting(Path::new(index_path)).map_err(|e| {
            ERROR!(function:"regex_term_bitmap", "{}", e);
            TantivySearchError::IndexUtilsError(e)
        })?;
    let config: Config =
        serde_json::from_str(&index_parameter.tokenizers_json_parameter)
            .map_err(|e| TokenizerUtilsError::JsonDeserializeError(e.to_string()))?;
    let mut lower_case = true;
    let column = config.get_columns().get(column_name);
    if column.is_some() {
        match column.unwrap().get_tokenizer() {
            ColumnTokenizer::Default { .. } => {
                lower_case=true;
            }
            ColumnTokenizer::Raw { .. } => {
                lower_case=false;
            }
            ColumnTokenizer::Simple { case_sensitive, .. } => {
                lower_case=!case_sensitive;
            }
            ColumnTokenizer::Stem { case_sensitive, .. } => {
                lower_case=!case_sensitive;
            }
            ColumnTokenizer::Whitespace { case_sensitive, .. } => {
                lower_case=!case_sensitive;
            }
            ColumnTokenizer::Ngram { case_sensitive, .. } => {
                lower_case=!case_sensitive;
            }
            ColumnTokenizer::Chinese{ case_sensitive, .. } => {
                lower_case=!case_sensitive;
            }
        }
    }

    let mut pattern_handled = pattern.to_string();
    if lower_case {
        // Keep same with Lower Case Tokenizer.
        pattern_handled.make_ascii_lowercase();
    }


    // Choose query strategy to construct query executor.
    let sentence_query: RegexQueryStrategy<'_> = RegexQueryStrategy {
        column_name,
        pattern: &pattern_handled,
    };
    let query_executor: QueryExecutor<'_, Arc<RoaringBitmap>> = QueryExecutor::new(&sentence_query);

    // Compute query results.
    let result: Arc<RoaringBitmap> = query_executor
        .execute(&index_reader_bridge.reader.searcher())
        .map_err(|e| {
            ERROR!(function:"regex_term_bitmap", "{}", e);
            TantivySearchError::IndexSearcherError(e)
        })?;

    // Convert results to u8 bitmap.
    let row_ids_number: Vec<u32> = result.iter().collect();
    let u8_bitmap: Vec<u8> = ConvertUtils::row_ids_to_u8_bitmap(&row_ids_number);

    Ok(u8_bitmap)
}
