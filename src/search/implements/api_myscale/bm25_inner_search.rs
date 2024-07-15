use std::collections::HashMap;
use tantivy::reader::multi_parts_statistics::MultiPartsStatistics;
use tantivy::schema::Field;
use tantivy::Term;
use crate::common::errors::TantivySearchError;
use crate::{common::constants::LOG_CALLBACK, DEBUG, ERROR};
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::ffi::{RowIdWithScore, Statistics};
use crate::search::implements::search_utils::SearchUtils;
use crate::search::implements::strategy::{QueryExecutor, QueryStrategy};

pub(super) fn bm25_inner_search(
    index_path: &str,
    statistics: &Statistics,
    strategy: &dyn QueryStrategy<Vec<RowIdWithScore>>,
) -> Result<Vec<RowIdWithScore>, TantivySearchError> {
    static FUNC_NAME: &str = "bm25_inner_search";

    let bridge = SearchUtils::get_index_reader_bridge(FUNC_NAME, index_path)?;

    // Choose query strategy to construct query executor.
    let query_executor: QueryExecutor<'_, Vec<RowIdWithScore>> = QueryExecutor::new(strategy);

    let searcher = &mut bridge.reader.searcher();

    // Not use statistics info.
    if statistics.docs_freq.len() == 0 {
        let result: Vec<RowIdWithScore> = query_executor.execute(searcher).map_err(|e| {
            ERROR!(function: FUNC_NAME, "{}", e);
            TantivySearchError::IndexSearcherError(e)
        })?;

        return Ok(result);
    }

    let mut doc_freq_map: HashMap<Term, u64> = HashMap::new();
    for item in &statistics.docs_freq {
        let col_field = Field::from_field_id(item.field_id);
        let term: Term = Term::from_field_text(col_field, &item.term_str);
        doc_freq_map
            .entry(term)
            .and_modify(|count| *count += item.doc_freq)
            .or_insert(item.doc_freq);
    }

    let mut total_num_tokens_map: HashMap<Field, u64> = HashMap::new();
    for item in &statistics.total_num_tokens {
        total_num_tokens_map
            .entry(Field::from_field_id(item.field_id))
            .and_modify(|count| *count += item.field_total_tokens)
            .or_insert(item.field_total_tokens);
    }

    let multi_parts_statistics = MultiPartsStatistics {
        doc_freq_map,
        total_num_tokens: total_num_tokens_map,
        total_num_docs: statistics.total_num_docs,
    };

    DEBUG!(function: FUNC_NAME, "index_path:[{:?}], use MultiPartsStatistics[{:?}]", index_path, multi_parts_statistics);

    let _ = searcher.update_multi_parts_statistics(multi_parts_statistics);

    let result: Vec<RowIdWithScore> = query_executor.execute(searcher).map_err(|e| {
        ERROR!(function: FUNC_NAME, "{}", e);
        TantivySearchError::IndexSearcherError(e)
    })?;

    Ok(result)
}