pub mod strategy;
mod api_clickhouse;
mod api_common;
mod api_myscale;

mod search_utils;

use search_utils::SearchUtils;

pub use api_clickhouse::{
    query_term_bitmap,
    query_terms_bitmap,
    query_sentence_bitmap,
    regex_term_bitmap
};

pub use api_common::{
    load_index_reader,
    free_index_reader,
    get_indexed_doc_counts
};

pub use api_myscale::{
    bm25_natural_language_search,
    bm25_standard_search,
    get_doc_freq,
    get_total_num_docs,
    get_total_num_tokens
};