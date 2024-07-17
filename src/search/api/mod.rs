mod api_clickhouse;
mod api_common;
mod api_myscale;

pub use api_myscale::{
    ffi_bm25_search,
    ffi_get_total_num_tokens,
    ffi_get_doc_freq,
    ffi_get_total_num_docs
};

pub use api_common::{
    ffi_free_index_reader,
    ffi_get_indexed_doc_counts,
    ffi_load_index_reader
};

pub use api_clickhouse::{
    ffi_query_sentence_bitmap,
    ffi_query_term_bitmap,
    ffi_query_terms_bitmap,
    ffi_regex_term_bitmap
};