mod api_load_index_reader;
mod api_free_index_reader;
mod api_get_indexed_doc_counts;

pub use api_load_index_reader::load_index_reader;
pub use api_free_index_reader::free_index_reader;
pub use api_get_indexed_doc_counts::get_indexed_doc_counts;