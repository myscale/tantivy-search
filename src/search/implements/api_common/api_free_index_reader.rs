use crate::common::constants::FFI_INDEX_SEARCHER_CACHE;

pub fn free_index_reader(index_path: &str) -> bool {
    if let Err(_) = FFI_INDEX_SEARCHER_CACHE.remove_index_reader_bridge(index_path.to_string()) {
        return false;
    }
    // key exists and success removed.
    true
}
