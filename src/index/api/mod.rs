mod api_index;

pub use api_index::{
    ffi_create_index_with_parameter, ffi_create_index, ffi_index_multi_column_docs,
    ffi_delete_row_ids, ffi_index_writer_commit, ffi_free_index_writer
};