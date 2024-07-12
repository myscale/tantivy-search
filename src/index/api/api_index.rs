use crate::index::implements::*;
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::{common::constants::LOG_CALLBACK, ERROR};
use crate::{cxx_vector_converter, CXX_STRING_CONVERTER, CXX_VECTOR_STRING_CONVERTER};
use cxx::{CxxString, CxxVector};

pub fn ffi_create_index_with_parameter(
    index_path: &CxxString,
    column_names: &CxxVector<CxxString>,
    index_json_parameter: &CxxString,
) -> bool {
    static FUNC_NAME: &str = "ffi_create_index_with_parameter";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: FUNC_NAME, "Can't convert 'index_path', message: {}", e);
            return false;
        }
    };

    let column_names: Vec<String> = match CXX_VECTOR_STRING_CONVERTER.convert(column_names) {
        Ok(names) => names,
        Err(e) => {
            ERROR!(function: FUNC_NAME, "Can't convert 'column_names', message: {}", e);
            return false;
        }
    };

    let index_json_parameter: String = match CXX_STRING_CONVERTER.convert(index_json_parameter) {
        Ok(json) => json,
        Err(e) => {
            ERROR!(function: FUNC_NAME, "Can't convert 'index_json_parameter', message: {}", e);
            return false;
        }
    };

    create_index_with_parameter(&index_path, &column_names, &index_json_parameter)
        .unwrap_or_else(|e| {
            ERROR!(function: FUNC_NAME, "Error creating index: {}", e);
            false
        })
}

pub fn ffi_create_index(index_path: &CxxString, column_names: &CxxVector<CxxString>) -> bool {
    static FUNC_NAME: &str = "ffi_create_index";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: FUNC_NAME, "Can't convert 'index_path', message: {}", e);
            return false;
        }
    };

    let column_names: Vec<String> = match CXX_VECTOR_STRING_CONVERTER.convert(column_names) {
        Ok(names) => names,
        Err(e) => {
            ERROR!(function: FUNC_NAME, "Can't convert 'column_names', message: {}", e);
            return false;
        }
    };

    create_index(&index_path, &column_names).unwrap_or_else(|e| {
        ERROR!(function: FUNC_NAME, "Error creating index: {}", e);
        false
    })
}

pub fn ffi_index_multi_column_docs(
    index_path: &CxxString,
    row_id: u64,
    column_names: &CxxVector<CxxString>,
    column_docs: &CxxVector<CxxString>,
) -> bool {
    static FUNC_NAME: &str = "ffi_index_multi_column_docs";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: FUNC_NAME, "Can't convert 'index_path', message: {}", e);
            return false;
        }
    };

    let column_names: Vec<String> = match CXX_VECTOR_STRING_CONVERTER.convert(column_names) {
        Ok(names) => names,
        Err(e) => {
            ERROR!(function: FUNC_NAME, "Can't convert 'column_names', message: {}", e);
            return false;
        }
    };

    let column_docs: Vec<String> = match CXX_VECTOR_STRING_CONVERTER.convert(column_docs) {
        Ok(docs) => docs,
        Err(e) => {
            ERROR!(function: FUNC_NAME, "Can't convert 'column_docs', message: {}", e);
            return false;
        }
    };

    if column_names.len() != column_docs.len() {
        ERROR!(function: FUNC_NAME, "column_names size doesn't match column_docs size");
        return false;
    }

    if column_names.len() == 0 || column_docs.len() == 0 {
        ERROR!(function: FUNC_NAME, "column_names and column_docs can't be empty");
        return false;
    }

    index_multi_column_docs(&index_path, row_id, &column_names, &column_docs)
        .unwrap_or_else(|e| {
            ERROR!(function: FUNC_NAME, "Error indexing multi-column docs: {}", e);
            false
        })
}

pub fn ffi_delete_row_ids(index_path: &CxxString, row_ids: &CxxVector<u32>) -> bool {
    static FUNC_NAME: &str = "ffi_delete_row_ids";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: FUNC_NAME, "Can't convert 'index_path', message: {}", e);
            return false;
        }
    };

    let row_ids: Vec<u32> = match cxx_vector_converter::<u32>().convert(row_ids) {
        Ok(ids) => ids,
        Err(e) => {
            ERROR!(function: FUNC_NAME, "Can't convert 'row_ids', message: {}", e);
            return false;
        }
    };

    delete_row_ids(&index_path, &row_ids).unwrap_or_else(|e| {
        ERROR!(function: FUNC_NAME, "Error deleting row ids: {}", e);
        false
    })
}

pub fn ffi_index_writer_commit(index_path: &CxxString) -> bool {
    static FUNC_NAME: &str = "ffi_index_writer_commit";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: FUNC_NAME, "Can't convert 'index_path', message: {}", e);
            return false;
        }
    };

    commit_index(&index_path).unwrap_or_else(|e| {
        ERROR!(function: FUNC_NAME, "Error committing index: {}", e);
        false
    })
}

pub fn ffi_free_index_writer(index_path: &CxxString) -> bool {
    static FUNC_NAME: &str = "ffi_free_index_writer";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            ERROR!(function: FUNC_NAME, "Can't convert 'index_path', message: {}", e);
            return false;
        }
    };

    free_index_writer(&index_path).unwrap_or_else(|e| {
        ERROR!(function: FUNC_NAME, "Error freeing index writer: {}", e);
        false
    })
}
