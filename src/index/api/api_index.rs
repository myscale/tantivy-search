use crate::index::implements::*;
use crate::{cxx_vector_converter, CXX_STRING_CONVERTER, CXX_VECTOR_STRING_CONVERTER};
use cxx::{CxxString, CxxVector};
use crate::ffi::{FFIBoolResult, FFIError};
use crate::utils::api_utils::ApiUtils;

pub fn ffi_create_index_with_parameter(
    index_path: &CxxString,
    column_names: &CxxVector<CxxString>,
    index_json_parameter: &CxxString,
) -> FFIBoolResult {
    static FUNC_NAME: &str = "ffi_create_index_with_parameter";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't convert 'index_path'", e.to_string());
        }
    };

    let column_names: Vec<String> = match CXX_VECTOR_STRING_CONVERTER.convert(column_names) {
        Ok(names) => names,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't convert 'column_names'", e.to_string());

        }
    };

    let index_json_parameter: String = match CXX_STRING_CONVERTER.convert(index_json_parameter) {
        Ok(json) => json,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't convert 'index_json_parameter'", e.to_string());
        }
    };

    match create_index_with_parameter(&index_path, &column_names, &index_json_parameter){
        Ok(result) => FFIBoolResult {
            result,
            error: FFIError {
                is_error: false,
                message: String::new(),
            },
        },
        Err(e) => {
            ApiUtils::handle_error(FUNC_NAME, "Error creating index", e.to_string())
        }
    }

}

pub fn ffi_create_index(index_path: &CxxString, column_names: &CxxVector<CxxString>) -> FFIBoolResult {
    static FUNC_NAME: &str = "ffi_create_index";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't convert 'index_path'", e.to_string());
        }
    };

    let column_names: Vec<String> = match CXX_VECTOR_STRING_CONVERTER.convert(column_names) {
        Ok(names) => names,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't convert 'column_names'", e.to_string());
        }
    };

    match create_index(&index_path, &column_names) {
        Ok(result) => FFIBoolResult {
            result,
            error: FFIError {
                is_error: false,
                message: String::new(),
            },
        },
        Err(e) => {
            ApiUtils::handle_error(FUNC_NAME, "Error creating index", e.to_string())
        }
    }
}

pub fn ffi_index_multi_column_docs(
    index_path: &CxxString,
    row_id: u64,
    column_names: &CxxVector<CxxString>,
    column_docs: &CxxVector<CxxString>,
) -> FFIBoolResult {
    static FUNC_NAME: &str = "ffi_index_multi_column_docs";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't convert 'index_path'", e.to_string());

        }
    };

    let column_names: Vec<String> = match CXX_VECTOR_STRING_CONVERTER.convert(column_names) {
        Ok(names) => names,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't convert 'column_names'", e.to_string());
        }
    };

    let column_docs: Vec<String> = match CXX_VECTOR_STRING_CONVERTER.convert(column_docs) {
        Ok(docs) => docs,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't convert 'column_docs'", e.to_string());
        }
    };

    if column_names.len() != column_docs.len() {
        return ApiUtils::handle_error(FUNC_NAME, "column_names size doesn't match column_docs size", "".to_string());
    }

    if column_names.len() == 0 || column_docs.len() == 0 {
        return ApiUtils::handle_error(FUNC_NAME, "column_names and column_docs can't be empty", "".to_string());
    }

    match index_multi_column_docs(&index_path, row_id, &column_names, &column_docs) {
        Ok(result) => FFIBoolResult {
            result,
            error: FFIError {
                is_error: false,
                message: String::new(),
            },
        },
        Err(e) => {
            ApiUtils::handle_error(FUNC_NAME, "Error indexing multi-column docs", e.to_string())
        }
    }
}

pub fn ffi_delete_row_ids(index_path: &CxxString, row_ids: &CxxVector<u32>) -> FFIBoolResult {
    static FUNC_NAME: &str = "ffi_delete_row_ids";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't convert 'index_path'", e.to_string());
        }
    };

    let row_ids: Vec<u32> = match cxx_vector_converter::<u32>().convert(row_ids) {
        Ok(ids) => ids,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't convert 'row_ids'", e.to_string());
        }
    };

    match delete_row_ids(&index_path, &row_ids) {
        Ok(result) => FFIBoolResult {
            result,
            error: FFIError {
                is_error: false,
                message: String::new(),
            },
        },
        Err(e) => {
            ApiUtils::handle_error(FUNC_NAME, "Error deleting row ids", e.to_string())
        }
    }
}

pub fn ffi_index_writer_commit(index_path: &CxxString) -> FFIBoolResult {
    static FUNC_NAME: &str = "ffi_index_writer_commit";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't convert 'index_path'", e.to_string());
        }
    };

    match commit_index(&index_path) {
        Ok(result) => FFIBoolResult {
            result,
            error: FFIError {
                is_error: false,
                message: String::new(),
            },
        },
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Error commit index", e.to_string());
        }
    }
}

pub fn ffi_free_index_writer(index_path: &CxxString) -> FFIBoolResult {
    static FUNC_NAME: &str = "ffi_free_index_writer";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't convert 'index_path'", e.to_string());
        }
    };

    match free_index_writer(&index_path) {
        Ok(result) => FFIBoolResult {
            result,
            error: FFIError {
                is_error: false,
                message: String::new(),
            },
        },
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Error freeing index writer", e.to_string());
        }
    }
}
