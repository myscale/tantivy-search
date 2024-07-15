use std::{num::TryFromIntError, str::Utf8Error};
use tantivy::TantivyError;
use thiserror::Error;

#[derive(Debug, Clone, Error)]
#[allow(dead_code)]
pub enum IndexUtilsError {
    #[error("Failed to convert cxx vector variable. '{0}'")]
    JsonParseError(String),
    #[error("Failed to convert cxx vector variable. '{0}'")]
    JsonSerializeError(String),
    #[error("Failed to convert cxx vector variable. '{0}'")]
    JsonDeserializeError(String),

    #[error("Failed to handle directory. '{0}'")]
    DirectoryIOError(String),
    #[error("Failed to remove directory. '{0}'")]
    RemoveDirectoryError(String),
    #[error("Failed to create directory. '{0}'")]
    CreateDirectoryError(String),

    #[error("Failed to read file. '{0}'")]
    ReadFileError(String),
    #[error("Failed to write file. '{0}'")]
    WriteFileError(String),

    #[error("Failed to config tokenizer. '{0}'")]
    ConfigTokenizerError(String),

    #[error(transparent)]
    TokenizerError(#[from] TokenizerError),
}

#[derive(Debug, Clone, Error)]
#[allow(dead_code)]
pub enum TokenizerError {
    #[error("Failed to deserialize index parameter. '{0}'")]
    JsonDeserializeError(String),
    #[error("Failed to serialize index parameter. '{0}'")]
    JsonSerializeError(String),

    #[error("Failed to remove directory. '{0}'")]
    RemoveDirectoryError(String),
    #[error("Error happened when create directory. '{0}'")]
    CreateDirectoryError(String),

    #[error("Error happened when write file. '{0}'")]
    WriteFileError(String),
    #[error("Failed to read index parameter from file. '{0}'")]
    ReadFileError(String),

    #[error("Failed to build TextAnalyzer. '{0}'")]
    BuildTokenizerError(String),

    #[error("Failed to build TextAnalyzer, TantivyError happened: '{0}'")]
    TantivyError(#[from] TantivyError),
}

#[derive(Debug, Clone, Error)]
#[allow(dead_code)]
pub enum IndexSearcherError {
    #[error("Missing field/column in tantivy schema. '{0}'")]
    MissingFieldError(String),
    #[error("At least one field should be choose.")]
    EmptyFieldsError,

    #[error("Unexpect behavior. '{0}'")]
    InternalError(String),

    #[error(transparent)]
    TantivyError(#[from] TantivyError),

    #[error("Failed to convert integer: {0}")]
    TryFromIntError(#[from] TryFromIntError),

    #[error("Bitmap may be overflow. '{0}'")]
    BitmapOverflowError(String),

    #[error("Query parse error. '{0}'")]
    QueryParserError(String),
}

#[derive(Debug, Clone, Error)]
#[allow(dead_code)]
pub enum CxxConvertError {
    #[error("Failed to convert cxx vector variable. '{0}'")]
    CxxVectorConvertError(String),
    #[error("Failed to convert cxx element variable. '{0}'")]
    CxxElementConvertError(String),
    #[error("Failed to convert CxxString to Rust String: {0}")]
    Utf8Error(#[from] Utf8Error),
}

/// The library's error enum
#[derive(Debug, Clone, Error)]
#[allow(dead_code)]
pub enum TantivySearchError {
    #[error(transparent)]
    CxxConvertError(#[from] CxxConvertError),

    #[error(transparent)]
    IndexUtilsError(#[from] IndexUtilsError),

    #[error(transparent)]
    TokenizerError(#[from] TokenizerError),

    #[error(transparent)]
    IndexSearcherError(#[from] IndexSearcherError),

    #[error(transparent)]
    TantivyError(#[from] TantivyError),

    #[error("Index not exists: '{0}'")]
    IndexNotExists(String),

    /// An internal error occurred. This is are internal states that should not be reached.
    /// e.g. a data structure is incorrectly initialized.
    #[error("Internal error: '{0}'")]
    InternalError(String),

    #[error("An invalid argument was passed: '{0}'")]
    InvalidArgument(String),
}
