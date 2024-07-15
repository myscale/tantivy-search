use std::collections::HashMap;
use std::error::Error;
use std::fmt::Debug;
use std::fs;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use serde::{Deserialize, Serialize};
use tantivy::Index;
use crate::common::errors::{IndexUtilsError};
use crate::tokenizer::ingredient::Config;
use crate::logger::logger_bridge::TantivySearchLogger;
use crate::{common::constants::LOG_CALLBACK, DEBUG, WARNING};
use crate::common::constants::INDEX_INFO_FILE_NAME;
use crate::tokenizer::parser::{TokenizerConfig, TokenizerUtils};

#[derive(Serialize, Deserialize, Debug)]
pub struct IndexParameterInDisk {
    #[serde(default = "empty_index_json_parameter")]
    // Use `String` rather than `&'a str` will avoid lifetime issue while deserialize json string.
    pub tokenizers_json_parameter: String,
}

fn empty_index_json_parameter() -> String {
    "{}".to_string()
}

impl Default for IndexParameterInDisk {
    fn default() -> Self {
        Self {
            tokenizers_json_parameter: "{}".to_string(),
        }
    }
}


pub struct IndexUtils;

type ConfigResult = Result<Config, IndexUtilsError>;

type IndexJsonParameterResult = Result<IndexParameterInDisk, IndexUtilsError>;

type StringResult = Result<String, IndexUtilsError>;

type EmptyResult = Result<(), IndexUtilsError>;

impl IndexUtils {
    fn format_path_error<E: Error>(path: &Path, error: E) -> String {
        format!("path: {:?}, message: {}", path, error.to_string())
    }

    fn format_error<E, T>(obj: T, error: E) -> String
    where
        E: Error,
        T: Debug
    {
        format!("obj: {:?}, message: {}", obj, error.to_string())
    }

    fn read_file(path: &Path) -> StringResult {
        let mut file = File::open(path).map_err(|e| {
            IndexUtilsError::ReadFileError(Self::format_path_error(path, e))
        })?;
        let mut content = String::new();
        file.read_to_string(&mut content).map_err(|e| {
            IndexUtilsError::ReadFileError(Self::format_path_error(path, e))
        })?;
        Ok(content)
    }

    fn parse_index_parameter(parameter: String) -> IndexJsonParameterResult {
        serde_json::from_str(parameter.as_str()).map_err(|e| {
            IndexUtilsError::JsonDeserializeError(Self::format_error(parameter, e))
        })
    }

    fn deserialize_config(parameter: IndexParameterInDisk) -> ConfigResult {
        serde_json::from_str(parameter.tokenizers_json_parameter.as_str()).map_err(|e| {
            IndexUtilsError::JsonDeserializeError(Self::format_error(parameter, e))
        })
    }

    fn initialize_index_directory(path: &Path, enable_recreate_directory: bool) -> EmptyResult {
        // initialize index json parameter directory
        if path.exists() {
            if enable_recreate_directory {
                WARNING!("Index files directory not empty, will recreate it: {:?}", path);
                fs::remove_dir_all(path).map_err(|e| {
                    IndexUtilsError::RemoveDirectoryError(Self::format_path_error(path, e))
                })?;
                fs::create_dir_all(path).map_err(|e| {
                    IndexUtilsError::CreateDirectoryError(Self::format_path_error(path, e))
                })?;
            } else {
                return Err(IndexUtilsError::RemoveDirectoryError(
                    "Not allowed recreate".to_string())
                );
            }
        } else {
            // index directory not exist, create it.
            fs::create_dir_all(path).map_err(|e| {
                IndexUtilsError::CreateDirectoryError(Self::format_path_error(path, e))
            })?;
        }
        return Ok(());
    }

    pub fn load_tokenizer_config(index_files_directory: &Path) -> ConfigResult {
        let path = index_files_directory.join(INDEX_INFO_FILE_NAME);
        let config = if path.exists() {
            Self::read_file(&path).and_then(Self::parse_index_parameter)
        } else {
            let default_parameter = IndexParameterInDisk::default();
            Self::parse_index_parameter(default_parameter.tokenizers_json_parameter)
        };
        config.and_then(Self::deserialize_config)
    }

    // Save custom index json parameter (json content) to the index directory.
    pub fn save_index_parameter_to_disk(
        path: &Path,
        tokenizer_json_parameter: String,
        // FixMe: more custom settings.
        enable_recreate_directory: bool,
    ) -> EmptyResult {
        Self::initialize_index_directory(path, enable_recreate_directory)?;

        let parameter = IndexParameterInDisk {
            tokenizers_json_parameter: tokenizer_json_parameter.to_string(),
        };
        let file_path = path.join(INDEX_INFO_FILE_NAME);
        DEBUG!(function:"save_index_parameter_to_disk", "index parameter: {:?}", parameter);
        let setting_json = serde_json::to_string(&parameter).map_err(|e| {
            IndexUtilsError::JsonSerializeError(Self::format_path_error(file_path.as_path(), e))
        })?;

        File::create(&file_path)
            .and_then(|mut file: File| file.write_all(setting_json.as_bytes()))
            .map_err(|e| {
                IndexUtilsError::WriteFileError(Self::format_path_error(file_path.as_path(), e))
            })
    }

    pub fn register_tokenizers_from_config_map(
        index: &Index,
        config_map: &HashMap<String, TokenizerConfig>
    ) {
        let combine = |left:&str, right:&str| {format!("{}_{}", left, right)};
        for (col_name, config) in config_map.iter() {
            let tokenizer_name = combine(col_name, config.tokenizer_name.as_str());
            index.tokenizers().register(tokenizer_name.as_str(), config.text_analyzer.clone());
        }
    }

    pub fn register_tokenizers_from_disk(
        path: &Path,
        index: &Index
    ) -> Result<(), IndexUtilsError> {
        let raw_config: Config = Self::load_tokenizer_config(path)?;

        let config_map: HashMap<String, TokenizerConfig> =
            TokenizerUtils::parser_from_tokenizer_config(raw_config)?;
        Self::register_tokenizers_from_config_map(index, &config_map);

        Ok(())
    }


}


#[cfg(test)]
mod test {
    use crate::utils::index_utils::IndexParameterInDisk;

    #[test]
    fn test_default() {
        let parameter = IndexParameterInDisk::default();
        assert_eq!(parameter.tokenizers_json_parameter, "{}");
    }

    #[test]
    fn serialize_json_parameter() {
        let parameter: IndexParameterInDisk = IndexParameterInDisk {
            tokenizers_json_parameter: "{\"text\":{\"tokenizer\":{\"type\":\"default\"}}}"
                .to_string(),
        };
        let json = serde_json::to_string(&parameter).unwrap();
        assert_eq!(
            json,
            r#"{"tokenizers_json_parameter":"{\"text\":{\"tokenizer\":{\"type\":\"default\"}}}"}"#
        );
    }

    #[test]
    fn deserialize_json_parameter() {
        let json =
            r#"{"tokenizers_json_parameter":"{\"text\":{\"tokenizer\":{\"type\":\"default\"}}}"}"#;
        let parameter: IndexParameterInDisk = serde_json::from_str(&json).unwrap();
        assert_eq!(
            parameter.tokenizers_json_parameter,
            "{\"text\":{\"tokenizer\":{\"type\":\"default\"}}}"
        );
    }
}
