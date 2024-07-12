use std::str::FromStr;

use crate::tokenizer::languages::{SupportFilterLanguage, SupportLanguageAlgorithm};
use serde::{Deserialize, Deserializer, Serialize};

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
#[serde(tag = "type")]
pub enum Tokenizer {
    #[serde(rename = "default")]
    Default {
        #[serde(default)]
        store_doc: bool,
    },
    #[serde(rename = "raw")]
    Raw {
        #[serde(default)]
        store_doc: bool,
    },
    #[serde(rename = "simple")]
    Simple {
        #[serde(default, deserialize_with = "stop_word_filters_validator")]
        stop_word_filters: Vec<String>,
        #[serde(default)]
        store_doc: bool,
        #[serde(default = "default_length_limit")]
        length_limit: usize,
        #[serde(default)]
        case_sensitive: bool,
    },
    #[serde(rename = "stem")]
    Stem {
        #[serde(default, deserialize_with = "stop_word_filters_validator")]
        stop_word_filters: Vec<String>,
        #[serde(default, deserialize_with = "stem_languages_filters_validator")]
        stem_languages: Vec<String>,
        #[serde(default)]
        store_doc: bool,
        #[serde(default = "default_length_limit")]
        length_limit: usize,
        #[serde(default)]
        case_sensitive: bool,
    },
    #[serde(rename = "whitespace")]
    Whitespace {
        #[serde(default, deserialize_with = "stop_word_filters_validator")]
        stop_word_filters: Vec<String>,
        #[serde(default)]
        store_doc: bool,
        #[serde(default = "default_length_limit")]
        length_limit: usize,
        #[serde(default)]
        case_sensitive: bool,
    },
    #[serde(rename = "ngram")]
    Ngram {
        #[serde(default = "default_min_gram")]
        min_gram: usize,
        #[serde(default = "default_max_gram")]
        max_gram: usize,
        #[serde(default)]
        prefix_only: bool,
        #[serde(default, deserialize_with = "stop_word_filters_validator")]
        stop_word_filters: Vec<String>,
        #[serde(default)]
        store_doc: bool,
        #[serde(default = "default_length_limit")]
        length_limit: usize,
        #[serde(default)]
        case_sensitive: bool,
    },
    #[serde(rename = "chinese")]
    Chinese {
        #[serde(
            default = "chinese_jieba_default",
            deserialize_with = "chinese_jieba_validator"
        )]
        jieba: String,
        #[serde(
            default = "chinese_mode_default",
            deserialize_with = "chinese_mode_validator"
        )]
        mode: String,
        #[serde(default)]
        hmm: bool,
        #[serde(default)]
        store_doc: bool,
        #[serde(default)]
        case_sensitive: bool,
    },
    #[serde(rename = "icu")]
    Icu {
        #[serde(
            default = "icu_mode_default",
            deserialize_with = "icu_mode_validator"
        )]
        mode: String,
        #[serde(default, deserialize_with = "stop_word_filters_validator")]
        stop_word_filters: Vec<String>,
        #[serde(default, deserialize_with = "stem_languages_filters_validator")]
        stem_languages: Vec<String>,
        #[serde(default)]
        store_doc: bool,
        #[serde(default)]
        case_sensitive: bool,
    },
}

fn default_length_limit() -> usize {
    40
}

fn default_min_gram() -> usize {
    2
}

fn default_max_gram() -> usize {
    3
}

fn chinese_jieba_default() -> String {
    "default".to_string()
}

fn chinese_mode_default() -> String {
    "search".to_string()
}

fn chinese_jieba_validator<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let jieba = String::deserialize(deserializer)?;
    static VALID_JIEBA: &[&str] = &["default", "empty"];
    if !VALID_JIEBA.contains(&jieba.as_str()) {
        return Err(serde::de::Error::custom(format!(
            "Invalid value for jieba: {}. Expected {:?}.",
            jieba, VALID_JIEBA
        )));
    }
    Ok(jieba)
}

fn chinese_mode_validator<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let mode = String::deserialize(deserializer)?;
    static VALID_MODE: &[&str] = &["all", "default", "search", "unicode"];
    if !VALID_MODE.contains(&mode.as_str()) {
        return Err(serde::de::Error::custom(format!(
            "Invalid value for mode: {}. Expected {:?}.",
            mode, VALID_MODE
        )));
    }
    Ok(mode)
}

fn stop_word_filters_validator<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let stop_word_filters: Vec<String> = Vec::deserialize(deserializer)?;
    for item in &stop_word_filters {
        if SupportFilterLanguage::from_str(item).is_err() {
            return Err(serde::de::Error::custom(format!(
                "Invalid stop word filter: {}",
                item
            )));
        }
    }
    Ok(stop_word_filters)
}

fn stem_languages_filters_validator<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let stem_languages: Vec<String> = Vec::deserialize(deserializer)?;
    for item in &stem_languages {
        if SupportLanguageAlgorithm::from_str(item).is_err() {
            return Err(serde::de::Error::custom(format!(
                "Unsupported stem language: {}",
                item
            )));
        }
    }
    Ok(stem_languages)
}


fn icu_mode_default() -> String { "word".to_string() }

fn icu_mode_validator<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let mode = String::deserialize(deserializer)?;
    static VALID_MODE: &[&str] = &["grapheme", "line", "sentence", "word"];
    if !VALID_MODE.contains(&mode.as_str()) {
        return Err(serde::de::Error::custom(format!(
            "Invalid value for mode: {}. Expected {:?}.",
            mode, VALID_MODE
        )));
    }
    Ok(mode)
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_default() {
        // valid json str
        let json_str = r#"{"type": "default", "store_doc": true}"#;
        let tokenizer: Tokenizer = serde_json::from_str(json_str).unwrap();
        match tokenizer {
            Tokenizer::Default { store_doc } => {
                assert!(store_doc);
            }
            _ => panic!("Unexpected variant"),
        }

        // invalid type
        let invalid_json_1 = r#"{"type": "default", "store_doc": 12}"#;
        let invalid_tokenizer_1: Result<Tokenizer, _> = serde_json::from_str(invalid_json_1);
        assert!(invalid_tokenizer_1.is_err());
        // invalid key
        let invalid_json_2 = r#"{"type": "default", "store_doc": false, "case_sensitive": false}"#;
        let invalid_tokenizer_2: Result<Tokenizer, _> = serde_json::from_str(invalid_json_2);
        assert!(invalid_tokenizer_2.is_err());
        // broken json
        let invalid_json_3 = r#"{"type": "default", store_doc: false}"#;
        let invalid_tokenizer_3: Result<Tokenizer, _> = serde_json::from_str(invalid_json_3);
        assert!(invalid_tokenizer_3.is_err());
    }

    #[test]
    fn test_deserialize_raw() {
        // valid json str
        let json_str = r#"{"type": "raw", "store_doc": false}"#;
        let tokenizer: Tokenizer = serde_json::from_str(json_str).unwrap();
        match tokenizer {
            Tokenizer::Raw { store_doc } => {
                assert!(!store_doc);
            }
            _ => panic!("Unexpected variant"),
        }
        // invalid type
        let invalid_json_1 = r#"{"type": "raw", "store_doc": "no"}"#;
        let invalid_tokenizer_1: Result<Tokenizer, _> = serde_json::from_str(invalid_json_1);
        assert!(invalid_tokenizer_1.is_err());
        // invalid key
        let invalid_json_2 = r#"{"type": "raw", "store_doc": false, "case_sensitive": true}"#;
        let invalid_tokenizer_2: Result<Tokenizer, _> = serde_json::from_str(invalid_json_2);
        assert!(invalid_tokenizer_2.is_err());
        // broken json
        let invalid_json_3 = r#"{"type": "raw", store_doc: false}"#;
        let invalid_tokenizer_3: Result<Tokenizer, _> = serde_json::from_str(invalid_json_3);
        assert!(invalid_tokenizer_3.is_err());
    }

    #[test]
    #[rustfmt::skip]
    fn test_deserialize_simple() {
        let json_str = r#"
            {
                "type": "simple",
                "stop_word_filters": ["english", "german"],
                "store_doc": true,
                "length_limit": 50,
                "case_sensitive": false
            }
        "#;
        let tokenizer: Tokenizer = serde_json::from_str(json_str).unwrap();
        match tokenizer {
            Tokenizer::Simple {
                stop_word_filters,
                store_doc,
                length_limit,
                case_sensitive,
            } => {
                assert_eq!(stop_word_filters, vec!["english", "german"]);
                assert!(store_doc);
                assert_eq!(length_limit, 50);
                assert!(!case_sensitive);
            }
            _ => panic!("Unexpected variant"),
        }

        // invalid type
        let invalid_json_1 = r#"
            {
                "type": "simple",
                "stop_word_filters": ["english", "german"],
                "store_doc": true,
                "length_limit": 50,
                "case_sensitive": "false"
            }
        "#;
        let invalid_tokenizer_1: Result<Tokenizer, _> = serde_json::from_str(invalid_json_1);
        assert!(invalid_tokenizer_1.is_err());

        // invalid value
        let invalid_json_2 = r#"
            {
                "type": "simple",
                "stop_word_filters": ["chinese", "german"],
                "store_doc": true,
                "length_limit": 50,
                "case_sensitive": false
            }
        "#;
        let invalid_tokenizer_2: Result<Tokenizer, _> = serde_json::from_str(invalid_json_2);
        assert!(invalid_tokenizer_2.is_err());

        // invalid key
        let invalid_json_3 = r#"
            {
                "type": "simple",
                "stop_word_filters": ["english", "german"],
                "store_doc": true,
                "length_limit": 50,
                "case_sensitive": false,
                "mode": "all"
            }
        "#;
        let invalid_tokenizer_3: Result<Tokenizer, _> = serde_json::from_str(invalid_json_3);
        assert!(invalid_tokenizer_3.is_err());
    }

    #[test]
    #[rustfmt::skip]
    fn test_deserialize_stem() {
        let json_str = r#"
            {
                "type": "stem",
                "stop_word_filters": ["english", "german"],
                "stem_languages": ["english", "french"],
                "store_doc": true,
                "length_limit": 60,
                "case_sensitive": false
            }
        "#;
        let tokenizer: Tokenizer = serde_json::from_str(json_str).unwrap();
        match tokenizer {
            Tokenizer::Stem {
                stop_word_filters,
                stem_languages,
                store_doc,
                length_limit,
                case_sensitive,
            } => {
                assert_eq!(stop_word_filters, vec! ["english", "german"]);
                assert_eq!(stem_languages, vec!["english", "french"]);
                assert!(store_doc);
                assert_eq!(length_limit, 60);
                assert!(!case_sensitive);
            }
            _ => panic!("Unexpected variant"),
        }

        // invalid value type
        let invalid_json_1 = r#"
            {
                "type": "stem",
                "stop_word_filters": ["english", "german"],
                "stem_languages": ["english", "french"],
                "store_doc": true,
                "length_limit": 60,
                "case_sensitive": "false"
            }
        "#;
        let invalid_tokenizer_1: Result<Tokenizer, _> = serde_json::from_str(invalid_json_1);
        assert!(invalid_tokenizer_1.is_err());

        // invalid value
        let invalid_json_2 = r#"
            {
                "type": "stem",
                "stop_word_filters": ["english", "chinese"],
                "stem_languages": ["chinese", "french"],
                "store_doc": true,
                "length_limit": 60,
                "case_sensitive": false
            }
        "#;
        let invalid_tokenizer_2: Result<Tokenizer, _> = serde_json::from_str(invalid_json_2);
        assert!(invalid_tokenizer_2.is_err());

        // invalid key
        let invalid_json_3 = r#"
            {
                "type": "stem",
                "stop_word_filters": ["english", "german"],
                "stem_languages": ["english", "french"],
                "store_doc": true,
                length_limit: 50,
                "case_sensitive": false,
                "mode": "all"
            }
        "#;
        let invalid_tokenizer_3: Result<Tokenizer, _> = serde_json::from_str(invalid_json_3);
        assert!(invalid_tokenizer_3.is_err());
    }

    #[test]
    fn test_deserialize_whitespace() {
        let json_str = r#"
            {
                "type": "whitespace",
                "stop_word_filters": ["english", "german"],
                "store_doc": true,
                "length_limit": 70,
                "case_sensitive": false
            }
        "#;
        let tokenizer: Tokenizer = serde_json::from_str(json_str).unwrap();
        match tokenizer {
            Tokenizer::Whitespace {
                stop_word_filters,
                store_doc,
                length_limit,
                case_sensitive,
            } => {
                assert_eq!(stop_word_filters, vec!["english", "german"]);
                assert!(store_doc);
                assert_eq!(length_limit, 70);
                assert!(!case_sensitive);
            }
            _ => panic!("Unexpected variant"),
        }

        // invalid value type
        let invalid_json_1 = r#"
            {
                "type": "whitespace",
                "stop_word_filters": ["english", "german"],
                "store_doc": "true",
                "length_limit": 70,
                "case_sensitive": false
            }
        "#;
        let invalid_tokenizer_1: Result<Tokenizer, _> = serde_json::from_str(invalid_json_1);
        assert!(invalid_tokenizer_1.is_err());

        // invalid value
        let invalid_json_2 = r#"
            {
                "type": "whitespace",
                "stop_word_filters": ["english", "german"],
                "store_doc": true,
                "length_limit": -1,
                "case_sensitive": false
            }
        "#;
        let invalid_tokenizer_2: Result<Tokenizer, _> = serde_json::from_str(invalid_json_2);
        assert!(invalid_tokenizer_2.is_err());

        // invalid key
        let invalid_json_3 = r#"
            {
                "type": "whitespace",
                "stop_word_filters": ["english", "german"],
                "store_doc: true,
                "length_limit": 20,
                "case_sensitive": false
            }
        "#;
        let invalid_tokenizer_3: Result<Tokenizer, _> = serde_json::from_str(invalid_json_3);
        assert!(invalid_tokenizer_3.is_err());
    }

    #[test]
    fn test_deserialize_ngram() {
        let json_str = r#"
            {
                "type": "ngram",
                "min_gram": 2,
                "max_gram": 4,
                "prefix_only": true,
                "stop_word_filters": ["english", "german"],
                "store_doc": true,
                "length_limit": 80,
                "case_sensitive": true
            }
        "#;
        let tokenizer: Tokenizer = serde_json::from_str(json_str).unwrap();
        match tokenizer {
            Tokenizer::Ngram {
                min_gram,
                max_gram,
                prefix_only,
                stop_word_filters,
                store_doc,
                length_limit,
                case_sensitive,
            } => {
                assert_eq!(min_gram, 2);
                assert_eq!(max_gram, 4);
                assert!(prefix_only);
                assert_eq!(stop_word_filters, vec!["english", "german"]);
                assert!(store_doc);
                assert_eq!(length_limit, 80);
                assert!(case_sensitive);
            }
            _ => panic!("Unexpected variant"),
        }

        // invalid value type
        let invalid_json_1 = r#"
            {
                "type": "ngram",
                "min_gram": false,
                "max_gram": 4,
                "prefix_only": true,
                "stop_word_filters": ["english", "german"],
                "store_doc": true,
                "length_limit": 80,
                "case_sensitive": true
            }
        "#;
        let invalid_tokenizer_1: Result<Tokenizer, _> = serde_json::from_str(invalid_json_1);
        assert!(invalid_tokenizer_1.is_err());

        // invalid value
        let invalid_json_2 = r#"
            {
                "type": "ngram",
                "min_gram": -1,
                "max_gram": 4,
                "prefix_only": true,
                "stop_word_filters": ["chinese", "german"],
                "store_doc": true,
                "length_limit": 80,
                "case_sensitive": true
            }
        "#;
        let invalid_tokenizer_2: Result<Tokenizer, _> = serde_json::from_str(invalid_json_2);
        assert!(invalid_tokenizer_2.is_err());

        // invalid key
        let invalid_json_3 = r#"
            {
                "type": "ngram",
                "ngram": 2,
                "max_gram": 4,
                "prefix_only": true,
                "stop_word_filters": ["german"],
                "store_doc": true,
                "length_limit": 80,
                "case_sensitive": true
            }
        "#;
        let invalid_tokenizer_3: Result<Tokenizer, _> = serde_json::from_str(invalid_json_3);
        assert!(invalid_tokenizer_3.is_err());
    }

    #[test]
    fn test_deserialize_chinese() {
        let json_str = r#"
            {
                "type": "chinese",
                "jieba": "default",
                "mode": "search",
                "hmm": true,
                "store_doc": false,
                "case_sensitive": false
            }
        "#;
        let tokenizer: Tokenizer = serde_json::from_str(json_str).unwrap();
        match tokenizer {
            Tokenizer::Chinese {
                jieba,
                mode,
                hmm,
                store_doc,
                case_sensitive,
            } => {
                assert_eq!(jieba, "default");
                assert_eq!(mode, "search");
                assert!(hmm);
                assert!(!store_doc);
                assert!(!case_sensitive);
            }
            _ => panic!("Unexpected variant"),
        }

        // invalid value type
        let invalid_json_1 = r#"
            {
                "type": "chinese",
                "jieba": "default",
                "mode": all,
                "hmm": true,
                "store_doc": false,
                "case_sensitive": false
            }
        "#;
        let invalid_tokenizer_1: Result<Tokenizer, _> = serde_json::from_str(invalid_json_1);
        assert!(invalid_tokenizer_1.is_err());

        // invalid value
        let invalid_json_2 = r#"
            {
                "type": "chinese",
                "jieba": "default",
                "mode": "apple",
                "hmm": true,
                "store_doc": false,
                "case_sensitive": false
            }
        "#;
        let invalid_tokenizer_2: Result<Tokenizer, _> = serde_json::from_str(invalid_json_2);
        assert!(invalid_tokenizer_2.is_err());

        // invalid key
        let invalid_json_3 = r#"
            {
                "type": "chinese",
                "jieba": "default",
                "mode: "all",
                "hmm": true,
                "store_doc": false,
                "case_sensitive": false
            }
        "#;
        let invalid_tokenizer_3: Result<Tokenizer, _> = serde_json::from_str(invalid_json_3);
        assert!(invalid_tokenizer_3.is_err());
    }


    #[test]
    fn test_deserialize_icu() {
        let json_str = r#"
            {
                "type": "icu",
                "mode": "word",
                "store_doc": false,
                "case_sensitive": false
            }
        "#;
        let tokenizer: Tokenizer = serde_json::from_str(json_str).unwrap();
        match tokenizer {
            Tokenizer::Icu {
                mode,
                store_doc,
                case_sensitive, ..
            } => {
                assert_eq!(mode, "word");
                assert!(!store_doc);
                assert!(!case_sensitive);
            }
            _ => panic!("Unexpected variant"),
        }

        // invalid value type
        let invalid_json_1 = r#"
            {
                "type": "icu",
                "mode": 0,
                "store_doc": false,
                "case_sensitive": false
            }
        "#;
        let invalid_tokenizer_1: Result<Tokenizer, _> = serde_json::from_str(invalid_json_1);
        assert!(invalid_tokenizer_1.is_err());

        // invalid value
        let invalid_json_2 = r#"
            {
                "type": "icu",
                "mode": "all",
                "store_doc": false,
                "case_sensitive": false
            }
        "#;
        let invalid_tokenizer_2: Result<Tokenizer, _> = serde_json::from_str(invalid_json_2);
        assert!(invalid_tokenizer_2.is_err());

        // invalid key
        let invalid_json_3 = r#"
            {
                "type": "icu",
                "hnm": false,
                "store_doc": false,
                "case_sensitive": false
            }
        "#;
        let invalid_tokenizer_3: Result<Tokenizer, _> = serde_json::from_str(invalid_json_3);
        assert!(invalid_tokenizer_3.is_err());
    }
}
