use super::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    #[serde(flatten)]
    columns: std::collections::HashMap<String, Column>,
}

impl Config {
    pub fn get_columns(&self) -> &std::collections::HashMap<String, Column> {
        &self.columns
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize() {
        let json_str = r#"
            {
                "col1": { "tokenizer": { "type": "default", "store_doc": false } },
                "col2": { "tokenizer": { "type": "simple", "case_sensitive": true } }
            }
        "#;
        let config: Config = serde_json::from_str(json_str).unwrap();
        let columns = config.get_columns();
        assert!(columns.contains_key("col1"));
        assert!(columns.contains_key("col2"));
        assert!(!columns.contains_key("col3"));

        if let Some(column) = config.get_columns().get("col1") {
            if let Tokenizer::Default { store_doc, .. } = column.get_tokenizer() {
                assert!(!store_doc);
            } else {
                panic!("Error happened when parse `config` json parameter!")
            }
        } else {
            panic!("`col1` not found!")
        }

        if let Some(column) = config.get_columns().get("col2") {
            if let Tokenizer::Simple { case_sensitive, .. } = column.get_tokenizer() {
                assert!(case_sensitive);
            } else {
                panic!("Error happened when parse `config` json parameter!")
            }
        } else {
            panic!("`col2` not found!")
        }
    }
}
