use super::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Column {
    tokenizer: Tokenizer,
}

impl Column {
    pub fn get_tokenizer(&self) -> &Tokenizer {
        &self.tokenizer
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize() {
        let json_str = r#"{ "tokenizer": { "type": "default", "store_doc": true } }"#;
        let result: Column = serde_json::from_str(json_str).unwrap();
        if let Tokenizer::Default { store_doc } = result.tokenizer {
            assert!(store_doc)
        } else {
            panic!("Error happened when parse `column` json parameter")
        }

        // invalid value type
        let invalid_json_str_1 = r#"{ "tokenizer": { "type": "default", "store_doc": "true" } }"#;
        let result_1: Result<Column, _> = serde_json::from_str(invalid_json_str_1);
        assert!(result_1.is_err());

        // invalid key
        let invalid_json_str_2 = r#"{ "tokenizer": { "type": "default", store_doc": true } }"#;
        let result_2: Result<Column, _> = serde_json::from_str(invalid_json_str_2);
        assert!(result_2.is_err());
    }
}
