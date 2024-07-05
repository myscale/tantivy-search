use std::str::FromStr;
use std::sync::Arc;
use std::collections::HashMap;
use jieba_rs::Jieba;
use tantivy::tokenizer::{LowerCaser, NgramTokenizer, RawTokenizer, RemoveLongFilter, SimpleTokenizer, Stemmer, StopWordFilter, TextAnalyzer, WhitespaceTokenizer};
use crate::common::errors::TantivySearchTokenizerError;
use crate::tokenizer::core::cangjie::{CangJieTokenizer, TokenizerOption};
use crate::tokenizer::ingredient::{Config, Tokenizer};
use crate::tokenizer::languages::{SupportFilterLanguage, SupportLanguageAlgorithm};

pub struct TantivySearchTokenizerConfig {
    pub tokenizer_name: String,
    pub text_analyzer: TextAnalyzer,
    pub doc_store: bool,
}
impl TantivySearchTokenizerConfig {
    fn new(tokenizer_name: String, text_analyzer: TextAnalyzer, doc_store: bool) -> Self {
        Self {
            tokenizer_name,
            text_analyzer,
            doc_store,
        }
    }
}

pub struct TokenizerWrapper<'a> {
    tokenizer: &'a Tokenizer,
}
impl<'a> TokenizerWrapper<'a> {
    pub fn new(tokenizer: &'a Tokenizer) -> Self {
        Self {
            tokenizer,
        }
    }

    pub fn is_store_doc(&self) -> bool {
        match self.tokenizer {
            Tokenizer::Default { store_doc }
            | Tokenizer::Raw { store_doc }
            | Tokenizer::Simple { store_doc, .. }
            | Tokenizer::Stem { store_doc, .. }
            | Tokenizer::Whitespace { store_doc, .. }
            | Tokenizer::Ngram { store_doc, .. }
            | Tokenizer::Chinese { store_doc, .. } => *store_doc
        }
    }

    pub fn is_lower_case(&self) -> bool {
        let lower_case = match self.tokenizer {
            Tokenizer::Default { .. } => true,
            Tokenizer::Raw { .. } => false,
            Tokenizer::Simple { case_sensitive, .. }
            | Tokenizer::Stem { case_sensitive, .. }
            | Tokenizer::Whitespace { case_sensitive, .. }
            | Tokenizer::Ngram { case_sensitive, .. }
            | Tokenizer::Chinese { case_sensitive, .. } => !*case_sensitive
        };
        return lower_case;
    }

    pub fn generate_text_analyzer(&self) -> Result<TextAnalyzer, TantivySearchTokenizerError> {
        match self.tokenizer {
            Tokenizer::Default { .. } => {
                Ok(
                    TextAnalyzer::builder(SimpleTokenizer::default())
                        .filter(RemoveLongFilter::limit(40))
                        .filter(LowerCaser)
                        .build()
                )
            }
            Tokenizer::Raw { .. } => {
                Ok(TextAnalyzer::builder(RawTokenizer::default()).build())
            }
            Tokenizer::Simple {
                stop_word_filters,
                length_limit,
                case_sensitive,
                ..
            } => {
                let mut builder = TextAnalyzer::builder(SimpleTokenizer::default()).dynamic();
                builder = builder.filter_dynamic(RemoveLongFilter::limit(*length_limit));

                for stop_word_filter in stop_word_filters {
                    let language = SupportFilterLanguage::from_str(stop_word_filter.as_str())
                        .unwrap()
                        .to_language();
                    if language.is_some() {
                        builder = builder.filter_dynamic(StopWordFilter::new(language.unwrap()).unwrap());
                    }
                }

                if *case_sensitive == false {
                    builder = builder.filter_dynamic(LowerCaser);
                }

                Ok(builder.build())
            }
            Tokenizer::Stem {
                stop_word_filters,
                stem_languages,
                length_limit,
                case_sensitive,
                ..
            } => {
                let mut builder = TextAnalyzer::builder(SimpleTokenizer::default()).dynamic();

                builder = builder.filter_dynamic(RemoveLongFilter::limit(*length_limit));

                for stop_word_filter in stop_word_filters {
                    let language = SupportFilterLanguage::from_str(stop_word_filter.as_str())
                        .unwrap()
                        .to_language();
                    if language.is_some() {
                        builder = builder
                            .filter_dynamic(StopWordFilter::new(language.unwrap()).unwrap());
                    }
                }

                for stem_language in stem_languages {
                    let language = SupportLanguageAlgorithm::from_str(stem_language.as_str())
                        .unwrap()
                        .to_language();
                    if language.is_some() {
                        builder = builder.filter_dynamic(Stemmer::new(language.unwrap()));
                    }
                }

                if *case_sensitive == false {
                    builder = builder.filter_dynamic(LowerCaser);
                }

                Ok(builder.build())
            }
            Tokenizer::Whitespace {
                stop_word_filters,
                length_limit,
                case_sensitive,
                ..
            } => {
                let mut builder =
                    TextAnalyzer::builder(WhitespaceTokenizer::default()).dynamic();

                builder = builder.filter_dynamic(RemoveLongFilter::limit(*length_limit));

                for stop_word_filter in stop_word_filters {
                    let language = SupportFilterLanguage::from_str(stop_word_filter.as_str())
                        .unwrap()
                        .to_language();
                    if language.is_some() {
                        builder = builder
                            .filter_dynamic(StopWordFilter::new(language.unwrap()).unwrap());
                    }
                }

                if *case_sensitive == false {
                    builder = builder.filter_dynamic(LowerCaser);
                }

                Ok(builder.build())
            }
            Tokenizer::Ngram {
                min_gram,
                max_gram,
                prefix_only,
                stop_word_filters,
                length_limit,
                case_sensitive,
                ..
            } => {
                if min_gram >= max_gram || (*min_gram == 0 && *max_gram == 0) {
                    return Err(TantivySearchTokenizerError::BuildTokenizerError(
                        "`min_gram` should be smaller than `max_gram`".to_string(),
                    ));
                }

                let mut builder = TextAnalyzer::builder(
                    NgramTokenizer::new(
                        *min_gram,
                        *max_gram,
                        *prefix_only,
                    ).map_err(|e| { TantivySearchTokenizerError::TantivyError(e) })?
                ).dynamic();

                builder = builder.filter_dynamic(RemoveLongFilter::limit(*length_limit));

                for stop_word_filter in stop_word_filters {
                    let language = SupportFilterLanguage::from_str(stop_word_filter.as_str())
                        .unwrap()
                        .to_language();
                    if language.is_some() {
                        builder = builder
                            .filter_dynamic(StopWordFilter::new(language.unwrap()).unwrap());
                    }
                }

                if *case_sensitive == false {
                    builder = builder.filter_dynamic(LowerCaser);
                }

                Ok(builder.build())
            }
            Tokenizer::Chinese {
                jieba,
                mode,
                hmm,
                case_sensitive,
                ..
            } => {
                let jieba_mode: Jieba = match jieba.as_str() {
                    "default" => Jieba::default(),
                    "empty" => Jieba::empty(),
                    _ => Jieba::empty(),
                };

                let tokenizer_option: TokenizerOption = match mode.as_str() {
                    "all" => TokenizerOption::All,
                    "unicode" => TokenizerOption::Unicode,
                    "default" => TokenizerOption::Default { hmm: *hmm },
                    "search" => TokenizerOption::ForSearch { hmm: *hmm },
                    _ => TokenizerOption::Unicode, // default option
                };

                let mut builder = TextAnalyzer::builder(CangJieTokenizer {
                    worker: Arc::new(jieba_mode),
                    option: tokenizer_option,
                }).dynamic();

                if *case_sensitive == false {
                    builder = builder.filter_dynamic(LowerCaser);
                }
                Ok(builder.build())
            }
        }
    }

    pub fn get_tokenizer_name(&self) -> &str {
        match self.tokenizer {
            Tokenizer::Default { .. } => "default",
            Tokenizer::Raw { .. } => "raw",
            Tokenizer::Simple { .. } => "simple",
            Tokenizer::Stem { .. } => "stem",
            Tokenizer::Whitespace { .. } => "whitespace",
            Tokenizer::Ngram { .. } => "ngram",
            Tokenizer::Chinese { .. } => "chinese",
        }
    }

}

pub struct TantivySearchTokenizerUtils;

impl TantivySearchTokenizerUtils {
    pub fn parser_from_tokenizer_config(
        tokenizer_config: Config
    ) -> Result<HashMap<String, TantivySearchTokenizerConfig>, TantivySearchTokenizerError> {
        let mut tokenizer_map: HashMap<String, TantivySearchTokenizerConfig> = HashMap::new();
        for (col_name, col) in tokenizer_config.get_columns() {
            let tokenizer = col.get_tokenizer();
            let inner_tokenizer = TokenizerWrapper::new(tokenizer);
            let tokenizer_config = TantivySearchTokenizerConfig::new(
                inner_tokenizer.get_tokenizer_name().to_string(),
                inner_tokenizer.generate_text_analyzer()?,
                inner_tokenizer.is_store_doc()
            );
            tokenizer_map.insert(col_name.to_string(), tokenizer_config);
        }
        Ok(tokenizer_map)
    }

    pub fn parser_index_json_parameter(
        tokenizer_json_str: &str
    ) -> Result<HashMap<String, TantivySearchTokenizerConfig>, TantivySearchTokenizerError> {
        let config: Config = serde_json::from_str(tokenizer_json_str)
            .map_err(
                |e| TantivySearchTokenizerError::JsonDeserializeError(e.to_string())
            )?;
        Self::parser_from_tokenizer_config(config)
    }

    pub fn verify_index_json_parameter(
        tokenizer_json_str: &str
    ) -> Result<bool, TantivySearchTokenizerError> {
        let _: Config = serde_json::from_str(tokenizer_json_str)
            .map_err(|e| TantivySearchTokenizerError::JsonDeserializeError(e.to_string()))?;
        Ok(true)
    }
}