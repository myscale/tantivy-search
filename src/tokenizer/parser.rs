use std::str::FromStr;
use std::sync::Arc;
use std::collections::HashMap;
use jieba_rs::Jieba;
use tantivy::tokenizer::{LowerCaser, NgramTokenizer, RawTokenizer, RemoveLongFilter, SimpleTokenizer, Stemmer, StopWordFilter, TextAnalyzer, TextAnalyzerBuilder, WhitespaceTokenizer};
use crate::common::errors::TokenizerError;
use crate::tokenizer::core::cangjie::{CangjieTokenizer, CangjieOption};
use crate::tokenizer::core::icu::{IcuOption, IcuTokenizer};
use crate::tokenizer::ingredient::{Config, Tokenizer};
use crate::tokenizer::languages::{SupportFilterLanguage, SupportLanguageAlgorithm};

pub struct TokenizerConfig {
    pub tokenizer_name: String,
    pub text_analyzer: TextAnalyzer,
    pub doc_store: bool,
}
impl TokenizerConfig {
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
    fn apply_stop_word_filters(
        mut builder: TextAnalyzerBuilder,
        stop_word_filters: &Vec<String>,
    ) -> TextAnalyzerBuilder {
        for stop_word_filter in stop_word_filters {
            let language = SupportFilterLanguage::from_str(stop_word_filter.as_str())
                .unwrap()
                .to_language();
            if language.is_some() {
                builder = builder.filter_dynamic(StopWordFilter::new(language.unwrap()).unwrap());
            }
        }
        builder
    }

    fn apply_stem_word_filters(
        mut builder: TextAnalyzerBuilder,
        stem_word_filters: &Vec<String>,
    ) -> TextAnalyzerBuilder {
        for stem_language in stem_word_filters {
            let language = SupportLanguageAlgorithm::from_str(stem_language.as_str())
                .unwrap()
                .to_language();
            if language.is_some() {
                builder = builder.filter_dynamic(Stemmer::new(language.unwrap()));
            }
        }
        builder
    }

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
            | Tokenizer::Chinese { store_doc, .. }
            | Tokenizer::Icu { store_doc, .. } => *store_doc
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
            | Tokenizer::Icu { case_sensitive, .. }
            | Tokenizer::Chinese { case_sensitive, .. } => !*case_sensitive
        };
        return lower_case;
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
            Tokenizer::Icu { .. } => "icu"
        }
    }

    pub fn generate_text_analyzer(&self) -> Result<TextAnalyzer, TokenizerError> {
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
                builder = Self::apply_stop_word_filters(builder, &stop_word_filters);
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
                builder = Self::apply_stop_word_filters(builder, &stop_word_filters);
                builder = Self::apply_stem_word_filters(builder, &stem_languages);
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
                builder = Self::apply_stop_word_filters(builder, &stop_word_filters);
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
                    return Err(TokenizerError::BuildTokenizerError(
                        "`min_gram` should be smaller than `max_gram`".to_string(),
                    ));
                }

                let mut builder = TextAnalyzer::builder(
                    NgramTokenizer::new(
                        *min_gram,
                        *max_gram,
                        *prefix_only,
                    ).map_err(|e| { TokenizerError::TantivyError(e) })?
                ).dynamic();

                builder = builder.filter_dynamic(RemoveLongFilter::limit(*length_limit));
                builder = Self::apply_stop_word_filters(builder, &stop_word_filters);
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

                let tokenizer_option: CangjieOption = match mode.as_str() {
                    "all" => CangjieOption::All,
                    "unicode" => CangjieOption::Unicode,
                    "default" => CangjieOption::Default { hmm: *hmm },
                    "search" => CangjieOption::ForSearch { hmm: *hmm },
                    _ => CangjieOption::Unicode, // default option
                };

                let mut builder = TextAnalyzer::builder(CangjieTokenizer {
                    worker: Arc::new(jieba_mode),
                    option: tokenizer_option,
                }).dynamic();

                if *case_sensitive == false {
                    builder = builder.filter_dynamic(LowerCaser);
                }
                Ok(builder.build())
            }

            Tokenizer::Icu {
                mode,
                stop_word_filters,
                stem_languages,
                case_sensitive,
                ..
            } => {
                let tokenizer_option: IcuOption = match mode.as_str() {
                    "grapheme" => IcuOption::Grapheme,
                    "line" => IcuOption::Line,
                    "sentence" => IcuOption::Sentence,
                    "word" => IcuOption::Word,
                    _ => IcuOption::Word, // default option
                };

                let mut builder = TextAnalyzer::builder(IcuTokenizer {
                    option: tokenizer_option,
                }).dynamic();
                builder = Self::apply_stop_word_filters(builder, &stop_word_filters);
                builder = Self::apply_stem_word_filters(builder, &stem_languages);
                if *case_sensitive == false {
                    builder = builder.filter_dynamic(LowerCaser);
                }
                Ok(builder.build())
            }
        }
    }
}

pub struct TokenizerUtils;

impl TokenizerUtils {
    pub fn parser_from_tokenizer_config(
        tokenizer_config: Config
    ) -> Result<HashMap<String, TokenizerConfig>, TokenizerError> {
        let mut tokenizer_map: HashMap<String, TokenizerConfig> = HashMap::new();
        for (col_name, col) in tokenizer_config.get_columns() {
            let tokenizer = col.get_tokenizer();
            let inner_tokenizer = TokenizerWrapper::new(tokenizer);
            let tokenizer_config = TokenizerConfig::new(
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
    ) -> Result<HashMap<String, TokenizerConfig>, TokenizerError> {
        let config: Config = serde_json::from_str(tokenizer_json_str)
            .map_err(
                |e| TokenizerError::JsonDeserializeError(e.to_string())
            )?;
        Self::parser_from_tokenizer_config(config)
    }

    pub fn verify_index_json_parameter(
        tokenizer_json_str: &str
    ) -> Result<bool, TokenizerError> {
        let _: Config = serde_json::from_str(tokenizer_json_str)
            .map_err(|e| TokenizerError::JsonDeserializeError(e.to_string()))?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use tantivy::{Document, Index, IndexReader};
    use tantivy::query::QueryParser;
    use tantivy::schema::{FAST, Field, INDEXED, IndexRecordOption, Schema, TEXT, TextFieldIndexing, TextOptions};
    use crate::ffi::RowIdWithScore;
    use crate::search::collector::top_dos_with_bitmap_collector::TopDocsWithFilter;
    use crate::tokenizer::parser::{TokenizerConfig, TokenizerUtils};


    #[test]
    fn test_icu_json_parameter(){
        let param = r#"
        {
            "text": {
                "tokenizer":{
                    "type": "icu",
                    "stop_word_filters": ["english", "german"],
                    "stem_languages": ["english", "spanish"],
                    "store_doc": true,
                    "case_sensitive": false
                }
            }
        }
        "#;
        // english stem
        let res_0 = index_parameter_search_helper(param, "lock", 10);
        assert_eq!(res_0.len(), 9);
        // english lower case
        let res_1 = index_parameter_search_helper(param, "moon", 10);
        assert_eq!(res_1[0].row_id, 1);
        // english stop word
        let res_2 = index_parameter_search_helper(param, "the", 10);
        assert_eq!(res_2.len(), 0);
        // spanish stem
        let res_3 = index_parameter_search_helper(param, "força", 10);
        assert_eq!(res_3.len(), 1);
        // german upper case
        let res_4 = index_parameter_search_helper(param, "Mond", 10);
        assert_eq!(res_4.len(), 1);
        // german lower case
        let res_5 = index_parameter_search_helper(param, "mond", 10);
        assert_eq!(res_5.len(), 1);
        // german stop word
        let res_6 = index_parameter_search_helper(param, "auf", 10);
        assert_eq!(res_6.len(), 0);
        // chinese and japanese
        let res_7 = index_parameter_search_helper(param, "潮汐", 10);
        assert_eq!(res_7.len(), 2);
    }


    fn index_parameter_search_helper(
        index_json_parameter:&str,
        query_str: &str,
        limit: usize
    ) -> Vec<RowIdWithScore>
    {
        let reader = prepare_index_data(index_json_parameter);
        let search_fields = [Field::from_field_id(1)].to_vec();
        let query_parser: QueryParser =
            QueryParser::for_index(reader.searcher().index(), search_fields.clone());
        let top_docs_collector: TopDocsWithFilter =
            TopDocsWithFilter::with_limit(limit)
                .with_searcher(reader.searcher().clone())
                .with_text_fields(search_fields.clone())
                .with_stored_text(true);
        let text_query = query_parser.parse_query(query_str).unwrap();
        reader.searcher()
            .search(&text_query, &top_docs_collector)
            .unwrap()
    }

    // col: row_id, text
    // FixMe: Use TermsQuery rather than ParserQuery.
    fn prepare_index_data(index_json_parameter: &str) -> IndexReader {
        let mut schema_builder = Schema::builder();
        schema_builder.add_u64_field("row_id", FAST | INDEXED);
        let col_tokenizer_map: HashMap<String, TokenizerConfig> =
            TokenizerUtils::parser_index_json_parameter(index_json_parameter).expect("");
        let tokenizer_name = |cn:&str, tn:&str| format!("{}_{}", cn, tn);
        if let Some(tokenizer_config) = col_tokenizer_map.get("text") {
            let mut text_options = TextOptions::default().set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer(tokenizer_name("text", tokenizer_config.tokenizer_name.as_str()).as_str())
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            );
            if tokenizer_config.doc_store {
                text_options = text_options.set_stored();
            }
            schema_builder.add_text_field("text", text_options);
        } else {
            schema_builder.add_text_field("text", TEXT);
        }
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema.clone());
        for (col_name, tokenizer_config) in col_tokenizer_map.iter() {
            index.tokenizers().register(
                tokenizer_name(col_name, tokenizer_config.tokenizer_name.as_str()).as_str(),
                tokenizer_config.text_analyzer.clone()
            );
        }
        let mut writer = index.writer_with_num_threads(2, 1024 * 1024 * 64).unwrap();
        let row_id_field = schema.get_field("row_id").unwrap();
        let text_field = schema.get_field("text").unwrap();
        // Index some documents.
        let docs = vec![
            "在地月潮汐锁定（Tidal Locking）系统中，潮汐力会使月球产生潮汐隆起。",
            "In the Earth-Moon tidal locking system, tidal forces cause the Moon to experience tidal bulges.",
            "地球と月の潮汐ロッキング（Tidal Locking）システムにおいて、潮汐力は月に潮汐膨らみを引き起こします。",
            "지구-달 조석 고정(Tidal Locking) 시스템에서, 조석력은 달에 조석 팽창을 일으킵니다.",
            "В системе приливного захвата Земли и Луны (Tidal Locking), приливные силы вызывают приливные выпуклости на Луне.",
            "Im Erde-Mond-Gezeitenverriegelungssystem (Tidal Locking) verursachen Gezeitenkräfte Gezeitenwölbungen auf dem Mond.",
            "Dans le système de verrouillage des marées Terre-Lune (Tidal Locking), les forces de marée provoquent des renflements de marée sur la Lune.",
            "En el sistema de bloqueo de mareas Tierra-Luna (Tidal Locking), las fuerzas de marea provocan protuberancias de marea en la Luna.",
            "No sistema de bloqueio de marés Terra-Lua (Tidal Locking), as forças de maré causam protuberâncias de maré na Lua.",
        ];
        for row_id in 0..docs.len(){
            let mut doc = Document::default();
            doc.add_u64(row_id_field, row_id as u64);
            doc.add_text(text_field, &docs[row_id]);
            let _ = writer.add_document(doc);
        }
        writer.commit().unwrap();
        return writer.index().reader().unwrap();
    }
}