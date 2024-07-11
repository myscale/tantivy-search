use std::collections::HashSet;
use tantivy::schema::{FieldType, Schema, TextFieldIndexing};
use tantivy::Term;
use tantivy::tokenizer::{BoxTokenStream, TextAnalyzer};
use crate::common::errors::TantivySearchError;
use crate::{common::constants::LOG_CALLBACK, DEBUG, ERROR};
use crate::logger::logger_bridge::TantivySearchLogger;

use crate::ffi::DocWithFreq;
use crate::search::implements::search_utils::SearchUtils;

pub fn get_doc_freq(
    index_path: &str,
    sentence: &str,
) -> Result<Vec<DocWithFreq>, TantivySearchError> {
    static FUNC_NAME: &str = "get_doc_freq";

    let bridge = SearchUtils::get_index_reader_bridge(FUNC_NAME, index_path)?;
    let searcher = bridge.reader.searcher();
    let schema: Schema = bridge.index.schema();
    let mut terms: HashSet<Term> = HashSet::new();

    for (col_field, col_field_entry) in schema.fields() {
        let field_type = col_field_entry.field_type();
        if !field_type.is_indexed() {
            continue;
        }
        if let FieldType::Str(ref str_options) = field_type {
            let indexing_options: &TextFieldIndexing =
                str_options.get_indexing_options().ok_or_else(|| {
                    let error_msg: String = format!(
                        "column field:{} not indexed, but this error msg shouldn't display",
                        col_field_entry.name()
                    );
                    ERROR!(function: FUNC_NAME, "{}", error_msg);
                    TantivySearchError::InternalError(error_msg)
                })?;
            let mut text_analyzer: TextAnalyzer = searcher
                .index()
                .tokenizers()
                .get(indexing_options.tokenizer())
                .unwrap();
            let mut token_stream: BoxTokenStream<'_> = text_analyzer.token_stream(sentence);
            token_stream.process(&mut |token| {
                let term: Term = Term::from_field_text(col_field, &token.text);
                terms.insert(term);
            });

        }
    }
    let mut doc_with_freq_vector: Vec<DocWithFreq> = vec![];
    for term in terms {
        let doc_freq = searcher.doc_freq(&term).map_err(|e| {
            ERROR!(function: FUNC_NAME, "{}", e);
            TantivySearchError::TantivyError(e)
        })?;
        let doc_with_freq = DocWithFreq::new(
            term.value().as_str().unwrap_or("").to_string(),
            term.field().field_id(),
            doc_freq,
        );
        doc_with_freq_vector.push(doc_with_freq);
    }
    DEBUG!(function: FUNC_NAME, "index_path:[{:?}], doc_with_freq_vector:[{:?}]", index_path, doc_with_freq_vector);
    Ok(doc_with_freq_vector)
}