use super::*;
use jieba_rs::Jieba;
use log::{trace};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct CangJieTokenizer {
    /// Separation algorithm provider
    pub worker: Arc<Jieba>,
    /// Separation config
    pub option: TokenizerOption,
}

impl Default for CangJieTokenizer {
    fn default() -> Self {
        CangJieTokenizer {
            worker: Arc::new(Jieba::empty()),
            option: TokenizerOption::Default { hmm: false },
        }
    }
}

impl ::tantivy::tokenizer::Tokenizer for CangJieTokenizer {
    type TokenStream<'a> = CangjieTokenStream<'a>;

    /// Cut text into tokens
    fn token_stream<'a>(&mut self, text: &'a str) -> CangjieTokenStream<'a> {
        let result = match self.option {
            TokenizerOption::All => self.worker.cut_all(text),
            TokenizerOption::Default { hmm: use_hmm } => self.worker.cut(text, use_hmm),
            TokenizerOption::ForSearch { hmm: use_hmm } => {
                self.worker.cut_for_search(text, use_hmm)
            }
            TokenizerOption::Unicode => {
                text.chars()
                    .fold((0usize, vec![]), |(offset, mut result), the_char| {
                        result.push(&text[offset..offset + the_char.len_utf8()]);
                        (offset + the_char.len_utf8(), result)
                    })
                    .1
            }
        };
        let result = result
            .iter()
            .flat_map(|s| {
                s.split(|c: char| !c.is_alphanumeric())
                    .filter(|substr| !substr.is_empty())
                    .collect::<Vec<&str>>()
            })
            .collect();
        trace!("{:?}->{:?}", text, result);
        CangjieTokenStream::new(text, result)
    }
}
