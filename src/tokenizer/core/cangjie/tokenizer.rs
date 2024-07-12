use super::*;
use jieba_rs::Jieba;
use log::{trace};
use std::sync::Arc;
use tantivy::tokenizer::Tokenizer;
use super::super::TokenUtils;

#[derive(Clone, Debug)]
pub struct CangjieTokenizer {
    /// Separation algorithm provider
    pub worker: Arc<Jieba>,
    /// Separation config
    pub option: CangjieOption,
}

impl Default for CangjieTokenizer {
    fn default() -> Self {
        CangjieTokenizer {
            worker: Arc::new(Jieba::empty()),
            option: CangjieOption::Default { hmm: false },
        }
    }
}

impl Tokenizer for CangjieTokenizer {
    type TokenStream<'a> = CangjieTokenStream<'a>;

    /// Cut text into tokens
    fn token_stream<'a>(&mut self, text: &'a str) -> CangjieTokenStream<'a> {
        let result = match self.option {
            CangjieOption::All => self.worker.cut_all(text),
            CangjieOption::Default { hmm: use_hmm } => self.worker.cut(text, use_hmm),
            CangjieOption::ForSearch { hmm: use_hmm } => {
                self.worker.cut_for_search(text, use_hmm)
            }
            CangjieOption::Unicode => {
                text.chars()
                    .fold((0usize, vec![]), |(offset, mut result), the_char| {
                        result.push(&text[offset..offset + the_char.len_utf8()]);
                        (offset + the_char.len_utf8(), result)
                    })
                    .1
            }
        };
        let result = TokenUtils::remove_nonsense_token(result);
        trace!("{:?}->{:?}", text, result);
        CangjieTokenStream::new(text, result)
    }
}


#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use jieba_rs::Jieba;
    use tantivy::tokenizer::{TextAnalyzer, Token};
    use crate::tokenizer::core::cangjie::{CangjieOption, CangjieTokenizer};
    use crate::tokenizer::core::tests::assert_token;

    #[test]
    fn test_search_mode_with_hmm() {
        let tokens = token_stream_helper(
            "在地月潮汐锁定（Tidal Locking）系统中，潮汐力会使月球产生潮汐隆起。",
            Jieba::default(),
            CangjieOption::ForSearch { hmm: true }
        );
        assert_eq!(tokens.len(), 16);
        assert_token(&tokens[0], 0, "在", 0, 3);
        assert_token(&tokens[1], 1, "地", 3, 6);
        assert_token(&tokens[2], 2, "月", 6, 9);
        assert_token(&tokens[3], 3, "潮汐", 9, 15);
        assert_token(&tokens[4], 4, "锁定", 15, 21);
        assert_token(&tokens[5], 5, "Tidal", 24, 29);
        assert_token(&tokens[6], 6, "Locking", 30, 37);
        assert_token(&tokens[7], 7, "系统", 40, 46);
        assert_token(&tokens[8], 8, "中", 46, 49);
        assert_token(&tokens[9], 9, "潮汐", 52, 58);
        assert_token(&tokens[10], 10, "力会", 58, 64);
        assert_token(&tokens[11], 11, "使", 64, 67);
        assert_token(&tokens[12], 12, "月球", 67, 73);
        assert_token(&tokens[13], 13, "产生", 73, 79);
        assert_token(&tokens[14], 14, "潮汐", 79, 85);
        assert_token(&tokens[15], 15, "隆起", 85, 91);
    }

    #[test]
    fn test_search_mode() {
        let tokens = token_stream_helper(
            "在地月潮汐锁定（Tidal Locking）系统中，潮汐力会使月球产生潮汐隆起。",
            Jieba::default(),
            CangjieOption::ForSearch { hmm: false }
        );
        assert_eq!(tokens.len(), 17);
        assert_token(&tokens[0], 0, "在", 0, 3);
        assert_token(&tokens[1], 1, "地", 3, 6);
        assert_token(&tokens[2], 2, "月", 6, 9);
        assert_token(&tokens[3], 3, "潮汐", 9, 15);
        assert_token(&tokens[4], 4, "锁定", 15, 21);
        assert_token(&tokens[5], 5, "Tidal", 24, 29);
        assert_token(&tokens[6], 6, "Locking", 30, 37);
        assert_token(&tokens[7], 7, "系统", 40, 46);
        assert_token(&tokens[8], 8, "中", 46, 49);
        assert_token(&tokens[9], 9, "潮汐", 52, 58);
        assert_token(&tokens[10], 10, "力", 58, 61);
        assert_token(&tokens[11], 11, "会", 61, 64);
        assert_token(&tokens[12], 12, "使", 64, 67);
        assert_token(&tokens[13], 13, "月球", 67, 73);
        assert_token(&tokens[14], 14, "产生", 73, 79);
        assert_token(&tokens[15], 15, "潮汐", 79, 85);
        assert_token(&tokens[16], 16, "隆起", 85, 91);
    }

    #[test]
    fn test_empty_jieba() {
        let tokens = token_stream_helper(
            "月球(Moon)产生潮汐隆起。",
            Jieba::empty(),
            CangjieOption::ForSearch { hmm: false }
        );
        assert_eq!(tokens.len(), 9);
        assert_token(&tokens[0], 0, "月", 0, 3);
        assert_token(&tokens[1], 1, "球", 3, 6);
        assert_token(&tokens[2], 2, "Moon", 7, 11);
        assert_token(&tokens[3], 3, "产", 12, 15);
        assert_token(&tokens[4], 4, "生", 15, 18);
        assert_token(&tokens[5], 5, "潮", 18, 21);
        assert_token(&tokens[6], 6, "汐", 21, 24);
        assert_token(&tokens[7], 7, "隆", 24, 27);
        assert_token(&tokens[8], 8, "起", 27, 30);
    }

    fn token_stream_helper(text: &str, jieba_mode: Jieba, option: CangjieOption) -> Vec<Token> {
        let tokenizer = CangjieTokenizer {
            worker: Arc::new(jieba_mode),
            option,
        };
        let mut text_analyzer = TextAnalyzer::from(tokenizer);
        let mut token_stream = text_analyzer.token_stream(text);
        let mut tokens: Vec<Token> = vec![];
        let mut add_token = |token: &Token| {
            tokens.push(token.clone());
        };
        token_stream.process(&mut add_token);
        tokens
    }
}
