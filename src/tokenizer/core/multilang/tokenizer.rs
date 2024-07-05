use super::*;
use jieba_rs::Jieba;
use log::{trace};
use std::sync::Arc;
use crate::tokenizer::core::multilang::options::IcuOption;
use crate::tokenizer::core::multilang::stream::IcuTokenStream;
use icu::segmenter::{GraphemeClusterSegmenter, LineSegmenter, SentenceSegmenter, WordSegmenter};


enum SegmentWorker {
    Grapheme(),
    Line,
    Sentence,
    Word, // work best
}


#[derive(Clone, Debug)]
pub struct IcuTokenizer {
    /// Separation config
    pub option: IcuOption,
}

impl Default for IcuTokenizer {
    fn default() -> Self {
        IcuTokenizer {
            option: IcuOption::Word,
        }
    }
}

impl ::tantivy::tokenizer::Tokenizer for IcuTokenizer {
    type TokenStream<'a> = IcuTokenStream<'a>;

    /// Cut text into tokens
    fn token_stream<'a>(&mut self, text: &'a str) -> IcuTokenStream<'a> {
        let result = match self.option {
            IcuOption::Grapheme => {
                let graph_segmenter = GraphemeClusterSegmenter::new();
                let graph_breakpoints: Vec<usize> = graph_segmenter.segment_str(text).collect();
                let graphs: Vec<&str> = graph_breakpoints
                    .windows(2)
                    .map(|window| &text[window[0]..window[1]])
                    .collect();
                graphs
            },
            IcuOption::Line => {
                let line_segmenter = LineSegmenter::new_auto();
                let line_breakpoints: Vec<usize> = line_segmenter.segment_str(text).collect();
                let words: Vec<&str> = line_breakpoints
                    .windows(2)
                    .map(|window| &text[window[0]..window[1]])
                    .collect();
                words
            },
            IcuOption::Sentence => {
                let sentence_segmenter = SentenceSegmenter::new();
                let sentence_breakpoints: Vec<usize> = sentence_segmenter.segment_str(text).collect();
                let sentence: Vec<&str> = sentence_breakpoints
                    .windows(2)
                    .map(|window| &text[window[0]..window[1]])
                    .collect();
                sentence
            }
            IcuOption::Word => {
                let word_segmenter = WordSegmenter::new_auto();
                let word_breakpoints: Vec<usize> = word_segmenter.segment_str(text).collect();
                let words: Vec<&str> = word_breakpoints
                    .windows(2)
                    .map(|window| &text[window[0]..window[1]])
                    .collect();
                words
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
        IcuTokenStream::new(text, result)
    }
}
