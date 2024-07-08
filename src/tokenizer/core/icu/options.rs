/// ICU Tokenizer Option
#[derive(Debug, Clone)]
pub enum IcuOption {
    Grapheme,
    Line,
    Sentence,
    Word, // work best
}
