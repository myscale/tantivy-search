use super::*;
use super::super::TokenUtils;
use log::{trace};
use icu::segmenter::{GraphemeClusterSegmenter, LineSegmenter, SentenceSegmenter, WordSegmenter};

#[allow(unused)]
enum SegmentWorker {
    Grapheme(),
    Line,
    Sentence,
    Word, // works best
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
        let breakpoints: Vec<usize> = match self.option {
            IcuOption::Grapheme => {
                let icu_grapheme = GraphemeClusterSegmenter::new();
                icu_grapheme.segment_str(text).collect()
            },
            IcuOption::Line => {
                let icu_line = LineSegmenter::new_auto();
                icu_line.segment_str(text).collect()
            },
            IcuOption::Sentence => {
                let icu_sentence = SentenceSegmenter::new();
                icu_sentence.segment_str(text).collect()
            }
            IcuOption::Word => {
                let icu_word = WordSegmenter::new_auto();
                icu_word.segment_str(text).collect()
            }
        };
        let tokens: Vec<&str> = breakpoints
            .windows(2)
            .map(|window| &text[window[0]..window[1]])
            .collect();
        let processed_tokens = TokenUtils::remove_nonsense_token(tokens);
        trace!("{:?}->{:?}", text, processed_tokens);
        IcuTokenStream::new(text, processed_tokens)
    }
}



#[cfg(test)]
mod tests {
    use tantivy::tokenizer::{TextAnalyzer, Token};
    use crate::tokenizer::core::icu::{IcuOption, IcuTokenizer};
    use crate::tokenizer::core::tests::assert_token;


    #[test]
    fn test_word_mode_with_chinese_simple() {
        let tokens = token_stream_helper(
            "在地月潮汐锁定（Tidal Locking）系统中，潮汐力会使月球产生潮汐隆起。",
            IcuOption::Word
        );
        assert_eq!(tokens.len(), 16);
        assert_token(&tokens[0], 0, "在地", 0, 6);
        assert_token(&tokens[1], 1, "月", 6, 9);
        assert_token(&tokens[2], 2, "潮汐", 9, 15);
        assert_token(&tokens[3], 3, "锁定", 15, 21);
        assert_token(&tokens[4], 4, "Tidal", 24, 29);
        assert_token(&tokens[5], 5, "Locking", 30, 37);
        assert_token(&tokens[6], 6, "系统", 40, 46);
        assert_token(&tokens[7], 7, "中", 46, 49);
        assert_token(&tokens[8], 8, "潮汐", 52, 58);
        assert_token(&tokens[9], 9, "力", 58, 61);
        assert_token(&tokens[10], 10, "会", 61, 64);
        assert_token(&tokens[11], 11, "使", 64, 67);
        assert_token(&tokens[12], 12, "月球", 67, 73);
        assert_token(&tokens[13], 13, "产生", 73, 79);
        assert_token(&tokens[14], 14, "潮汐", 79, 85);
        assert_token(&tokens[15], 15, "隆起", 85, 91);
    }
    #[test]
    fn test_word_mode_with_chinese_traditional() {
        let tokens = token_stream_helper(
            "在地月潮汐鎖定（Tidal Locking）系統中，潮汐力會使月球產生潮汐隆起。",
            IcuOption::Word
        );
        assert_eq!(tokens.len(), 16);
        assert_token(&tokens[0], 0, "在地", 0, 6);
        assert_token(&tokens[1], 1, "月", 6, 9);
        assert_token(&tokens[2], 2, "潮汐", 9, 15);
        assert_token(&tokens[3], 3, "鎖定", 15, 21);
        assert_token(&tokens[4], 4, "Tidal", 24, 29);
        assert_token(&tokens[5], 5, "Locking", 30, 37);
        assert_token(&tokens[6], 6, "系統", 40, 46);
        assert_token(&tokens[7], 7, "中", 46, 49);
        assert_token(&tokens[8], 8, "潮汐", 52, 58);
        assert_token(&tokens[9], 9, "力", 58, 61);
        assert_token(&tokens[10], 10, "會", 61, 64);
        assert_token(&tokens[11], 11, "使", 64, 67);
        assert_token(&tokens[12], 12, "月球", 67, 73);
        assert_token(&tokens[13], 13, "產生", 73, 79);
        assert_token(&tokens[14], 14, "潮汐", 79, 85);
        assert_token(&tokens[15], 15, "隆起", 85, 91);
    }
    #[test]
    fn test_word_mode_with_english() {
        let tokens = token_stream_helper(
            "In the Earth-Moon tidal locking system, tidal forces cause the Moon to experience tidal bulges.",
            IcuOption::Word
        );
        assert_eq!(tokens.len(), 16);
        assert_token(&tokens[0], 0, "In", 0, 2);
        assert_token(&tokens[1], 1, "the", 3, 6);
        assert_token(&tokens[2], 2, "Earth", 7, 12);
        assert_token(&tokens[3], 3, "Moon", 13, 17);
        assert_token(&tokens[4], 4, "tidal", 18, 23);
        assert_token(&tokens[5], 5, "locking", 24, 31);
        assert_token(&tokens[6], 6, "system", 32, 38);
        assert_token(&tokens[7], 7, "tidal", 40, 45);
        assert_token(&tokens[8], 8, "forces", 46, 52);
        assert_token(&tokens[9], 9, "cause", 53, 58);
        assert_token(&tokens[10], 10, "the", 59, 62);
        assert_token(&tokens[11], 11, "Moon", 63, 67);
        assert_token(&tokens[12], 12, "to", 68, 70);
        assert_token(&tokens[13], 13, "experience", 71, 81);
        assert_token(&tokens[14], 14, "tidal", 82, 87);
        assert_token(&tokens[15], 15, "bulges", 88, 94);
    }
    #[test]
    fn test_word_mode_with_japan() {
        let tokens = token_stream_helper(
            "地球と月の潮汐固定システム（タイダルロッキング）では、潮汐力によって月が潮汐隆起を引き起こします。",
            IcuOption::Word
        );
        assert_eq!(tokens.len(), 22);
        assert_token(&tokens[0], 0, "地球", 0, 6);
        assert_token(&tokens[1], 1, "と", 6, 9);
        assert_token(&tokens[2], 2, "月", 9, 12);
        assert_token(&tokens[3], 3, "の", 12, 15);
        assert_token(&tokens[4], 4, "潮汐", 15, 21);
        assert_token(&tokens[5], 5, "固定", 21, 27);
        assert_token(&tokens[6], 6, "システム", 27, 39);
        assert_token(&tokens[7], 7, "タイダルロッキング", 42, 69);
        assert_token(&tokens[8], 8, "では", 72, 78);
        assert_token(&tokens[9], 9, "潮汐", 81, 87);
        assert_token(&tokens[10], 10, "力", 87, 90);
        assert_token(&tokens[11], 11, "によって", 90, 102);
        assert_token(&tokens[12], 12, "月", 102, 105);
        assert_token(&tokens[13], 13, "が", 105, 108);
        assert_token(&tokens[14], 14, "潮汐", 108, 114);
        assert_token(&tokens[15], 15, "隆起", 114, 120);
        assert_token(&tokens[16], 16, "を", 120, 123);
        assert_token(&tokens[17], 17, "引き", 123, 129);
        assert_token(&tokens[18], 18, "起", 129, 132);
        assert_token(&tokens[19], 19, "こ", 132, 135);
        assert_token(&tokens[20], 20, "しま", 135, 141);
        assert_token(&tokens[21], 21, "す", 141, 144);
    }
    #[test]
    fn test_word_mode_with_korea() {
        let tokens = token_stream_helper(
            "지구-달 조석 고정 시스템(타이달 록킹)에서 조석력은 달에 조석 융기를 발생시킵니다.",
            IcuOption::Word
        );
        assert_eq!(tokens.len(), 13);
        assert_token(&tokens[0], 0, "지구", 0, 6);
        assert_token(&tokens[1], 1, "달", 7, 10);
        assert_token(&tokens[2], 2, "조석", 11, 17);
        assert_token(&tokens[3], 3, "고정", 18, 24);
        assert_token(&tokens[4], 4, "시스템", 25, 34);
        assert_token(&tokens[5], 5, "타이달", 35, 44);
        assert_token(&tokens[6], 6, "록킹", 45, 51);
        assert_token(&tokens[7], 7, "에서", 52, 58);
        assert_token(&tokens[8], 8, "조석력은", 59, 71);
        assert_token(&tokens[9], 9, "달에", 72, 78);
        assert_token(&tokens[10], 10, "조석", 79, 85);
        assert_token(&tokens[11], 11, "융기를", 86, 95);
        assert_token(&tokens[12], 12, "발생시킵니다", 96, 114);
    }
    #[test]
    fn test_word_mode_with_french() {
        let tokens = token_stream_helper(
            "Dans le système de verrouillage gravitationnel Terre-Lune (Tidal Locking), les forces de marée provoquent des renflements de marée sur la Lune.",
            IcuOption::Word
        );
        assert_eq!(tokens.len(), 22);
        assert_token(&tokens[0], 0, "Dans", 0, 4);
        assert_token(&tokens[1], 1, "le", 5, 7);
        assert_token(&tokens[2], 2, "système", 8, 16);
        assert_token(&tokens[3], 3, "de", 17, 19);
        assert_token(&tokens[4], 4, "verrouillage", 20, 32);
        assert_token(&tokens[5], 5, "gravitationnel", 33, 47);
        assert_token(&tokens[6], 6, "Terre", 48, 53);
        assert_token(&tokens[7], 7, "Lune", 54, 58);
        assert_token(&tokens[8], 8, "Tidal", 60, 65);
        assert_token(&tokens[9], 9, "Locking", 66, 73);
        assert_token(&tokens[10], 10, "les", 76, 79);
        assert_token(&tokens[11], 11, "forces", 80, 86);
        assert_token(&tokens[12], 12, "de", 87, 89);
        assert_token(&tokens[13], 13, "marée", 90, 96);
        assert_token(&tokens[14], 14, "provoquent", 97, 107);
        assert_token(&tokens[15], 15, "des", 108, 111);
        assert_token(&tokens[16], 16, "renflements", 112, 123);
        assert_token(&tokens[17], 17, "de", 124, 126);
        assert_token(&tokens[18], 18, "marée", 127, 133);
        assert_token(&tokens[19], 19, "sur", 134, 137);
        assert_token(&tokens[20], 20, "la", 138, 140);
        assert_token(&tokens[21], 21, "Lune", 141, 145);
    }
    #[test]
    fn test_word_mode_with_german() {
        let tokens = token_stream_helper(
            "Im Erd-Mond-Gezeitenverriegelungssystem (Tidal Locking) verursachen Gezeitenkräfte Gezeitenwölbungen auf dem Mond.",
            IcuOption::Word
        );
        assert_eq!(tokens.len(), 12);
        assert_token(&tokens[0], 0, "Im", 0, 2);
        assert_token(&tokens[1], 1, "Erd", 3, 6);
        assert_token(&tokens[2], 2, "Mond", 7, 11);
        assert_token(&tokens[3], 3, "Gezeitenverriegelungssystem", 12, 39);
        assert_token(&tokens[4], 4, "Tidal", 41, 46);
        assert_token(&tokens[5], 5, "Locking", 47, 54);
        assert_token(&tokens[6], 6, "verursachen", 56, 67);
        assert_token(&tokens[7], 7, "Gezeitenkräfte", 68, 83);
        assert_token(&tokens[8], 8, "Gezeitenwölbungen", 84, 102);
        assert_token(&tokens[9], 9, "auf", 103, 106);
        assert_token(&tokens[10], 10, "dem", 107, 110);
        assert_token(&tokens[11], 11, "Mond", 111, 115);
    }
    #[test]
    fn test_word_mode_with_russia() {
        let tokens = token_stream_helper(
            "В системе приливного захвата Земля-Луна (Tidal Locking) приливные силы вызывают приливные выпуклости на Луне.",
            IcuOption::Word
        );
        assert_eq!(tokens.len(), 15);
        assert_token(&tokens[0], 0, "В", 0, 2);
        assert_token(&tokens[1], 1, "системе", 3, 17);
        assert_token(&tokens[2], 2, "приливного", 18, 38);
        assert_token(&tokens[3], 3, "захвата", 39, 53);
        assert_token(&tokens[4], 4, "Земля", 54, 64);
        assert_token(&tokens[5], 5, "Луна", 65, 73);
        assert_token(&tokens[6], 6, "Tidal", 75, 80);
        assert_token(&tokens[7], 7, "Locking", 81, 88);
        assert_token(&tokens[8], 8, "приливные", 90, 108);
        assert_token(&tokens[9], 9, "силы", 109, 117);
        assert_token(&tokens[10], 10, "вызывают", 118, 134);
        assert_token(&tokens[11], 11, "приливные", 135, 153);
        assert_token(&tokens[12], 12, "выпуклости", 154, 174);
        assert_token(&tokens[13], 13, "на", 175, 179);
        assert_token(&tokens[14], 14, "Луне", 180, 188);
    }
    #[test]
    fn test_word_mode_with_spanish() {
        let tokens = token_stream_helper(
            "En el sistema de acoplamiento por marea Tierra-Luna (Tidal Locking), las fuerzas de marea causan abultamientos de marea en la Luna.",
            IcuOption::Word
        );
        assert_eq!(tokens.len(), 22);

        assert_token(&tokens[0], 0, "En", 0, 2);
        assert_token(&tokens[1], 1, "el", 3, 5);
        assert_token(&tokens[2], 2, "sistema", 6, 13);
        assert_token(&tokens[3], 3, "de", 14, 16);
        assert_token(&tokens[4], 4, "acoplamiento", 17, 29);
        assert_token(&tokens[5], 5, "por", 30, 33);
        assert_token(&tokens[6], 6, "marea", 34, 39);
        assert_token(&tokens[7], 7, "Tierra", 40, 46);
        assert_token(&tokens[8], 8, "Luna", 47, 51);
        assert_token(&tokens[9], 9, "Tidal", 53, 58);
        assert_token(&tokens[10], 10, "Locking", 59, 66);
        assert_token(&tokens[11], 11, "las", 69, 72);
        assert_token(&tokens[12], 12, "fuerzas", 73, 80);
        assert_token(&tokens[13], 13, "de", 81, 83);
        assert_token(&tokens[14], 14, "marea", 84, 89);
        assert_token(&tokens[15], 15, "causan", 90, 96);
        assert_token(&tokens[16], 16, "abultamientos", 97, 110);
        assert_token(&tokens[17], 17, "de", 111, 113);
        assert_token(&tokens[18], 18, "marea", 114, 119);
        assert_token(&tokens[19], 19, "en", 120, 122);
        assert_token(&tokens[20], 20, "la", 123, 125);
        assert_token(&tokens[21], 21, "Luna", 126, 130);

    }
    #[test]
    fn test_word_mode_with_portuguese() {
        let tokens = token_stream_helper(
            "No sistema de travamento por maré Terra-Lua (Tidal Locking), as forças de maré causam protuberâncias de maré na Lua.",
            IcuOption::Word
        );
        assert_eq!(tokens.len(), 20);
        assert_token(&tokens[0], 0, "No", 0, 2);
        assert_token(&tokens[1], 1, "sistema", 3, 10);
        assert_token(&tokens[2], 2, "de", 11, 13);
        assert_token(&tokens[3], 3, "travamento", 14, 24);
        assert_token(&tokens[4], 4, "por", 25, 28);
        assert_token(&tokens[5], 5, "maré", 29, 34);
        assert_token(&tokens[6], 6, "Terra", 35, 40);
        assert_token(&tokens[7], 7, "Lua", 41, 44);
        assert_token(&tokens[8], 8, "Tidal", 46, 51);
        assert_token(&tokens[9], 9, "Locking", 52, 59);
        assert_token(&tokens[10], 10, "as", 62, 64);
        assert_token(&tokens[11], 11, "forças", 65, 72);
        assert_token(&tokens[12], 12, "de", 73, 75);
        assert_token(&tokens[13], 13, "maré", 76, 81);
        assert_token(&tokens[14], 14, "causam", 82, 88);
        assert_token(&tokens[15], 15, "protuberâncias", 89, 104);
        assert_token(&tokens[16], 16, "de", 105, 107);
        assert_token(&tokens[17], 17, "maré", 108, 113);
        assert_token(&tokens[18], 18, "na", 114, 116);
        assert_token(&tokens[19], 19, "Lua", 117, 120);
    }
    #[test]
    fn test_word_mode_with_italian() {
        let tokens = token_stream_helper(
            "Nel sistema di bloccaggio mareale Terra-Luna (Tidal Locking), le forze mareali causano rigonfiamenti mareali sulla Luna.",
            IcuOption::Word
        );
        assert_eq!(tokens.len(), 17);
        assert_token(&tokens[0], 0, "Nel", 0, 3);
        assert_token(&tokens[1], 1, "sistema", 4, 11);
        assert_token(&tokens[2], 2, "di", 12, 14);
        assert_token(&tokens[3], 3, "bloccaggio", 15, 25);
        assert_token(&tokens[4], 4, "mareale", 26, 33);
        assert_token(&tokens[5], 5, "Terra", 34, 39);
        assert_token(&tokens[6], 6, "Luna", 40, 44);
        assert_token(&tokens[7], 7, "Tidal", 46, 51);
        assert_token(&tokens[8], 8, "Locking", 52, 59);
        assert_token(&tokens[9], 9, "le", 62, 64);
        assert_token(&tokens[10], 10, "forze", 65, 70);
        assert_token(&tokens[11], 11, "mareali", 71, 78);
        assert_token(&tokens[12], 12, "causano", 79, 86);
        assert_token(&tokens[13], 13, "rigonfiamenti", 87, 100);
        assert_token(&tokens[14], 14, "mareali", 101, 108);
        assert_token(&tokens[15], 15, "sulla", 109, 114);
        assert_token(&tokens[16], 16, "Luna", 115, 119);
    }

    fn token_stream_helper(text: &str, option: IcuOption) -> Vec<Token> {
        let tokenizer = IcuTokenizer {
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