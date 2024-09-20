#[cfg(test)]
mod tests {
    use crate::ffi::RowIdWithScore;
    use crate::index::implements::create_index_with_parameter;
    use crate::search::bridge::index_reader_bridge::IndexReaderBridge;
    use crate::search::collector::top_dos_with_bitmap_collector::TopDocsWithFilter;
    use crate::search::implements::load_index_reader;
    use crate::search::implements::strategy::BM25NaturalLanguageStrategy;
    use crate::search::implements::strategy::BM25StandardQueryStrategy;
    use crate::search::implements::strategy::QueryExecutor;
    use crate::{FFI_INDEX_SEARCHER_CACHE, FFI_INDEX_WRITER_CACHE};
    use std::ffi::{c_char, CString};
    use std::sync::Arc;
    use tantivy::query::{Query, QueryParser};
    use tantivy::schema::{Schema, FAST, INDEXED, TEXT};
    use tantivy::Document;
    use tempfile::TempDir;
    use crate::common::TEST_MUTEX;

    fn to_c_str(s: &str) -> *const c_char {
        CString::new(s).unwrap().into_raw()
    }

    pub fn test_create_index_with_valid_tokenizer(tokenizer_str: &str) {
        let _guard = TEST_MUTEX.lock().unwrap();
        let _log_directory = to_c_str("/tmp");
        let _log_level = to_c_str("info"); // Assuming this is an invalid log level.
                                           // tantivy_search_log4rs_initialize(log_directory, log_level, false, true, false);
        let temp_directory = TempDir::new().unwrap();
        let temp_directory_str = temp_directory.path().to_str().unwrap();

        let result = create_index_with_parameter(
            temp_directory_str,
            &vec!["col1".to_string()],
            tokenizer_str,
        );
        assert!(result.is_ok());

        // Get index writer from CACHE
        let index_writer_bridge = FFI_INDEX_WRITER_CACHE
            .get_index_writer_bridge(temp_directory_str.to_string())
            .unwrap();
        let mocked_docs: Vec<String> = vec![
            "在繁忙的城市生活中，finding tranquility can be challenging. 每天的压力源源不断，工作和家庭之间的平衡让人疲惫。However,
             taking time to meditate and relax——即使只是几分钟——可以帮助我们恢复内心的平静。
             John Doe 曾经在《The Art of Peace》中提到，冥想是保持内心宁静的关键。".to_string(),

            "学习一门新的语言不仅仅是掌握语法和词汇。It's also about understanding the culture and traditions behind it.
             例如，学习中文（尤其是汉字）可以帮助你更好地了解中国的历史和风俗——这是一种深入的文化体验。
             Linguist Noam Chomsky 在《Syntactic Structures》中强调了语言结构的重要性。".to_string(),

            "科技的进步改变了我们的生活方式。From smartphones to smart homes, technology is integrated into every aspect of our daily lives.
             这种变化带来了便利，也带来了新的挑战——比如隐私问题和信息安全。Elon Musk 的公司 SpaceX 正在探索如何通过技术改变人类的未来。".to_string(),

            "旅行是扩展视野的最佳方式之一。Exploring new destinations allows you to experience different cultures and meet new people.
             每一次旅行都是一次成长的机会，让你对世界有更深的理解——这种经历是无价的。
             Jules Verne 的《Around the World in Eighty Days》描述了旅行的奇妙冒险。".to_string(),

            "健康的生活方式包括均衡的饮食和规律的锻炼。
             Eating a variety of nutritious foods and staying active can significantly improve your overall well-being.
             健康不仅仅是身体的健康，还有心理的健康——两者同样重要。Dr. Robert Atkins 的《Atkins Diet》提供了一种特殊的饮食方法。".to_string(),

            "艺术是表达情感的一种方式。Whether it's painting, music, or dance, art allows individuals to convey their deepest thoughts and feelings.
             通过艺术，人们可以找到一种独特的表达方式——这是一种心灵的释放。Vincent van Gogh 的作品《Starry Night》是艺术表达的经典之作。".to_string(),

            "教育是社会进步的基石。Quality education provides individuals with the knowledge and skills they need to succeed in life.
             一个良好的教育体系可以提高国家的竞争力和创新能力——这对未来至关重要。Malala Yousafzai 在《I Am Malala》中呼吁全球教育平等。".to_string(),

            "环境保护是我们共同的责任。Protecting our planet is crucial for the survival of future generations.
             我们应该采取积极的行动来减少污染和保护自然资源——这不仅仅是为了我们自己。Rachel Carson 的《Silent Spring》揭示了环境保护的重要性。".to_string(),

            "经济的发展离不开创新和创业。Entrepreneurship drives economic growth by creating new jobs and industries.
             一个鼓励创新的环境可以激发更多人的创造力和潜力——从而促进社会的繁荣。Steve Jobs 创立的 Apple 公司就是创新驱动经济的典范。".to_string(),

            "家庭是每个人最温暖的港湾。No matter how far you go, home is always where the heart is.
             家庭成员之间的支持和关爱是我们克服困难的最大动力——这种力量是无与伦比的。J.K. Rowling 的《Harry Potter》系列中强调了家庭和友谊的力量。".to_string(),
        ];
        let mut schema_builder = Schema::builder();
        schema_builder.add_u64_field("row_id", FAST | INDEXED);
        schema_builder.add_text_field("col1", TEXT);
        let schema = schema_builder.build();

        // Get fields from `schema`.
        let row_id_field = schema.get_field("row_id").unwrap();
        let col1_field = schema.get_field("col1").unwrap();

        for row_id in 0..mocked_docs.len() {
            let mut doc = Document::default();
            doc.add_u64(row_id_field, row_id as u64);
            doc.add_text(col1_field, &mocked_docs[row_id]);
            let result = index_writer_bridge.add_document(doc);
            assert!(result.is_ok());
        }
        assert!(index_writer_bridge.commit().is_ok());
        assert!(load_index_reader(temp_directory_str).is_ok());

        let index_reader_bridge: Arc<IndexReaderBridge> = FFI_INDEX_SEARCHER_CACHE
            .get_index_reader_bridge(temp_directory_str.to_string())
            .unwrap();
        let parser = QueryParser::for_index(&index_writer_bridge.index, vec![col1_field]);
        let top_docs_collector: TopDocsWithFilter = TopDocsWithFilter::with_limit(10)
            .with_searcher(index_reader_bridge.reader.searcher().clone())
            .with_text_fields(vec![col1_field.clone()]);

        // let sentence = "冥想能够对身体健康有帮助吗";
        let sentence = "Elon Musk 是否能够使得世界经济繁荣";
        println!("\n----query en-zh `QueryParser`: {:?}-----", sentence);
        let text_query: Box<dyn Query> = parser.parse_query(sentence).unwrap();
        let result = index_reader_bridge
            .reader
            .searcher()
            .search(&text_query, &top_docs_collector)
            .unwrap();
        println!("searched res count:{:?}", result.len());
        for re in result {
            println!("rowid with score is: {:?}", re);
        }

        println!(
            "\n----query english-zh `BM25StandardQueryStrategy`: {:?}-----",
            sentence
        );
        let bm25_sentence_strategy: BM25StandardQueryStrategy<'_> = BM25StandardQueryStrategy {
            // column_names: &vec!["col1".to_string()],
            sentence,
            column_names: &vec![],
            top_k: &10,
            query_with_filter: &false,
            u8_alive_bitmap: &vec![],
            need_doc: &true,
            operation_or: &true,
        };
        let query_executor: QueryExecutor<'_, Vec<RowIdWithScore>> =
            QueryExecutor::new(&bm25_sentence_strategy);

        // Compute query results.
        let result: Vec<RowIdWithScore> = query_executor
            .execute(&index_reader_bridge.reader.searcher())
            .unwrap();
        println!("searched res count:{:?}", result.len());
        for re in result {
            println!("rowid with score is: {:?}", re);
        }

        println!(
            "\n----query english-zh `BM25NaturalLanguageStrategy`: {:?}-----",
            sentence
        );
        let bm25_nlq_strategy: BM25NaturalLanguageStrategy<'_> = BM25NaturalLanguageStrategy {
            // column_names: &vec!["col1".to_string()],
            sentence,
            column_names: &vec![],
            top_k: &10,
            query_with_filter: &false,
            u8_alive_bitmap: &vec![],
            need_doc: &true,
            operation_or: &true,
        };
        let query_executor: QueryExecutor<'_, Vec<RowIdWithScore>> =
            QueryExecutor::new(&bm25_nlq_strategy);

        // Compute query results.
        let result: Vec<RowIdWithScore> = query_executor
            .execute(&index_reader_bridge.reader.searcher())
            .unwrap();
        println!("searched res count:{:?}", result.len());
        for re in result {
            println!("rowid with score is: {:?}", re);
        }

        println!("num cpus is {:?}", num_cpus::get());
    }

    #[test]
    pub fn test_different_tokenizers() {
        let zh_char = '哈';
        println!("哈 is_alphanumeric {:?}", zh_char.is_alphanumeric());
        println!(
            "{:?}",
            "{\"col1\":{\"tokenizer\":{\"type\":\"simple\", \"case_sensitive\":false}}}"
        );
        test_create_index_with_valid_tokenizer(
            "{\"col1\":{\"tokenizer\":{\"type\":\"simple\", \"case_sensitive\":false}}}",
        );

        // println!("{:?}", "{\"col1\":{\"tokenizer\":{\"type\":\"stem\",\"stop_word_filters\":[\"english\"]}}}");
        // test_create_index_with_valid_tokenizer("{\"col1\":{\"tokenizer\":{\"type\":\"stem\",\"stop_word_filters\":[\"english\"]}}}");

        // println!("{:?}", "{\"col1\":{\"tokenizer\":{\"type\":\"chinese\",\"jieba\":\"default\",\"mode\":\"all\",\"hmm\":false,\"store_doc\":true}}}");
        // test_create_index_with_valid_tokenizer("{\"col1\":{\"tokenizer\":{\"type\":\"chinese\",\"jieba\":\"default\",\"mode\":\"all\",\"hmm\":false,\"store_doc\":true}}}");

        // println!("{:?}", "{\"col1\":{\"tokenizer\":{\"type\":\"chinese\",\"jieba\":\"default\",\"mode\":\"unicode\",\"hmm\":false,\"store_doc\":true}}}");
        // test_create_index_with_valid_tokenizer("{\"col1\":{\"tokenizer\":{\"type\":\"chinese\",\"jieba\":\"default\",\"mode\":\"unicode\",\"hmm\":false,\"store_doc\":true}}}");

        println!("{:?}", "{\"col1\":{\"tokenizer\":{\"type\":\"chinese\",\"jieba\":\"default\",\"mode\":\"search\",\"hmm\":false,\"store_doc\":true,\"case_sensitive\":false}}}");
        test_create_index_with_valid_tokenizer("{\"col1\":{\"tokenizer\":{\"type\":\"chinese\",\"jieba\":\"default\",\"mode\":\"search\",\"hmm\":false,\"store_doc\":true,\"case_sensitive\":false}}}");

        // println!("{:?}", "{\"col1\":{\"tokenizer\":{\"type\":\"chinese\",\"jieba\":\"default\",\"mode\":\"search\",\"hmm\":true,\"store_doc\":true}}}");
        // test_create_index_with_valid_tokenizer("{\"col1\":{\"tokenizer\":{\"type\":\"chinese\",\"jieba\":\"default\",\"mode\":\"search\",\"hmm\":true,\"store_doc\":true}}}");

        // println!("{:?}", "{\"col1\":{\"tokenizer\":{\"type\":\"chinese\",\"jieba\":\"default\",\"mode\":\"default\",\"hmm\":false,\"store_doc\":true}}}");
        // test_create_index_with_valid_tokenizer("{\"col1\":{\"tokenizer\":{\"type\":\"chinese\",\"jieba\":\"default\",\"mode\":\"default\",\"hmm\":false,\"store_doc\":true}}}");

        // println!("{:?}", "{\"col1\":{\"tokenizer\":{\"type\":\"chinese\",\"jieba\":\"default\",\"mode\":\"default\",\"hmm\":true,\"store_doc\":true}}}");
        // test_create_index_with_valid_tokenizer("{\"col1\":{\"tokenizer\":{\"type\":\"chinese\",\"jieba\":\"default\",\"mode\":\"default\",\"hmm\":true,\"store_doc\":true}}}");

        // println!("{:?}", "{\"col1\":{\"tokenizer\":{\"type\":\"chinese\",\"jieba\":\"empty\",\"mode\":\"all\",\"hmm\":false,\"store_doc\":true}}}");
        // test_create_index_with_valid_tokenizer("{\"col1\":{\"tokenizer\":{\"type\":\"chinese\",\"jieba\":\"empty\",\"mode\":\"all\",\"hmm\":false,\"store_doc\":true}}}");

        // println!("{:?}", "{\"col1\":{\"tokenizer\":{\"type\":\"chinese\",\"jieba\":\"empty\",\"mode\":\"unicode\",\"hmm\":false,\"store_doc\":true}}}");
        // test_create_index_with_valid_tokenizer("{\"col1\":{\"tokenizer\":{\"type\":\"chinese\",\"jieba\":\"empty\",\"mode\":\"unicode\",\"hmm\":false,\"store_doc\":true}}}");

        // println!("{:?}", "{\"col1\":{\"tokenizer\":{\"type\":\"chinese\",\"jieba\":\"empty\",\"mode\":\"search\",\"hmm\":false,\"store_doc\":true}}}");
        // test_create_index_with_valid_tokenizer("{\"col1\":{\"tokenizer\":{\"type\":\"chinese\",\"jieba\":\"empty\",\"mode\":\"search\",\"hmm\":false,\"store_doc\":true}}}");

        // println!("{:?}", "{\"col1\":{\"tokenizer\":{\"type\":\"chinese\",\"jieba\":\"empty\",\"mode\":\"search\",\"hmm\":true,\"store_doc\":true}}}");
        // test_create_index_with_valid_tokenizer("{\"col1\":{\"tokenizer\":{\"type\":\"chinese\",\"jieba\":\"empty\",\"mode\":\"search\",\"hmm\":true,\"store_doc\":true}}}");

        // println!("{:?}", "{\"col1\":{\"tokenizer\":{\"type\":\"chinese\",\"jieba\":\"empty\",\"mode\":\"default\",\"hmm\":false,\"store_doc\":true}}}");
        // test_create_index_with_valid_tokenizer("{\"col1\":{\"tokenizer\":{\"type\":\"chinese\",\"jieba\":\"empty\",\"mode\":\"default\",\"hmm\":false,\"store_doc\":true}}}");

        // println!("{:?}", "{\"col1\":{\"tokenizer\":{\"type\":\"chinese\",\"jieba\":\"empty\",\"mode\":\"default\",\"hmm\":true,\"store_doc\":true}}}");
        // test_create_index_with_valid_tokenizer("{\"col1\":{\"tokenizer\":{\"type\":\"chinese\",\"jieba\":\"empty\",\"mode\":\"default\",\"hmm\":true,\"store_doc\":true}}}");
    }
}
