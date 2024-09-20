use std::collections::HashMap;
use std::iter::zip;
use std::sync::{Arc, Mutex};
use once_cell::sync::Lazy;
use tantivy::{collector::Count, Document, Index, merge_policy::LogMergePolicy, Opstamp, query::QueryParser, ReloadPolicy, schema::{FAST, INDEXED, Schema, TEXT}};
use tantivy::schema::{Field, STORED};
use tempfile::TempDir;
use crate::{FFI_INDEX_WRITER_CACHE, index::bridge::IndexWriterBridge};
use crate::common::constants::FFI_INDEX_SEARCHER_CACHE;
use crate::ffi::{DocWithFreq, FieldTokenNums, RowIdWithScore, Statistics};
use crate::search::bridge::index_reader_bridge::IndexReaderBridge;
use crate::search::implements::{bm25_natural_language_search, bm25_standard_search, get_doc_freq, get_total_num_docs, get_total_num_tokens, load_index_reader};

pub struct SinglePartTest;
#[allow(dead_code)]
pub static TEST_MUTEX: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

impl SinglePartTest {
    #[allow(dead_code)]
    pub(crate) fn get_column_names() -> &'static Vec<String> {
        static COLUMN_NAMES: Lazy<Vec<String>> = Lazy::new(|| {
            vec![
                "col1".to_string(),
                "col2".to_string(),
                "col3".to_string(),
            ]
        });
        return &COLUMN_NAMES;
    }

    #[allow(dead_code)]
    pub(crate) fn get_mocked_docs() -> Vec<Vec<&'static str>> {
        // You can't update existing content, you can only add new columns,
        // and make sure that the new columns are the same length as the other columns
        const ROWS_LIMIT: usize = 5;
        static COL1: [&str; ROWS_LIMIT] = [
            "Ancient empires rise and fall, shaping history's course.",
            "Artistic expressions reflect diverse cultural heritages.",
            "Social movements transform societies, forging new paths.",
            "Strategic military campaigns alter the balance of power.",
            "Ancient philosophies provide wisdom for modern dilemmas.",
        ];
        static COL2: [&str; ROWS_LIMIT] = [
            "Brave explorers venture into uncharted territories, expanding horizons.",
            "Brilliant minds unravel nature's judgment through scientific inquiry.",
            "Economic systems evolve, influencing global trade and prosperity.",
            "Environmental challenges demand innovative solutions for sustainability.",
            "Ethical dilemmas test the boundaries of moral reasoning and Judgment.",
        ];

        static COL3: [&str; ROWS_LIMIT] = [
            "Groundbreaking inventions revolutionize industries and daily life.",
            "Iconic leaders inspire generations with their vision and charisma.",
            "Literary masterpieces capture the essence of the human experience.",
            "Majestic natural wonders showcase the breathtaking beauty of Earth.",
            "Philosophical debates shape our understanding of reality and existence.",
        ];
        return vec![COL1.to_vec(), COL2.to_vec(), COL3.to_vec()];
    }

    #[allow(dead_code)]
    fn create_multi_columns_schema(columns_number: usize) -> Schema {
        let mut schema_builder = Schema::builder();
        schema_builder.add_u64_field("row_id", FAST | INDEXED);
        let col_name = |idx: usize| { format!("col{}", idx + 1) };
        for col_idx in 0..columns_number {
            schema_builder.add_text_field(col_name(col_idx).as_str(), TEXT | STORED);
        }
        let schema = schema_builder.build();
        return schema;
    }

    #[allow(dead_code)]
    fn create_multi_columns_index(index_directory: &str, columns_number: usize) -> Index {
        let schema = SinglePartTest::create_multi_columns_schema(columns_number);
        let index = Index::create_in_dir(index_directory, schema).unwrap();
        index
    }

    #[allow(dead_code)]
    fn store_multi_columns_documents<F>(mut add_document: F, multi_columns_docs: Vec<Vec<&'static str>>)
    where
        F: FnMut(Document) -> Opstamp,
    {
        let min_rows = multi_columns_docs
            .iter()
            .map(|item| item.len())
            .min()
            .unwrap();
        for row_id in 0..min_rows {
            let mut doc = Document::default();
            doc.add_u64(Field::from_field_id(0), row_id as u64);
            for col_idx in 0..multi_columns_docs.len() {
                doc.add_text(Field::from_field_id((col_idx + 1) as u32), &multi_columns_docs[col_idx][row_id]);
            }
            add_document(doc);
        }
    }

    #[allow(dead_code)]
    pub fn create_index_and_get_writer_bridge(index_directory: &str, skip_insert_cache: bool) -> Arc<IndexWriterBridge> {
        let multi_columns_docs = SinglePartTest::get_mocked_docs();
        let index = SinglePartTest::create_multi_columns_index(index_directory, multi_columns_docs.len());
        let writer = index.writer_with_num_threads(2, 1024 * 1024 * 64).unwrap();
        writer.set_merge_policy(Box::new(LogMergePolicy::default()));

        let bridge = Arc::new(IndexWriterBridge {
            path: index_directory.to_string(),
            index,
            writer: Mutex::new(Some(writer)),
        });
        if !skip_insert_cache {
            FFI_INDEX_WRITER_CACHE.set_index_writer_bridge(index_directory.to_string(), bridge.clone()).expect("");
        }
        return bridge;
    }

    #[allow(dead_code)]
    pub fn create_index_and_get_reader_bridge(index_directory: &str, skip_insert_cache: bool) -> Arc<IndexReaderBridge> {
        let writer_bridge = Self::create_index_and_get_writer_bridge(index_directory, false);
        let reader = writer_bridge.index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .expect("Can't set reload policy");
        let bridge = Arc::new(
            IndexReaderBridge {
                path: writer_bridge.path.to_string(),
                index: writer_bridge.index.clone(),
                reader: reader.clone(),
            }
        );
        if !skip_insert_cache {
            FFI_INDEX_SEARCHER_CACHE.set_index_reader_bridge(index_directory.to_string(), bridge.clone()).expect("");
        }
        return bridge;
    }

    #[allow(dead_code)]
    pub fn index_docs_and_get_writer_bridge(
        index_directory: &str, pre_create_index: bool, need_commit: bool, wait_merge_threads: bool,
    ) -> Arc<IndexWriterBridge>
    {
        if pre_create_index {
            Self::create_index_and_get_writer_bridge(index_directory, false);
        }
        let index_writer_bridge = FFI_INDEX_WRITER_CACHE
            .get_index_writer_bridge(index_directory.to_string())
            .unwrap();
        let multi_col_docs = SinglePartTest::get_mocked_docs();
        SinglePartTest::store_multi_columns_documents(|doc| index_writer_bridge.add_document(doc).unwrap(), multi_col_docs);
        if need_commit {
            assert!(index_writer_bridge.commit().is_ok());
        }
        if wait_merge_threads {
            assert!(index_writer_bridge.wait_merging_threads().is_ok());
        }

        index_writer_bridge
    }

    #[allow(dead_code)]
    pub fn index_docs_and_get_reader_bridge(
        index_directory: &str, pre_create_index: bool, need_commit: bool, wait_merge_threads: bool,
    ) -> Arc<IndexReaderBridge>
    {
        let writer_bridge = Self::index_docs_and_get_writer_bridge(index_directory, pre_create_index, need_commit, wait_merge_threads);
        let reader = writer_bridge.index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .expect("Can't set reload policy");
        Arc::new(
            IndexReaderBridge {
                path: writer_bridge.path.to_string(),
                index: writer_bridge.index.clone(),
                reader: reader.clone(),
            }
        )
    }

    #[allow(dead_code)]
    pub fn index_docs_and_get_all_bridge(
        index_directory: &str, pre_create_index: bool, need_commit: bool, wait_merge_threads: bool,
    ) -> (Arc<IndexWriterBridge>, Arc<IndexReaderBridge>)
    {
        let writer_bridge = Self::index_docs_and_get_writer_bridge(index_directory, pre_create_index, need_commit, wait_merge_threads);
        let reader = writer_bridge.index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .expect("Can't set reload policy");

        (
            writer_bridge.clone(),
            Arc::new(
                IndexReaderBridge {
                    path: writer_bridge.path.to_string(),
                    index: writer_bridge.index.clone(),
                    reader: reader.clone(),
                }
            )
        )
    }

    #[allow(dead_code)]
    pub fn search_with_index_writer_bridge(index_writer_bridge: Arc<IndexWriterBridge>) {
        // FixMe: If mocked docs changed, please update this function.
        assert!(SinglePartTest::get_mocked_docs().len() >= 3);

        let parser_col1 = QueryParser::for_index(&index_writer_bridge.index, vec![Field::from_field_id(1)]);
        let parser_col2 = QueryParser::for_index(&index_writer_bridge.index, vec![Field::from_field_id(2)]);
        let parser_col3 = QueryParser::for_index(&index_writer_bridge.index, vec![Field::from_field_id(3)]);
        let parser_all = QueryParser::for_index(
            &index_writer_bridge.index,
            vec![Field::from_field_id(1), Field::from_field_id(2), Field::from_field_id(3)],
        );

        let text_query_in_col1 = parser_col1.parse_query("of").unwrap();
        let text_query_in_col2 = parser_col2.parse_query("of").unwrap();
        let text_query_in_col3 = parser_col3.parse_query("of").unwrap();
        let text_query_in_all = parser_all.parse_query("of").unwrap();

        let searcher = index_writer_bridge.index.reader().unwrap().searcher();
        let count_1 = searcher.search(&text_query_in_col1, &Count).unwrap();
        let count_2 = searcher.search(&text_query_in_col2, &Count).unwrap();
        let count_3 = searcher.search(&text_query_in_col3, &Count).unwrap();
        let count_a = searcher.search(&text_query_in_all, &Count).unwrap();

        assert_eq!(count_1, 1);
        assert_eq!(count_2, 1);
        assert_eq!(count_3, 3);
        assert_eq!(count_a, 3);
    }

    #[allow(dead_code)]
    pub fn single_part_test_helper(
        enable_nlq: bool, query_str: &str, u8_alive_bitmap: &Vec<u8>,
        query_with_filter: bool, operator_or: bool
    ) -> Vec<RowIdWithScore>
    {
        let _guard = TEST_MUTEX.lock().unwrap();
        let tmp_dir = TempDir::new().unwrap();
        let tmp_dir = tmp_dir.path().to_str().unwrap();

        let _ = SinglePartTest::index_docs_and_get_reader_bridge(tmp_dir, true, true, true);
        assert!(load_index_reader(tmp_dir).unwrap());

        let res: Vec<RowIdWithScore>;
        if enable_nlq {
            res = bm25_natural_language_search(
                tmp_dir,
                query_str,
                &vec![],
                100000,
                u8_alive_bitmap,
                query_with_filter,
                operator_or,
                &Statistics::default(),
                true,
            ).unwrap();
        } else {
            res = bm25_standard_search(
                tmp_dir,
                query_str,
                &vec![],
                100000,
                u8_alive_bitmap,
                query_with_filter,
                operator_or,
                &Statistics::default(),
                true,
            ).unwrap();
        }
        for row in res.clone() {
            println!("{:?}", row);
        }
        return res;
    }
}

pub struct MultiPartsTest;

impl MultiPartsTest {
    const COLUMN_NUMBER: usize = 3;
    const SUPPORTED_PART_INDEX: [usize; 2] = [0, 1];
    // Mock data for part0
    #[allow(dead_code)]
    fn get_mocked_docs_for_part0() -> Vec<Vec<&'static str>> {
        static COL1: [&str; 10] = [
            "Ancient empires rise and fall, shaping history's course.",
            "Artistic expressions reflect diverse cultural heritages.",
            "Social movements transform societies, forging new paths.",
            "Strategic military campaigns alter the balance of power.",
            "Ancient philosophies provide wisdom for modern dilemmas.",
            "Revolutionary leaders challenge the status quo, inspiring change.",
            "Architectural wonders stand as testaments to human creativity.",
            "Trade routes expand horizons, connecting distant cultures.",
            "Great thinkers challenge societal norms, advancing human thought.",
            "Historic discoveries uncover lost civilizations and their secrets.",
        ];
        static COL2: [&str; 10] = [
            "Brave explorers venture into uncharted territories, expanding horizons.",
            "Brilliant minds unravel nature's judgment through scientific inquiry.",
            "Economic systems evolve, influencing global trade and prosperity.",
            "Environmental challenges demand innovative solutions for sustainability.",
            "Ethical dilemmas test the boundaries of moral reasoning and judgment.",
            "Technological innovations disrupt industries, creating new markets.",
            "Education reforms empower future generations with knowledge.",
            "Civic movements advocate for justice and equality.",
            "Art and music fuse to express the unspoken language of cultures.",
            "Medicine advances, pushing the boundaries of human health and longevity.",
        ];
        static COL3: [&str; 10] = [
            "Groundbreaking inventions revolutionize industries and daily life.",
            "Iconic leaders inspire generations with their vision and charisma.",
            "Literary masterpieces capture the essence of the human experience.",
            "Majestic natural wonders showcase the breathtaking beauty of Earth.",
            "Philosophical debates shape our understanding of reality and existence.",
            "Scientific breakthroughs offer solutions to global challenges.",
            "Humanitarian efforts alleviate suffering and provide hope.",
            "Sustainable practices protect ecosystems for future generations.",
            "Digital transformation reshapes the way societies function.",
            "Athletic achievements inspire excellence and unity in sports.",
        ];
        return vec![COL1.to_vec(), COL2.to_vec(), COL3.to_vec()];
    }

    // Mock data for part1 (Modified to include more rows)
    #[allow(dead_code)]
    fn get_mocked_docs_for_part1() -> Vec<Vec<&'static str>> {
        static COL1: [&str; 20] = [
            "Technological advancements redefine the future of work and leisure.",
            "Historic treaties shape the geopolitical landscape of nations.",
            "Culinary traditions blend to create unique global cuisines.",
            "Dynamic educational methods reshape learning paradigms.",
            "Vibrant festivals celebrate the rich tapestry of human cultures.",
            "Innovative art forms emerge, blending tradition with modernity.",
            "Migration patterns influence cultural exchanges and societal integration.",
            "Social media revolutionizes communication, fostering global connections.",
            "Climate change advocacy prompts action and policy change.",
            "Entrepreneurial ventures spur economic growth and innovation.",
            "Classical music orchestras innovate with modern compositions.",
            "Digital literacy programs bridge the gap between generations.",
            "Healthcare equity becomes a primary focus in policy development.",
            "Art conservation techniques evolve with new science and technology.",
            "Public transportation upgrades reduce congestion and pollution.",
            "Heritage languages are revitalized through educational programs.",
            "Urban renewal projects transform declining areas into vibrant communities.",
            "Data privacy laws strengthen protection for consumers.",
            "Microfinance institutions support small businesses in developing countries.",
            "Disaster-resistant infrastructure mitigates the effects of extreme weather.",
        ];
        static COL2: [&str; 20] = [
            "Innovators pioneer sustainable energy solutions to combat climate change.",
            "Researchers decode genetic mysteries, unlocking new medical treatments.",
            "Financial markets adapt to emerging technologies and changing economies.",
            "Urban planners design smart cities for increased livability and efficiency.",
            "Human health rights movements advocate for equality and justice worldwide.",
            "Autonomous vehicles transform the transportation industry.",
            "Cybersecurity measures intensify in response to growing threats.",
            "Space exploration reaches new frontiers, aiming for Mars colonization.",
            "Renewable resources gain prominence, reducing reliance on fossil fuels.",
            "Cultural heritage sites receive modern tech for preservation and education.",
            "Agricultural drones improve crop monitoring and management.",
            "Biodiversity research drives conservation efforts worldwide.",
            "E-learning platforms expand access to education across borders.",
            "Mass transit systems innovate with green technology.",
            "Nutrition science advances understanding of diet and health.",
            "Renewable energy projects proliferate, driven by policy and technology.",
            "Social entrepreneurship tackles societal issues with innovative business models.",
            "Virtual museums make art accessible to a global audience.",
            "Water purification technologies address global drinking water shortages.",
            "Wildlife corridors facilitate animal movement and habitat connectivity.",
        ];
        static COL3: [&str; 20] = [
            "Pioneering space missions explore the uncharted realms of the cosmos.",
            "Renowned artists disrupt traditional mediums with digital art.",
            "Global collaborations foster peace and understanding among nations.",
            "Revolutionary sports techniques enhance athlete performance and safety.",
            "Scientific debates highlight the ethical considerations of AI advancements.",
            "Virtual reality revolutionizes training and education sectors.",
            "Oceanic research vessels uncover mysteries of the deep sea.",
            "Archaeological findings rewrite history with new discoveries.",
            "Telehealth becomes integral to modern healthcare systems.",
            "Advancements in robotics automate tasks, improving efficiency and safety.",
            "Augmented reality applications enhance user experiences in various sectors.",
            "Biotechnology firms engineer solutions for environmental and health issues.",
            "Community gardens increase local food production and community engagement.",
            "Drone technology advances impact surveillance, delivery, and entertainment sectors.",
            "Eco-friendly buildings set new standards for sustainable construction.",
            "Futuristic transportation concepts promise speed and sustainability.",
            "Genetic research sheds light on diseases and potential therapies.",
            "Holographic displays revolutionize entertainment and advertising.",
            "Interactive learning tools transform educational experiences.",
            "Job automation trends reshape workforce dynamics and skill demands.",
        ];
        return vec![COL1.to_vec(), COL2.to_vec(), COL3.to_vec()];
    }

    #[allow(dead_code)]
    fn create_index_and_get_writer_bridge(index_directory: &str) -> Arc<IndexWriterBridge> {
        let schema = SinglePartTest::create_multi_columns_schema(MultiPartsTest::COLUMN_NUMBER);
        let index = Index::create_in_dir(index_directory, schema).unwrap();
        let writer = index.writer_with_num_threads(2, 1024 * 1024 * 64).unwrap();
        writer.set_merge_policy(Box::new(LogMergePolicy::default()));
        let bridge = Arc::new(IndexWriterBridge {
            path: index_directory.to_string(),
            index,
            writer: Mutex::new(Some(writer)),
        });
        FFI_INDEX_WRITER_CACHE.set_index_writer_bridge(index_directory.to_string(), bridge.clone()).expect("");
        return bridge;
    }

    #[allow(dead_code)]
    pub fn index_docs_and_get_writer_bridge_for_part(
        index_directory: &str, pre_create_index: bool, need_commit: bool, wait_merge_threads: bool, part_index: usize,
    ) -> Arc<IndexWriterBridge>
    {
        if !MultiPartsTest::SUPPORTED_PART_INDEX.contains(&part_index) {
            panic!("invalid parameter");
        }
        if pre_create_index {
            let _ = MultiPartsTest::create_index_and_get_writer_bridge(index_directory);
        }
        let index_writer_bridge = FFI_INDEX_WRITER_CACHE
            .get_index_writer_bridge(index_directory.to_string())
            .unwrap();
        if part_index == 0 {
            let multi_col_docs = MultiPartsTest::get_mocked_docs_for_part0();
            SinglePartTest::store_multi_columns_documents(|doc| index_writer_bridge.add_document(doc).unwrap(), multi_col_docs);
        } else if part_index == 1 {
            let multi_col_docs = MultiPartsTest::get_mocked_docs_for_part1();
            SinglePartTest::store_multi_columns_documents(|doc| index_writer_bridge.add_document(doc).unwrap(), multi_col_docs);
        }
        if need_commit {
            assert!(index_writer_bridge.commit().is_ok());
        }
        if wait_merge_threads {
            assert!(index_writer_bridge.wait_merging_threads().is_ok());
        }
        index_writer_bridge
    }

    #[allow(dead_code)]
    pub fn index_docs_and_get_statistics_for_part(
        index_directory: &str, query_str: &str, part_index: usize,
    ) -> (Vec<DocWithFreq>, u64, Vec<FieldTokenNums>) {
        if !MultiPartsTest::SUPPORTED_PART_INDEX.contains(&part_index) {
            panic!("invalid parameter");
        }

        let _ = MultiPartsTest::index_docs_and_get_writer_bridge_for_part(
            index_directory, true, true, true, part_index,
        );
        assert!(load_index_reader(index_directory).unwrap());
        let doc_freq = get_doc_freq(index_directory, query_str).expect("");
        let total_num_docs = get_total_num_docs(index_directory).expect("");
        let total_num_tokens = get_total_num_tokens(index_directory).expect("");

        return (doc_freq, total_num_docs, total_num_tokens);
    }

    #[allow(dead_code)]
    pub fn index_docs_and_get_writer_bridge_optimized(
        index_directory: &str, pre_create_index: bool, need_commit: bool, wait_merge_threads: bool,
    ) -> Arc<IndexWriterBridge>
    {
        if pre_create_index {
            let _ = MultiPartsTest::create_index_and_get_writer_bridge(index_directory);
        }
        let index_writer_bridge = FFI_INDEX_WRITER_CACHE
            .get_index_writer_bridge(index_directory.to_string())
            .unwrap();

        let multi_col_docs_part0 = MultiPartsTest::get_mocked_docs_for_part0();
        let multi_col_docs_part1 = MultiPartsTest::get_mocked_docs_for_part1();
        SinglePartTest::store_multi_columns_documents(|doc| index_writer_bridge.add_document(doc).unwrap(), multi_col_docs_part0);
        SinglePartTest::store_multi_columns_documents(|doc| index_writer_bridge.add_document(doc).unwrap(), multi_col_docs_part1);

        if need_commit {
            assert!(index_writer_bridge.commit().is_ok());
        }
        if wait_merge_threads {
            assert!(index_writer_bridge.wait_merging_threads().is_ok());
        }
        index_writer_bridge
    }

    #[allow(dead_code)]
    pub fn merge_doc_freq(doc_freq_vec: Vec<Vec<DocWithFreq>>) -> Vec<DocWithFreq> {
        let mut map: HashMap<(String, u32), DocWithFreq> = HashMap::new();
        for item in doc_freq_vec.into_iter().flatten() {
            let key = (item.term_str.clone(), item.field_id);
            map.entry(key)
                .and_modify(|e| e.doc_freq += item.doc_freq)
                .or_insert(item);
        }
        map.into_values().collect()
    }

    #[allow(dead_code)]
    pub fn merge_total_num_tokens(total_num_tokens_vec: Vec<Vec<FieldTokenNums>>) -> Vec<FieldTokenNums> {
        let mut map: HashMap<u32, FieldTokenNums> = HashMap::new();
        for item in total_num_tokens_vec.into_iter().flatten() {
            let key = item.field_id;
            map.entry(key)
                .and_modify(|e| e.field_total_tokens += item.field_total_tokens)
                .or_insert(item);
        }
        map.into_values().collect()
    }

    #[allow(dead_code)]
    pub fn multi_parts_test_helper(enable_nlq: bool, query_str: &str, u8_alive_bitmap: &Vec<u8>, query_with_filter: bool, operator_or: bool) -> usize {
        static FUNC_NAME:&str = "multi_parts_test_helper";

        let _guard = TEST_MUTEX.lock().unwrap();
        // part-0
        let part_0_dir = TempDir::new().unwrap();
        let part_0_dir = part_0_dir.path().to_str().unwrap();
        // part-1
        let part_1_dir = TempDir::new().unwrap();
        let part_1_dir = part_1_dir.path().to_str().unwrap();
        // part-optimized
        let part_optimized_dir = TempDir::new().unwrap();
        let part_optimized_dir = part_optimized_dir.path().to_str().unwrap();

        // index and get statistics for part-0
        let (doc_freq_0, total_num_docs_0, total_num_tokens_0) =
            MultiPartsTest::index_docs_and_get_statistics_for_part(part_0_dir, query_str, 0);
        // index and get statistics for part-1
        let (doc_freq_1, total_num_docs_1, total_num_tokens_1) =
            MultiPartsTest::index_docs_and_get_statistics_for_part(part_1_dir, query_str, 1);
        // index docs for part-optimized
        let _ = MultiPartsTest::index_docs_and_get_writer_bridge_optimized(part_optimized_dir, true, true, true);
        assert!(load_index_reader(part_optimized_dir).unwrap());
        // Combine statistics
        let combined_doc_freq = MultiPartsTest::merge_doc_freq(vec![doc_freq_0, doc_freq_1]);
        let combined_total_num_tokens = MultiPartsTest::merge_total_num_tokens(vec![total_num_tokens_0, total_num_tokens_1]);
        let combined_total_num_docs = total_num_docs_0 + total_num_docs_1;

        // Search from part-0 with given statistics.
        let part_0_res: Vec<RowIdWithScore>;
        if enable_nlq {
            part_0_res = bm25_natural_language_search(
                part_0_dir, query_str,
                &vec![],
                10000,
                u8_alive_bitmap,
                query_with_filter,
                operator_or,
                &Statistics::new(combined_doc_freq.clone(), combined_total_num_tokens.clone(), combined_total_num_docs),
                true,
            ).unwrap();
        } else {
            part_0_res = bm25_standard_search(
                part_0_dir, query_str,
                &vec![],
                10000,
                u8_alive_bitmap,
                query_with_filter,
                operator_or,
                &Statistics::new(combined_doc_freq.clone(), combined_total_num_tokens.clone(), combined_total_num_docs),
                true,
            ).unwrap();
        }
        println!("[MultiPartsTest::{}]- ❄️- - - - - - part-0 search result, enable_nlq:{}, u8_alive:{:?}, with_filter:{}, operator:{}", FUNC_NAME, enable_nlq, u8_alive_bitmap, query_with_filter, operator_or);
        for row in part_0_res.clone() {
            println!("{:?}", row);
        }

        // Search from part-1 with given statistics.
        #[warn(unused_assignments)]
        let part_1_res: Vec<RowIdWithScore>;
        if enable_nlq {
            part_1_res = bm25_natural_language_search(
                part_1_dir, query_str,
                &vec![],
                10000,
                u8_alive_bitmap,
                query_with_filter,
                operator_or,
                &Statistics::new(combined_doc_freq.clone(), combined_total_num_tokens.clone(), combined_total_num_docs),
                true,
            ).unwrap();
        } else {
            part_1_res = bm25_standard_search(
                part_1_dir, query_str,
                &vec![],
                10000,
                u8_alive_bitmap,
                query_with_filter,
                operator_or,
                &Statistics::new(combined_doc_freq.clone(), combined_total_num_tokens.clone(), combined_total_num_docs),
                true,
            ).unwrap();
        }
        println!("[MultiPartsTest::{}]- ❄️- - - - - - part-1 search result, enable_nlq:{}, u8_alive:{:?}, with_filter:{}, operator:{}", FUNC_NAME, enable_nlq, u8_alive_bitmap, query_with_filter, operator_or);
        for row in part_1_res.clone() {
            println!("{:?}", row);
        }
        // Search from optimized part.
        let optimized_result: Vec<RowIdWithScore>;
        if enable_nlq {
            optimized_result = bm25_natural_language_search(
                part_optimized_dir,
                query_str,
                &vec![],
                10000,
                u8_alive_bitmap,
                query_with_filter,
                operator_or,
                &Statistics::default(),
                true,
            ).unwrap();
        } else {
            optimized_result = bm25_standard_search(
                part_optimized_dir,
                query_str,
                &vec![],
                10000,
                u8_alive_bitmap,
                query_with_filter,
                operator_or,
                &Statistics::default(),
                true,
            ).unwrap();
        }
        println!("[MultiPartsTest::{}]- ❄️- - - - - - part-optimize search result, enable_nlq:{}, u8_alive:{:?}, with_filter:{}, operator:{}", FUNC_NAME, enable_nlq, u8_alive_bitmap, query_with_filter, operator_or);
        for row in optimized_result.clone() {
            println!("{:?}", row);
        }
        // Merge and sort multi parts results.
        let mut parts_result: Vec<RowIdWithScore> = part_0_res.into_iter().chain(part_1_res).collect();
        parts_result.sort();

        assert_eq!(parts_result.len(), optimized_result.len());
        for (left, right) in zip(&parts_result, &optimized_result) {
            assert_eq!(left.row_id, right.row_id);
            assert_eq!(format!("{:.3}", left.score), format!("{:.3}", right.score));

        }
        println!("\n");

        optimized_result.len()
    }
}


