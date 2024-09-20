use crate::common::errors::TantivySearchError;
use crate::ffi::{RowIdWithScore, Statistics};
use crate::search::implements::api_myscale::bm25_inner_search::bm25_inner_search;
use crate::search::implements::strategy::BM25StandardQueryStrategy;

pub fn bm25_standard_search(
    index_path: &str,
    sentence: &str,
    column_names: &Vec<String>,
    top_k: u32,
    u8_alive_bitmap: &Vec<u8>,
    query_with_filter: bool,
    operation_or: bool,
    statistics: &Statistics,
    need_doc: bool,
) -> Result<Vec<RowIdWithScore>, TantivySearchError> {
    // Choose query strategy to construct query executor.
    let bm25_standard_query: BM25StandardQueryStrategy<'_> = BM25StandardQueryStrategy {
        sentence,
        column_names,
        top_k: &top_k,
        query_with_filter: &query_with_filter,
        u8_alive_bitmap,
        need_doc: &need_doc,
        operation_or: &operation_or,
    };

    bm25_inner_search(index_path, statistics, &bm25_standard_query)
}


#[cfg(test)]
mod tests {
    use crate::common::{MultiPartsTest, SinglePartTest};

    #[test]
    fn normal_test_single_part_operation_or() {
        let res = SinglePartTest::single_part_test_helper(
            false,
            "col2:ancient OR (moral horizons)",
            &vec![],
            false,
            true
        );

        assert_eq!(res.len(), 2);
        assert_eq!(res[0].row_id, 0);
        assert_eq!(format!("{:.3}", res[0].score), format!("{:.3}", 2.2181613));
        assert_eq!(res[1].row_id, 4);
        assert_eq!(format!("{:.3}", res[1].score), format!("{:.3}", 2.181346));
    }

    #[test]
    fn normal_test_single_part_operation_and() {
        let res_1 = SinglePartTest::single_part_test_helper(
            false,
            "col3:(ancient rise fall)",  // col3 will be regarded as a simple token.
            &vec![],
            false,
            false
        );
        assert_eq!(res_1.len(), 0);

        let res_2 = SinglePartTest::single_part_test_helper(
            false,
            "ancient rise fall",
            &vec![],
            false,
            false
        );
        assert_eq!(res_2.len(), 1);
        assert_eq!(res_2[0].row_id, 0);
        assert_eq!(format!("{:.3}", res_2[0].score), format!("{:.3}", 3.3516014));
    }

    #[test]
    fn normal_test_single_part_with_filter() {
        let res = SinglePartTest::single_part_test_helper(
            false,
            "col2:(ancient rise fall)",
            &vec![16],
            true,
            true
        );

        assert_eq!(res.len(), 1);
        assert_eq!(res[0].row_id, 4);
        assert_eq!(format!("{:.3}", res[0].score), format!("{:.3}", 0.8952658));
    }

    #[test]
    fn normal_test_single_part_no_filter() {
        let res = SinglePartTest::single_part_test_helper(
            false,
            "col2:(ancient rise fall)",
            &vec![16],
            false,
            true
        );

        assert_eq!(res.len(), 2);
        assert_eq!(res[0].row_id, 0);
        assert_eq!(format!("{:.3}", res[0].score), format!("{:.3}", 3.3516011));
        assert_eq!(res[1].row_id, 4);
        assert_eq!(format!("{:.3}", res[1].score), format!("{:.3}", 0.8952658));
    }

    #[test]
    fn normal_test_multi_parts_no_filter(){
        assert_eq!(MultiPartsTest::multi_parts_test_helper(false, "Ancient provide wisdom modern dilemmas", &vec![], false, true), 6);
        assert_eq!(MultiPartsTest::multi_parts_test_helper(false, "Human health", &vec![], false, false), 2);
    }

    #[test]
    fn normal_test_multi_parts_with_filter(){
        assert_eq!(MultiPartsTest::multi_parts_test_helper(false, "Ancient provide wisdom modern dilemmas", &vec![80], true, true), 2);
        assert_eq!(MultiPartsTest::multi_parts_test_helper(false, "Human health", &vec![16], true, false), 1);
    }

}