use crate::common::errors::IndexSearcherError;
use tantivy::Searcher;

pub trait QueryStrategy<T> {
    fn execute(&self, searcher: &Searcher) -> Result<T, IndexSearcherError>;
}

pub struct QueryExecutor<'a, T> {
    strategy: &'a dyn QueryStrategy<T>,
}

impl<'a, T> QueryExecutor<'a, T> {
    // QueryExecutor instance lifetime shouldn't live longer than the fun new parameter `strategy`
    pub fn new(strategy: &'a dyn QueryStrategy<T>) -> Self {
        QueryExecutor { strategy }
    }
    pub fn execute(&self, searcher: &Searcher) -> Result<T, IndexSearcherError> {
        self.strategy.execute(searcher)
    }
}
