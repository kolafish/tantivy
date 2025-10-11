use tantivy::index::SegmentReader;
use tantivy::query::{Query, EnableScoring, Scorer};
use tantivy::DocAddress;
use tantivy::Searcher;
use std::collections::BinaryHeap;

/// SegmentQueryExecutor 实现 segment 级查询执行，利用时间顺序进行早停
pub struct SegmentQueryExecutor {
    query: Box<dyn Query>,
    timestamp_field: String,
    limit: usize,
}

impl SegmentQueryExecutor {
    /// 创建新的 SegmentQueryExecutor
    pub fn new(query: Box<dyn Query>, timestamp_field: String, limit: usize) -> Self {
        Self {
            query,
            timestamp_field,
            limit,
        }
    }

    /// 在单个 segment 上执行查询，利用时间顺序进行早停
    pub fn execute_on_segment(
        &self,
        searcher: &Searcher,
        segment_reader: &SegmentReader,
        segment_ord: u32,
    ) -> tantivy::Result<Vec<(i64, DocAddress)>> {
        let weight = self.query.weight(EnableScoring::enabled_from_searcher(searcher))?;
        let mut scorer = weight.scorer(segment_reader, 1.0)?;
        
        // 获取时间戳 fast field
        let _timestamp_field = segment_reader.schema().get_field(&self.timestamp_field)
            .map_err(|_| tantivy::TantivyError::SchemaError(format!("Field '{}' not found", self.timestamp_field)))?;
        
        let fast_field = segment_reader.fast_fields().i64(&self.timestamp_field)
            .map_err(|_| tantivy::TantivyError::SchemaError(format!("Fast field '{}' not found", self.timestamp_field)))?
            .first_or_default_col(0i64);
        
        let mut results = Vec::new();
        
        // 收集匹配的文档，由于segment内已按时间排序，可以直接早停
        let mut doc_id = scorer.doc();
        
        while doc_id != tantivy::TERMINATED && results.len() < self.limit {
            let timestamp = fast_field.get_val(doc_id);
            let _score = scorer.score();
            results.push((timestamp, DocAddress::new(segment_ord, doc_id)));
            
            // 移动到下一个文档
            doc_id = scorer.advance();
        }
        
        // 按时间戳降序排序
        results.sort_by_key(|(ts, _)| -ts);
        results.truncate(self.limit);
        
        Ok(results)
    }

    /// 在多个 segments 上执行查询，使用堆合并结果并实现早停优化
    pub fn execute_on_segments(
        &self,
        searcher: &Searcher,
        segment_readers: &[SegmentReader],
    ) -> tantivy::Result<Vec<(i64, DocAddress)>> {
        let mut heap = BinaryHeap::new();
        let mut segment_scorers = Vec::new();
        
        // 为每个 segment 创建 scorer 并获取第一个结果
        for (seg_idx, segment_reader) in segment_readers.iter().enumerate() {
            let weight = self.query.weight(EnableScoring::enabled_from_searcher(searcher))?;
            let scorer = weight.scorer(segment_reader, 1.0)?;
            
            let _timestamp_field = segment_reader.schema().get_field(&self.timestamp_field)
                .map_err(|_| tantivy::TantivyError::SchemaError(format!("Field '{}' not found", self.timestamp_field)))?;
            
            let fast_field = segment_reader.fast_fields().i64(&self.timestamp_field)
                .map_err(|_| tantivy::TantivyError::SchemaError(format!("Fast field '{}' not found", self.timestamp_field)))?
                .first_or_default_col(0i64);
            
            segment_scorers.push((scorer, fast_field));
            
            // 将第一个匹配的文档加入堆（直接使用最大堆，按时间戳降序）
            let doc_id = segment_scorers[seg_idx].0.doc();
            if doc_id != tantivy::TERMINATED {
                let timestamp = segment_scorers[seg_idx].1.get_val(doc_id);
                let doc_addr = DocAddress::new(seg_idx as u32, doc_id);
                heap.push((timestamp, seg_idx, doc_id, doc_addr));
            }
        }
        
        let mut results = Vec::new();
        
        // 使用堆进行合并，实现早停优化
        while let Some((timestamp, seg_idx, _doc_id, doc_addr)) = heap.pop() {
            results.push((timestamp, doc_addr));
            
            // 如果已经收集到足够的结果，可以早停
            if results.len() >= self.limit {
                break;
            }
            
            // 从当前 segment 获取下一个结果
            let next_doc_id = segment_scorers[seg_idx].0.advance();
            if next_doc_id != tantivy::TERMINATED {
                let next_timestamp = segment_scorers[seg_idx].1.get_val(next_doc_id);
                let next_doc_addr = DocAddress::new(seg_idx as u32, next_doc_id);
                heap.push((next_timestamp, seg_idx, next_doc_id, next_doc_addr));
            }
        }
        
        // 由于使用了最大堆，结果已经是按时间戳降序排列的
        // 但为了确保正确性，我们再次排序
        results.sort_by_key(|(ts, _)| -ts);
        results.truncate(self.limit);
        
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tantivy::schema::*;
    use tantivy::query::TermQuery;
    use tantivy::Term;
    use tantivy::Index;

    #[test]
    fn test_segment_query_executor() -> tantivy::Result<()> {
        // 创建测试 schema
        let mut schema_builder = Schema::builder();
        schema_builder.add_i64_field("timestamp", FAST | INDEXED);
        schema_builder.add_text_field("content", TEXT);
        let schema = schema_builder.build();
        
        // 创建测试索引
        let index = Index::create_in_ram(schema.clone());
        let mut writer = index.writer(50_000_000)?;
        
        // 添加测试文档
        writer.add_document(doc!(schema.get_field("timestamp").unwrap() => 1000i64, schema.get_field("content").unwrap() => "test content"))?;
        writer.add_document(doc!(schema.get_field("timestamp").unwrap() => 2000i64, schema.get_field("content").unwrap() => "test content"))?;
        writer.commit()?;
        
        // 创建查询
        let term_query = TermQuery::new(
            Term::from_field_text(schema.get_field("content").unwrap(), "test"),
            IndexRecordOption::Basic,
        );
        
        // 测试查询执行
        let searcher = index.reader()?.searcher();
        let segment_reader = &searcher.segment_readers()[0];
        let executor = SegmentQueryExecutor::new(
            Box::new(term_query),
            "timestamp".to_string(),
            10,
        );
        
        let result = executor.execute_on_segment(&searcher, segment_reader, 0)?;
        
        // 验证结果
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, 2000); // 第一个结果时间戳应该更大
        assert_eq!(result[1].0, 1000);
        
        Ok(())
    }
}
