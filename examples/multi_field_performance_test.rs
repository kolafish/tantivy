// # Multi-Field Performance Test
//
// This example demonstrates performance testing for multiple filter conditions
// and multi-field sorting scenarios using Tantivy.
//
// Features:
// - Supports both traditional and time-sorted indexing modes
// - Creates text indexes for "severity_text" and "body" fields
// - Creates fast fields for "tenant_id" and "timestamp"
// - Supports multiple query types with filtering and sorting
// - Performance benchmarking with timing measurements
//
// Usage:
//   cargo run --example multi_field_performance_test -- --mode index --index-type traditional
//   cargo run --example multi_field_performance_test -- --mode index --index-type time_sorted
//   cargo run --example multi_field_performance_test -- --mode query --index-type traditional
//   cargo run --example multi_field_performance_test -- --mode query --index-type time_sorted

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::ops::Bound;
use std::path::Path;
use std::time::Instant;

use clap::Parser;
use serde_json::Value as JsonValue;
use tantivy::collector::{TopDocs, Count};
use tantivy::directory::MmapDirectory;
use tantivy::query::{
    BooleanQuery, Occur, RangeQuery, TermQuery,
};
use tantivy::schema::*;
use tantivy::{doc, Index, IndexWriter, Order, ReloadPolicy, Searcher, TantivyDocument};
use tantivy::indexer::IndexWriterOptions;
mod segment_query_executor;
use segment_query_executor::SegmentQueryExecutor;
use log::{info, debug, error};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Mode to run: 'index' for building index, 'query' for running queries, 'compare' for performance comparison
    #[arg(short = 'm', long, default_value = "query")]
    mode: String,
    
    /// Path to the data file
    #[arg(short = 'd', long, default_value = "/Users/jin/Desktop/big-ann-benchmarks/hdfs_log_data/hdfs-logs-multitenants.json")]
    data_path: String,
    
    /// Index directory path (default: ./traditional_index for traditional, ./time_sorted_index for time_sorted)
    #[arg(short = 'i', long)]
    index_path: Option<String>,
    
    /// Number of documents to index (default: 1,000,000)
    #[arg(short = 'n', long, default_value = "1000000")]
    max_docs: usize,

    /// Collect more than limit before secondary sort (for 2-key sort)
    #[arg(long, default_value = "1000")]
    prelimit: usize,

    /// Enable time-sorted indexing for performance comparison
    #[arg(long)]
    time_sorted: bool,

    /// Index type: 'traditional' or 'time_sorted'
    #[arg(long, default_value = "traditional")]
    index_type: String,

    /// Query execution method: 'segment_by_segment' or 'all_segments' (default: 'all_segments')
    #[arg(long, default_value = "all_segments")]
    query_method: String,
}

#[derive(Debug)]
struct LogEntry {
    severity_text: String,
    body: String,
    tenant_id: u64,
    timestamp: i64,
}

fn parse_log_entry(line: &str) -> Option<LogEntry> {
    let json: JsonValue = serde_json::from_str(line).ok()?;
    
    Some(LogEntry {
        severity_text: json["severity_text"].as_str().unwrap_or("").to_string(),
        body: json["body"].as_str().unwrap_or("").to_string(),
        tenant_id: json["tenant_id"].as_u64().unwrap_or(0),
        timestamp: json["timestamp"].as_i64().unwrap_or(0),
    })
}

fn create_schema() -> Schema {
    let mut schema_builder = Schema::builder();
    
    // Text fields for full-text search
    schema_builder.add_text_field("severity_text", TEXT | STORED);
    schema_builder.add_text_field("body", TEXT | STORED);
    
    // Fast fields for filtering and sorting
    schema_builder.add_u64_field("tenant_id", INDEXED | FAST | STORED);
    schema_builder.add_i64_field("timestamp", INDEXED | FAST | STORED);
    
    schema_builder.build()
}

#[derive(Debug, Clone)]
struct QuerySpec {
    severity_text: String,
    body_token: String,
    ts_start: i64,
    ts_end: i64,
    tenant_start: u64,
    tenant_end: u64,
}




// ============================================================================
// 通用辅助函数
// ============================================================================

fn get_default_index_path(index_type: &str) -> String {
    match index_type {
        "traditional" => "./traditional_index".to_string(),
        "time_sorted" => "./time_sorted_index".to_string(),
        _ => "./index".to_string(),
    }
}

fn ensure_index_directory_clean(index_path: &str) -> tantivy::Result<()> {
    if std::path::Path::new(index_path).exists() {
        info!("Index directory already exists, removing: {}", index_path);
        std::fs::remove_dir_all(index_path)?;
        info!("Old index directory removed successfully");
    }
    Ok(())
}

fn create_index_with_schema(index_path: &str) -> tantivy::Result<Index> {
    let schema = create_schema();
    info!("Schema created with {} fields", schema.fields().count());
    
    // 确保索引目录存在且干净
    ensure_index_directory_clean(index_path)?;
    info!("Creating index directory: {}", index_path);
    std::fs::create_dir_all(index_path)?;
    
    let index = Index::create_in_dir(index_path, schema.clone())?;
    info!("Index created successfully");
    Ok(index)
}

fn build_common_query(schema: &Schema, spec: &QuerySpec) -> BooleanQuery {
    let sev_field = schema.get_field("severity_text").unwrap();
    let body_field = schema.get_field("body").unwrap();
    let ts_field = schema.get_field("timestamp").unwrap();
    let ten_field = schema.get_field("tenant_id").unwrap();

    let sev_term = Term::from_field_text(sev_field, &spec.severity_text.to_ascii_lowercase());
    let sev_query = TermQuery::new(sev_term, IndexRecordOption::Basic);

    let body_term = Term::from_field_text(body_field, &spec.body_token.to_ascii_lowercase());
    let body_query = TermQuery::new(body_term, IndexRecordOption::Basic);

    let ts_range = RangeQuery::new(
        Bound::Included(Term::from_field_i64(ts_field, spec.ts_start)),
        Bound::Included(Term::from_field_i64(ts_field, spec.ts_end)),
    );

    let ten_range = RangeQuery::new(
        Bound::Included(Term::from_field_u64(ten_field, spec.tenant_start)),
        Bound::Included(Term::from_field_u64(ten_field, spec.tenant_end)),
    );

    BooleanQuery::new(vec![
        (Occur::Must, Box::new(sev_query)),
        (Occur::Must, Box::new(body_query)),
        (Occur::Must, Box::new(ts_range)),
        (Occur::Must, Box::new(ten_range)),
    ])
}

fn run_common_queries(searcher: &Searcher, schema: &Schema, spec: &QuerySpec) -> tantivy::Result<()> {
    let query = build_common_query(schema, spec);

    // Top-100 查询
    info!("Starting search with TopDocs collector (limit=100, order by timestamp desc)");
    let t0 = Instant::now();
    let collector = TopDocs::with_limit(100).order_by_fast_field("timestamp", Order::Desc);
    let _top_docs: Vec<(i64, tantivy::DocAddress)> = searcher.search(&query, &collector)?;
    let latency_ms = t0.elapsed().as_millis();
    println!("Top-100 latency (timestamp desc): {} ms", latency_ms);

    // Count 查询
    info!("Starting count query with built-in Count collector");
    let t1 = Instant::now();
    let count = searcher.search(&query, &Count)?;
    let count_latency_ms = t1.elapsed().as_millis();
    println!("Count(*) = {} ({} ms)", count, count_latency_ms);
    
    Ok(())
}

fn print_query_results(results: &[(i64, u64, tantivy::DocAddress)], title: &str) {
    println!("\n=== {} (first 20) ===", title);
    for (i, (ts, tenant, addr)) in results.iter().take(20).enumerate() {
        println!("  [{}] timestamp={}, tenant_id={}, addr=({},{})", 
                 i, ts, tenant, addr.segment_ord, addr.doc_id);
    }
    
    if results.len() > 20 {
        println!("\n=== {} (last 20) ===", title);
        let start_idx = results.len().saturating_sub(20);
        for (i, (ts, tenant, addr)) in results.iter().skip(start_idx).enumerate() {
            let actual_idx = start_idx + i;
            println!("  [{}] timestamp={}, tenant_id={}, addr=({},{})", 
                     actual_idx, ts, tenant, addr.segment_ord, addr.doc_id);
        }
    }
    
    println!("Total results: {}", results.len());
}

fn create_default_query_spec() -> QuerySpec {
    QuerySpec { 
        severity_text: "INFO".to_string(), 
        body_token: "blk".to_string(), 
        ts_start: 1441123692, 
        ts_end: 1467287750, 
        tenant_start: 10, 
        tenant_end: 73 
    }
}

fn load_index_and_validate(index_path: &str, index_type: &str) -> tantivy::Result<(Index, Searcher, Schema)> {
    info!("Loading {} index from: {}", index_type, index_path);
    
    let (index, searcher) = load_index(index_path)?;
    let schema = index.schema();
    
    info!("Index loaded successfully");
    info!("Schema: {:?}", schema);
    info!("Number of segments: {}", searcher.segment_readers().len());
    
    // Check if index has any documents
    let total_docs = searcher.num_docs();
    info!("Total documents in index: {}", total_docs);
    
    if total_docs == 0 {
        error!("Index is empty! Please rebuild the index first.");
        eprintln!("Error: Index is empty! Please run with --mode index first to build the index");
        return Err(tantivy::TantivyError::InvalidArgument("Index is empty".to_string()));
    }
    
    Ok((index, searcher, schema))
}

fn run_common_query_tests(searcher: &Searcher, schema: &Schema, spec: &QuerySpec) -> tantivy::Result<()> {
    info!("Query parameters: severity_text='{}', body contains '{}', ts:[{},{}], tenant:[{},{}]",
          spec.severity_text, spec.body_token, spec.ts_start, spec.ts_end, spec.tenant_start, spec.tenant_end);

    // 运行通用查询
    run_common_queries(searcher, schema, spec)?;
    
    Ok(())
}

// ============================================================================
// 传统索引构建和查询
// ============================================================================

fn build_traditional_index(data_path: &str, index_path: &str, max_docs: usize) -> tantivy::Result<()> {
    info!("Building traditional index from {} (max {} documents)", data_path, max_docs);
    
    let index = create_index_with_schema(index_path)?;
    let schema = index.schema();
    
    // 配置IndexWriter选项
    let writer_options = IndexWriterOptions::builder()
        .memory_budget_per_thread(500_000_000) // 500MB per thread
        .num_worker_threads(2) // 2个索引线程
        .num_merge_threads(2) // 2个合并线程
        .build();
    
    info!("Creating IndexWriter with 500MB memory budget per thread");
    let mut index_writer: IndexWriter = index.writer_with_options(writer_options)?;
    
        // 使用默认合并策略
        info!("Using default merge policy for traditional indexing");
    
    info!("Opening data file: {}", data_path);
    let file = File::open(data_path)?;
    let reader = BufReader::new(file);
    
    let mut doc_count = 0;
    let start_time = Instant::now();
    
        // 传统模式：逐行读取并立即写入
        info!("Using traditional indexing mode (no pre-sorting)...");
    for line in reader.lines() {
        if doc_count >= max_docs {
            info!("Reached maximum document limit: {}", max_docs);
            break;
        }
        
        let line = line?;
        if let Some(log_entry) = parse_log_entry(&line) {
            debug!("Parsing document {}: severity={}, tenant_id={}, timestamp={}", 
                   doc_count, log_entry.severity_text, log_entry.tenant_id, log_entry.timestamp);
            
            let doc = doc!(
                schema.get_field("severity_text").unwrap() => log_entry.severity_text,
                schema.get_field("body").unwrap() => log_entry.body,
                schema.get_field("tenant_id").unwrap() => log_entry.tenant_id,
                schema.get_field("timestamp").unwrap() => log_entry.timestamp
            );
            
            index_writer.add_document(doc)?;
            doc_count += 1;
            
            if doc_count % 100_000 == 0 {
                info!("Indexed {} documents...", doc_count);
            }
        } else {
            debug!("Skipping invalid log entry at line {}", doc_count);
        }
    }
    
    // 提交索引
    info!("Committing index...");
    let commit_start = Instant::now();
    index_writer.commit()?;
    let commit_duration = commit_start.elapsed();
    info!("Index commit completed in {:.2}s", commit_duration.as_secs_f64());
    
    // 等待所有合并线程完成
    info!("Waiting for all merge operations to complete...");
    index_writer.wait_merging_threads()?;
    
    // 检查最终的段数量
    let reader_final = index.reader()?;
    let searcher_final = reader_final.searcher();
    let segments_final = searcher_final.segment_readers().len();
    info!("Index has {} segments after indexing and merging", segments_final);
    
    let duration = start_time.elapsed();
    info!("Traditional indexing completed in {:.2}s", duration.as_secs_f64());
    info!("Indexed {} documents", doc_count);
    info!("Index saved to: {}", index_path);
    println!("Traditional indexing completed in {:.2}s", duration.as_secs_f64());
    println!("Indexed {} documents", doc_count);
    println!("Index saved to: {}", index_path);
    
    Ok(())
}

// ============================================================================
// 时间排序索引构建和查询
// ============================================================================

fn build_time_sorted_index(data_path: &str, index_path: &str, max_docs: usize) -> tantivy::Result<()> {
    info!("Building multi-segment time-sorted index from {} (max {} documents)", data_path, max_docs);
    
    let index = create_index_with_schema(index_path)?;
    let schema = index.schema();
    
    info!("Opening data file: {}", data_path);
    let file = File::open(data_path)?;
    let reader = BufReader::new(file);
    
    let mut doc_count = 0;
    let start_time = Instant::now();
    
    // 时间排序模式：先收集所有文档，排序后分批写入
    info!("Loading and parsing all documents for time-sorted indexing...");
    let mut all_docs: Vec<LogEntry> = Vec::with_capacity(max_docs);
    let mut line_count = 0;
    
    for line in reader.lines() {
        if all_docs.len() >= max_docs {
            info!("Reached maximum document limit: {}", max_docs);
            break;
        }
        
        let line = line?;
        line_count += 1;
        if let Some(log_entry) = parse_log_entry(&line) {
            all_docs.push(log_entry);
        } else {
            debug!("Skipping invalid log entry at line {}", line_count);
        }
    }
    
    info!("Parsed {} valid documents from {} lines", all_docs.len(), line_count);
    
    // 按timestamp降序排序
    info!("Sorting {} documents by timestamp (descending) for time-sorted indexing", all_docs.len());
    let sort_start = Instant::now();
    all_docs.sort_by_key(|d| -d.timestamp); // 负号实现降序
    let sort_duration = sort_start.elapsed();
    info!("Document sorting completed in {:.2}s", sort_duration.as_secs_f64());
    
    // 分批创建segment，每100万文档一个segment
    const SEGMENT_SIZE: usize = 1_000_000;
    let mut segment_count = 0;
    let mut total_indexed = 0;
    
    for chunk_start in (0..all_docs.len()).step_by(SEGMENT_SIZE) {
        let chunk_end = std::cmp::min(chunk_start + SEGMENT_SIZE, all_docs.len());
        let chunk_docs = &all_docs[chunk_start..chunk_end];
        
        if chunk_docs.is_empty() {
            break;
        }
        
        segment_count += 1;
        info!("Creating segment {} with {} documents (range: {} to {})", 
              segment_count, chunk_docs.len(), chunk_start, chunk_end - 1);
        
        
        // 为每个segment创建新的IndexWriter
        let mut index_writer = index.writer_with_options(
            tantivy::indexer::IndexWriterOptions::builder()
                .memory_budget_per_thread(500_000_000)
                .num_worker_threads(1)
                .num_merge_threads(1)
                .build()
        )?;
        
        // 写入当前segment的文档
        for (i, log_entry) in chunk_docs.iter().enumerate() {
            debug!("Writing document {} to segment {}: severity={}, tenant_id={}, timestamp={}", 
                   i, segment_count, log_entry.severity_text, log_entry.tenant_id, log_entry.timestamp);
            
            let doc = doc!(
                schema.get_field("severity_text").unwrap() => log_entry.severity_text.clone(),
                schema.get_field("body").unwrap() => log_entry.body.clone(),
                schema.get_field("tenant_id").unwrap() => log_entry.tenant_id,
                schema.get_field("timestamp").unwrap() => log_entry.timestamp
            );
            
            index_writer.add_document(doc)?;
            total_indexed += 1;
        }
        
        // 提交当前segment
        info!("Committing segment {} with {} documents...", segment_count, chunk_docs.len());
        index_writer.commit()?;
        index_writer.wait_merging_threads()?;
        
        info!("Segment {} completed: {} documents indexed", segment_count, chunk_docs.len());
    }
    
    // 检查最终的段数量
    let reader_final = index.reader()?;
    let searcher_final = reader_final.searcher();
    let segments_final = searcher_final.segment_readers().len();
    info!("Index has {} segments after indexing", segments_final);
    
    let duration = start_time.elapsed();
    info!("Multi-segment time-sorted indexing completed in {:.2}s", duration.as_secs_f64());
    info!("Indexed {} documents in {} segments", total_indexed, segment_count);
    info!("Index saved to: {}", index_path);
    println!("Multi-segment time-sorted indexing completed in {:.2}s", duration.as_secs_f64());
    println!("Indexed {} documents in {} segments", total_indexed, segment_count);
    println!("Index saved to: {}", index_path);
    
    Ok(())
}

fn load_index(index_path: &str) -> tantivy::Result<(Index, Searcher)> {
    info!("Opening MmapDirectory from: {}", index_path);
    let directory = MmapDirectory::open(index_path)?;
    
    info!("Opening Index from directory");
    let index = Index::open(directory)?;
    
    info!("Creating IndexReader with OnCommitWithDelay reload policy");
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommitWithDelay)
        .try_into()?;
    
    info!("Creating Searcher from IndexReader");
    let searcher = reader.searcher();
    info!("Index loaded successfully with {} segments", searcher.segment_readers().len());
    Ok((index, searcher))
}




fn run_time_sorted_query_all_segments(
    searcher: &Searcher,
    schema: &Schema,
    spec: &QuerySpec,
    limit: usize,
) -> tantivy::Result<Vec<(i64, u64, tantivy::DocAddress)>> {
    info!("=== Time-Sorted Query Execution (All Segments) ===");
    
    // 构建查询
    let query = build_common_query(schema, spec);
    let executor = SegmentQueryExecutor::new(Box::new(query), "timestamp".to_string(), limit);
    
    // 获取所有segment readers
    let segment_readers = searcher.segment_readers();
    info!("Processing {} segments using execute_on_segments", segment_readers.len());
    
    // 使用 execute_on_segments 在多个segments上执行查询
    let segment_results = executor.execute_on_segments(searcher, &segment_readers)?;
    
    info!("execute_on_segments returned {} results", segment_results.len());
    
    // 转换结果格式并添加其他字段信息
    let mut all_results: Vec<(i64, u64, tantivy::DocAddress)> = Vec::new();
    
    for (timestamp, doc_addr) in segment_results {
        // 获取文档的其他字段值
        let doc = searcher.doc::<TantivyDocument>(doc_addr)?;
        let tenant_id = doc.get_first(schema.get_field("tenant_id")?)
            .and_then(|v| v.as_u64()).unwrap_or(0);
        
        all_results.push((timestamp, tenant_id, doc_addr));
    }
    // info!("before Global early termination: truncated to {} results", all_results.len());

    // 按时间降序排序最终结果
    // all_results.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    
    // // 应用全局早停：如果已经收集到足够的结果，截断到limit
    // if all_results.len() > limit {
    //     all_results.truncate(limit);
    //     info!("Global early termination: truncated to {} results", all_results.len());
    // }
    
    info!("Time-sorted query completed: {} results", all_results.len());
    Ok(all_results)
}

fn run_time_sorted_query_segment_by_segment(
    searcher: &Searcher,
    schema: &Schema,
    spec: &QuerySpec,
    limit: usize,
) -> tantivy::Result<Vec<(i64, u64, tantivy::DocAddress)>> {
    info!("=== Time-Sorted Query Execution (Segment by Segment) ===");
    
    // 构建查询
    let query = build_common_query(schema, spec);
    let executor = SegmentQueryExecutor::new(Box::new(query), "timestamp".to_string(), limit);
    
    let mut all_results: Vec<(i64, u64, tantivy::DocAddress)> = Vec::new();
    
    // 逐个处理每个segment
    for (segment_idx, segment_reader) in searcher.segment_readers().iter().enumerate() {
        info!("Processing segment {} with {} documents", segment_idx, segment_reader.num_docs());
        
        // 使用 SegmentQueryExecutor 在单个segment内执行查询
        let segment_results = executor.execute_on_segment(searcher, segment_reader, segment_idx as u32)?;
        
        if !segment_results.is_empty() {
            info!("Segment {} returned {} results", segment_idx, segment_results.len());
            
            // 转换结果格式并添加其他字段信息
            for (timestamp, doc_addr) in segment_results {
                // 获取文档的其他字段值
                let doc = searcher.doc::<TantivyDocument>(doc_addr)?;
                let tenant_id = doc.get_first(schema.get_field("tenant_id")?)
                    .and_then(|v| v.as_u64()).unwrap_or(0);
                
                all_results.push((timestamp, tenant_id, doc_addr));
            }
        } else {
            info!("Segment {} returned no results", segment_idx);
        }
    }
    
    // 按时间降序排序最终结果
    all_results.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    
    // 应用全局早停：如果已经收集到足够的结果，截断到limit
    if all_results.len() > limit {
        all_results.truncate(limit);
        info!("Global early termination: truncated to {} results", all_results.len());
    }
    
    info!("Time-sorted query completed: {} results", all_results.len());
    Ok(all_results)
}


fn run_traditional_queries(index_path: &str) -> tantivy::Result<()> {
    let (_index, searcher, schema) = load_index_and_validate(index_path, "traditional")?;
    
    // 运行查询规格
    println!("\n=== Running Traditional Queries ===");
    let spec = create_default_query_spec();
    
    // 运行通用查询测试
    run_common_query_tests(&searcher, &schema, &spec)?;
    
    // 传统查询测试 - 使用真正的Top-100查询
    println!("\n--- Traditional Query Test ---");
    let start = Instant::now();
    let query = build_common_query(&schema, &spec);
    let collector = TopDocs::with_limit(100).order_by_fast_field("timestamp", Order::Desc);
    let top_docs: Vec<(i64, tantivy::DocAddress)> = searcher.search(&query, &collector)?;
    
    // 获取文档的完整信息用于显示
    let mut traditional_results: Vec<(i64, u64, tantivy::DocAddress)> = Vec::new();
    for (timestamp, doc_addr) in top_docs {
        let doc = searcher.doc::<TantivyDocument>(doc_addr)?;
        let tenant_id = doc.get_first(schema.get_field("tenant_id")?)
            .and_then(|v| v.as_u64()).unwrap_or(0);
        traditional_results.push((timestamp, tenant_id, doc_addr));
    }
    
    let traditional_time = start.elapsed();
    
    println!("Traditional query: {}ms, {} results", 
             traditional_time.as_millis(), traditional_results.len());
    
    // 打印结果
    print_query_results(&traditional_results, "Query Results");
    
    Ok(())
}

fn run_time_sorted_queries(index_path: &str, query_method: &str) -> tantivy::Result<()> {
    let (_index, searcher, schema) = load_index_and_validate(index_path, "time-sorted")?;
    
    // 运行查询规格
    println!("\n=== Running Time-Sorted Queries (Method: {}) ===", query_method);
    let spec = create_default_query_spec();
    
    // 运行通用查询测试
    run_common_query_tests(&searcher, &schema, &spec)?;
    
    // 时间排序查询测试
    println!("\n--- Time-Sorted Query Test ---");
    let start = Instant::now();
    
    let time_sorted_results = match query_method {
        "all_segments" => {
            println!("Using execute_on_segments method");
            run_time_sorted_query_all_segments(&searcher, &schema, &spec, 100)?
        }
        "segment_by_segment" => {
            println!("Using execute_on_segment method (segment by segment)");
            run_time_sorted_query_segment_by_segment(&searcher, &schema, &spec, 100)?
        }
        _ => {
            eprintln!("Error: Invalid query method '{}'. Use 'all_segments' or 'segment_by_segment'", query_method);
            return Err(tantivy::TantivyError::InvalidArgument(format!("Invalid query method: {}", query_method)));
        }
    };
    
    let time_sorted_time = start.elapsed();
    
    println!("Time-sorted query ({}): {}ms, {} results", 
             query_method, time_sorted_time.as_millis(), time_sorted_results.len());
    
    // 打印结果
    print_query_results(&time_sorted_results, "Query Results");
    
    Ok(())
}


fn main() -> tantivy::Result<()> {
    // Initialize logging
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .init();
    
    let args = Args::parse();
    
    info!("Starting multi-field performance test with mode: {}", args.mode);
    
    // 确定索引路径
    let index_path = args.index_path.unwrap_or_else(|| get_default_index_path(&args.index_type));
    info!("Using index path: {}", index_path);
    
    match args.mode.as_str() {
        "index" => {
            info!("Running in index mode");
            if !Path::new(&args.data_path).exists() {
                error!("Data file not found at {}", args.data_path);
                eprintln!("Error: Data file not found at {}", args.data_path);
                std::process::exit(1);
            }
            
            match args.index_type.as_str() {
                "traditional" => {
                    build_traditional_index(&args.data_path, &index_path, args.max_docs)?;
                }
                "time_sorted" => {
                    build_time_sorted_index(&args.data_path, &index_path, args.max_docs)?;
                }
                _ => {
                    error!("Invalid index type: {}", args.index_type);
                    eprintln!("Error: Invalid index type '{}'. Use 'traditional' or 'time_sorted'", args.index_type);
                    std::process::exit(1);
                }
            }
        }
        "query" => {
            info!("Running in query mode");
            if !Path::new(&index_path).exists() {
                error!("Index directory not found at {}", index_path);
                eprintln!("Error: Index directory not found at {}", index_path);
                eprintln!("Please run with --mode index first to build the index");
                std::process::exit(1);
            }
            
            match args.index_type.as_str() {
                "traditional" => {
                    run_traditional_queries(&index_path)?;
                }
                "time_sorted" => {
                    run_time_sorted_queries(&index_path, &args.query_method)?;
                }
                _ => {
                    error!("Invalid index type: {}", args.index_type);
                    eprintln!("Error: Invalid index type '{}'. Use 'traditional' or 'time_sorted'", args.index_type);
                std::process::exit(1);
            }
            }
        }
        _ => {
            error!("Invalid mode: {}", args.mode);
            eprintln!("Error: Invalid mode '{}'. Use 'index' or 'query'", args.mode);
            std::process::exit(1);
        }
    }
    
    Ok(())
}
