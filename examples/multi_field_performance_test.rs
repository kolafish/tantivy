// # Multi-Field Performance Test
//
// This example demonstrates performance testing for multiple filter conditions
// and multi-field sorting scenarios using Tantivy.
//
// Features:
// - Indexes 1M rows from hdfs-logs-multitenants.json
// - Creates text indexes for "severity_text" and "body" fields
// - Creates fast fields for "tenant_id" and "timestamp"
// - Supports multiple query types with filtering and sorting
// - Performance benchmarking with timing measurements
//
// Usage:
//   cargo run --example multi_field_performance_test -- --mode index
//   cargo run --example multi_field_performance_test -- --mode query

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::ops::Bound;
use std::path::Path;
use std::time::Instant;

use clap::Parser;
use std::collections::HashMap;
use serde_json::Value as JsonValue;
use tantivy::collector::{TopDocs, Collector, SegmentCollector};
use tantivy::index::SegmentReader;
use tantivy::Score;
use tantivy::directory::MmapDirectory;
use tantivy::query::{
    BooleanQuery, Occur, QueryParser, RangeQuery, TermQuery,
};
use tantivy::schema::*;
use tantivy::{doc, Index, IndexWriter, Order, ReloadPolicy, Searcher, TantivyDocument};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Mode to run: 'index' for building index, 'query' for running queries
    #[arg(short = 'm', long, default_value = "query")]
    mode: String,
    
    /// Path to the data file
    #[arg(short = 'd', long, default_value = "/Users/jin/Desktop/big-ann-benchmarks/hdfs_log_data/hdfs-logs-multitenants.json")]
    data_path: String,
    
    /// Index directory path
    #[arg(short = 'i', long, default_value = "./hdfs_logs_index")]
    index_path: String,
    
    /// Number of documents to index (default: 1,000,000)
    #[arg(short = 'n', long, default_value = "1000000")]
    max_docs: usize,

    /// Collect more than limit before secondary sort (for 2-key sort)
    #[arg(long, default_value = "1000")]
    prelimit: usize,
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

fn tokenize_simple(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    for ch in text.chars() {
        if ch.is_alphanumeric() {
            current.push(ch.to_ascii_lowercase());
        } else if !current.is_empty() {
            if current.len() >= 3 { tokens.push(current.clone()); }
            current.clear();
        }
    }
    if !current.is_empty() && current.len() >= 3 { tokens.push(current); }
    tokens
}

fn analyze_data(data_path: &str, max_docs: usize) -> tantivy::Result<Vec<QuerySpec>> {
    println!("Analyzing first {} lines from {}", max_docs, data_path);
    let file = File::open(data_path)?;
    let reader = BufReader::new(file);

    let mut total: usize = 0;
    let mut severity_counts: HashMap<String, usize> = HashMap::new();
    let mut body_token_counts: HashMap<String, usize> = HashMap::new();
    let mut timestamps: Vec<i64> = Vec::with_capacity(max_docs.min(1_000_000));
    let mut tenant_ids: Vec<u64> = Vec::with_capacity(max_docs.min(1_000_000));

    for line in reader.lines() {
        if total >= max_docs { break; }
        let line = match line { Ok(l) => l, Err(_) => continue };
        let json: JsonValue = match serde_json::from_str(&line) { Ok(j) => j, Err(_) => continue };
        let sev = json["severity_text"].as_str().unwrap_or("").to_string();
        let body = json["body"].as_str().unwrap_or("");
        let ts = json["timestamp"].as_i64().unwrap_or(0);
        let ten = json["tenant_id"].as_u64().unwrap_or(0);

        if !sev.is_empty() { *severity_counts.entry(sev).or_default() += 1; }
        for tok in tokenize_simple(body) { *body_token_counts.entry(tok).or_default() += 1; }
        timestamps.push(ts);
        tenant_ids.push(ten);
        total += 1;
    }

    if total == 0 { return Ok(vec![]); }

    timestamps.sort_unstable();
    tenant_ids.sort_unstable();

    let pct = |v: &Vec<i64>, p: f64| -> i64 {
        let idx = ((v.len() as f64 - 1.0) * p).round().clamp(0.0, (v.len() - 1) as f64) as usize;
        v[idx]
    };
    let pct_u = |v: &Vec<u64>, p: f64| -> u64 {
        let idx = ((v.len() as f64 - 1.0) * p).round().clamp(0.0, (v.len() - 1) as f64) as usize;
        v[idx]
    };

    // pick common sev values
    let mut sev_sorted: Vec<(String, usize)> = severity_counts.into_iter().collect();
    sev_sorted.sort_by_key(|(_, c)| std::cmp::Reverse(*c));
    let sev_top: Vec<String> = sev_sorted.iter().take(3).map(|(s, _)| s.clone()).collect();
    let sev_fallback = sev_top.get(0).cloned().unwrap_or_else(|| "info".to_string());

    // pick common body tokens
    let mut body_sorted: Vec<(String, usize)> = body_token_counts.into_iter().collect();
    body_sorted.sort_by_key(|(_, c)| std::cmp::Reverse(*c));
    let body_top: Vec<String> = body_sorted.iter().take(50).map(|(t, _)| t.clone()).collect();
    let body_fallback = body_top.get(0).cloned().unwrap_or_else(|| "info".to_string());

    // build 4 specs targeting rough selectivities by timestamp/tenant ranges width
    let ts_ranges = vec![
        (pct(&timestamps, 0.50), pct(&timestamps, 1.00)), // ~50%
        (pct(&timestamps, 0.80), pct(&timestamps, 1.00)), // ~20%
        (pct(&timestamps, 0.95), pct(&timestamps, 1.00)), // ~5%
        (pct(&timestamps, 0.995), pct(&timestamps, 1.00)), // ~0.5%
    ];
    let ten_ranges = vec![
        (pct_u(&tenant_ids, 0.00), pct_u(&tenant_ids, 1.00)), // ~100%
        (pct_u(&tenant_ids, 0.20), pct_u(&tenant_ids, 0.80)), // ~60%
        (pct_u(&tenant_ids, 0.40), pct_u(&tenant_ids, 0.60)), // ~20%
        (pct_u(&tenant_ids, 0.49), pct_u(&tenant_ids, 0.51)), // ~2%
    ];

    let mut specs = Vec::new();
    for i in 0..4 {
        specs.push(QuerySpec {
            severity_text: sev_top.get(i).cloned().unwrap_or_else(|| sev_fallback.clone()),
            body_token: body_top.get(i * 2).cloned().unwrap_or_else(|| body_fallback.clone()),
            ts_start: ts_ranges[i].0,
            ts_end: ts_ranges[i].1,
            tenant_start: ten_ranges[i].0,
            tenant_end: ten_ranges[i].1,
        });
    }

    println!("Analysis complete over {} docs.", total);
    println!("Suggested query specs:");
    for (i, s) in specs.iter().enumerate() {
        println!(
            "Q{}: severity_text='{}', body contains '{}', ts:[{},{}], tenant:[{},{}]",
            i + 1,
            s.severity_text,
            s.body_token,
            s.ts_start,
            s.ts_end,
            s.tenant_start,
            s.tenant_end
        );
    }
    Ok(specs)
}

// Custom count collector modeled after examples/custom_collector.rs
#[derive(Default, Clone, Copy)]
struct HitCount(usize);

struct CountCollector;

impl Collector for CountCollector {
    type Fruit = HitCount;
    type Child = CountSegmentCollector;

    fn for_segment(
        &self,
        _segment_local_id: u32,
        _segment_reader: &SegmentReader,
    ) -> tantivy::Result<CountSegmentCollector> {
        Ok(CountSegmentCollector { count: 0 })
    }

    fn requires_scoring(&self) -> bool { false }

    fn merge_fruits(&self, segment_counts: Vec<HitCount>) -> tantivy::Result<HitCount> {
        let mut total = 0usize;
        for sc in segment_counts { total += sc.0; }
        Ok(HitCount(total))
    }
}

struct CountSegmentCollector { count: usize }

impl SegmentCollector for CountSegmentCollector {
    type Fruit = HitCount;

    fn collect(&mut self, _doc: u32, _score: Score) { self.count += 1; }

    fn harvest(self) -> HitCount { HitCount(self.count) }
}

fn run_spec_query(
    searcher: &Searcher,
    schema: &Schema,
    spec: &QuerySpec,
    prelimit: usize,
) -> tantivy::Result<Vec<(i64, u64, tantivy::DocAddress)>> {
    // Fields
    let sev_field = schema.get_field("severity_text").unwrap();
    let body_field = schema.get_field("body").unwrap();
    let ts_field = schema.get_field("timestamp").unwrap();
    let ten_field = schema.get_field("tenant_id").unwrap();

    // Build MUST term for severity (lowercased for default analyzer)
    let sev_term = Term::from_field_text(sev_field, &spec.severity_text.to_ascii_lowercase());
    let sev_query = TermQuery::new(sev_term, IndexRecordOption::Basic);

    // Build MUST term for body token
    let body_term = Term::from_field_text(body_field, &spec.body_token.to_ascii_lowercase());
    let body_query = TermQuery::new(body_term, IndexRecordOption::Basic);

    // Timestamp range
    let ts_range = RangeQuery::new(
        Bound::Included(Term::from_field_i64(ts_field, spec.ts_start)),
        Bound::Included(Term::from_field_i64(ts_field, spec.ts_end)),
    );

    // Tenant range
    let ten_range = RangeQuery::new(
        Bound::Included(Term::from_field_u64(ten_field, spec.tenant_start)),
        Bound::Included(Term::from_field_u64(ten_field, spec.tenant_end)),
    );

    let query = BooleanQuery::new(vec![
        (Occur::Must, Box::new(sev_query)),
        (Occur::Must, Box::new(body_query)),
        (Occur::Must, Box::new(ts_range)),
        (Occur::Must, Box::new(ten_range)),
    ]);

    // Collect with primary sort by timestamp desc
    let prim: Vec<(i64, tantivy::DocAddress)> = searcher.search(
        &query,
        &TopDocs::with_limit(prelimit).order_by_fast_field("timestamp", Order::Desc),
    )?;

    // Secondary sort by tenant_id asc; materialize rows
    let mut rows: Vec<(i64, u64, tantivy::DocAddress)> = Vec::with_capacity(prim.len());
    for (ts, addr) in prim.into_iter() {
        let doc: TantivyDocument = searcher.doc(addr)?;
        let ts_v = doc.get_first(ts_field).and_then(|v| v.as_i64()).unwrap_or(ts);
        let ten_v = doc.get_first(ten_field).and_then(|v| v.as_u64()).unwrap_or(0);
        rows.push((ts_v, ten_v, addr));
    }
    rows.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    Ok(rows)
}

fn build_index(data_path: &str, index_path: &str, max_docs: usize) -> tantivy::Result<()> {
    println!("Building index from {} (max {} documents)", data_path, max_docs);
    
    let schema = create_schema();
    // Ensure index directory exists
    std::fs::create_dir_all(index_path)?;
    let index = Index::create_in_dir(index_path, schema.clone())?;
    
    let mut index_writer: IndexWriter = index.writer(100_000_000)?; // 100MB memory budget
    
    let file = File::open(data_path)?;
    let reader = BufReader::new(file);
    
    let mut doc_count = 0;
    let start_time = Instant::now();
    
    for line in reader.lines() {
        if doc_count >= max_docs {
            break;
        }
        
        let line = line?;
        if let Some(log_entry) = parse_log_entry(&line) {
            let doc = doc!(
                schema.get_field("severity_text").unwrap() => log_entry.severity_text,
                schema.get_field("body").unwrap() => log_entry.body,
                schema.get_field("tenant_id").unwrap() => log_entry.tenant_id,
                schema.get_field("timestamp").unwrap() => log_entry.timestamp
            );
            
            index_writer.add_document(doc)?;
            doc_count += 1;
            
            if doc_count % 100_000 == 0 {
                println!("Indexed {} documents...", doc_count);
            }
        }
    }
    
    println!("Committing index...");
    index_writer.commit()?;
    
    let duration = start_time.elapsed();
    println!("Indexing completed in {:.2}s", duration.as_secs_f64());
    println!("Indexed {} documents", doc_count);
    println!("Index saved to: {}", index_path);
    
    Ok(())
}

fn load_index(index_path: &str) -> tantivy::Result<(Index, Searcher)> {
    let directory = MmapDirectory::open(index_path)?;
    let index = Index::open(directory)?;
    
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommitWithDelay)
        .try_into()?;
    
    let searcher = reader.searcher();
    Ok((index, searcher))
}


fn run_queries(index_path: &str) -> tantivy::Result<()> {
    println!("Loading index from: {}", index_path);
    
    let (index, searcher) = load_index(index_path)?;
    let schema = index.schema();
    
    println!("Index loaded successfully");
    println!("Schema: {:?}", schema);
    
    // Run only the four analyzed query specs
    println!("\n=== Running query specs ===");
    let specs = vec![
        QuerySpec { severity_text: "INFO".to_string(), body_token: "blk".to_string(), ts_start: 1441123692, ts_end: 1467287851, tenant_start: 1, tenant_end: 100 },
        QuerySpec { severity_text: "WARN".to_string(), body_token: "108841162".to_string(), ts_start: 1441170628, ts_end: 1467287851, tenant_start: 20, tenant_end: 81 },
        QuerySpec { severity_text: "INFO".to_string(), body_token: "dest".to_string(), ts_start: 1461506660, ts_end: 1467287851, tenant_start: 40, tenant_end: 60 },
        QuerySpec { severity_text: "INFO".to_string(), body_token: "hdfs".to_string(), ts_start: 1462238950, ts_end: 1467287851, tenant_start: 49, tenant_end: 51 },
    ];
    for (i, spec) in specs.iter().enumerate() {
        println!("\n--- Query Spec {} ---", i + 1);
        println!(
            "severity_text='{}', body contains '{}', ts:[{},{}], tenant:[{},{}]",
            spec.severity_text, spec.body_token, spec.ts_start, spec.ts_end, spec.tenant_start, spec.tenant_end
        );

        // Build the same query as run_spec_query
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
        let query = BooleanQuery::new(vec![
            (Occur::Must, Box::new(sev_query)),
            (Occur::Must, Box::new(body_query)),
            (Occur::Must, Box::new(ts_range)),
            (Occur::Must, Box::new(ten_range)),
        ]);

        // Measure latency for top-100 using fast field sort by timestamp desc,
        // and apply secondary tie-break by tenant_id asc only for docs with equal timestamp at the cutoff.
        let t0 = Instant::now();
        let mut collected: Vec<(i64, tantivy::DocAddress)> = Vec::new();
        let mut offset = 0;
        let batch = 256; // small batches; still using fast field order
        let mut cutoff_ts: Option<i64> = None;
        loop {
            let batch_docs: Vec<(i64, tantivy::DocAddress)> = searcher.search(
                &query,
                &TopDocs::with_limit(batch).and_offset(offset).order_by_fast_field("timestamp", Order::Desc),
            )?;
            if batch_docs.is_empty() { break; }
            collected.extend_from_slice(&batch_docs);
            if collected.len() >= 100 {
                cutoff_ts = Some(collected[99].0);
            }
            // Stop when we have at least 100 and the last doc has timestamp strictly less than cutoff
            if let Some(cut) = cutoff_ts {
                let last_ts = collected.last().map(|x| x.0).unwrap_or(cut);
                if last_ts < cut { break; }
            }
            offset += batch;
            if collected.len() > 5000 { break; } // safety cap
        }
        // Separate docs >= cutoff_ts, then tie-break equal timestamps by tenant_id asc, and take 100
        let cut = cutoff_ts.unwrap_or_else(|| collected.last().map(|x| x.0).unwrap_or(i64::MIN));
        let mut top_slice: Vec<(i64, u64)> = Vec::new();
        for (ts, addr) in collected.into_iter() {
            if ts < cut { break; }
            let doc: TantivyDocument = searcher.doc(addr)?;
            let ten_v = doc.get_first(ten_field).and_then(|v| v.as_u64()).unwrap_or(0);
            top_slice.push((ts, ten_v));
        }
        top_slice.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
        let _top100: Vec<(i64, u64)> = top_slice.into_iter().take(100).collect();
        let latency_ms = t0.elapsed().as_millis();
        println!("Top-100 latency (timestamp desc, tenant_id asc tie-break): {} ms", latency_ms);

        // Count(*) with custom collector
        let t1 = Instant::now();
        let count = searcher.search(&query, &CountCollector)?;
        let count_latency_ms = t1.elapsed().as_millis();
        println!("Count(*) = {} ({} ms)", count.0, count_latency_ms);
    }
    
    println!("\n=== Performance Summary ===");
    println!("All queries completed successfully!");
    
    Ok(())
}

fn main() -> tantivy::Result<()> {
    let args = Args::parse();
    
    match args.mode.as_str() {
        "analyze" => {
            let _specs = analyze_data(&args.data_path, args.max_docs)?;
        }
        "index" => {
            if !Path::new(&args.data_path).exists() {
                eprintln!("Error: Data file not found at {}", args.data_path);
                std::process::exit(1);
            }
            build_index(&args.data_path, &args.index_path, args.max_docs)?;
        }
        "query" => {
            if !Path::new(&args.index_path).exists() {
                eprintln!("Error: Index directory not found at {}", args.index_path);
                eprintln!("Please run with --mode index first to build the index");
                std::process::exit(1);
            }
            run_queries(&args.index_path)?;
        }
        _ => {
            eprintln!("Error: Invalid mode '{}'. Use 'index' or 'query'", args.mode);
            std::process::exit(1);
        }
    }
    
    Ok(())
}
