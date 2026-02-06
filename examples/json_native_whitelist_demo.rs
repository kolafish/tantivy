use serde_json::json;
use tantivy::collector::{Count, TopDocs};
use tantivy::query::{QueryParser, RangeQuery, TermQuery};
use tantivy::schema::{
    IndexRecordOption, Schema, TextFieldIndexing, TextOptions, FAST, STORED, STRING, TEXT,
};
use tantivy::tokenizer::NgramTokenizer;
use tantivy::{doc, Index, Order, Term};

use std::ops::Bound;

fn main() -> tantivy::Result<()> {
    let mut schema_builder = Schema::builder();
    let data_field = schema_builder.add_json_field("data", STORED | TEXT | FAST);
    let sort_price_field = schema_builder.add_f64_field("sort_price", FAST | STORED);
    let sku_exact_field = schema_builder.add_text_field("sku_exact", STRING | STORED);

    let title_ngram_options = TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer("title_ngram")
            .set_index_option(IndexRecordOption::Basic),
    );
    let title_ngram_field = schema_builder.add_text_field("title_ngram", title_ngram_options);

    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema);

    index
        .tokenizers()
        .register("title_ngram", NgramTokenizer::new(2, 4, false)?);

    let mut writer = index.writer(50_000_000)?;

    writer.add_document(doc!(
        data_field => json!({
            "title": "Hybrid City Bike",
            "category": "bike",
            "sku": "BK-R93R-44",
            "price": 899.0,
            "stock": 8
        }),
        sort_price_field => 899.0f64,
        sku_exact_field => "BK-R93R-44",
        title_ngram_field => "hybrid city bike"
    ))?;

    writer.add_document(doc!(
        data_field => json!({
            "title": "Mountain Bike Pro",
            "category": "bike",
            "sku": "BK-MT-900",
            "price": 1299.0,
            "stock": 0
        }),
        sort_price_field => 1299.0f64,
        sku_exact_field => "BK-MT-900",
        title_ngram_field => "mountain bike pro"
    ))?;

    writer.add_document(doc!(
        data_field => json!({
            "title": "Road Helmet",
            "category": "helmet",
            "sku": "HM-100",
            "price": 129.0,
            "stock": 30
        }),
        sort_price_field => 129.0f64,
        sku_exact_field => "HM-100",
        title_ngram_field => "road helmet"
    ))?;

    writer.commit()?;

    let reader = index.reader()?;
    let searcher = reader.searcher();
    let query_parser = QueryParser::for_index(&index, vec![data_field, sku_exact_field]);

    // 1) Native JSON filtering + range (FAST JSON)
    let in_stock_bikes = query_parser.parse_query("data.category:bike AND data.stock:[1 TO *]")?;
    let in_stock_bike_count = searcher.search(&*in_stock_bikes, &Count)?;
    assert_eq!(in_stock_bike_count, 1);

    // 2) Native JSON range API
    let stock_min = {
        let mut term = Term::from_field_json_path(data_field, "stock", true);
        term.append_type_and_fast_value(1u64);
        term
    };
    let stock_max = {
        let mut term = Term::from_field_json_path(data_field, "stock", true);
        term.append_type_and_fast_value(100u64);
        term
    };
    let stock_range = RangeQuery::new(Bound::Included(stock_min), Bound::Included(stock_max));
    let in_stock_count = searcher.search(&stock_range, &Count)?;
    assert_eq!(in_stock_count, 2);

    // 3) Sort whitelist field
    let bike_query = query_parser.parse_query("data.category:bike")?;
    let sorted_bikes = searcher.search(
        &*bike_query,
        &TopDocs::with_limit(10).order_by_fast_field::<f64>("sort_price", Order::Asc),
    )?;
    assert_eq!(sorted_bikes.len(), 2);
    assert_eq!(sorted_bikes[0].0, 899.0f64);
    assert_eq!(sorted_bikes[1].0, 1299.0f64);

    // 4) Analyzer whitelist fields
    let sku_query = query_parser.parse_query("sku_exact:BK-R93R-44")?;
    let sku_hits = searcher.search(&*sku_query, &Count)?;
    assert_eq!(sku_hits, 1);

    let title_prefix_term = Term::from_field_text(title_ngram_field, "hyb");
    let title_prefix_query = TermQuery::new(title_prefix_term, IndexRecordOption::Basic);
    let title_prefix_hits = searcher.search(&title_prefix_query, &Count)?;
    assert_eq!(title_prefix_hits, 1);

    println!("json_native_whitelist_demo: all checks passed.");
    Ok(())
}
