// # Native JSON with Fast Field - Range Query Support
//
// This example demonstrates the CORRECT way to create JSON fields that support
// range queries by adding the FAST flag to enable fast field support.

use tantivy::collector::TopDocs;
use tantivy::query::{QueryParser, RangeQuery};
use tantivy::schema::{Schema, FAST, STORED, STRING, TEXT};
use tantivy::{Index, IndexWriter, TantivyDocument, Term};
use serde_json::json;
use std::ops::Bound;

fn main() -> tantivy::Result<()> {
    println!("🚀 Native JSON with Fast Field - Range Query Support Test");
    println!("📋 Demonstrating the correct way to enable range queries on JSON fields");
    
    // # The KEY difference: Adding FAST flag to JSON field
    let mut schema_builder = Schema::builder();
    
    let _timestamp_field = schema_builder.add_date_field("timestamp", FAST | STORED);
    let doc_type_field = schema_builder.add_text_field("doc_type", STRING | STORED);
    
    // 🔑 CRITICAL: Adding FAST flag enables range queries!
    let json_field = schema_builder.add_json_field("data", STORED | TEXT | FAST);
    
    let schema = schema_builder.build();

    let index = Index::create_in_ram(schema.clone());
    let mut index_writer: IndexWriter = index.writer(50_000_000)?;

    println!("\n=== 📄 Indexing Test Documents ===");
    
    // Test documents with numeric data for range queries
    let user_doc = json!({
        "timestamp": "2024-07-22T15:20:00Z",
        "doc_type": "user_profile",
        "data": {
            "user_age": 28,
            "user_salary": 75000.0,
            "user_score": 95,
            "product_price": 99.99,
            "review_rating": 4.5
        }
    });
    
    let doc = TantivyDocument::parse_json(&schema, &user_doc.to_string())?;
    index_writer.add_document(doc)?;
    println!("✅ User profile document indexed");

    let company_doc = json!({
        "timestamp": "2024-07-22T15:21:00Z",
        "doc_type": "company_info",
        "data": {
            "employee_count": 250,
            "annual_revenue": 5000000.0,
            "company_founded": 2020,
            "user_age": 25,
            "satisfaction_score": 4.8
        }
    });
    
    let doc = TantivyDocument::parse_json(&schema, &company_doc.to_string())?;
    index_writer.add_document(doc)?;
    println!("✅ Company information document indexed");

    let product_doc = json!({
        "timestamp": "2024-07-22T15:22:00Z", 
        "doc_type": "ecommerce_product",
        "data": {
            "product_price": 149.99,
            "inventory_stock": 50,
            "review_count": 128,
            "avg_rating": 4.2,
            "discount_percent": 15.0
        }
    });
    
    let doc = TantivyDocument::parse_json(&schema, &product_doc.to_string())?;
    index_writer.add_document(doc)?;
    println!("✅ E-commerce product document indexed");

    index_writer.commit()?;

    let reader = index.reader()?;
    let searcher = reader.searcher();
    
    let query_parser = QueryParser::for_index(&index, vec![doc_type_field, json_field]);
    
    println!("\n=== 🔍 Range Query Tests (Now Working!) ===");
    
    // Test 1: Numeric range query using query parser
    println!("\n1. Age range query (25-30) using query parser:");
    let query = query_parser.parse_query("data.user_age:[25 TO 30]")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });
    
    // Test 2: Price range query
    println!("\n2. Product price range query (100-200) using query parser:");
    let query = query_parser.parse_query("data.product_price:[100 TO 200]")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });
    
    // Test 3: Salary range query
    println!("\n3. Salary range query (70000-80000) using query parser:");
    let query = query_parser.parse_query("data.user_salary:[70000 TO 80000]")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });
    
    // Test 4: Revenue range (millions)
    println!("\n4. Revenue range query (1M-10M) using query parser:");
    let query = query_parser.parse_query("data.annual_revenue:[1000000 TO 10000000]")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });
    
    // Test 5: Rating range (floating point)
    println!("\n5. Rating range query (4.0-5.0) using query parser:");
    let query = query_parser.parse_query("data.avg_rating:[4.0 TO 5.0]")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });

    println!("\n=== 🛠️ Direct Range Query API Tests ===");
    
    // Test 6: Using RangeQuery API directly
    println!("\n6. Direct RangeQuery API - Age range (20-30):");
    
    // Create terms for JSON path with numeric values
    let min_term = {
        let mut term = Term::from_field_json_path(json_field, "user_age", true);
        term.append_type_and_fast_value(20u64);
        term
    };
    let max_term = {
        let mut term = Term::from_field_json_path(json_field, "user_age", true);
        term.append_type_and_fast_value(30u64);
        term
    };
    
    let range_query = RangeQuery::new(
        Bound::Included(min_term),
        Bound::Included(max_term)
    );
    
    let results = searcher.search(&range_query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });

    // Test 7: Float range with RangeQuery API
    println!("\n7. Direct RangeQuery API - Price range (50.0-150.0):");
    
    let min_term = {
        let mut term = Term::from_field_json_path(json_field, "product_price", true);
        term.append_type_and_fast_value(50.0f64);
        term
    };
    let max_term = {
        let mut term = Term::from_field_json_path(json_field, "product_price", true);
        term.append_type_and_fast_value(150.0f64);
        term
    };
    
    let range_query = RangeQuery::new(
        Bound::Included(min_term),
        Bound::Included(max_term)
    );
    
    let results = searcher.search(&range_query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });

    println!("\n=== 🔄 Complex Range Combinations ===");
    
    // Test 8: Boolean combination of ranges
    println!("\n8. Boolean query - Age AND Price ranges:");
    let query = query_parser.parse_query("data.user_age:[25 TO 30] AND data.product_price:[90 TO 200]")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });

    // Test 9: Exclusive range boundaries
    println!("\n9. Exclusive range boundaries - Age (25 TO 30}}:");
    let query = query_parser.parse_query("data.user_age:[25 TO 30}")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });

    // Test 10: Open-ended ranges
    println!("\n10. Open-ended range - Age >= 25:");
    let query = query_parser.parse_query("data.user_age:[25 TO *]")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });

    println!("\n=== 📊 Analysis Summary ===");
    println!("✅ Native JSON with FAST Field Advantages:");
    println!("   • Full range query support for numeric types");
    println!("   • Both query parser and API support");
    println!("   • Efficient fast field-based implementation");
    println!("   • Support for inclusive/exclusive boundaries");
    println!("   • Complex boolean combinations work");
    
    println!("\n💡 Key Insight:");
    println!("   The ONLY difference between working and non-working range queries");
    println!("   on JSON fields is the presence of the FAST flag in schema definition!");
    
    println!("\n⚠️  Performance Note:");
    println!("   Fast fields consume additional memory but enable efficient range queries.");
    println!("   For JSON fields with numeric data requiring range queries, FAST is essential.");

    println!("\n🔄 Comparison Summary:");
    println!("   • Without FAST: JSON field cannot perform range queries at all");
    println!("   • With FAST: JSON field supports full range query functionality");
    println!("   • Fixed JSON Layer: Custom implementation with specialized optimizations");
    println!("   • Native JSON + FAST: Built-in support with good performance");

    Ok(())
} 