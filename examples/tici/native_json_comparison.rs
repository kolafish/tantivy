// # Native JSON Field Comparison Example
//
// This example demonstrates using tantivy's native JSON field capabilities
// to handle the same test cases as fixed_json_layer.rs, comparing performance
// and functionality with the custom JSON processing layer.

use tantivy::collector::TopDocs;
use tantivy::query::{QueryParser, RangeQuery};

use tantivy::schema::{Schema, FAST, STORED, STRING, TEXT};
use tantivy::{Index, IndexWriter, TantivyDocument, Term};

use serde_json::json;
use std::ops::Bound;


fn main() -> tantivy::Result<()> {
    println!("🚀 Native JSON Field Comparison Test");
    println!("📋 Testing tantivy's native JSON capabilities with complex data");
    
    // # Defining the schema with native JSON field
    let mut schema_builder = Schema::builder();
    
    // Add a timestamp field for general date queries
    let _timestamp_field = schema_builder.add_date_field("timestamp", FAST | STORED);
    
    // Add a document type field for categorization
    let doc_type_field = schema_builder.add_text_field("doc_type", STRING | STORED);
    
    // Add the main JSON field that will store all our complex data
    let json_field = schema_builder.add_json_field("data", STORED | TEXT |FAST);
    
    let schema = schema_builder.build();

    // # Create index
    let index = Index::create_in_ram(schema.clone());
    let mut index_writer: IndexWriter = index.writer(50_000_000)?;

    // # Test documents - same data as fixed_json_layer.rs but structured for native JSON
    println!("\n=== 📄 Indexing Test Documents ===");
    
    // Document 1: User Profile
    let user_doc = json!({
        "timestamp": "2024-07-22T15:20:00Z",
        "doc_type": "user_profile",
        "data": {
            "user_name": "Alice Smith",
            "user_email": "alice@example.com", 
            "user_age": 28,
            "user_tags": ["rust", "search", "database"],
            "user_scores": [95, 87, 92],
            "user_languages": ["english", "chinese"],
            "user_created_at": "2024-01-15T10:30:00Z",
            "user_last_login": "2024-07-20T14:25:00Z",
            "product_id": "PROD123456",
            "product_price": 99.99,
            "product_active": true,
            "product_description": "A high-quality search engine library for Rust applications",
            "product_release_date": "2024-03-10"
        }
    });
    
    let doc = TantivyDocument::parse_json(&schema, &user_doc.to_string())?;
    index_writer.add_document(doc)?;
    println!("✅ User profile document indexed");

    // Document 2: Company Information  
    let company_doc = json!({
        "timestamp": "2024-07-22T15:21:00Z",
        "doc_type": "company_info",
        "data": {
            "company_name": "Tech Innovations Inc",
            "company_email": "contact@techinnovations.com",
            "company_founded": 2020,
            "company_categories": ["software", "AI", "machine learning"],
            "company_ratings": [4.8, 4.5, 4.9],
            "company_technologies": ["python", "rust", "tensorflow"],
            "company_country": "USA", 
            "company_city": "San Francisco",
            "company_coordinates": [37.7749, -122.4194],
            "company_established_date": "2020-01-15T09:00:00Z",
            "company_last_updated": "2024-07-22T16:45:00Z",
            "user_age": 25
        }
    });
    
    let doc = TantivyDocument::parse_json(&schema, &company_doc.to_string())?;
    index_writer.add_document(doc)?;
    println!("✅ Company information document indexed");

    // Document 3: E-commerce Product
    let product_doc = json!({
        "timestamp": "2024-07-22T15:22:00Z", 
        "doc_type": "ecommerce_product",
        "data": {
            "product_name": "Wireless Headphones",
            "product_sku": "WH001234",
            "product_price": 149.99,
            "review_ratings": [5, 4, 5, 3, 4],
            "review_comments": ["excellent quality", "great sound", "comfortable"],
            "review_verified": [true, true, false, true, true],
            "inventory_stock": 50,
            "inventory_colors": ["black", "white", "blue"],
            "inventory_availability": true,
            "product_launch_date": "2024-02-14",
            "test_wrong": 25,
            "inventory_last_updated": "2024-07-21T08:30:00Z"
        }
    });
    
    let doc = TantivyDocument::parse_json(&schema, &product_doc.to_string())?;
    index_writer.add_document(doc)?;
    println!("✅ E-commerce product document indexed");

    // Document 4: Academic Paper
    let paper_doc = json!({
        "timestamp": "2024-07-22T15:23:00Z",
        "doc_type": "academic_paper", 
        "data": {
            "paper_title": "Advanced Information Retrieval Systems",
            "paper_authors": ["Dr. John Smith", "Prof. Jane Doe"],
            "paper_year": 2023,
            "test_wrong": 149.99,
            "product_price": 19.99,
            "metrics_citations": 42,
            "metrics_downloads": [120, 95, 87, 76],
            "metrics_impact_factor": 2.8,
            "research_keywords": ["information retrieval", "search engines", "natural language processing"],
            "paper_published_date": "2023-05-20T12:00:00Z",
            "paper_submitted_date": "2023-02-28",
            "metrics_last_calculated": "2024-07-15T10:00:00Z"
        }
    });
    
    let doc = TantivyDocument::parse_json(&schema, &paper_doc.to_string())?;
    index_writer.add_document(doc)?;
    println!("✅ Academic paper document indexed");

    index_writer.commit()?;

    // # Set up search
    let reader = index.reader()?;
    let searcher = reader.searcher();
    
    // Set up query parser with JSON field as default
    let query_parser = QueryParser::for_index(&index, vec![doc_type_field, json_field]);
    
    println!("\n=== 🔍 Running Query Comparison Tests ===");
    
    // Test 1: Array exact search - user tags
    println!("\n📝 === Text Query Tests ===");
    println!("\n1. Array exact query for 'rust' in user_tags:");
    let query = query_parser.parse_query("data.user_tags:rust")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });
    
    // Test 2: Array search - company technologies
    println!("\n2. Array exact query for 'python' in company_technologies:");
    let query = query_parser.parse_query("data.company_technologies:python")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });
    
    // Test 3: Long text search with tokenization
    println!("\n3. Long text token query for 'library' in product_description:");
    let query = query_parser.parse_query("data.product_description:library")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });
    
    // Test 4: Academic keywords search
    println!("\n4. Academic keyword query for 'information' in research_keywords:");
    let query = query_parser.parse_query("data.research_keywords:information")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });

    // Test 5: Exact match tests
    println!("\n🎯 === Exact Match Tests ===");
    println!("\n5. Exact query for product_sku:");
    let query = query_parser.parse_query("data.product_sku:WH001234")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });
    
    // Test 6: Email exact search
    println!("\n6. Exact query for company_email:");
    let query = query_parser.parse_query("data.company_email:\"contact@techinnovations.com\"")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });

    // Test 7: Numeric queries (Note: Native JSON has limitations with range queries)
    println!("\n🔢 === Number Query Tests ===");
    println!("\n7. Exact numeric query for product_price (149.99):");
    let query = query_parser.parse_query("data.product_price:149.99")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });
    
    // Test 8: Age exact query
    println!("\n8. Exact numeric query for user_age (28):");
    let query = query_parser.parse_query("data.user_age:28")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });
    
    println!("\n   ⚠️  Note: Native JSON fields don't support numeric range queries like [140 TO 160]");
    println!("   This is a significant limitation compared to the custom Fixed JSON Layer!");

    // Test 9: Array content queries
    println!("\n📚 === Array Content Query Tests ===");
    println!("\n9. Array exact query for 'black' in inventory_colors:");
    let query = query_parser.parse_query("data.inventory_colors:black")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });
    
    // Test 10: Review comments search
    println!("\n10. Array text query for 'excellent' in review_comments:");
    let query = query_parser.parse_query("data.review_comments:excellent")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });

    // Test 11: Language search
    println!("\n11. Array exact query for 'chinese' in user_languages:");
    let query = query_parser.parse_query("data.user_languages:chinese")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });

    // Test 12: Geographic search
    println!("\n🔄 === Complex Query Tests ===");
    println!("\n12. Geographic query for 'San Francisco' in company_city:");
    let query = query_parser.parse_query("data.company_city:\"San Francisco\"")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });

    // Test 13: Author search
    println!("\n13. Author exact query for 'Dr. John Smith' in paper_authors:");
    let query = query_parser.parse_query("data.paper_authors:\"Dr. John Smith\"")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });

    // Test 14: Boolean query combinations
    println!("\n14. Boolean query: rust AND search (in different fields):");
    let query = query_parser.parse_query("data.user_tags:rust AND data.product_description:search")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });

    // Test 15: Document type filtering
    println!("\n15. Document type filtering for 'user_profile':");
    let query = query_parser.parse_query("doc_type:user_profile")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });

    // Test 16: Combined field and document type query
    println!("\n16. Combined query: doc_type AND specific data field:");
    let query = query_parser.parse_query("doc_type:ecommerce_product AND data.product_name:Wireless")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });

    // Test 17: Wildcard search
    println!("\n17. Wildcard search for emails:");
    let query = query_parser.parse_query("data.user_email:*@example.com")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });

    // Test 18: Phrase search
    println!("\n18. Phrase search in product description:");
    let query = query_parser.parse_query("data.product_description:\"search engine\"")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });

    // Test 19: Using RangeQuery API directly
    println!("\n19. Direct RangeQuery API - Age range (20-30):");

    // Create terms for JSON path with numeric values
    let min_term = {
        let mut term = Term::from_field_json_path(json_field, "user_age", true);
        term.append_type_and_fast_value(24u64);
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


    println!("\n=== 📊 Analysis Summary ===");
    println!("✅ Native JSON Field Advantages:");
    println!("   • Simple schema setup");
    println!("   • Built-in path-based querying (data.field.subfield)");
    println!("   • Automatic tokenization for text content");
    println!("   • Good integration with query parser");
    println!("   • Support for complex boolean queries");
    
    println!("\n⚠️  Potential Limitations Observed:");
    println!("   • Numeric range queries may be limited");
    println!("   • Date range queries not easily supported");
    println!("   • Less control over tokenization strategies");
    println!("   • May not handle very large JSON documents efficiently");
    
    println!("\n🔄 Comparison with Fixed JSON Layer:");
    println!("   • Fixed layer: More complex setup, better control");
    println!("   • Native JSON: Simpler setup, good for basic use cases");
    println!("   • Fixed layer: Custom tokenization, optimized for specific patterns");
    println!("   • Native JSON: Standard tokenization, general-purpose");

    Ok(())
} 