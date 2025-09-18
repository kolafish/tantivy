use tantivy::{
    collector::TopDocs,
    doc,
    query::{TermQuery, Query, BooleanQuery, Occur},
    schema::{Schema, STORED, TEXT, Term, IndexRecordOption},
    DocAddress,
    Index, Result,
};

fn main() -> Result<()> {
    // Create schema
    let mut schema_builder = Schema::builder();
    let text_field = schema_builder.add_text_field("text", TEXT | STORED);
    let schema = schema_builder.build();

    // Create index in memory
    let index = Index::create_in_ram(schema);
    let mut index_writer = index.writer(50_000_000)?;

    // Add sample documents
    index_writer.add_document(doc!(
        text_field => "programming language rust",
    ))?;
    index_writer.add_document(doc!(
        text_field => "programming basics tutorial",
    ))?;
    index_writer.add_document(doc!(
        text_field => "rust programming guide",
    ))?;
    index_writer.add_document(doc!(
        text_field => "basic rust examples",
    ))?;

    // Commit changes
    index_writer.commit()?;

    // Create searcher
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Create a term query for "pro"
    let prefix_term = "pro";
    let prefix_term = Term::from_field_text(text_field, prefix_term);
    let term_query = TermQuery::new(prefix_term.clone(), IndexRecordOption::Basic);

    // Search
    let top_docs: Vec<(f32, DocAddress)> = searcher.search(&term_query, &TopDocs::with_limit(10))?;

    println!("Results for prefix '{:?}':", prefix_term);
    for (_score, doc_address) in top_docs {
        let retrieved_doc = searcher.doc(doc_address)?;
        let text = retrieved_doc.get_first(text_field).unwrap().as_text().unwrap();
        println!("- {}", text);
    }

    // Now let's try with trigram prefix matching
    println!("\nTrigram prefix matching example:");
    let search_term = "pro";
    let trigrams = get_trigrams(search_term);
    println!("Trigrams for '{}': {:?}", search_term, trigrams);

    // Create a boolean query combining trigrams
    let mut subqueries: Vec<(Occur, Box<dyn Query>)> = Vec::new();
    for trigram in trigrams {
        let term = Term::from_field_text(text_field, &trigram);
        let term_query = TermQuery::new(term, IndexRecordOption::Basic);
        subqueries.push((Occur::Must, Box::new(term_query)));
    }
    
    let boolean_query = BooleanQuery::from(subqueries);
    
    // Search with the trigram query
    let trigram_results: Vec<(f32, DocAddress)> = searcher.search(&boolean_query, &TopDocs::with_limit(10))?;
    
    println!("Results for trigram search of '{}':", search_term);
    for (score, doc_address) in trigram_results {
        let retrieved_doc = searcher.doc(doc_address)?;
        let text = retrieved_doc.get_first(text_field).unwrap().as_text().unwrap();
        println!("- Score: {:.2}, Document: {}", score, text);
    }

    Ok(())
}

/// Generate trigrams from a given text
fn get_trigrams(text: &str) -> Vec<String> {
    if text.len() < 3 {
        return vec![text.to_string()];
    }

    let chars: Vec<char> = text.chars().collect();
    let mut trigrams = Vec::new();

    for i in 0..=chars.len() - 3 {
        let trigram: String = chars[i..i + 3].iter().collect();
        trigrams.push(trigram);
    }

    trigrams
} 