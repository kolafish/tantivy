# Unleashing the Power of Your Data: Advanced JSON Search in TiCI

## 1. Executive Summary

We are excited to announce **planned support for advanced JSON indexing and search** within TiCI. This powerful new feature will empower you to unlock the full value of your semi-structured data. You will be able to store complex JSON objects in a single column and query any field with high performance, leveraging customizable text analysis and seamless integration with the standard SQL you already know and love.

This document provides a technical overview of the feature, its usage, and the underlying design principles.

## 2. How It Works: A High-Level View

The new JSON functionality is built on two core processes: an intelligent indexing pipeline that deconstructs JSON for efficient storage, and a query execution path that translates standard SQL into fast index lookups.

### Indexing: From Raw JSON to Searchable Fields

When a JSON document is ingested, TiCI flattens it into a set of path-value pairs. Each pair is then mapped to a specific analyzer based on your rules, which tokenizes the value into searchable terms. This process ensures that data is stored optimally for the types of queries you need to run.

**Diagram A: JSON Indexing Flow**

```mermaid
graph TD
    A[<B>JSON Document</B><br/>{"user_name": "Alice", "user_tags": ["rust", "db"]}] --> B{Flatten to<br/>Path-Value Pairs};
    B --> C["user_name: \"Alice\"<br/>user_tags: \"rust\"<br/>user_tags: \"db\""];
    C --> D{Analyzer Mapping};
    D -- "user_name path matches text rule" --> E[Apply Standard Analyzer];
    D -- "user_tags path matches tag rule" --> F[Apply Keyword Analyzer];
    E --> G["Token: <br/>'user_name|alice'"];
    F --> H["Tokens: <br/>'user_tags|rust'<br/>'user_tags|db'"];
    subgraph "Inverted Index"
        G --> I[Searchable Terms]
        H --> I
    end
```

### Querying: Seamless Integration with SQL

Your application interacts with the data using standard SQL. TiCI's full-text search functions (`fts_match_*`) are transparently accelerated by the new index. The TiCI query engine translates these functions into an optimized internal plan, executing them against the inverted index for maximum performance while other SQL predicates are handled by TiDB as usual.

**Diagram B: Query Execution Pipeline**

```mermaid
graph TD
    A[<B>User SQL Query</B><br/>SELECT * FROM user_profile<br/>WHERE fts_match_word(data, 'tags', 'rust')<br/>AND JSON_EXTRACT(data, '$.age') > 25] --> B{TiDB SQL Parser};
    B --> C{fts_match_word(...) expression};
    C --> D[<B>TiCI Query Translator</B>];
    D --> E[Internal TermQuery<br/>"tags|rust"];
    B --> F{JSON_EXTRACT(...) expression};
    F --> G[Standard TiDB Execution];
    subgraph "TiCI Internal Plan (BooleanQuery)"
        E --> H{MUST clause};
    end
    H --> I[Inverted Index Lookup];
    subgraph "TiDB"
        G --> J{Result Set Filtering};
    end
    I --> J;
    J --> K[<B>Final Results</B>];
```

## 3. Core Capabilities

*   **Flexible Indexing Schema**:
    *   Define custom text-analysis rules for different JSON paths. You can apply full-text search to a product description, keyword analysis to a "tags" field, and prefix matching to a phone number—all within the same JSON object.
    *   Layer multiple search behaviors (like full-text and prefix matching) on a single field to support diverse query patterns.

*   **Rich Analyzer Support**:
    *   **Standard Tokenizer**: For general-purpose text that needs to be broken down by whitespace and punctuation.
    *   **N-gram Tokenizer**: Ideal for substring or prefix matching, commonly used for IDs, phone numbers, and codes.
    *   **Keyword Tokenizer**: Treats the entire value as a single token, perfect for exact-match filtering on identifiers, status codes, or tags.

*   **Powerful, Structured Filtering**:
    *   Perform exact, full-text, or prefix matches on string fields.
    *   Efficiently find values within string or numeric arrays.
    *   Execute fast range queries on numbers and dates.
    *   Combine multiple filter conditions seamlessly using standard SQL boolean logic (`AND`, `OR`, `NOT`).

## 4. Usage in TiDB

Getting started is simple. Define a `FULLTEXT` index on your `JSON` column, and you are ready to query.

### 🔨 Table and Index Definition

```sql
CREATE TABLE user_profile (
  id BIGINT PRIMARY KEY,
  data JSON,
  FULLTEXT INDEX idx_data (data)
);
```

### 🔍 Example JSON Document

```json
{
  "user_name": "Alice Smith",
  "user_age": 28,
  "user_tags": ["rust", "search", "database"],
  "user_scores": [95, 87, 92],
  "user_phone": "13812345678",
  "user_languages": ["english", "chinese"],
  "user_created_at": "2024-01-15T10:30:00Z"
}
```

### 🔍 Query Examples

All filters are processed via standard TiDB SQL semantics. TiCI transparently accelerates the `fts_match_*` expressions using its specialized index, without altering SQL's behavior.

#### 1. Single field match

```sql
SELECT * FROM user_profile
WHERE fts_match_word(data, 'user_name', 'Alice');
```

#### 2. Keyword match in a string array

```sql
SELECT COUNT(*) FROM user_profile
WHERE fts_match_word(data, 'user_tags', 'database');
```

#### 3. Prefix match on phone number

```sql
SELECT * FROM user_profile
WHERE fts_match_prefix(data, 'user_phone', '1381234');
```

#### 4. Combined filters

```sql
SELECT * FROM user_profile
WHERE fts_match_word(data, 'user_tags', 'rust')
  AND fts_match_word(data, 'user_languages', 'english')
  AND fts_match_prefix(data, 'user_phone', '138')
  AND JSON_EXTRACT(data, '$.user_age') BETWEEN 25 AND 35;
```

## 5. Under the Hood: Advanced Details

### How Tokenization Works

At the core of the indexing process is a set of customizable tokenizers. TiCI prefixes each generated token with its full JSON path to ensure queries are precise. For example, a search for `alice` against the `user_name` field will not accidentally match a value in a different field.

**Diagram C: Tokenizer Output Comparison**
The choice of analyzer dramatically changes how data is indexed and searched.

```mermaid
graph TD
    subgraph "Input Value"
        A["user_phone: '13812345678'"]
    end

    A --> B{Select Analyzer};

    subgraph "Keyword Analyzer (for Exact Match)"
        B -- "path rule: keyword" --> K[Single Token:<br/>"user_phone|13812345678"]
    end

    subgraph "Standard Analyzer (for General Text)"
        B -- "path rule: standard" --> S[Single Token:<br/>"user_phone|13812345678"]
    end

    subgraph "N-gram Analyzer (for Prefix/Substring Match)"
        B -- "path rule: ngram(3)" --> N[Multiple Tokens:<br/>"user_phone|138"<br/>"user_phone|381"<br/>"user_phone|812"<br/>"..."]
    end
```

### Indexing Logic

1.  **Flatten JSON**: The input document is deconstructed into a flat list of key-value pairs, where the key is the full JSON path.
2.  **Apply Dynamic Templates**: TiCI matches each path against your configured templates to select the right analyzer.
3.  **Default Logic**: If no template matches a path, a default analyzer is chosen based on the value type:
    *   Text with whitespace/punctuation → **Standard Analyzer**.
    *   Identifier-like strings → **Keyword Analyzer**.
    *   Date-like strings → Parsed as a timestamp for range queries.
    *   Numeric values → Encoded for fast term and range queries.

### Query Logic

Internally, TiCI translates `fts_match_*` calls into a `BooleanQuery` that combines different term, range, and prefix filters using `MUST`, `SHOULD`, and `MUST_NOT` clauses. For example:

```rust
// SQL: WHERE fts_match_word(data, 'user_tags', 'rust')
//      AND fts_match_word(data, 'user_languages', 'english')

BooleanQuery {
  subqueries: [
    (Must, TermQuery(term: "user_tags|rust")),
    (Must, TermQuery(term: "user_languages|english")),
  ]
}
```

## 6. Design Considerations (Current Limitations)

To deliver a robust and performant feature, we have made specific design choices.

*   **Flattened Data Model**: The index "flattens" arrays of objects. This means parent-child relationships within a nested structure are not preserved in the index, which can lead to false positives when querying across multiple fields of a nested object. For example, if you have `[{product: "A", color: "red"}, {product: "B", color: "blue"}]`, a query for `product: "A" AND color: "blue"` would match.

*   **Static Type Inference**: The type of a field (e.g., text, number) is inferred during the first ingestion and remains fixed. A field that contains both `"123"` and `123` will be treated as either text or numeric based on the first value seen, and subsequent documents must conform. 