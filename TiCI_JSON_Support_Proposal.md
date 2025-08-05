# Native JSON Indexing in TiDB with TiCI

## 1. Executive Summary

For customers accustomed to the search capabilities of systems like Elasticsearch, moving to a powerful distributed SQL database like TiDB presents a dilemma: how do you retain fast, flexible, text-oriented search while gaining the ability to perform complex analytical queries?

We are excited to introduce the next evolution of TiCI: **native JSON indexing and expanded type support**.

Currently, TiCI provides powerful text-search capabilities. In our upcoming version, we are extending this power to structured data. This initiative makes **native JSON support** a headline feature, allowing you to index and query fields within your JSON documents. This enhancement also brings official support for indexing standalone **numeric, date, and boolean types** with the same high performance. The result is a unified system that delivers the best of both worlds: the high-performance search you rely on and the sophisticated SQL analytics you need, all in one place.

This document outlines our proposed design for this transformative feature.

## 2. A Powerful and SQL-Native Query Interface

To provide a clear and powerful interface that feels native to the TiDB/MySQL ecosystem, we will enhance the existing `fts_match` family of functions. These functions act as markers, telling the TiDB query optimizer to delegate the work to TiCI's specialized index, ensuring maximum performance.

### Proposed Functions:

*   `fts_match_word(json_col, path, query_text)`: The primary function for single-term matching.
*   `fts_match_prefix(json_col, path, prefix_text)`: For dedicated prefix matching.
*   `fts_range(json_col, path)`: A marker for accelerating range queries on numeric and date fields. It is used in conjunction with standard SQL operators (`>`, `<`, `BETWEEN`).
*   `fts_exists(json_col, path)`: A boolean function to check if a given JSON path exists and is not `null`.

## 3. Usage and Examples

### 🔨 Table and Index Definition

First, define a `FULLTEXT` index on your `JSON` column. The `COMMENT` clause is used to pass TiCI-specific configuration, such as custom analyzers for different JSON paths.

```sql
CREATE TABLE products (
  id BIGINT PRIMARY KEY,
  data JSON,
  FULLTEXT INDEX idx_product_data (data) COMMENT 'tici:{
    "default_analyzer": "standard",
    "path_configs": [
      {"path": "$.product_code", "analyzer": "keyword"},
      {"path": "$.title", "analyzer": "english_stemmer"},
      {"path": "$.phone_numbers[*]", "analyzer": "edge_ngram_3_10"}
    ]
  }'
);
```

### 🔍 Example JSON Document

```json
{
  "title": "Awesome Steel Bike",
  "product_code": "BK-R93R-44",
  "stock_level": 8,
  "on_sale": true,
  "tags": ["bicycle", "sports", "outdoors"],
  "phone_numbers": ["13812345678", "15987654321"],
  "release_date": "2023-05-01T10:00:00Z"
}
```

### 🔍 Query Examples

#### 1. Text Search with Sorting and Pagination

Find products matching "bike", order by release date, and return the top 10. The `ORDER BY` on `release_date` is accelerated by TiCI.

```sql
SELECT id, data->>'$.title'
FROM products
WHERE fts_match_word(data, '$.title', 'bike')
ORDER BY data->>'$.release_date' DESC
LIMIT 10;
```

#### 2. Combined Exact Match and Range Filter

Find bikes in stock that are not on sale. Both conditions are pushed down to TiCI.

```sql
SELECT * FROM products
WHERE fts_match_word(data, '$.on_sale', 'false')
  AND fts_range(data, '$.stock_level') > 0;
```

#### 3. Searching within an Array (`NOT IN` equivalent)

Find products tagged "sports" but not "outdoors".

```sql
SELECT * FROM products
WHERE fts_match_word(data, '$.tags', 'sports')
  AND NOT fts_match_word(data, '$.tags', 'outdoors');
```

#### 4. Checking for NULL values

Find products where the `product_code` field exists and is not null.

```sql
SELECT * FROM products
WHERE fts_exists(data, '$.product_code');
```

#### 5. Prefix Search on an Array of Strings

Find a product by the prefix of a phone number.

```sql
SELECT * FROM products
WHERE fts_match_prefix(data, '$.phone_numbers', '138123');
```

#### 6. Complex Combined Query

Find outdoor products that are on sale, have a stock level between 5 and 20, and have a product code that starts with "BK-". Order the results by most recently released.

```sql
SELECT id, data->>'$.title', data->>'$.stock_level'
FROM products
WHERE
  fts_match_word(data, '$.tags', 'outdoors')
  AND fts_match_word(data, '$.on_sale', 'true')
  AND fts_match_prefix(data, '$.product_code', 'BK-')
  AND fts_range(data, '$.stock_level') BETWEEN 5 AND 20
ORDER BY data->>'$.release_date' DESC;
```

---

## 4. Under the Hood: The Internal Design

### The Indexing Pipeline

TiCI processes JSON by flattening it into path-value pairs. Based on your configuration, it applies specific analyzers to each path, converting values into indexed terms for fast retrieval. This indexing process is the key to accelerating queries on all data types.

**Diagram A: The Indexing Pipeline**
```mermaid
graph TD
    subgraph "Input: Example JSON Document"
        A["{<br/>'title': 'Awesome Steel Bike',<br/>'product_code': 'BK-R93R-44',<br/>'on_sale': true,<br/>'phone_numbers': ['138...'],<br/>...<br/>}"]
    end

    A --> B{"Flatten to<br/>Path-Value Pairs"};

    subgraph "Example Path-Value Pairs"
        C["'$.title':<br/>'Awesome Steel Bike'"]
        D["'$.product_code':<br/>'BK-R93R-44'"]
        E["'$.on_sale':<br/>true"]
        F["'$.phone_numbers[0]':<br/>'13812345678'"]
    end
    B --> C & D & E & F

    C --> G{"Automatic Mapping Logic<br/>(based on value type and path config)"};
    D --> G;
    E --> G;
    F --> G;

    subgraph "Tantivy Index with Fixed Internal Fields"
        direction TB
        subgraph "Text Fields"
            H["<b>text_analyzed</b><br>(Standard Tokenizer)"]
            I["<b>text_raw</b><br>(Keyword/Identifier)"]
            J["<b>text_ngram</b><br>(Edge N-Gram Tokenizer)"]
        end
        subgraph "Typed Fields (Bytes)"
            K["<b>number_field</b>"]
            L["<b>date_field</b>"]
            M["<b>bytes_field</b><br/>(for Booleans, etc.)"]
        end
    end
    
    G -- "'$.title' -> Analyzed" --> H
    G -- "'$.product_code' -> Raw" --> I
    G -- "'$.on_sale' -> Bool" --> M
    G -- "'$.phone_numbers' -> N-Gram" --> J

    subgraph "Indexed Terms (Path-Prefixed)"
        H ==> P["'title__awesom',<br/>'title__steel', ..."]
        I ==> Q["'product_code__BK-R93R-44'"]
        M ==> R["'on_sale__' + encoded(true)"]
        J ==> S["'phone_numbers__138',<br/>'phone_numbers__1381', ..."]
    end
```

### The Query Execution Pipeline

When you run a query using `fts_*` functions, the TiDB optimizer recognizes them and rewrites the plan to delegate the filtering work to TiCI. **Crucially, this includes range filters and sorting**, which are executed efficiently on the TiCI index, not on the TiDB level.

**Diagram B: The Query Pipeline (based on Query Example #6)**
```mermaid
graph TD
    A["<b>User SQL Query (Example #6)</b><br/>...<br/>WHERE fts_match_word(data, '$.tags', 'outdoors')<br/>  AND fts_match_word(data, '$.on_sale', 'true')<br/>  AND fts_match_prefix(data, '$.product_code', 'BK-')<br/>  AND fts_range(data, '$.stock_level') BETWEEN 5 AND 20<br/>ORDER BY data->>'$.release_date' DESC"]

    A --> B{"TiCI Query Translator"};

    subgraph "Translate Each Condition to a Tantivy Query"
        direction LR
        B -- "fts_match_word('$.tags', ...)<br/>(path is keyword)" --> C{"TermQuery on <b>text_raw</b><br/>Term: 'tags__outdoors'"}
        B -- "fts_match_word('$.on_sale', ...)<br/>(path is boolean)" --> D{"TermQuery on <b>bytes_field</b><br/>Term: 'on_sale__' + encoded(true)"}
        B -- "fts_match_prefix('$.product_code', ...)<br/>(path is keyword)" --> E{"PrefixQuery on <b>text_raw</b><br/>Prefix: 'product_code__BK-'"}
        B -- "fts_range('$.stock_level', ...)<br/>(path is numeric)" --> F{"RangeQuery on <b>number_field</b><br/>Range: [5, 20]"}
    end

    subgraph "Combine into a Single Plan"
        C & D & E & F --> G["BooleanQuery<br/>(all with MUST clause)"]
    end

    subgraph "Execute and Sort"
      G --> H["<b>Step 1:</b><br>Inverted Index Search<br/>(finds matching doc IDs)"]
      A -- "ORDER BY '$.release_date'" --> I
      H --> I{"<b>Step 2:</b><br>Sort Results<br/>(uses Columnar Store on <b>date_field</b> for speed)"}
    end

    I --> J["<B>Final Sorted Doc IDs</B>"];
```

### Understanding Analyzers

The choice of analyzer is critical for defining *how* a field can be searched.

**Diagram C: Analyzer Comparison for "Awesome Steel Bike"**
```mermaid
graph TD
    A["Input Text:<br/>'Awesome Steel Bike'"]

    subgraph "Keyword Analyzer (Exact Match)"
        A --> B["One Token:<br/>'Awesome Steel Bike'"]
    end

    subgraph "Standard Analyzer (Full-Text Search)"
        A --> C["Tokens:<br/>'awesome', 'steel', 'bike'"]
    end

    subgraph "Edge N-gram Analyzer (Prefix Search)"
        A --> D["Tokens:<br/>'a', 'aw', 'awe', 'awes', ...<br/>'s', 'st', 'ste', 'stee', ...<br/>'b', 'bi', 'bik', 'bike'"]
    end
```

### Indexing for Performance: Beyond Text

For numeric, date, and boolean fields, TiCI employs a sophisticated two-part strategy to deliver high performance for both filtering and sorting.

1.  **Inverted Index for Fast Filtering**: All values, including numbers and dates, are first converted into a comparable byte-encoded format and placed into TiCI's inverted index. This allows the full power of the inverted index to be used for these types.
    *   **Exact Matches** (e.g., `stock_level = 8`) become fast term lookups.
    *   **Range Scans** (e.g., `stock_level > 5`) become efficient range lookups on the terms in the inverted index.

2.  **Columnar Store for Fast Sorting**: When a query requires sorting (`ORDER BY`) on a numeric or date field, retrieving values one by one from the inverted index would be inefficient. To solve this, TiCI maintains a separate, auxiliary **columnar store** for these fields. This is a simple data structure that stores the values sequentially and can be accessed directly by a document's internal ID. After the inverted index has rapidly filtered the documents down to a small result set, this columnar store provides a high-speed path to retrieve the values needed for the final sort.

This dual approach ensures that both filtering and sorting operations are executed with maximum efficiency.

## 5. Design Considerations (Current Limitations)

*   **Flattened Data Model**: The index "flattens" arrays of objects. This means parent-child relationships within a nested structure are not preserved in the index, which can lead to false positives when querying across multiple fields of a nested object. For example, if you have `variants: [{color: "red", size: "L"}, {color: "blue", size: "M"}]`, a query for `color: "red" AND size: "M"` would match.

*   **Static Type Inference**: The type of a field (e.g., text, number) is inferred during the first ingestion and remains fixed. A field that contains both `"123"` and `123` will be treated as either text or numeric based on the first value seen, and subsequent documents must conform.

*   **No Phrase Matching**: The initial version will support single-term matching via `fts_match_word`. Support for multi-term phrase queries is planned for a future release.

*   **No Relevance-Based Scoring**: Queries are based on boolean matching, not relevance scoring. There is no support for ordering results by a relevance score like TF-IDF or BM25. 