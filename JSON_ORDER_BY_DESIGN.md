# Design for High-Performance Sorting on Dynamic JSON Fields

## 1. Introduction

This document outlines the challenges and proposed solutions for implementing efficient `ORDER BY` functionality on fields within JSON documents in our Tantivy-based indexing system.

While our primary JSON support proposal provides a powerful way to filter on dynamic JSON data, sorting (`ORDER BY`) introduces a unique set of performance considerations. This document details the current limitations in Tantivy, a potential naive implementation, its performance drawbacks, and a recommended robust solution that ensures high-performance sorting.

The proposed design is divided into four sections:
1.  **Current State**: An overview of Tantivy's fast fields and their role in sorting.
2.  **Naive `ORDER BY` on Bytes**: A proposal to enable sorting on `bytes` fields by adapting existing mechanisms.
3.  **The Performance Problem**: An analysis of why the naive approach is inefficient for our dynamic JSON use case.
4.  **Recommended Solution**: A proposal for user-configured sort fields to achieve optimal performance.

## 2. Part 1: Tantivy's Fast Fields and Sorting - The Current Landscape

### What are Fast Fields?
Fast fields in Tantivy are column-oriented storage structures optimized for efficient per-document value access. They are the core mechanism that enables features requiring rapid lookups of a field's value for a given document ID. Key use cases include:
- Sorting (`ORDER BY`)
- Range queries
- Faceted aggregations

Under the hood, fast fields store values in a compact, columnar format. For single-valued fields, this allows for constant-time retrieval of a value given a document ID. For multi-valued fields (like an array of tags), it stores a flattened list of all values along with a separate offset index to map document IDs to their corresponding range of values.

### How Fast Fields Enable Sorting
Sorting requires efficiently accessing the values of the `ORDER BY` field for the documents that match the query's `WHERE` clause. Fast fields are essential for this. The `TopDocs` collector, which manages sorting, uses fast fields to retrieve these values directly without having to re-read and parse data from other parts of the index (like the inverted index or stored fields).

### The Limitation: No Direct Sorting on `bytes` Fields
A key challenge is that Tantivy's `TopDocs` collector does not natively support sorting on raw `bytes` fields. The collector is primarily designed to work with numeric types, and specifically uses a `u64_collector` to read fast field data.

- **Supported Types**: `u64`, `i64`, `f64`, `datetime`, and `bool` fields can be directly sorted because their values have a natural numeric representation that fits the collector's architecture.
- **Text Fields**: Sorting on `text` fields is supported through a clever workaround. Instead of sorting on the raw string values, Tantivy stores the **term ordinal** (a unique `u64` ID for each term in the field's dictionary) in the fast field. The collector can then sort these `u64` ordinals, which correctly reflects the lexicographical order of the original terms.
- **Bytes Fields**: Although the schema allows a `bytes` field to be marked as `fast`, and the necessary reader/writer logic exists to store and retrieve the raw bytes, the `TopDocs` collector lacks a mechanism to sort on them directly.

This presents a problem for our JSON indexing strategy, where we map all non-numeric types (strings, booleans, etc.) to a path-prefixed `bytes` field. To support `ORDER BY` on a JSON string field like `data->>'$.product_code'`, we must bridge this gap.

## 3. Part 2: A Naive Approach - Enabling `ORDER BY` on Bytes Fields

To overcome the limitation described above, we can draw inspiration from how `text` fields are handled and implement a similar ordinal-based strategy for `bytes` fields.

### The Proposal
1.  **Dictionary Encoding for Bytes**: For any `bytes` field that needs to be sortable, we will build a dictionary of all its unique values during indexing. Each unique byte sequence (e.g., `'product_code__BK-R93R-44'`) is assigned a unique, monotonically increasing `u64` ordinal.
2.  **Store Ordinals in Fast Field**: Instead of storing the raw bytes, we store these `u64` ordinals in the associated fast field. This dictionary-encoded data is compact and, critically, compatible with the `u64_collector` used by `TopDocs`.
3.  **Enhance the Collector**: We would introduce a new function to the `TopDocs` collector, perhaps named `order_by_bytes_fast_field`. This function would work similarly to `order_by_string_fast_field`, instructing the collector to use the `u64` fast field containing the term ordinals for the specified `bytes` field.

With this change, the `bytes` field becomes sortable, seemingly solving our problem. However, this approach introduces a significant performance issue when applied to the dynamic nature of our JSON indexing schema.

## 4. Part 3: The Performance Problem - The Inefficiency of Multi-Valued Fields

Our core JSON indexing strategy involves mapping a potentially unlimited number of JSON paths to a small, fixed number of underlying Tantivy fields (e.g., `text_raw`, `number_field`). For any given document, a field like `text_raw` acts as a **multi-valued field**, holding all string-like values from the source JSON.

For example, the JSON document:
```json
{ "product_code": "A", "color": "blue", "on_sale": true }
```
would result in multiple path-prefixed entries in the `bytes` field for a single document ID: `'product_code__A'`, `'color__blue'`, `'on_sale__true'`.

### The Sorting Bottleneck
When a user executes `ORDER BY data->>'$.product_code'`, the naive implementation runs into a major bottleneck. Tantivy's fast field access is optimized for retrieving *all* values for a given document, not for picking a *specific* value from a multi-valued set based on a prefix.

To find the sort key for a single document, the process would be:
1.  Use the document ID to find the slice of `u64` ordinals belonging to it in the fast field.
2.  Iterate through every ordinal in that slice.
3.  For each ordinal, perform a reverse lookup in the `bytes` dictionary to get the original byte value (e.g., `'color__blue'`).
4.  Check if this value starts with the desired path prefix (e.g., `'product_code__'`).
5.  If it matches, we have found the sort key. If not, continue iterating.

This linear scan must be performed for every document in the result set. For JSON objects with hundreds of keys, this process is prohibitively slow and negates the "fast" in "fast fields." The efficiency of constant-time access is lost to a slow, repeated search *within* each document's data.

## 5. Part 4: The Recommended Solution - Explicitly Configured Sort Fields

To achieve true high-performance sorting, we must avoid the multi-value scanning problem. The most robust way to do this is to require users to declare which JSON paths they intend to use for sorting at index creation time.

### The Proposal
We will introduce a new configuration option in the `COMMENT` clause of the `CREATE TABLE` statement, for example, `sortable_fields`. This would allow a user to map specific JSON paths to dedicated, high-performance sort channels.

**Example Configuration:**
```sql
CREATE TABLE products (
  id BIGINT PRIMARY KEY,
  data JSON,
  FULLTEXT INDEX idx_product_data (data) COMMENT 'tici:{
    ... -- other configs
    "sortable_fields": {
      "release_date": {"path": "$.release_date", "type": "datetime"},
      "price": {"path": "$.price", "type": "f64"},
      "code": {"path": "$.product_code", "type": "bytes"}
    }
  }'
);
```

### Internal Implementation
Internally, TiCI will pre-define a small, fixed number of dedicated, **single-valued** fast fields for sorting (e.g., `sort_u64_1`, `sort_u64_2`, `sort_f64_1`, `sort_bytes_1`, `sort_bytes_2`, etc.).

1.  **Static Mapping**: The `sortable_fields` configuration creates a static mapping between a user-defined name (`price`) and an internal Tantivy field (`sort_f64_1`).
2.  **Targeted Indexing**: During indexing, when the engine encounters a path that matches a configured sortable path (e.g., `$.price`), it extracts the value and writes it directly to the corresponding dedicated single-value fast field (`sort_f64_1`). This value is stored *in addition* to its normal indexing in the multi-valued fields used for filtering.
3.  **Efficient Sorting**: When a query like `ORDER BY data->>'$.price'` arrives, the planner recognizes that `price` is a pre-configured sort key and instructs the `TopDocs` collector to sort directly on the `sort_f64_1` fast field.

### Benefits of This Approach
-   **Maximum Performance**: Sorting operates on single-valued fast fields, restoring the constant-time access that makes sorting efficient. No scanning is required.
-   **Type Safety**: Explicitly defining the type (`f64`, `datetime`, `bytes`) allows for proper handling and encoding.
-   **Clear Contract**: The user makes a conscious decision about which fields are important for sorting, which is a common requirement in search applications.

The trade-off is a small loss in flexibility, as sortable fields must be defined upfront. However, this is a standard practice in high-performance search systems and provides a far superior user experience by guaranteeing fast and predictable `ORDER BY` performance. 