# Tantivy JSON 支持方案评估（2026-02）

> 本文档是对半年前 JSON 支持方案的全面重新评估，结合外部系统（ES / Doris / ClickHouse）调研、Tantivy 原生能力演进、以及上游最新变更，给出当前最佳落地方案。

---

## 0. 结论先行

**推荐方案：原生 JSON + 白名单增强（三层架构）**

| 层 | 职责 | 实现方式 |
|---|---|---|
| **主索引层** | 覆盖 80% 查询场景 | Tantivy 原生 JSON field（`TEXT \| FAST \| STORED`） |
| **白名单增强层** | 热路径的 analyzer 定制 + 排序 | 对声明的路径抽取为独立 typed field |
| **类型治理层** | schema 约束 + 写入校验 | path→type 注册表，写入时软/硬校验 |

核心依据：Tantivy 0.24+ 已补齐 JSON fast field range、path 级动态列、JSON 聚合等能力，半年前方案中关于"原生 JSON 不支持 range/排序"的若干判断已过时。自定义 fixed layer 的维护成本远大于收益。

---

## 1. 外部系统调研

### 1.1 Elasticsearch

ES 提供四种 JSON 处理策略，每种有不同的权衡：

**Object（默认）**：将 JSON 扁平化为 dot-path 独立字段。每个路径有独立倒排索引、doc values、stored fields。**核心缺陷**：数组对象的跨字段关联丢失——`user.first=alice AND user.last=smith` 会错误匹配 `[{first:alice, last:white}, {first:john, last:smith}]`。

**Nested**：每个嵌套对象作为独立 Lucene 文档存储，通过 `ToParentBlockJoinQuery` 实现索引时 join。**保留了数组内对象的字段关联**，但代价显著：每个嵌套对象 = 1 个 Lucene 文档；默认限制 50 个 nested mapping / 10000 个嵌套对象；更新任一嵌套字段需重新索引整个父文档。

**Flattened**：所有叶子 key-value 放入单个 Lucene 字段，token 格式为 `key\0value`。**解决 mapping 爆炸问题**（ES 默认 1000 字段限制），但所有值按 keyword 处理——不支持数值 range、不支持全文检索、不支持 highlight。

**subobjects:false（ES 8.x 新增）**：dot-path key 作为字面字段名，但保留正确类型（不像 flattened 全部 keyword 化）。是 object 和 flattened 之间的折中。

**对 Tantivy 的启发**：
- ES 的主流实践本质是"路径治理 + 类型治理 + 热字段显式建模"，不是完全自由的 schemaless
- mapping 爆炸问题在 Tantivy（嵌入式库）中不如 ES（分布式集群）严重，但数组对象关联丢失和 query 灵活性 vs 存储成本的权衡同样适用
- nested 语义需要 Lucene 的 block join 基础设施，Tantivy 需从零实现，短期不现实

### 1.2 Apache Doris

Doris 的 `VARIANT` 类型（2.1+）是当前业界最接近"理想 JSON 列式存储"的实现：

**核心机制**：写入时自动将 JSON 拆解为独立列式子列（sub-columns）。每个 JSON path 成为一个原生列，享受与静态列相同的编码（字典、RLE 等）、压缩和索引。

**类型推断与合并**：在 Memtable flush 时构建前缀树，追踪每个 path 的类型。冲突时做"最小公共类型"合并（如 TinyInt + BigInt → BigInt），无法调和时退化为 JSONB 二进制存储。

**稀疏列处理**：NULL 比例高的低频 path 不独立建列，而是打包进共享 JSONB 列（`variant_max_subcolumns_count` 默认 2048）。防止"列爆炸"。

**索引支持**：ZoneMap（min/max 裁剪）、BloomFilter、倒排索引（对文本支持分词、对数值使用 BKD Tree、posting list 用 Roaring Bitmap 压缩）。3.1+ 支持按 path 配置不同索引策略。

**性能数据**：VARIANT 查询比 JSON(JSONB) 快 8x，存储节省 65%；比 ES 约快 2x，存储节省 80%。

**已知限制**：不能作为主键/排序键；Schema Template 不可 ALTER 修改；全列读取（SELECT *）时需扫描所有子列；类型冲突退化为 JSONB 后性能下降。

**对 Tantivy 的启发**：
- 写入时拆列是性能的核心——Tantivy 的 JSON fast field 已实现 path 级动态列，思路一致
- 稀疏路径治理（热路径独立列 + 冷路径共享存储）是必须解决的工程问题
- 倒排索引 + 列式存储的结合正是 Tantivy 的天然优势

### 1.3 ClickHouse

ClickHouse 新 JSON 类型（v24.8 引入，v25.3 production-ready）是最复杂也最灵活的实现：

**核心创新——Variant/Dynamic 类型**：不做类型合并，而是用 discriminated union 保留每行的原始类型。一个 path 可以同时在不同行存储 Int64、String、Array，各自有独立子列文件。UInt8 discriminator 标识每行的实际类型。

**三级存储**：
1. **Typed Paths**（schema 声明）：与原生列性能相同，零 overhead
2. **Dynamic Paths**（自动推断，默认 max 1024）：Dynamic 类型子列，近原生性能
3. **Shared Data**（溢出）：Map(String,String) 结构存储低频 path

**v25.8 advanced format**：对 shared data 引入 granule 级元数据，选择性读取比旧格式快 58x、内存减少 3300x。

**已知限制**：无法作为主键；NULL 无法区分"值为 null"和"path 不存在"；path 扁平化歧义（`a.b.c` 无法区分嵌套 vs 带点 key）；不支持 JSON 子列上的倒排索引（roadmap 中）。

**对 Tantivy 的启发**：
- 类型保留（Variant）vs 类型合并（Doris LCT）是核心设计选择——对搜索引擎而言，倒排索引天然是类型相关的，path 级类型治理比 Variant 更实用
- 热路径提升 + 冷路径共享是三个系统的共识
- **ClickHouse 不支持 JSON 子列的倒排索引**——这恰恰是 Tantivy 的核心竞争力所在

### 1.4 三系统对比总结

| 维度 | ES | Doris VARIANT | ClickHouse JSON |
|---|---|---|---|
| **存储模型** | 每 path 独立 Lucene 字段 | 自动拆子列 + 稀疏共享列 | Variant/Dynamic 子列 + Shared |
| **类型冲突** | dynamic mapping 推断 + 强制 | LCT 合并，JSONB fallback | 保留所有类型，Variant union |
| **倒排索引** | 全支持 | 支持（BKD + Roaring Bitmap） | **不支持**（roadmap） |
| **数组对象关联** | nested（重） / object（丢失） | 不保留 | 不保留 |
| **热路径优化** | mapping + flattened | 自动频次提升 | max_dynamic_paths |
| **排序能力** | doc values | 原生列式 | 原生列式 |

**共同结论**：三个系统都不是"完全自由 schemaless"——都需要路径治理、类型治理、热路径显式建模。这验证了我们"白名单增强"策略的正确性。

---

## 2. Tantivy 原生 JSON 能力现状（v0.25）

### 2.1 已具备能力

| 能力 | 状态 | 代码位置 |
|---|---|---|
| JSON fast field range query | 0.24 引入 | `src/query/range_query/range_query_fastfield.rs` |
| JSON path 的 QueryParser 语法 + 类型推断 | 支持 | `src/query/query_parser/query_parser.rs` |
| JSON fast field 按 path 列式访问 | 支持 | `src/fastfield/readers.rs`（动态列） |
| JSON path 参与聚合 | 支持 | `src/aggregation/agg_tests.rs` |
| JSON path exists 查询 | 支持 | `src/query/exist_query.rs` |
| JSON path term/fulltext 查询 | 支持 | 原生能力 |

### 2.2 仍存在的边界

1. **RangeQuery 依赖 fast field**：JSON 的 RangeQuery 当前要求字段标记为 FAST（`src/query/range_query/range_query.rs` 明确报错）
2. **扁平化模型**：数组对象跨对象匹配问题（同 ES object 类型），无 nested 语义
3. **JSON path 不支持 regex query**
4. **字段级 tokenizer**：原生 JSON 是整个字段统一 tokenizer，不支持按 path 配置不同 analyzer
5. **排序**：JSON fast field 是多值动态列，按特定 path 排序需要从动态列中提取对应 path 的值，性能不如独立单值 fast field

### 2.3 上游最新变更（落后 93 个 commit）

当前 fork 基于 `bc1c7898`，upstream/main 已领先 93 个 commit。JSON 相关的上游改进：

- `#2694` Optimize ExistsQuery for a high number of dynamic columns
- `#2693` Add fast field fallback for term query if not indexed
- `#2761` Handle JSON fields and columnar in space_usage
- `#2783` Optimize RangeDocSet for non-overlapping query ranges
- `#2787` Add benchmark for boolean query with range sub query
- `#2754` Added some benchmark for top K by a fast field
- `#2816` Fix closing parenthesis error on elastic range queries

**建议**：新建分支从 `upstream/main` 拉取纯净基线，再将 demo/文档迁移过去。当前工作树有大量本地修改和编译错误，不宜直接 merge。

---

## 3. 半年前方案复盘

### 3.1 方案概述

半年前的方案（`TiCI_JSON_Support_Proposal.md` + `JSON_ORDER_BY_DESIGN.md`）核心设计：

- **自定义 fixed layer**（`examples/fixed_json_layer.rs`，~950 行）：将 JSON 扁平化为 path-prefixed tokens，存入固定数量的内部字段（text_analyzed / text_raw / text_ngram / json_number / json_date）
- **path_configs**：按路径配置 analyzer（keyword / english_stemmer / edge_ngram 等）
- **sortable_fields**：声明排序字段，抽取为独立 fast field
- **SQL 接口**：`fts_match_word` / `fts_match_scalar` / `fts_range` / `fts_exists`

### 3.2 仍然有价值的部分

1. **path_configs（路径级 analyzer）**：思路正确，与 ES/Doris/ClickHouse 的实践一致。原生 JSON 不支持 per-path tokenizer，这确实是需要增强的点
2. **sortable_fields（排序白名单）**：思路正确。通过抽取为独立单值 fast field 实现 O(1) 排序，避免多值列扫描
3. **SQL 函数接口**：`fts_match_word` / `fts_range` / `fts_exists` 的语义抽象方向正确
4. **Generated Column 替代方案**：提供了用户可选的 schema 级替代路径

### 3.3 已过时或应收敛的部分

1. **"原生 JSON 不支持 range" 的结论已过时**：Tantivy 0.24+ 已支持 JSON fast field range query。`native_json_comparison.rs` 的 18 个测试用例中的 range 相关结论需更新
2. **"bytes 无法排序" 的前提已变化**：`JSON_ORDER_BY_DESIGN.md` 中关于"需要 path 前缀扫描"的核心假设与当前 JSON fast field 的 path 级动态列不一致——当前实现中每个 JSON path 已经是独立的动态列
3. **fixed layer 的投入产出比低**：
   - 自定义编码（sign-flip IEEE754、sortable bytes）、自定义 tokenizer pipeline、类型推断与日期解析全自维护
   - 局限于扁平 JSON（不支持嵌套），与原生 JSON 能力高度重叠
   - 与 upstream 演进持续偏离，维护成本高
4. **No Phrase Matching / No BM25 Scoring 的限制**：原生 JSON 天然支持 phrase query 和 BM25 scoring，fixed layer 反而不支持

---

## 4. 推荐方案

### 4.1 架构总览

```
┌─────────────────────────────────────────────────────┐
│                   SQL Interface                      │
│  fts_match_word / fts_range / fts_exists / ORDER BY │
└────────────────────┬────────────────────────────────┘
                     │ 路由
┌────────────────────▼────────────────────────────────┐
│              Query Router / Planner                  │
│  判断 path 是否命中白名单 → 路由到增强层或原生层       │
└───┬─────────────────────────────────────────┬───────┘
    │                                         │
    ▼                                         ▼
┌───────────────────────┐    ┌────────────────────────┐
│   主索引层（原生 JSON）  │    │   白名单增强层           │
│                       │    │                        │
│ field: data           │    │ analyzer_whitelist:     │
│ flags: TEXT|FAST|STORED│   │   $.title → english    │
│                       │    │   $.code  → keyword    │
│ 覆盖:                 │    │                        │
│ - term/fulltext query │    │ sortable_whitelist:     │
│ - range query (fast)  │    │   $.price → f64 field  │
│ - exists query        │    │   $.date  → datetime   │
│ - aggregation         │    │                        │
│ - phrase query + BM25 │    │ 实现: 独立 typed field  │
└───────────────────────┘    └────────────────────────┘
                     │
              ┌──────▼──────┐
              │ 类型治理层   │
              │             │
              │ path→type   │
              │ 注册表      │
              │             │
              │ 写入校验    │
              └─────────────┘
```

### 4.2 主索引层：原生 JSON

```rust
let mut schema_builder = Schema::builder();
let json_options = JsonObjectOptions::default()
    .set_indexing_options(TextFieldIndexing::default()
        .set_tokenizer("default")
        .set_index_option(IndexRecordOption::WithFreqsAndPositions))
    .set_fast(None)           // 启用 fast field
    .set_stored();            // 启用 stored
    // .set_expand_dots(true) // 如需 dot expansion

let data_field = schema_builder.add_json_field("data", json_options);
```

**覆盖能力**：
- `data.title:bike` → 全文检索
- `data.stock_level:[5 TO 100]` → fast field range query
- `data.product_code:BK-R93R-44` → term query
- ExistsQuery on `data.tags`
- Aggregation on `data.price`
- Phrase query: `data.title:"steel bike"`
- BM25 relevance scoring

### 4.3 白名单增强层

#### Analyzer Whitelist（路径级分词器）

对需要特殊分词的热路径，复制到独立 text field 并配置专属 tokenizer：

```rust
// 对 $.title 使用 english stemmer
let title_field = schema_builder.add_text_field("wl_title",
    TextOptions::default()
        .set_indexing_options(TextFieldIndexing::default()
            .set_tokenizer("en_stem")
            .set_index_option(IndexRecordOption::WithFreqsAndPositions))
        .set_stored());

// 对 $.product_code 使用 keyword（精确匹配）
let code_field = schema_builder.add_text_field("wl_code",
    TextOptions::default()
        .set_indexing_options(TextFieldIndexing::default()
            .set_tokenizer("raw"))
        .set_stored());
```

写入时同步：原生 JSON field 正常写入 + 白名单 path 同时写入对应独立字段。

#### Sortable Whitelist（排序白名单）

```rust
// 对 $.price 抽取为独立 f64 fast field
let price_field = schema_builder.add_f64_field("sort_price", FAST);

// 对 $.release_date 抽取为独立 datetime fast field
let date_field = schema_builder.add_date_field("sort_release_date", FAST);
```

查询时路由：当 `ORDER BY data->>'$.price'` 时，router 识别为白名单字段，使用 `sort_price` 进行排序而非从 JSON 动态列提取。

### 4.4 类型治理层

```json
{
  "type_registry": {
    "$.price": "f64",
    "$.stock_level": "u64",
    "$.release_date": "datetime",
    "$.on_sale": "bool",
    "$.title": "text",
    "$.product_code": "keyword"
  },
  "on_type_mismatch": "coerce_or_warn"
}
```

写入时行为：
- `strict`：类型不匹配直接拒绝
- `coerce_or_warn`：尝试类型转换，失败则降级为字符串 + 日志告警
- `permissive`：接受任意类型（默认行为，与原生 JSON 一致）

### 4.5 配置示例（TiCI SQL 接口）

```sql
ALTER TABLE products ADD FULLTEXT INDEX idx_data (data) PARAMETER 'tici:{
  "default_analyzer": "standard",

  "analyzer_whitelist": [
    {"path": "$.title", "analyzer": "english_stemmer"},
    {"path": "$.product_code", "analyzer": "keyword"},
    {"path": "$.phone_numbers[*]", "analyzer": "edge_ngram_3_10"}
  ],

  "sortable_fields": {
    "price": {"path": "$.price", "type": "f64"},
    "release_date": {"path": "$.release_date", "type": "datetime"}
  },

  "type_registry": {
    "$.price": "f64",
    "$.stock_level": "u64",
    "$.release_date": "datetime"
  }
}';
```

### 4.6 Query 示例

```sql
-- 1. 全文检索 + 排序 + 分页（title 走白名单 analyzer，排序走 sortable_fields）
SELECT id, data->>'$.title'
FROM products
WHERE fts_match_word(data, '$.title', 'bike')
ORDER BY data->>'$.release_date' DESC
LIMIT 10;

-- 2. Range + 精确匹配（stock_level 走原生 JSON fast field range）
SELECT * FROM products
WHERE fts_match_word(data, '$.on_sale', 'false')
  AND fts_range(data, '$.stock_level') > 0;

-- 3. Exists 查询
SELECT * FROM products
WHERE fts_exists(data, '$.product_code');

-- 4. 复合查询
SELECT id, data->>'$.title', data->>'$.stock_level'
FROM products
WHERE fts_match_word(data, '$.tags', 'outdoors')
  AND fts_range(data, '$.stock_level') BETWEEN 5 AND 20
ORDER BY data->>'$.price' ASC
LIMIT 20;
```

---

## 5. 方案对比：新 vs 旧

| 维度 | 旧方案（fixed layer） | 新方案（原生 + 白名单） |
|---|---|---|
| **Range Query** | 自定义 bytes 编码 + path prefix | 原生 JSON fast field range |
| **排序** | 自定义 bytes ordinal 排序 | 白名单→独立 fast field |
| **全文检索** | 自定义 PathPrefixTokenizer | 原生 JSON tokenizer |
| **Phrase Query** | 不支持 | 原生支持 |
| **BM25 Scoring** | 不支持 | 原生支持 |
| **Per-path Analyzer** | 自定义实现 | 白名单增强层 |
| **嵌套 JSON** | 不支持（仅扁平） | 原生支持任意层级 |
| **维护成本** | 高（~950 行自定义代码） | 低（薄 routing 层） |
| **Upstream 兼容** | 差（持续偏离） | 好（依赖原生能力） |
| **Aggregation** | 不支持 | 原生支持 |

---

## 6. 已知局限与风险

### 6.1 架构级局限

1. **无 nested 语义**：与 ES/Doris/ClickHouse 一致，数组对象扁平化处理，跨对象字段关联丢失。这是 Lucene 架构的固有限制，短期无法解决
2. **白名单非全自动**：需要用户显式声明热路径，不像 Doris VARIANT 自动识别高频路径。这是一个有意的设计取舍——显式优于隐式，避免不可预测的性能波动
3. **JSON path 不支持 regex query**：上游限制，需关注后续版本

### 6.2 性能边界

1. **非白名单路径的排序**：只能通过 JSON 动态列获取，性能不如独立 fast field。需在文档和 SLA 中明确
2. **高基数动态路径**：如果 JSON 有上千个不同 path，动态列的元数据开销会增加。需参考上游 `#2694`（ExistsQuery 动态列优化）的经验
3. **类型冲突**：同一 path 在不同文档中有不同类型时，原生 JSON 会按实际类型索引。range query 只匹配对应类型的文档，不做自动类型转换

### 6.3 与上游的同步策略

当前落后 upstream 93 个 commit，其中包含多个 JSON 相关优化。建议：

1. **不在当前工作树直接 merge**（本地有大量未提交改动和编译错误）
2. **新建分支**从 `upstream/main` 拉取纯净基线
3. 将 demo/文档/配置迁移到新分支
4. 验证原生 JSON 能力在最新代码上的表现

---

## 7. 实施路线图

### Phase 1：基础能力验证（1-2 周）

- [ ] 从 `upstream/main` 创建纯净分支
- [ ] 验证原生 JSON field（TEXT | FAST | STORED）的 range/aggregation/exists 能力
- [ ] 编写 benchmark：原生 JSON 性能 vs 旧 fixed layer 性能
- [ ] 更新 `native_json_comparison.rs` 的测试用例

### Phase 2：白名单增强层实现（2-3 周）

- [ ] 实现 analyzer_whitelist routing（写入时同步到独立字段）
- [ ] 实现 sortable_fields routing（写入时抽取到独立 fast field）
- [ ] 实现 query router（根据白名单配置选择查询路径）
- [ ] 编写端到端测试

### Phase 3：类型治理与 SQL 接口（2-3 周）

- [ ] 实现 type_registry 和写入校验
- [ ] 适配 TiCI SQL 函数接口
- [ ] 实现 PARAMETER 配置解析
- [ ] 编写集成测试

### Phase 4：性能优化与文档（1-2 周）

- [ ] 排序性能优化（白名单 vs 动态列对比）
- [ ] 高基数路径场景测试
- [ ] 更新 TiCI_JSON_Support_Proposal.md
- [ ] 更新 JSON_ORDER_BY_DESIGN.md

---

## 8. 参考资料

### 外部系统

**Elasticsearch**
- [Dynamic Field Mapping](https://www.elastic.co/guide/en/elasticsearch/reference/current/dynamic-field-mapping.html)
- [Object Type](https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/object)
- [Nested Type](https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/nested)
- [Flattened Type](https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/flattened)
- [subobjects:false](https://www.elastic.co/guide/en/elasticsearch/reference/current/subobjects.html)
- [Mapping Limit](https://www.elastic.co/docs/reference/elasticsearch/index-settings/mapping-limit)

**Apache Doris**
- [VARIANT Data Type](https://doris.apache.org/docs/3.x/sql-manual/basic-element/sql-data-types/semi-structured/VARIANT/)
- [Variant Technical Deep Dive](https://doris.apache.org/blog/variant-tech-deepdive-202601/)
- [Inverted Index](https://doris.apache.org/docs/3.x/table-design/index/inverted-index/)
- [Variant in Doris 2.1](https://doris.apache.org/blog/variant-in-apache-doris-2.1/)

**ClickHouse**
- [New JSON Data Type](https://clickhouse.com/docs/sql-reference/data-types/newjson)
- [How We Built a New JSON Type](https://clickhouse.com/blog/a-new-powerful-json-data-type-for-clickhouse)
- [JSON Gets Even Better (v25.8)](https://clickhouse.com/blog/json-data-type-gets-even-better)
- [JSON Best Practices](https://clickhouse.com/docs/best-practices/use-json-where-appropriate)
- [JSON Benchmark: 1B Documents](https://clickhouse.com/blog/json-bench-clickhouse-vs-mongodb-elasticsearch-duckdb-postgresql)

### Tantivy 仓库内证据

- `Cargo.toml`（v0.25.0）
- `CHANGELOG.md`（JSON fast field range 0.24+）
- `src/query/range_query/range_query.rs` / `range_query_fastfield.rs`
- `src/fastfield/readers.rs`（动态列访问）
- `src/aggregation/agg_tests.rs`（JSON aggregation）
- `src/query/exist_query.rs`（JSON exists）
- `src/schema/json_object_options.rs`（JSON 字段配置）
- `src/core/json_utils.rs`（JSON 索引核心逻辑）
- `examples/json_native_whitelist_demo.rs`（白名单 demo）
- `examples/native_json_comparison.rs`（原生 vs fixed layer 对比）
- `TiCI_JSON_Support_Proposal.md`（旧 SQL 接口提案）
- `JSON_ORDER_BY_DESIGN.md`（旧排序设计）
- `JSON_SUPPORT_REEVALUATION_2026-02-06.md`（上次评估）

### 上游 JSON 相关 PR

- [#2694 Optimize ExistsQuery for dynamic columns](https://github.com/quickwit-oss/tantivy/pull/2694)
- [#2693 Add fast field fallback for term query](https://github.com/quickwit-oss/tantivy/pull/2693)
- [#2761 Handle JSON fields in space_usage](https://github.com/quickwit-oss/tantivy/pull/2761)
- [#2783 Optimize RangeDocSet for non-overlapping ranges](https://github.com/quickwit-oss/tantivy/pull/2783)
