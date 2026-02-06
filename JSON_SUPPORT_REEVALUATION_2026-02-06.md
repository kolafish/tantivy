# Tantivy JSON 支持再评估（2026-02-06）

## 0. 结论先行

截至 2026-02-06，这个仓库继续推进 JSON 支持的最佳路径是：

1. 以 Tantivy 原生 JSON（`TEXT | FAST`）作为主线能力。
2. 保留你旧方案里的两个思想，但不再保留整套 fixed layer：
   - `path` 级 analyzer 配置（按白名单路径增强）。
   - `sortable_fields`（按白名单路径抽取为独立可排序字段）。
3. 不建议现在把“所有 JSON 能力”都继续押在 `examples/fixed_json_layer.rs` 的自定义编码方案上。

核心原因：Tantivy 0.25 已补齐了旧方案最关键的一部分（JSON fast field range + JSON path fast column + JSON 聚合等），旧方案里关于“原生 JSON range/排序能力不足”的若干判断已过时。

---

## 1. 外部系统调研（ES / Doris / ClickHouse）

### 1.1 Elasticsearch（ES）

现状（官方文档）：

- ES 将 JSON 映射到不同字段类型，而不是单一 `json` 类型；动态映射会把 JSON 字段推断为 `object/long/float/date/boolean/text` 等类型。  
  参考：  
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/dynamic-field-mapping.html
- `object` 类型会把对象层级扁平化；数组对象在普通 `object` 下会出现“跨对象匹配”问题。  
  参考：  
  - https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/object
- `nested` 可避免上述问题，但代价更高（文档与查询都更重）。  
  参考：  
  - https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/nested
- `flattened` 适合“键很多/不固定”的对象，但所有叶子值按 keyword 视角处理，range 是按字符串语义，不支持 highlight。  
  参考：  
  - https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/flattened
- 默认字段数限制是 1000（`index.mapping.total_fields.limit`），动态键过多会触发治理问题。  
  参考：  
  - https://www.elastic.co/docs/reference/elasticsearch/index-settings/mapping-limit

对我们有价值的启发：

- ES 的主流实践本质上也是“路径治理 + 类型治理 + 热字段显式建模”，不是完全自由的 schemaless。

### 1.2 Apache Doris

现状（官方文档）：

- Doris 提供 `JSON` 类型（文档描述为二进制存储，层级深度上限 100）。  
  参考：  
  - https://doris.apache.org/docs/4.x/sql-manual/basic-element/sql-data-types/semi-structured/JSON/
- Doris 在 4.x 里强调 `VARIANT` 作为高动态半结构化类型：自动拆子列、可跳过无关数据、对宽 JSON 的点查/过滤更友好。  
  参考：  
  - https://doris.apache.org/docs/4.x/sql-manual/basic-element/sql-data-types/semi-structured/VARIANT/
- `VARIANT` 当前仍有边界：不能作为 Key 列、不能作为 BloomFilter/二级索引列等。  
  同上。
- 倒排索引文档显示支持 `VARIANT`，并支持 path index（`properties.xxx`）。  
  参考：  
  - https://doris.apache.org/docs/4.x/table-design/index/inverted-index/overview/

对我们有价值的启发：

- Doris 的方向是“动态列自动拆分 + 明确索引能力边界”，这和我们后续做 `sortable_fields`/`analyzer whitelist` 的思路一致。

### 1.3 ClickHouse

现状（官方文档）：

- ClickHouse 新 `JSON` 类型在 **v25.3 标记 production-ready**，更早版本建议用于评估/测试。  
  参考：  
  - https://clickhouse.com/docs/best-practices/use-json-where-appropriate  
  - https://clickhouse.com/docs/en/sql-reference/data-types/newjson
- `JSON` 会把一部分动态路径作为 subcolumn 存储（`max_dynamic_paths`，默认 1024），其余进入 shared data structure。  
  参考：  
  - https://clickhouse.com/docs/best-practices/use-json-where-appropriate  
  - https://clickhouse.com/docs/en/sql-reference/data-types/newjson
- 官方仍强调：JSON 在分析型场景应谨慎使用，优先提取稳定列、只把“确实动态”的部分放 JSON。  
  参考：  
  - https://clickhouse.com/docs/best-practices/use-json-where-appropriate

对我们有价值的启发：

- 即便是 ClickHouse，也不是“所有字段都放 JSON 再在线自适应”，而是“热路径抽取 + 动态路径限额”。

---

## 2. Tantivy 当前原生 JSON 能力（基于本仓库代码）

当前仓库版本：`tantivy = 0.25.0`（`Cargo.toml`）。

### 2.1 已具备能力

1. JSON fast field range 已支持（0.24 引入，changelog 明确）。  
   - `CHANGELOG.md`：`Support fast field range queries on json fields`。  
   - 代码与测试：`src/query/range_query/range_query_fastfield.rs`（`json_range_test`、`json_range_mixed_val`）。
2. QueryParser 支持 JSON path 字段语法与类型推断（含日期/数值/bool 的搜索 token 推断）。  
   - `src/query/query_parser/query_parser.rs`  
   - `src/lib.rs` 中 `test_searcher_on_json_field_with_type_inference`
3. JSON fast field 在列式层按 path 可访问（`json.foo` 这种形式）。  
   - `src/fastfield/readers.rs`（`resolve_column_name_given_default_field`、`dynamic_column_handles` 测试）
4. JSON path 可参与聚合。  
   - `src/aggregation/agg_tests.rs`（`json.color`、`json.price` 等）
5. JSON path 可做 exists 查询。  
   - `src/query/exist_query.rs`（`test_exists_query_json`）

### 2.2 仍存在的边界

1. JSON 的 RangeQuery 当前要求字段是 fast field。  
   - `src/query/range_query/range_query.rs` 明确返回错误：`RangeQuery on JSON is only supported for fast fields currently`
2. JSON 仍是扁平化模型，数组对象会有跨对象匹配问题（非 nested 语义）。  
   - `doc/src/json.md`
3. JSON path 不支持 regex query。  
   - `src/query/query_parser/query_parser.rs` 测试用例包含该限制。
4. 按 path 的 analyzer 精细化配置（ES 风格）不是原生能力；原生 JSON 仍是字段级 tokenizer 策略。
5. 文档存在历史滞后：`doc/src/json.md` 仍有“JSON 不支持 range”的旧描述，与 0.24+ 实现不一致。

---

## 3. 对你半年前 Demo/方案的复盘

### 3.1 本地历史定位

- Demo 起点：`d2e276bba`（`examples/fixed_json_layer.rs`）
- 引入 native-json 对比：`50a8eef35`（`examples/native_json_comparison.rs`）
- 根目录方案文档：
  - `TiCI_JSON_Support_Proposal.md`（2025-08 起多次迭代）
  - `JSON_ORDER_BY_DESIGN.md`（2025-08）

### 3.2 旧方案里仍然有价值的部分

1. `path_configs`（路径级 analyzer）思路正确，且和 ES/Doris/ClickHouse 的现实路径一致。
2. `sortable_fields` 思路正确：排序字段白名单化，避免“所有动态字段都可高性能排序”的不切实际目标。
3. 在 SQL 接口上抽象 `fts_match_word / fts_range / fts_exists` 的方向正确。

### 3.3 旧方案中已过时或应收敛的部分

1. `native_json_comparison` 里“原生 JSON 不支持 range”的结论已过时。  
   - 当前 Tantivy 0.24+ 已支持 JSON fast-field range。
2. `JSON_ORDER_BY_DESIGN.md` 对“bytes 无法排序、需要路径前缀扫描”的核心假设与当前实现不再完全一致。  
   - Tantivy JSON fast field 已是 path 级动态列，不是单一 bytes 大字段扫描模型。
3. `fixed_json_layer` 整体成本偏高：  
   - 自定义编码、自定义 token pipeline、类型推断与日期解析策略全自维护；  
   - 扁平对象限制强，和原生 JSON 能力重复较多；  
   - 后续与 upstream 演进容易持续偏离。

---

## 4. 现在的最佳方案（建议落地版）

### 4.1 目标

在“尽量贴近 upstream Tantivy”的前提下，补齐你业务真正缺口：路径级 analyzer、排序稳定性、类型治理。

### 4.2 方案

1. 主索引层：使用原生 JSON
   - `data` 字段采用 `TEXT | FAST | STORED`（按需加 `expand_dots`）。
   - 默认覆盖：term/fulltext、exists、range（fast-field 条件下）、聚合。
2. 路径白名单增强层（只对热点路径）
   - `analyzer_whitelist`：对少量路径复制到专用 text 字段，使用专属 tokenizer。
   - `sortable_whitelist`：对少量路径复制到专用 numeric/date/string fast field，供稳定排序与分页。
3. 类型治理层
   - 建路径-类型注册表（例如 `$.price -> f64`）。
   - 写入时做软/硬校验（拒绝、降级到字符串、或旁路告警）。
4. SQL 侧接口层
   - 保持 `fts_match_word` / `fts_range` / `fts_exists` 语义。
   - 解析器路由到：原生 JSON path 或增强层白名单字段。

### 4.3 为什么这是“现在最优”

1. 能复用 Tantivy 已有能力，避免重复造轮子。
2. 保留你 demo 中真正有价值的“路径治理”思想。
3. 复杂度可控：先上线 80% 能力，再按业务热点加白名单增强。

### 4.4 已知局限（需在文档和接口明确）

1. 不提供 ES nested 级别的对象数组严格语义。
2. 路径 analyzer 不做全量动态配置，只做白名单。
3. sort 只承诺白名单字段的稳定语义，非白名单不做强 SLA。

---

## 5. 是否现在同步 upstream

已执行 `git fetch --prune upstream`，当前分叉状态：

- `main...upstream/main = 64 (left) / 93 (right)`  
  即：你的 `main` 领先 64 个提交，同时落后 upstream/main 93 个提交。

建议：

1. 这次评估先不直接在当前工作树做 merge/rebase（你本地有大量未提交改动）。
2. 另起分支从 `upstream/main` 做一次“纯净基线”分支，再挑选你关心的 JSON demo/文档迁移过去对比。

---

## 6. 文档自审（第 2 轮 review 结论）

本文件二次自审后，已补齐以下容易遗漏点：

1. 明确写出“`RangeQuery on JSON` 仅 fast-field 支持”的真实边界。
2. 单独标出 `doc/src/json.md` 与当前实现不一致的风险（避免后续再次被旧文档误导）。
3. 把旧方案里“应保留思想”与“应收敛实现”分开写，便于后续工程决策。

---

## 7. 参考链接

### Elasticsearch

- https://www.elastic.co/guide/en/elasticsearch/reference/current/dynamic-field-mapping.html
- https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/object
- https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/nested
- https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/flattened
- https://www.elastic.co/docs/reference/elasticsearch/index-settings/mapping-limit

### Apache Doris

- https://doris.apache.org/docs/4.x/sql-manual/basic-element/sql-data-types/semi-structured/JSON/
- https://doris.apache.org/docs/4.x/sql-manual/basic-element/sql-data-types/semi-structured/VARIANT/
- https://doris.apache.org/docs/4.x/table-design/index/inverted-index/overview/

### ClickHouse

- https://clickhouse.com/docs/en/sql-reference/data-types/newjson
- https://clickhouse.com/docs/best-practices/use-json-where-appropriate

### Tantivy 仓库内证据

- `Cargo.toml`
- `CHANGELOG.md`
- `src/query/range_query/range_query.rs`
- `src/query/range_query/range_query_fastfield.rs`
- `src/fastfield/readers.rs`
- `src/aggregation/agg_tests.rs`
- `src/query/exist_query.rs`
- `src/query/query_parser/query_parser.rs`
- `doc/src/json.md`
- `examples/fixed_json_layer.rs`
- `examples/native_json_comparison.rs`
- `TiCI_JSON_Support_Proposal.md`
- `JSON_ORDER_BY_DESIGN.md`
