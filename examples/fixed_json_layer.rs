use tantivy::schema::{Schema, SchemaBuilder, TextFieldIndexing, TextOptions, NumericOptions, Field, IndexRecordOption};
use tantivy::{Index, TantivyDocument, Term, DateTime};
use tantivy::tokenizer::{Tokenizer, TokenStream, Token};
use serde_json::{Value, Map};
use std::path::Path;

/// 自定义路径前缀分词器 - 实现 Tantivy Tokenizer trait
/// 输入格式：path__separator__actual_text
/// 输出：对actual_text分词，每个token加上path__separator__前缀
#[derive(Clone)]
pub struct PathPrefixTokenizer {
    path_separator: String,
}

impl PathPrefixTokenizer {
    pub fn new(path_separator: String) -> Self {
        Self { path_separator }
    }
    
    /// 辅助方法：手动分词并返回token字符串列表
    pub fn tokenize_to_strings(&mut self, text: &str) -> Vec<String> {
        let mut token_stream = self.token_stream(text);
        let mut tokens = Vec::new();
        
        while token_stream.advance() {
            tokens.push(token_stream.token().text.clone());
        }
        
        tokens
    }
}

impl Tokenizer for PathPrefixTokenizer {
    type TokenStream<'a> = PathPrefixTokenStream;

    fn token_stream<'a>(&mut self, text: &'a str) -> Self::TokenStream<'a> {
        PathPrefixTokenStream::new(text, &self.path_separator)
    }
}

/// 路径前缀Token流 - 实现 TokenStream trait
pub struct PathPrefixTokenStream {
    tokens: Vec<Token>,
    current_index: usize,
}

impl PathPrefixTokenStream {
    fn new(text: &str, path_separator: &str) -> Self {
        let mut tokens = Vec::new();
        
        // 查找最后一个路径分隔符的位置
        if let Some(last_sep_pos) = text.rfind(path_separator) {
            let path_prefix = &text[..last_sep_pos + path_separator.len()];
            let actual_text = &text[last_sep_pos + path_separator.len()..];
            
            // 简单分词：按空格和标点符号分割
            let words: Vec<&str> = actual_text
                .split_whitespace()
                .flat_map(|word| word.split(|c: char| !c.is_alphanumeric()))
                .filter(|token| !token.is_empty() && token.len() > 2)
                .collect();
            
            for (position, token) in words.iter().enumerate() {
                let prefixed_token = format!("{}{}", path_prefix, token.to_lowercase());
                tokens.push(Token {
                    offset_from: 0,
                    offset_to: prefixed_token.len(),
                    position: position,
                    text: prefixed_token,
                    position_length: 1,
                });
            }
        } else {
            // 如果没有分隔符，直接作为一个token
            tokens.push(Token {
                offset_from: 0,
                offset_to: text.len(),
                position: 0,
                text: text.to_string(),
                position_length: 1,
            });
        }
        
        Self {
            tokens,
            current_index: 0,
        }
    }
}

impl TokenStream for PathPrefixTokenStream {
    fn advance(&mut self) -> bool {
        if self.current_index < self.tokens.len() {
            self.current_index += 1;
            true
        } else {
            false
        }
    }

    fn token(&self) -> &Token {
        &self.tokens[self.current_index - 1]
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.tokens[self.current_index - 1]
    }
}

/// 值编码模块 - 用于将数值/日期等类型编码为可按字典序排序的字符串
///
/// ## 设计动机
///
/// 在 `tantivy` 中，专用的数值字段（如 `f64`, `date`）通过 `Term` 内部的 `set_fast_value`
/// 逻辑，将数值转换为保留其大小顺序的大端字节序（`&[u8]`）进行存储和范围查询。
/// 这种二进制表示是最高效的，但不一定是合法的 UTF-8 字符串。
///
/// 在本实现中，为了在一个字段内同时实现 “路径过滤” 和 “范围查询”，我们将数值/日期字段
/// 定义为了 `text` 类型，并使用 `raw` 分词器。这意味着我们必须将 `路径` 和 `值`
/// 拼接成一个**单一的、合法的字符串**来作为 `Term`。
///
/// ## 实现策略
///
/// `value_coder` 的作用就是解决这个问题：
/// 1. **借鉴核心思想**: 采用与 `tantivy` 内部相同的位操作逻辑，将 `f64`/`i64` (来自`DateTime`)
///    转换为一个保留原始大小顺序的 `u64`。
/// 2. **适配为字符串**: 将这个 `u64` 值编码为一个定长的**十六进制字符串**。十六进制表示法
///    既能完整地代表底层的二进制数据，其字典序也等同于原始数值的顺序，同时它本身是
///    合法的 UTF-8 字符。
///
/// 最终，我们可以安全地构建如 `product_price__800533325996fbe5` 这样的字符串，
/// 它可以在一个 `text` 字段上通过 `RangeQuery` 进行高效的、带路径的范围查找。
mod value_coder {
    use tantivy::DateTime;

    /// 将 i64 编码为保持排序性的 u64 (Sign-flipping)
    fn i64_to_sortable_u64(val: i64) -> u64 {
        (val as u64) ^ (1u64 << 63)
    }

    /// 将 f64 编码为保持排序性的 u64
    /// 正数: sign bit 设为 1
    /// 负数: 所有 bit 位取反
    fn f64_to_sortable_u64(val: f64) -> u64 {
        let u64_val = val.to_bits();
        if val >= 0.0 {
            u64_val | (1u64 << 63)
        } else {
            !u64_val
        }
    }

    pub fn encode_f64(val: f64) -> String {
        format!("{:016x}", f64_to_sortable_u64(val))
    }

    pub fn encode_date(val: DateTime) -> String {
        let i64_val = val.into_timestamp_micros();
        format!("{:016x}", i64_to_sortable_u64(i64_val))
    }
}

/// 优化版 JSON 处理层 - 扁平结构 + 自定义分词器 + 磁盘持久化
pub struct FixedJsonLayer {
    text_analyzed_field: Field,      // 分词文本字段（使用自定义分词器）
    text_raw_field: Field,           // 原始文本字段（raw分词器）
    number_field: Field,             // 数值字段
    bool_field: Field,               // 布尔字段
    date_field: Field,               // 日期字段（重要！）
    
    schema: Schema,
    config: JsonLayerConfig,
    path_tokenizer: PathPrefixTokenizer,  // 自定义路径前缀分词器
}

/// 配置
#[derive(Clone)]
pub struct JsonLayerConfig {
    pub path_separator: String,
    pub max_path_depth: usize,
    pub text_classification_rules: TextClassificationRules,
}

/// 简化版文本分类规则
/// 1. 标识符模式 (identifier_patterns)
/// - 默认包含: 大写ID、邮箱地址等结构化标识符
/// - 作用: 识别特殊格式，避免不必要的分词
/// 2. 分类逻辑简化:
/// - 包含空格或标点 → AnalyzedText (使用自定义分词器)
/// - 简短无空格 → Keyword (使用raw分词器)
/// - 匹配标识符模式 → Identifier (使用raw分词器)
#[derive(Clone)]
pub struct TextClassificationRules {
    pub identifier_patterns: Vec<regex::Regex>,
}

impl Default for JsonLayerConfig {
    fn default() -> Self {
        Self {
            path_separator: "__".to_string(),
            max_path_depth: 10,  // 保留深度限制（虽然现在是扁平结构）
            text_classification_rules: TextClassificationRules::default(),
        }
    }
}

impl Default for TextClassificationRules {
    fn default() -> Self {
        Self {
            identifier_patterns: vec![
                regex::Regex::new(r"^[A-Z0-9]{6,}$").unwrap(), // 大写ID
                regex::Regex::new(r"^[a-z0-9]+@[a-z0-9]+\.[a-z]+$").unwrap(), // 邮箱地址
                regex::Regex::new(r"^[A-Z]{2,3}[0-9]{6,}$").unwrap(), // 产品SKU格式
            ],
        }
    }
}

impl FixedJsonLayer {
    pub fn new() -> tantivy::Result<Self> {
        Self::new_with_config(JsonLayerConfig::default())
    }
    
    pub fn new_with_config(config: JsonLayerConfig) -> tantivy::Result<Self> {
        let mut schema_builder = SchemaBuilder::new();
        
        // 使用自定义分词器名称
        let text_analyzed_field = schema_builder.add_text_field(
            "json_text_analyzed",
            TextOptions::default()
                .set_indexing_options(
                    TextFieldIndexing::default()
                        .set_tokenizer("path_prefix")  // 使用自定义分词器！
                        .set_index_option(IndexRecordOption::Basic)
                )
                .set_stored()
        );
        
        let text_raw_field = schema_builder.add_text_field(
            "json_text_raw",
            TextOptions::default()
                .set_indexing_options(
                    TextFieldIndexing::default()
                        .set_tokenizer("raw")
                        .set_index_option(IndexRecordOption::Basic)
                )
                .set_stored()
        );
        
        // 将 number_field 和 date_field 定义为 text 字段，使用 raw 分词器
        // 以便存储 `path + encoded_value` 并支持范围查询
        let number_field = schema_builder.add_text_field(
            "json_number",
            TextOptions::default()
                .set_indexing_options(
                    TextFieldIndexing::default()
                        .set_tokenizer("raw")
                        .set_index_option(IndexRecordOption::Basic)
                )
        );
        
        let bool_field = schema_builder.add_bool_field(
            "json_bool",
            NumericOptions::default().set_indexed().set_stored().set_fast()
        );
        
        let date_field = schema_builder.add_text_field(
            "json_date",
            TextOptions::default()
                .set_indexing_options(
                    TextFieldIndexing::default()
                        .set_tokenizer("raw")
                        .set_index_option(IndexRecordOption::Basic)
                )
        );
        
        let schema = schema_builder.build();
        
        let path_tokenizer = PathPrefixTokenizer::new(config.path_separator.clone());
        
        Ok(FixedJsonLayer {
            text_analyzed_field,
            text_raw_field,
            number_field,
            bool_field,
            date_field,
            schema,
            config,
            path_tokenizer,
        })
    }
    
    /// 创建或打开磁盘索引
    pub fn create_or_open_index<P: AsRef<Path>>(&self, index_path: P) -> tantivy::Result<Index> {
        let index_path = index_path.as_ref();
        
        let index = if index_path.exists() {
            // 打开现有索引
            println!("📂 Opening existing index at: {:?}", index_path);
            Index::open_in_dir(index_path)?
        } else {
            // 创建新索引
            println!("🆕 Creating new index at: {:?}", index_path);
            std::fs::create_dir_all(index_path)?;
            Index::create_in_dir(index_path, self.schema.clone())?
        };
        
        // 注册自定义分词器
        index.tokenizers()
            .register("path_prefix", PathPrefixTokenizer::new(self.config.path_separator.clone()));
        
        Ok(index)
    }
    
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
    
    /// 处理扁平 JSON 对象（不支持嵌套）
    pub fn process_flat_json_object(&self, json_obj: &Map<String, Value>) -> tantivy::Result<TantivyDocument> {
        let mut doc = TantivyDocument::new();
        
        for (key, value) in json_obj {
            self.add_flat_value(&mut doc, key, value);
        }
        
        Ok(doc)
    }
    
    /// 添加扁平JSON值（处理数组和基本类型）
    fn add_flat_value(&self, doc: &mut TantivyDocument, field_name: &str, value: &Value) {
        match value {
            Value::String(s) => {
                // 尝试解析为日期，失败则作为文本处理
                if let Some(date_time) = self.try_parse_date(s) {
                    self.add_date_value(doc, field_name, date_time);
                } else {
                    let text_type = self.classify_text(s);
                    self.add_text_value(doc, field_name, s, text_type);
                }
            }
            Value::Number(n) => {
                if let Some(f) = n.as_f64() {
                    self.add_number_value(doc, field_name, f);
                }
            }
            Value::Bool(b) => {
                self.add_bool_value(doc, field_name, *b);
            }
            Value::Array(arr) => {
                // 处理数组：为每个元素添加相同的字段名
                for item in arr {
                    self.add_flat_value(doc, field_name, item);
                }
            }
            _ => {
                // 忽略 null 和其他类型
            }
        }
    }
    
    /// 尝试解析日期字符串
    fn try_parse_date(&self, s: &str) -> Option<DateTime> {
        // 简单的日期格式检查
        if s.len() < 8 {
            return None;
        }
        
        // 检查是否包含日期格式的基本特征
        let has_date_chars = s.contains('-') || s.contains('T') || s.contains(':');
        if !has_date_chars {
            return None;
        }
        
        // 尝试解析常见日期格式
        self.parse_date_formats(s)
    }
    
    /// 解析多种日期格式
    fn parse_date_formats(&self, s: &str) -> Option<DateTime> {
        use time::{PrimitiveDateTime, Date, Time};
        
        // 1. ISO 8601 with timezone: "2024-07-22T15:20:00Z"
        if let Ok(dt) = time::OffsetDateTime::parse(s, &time::format_description::well_known::Iso8601::DEFAULT) {
            return Some(DateTime::from_utc(dt));
        }
        
        // 2. ISO 8601 without timezone: "2024-07-22T15:20:00"
        if let Ok(dt) = PrimitiveDateTime::parse(s, &time::format_description::well_known::Iso8601::DEFAULT) {
            let offset_dt = dt.assume_utc();
            return Some(DateTime::from_utc(offset_dt));
        }
        
        // 3. Date only: "2024-07-22"
        if s.len() == 10 && s.matches('-').count() == 2 {
            if let Ok(date) = Date::parse(s, &time::format_description::parse("[year]-[month]-[day]").unwrap()) {
                let dt = PrimitiveDateTime::new(date, Time::MIDNIGHT);
                return Some(DateTime::from_utc(dt.assume_utc()));
            }
        }
        
        None
    }
    
    /// 添加日期值
    fn add_date_value(&self, doc: &mut TantivyDocument, field_name: &str, date_time: DateTime) {
        let encoded_date = value_coder::encode_date(date_time);
        let path_value = format!("{}{}{}", field_name, self.config.path_separator, encoded_date);
        doc.add_text(self.date_field, &path_value);
    }
    
    /// 简化的文本分类
    fn classify_text(&self, text: &str) -> TextType {
        // 1. 检查是否是标识符（邮箱、ID等特殊格式）
        for pattern in &self.config.text_classification_rules.identifier_patterns {
            if pattern.is_match(text) {
                return TextType::Identifier;
            }
        }
        
        // 2. 检查是否包含空格或标点符号
        if self.has_whitespace_or_punctuation(text) {
            TextType::AnalyzedText  // 需要分词
        } else {
            TextType::Keyword       // 简短关键词
        }
    }
    
    /// 检查文本是否包含空格或标点符号
    fn has_whitespace_or_punctuation(&self, text: &str) -> bool {
        text.chars().any(|c| {
            c.is_whitespace() || 
            c.is_ascii_punctuation() ||
            !c.is_alphanumeric()
        })
    }
    
    /// 添加文本值 - 智能分词策略
    fn add_text_value(&self, doc: &mut TantivyDocument, path: &str, value: &str, text_type: TextType) {
        let prefixed_value = format!("{}{}{}", path, self.config.path_separator, value);
        
        match text_type {
            TextType::AnalyzedText => {
                // 1. 原始字段：完整文本+路径前缀（用于精确匹配）
                doc.add_text(self.text_raw_field, &prefixed_value);
                
                // 2. 分析字段：使用自定义分词器，每个token带路径前缀
                let tokens = self.path_tokenizer.clone().tokenize_to_strings(&prefixed_value);
                for token in tokens {
                    doc.add_text(self.text_analyzed_field, &token);
                }
            }
            TextType::Keyword | TextType::Identifier => {
                // 关键词和标识符只添加到原始字段（raw分词器）
                doc.add_text(self.text_raw_field, &prefixed_value);
            }
        }
    }
    
    /// 添加数值
    fn add_number_value(&self, doc: &mut TantivyDocument, path: &str, value: f64) {
        let encoded_num = value_coder::encode_f64(value);
        let path_value = format!("{}{}{}", path, self.config.path_separator, encoded_num);
        doc.add_text(self.number_field, &path_value);
    }
    
    /// 添加布尔值
    fn add_bool_value(&self, doc: &mut TantivyDocument, path: &str, value: bool) {
        doc.add_bool(self.bool_field, value);
        
        let path_value = format!("{}{}{}_{}", path, self.config.path_separator, "bool", value);
        doc.add_text(self.text_raw_field, &path_value);
    }
}

/// 文本类型分类
#[derive(Debug, Clone)]
enum TextType {
    AnalyzedText,  // 需要分词的文本
    Keyword,       // 短关键词
    Identifier,    // 标识符
}

/// 智能查询构建器
pub struct SmartJsonQueryBuilder {
    layer: FixedJsonLayer,
}

impl SmartJsonQueryBuilder {
    pub fn new(layer: FixedJsonLayer) -> Self {
        Self { layer }
    }
    
    /// 智能路径查询 - 自动选择最佳查询策略
    pub fn smart_query(&self, path: &str, value: &str) -> tantivy::Result<Box<dyn tantivy::query::Query>> {
        use tantivy::query::{TermQuery, BooleanQuery, Occur};
        
        let prefixed_value = format!("{}{}{}", path, self.layer.config.path_separator, value);
        
        // 创建多个查询选项
        let mut subqueries = Vec::new();
        
        // 1. 原始字段查询（精确匹配）
        let raw_term = Term::from_field_text(self.layer.text_raw_field, &prefixed_value);
        subqueries.push((Occur::Should, Box::new(TermQuery::new(raw_term, IndexRecordOption::Basic)) as Box<dyn tantivy::query::Query>));
        
        // 2. 分词字段查询（如果可能包含可分析文本）
        if value.len() >= 3 {  // 对于较长的值也尝试分词查询
            let analyzed_term = Term::from_field_text(self.layer.text_analyzed_field, &prefixed_value);
            subqueries.push((Occur::Should, Box::new(TermQuery::new(analyzed_term, IndexRecordOption::Basic)) as Box<dyn tantivy::query::Query>));
        }
        
        if subqueries.len() == 1 {
            Ok(subqueries.into_iter().next().unwrap().1)
        } else {
            Ok(Box::new(BooleanQuery::new(subqueries)))
        }
    }
    
    /// 精确匹配查询
    pub fn exact_query(&self, path: &str, value: &str) -> tantivy::Result<Box<dyn tantivy::query::Query>> {
        use tantivy::query::TermQuery;
        
        let prefixed_value = format!("{}{}{}", path, self.layer.config.path_separator, value);
        let term = Term::from_field_text(self.layer.text_raw_field, &prefixed_value);
        Ok(Box::new(TermQuery::new(term, IndexRecordOption::Basic)))
    }
    
    // /// 数值范围查询（全局）
    // pub fn number_range_query(&self, min: f64, max: f64) -> tantivy::Result<Box<dyn tantivy::query::Query>> {
    //     use tantivy::query::RangeQuery;
    //     use std::ops::Bound;
        
    //     let min_term = Term::from_field_f64(self.layer.number_field, min);
    //     let max_term = Term::from_field_f64(self.layer.number_field, max);
        
    //     Ok(Box::new(RangeQuery::new(
    //         Bound::Included(min_term),
    //         Bound::Included(max_term)
    //     )))
    // }
    
    /// 带路径的数值范围查询 - 两阶段策略
    pub fn number_range_query_with_path(&self, path: &str, min: f64, max: f64) -> tantivy::Result<Box<dyn tantivy::query::Query>> {
        use tantivy::query::RangeQuery;
        use std::ops::Bound;
        
        let path_prefix = format!("{}{}", path, self.layer.config.path_separator);

        let min_str = format!("{}{}", path_prefix, value_coder::encode_f64(min));
        let max_str = format!("{}{}", path_prefix, value_coder::encode_f64(max));

        let min_term = Term::from_field_text(self.layer.number_field, &min_str);
        let max_term = Term::from_field_text(self.layer.number_field, &max_str);

        let range_query = RangeQuery::new(
            Bound::Included(min_term),
            Bound::Included(max_term)
        );
        
        Ok(Box::new(range_query))
    }
    
    // /// 日期范围查询（全局）
    // pub fn date_range_query(&self, start_date: &str, end_date: &str) -> tantivy::Result<Box<dyn tantivy::query::Query>> {
    //     use tantivy::query::RangeQuery;
    //     use std::ops::Bound;
        
    //     let start_dt = self.layer.parse_date_formats(start_date)
    //         .ok_or_else(|| tantivy::TantivyError::InvalidArgument(format!("Cannot parse start date: {}", start_date)))?;
    //     let end_dt = self.layer.parse_date_formats(end_date)
    //         .ok_or_else(|| tantivy::TantivyError::InvalidArgument(format!("Cannot parse end date: {}", end_date)))?;
            
    //     let start_term = Term::from_field_date(self.layer.date_field, start_dt);
    //     let end_term = Term::from_field_date(self.layer.date_field, end_dt);
        
    //     Ok(Box::new(RangeQuery::new(
    //         Bound::Included(start_term),
    //         Bound::Included(end_term)
    //     )))
    // }
    
    /// 带路径的日期范围查询 - 两阶段策略
    pub fn date_range_query_with_path(&self, path: &str, start_date: &str, end_date: &str) -> tantivy::Result<Box<dyn tantivy::query::Query>> {
        use tantivy::query::RangeQuery;
        use std::ops::Bound;
        
        let start_dt = self.layer.parse_date_formats(start_date)
            .ok_or_else(|| tantivy::TantivyError::InvalidArgument(format!("Cannot parse start date: {}", start_date)))?;
        let end_dt = self.layer.parse_date_formats(end_date)
            .ok_or_else(|| tantivy::TantivyError::InvalidArgument(format!("Cannot parse end date: {}", end_date)))?;
            
        let path_prefix = format!("{}{}", path, self.layer.config.path_separator);

        let start_str = format!("{}{}", path_prefix, value_coder::encode_date(start_dt));
        let end_str = format!("{}{}", path_prefix, value_coder::encode_date(end_dt));

        let start_term = Term::from_field_text(self.layer.date_field, &start_str);
        let end_term = Term::from_field_text(self.layer.date_field, &end_str);
        
        let range_query = RangeQuery::new(
            Bound::Included(start_term),
            Bound::Included(end_term)
        );
        
        Ok(Box::new(range_query))
    }
    
    /// 日期精确查询
    pub fn date_exact_query(&self, path: &str, date_str: &str) -> tantivy::Result<Box<dyn tantivy::query::Query>> {
        use tantivy::query::TermQuery;
        
        let date_time = self.layer.parse_date_formats(date_str)
            .ok_or_else(|| tantivy::TantivyError::InvalidArgument(format!("Cannot parse date: {}", date_str)))?;
            
        let encoded_date = value_coder::encode_date(date_time);
        let path_value = format!("{}{}{}", path, self.layer.config.path_separator, encoded_date);
        
        let term = Term::from_field_text(self.layer.date_field, &path_value);
        Ok(Box::new(TermQuery::new(term, IndexRecordOption::Basic)))
    }
}

fn main() -> tantivy::Result<()> {
    use serde_json::json;
    use tantivy::collector::TopDocs;
    
    println!("🚀 Optimized JSON Processing Layer - Flat Structure + Disk Persistence");
    println!("📋 Features: Flat JSON, custom tokenizer, disk persistence, array support");
    
    // 创建修复版 JSON 处理层
    let layer = FixedJsonLayer::new()?;
    
    // 测试自定义分词器和文本分类
    println!("\n=== 🔬 Testing Custom Tokenizer & Text Classification ===");
    let test_text = "description__A high-quality search engine library for Rust applications";
    let tokens = layer.path_tokenizer.clone().tokenize_to_strings(test_text);
    println!("📝 Input: {}", test_text);
    println!("🔗 Tokens: {:?}", tokens);
    println!("✨ Each token preserves path prefix for precise targeting!");
    
    // 测试简化的文本分类规则
    println!("\n=== 📊 Text Classification Examples ===");
    let test_cases = vec![
        ("rust", "短关键词，无空格"),
        ("search engine", "包含空格，需要分词"),
        ("alice@example.com", "邮箱格式，标识符"),
        ("PROD123456", "产品ID，标识符"),
        ("WH001234", "SKU格式，标识符"),
        ("high-quality", "包含连字符，需要分词"),
        ("San Francisco", "包含空格，需要分词"),
        ("python", "短关键词，无空格"),
    ];
    
    for (text, description) in test_cases {
        let text_type = layer.classify_text(text);
        let type_str = match text_type {
            TextType::AnalyzedText => "AnalyzedText",
            TextType::Keyword => "Keyword",
            TextType::Identifier => "Identifier",
        };
        println!("   '{}' → {} ({})", text, type_str, description);
    }
    
    // 创建磁盘索引
    let index_path = "./json_index";
    let index = layer.create_or_open_index(index_path)?;
    
    let mut index_writer = index.writer(50_000_000)?;
    
    // 扁平JSON测试数据集
    let test_documents = vec![
        // 文档1：用户档案（扁平结构）
        json!({
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
        }),
        
        // 文档2：企业信息（扁平结构）
        json!({
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

        }),
        
        // 文档3：电商产品（扁平结构）
        json!({
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
            "test_wrong":25,
            "user_age": 80,
            "inventory_last_updated": "2024-07-21T08:30:00Z"
        }),
        
        // 文档4：学术论文（扁平结构）
        json!({
            "paper_title": "Advanced Information Retrieval Systems",
            "paper_authors": ["Dr. John Smith", "Prof. Jane Doe"],
            "paper_year": 2023,
            "test_wrong":149.99,
            "product_price": 19.99,
            "metrics_citations": 42,
            "metrics_downloads": [120, 95, 87, 76],
            "metrics_impact_factor": 2.8,
            "research_keywords": ["information retrieval", "search engines", "natural language processing"],
            "paper_published_date": "2023-05-20T12:00:00Z",
            "paper_submitted_date": "2023-02-28",
            "metrics_last_calculated": "2024-07-15T10:00:00Z"
        })
    ];
    
    // 索引所有文档
    for (i, json_data) in test_documents.iter().enumerate() {
        if let Value::Object(obj) = json_data {
            let doc = layer.process_flat_json_object(obj)?;
            index_writer.add_document(doc)?;
            println!("✅ Document {} indexed successfully", i + 1);
        }
    }
    
    index_writer.commit()?;
    
    let query_builder = SmartJsonQueryBuilder::new(layer);
    let reader = index.reader()?;
    let searcher = reader.searcher();
    
    // 综合查询测试
    println!("\n=== 🔍 Comprehensive Query Tests ===");
    
    // 基础文本查询测试
    println!("\n📝 === Text Query Tests ===");
    
    // 1. 数组中的exact查询
    println!("\n1. Array exact query for 'rust' in user_tags:");
    let query = query_builder.smart_query("user_tags", "rust")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });
    
    // 2. 数组中的exact查询 - 技术栈
    println!("\n2. Array exact query for 'python' in company_technologies:");
    let query = query_builder.smart_query("company_technologies", "python")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });
    
    // 3. 长文本分词查询
    println!("\n3. Long text token query for 'library' in product_description:");
    let query = query_builder.smart_query("product_description", "library")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });
    
    // 4. 学术关键词查询
    println!("\n4. Academic keyword query for 'information' in research_keywords:");
    let query = query_builder.smart_query("research_keywords", "information")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });
    
    // 精确匹配查询测试
    println!("\n🎯 === Exact Match Tests ===");
    
    // 5. 产品SKU精确查询
    println!("\n5. Exact query for product_sku:");
    let query = query_builder.exact_query("product_sku", "WH001234")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });
    
    // 6. 邮箱精确查询
    println!("\n6. Exact query for company_email:");
    let query = query_builder.exact_query("company_email", "contact@techinnovations.com")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });
    
    // 数值范围查询测试
    println!("\n🔢 === Number Range Query Tests ===");
    
    // 7. 带路径的价格范围查询
    println!("\n7. Price range query with path (140-160) for product_price:");
    let query = query_builder.number_range_query_with_path("product_price", 140.0, 160.0)?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });
    
    // 8. 带路径的年龄范围查询
    println!("\n8. Age range query with path (25-30) for user_age:");
    let query = query_builder.number_range_query_with_path("user_age", 25.0, 30.0)?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });
    
    // 9. 带路径的评分范围查询
    println!("\n9. Rating range query with path (4.0-5.0) for company_ratings:");
    let query = query_builder.number_range_query_with_path("company_ratings", 4.0, 5.0)?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });
    
    // 数组内容查询测试
    println!("\n📚 === Array Content Query Tests ===");
    
    // 10. 颜色数组查询
    println!("\n10. Array exact query for 'black' in inventory_colors:");
    let query = query_builder.smart_query("inventory_colors", "black")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });
    
    // 11. 评论数组查询
    println!("\n11. Array text query for 'excellent' in review_comments:");
    let query = query_builder.smart_query("review_comments", "excellent")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });
    
    // 12. 语言偏好查询
    println!("\n12. Array exact query for 'chinese' in user_languages:");
    let query = query_builder.smart_query("user_languages", "chinese")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });
    
    // 复杂查询测试
    println!("\n🔄 === Complex Query Tests ===");
    
    // 13. 城市地理查询
    println!("\n13. Geographic query for 'San Francisco' in company_city:");
    let query = query_builder.smart_query("company_city", "San Francisco")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });
    
    // 14. 作者查询 - 使用精确匹配
    println!("\n14. Author exact query for 'Dr. John Smith' in paper_authors:");
    let query = query_builder.exact_query("paper_authors", "Dr. John Smith")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });
    
    // 日期查询测试
    println!("\n📅 === Date Query Tests ===");
    
    // 15. 带路径的用户创建时间范围查询
    println!("\n15. Date range query with path for user_created_at (Jan 2024):");
    let query = query_builder.date_range_query_with_path("user_created_at", "2024-01-01T00:00:00Z", "2024-01-31T23:59:59Z")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });
    
    // 16. 带路径的产品发布日期精确查询
    println!("\n16. Date exact query with path for product_launch_date:");
    let query = query_builder.date_exact_query("product_launch_date", "2024-02-14")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });
    
    // 17. 带路径的最近更新查询
    println!("\n17. Recent updates query with path for company_last_updated (July 2024):");
    let query = query_builder.date_range_query_with_path("company_last_updated", "2024-07-01T00:00:00Z", "2024-07-31T23:59:59Z")?;
    let results = searcher.search(&*query, &TopDocs::with_limit(10))?;
    println!("   Results: {} documents found {}", results.len(), if results.len() > 0 { "✅" } else { "❌" });
    
    
    
    println!("\n💡 Index Location: './json_index' (will persist between runs)");
    
    Ok(())
} 