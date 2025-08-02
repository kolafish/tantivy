use tantivy::schema::{
    self, Field, IndexRecordOption, Schema, SchemaBuilder, TextFieldIndexing, TextOptions,
};
use tantivy::tokenizer::{Token, Tokenizer, TokenStream};
use tantivy::{
    schema::{BytesOptions, INDEXED, FAST},
    DateTime, Index, TantivyDocument, Term,
};
use tantivy::query::{BooleanQuery, Occur, Query, TermQuery};
use tantivy::tokenizer::NgramTokenizer;

pub mod fixed_json_layer {
    use super::*;
    use serde_json::{Map, Value};
    use std::path::Path;
    use tantivy::query::{BooleanQuery, Occur, Query, TermQuery};

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

                if words.is_empty() {
                    // 如果分词结果为空（比如原始文本就是"__"），则将原始文本作为一个token
                    tokens.push(Token {
                        offset_from: 0,
                        offset_to: text.len(),
                        position: 0,
                        text: text.to_string(),
                        position_length: 1,
                    });
                } else {
                    for (position, token) in words.iter().enumerate() {
                        let prefixed_token = format!("{}{}", path_prefix, token.to_lowercase());
                        tokens.push(Token {
                            offset_from: 0,
                            offset_to: prefixed_token.len(),
                            position,
                            text: prefixed_token,
                            position_length: 1,
                        });
                    }
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

/// N-gram 分词器，保留路径前缀
#[derive(Clone)]
pub struct PathPrefixNgramTokenizer {
    path_separator: String,
    ngram_tokenizer: NgramTokenizer,
}

impl PathPrefixNgramTokenizer {
    pub fn new(path_separator: String, min_gram: usize, max_gram: usize) -> Self {
        Self {
            path_separator,
            ngram_tokenizer: NgramTokenizer::new(min_gram, max_gram, false).unwrap(),
        }
    }
}

impl Tokenizer for PathPrefixNgramTokenizer {
    type TokenStream<'a> = PathPrefixNgramTokenStream;

    fn token_stream<'a>(&mut self, text: &'a str) -> Self::TokenStream<'a> {
        PathPrefixNgramTokenStream::new(
            text,
            &self.path_separator,
            self.ngram_tokenizer.clone(),
        )
    }
}

/// PathPrefixNgramTokenizer 的 TokenStream
pub struct PathPrefixNgramTokenStream {
    tokens: Vec<Token>,
    current_index: usize,
}

impl PathPrefixNgramTokenStream {
    fn new(text: &str, path_separator: &str, mut ngram_tokenizer: NgramTokenizer) -> Self {
        let mut tokens = Vec::new();
        let mut position = 0;

        if let Some(last_sep_pos) = text.rfind(path_separator) {
            let path_prefix = &text[..last_sep_pos + path_separator.len()];
            let actual_text = &text[last_sep_pos + path_separator.len()..];

            let mut ngram_token_stream = ngram_tokenizer.token_stream(actual_text);
            while ngram_token_stream.advance() {
                let ngram_token = ngram_token_stream.token();
                let prefixed_token_text = format!("{}{}", path_prefix, ngram_token.text);
                tokens.push(Token {
                    offset_from: 0,
                    offset_to: prefixed_token_text.len(),
                    position,
                    text: prefixed_token_text,
                    position_length: 1,
                });
                position += 1;
            }
        } else {
            // No separator, just ngram the whole thing (fallback)
            let mut ngram_token_stream = ngram_tokenizer.token_stream(text);
            while ngram_token_stream.advance() {
                let ngram_token = ngram_token_stream.token().clone(); // clone to avoid borrow issues
                tokens.push(Token {
                    offset_from: ngram_token.offset_from,
                    offset_to: ngram_token.offset_to,
                    position,
                    text: ngram_token.text,
                    position_length: 1,
                });
                position += 1;
            }
        }

        Self {
            tokens,
            current_index: 0,
        }
    }
}

impl TokenStream for PathPrefixNgramTokenStream {
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

mod value_coder {
    use tantivy::DateTime;

    pub fn encode_f64(val: f64) -> [u8; 8] {
        // 将 f64 编码为保持排序性的 u64
        // 正数: sign bit 设为 1
        // 负数: 所有 bit 位取反
        let u64_val = val.to_bits();
        let sortable_u64 = if val >= 0.0 {
            u64_val | (1u64 << 63)
        } else {
            !u64_val
        };
        sortable_u64.to_be_bytes()
    }

    pub fn encode_date(val: DateTime) -> [u8; 8] {
        // 将 i64 编码为保持排序性的 u64 (Sign-flipping)
        let i64_val = val.into_timestamp_micros();
        let sortable_u64 = (i64_val as u64) ^ (1u64 << 63);
        sortable_u64.to_be_bytes()
    }
}

/// 优化版 JSON 处理层 - 扁平结构 + 自定义分词器 + 磁盘持久化
#[derive(Clone)]
pub struct FixedJsonLayer {
    text_analyzed_field: Field, // 分词文本字段（使用自定义分词器）
    text_raw_field: Field,      // 原始文本字段（raw分词器）
    text_ngram_field: Field,    // N-gram 字段，用于部分匹配
    number_field: Field,        // 数值字段
    date_field: Field,          // 日期字段
    schema: Schema,
    config: JsonLayerConfig,
    path_tokenizer: PathPrefixTokenizer, // 自定义路径前缀分词器
}

/// 配置
#[derive(Clone)]
pub struct JsonLayerConfig {
    pub path_separator: String,
    pub text_classification_rules: TextClassificationRules,
}

/// 简化版文本分类规则
#[derive(Clone)]
pub struct TextClassificationRules {
    pub identifier_patterns: Vec<regex::Regex>,
}

impl Default for JsonLayerConfig {
    fn default() -> Self {
        Self {
            path_separator: "__".to_string(),
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

/// 文本类型分类
#[derive(Debug, Clone)]
enum TextType {
    AnalyzedText, // 需要分词的文本
    Keyword,      // 短关键词
    Identifier,   // 标识符
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
                        .set_tokenizer("path_prefix") // 使用自定义分词器！
                        .set_index_option(IndexRecordOption::Basic),
                )
                .set_stored(),
        );

        let text_raw_field = schema_builder.add_text_field(
            "json_text_raw",
            TextOptions::default()
                .set_indexing_options(
                    TextFieldIndexing::default()
                        .set_tokenizer("raw")
                        .set_index_option(IndexRecordOption::Basic),
                )
                .set_stored(),
        );

        // N-gram 字段
        let text_ngram_field = schema_builder.add_text_field(
            "json_text_ngram",
            TextOptions::default().set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer("path_prefix_ngram")
                    .set_index_option(IndexRecordOption::Basic),
            ),
        );

        // 将 number_field 和 date_field 定义为 text 字段，使用 raw 分词器
        // 以便存储 `path + encoded_value` 并支持范围查询
        let number_field = schema_builder.add_bytes_field(
            "json_number",
            BytesOptions::default().set_indexed().set_fast(),
        );
        let date_field = schema_builder.add_bytes_field(
            "json_date",
            BytesOptions::default().set_indexed().set_fast(),
        );

        let schema = schema_builder.build();

        let path_tokenizer = PathPrefixTokenizer::new(config.path_separator.clone());

        Ok(FixedJsonLayer {
            text_analyzed_field,
            text_raw_field,
            text_ngram_field,
            number_field,
            date_field,
            schema,
            config,
            path_tokenizer,
        })
    }

    /// 创建或打开磁盘索引
    pub fn create_or_open_index<P: AsRef<Path>>(
        &self,
        index_path: P,
    ) -> tantivy::Result<Index> {
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
        let tokenizers = index.tokenizers();
        tokenizers.register(
            "path_prefix",
            PathPrefixTokenizer::new(self.config.path_separator.clone()),
        );
        // 注册 n-gram 分词器 (min=2, max=3)
        tokenizers.register(
            "path_prefix_ngram",
            PathPrefixNgramTokenizer::new(self.config.path_separator.clone(), 2, 3),
        );

        Ok(index)
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// 处理扁平 JSON 对象（不支持嵌套）
    pub fn process_flat_json_object(
        &self,
        json_obj: &Map<String, Value>,
    ) -> tantivy::Result<TantivyDocument> {
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
        if s.len() < 8 {
            return None;
        }
        // 检查是否包含日期格式的基本特征
        let has_date_chars = s.contains('-') || s.contains('T') || s.contains(':');
        if !has_date_chars {
            return None;
        }
        self.parse_date_formats(s)
    }

    /// 解析多种日期格式
    fn parse_date_formats(&self, s: &str) -> Option<DateTime> {
        use time::{Date, PrimitiveDateTime, Time};
        if let Ok(dt) =
            time::OffsetDateTime::parse(s, &time::format_description::well_known::Iso8601::DEFAULT)
        {
            return Some(DateTime::from_utc(dt));
        }
        if let Ok(dt) =
            PrimitiveDateTime::parse(s, &time::format_description::well_known::Iso8601::DEFAULT)
        {
            let offset_dt = dt.assume_utc();
            return Some(DateTime::from_utc(offset_dt));
        }
        if s.len() == 10 && s.matches('-').count() == 2 {
            if let Ok(date) =
                Date::parse(s, &time::format_description::parse("[year]-[month]-[day]").unwrap())
            {
                let dt = PrimitiveDateTime::new(date, Time::MIDNIGHT);
                return Some(DateTime::from_utc(dt.assume_utc()));
            }
        }
        None
    }

    /// 添加日期值
    fn add_date_value(&self, doc: &mut TantivyDocument, field_name: &str, date_time: DateTime) {
        let encoded_date = value_coder::encode_date(date_time);
        let mut path_value =
            format!("{}{}", field_name, self.config.path_separator).into_bytes();
        path_value.extend_from_slice(&encoded_date);
        doc.add_bytes(self.date_field, &path_value);
    }

    /// 简化的文本分类
    fn classify_text(&self, text: &str) -> TextType {
        for pattern in &self.config.text_classification_rules.identifier_patterns {
            if pattern.is_match(text) {
                return TextType::Identifier;
            }
        }
        if self.has_whitespace_or_punctuation(text) {
            TextType::AnalyzedText
        } else {
            TextType::Keyword
        }
    }

    /// 检查文本是否包含空格或标点符号
    fn has_whitespace_or_punctuation(&self, text: &str) -> bool {
        text.chars()
            .any(|c| c.is_whitespace() || c.is_ascii_punctuation() || !c.is_alphanumeric())
    }

    /// 添加文本值 - 智能分词策略
    fn add_text_value(
        &self,
        doc: &mut TantivyDocument,
        path: &str,
        value: &str,
        text_type: TextType,
    ) {
        let prefixed_value = format!("{}{}{}", path, self.config.path_separator, value);

        match text_type {
            TextType::AnalyzedText => {
                doc.add_text(self.text_raw_field, &prefixed_value);
                let tokens = self
                    .path_tokenizer
                    .clone()
                    .tokenize_to_strings(&prefixed_value);
                for token in tokens {
                    doc.add_text(self.text_analyzed_field, &token);
                }
                // 为可分析文本添加带路径的 n-gram 索引
                doc.add_text(self.text_ngram_field, &prefixed_value);
            }
            TextType::Keyword | TextType::Identifier => {
                doc.add_text(self.text_raw_field, &prefixed_value);
            }
        }
    }

    /// 添加数值
    fn add_number_value(&self, doc: &mut TantivyDocument, path: &str, value: f64) {
        let encoded_num = value_coder::encode_f64(value);
        let mut path_value = format!("{}{}", path, self.config.path_separator).into_bytes();
        path_value.extend_from_slice(&encoded_num);
        doc.add_bytes(self.number_field, &path_value);
    }

    /// 添加布尔值
    fn add_bool_value(&self, doc: &mut TantivyDocument, path: &str, value: bool) {
        let path_value = format!("{}{}{}", path, self.config.path_separator, value);
        doc.add_text(self.text_raw_field, &path_value);
    }
}

/// 智能查询构建器
pub struct SmartJsonQueryBuilder {
    layer: FixedJsonLayer,
}

impl SmartJsonQueryBuilder {
    pub fn new(layer: FixedJsonLayer) -> Self {
        Self { layer }
    }

    /// 智能查询: 对查询词分词，并同时搜索原文和词元
    pub fn smart_query(
        &self,
        path: &str,
        value: &str,
    ) -> tantivy::Result<Box<dyn tantivy::query::Query>> {
        let mut subqueries: Vec<(Occur, Box<dyn Query>)> = Vec::new();

        // 1. 原始字段查询（精确匹配整个查询字符串）
        let prefixed_value = format!("{}{}{}", path, self.layer.config.path_separator, value);
        let raw_term = Term::from_field_text(self.layer.text_raw_field, &prefixed_value);
        subqueries.push((
            Occur::Should,
            Box::new(TermQuery::new(raw_term, IndexRecordOption::Basic)),
        ));

        // 2. 分词字段查询 (AND查询，要求所有词元都存在)
        let mut tokenizer = self.layer.path_tokenizer.clone();
        let tokens = tokenizer.tokenize_to_strings(&prefixed_value);

        // 只有当分词结果多于一个，或者单个分词与原始值不同时，才进行分词查询
        if tokens.len() > 1 || (tokens.len() == 1 && tokens[0] != prefixed_value) {
            let mut token_queries = Vec::new();
            for token_str in tokens {
                let term = Term::from_field_text(self.layer.text_analyzed_field, &token_str);
                token_queries.push((
                    Occur::Must,
                    Box::new(TermQuery::new(term, IndexRecordOption::Basic)) as Box<dyn Query>,
                ));
            }
            if !token_queries.is_empty() {
                subqueries.push((Occur::Should, Box::new(BooleanQuery::new(token_queries))));
            }
        }

        Ok(Box::new(BooleanQuery::new(subqueries)))
    }

    /// N-gram 部分匹配查询
    pub fn ngram_query_with_path(
        &self,
        path: &str,
        value: &str,
    ) -> tantivy::Result<Box<dyn Query>> {
        use tantivy::query::{BooleanQuery, EmptyQuery, Occur, Query, TermQuery};
        use tantivy::tokenizer::Tokenizer;

        // 必须使用与索引时相同的 n-gram 配置来切分查询词
        let mut tokenizer = NgramTokenizer::new(2, 3, false)?;
        let mut token_stream = tokenizer.token_stream(value);
        let path_prefix = format!("{}{}", path, self.layer.config.path_separator);

        let mut subqueries = Vec::new();
        while token_stream.advance() {
            let ngram_text = &token_stream.token().text;
            let term_text = format!("{}{}", path_prefix, ngram_text);
            let term = Term::from_field_text(self.layer.text_ngram_field, &term_text);
            subqueries.push((
                Occur::Must,
                Box::new(TermQuery::new(term, IndexRecordOption::Basic)) as Box<dyn Query>,
            ));
        }

        if subqueries.is_empty() {
            return Ok(Box::new(EmptyQuery {}));
        }

        Ok(Box::new(BooleanQuery::new(subqueries)))
    }

    /// 精确匹配查询 (只查raw字段)
    pub fn exact_query(
        &self,
        path: &str,
        value: &str,
    ) -> tantivy::Result<Box<dyn tantivy::query::Query>> {
        let prefixed_value = format!("{}{}{}", path, self.layer.config.path_separator, value);
        let term = Term::from_field_text(self.layer.text_raw_field, &prefixed_value);
        Ok(Box::new(TermQuery::new(
            term,
            IndexRecordOption::Basic,
        )))
    }

    /// 带路径的数值范围查询
    pub fn number_range_query_with_path(
        &self,
        path: &str,
        min: f64,
        max: f64,
    ) -> tantivy::Result<Box<dyn tantivy::query::Query>> {
        use std::ops::Bound;
        use tantivy::query::RangeQuery;

        let path_prefix_bytes = format!("{}{}", path, self.layer.config.path_separator).into_bytes();

        let mut min_bytes = path_prefix_bytes.clone();
        min_bytes.extend_from_slice(&value_coder::encode_f64(min));

        let mut max_bytes = path_prefix_bytes;
        max_bytes.extend_from_slice(&value_coder::encode_f64(max));

        let min_term = Term::from_field_bytes(self.layer.number_field, &min_bytes);
        let max_term = Term::from_field_bytes(self.layer.number_field, &max_bytes);

        let range_query =
            RangeQuery::new(Bound::Included(min_term), Bound::Included(max_term));

        Ok(Box::new(range_query))
    }

    /// 带路径的日期范围查询
    pub fn date_range_query_with_path(
        &self,
        path: &str,
        start_date: &str,
        end_date: &str,
    ) -> tantivy::Result<Box<dyn tantivy::query::Query>> {
        use std::ops::Bound;
        use tantivy::query::RangeQuery;

        let start_dt = self.layer.parse_date_formats(start_date).ok_or_else(|| {
            tantivy::TantivyError::InvalidArgument(format!(
                "Cannot parse start date: {}",
                start_date
            ))
        })?;
        let end_dt = self.layer.parse_date_formats(end_date).ok_or_else(|| {
            tantivy::TantivyError::InvalidArgument(format!("Cannot parse end date: {}", end_date))
        })?;

        let path_prefix_bytes = format!("{}{}", path, self.layer.config.path_separator).into_bytes();

        let start_dt_bytes = value_coder::encode_date(start_dt);
        let mut min_bytes = path_prefix_bytes.clone();
        min_bytes.extend_from_slice(&start_dt_bytes);

        let end_dt_bytes = value_coder::encode_date(end_dt);
        let mut max_bytes = path_prefix_bytes;
        max_bytes.extend_from_slice(&end_dt_bytes);

        let start_term = Term::from_field_bytes(self.layer.date_field, &min_bytes);
        let end_term = Term::from_field_bytes(self.layer.date_field, &max_bytes);

        let range_query =
            RangeQuery::new(Bound::Included(start_term), Bound::Included(end_term));

        Ok(Box::new(range_query))
    }
}
}

fn main() -> tantivy::Result<()> {
    use fixed_json_layer::{FixedJsonLayer, SmartJsonQueryBuilder};
    use serde_json::json;
    use tantivy::collector::TopDocs;
    use tantivy::query::{BooleanQuery, Occur, Query};
    use tantivy::Searcher;

    println!("🚀 Fixed JSON Layer Example 🚀");

    // 1. 设置和索引
    let layer = FixedJsonLayer::new()?;
    let index_path = "./json_index_refined"; // 使用新的目录以避免冲突
    let index = layer.create_or_open_index(index_path)?;
    // 使用单线程写入，以保证在这个小例子中所有文档都在一个段内，使得 doc_id 连续
    let mut index_writer = index.writer_with_num_threads(1, 50_000_000)?;

    // 清理旧数据（仅为示例）
    index_writer.delete_all_documents()?;
    index_writer.commit()?;

    let test_documents = vec![
        json!({
            "user_name": "Alice Smith",
            "user_email": "alice@example.com",
            "user_age": 28,
            "user_tags": ["rust", "search", "database"],
            "product_active": true,
            "product_description": "A high-quality search engine library for Rust applications",
            "user_created_at": "2024-01-15T10:30:00Z",
        }),
        json!({
            "company_name": "Tech Innovations Inc",
            "company_country": "USA",
            "company_city": "San Francisco",
            "company_coordinates": [37.7749, -122.4194],
            "company_established_date": "2020-01-15T09:00:00Z",
            "user_age": 27
        }),
        
        // 文档3：电商产品（扁平结构）
        json!({
            "product_name": "Wireless Headphones",
            "product_sku": "WH001234",
            "product_price": 149.99,
            "review_ratings": [5, 4, 5, 3, 4],
            "review_comments": ["excellent quality", "great sound", "comfortable","library"],
            "review_verified": [true, true, false, true, true],
            "inventory_stock": 50,
            "inventory_colors": ["black", "white", "blue"],
            "company_established_date": "2020-11-15T09:00:00Z",
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
            "company_established_date": "2020-08-15",
            "paper_submitted_date": "2023-02-28",
            "metrics_last_calculated": "2024-07-15T10:00:00Z"
        })
    ];

    for (i, json_data) in test_documents.iter().enumerate() {
        if let serde_json::Value::Object(obj) = json_data {
            let doc = layer.process_flat_json_object(obj)?;
            index_writer.add_document(doc)?;
            println!("✅ Document {} indexed.", i + 1);
        }
    }
    index_writer.commit()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();
    let query_builder = SmartJsonQueryBuilder::new(layer.clone());

    // 2. 运行核心查询测试
    println!("\n🔍 Running Core Query Tests...");

    // 辅助函数，用于执行查询并打印结果
    fn run_query_and_print_results(
        searcher: &Searcher,
        schema: &Schema, // 传入 schema 用于打印
        query: Box<dyn Query>,
        description: &str,
    ) -> tantivy::Result<()> {
        println!("\n---");
        println!("💬 Query: {}", description);

        // =========================================================================
        // 阶段 1: 搜索与收集
        // - 调用 searcher.search()
        // - Tantivy 在此步骤中仅使用倒排索引和快速字段来查找匹配的文档ID。
        // - TopDocs collector 会收集评分最高的文档的地址 (DocAddress)。
        // - 此时，原始的、行存储的文档内容完全没有被读取。
        // =========================================================================
        println!("  -> [阶段 1] 开始搜索和收集... (仅使用索引)");
        let top_docs = searcher.search(&*query, &TopDocs::with_limit(5))?;
        println!("  -> [阶段 1] 完成. 找到 {} 个匹配文档的引用.", top_docs.len());

        if top_docs.is_empty() {
            return Ok(());
        }

        // =========================================================================
        // 阶段 2: 文档提取
        // - 迭代 `top_docs` 返回的结果 (一个包含 DocAddress 的Vec)。
        // - 对于每一个 DocAddress，调用 searcher.doc()。
        // - **就在 `searcher.doc()` 调用内部，Tantivy 才会访问行存 (Store)，
        //   解压并重建原始的 TantivyDocument。**
        // =========================================================================
        println!("  -> [阶段 2] 开始从行存(Store)中提取具体文档内容...");
        for (score, doc_address) in top_docs {
            // *** 读取行存数据的关键点就在下面这行代码 ***
            let doc = searcher.doc(doc_address)?;
            println!(
                "    - Doc Address: {:?}, Score: {:.4}, 内容: {}",
                doc_address,
                score,
                doc.to_json(schema) // 将文档转为JSON以便查看
            );
        }
        println!("  -> [阶段 2] 完成.");
        Ok(())
    }

    // --- 测试用例 ---
    let schema = layer.schema().clone();

    // a. 关键词查询 (精确匹配)
    let query = query_builder.smart_query("company_country", "USA")?;
    run_query_and_print_results(&searcher, &schema, query, "Keyword search for country 'USA'")?;

    // b. 分词查询
    let query = query_builder.smart_query("product_description", "search library")?;
    run_query_and_print_results(
        &searcher,
        &schema,
        query,
        "Tokenized search for 'search library' in description",
    )?;

    // c. 数组中的精确匹配
    let query = query_builder.smart_query("user_tags", "rust")?;
    run_query_and_print_results(
        &searcher,
        &schema,
        query,
        "Exact match for 'rust' in tags array",
    )?;

    // d. 数值范围查询
    let query = query_builder.number_range_query_with_path("user_age", 25.0, 30.0)?;
    run_query_and_print_results(
        &searcher,
        &schema,
        query,
        "Number range for age between 25 and 30",
    )?;

    // e. 日期范围查询
    let query = query_builder
        .date_range_query_with_path("company_established_date", "2020-01-01", "2020-12-31")?;
    run_query_and_print_results(
        &searcher,
        &schema,
        query,
        "Date range for establishment in year 2020",
    )?;

    // f. N-gram 部分词查询
    let query = query_builder.ngram_query_with_path("product_description", "librar")?;
    run_query_and_print_results(
        &searcher,
        &schema,
        query,
        "N-gram search for partial word 'librar' in 'product_description'",
    )?;

    // g. 数组数值范围查询
    let query = query_builder.number_range_query_with_path("metrics_downloads", 80.0, 90.0)?;
    run_query_and_print_results(
        &searcher,
        &schema,
        query,
        "Number range for metrics_downloads between 80 and 90",
    )?;

    // h. 组合范围查询
    let paper_year_query =
        query_builder.number_range_query_with_path("paper_year", 2023.0, 2023.0)?;
    let test_wrong_query =
        query_builder.number_range_query_with_path("test_wrong", 140.0, 150.0)?;
    let combined_query = BooleanQuery::new(vec![
        (Occur::Must, paper_year_query),
        (Occur::Must, test_wrong_query),
    ]);
    run_query_and_print_results(
        &searcher,
        &schema,
        Box::new(combined_query),
        "Combined range query for paper_year (2023) and test_wrong (140-150)",
    )?;

    println!("\n---\n💡 Index Location: '{}'", index_path);

    Ok(())
} 