# 解读Fst数据结构及读写流程
_Exported on 2025/7/25 at GMT+8 14:15:22 from Cursor (1.2.4)_

---

**User**

请仔细阅读这个代码，解读一下Fst的数据结构，读写流程

---

**Cursor**



好的，我已经仔细阅读了您提供的 `tantivy-fst` 的源代码。这是一个关于**有限状态转换机 (Finite State Transducer, FST)** 的实现。FST 是一种非常高效的数据结构，尤其适用于存储大量的键值对（比如词典），并能提供极快的查询速度和极高的压缩率。

下面我将为您解读其数据结构和读写流程。

### 1. FST 是什么？

在深入代码细节之前，我们先通过代码注释里的描述来理解 FST 的核心思想：

*   **类似压缩的前缀树 (Trie)**: 您可以把 FST 想象成一个前缀树。前缀树通过共享公共前缀来压缩键。但 FST 更进一步，它不仅压缩公共前缀，还压缩公共**后缀**。
*   **键值对存储**: FST 不仅能存储键（字符串），还能给每个键关联一个值。这个值（在代码中是 `u64` 类型）通过路径上各个转换 (Transition) 的输出值累加得到。
*   **高效压缩**: 注释中提到一个例子：存储从 "0000000001" 到 "1000000000" 这十亿个字符串，构建出的 FST 只占用了 **251 字节**。如果同时将这些字符串映射到它们的整数值，也只需要 **896 字节**。这展示了 FST 惊人的压缩能力。

### 2. Fst 的数据结构

FST 的主要数据结构是 `Fst`，其定义如下：

```rust
pub struct Fst<Data = Vec<u8>> {
    meta: FstMeta,
    data: Data,
}

struct FstMeta {
    version: u64,
    root_addr: CompiledAddr, // usize, 根节点的地址
    ty: FstType,           // u64, FST 的类型
    len: usize,              // FST 中键的数量
}
```

*   `Fst<Data>`: 这是 FST 的主结构体，它是一个只读的结构。
    *   `data`: `Fst` 的核心，它是一块连续的字节数据 (`[u8]`)，存储了整个 FST 的网络结构。`Data` 是一个泛型参数，可以是 `Vec<u8>`，也可以是 `&[u8]` 等，非常灵活。
    *   `meta`: `FstMeta` 结构体，包含了 FST 的元数据，如版本号、根节点的地址、键的总数等。这些元数据使得解析 `data` 成为可能。

#### FST 的序列化格式 (data 内部结构)

`data` 字节块并非简单的键值罗列，而是将 FST 的所有节点和转换关系序列化后的结果。其大致布局如下：

1.  **文件头 (Header)**:
    *   `version` (8 字节): FST 格式版本。
    *   `type` (8 字节): 用户定义的 FST 类型。
2.  **节点数据 (Node Data)**:
    *   这是 `data` 的主体部分，包含了所有节点 (Node) 的信息。
    *   每个节点代表 FST 中的一个状态，它由以下信息组成：
        *   一个标志位，表示这是否是一个**终态节点** (Final State)。如果一个路径到达终态节点，那么这个路径上的字符就组成了一个完整的键。
        *   如果它是终态节点，会有一个关联的**最终输出值 (Final Output)**。
        *   一个按输入字节排序的**转换列表 (Transitions)**。
    *   每个**转换 (Transition)** 的结构如下:
        ```rust
        pub struct Transition {
            pub inp: u8,            // 输入字节
            pub out: Output,        // 输出值
            pub addr: CompiledAddr, // 目标节点的地址
        }
        ```
    *   `Node` 和 `Transition` 并不是独立分配内存的，`Node` 实际上是一个 "视图" (View)，它解析 `data` 中特定地址 (`CompiledAddr`) 的一小段字节来提供节点信息，这种设计避免了额外的内存分配和指针跳转，非常高效。

3.  **文件尾 (Footer)**:
    *   `len` (8 字节): FST 中键的总数。
    *   `root_addr` (8 字节): 根节点在 `data` 中的地址。

### 3. "写"流程 (构建 FST)

FST 是**不可变**的，一旦创建就不能修改。它的构建（"写"流程）由 `Builder`（定义在 `build.rs` 中）完成。虽然代码中没有 `build.rs` 的内容，但我们可以从注释和 API 中推断其工作流程：

1.  **创建 Builder**: 实例化一个 `Builder`。
2.  **按序插入**: 必须**按照字典序（lexicographical order）** 依次插入键值对。这是大多数高效 FST 构建算法的要求。
3.  **增量构建与压缩**:
    *   `Builder` 在内部维护一个临时的、未完成的 FST 结构（类似一个 Trie）。
    *   每插入一个键，它会找到与上一个键的共同前缀，然后为新键的不同后缀部分创建新的节点。
    *   最关键的一步是**后缀压缩**。当一个节点的子节点全部构建完毕后，`Builder` 会尝试将这个节点与一个**状态缓存 (state cache)** 中的已有节点进行比较。如果发现结构完全相同的节点，就会复用它，从而实现后缀共享和压缩。
    *   注释中提到，这种基于缓存的策略压缩效果很好，但不保证生成**最小**的 FST（Minimal FST），因为保证最小化通常需要消耗大量内存。
4.  **完成构建**: 所有键值对插入完毕后，`Builder` 会将内部的 FST 结构序列化成最终的 `data` 字节数组。

### 4. "读"流程 (加载与查询)

#### 加载 FST

加载一个已存在的 FST 非常轻量：

*   通过 `Fst::new(data)` 函数完成。
*   它只读取 `data` 的文件头和文件尾来填充 `FstMeta` 结构，并进行版本和格式的校验。
*   这个过程**不会**去解析和加载所有的节点，所以它几乎是瞬时的，内存开销极小。

#### 查询 (`get`)

查询一个键的值是 FST 的核心优势所在，流程如下：

```rust
// 伪代码展示 get("feb") 的流程
let mut node = fst.root(); // 从根节点开始
let mut output = 0;

// 1. 跟随 'f'
let transition_f = node.find_transition('f');
output += transition_f.output; // 累加输出值
node = fst.node(transition_f.addr); // 跳转到下一个节点

// 2. 跟随 'e'
let transition_e = node.find_transition('e');
output += transition_e.output;
node = fst.node(transition_e.addr);

// 3. 跟随 'b'
let transition_b = node.find_transition('b');
output += transition_b.output;
node = fst.node(transition_b.addr);

// 4. 检查是否是最终节点
if node.is_final() {
    output += node.final_output(); // 加上最终节点的输出值
    return Some(output);
} else {
    return None;
}
```

这个过程的特点是：
*   **速度快**: 查询时间只和键的长度（L）成正比，即 O(L)，与 FST 中总的键值对数量无关。
*   **内存效率高**: 查询过程中不需要大的内存分配。

#### 流式读取 (`stream`)

除了单点查询，`fst` 还支持高效地遍历所有键值对：

*   `fst.stream()` 方法返回一个迭代器 (`Stream`)，可以按字典序遍历 FST 中所有的键值对。
*   其内部实现了一个**深度优先搜索 (DFS)**。它使用一个栈 (`stack`) 来保存遍历状态，避免了递归，效率很高。
*   它还支持范围查询（如 `ge`, `le`）和使用自动机进行更复杂的搜索。

希望这份解读能帮助您理解 `Fst` 的工作原理。总而言之，它是一个通过共享前后缀将大量有序键值对压缩成一个有向无环图，并能在此图上进行极快查询的强大数据结构。

---

**User**

请阅读termdict目录下的代码，梳理fst_termdict和sstable_termdict分别是干什么的

---

**Cursor**



好的，我们来分析 `tantivy/src/termdict` 目录下的代码，以理解 `fst_termdict` 和 `sstable_termdict` 的作用。

首先，`termdict` 是 "Term Dictionary" 的缩写，也就是**词典**。在搜索引擎中，词典是核心组件之一，它存储了索引中所有的**词元 (Term)**，并能快速查找到每个词元对应的元数据（比如它在倒排索引中的位置）。

为了理解 `fst_termdict` 和 `sstable_termdict`，我先阅读了 `src/termdict/mod.rs` 文件，这个文件定义了词典的核心抽象。

```rust
// src/termdict/mod.rs

// ...
/// The term dictionary is the structure that maps `Term`s to their `TermInfo`.
pub trait TermDictionary: Send + Sync {
    /// A cursor over the entries of the dictionary.
    type Streamer: TermStreamer + Send;

    /// Returns a cursor over the all of the terms of the dictionary.
    fn stream(&self) -> Self::Streamer;

    // ... methods for range queries, automaton searches ...

    /// Returns the number of terms in the dictionary.
    fn num_terms(&self) -> usize;

    /// Returns the term associated with a given term ordinal.
    fn term_ord(&self, term_ord: Term) -> Option<Term>;

    /// Returns the term ordinal associated with a given term.
    fn term_info(&self, term: &Term) -> Option<TermInfo>;

    // ...
}

/// `TermStreamer` is an iterator over a range of terms in a `TermDictionary`.
pub trait TermStreamer: Sized {
    /// Advance the cursor to the next term.
    fn advance(&mut self) -> bool;
    // ...
}
// ...
```

从 `mod.rs` 中我们可以看出：
*   **`TermDictionary` Trait**: 这是词典的通用接口（抽象）。它定义了所有词典实现都必须具备的功能，例如：
    *   `stream()`: 返回一个可以遍历所有词的迭代器 (`TermStreamer`)。
    *   `term_info()`: 根据一个词元 (Term) 查找其元数据 (`TermInfo`)。`TermInfo` 通常包含了这个词元在倒排索引中的文档频率、位置等信息。
    *   `term_ord()`: 通过词元的顺序编号（ordinal）来查找词元。
*   **`TermStreamer` Trait**: 这是词典迭代器的通用接口，用于按字典序遍历词典中的词。

现在，我们可以把 `fst_termdict` 和 `sstable_termdict` 理解为 `TermDictionary` trait 的两种不同实现。它们用不同的底层数据结构来存储词典，各有优劣。

---

### `fst_termdict`: 基于 FST 的词典

`fst_termdict` 目录下的代码实现了一个基于 **Finite State Transducer (FST)** 的词典。

*   **核心思想**:
    *   它使用 `fst` crate 来构建一个大型的 FST。
    *   **键 (Key)** 是词典中所有的词元 (Term)，它们经过排序后被插入 FST。
    *   **值 (Value)** 是一个数字，即这个词元的**顺序编号 (Term Ordinal)**。
*   **工作流程**:
    1.  **查找 (`term_info`)**: 当需要查找一个词（比如 "apple"）时，它会在 FST 中查找这个键。
    2.  如果找到了，FST 会返回与之关联的值，也就是 "apple" 的 Term Ordinal (例如 `123`)。
    3.  `fst_termdict` 内部还维护了另一个数据结构（通常是一个 `Vec<TermInfo>`），这个结构按 Term Ordinal 排序。它用上一步得到的 Ordinal (`123`) 作为索引，直接从这个 `Vec` 中 O(1) 地取出 `TermInfo`。
*   **优点**:
    *   **极高的内存效率**: FST 通过共享前后缀，能以非常小的空间存储海量的词元。
    *   **快速查找**: 在 FST 中查找一个词的速度与词的长度成正比，与词典大小无关。
    *   **支持高级搜索**: FST 天然支持前缀搜索、模糊搜索和正则表达式搜索等复杂查询。
*   **缺点**:
    *   构建过程相对复杂，且要求插入的词元必须是**有序的**。
    *   FST 本身是不可变的，一旦构建完成就不能修改。

**总结**: `fst_termdict` 是一种**只读、内存优化、功能强大**的词典实现，非常适合用于已经生成好的、不再改变的索引段 (Segment)。

---

### `sstable_termdict`: 基于 SSTable 的词典

`sstable_termdict` 目录下的代码实现了一个基于 **Sorted String Table (SSTable)** 的词典。`sstable` 是 `tantivy` 的另一个子 crate，它提供了一种在磁盘上存储有序键值对的结构。

*   **核心思想**:
    *   它将词典数据存储在多个块 (Block) 中。每个块包含了一系列有序的词元和它们对应的 `TermInfo`。
    *   为了能快速定位到包含某个词元的块，它还维护了一个**块索引 (Block Index)**。这个索引存储了每个块的第一个词元，使得可以通过二分查找快速定位。
*   **工作流程**:
    1.  **查找 (`term_info`)**: 当查找一个词（比如 "orange"）时：
    2.  首先在内存中的**块索引**上进行二分查找，找到可能包含 "orange" 的那个块。
    3.  然后从磁盘（或文件缓存）中加载这一个数据块。
    4.  最后在这个块内部再次进行二分查找，找到 "orange" 对应的 `TermInfo`。
*   **优点**:
    *   **结构相对简单**: 实现比 FST 更直接。
    *   **流式写入友好**: 适合在构建索引时流式地写入有序的词元。
    *   **良好的读取性能**: 通过两级索引结构，大部分查找只需要一次磁盘 I/O（如果块未被缓存）。
*   **缺点**:
    *   **内存占用**通常比 FST 要高，因为它不能像 FST 那样极致地压缩共享部分。
    *   高级搜索（如模糊搜索）不如 FST 那样原生和高效。

**总结**: `sstable_termdict` 是一种**为磁盘存储优化、结构清晰**的词典实现。它在性能和资源占用之间取得了很好的平衡，同样适用于只读的索引段。

### 两者关系与用途

`fst_termdict` 和 `sstable_termdict` 都是 `tantivy` 中用于**持久化存储**词典的方式。它们都实现了 `TermDictionary` trait，因此可以互换使用。在 `tantivy` 的索引结构中，一个段 (Segment) 在生成时，会将其词典序列化到磁盘，这时就可以选择其中一种格式。

*   从代码和社区的讨论来看，`fst_termdict` 通常是更现代和默认的选择，因为它提供了极致的压缩率和更丰富的功能。
*   `sstable_termdict` 可能作为一种备选方案或在某些特定场景下使用。

两者共同服务于 `tantivy` 的核心目标：提供一个高性能、高效率的全文搜索引擎。