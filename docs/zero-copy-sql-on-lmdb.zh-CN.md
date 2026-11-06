# 基于 LMDB 实现零拷贝的 SQL 计算

[English](zero-copy-sql-on-lmdb.md) | 中文

> 数据库把一行数据交到应用程序手里之前，字符串通常已经被拷贝过好几次：从文件到缓存，从缓存到执行器，再从执行器到你的结构体。这篇文章讨论的是另一种可能：如果存储层是 LMDB 这样基于 mmap 的 KV 引擎，只读事务读到的页在事务结束前地址不变，那么 SQL 引擎能不能让查询结果一路借用这些页，让完整的 SQL 计算做到零拷贝？

## Summary

SQLite 是应用进程内最常见的 SQL 数据库，在常见语言中都有稳定的绑定 API，这里以 Rust 的 rusqlite 为例。一个很常见的场景是：取出一个帖子下的全部评论，映射成结构体，按 `parent_id` 组装成回复树，再序列化成嵌套的 JSON 写给调用方：

```rust
use rusqlite::{params, Connection};
use serde::Serialize;
use std::collections::HashMap;
use std::error::Error;
use std::io::Write;

#[derive(Serialize)]
struct Post {
    id: i64,
    thread_id: i64,
    parent_id: i64, // 0 表示顶层评论
    user: String,
    body: String,
}

#[derive(Serialize)]
struct Comment<'a> {
    #[serde(flatten)]
    post: &'a Post,
    replies: Vec<Comment<'a>>,
}

fn build<'a>(children: &'a HashMap<i64, Vec<Post>>, parent: i64) -> Vec<Comment<'a>> {
    let Some(posts) = children.get(&parent) else { return Vec::new() };
    posts.iter()
        .map(|post| Comment { post, replies: build(children, post.id) })
        .collect()
}

fn write_thread(conn: &Connection, thread_id: i64, out: impl Write) -> Result<(), Box<dyn Error>> {
    let mut stmt = conn.prepare_cached("select id, thread_id, parent_id, user, body from posts where thread_id = ?1")?;
    let mut rows = stmt.query(params![thread_id])?;
    // 建树要等所有行到齐，而 row 只活到下一次 next，所以 Post 只能拥有 String
    let mut children: HashMap<i64, Vec<Post>> = HashMap::new();
    while let Some(row) = rows.next()? {
        let post = Post {
            id: row.get(0)?,
            thread_id: row.get(1)?,
            parent_id: row.get(2)?,
            user: row.get(3)?,
            body: row.get(4)?,
        };
        children.entry(post.parent_id).or_default().push(post);
    }
    serde_json::to_writer(out, &build(&children, 0))?;
    Ok(())
}
```

这样一段再普通不过的"取行、映射成结构体"的代码，数据要经过下面这些步骤，才能变成你手里的 `Post`：

```text
 数据库文件
     │
     │  默认：read() 把整页拷进 SQLite 的 page cache    [拷贝]
     │  - 开启 mmap_size：直接读映射进来的文件页
     ▼
 B 树页（page cache 或文件映射）
     │
     │  sqlite3_step → OP_Column 从记录中解出 user / body
     │    - 在页内：拷进寄存器缓冲区                    [拷贝]
     │    - 跨溢出页：malloc 缓冲区，逐页拼接            [分配][拷贝]
     │  有 ORDER BY / GROUP BY 时：写进 sorter 再读出    [拷贝]
     ▼
 寄存器缓冲区
     │
     │  sqlite3_column_text → rusqlite row.get()
     │  指针只活到下一次 next，Post 只能拥有一份拷贝
     │  user / body 各自变成 String                     [分配][拷贝] × 2
     ▼
 Post { user: String, body: String, .. }
     │
     │  按 parent_id 建树，递归写出
     │  serde_json::to_writer 写出 JSON
     ▼
 JSON
```

在这样的过程里，SQLite 和它的 API 约定都要求字符串换一个地方存放。字段越多、字符串越长、查询越复杂，这条路上的拷贝和分配就越多。这其实也是常见数据库的标准做法：自己管理 buffer pool，在用户态掌控内存，不让操作系统决定哪些页留在内存里。代价是 pool 里的页随时可能被替换，数据要交出去，就必须先拷贝一份。如果像 DBOS [1] 设想的那样，把数据库放进操作系统的内核，由它来管理操作系统的状态，用户态的 buffer pool 和内核的页缓存或许就不必再分成两层。

可是在常见的 Web 应用里，很多查询的主要工作其实只是数据的加工和传递：从 SQL 里取出数据，经过一些简单的处理转换成对应的结构体（Java 里的 POJO），再序列化成 JSON，通过 HTTP 返回。GraphQL [2] 把这件事做成了接口规范：调用方描述想要的字段和嵌套结构，服务端照着这个形状把数据库里的列取出来、组装好、序列化出去，几乎不做别的。这类几乎纯读取和重排的工作，在 Web 后端里占的比例越来越大。

在这样的情况下，`Post` 的每个 `String` 字段都要分配一次，字段越多，分配越多。再回头看刚才的流程图，你会发现字符串一共被拷贝了两次：第一次是 SQLite 在 `step` 取列时，把它从页里拷进寄存器缓冲区（即使启用了 mmap，这一次拷贝也省不掉）；第二次是映射成 `Post` 时，又从寄存器缓冲区拷进各个字段。但 `row.get_ref` 明明能直接借出 `&str`，让 `Post` 的字段借用它，不就省掉第二次拷贝了吗？遗憾的是，`row` 只活到下一次 `next`，借用它的 `Post` 带不出这一行，编译器会直接拒绝。而建树要等所有行到齐，SQL 的排序和 JSON 函数都给不出深度不定的嵌套结构，这些行必须同时拿在手里。这次拷贝来自 SQLite API 的约定，靠上层的写法很难绕开。

## 实现从存储到返回的零拷贝计算

### MMAP Storage

Andy Pavlo 说过，MMAP 会毁掉你的数据库和你的生活 [3]。这是他和 Crotty、Leis 那篇 CIDR 2022 论文被否掉的候选标题之一，最后定名为 *Are You Sure You Want to Use MMAP in Your Database Management System?*。论文列举了在数据库里用 mmap 替代 buffer pool 的四类问题：事务安全、IO 停顿、错误处理、性能。正如标题所说，这里并没有直接调用 mmap 自己实现存储，而是用 LMDB 作为存储后端。LMDB 是一个成熟的嵌入式 KV 存储引擎，它的设计规避了论文里最致命的事务安全问题 [4]：

- **默认只读映射。** 文件以只读方式映射，进程不能通过指针修改它。宿主程序的野指针写不坏数据库，映射里也不存在会被内核擅自写回磁盘的脏页；试图修改只读事务返回的值，会直接触发 SIGSEGV。
- **写入不经过映射。** 写事务要修改的页先在堆内存里改好，提交时用 `pwrite` 写回文件，再按配置 `fsync`。什么时候落盘、按什么顺序落盘，由 LMDB 自己决定。
- **写时复制的 B+ 树（影子分页）。** 已提交的页从不原地修改：改一个叶子，就写一份新页，并沿路径一直复制到根。
- **两个元数据页交替使用。** 提交的最后一步是写元数据页，记下新的根。两份交替写，崩溃后总有一份是完整的，数据库不会停在写了一半的状态。
- **读者表保护快照。** 每个只读事务都登记自己看到的版本，它可能读到的旧页不会被回收复用。所以只读事务读到的值，在事务结束前一直有效、地址不变，这正是 **零拷贝 SQL 计算** 所依赖的保证。
- **单写者。** 同一时刻只有一个写事务，论文也提到 LMDB 正是靠这一点避免了事务之间的冲突。

当然，论文里的另一些问题 LMDB 并没有解决：缺页时线程会同步阻塞，IO 错误会变成 SIGBUS，哪些页留在内存由内核决定。LMDB 的前提是热数据放得进内存，而在 SQLite 所面向的嵌入式场景里，这个前提通常是成立的。

### Data Encode

在 KV 存储之上实现 SQL，第一步是决定一行数据怎么变成 key 和 value。TiDB 的做法很有代表性 [5]：

```text
行：  t{TableID}_r{RowID}                       →  [col1, col2, col3, ...]
索引：t{TableID}_i{IndexID}_{索引列的值}_{RowID}  →  null
```

同一张表的行共享同一个前缀，按主键排好序，全表扫描和索引查询都变成了一段连续 key 的范围扫描。这里的实现也是同样的结构：key 是"表 ID + 类型标记 + 主键"，value 是这一行的各列。

对零拷贝来说，关键在 value 里字符串怎么存。以后面测试里用到的 `orders` 表为例，插入 `(1, 7, 'paid', 42, 'hello')` 这一行，它的 value 是这样的：

```text
  00                               空值位图
  01 00 00 00                      id = 1
  07 00 00 00                      customer_id = 7
  04 00 00 00 70 61 69 64          status：长度 4 + "paid"
  2a 00 00 00                      amount = 42
  05 00 00 00 68 65 6c 6c 6f       note：长度 5 + "hello"
  00 00 00 00 00 00 00 00          语句戳：写入这一行的语句编号，更新时用来跳过本语句刚写入的行
```

字符串以"长度 + 原始字节"的形式连续存放。解码 `note` 时只要读出 4 字节的长度，就得到一段指向 LMDB 页的（指针，长度），正好就是一个 `&str`，不需要任何转换。LMDB 又保证大值所在的溢出页是连续的，所以无论字符串多长，这段字节在映射里都是连续的一整块，可以直接借出去。注意一行的所有列是一个 LMDB value：LMDB 把放不进叶子的 value 分配在一段连续的溢出页上，所以不只是某个字段连续，而是整行连续，`status` 和 `note` 只是同一段字节里的两个偏移。

把大值放到单独的页上，和 PostgreSQL 的 TOAST、InnoDB 的外部页是同一个思路，解决的是行膨胀，让只读小列的查询不用碰大值。但这些机制不解决拷贝：值取回应用时照样要拷。真正不拷的做法是像 FILESTREAM 那样把值变成文件句柄交出去，可那样它就不再是 SQL 值了，引擎里不能再对它过滤、排序、求长度，和行的事务快照也对不齐。这里的区别在于，溢出页仍然在同一个映射、同一个只读事务里，算子和调用方拿到的是同一段字节。

### Execution

字符串能从存储里借出来，只是第一步。一条 SQL 要经过扫描、过滤、排序、分组、join 等一连串算子，才会把结果交给调用方，其中任何一层把字符串拷贝一份，零拷贝就在那里断掉。这是一种风格：存储给出一个生命周期，算子不打断它，Rust 的借用检查让这件事能在编译期得到保证。

**值的表示：要么拥有，要么借用。** 字符串和二进制值都用同一个类型表示（简化后）：

```rust
pub struct MaybeMapped<B: ?Sized>(Repr<B>);

enum Repr<B: ?Sized> {
    Owned(B::Owned),     // 自己拥有的内存：SQL 里的字面量、表达式算出来的新值
    Mapped { ptr, len }, // 一对（指针，长度），指向只读事务读到的页
}
```

两种状态会出现在同一条查询里。比如 `where note = 'hello'`，常量 `'hello'` 是 `Owned`，列值 `note` 是 `Mapped`。

至于列值什么时候是 `Mapped`，取决于事务。只读事务开始时声明"读到的字节在事务结束前一直有效、地址不变"，行解码就直接产出 `Mapped`；写事务无法保证这一点，解码时只能拷贝，产出 `Owned`。

**物化算子只拷贝指针。** 物化算子必然要收集上游算子的行：排序要攒齐所有行，分组要保存分组键，hash join 要建哈希表，窗口函数要留住整个分区。它们保存值时，`Mapped` 字符串只拷贝（指针，长度），不拷贝内容。整条查询都在同一个只读事务里，这些页不会失效，所以只要没有触发 spill，物化算子就能在内存容器里以借用的方式保存值。只有少数情况会产生新的字符串：表达式算出来的值（比如 `upper(note)`），以及数据量大到需要 spill 时，序列化写出再读回来的值。理论上 spill 出去的文件也可以用 mmap 读回并继续借用，但这是一项额外的做法。

**借用的期限：对外靠编译器，对内靠约定。** 借用的风险在于指针活得比页久，这里分两层处理：

- 交给调用方的结果，与事务的生命周期绑定。结论里的 `Post` 就是这样：它可以活过某一行，一直用到事务结束，但想把它带出事务，编译器会直接拒绝。
- 执行器内部的借用靠约定。同一条预编译的语句可以在不同事务里反复执行，所以每次执行开始前，算子都要清空上一次留下的借用值。这一层编译器检查不到，由 debug 构建下的存活检查兜底：读到已结束事务的借用值会直接 panic。

## Benchmarking SQL Execution: SQLite with mmap vs. LMDB SQL

下面的测试里，两边装载完全相同的数据、建同样的索引、执行同样的 SQL：语句只准备一次，所有执行都在同一个只读事务里完成。SQLite 开启了 `PRAGMA mmap_size`（1 GiB，大于数据库文件，整个文件都被映射），所以两边都通过内存映射读数据库文件，IO 这一层的条件是相同的。

- **LMDB SQL**：跑在 LMDB 只读事务上的 SQL 引擎，字符串从存储一路借用到调用方。
- **LMDB SQL（强制拷贝）**：同一条查询放进写事务执行，字符串会被拷贝，用作对照组。
- **SQLite**：SQLite 3.45.1，开启 mmap。

分配量在 libc 的 `malloc` 处统计。测试机为 i9-13900HX，程序绑定在同一个 P 核（`cpu8`）上运行。

测试的目的不是证明哪边更快，而是找出拷贝的成本从什么长度开始显现。所以字符串长度从 64 B 一直拉到 256 KiB：64 B 是 SQLite 领先的一档，256 KiB 则远超一般业务表里一列的大小，中间几档才是常见的文档、日志和 JSON 字段。

表 1 到表 3 使用下面的 `orders` 表。`note` 的长度从 64 B 到 256 KiB 分档装载，每档单独建库；每次查询按 `(customer_id, status)` 命中约 10 行。

```sql
create table orders (
    id          int primary key,
    customer_id int,
    status      varchar,  -- 'new' / 'paid' / 'shipped' / 'done' / 'void'
    amount      int,
    note        varchar
);
create index orders_customer_status on orders (customer_id, status);
```

**表 1：普通索引查询**

按索引取出约 10 行，调用方读取每个字符串。复现：`examples/alloc_trace.rs` [6]。

```sql
select id, status, amount, note from orders
where customer_id = $1 and status = $2 and amount > $3;
```

| note 长度 | LMDB SQL | LMDB SQL（强制拷贝） | SQLite |
| ---: | ---: | ---: | ---: |
| 64 B | 3.74 / 0.4 | 5.22 / 1.1 | 2.99 / 0 |
| 1 KiB | 3.70 / 0.4 | 5.33 / 10 | 3.44 / 0 |
| 4 KiB | 4.37 / 0.4 | 8.00 / 40 | 6.44 / 40 |
| 16 KiB | 3.65 / 0.4 | 13.07 / 160 | 11.60 / 158 |
| 64 KiB | 2.56 / 0.4 | 18.57 / 640 | 18.05 / 628 |
| 256 KiB | 2.38 / 0.4 | 61.58 / 2560 | 66.14 / 2458 |

单位：每格为"p50 耗时 µs / 每次执行分配 KiB"。

**表 2：长度函数**

查询条件和数据与表 1 相同，只把投影换成长度函数。第一列是取出 `note` 后由调用方对字符串取 `len()`；`char_length` 在 SQLite 里对应 `length()`。

| note 长度 | `select note`，调用方取 `len()` | `octet_length(note)` | `char_length(note)` |
| ---: | ---: | ---: | ---: |
| 64 B | 3.56 / 2.52 | 3.81 / 2.51 | 3.93 / 2.77 |
| 4 KiB | 4.33 / 6.87 | 4.77 / 2.93 | 7.34 / 16.07 |
| 64 KiB | 2.35 / 20.35 | 2.75 / 1.95 | 26.07 / 164.01 |
| 256 KiB | 2.14 / 69.03 | 2.35 / 1.82 | 89.25 / 627.00 |

单位：每格为"LMDB SQL / SQLite 的 p50 耗时 µs"。

**表 3：多重物化算子下的数据分配与计算性能**

读取与表 1 相同的行，但让字符串先后经过分组、窗口函数和排序三个物化算子。复现：`examples/alloc_trace.rs` 中的 `multi-op` 变体 [6]。

```sql
select status, note, peak, max(peak) over (partition by status)
from (select status, note, max(amount) as peak from orders
      where customer_id = $1 and status = $2 and amount > $3
      group by status, note) t
order by note desc;
```

| note 长度 | LMDB SQL | SQLite |
| ---: | ---: | ---: |
| 64 B | 7.91 / 4.2 | 12.74 / 109 |
| 4 KiB | 13.82 / 4.2 | 33.19 / 757 |
| 64 KiB | 84.74 / 4.1 | 328.34 / 10846 |
| 256 KiB | 320.24 / 4.0 | 7479.64 / 32945 |

单位：每格为"p50 耗时 µs / 每次执行分配 KiB"。

**表 4：100 篇 JSON / CSV 文档的解析**

每次按 `grp` 取出 100 篇文档，把结果保留到查询结束之后，再解析成结构体。LMDB SQL 的结构体字段直接借用事务里的文本；SQLite 按常见写法，每个字段一个 `String`。CSV 场景用结构相同的 `csv_docs` 表，`body` 存一行 CSV。复现：`examples/borrowed_parse.rs` [7]。

```sql
create table json_docs (
    id   int primary key,
    grp  int,      -- 每组 100 篇
    body varchar
);
create index json_docs_grp on json_docs (grp);

select id, body from json_docs where grp = $1;
```

| 文档大小 | 场景 | LMDB SQL | SQLite |
| ---: | --- | ---: | ---: |
| 256 B | 只保留原始行 | 28.34 / 3.5 | 24.64 / 28 |
| 256 B | 解析 JSON | 43.27 / 12 | 60.17 / 35 |
| 2 KiB | 解析 JSON | 68.80 / 12 | 97.20 / 214 |
| 16 KiB | 只保留原始行 | 28.60 / 3.5 | 137.69 / 3205 |
| 16 KiB | 解析 JSON | 187.33 / 12 | 292.65 / 3212 |
| 16 KiB | 解析 CSV | 35.15 / 12 | 172.15 / 3206 |

单位：每格为"p50 耗时 µs / 每次执行分配 KiB"，每次执行取 100 行。

### 表格分析

**LMDB SQL 的分配量和数据大小无关，SQLite 的分配量跟着返回的数据走。** 表 1 里 LMDB SQL 始终是 0.4 KiB；SQLite 从 4 KiB 起每行分配一次，总量约等于返回的字节数。SQLite 在 64 B 和 1 KiB 时显示 0，不表示没有拷贝：短字符串被拷进了一块复用的缓冲区，所以统计不到分配。检查返回的指针可以看到，LMDB SQL 的指针全部落在数据库文件的映射里，SQLite 的一个都没有。

**SQLite 在短数据上很快，但耗时随数据长度增长；LMDB SQL 的耗时和长度无关。** 64 B 时 SQLite 只要 2.99 µs，比 LMDB SQL 还快，拷贝几十个字节的代价可以忽略。随着 `note` 变长，SQLite 的耗时和分配量一起增长，到 256 KiB 时是 66.14 µs，拷贝成了主要开销。LMDB SQL 的耗时始终在 2.4 到 4.4 µs 之间，它不读字符串的内容，自然也不受长度影响。

**差距来自拷贝，不来自 IO。** 让 LMDB SQL 强制拷贝字符串后，它的耗时和分配都和 SQLite 落在同一个量级（256 KiB 时 61.6 µs 对 66.1 µs）。

**`octet_length` 两边都是常数时间，不能作为零拷贝的论据。** LMDB SQL 对借用的 `&str` 取 `len()`；SQLite 对只作为长度函数参数的列，直接从记录头读长度，不加载内容、不读溢出页（`OP_Column` 的 `OPFLAG_BYTELENARG`），所以反而更快。

**`char_length` 两边都要扫完全部字节，64 KiB 以上 LMDB SQL 快 6 到 7 倍。** LMDB SQL 直接在映射的页上计数（`chars().count()`，256 KiB 档约 28 GB/s）；SQLite 先把溢出页的内容拼进 `malloc` 的缓冲区，再逐字符计数。这是"读完全部内容、处理很轻"的典型情形：计算本身便宜，拷贝占了大头，零拷贝的收益最明显。如果写入时记下字符数或"是否纯 ASCII"，`char_length` 也能做到常数时间，目前没有实现。

**每经过一个物化算子，差距就放大一次。** 表 3 里 64 KiB 和 256 KiB 两档，SQLite 的分配量是返回数据的 13 到 17 倍，因为每个算子都要把记录写进去再读出来；LMDB SQL 只分配哈希表和排序数组用的约 4 KiB。LMDB SQL 的耗时仍然随 `note` 增长，原因是分组要对整条 `note` 求哈希。

**解析本身两边一样快。** 16 KiB 解析 JSON 时，扣掉取数据的耗时，纯解析两边差不多（159 µs 对 155 µs）。LMDB SQL 快出的部分，几乎全来自取数据时没有拷贝。

## 结论

回到一开始的代码案例中，在同样功能的代码下可以写成这样：

```rust
use kite_sql::db::{Transaction, TxnResultIter};
use kite_sql::storage::lmdb::LmdbStorage;
use kite_sql::Model;
use serde::Serialize;
use std::borrow::Cow;
use std::collections::HashMap;
use std::error::Error;
use std::io::Write;

#[derive(Default, Model, Serialize)]
#[model(table = "posts")]
struct Post<'a> {
    #[model(primary_key)]
    id: i32,
    thread_id: i32,
    parent_id: i32, // 0 表示顶层评论
    user: Cow<'a, str>,
    body: Cow<'a, str>,
}

#[derive(Serialize)]
struct Comment<'a> {
    #[serde(flatten)]
    post: &'a Post<'a>,
    replies: Vec<Comment<'a>>,
}

fn build<'a>(children: &'a HashMap<i32, Vec<Post<'a>>>, parent: i32) -> Vec<Comment<'a>> {
    let Some(posts) = children.get(&parent) else { return Vec::new() };
    posts.iter()
        .map(|post| Comment { post, replies: build(children, post.id) })
        .collect()
}

fn write_thread(tx: &mut Transaction<'_, LmdbStorage>, thread_id: i32, out: impl Write) -> Result<(), Box<dyn Error>> {
    // Post 借用 LMDB 的页，活到事务结束，所以可以先全部收齐再建树
    let mut children: HashMap<i32, Vec<Post>> = HashMap::new();
    let posts = tx
        .bind(|ctx| {
            ctx.from::<Post>()?
                .filter(|e| e.column(Post::thread_id())?.eq(thread_id))?
                .finish()
        })?
        // 每个字符串字段都是 Cow::Borrowed，直接指向 LMDB 的页
        .txn_orm::<Post>();
    for post in posts {
        let post = post?;
        children.entry(post.parent_id).or_default().push(post);
    }
    serde_json::to_writer(out, &build(&children, 0))?;
    Ok(())
}
```

同样的过程，换到这里是这样的：

```text
 数据库文件（LMDB 映射）
     │
     │  只读事务：页在事务结束前一直有效、地址不变
     ▼
 B+ 树页 / 溢出页（大值在映射里是连续的一整块）
     │
     │  行解码：读出 4 字节长度，得到（指针，长度）
     ▼
 Cow::Borrowed(&str)，直接指向页
     │
     │  txn_orm：放进 Post 的字段
     ▼
 Post { user: Cow::Borrowed, body: Cow::Borrowed, .. }（活到事务结束）
     │
     │  按 parent_id 建树：只移动指针
     │  serde_json::to_writer 写出 JSON
     ▼
 JSON
```

两段代码除了类型，几乎一模一样：逐行读出 `Post`，按 `parent_id` 放进 `HashMap`，最后递归建树并序列化。不同的是，这里的 map 里只有指针：`Post` 的字符串字段是 `Cow<'_, str>`，在只读事务里都是 `Cow::Borrowed`，直接指向 LMDB 的页。它们的生命周期绑在事务上而不是某一行上，所以第一行的 `Post` 可以和最后一行的一起留在 map 里等着建树，直到写出 JSON 为止，字符串一次都没有被拷贝。字段用 `Cow` 而不是 `&str`，是因为在写事务里页可能被改动，那时字段会退化成 `Cow::Owned` 的拷贝。rusqlite 那边不是不能借用，`row.get_ref` 就能借出 `&str`，但它只活到下一次 `next`；建树要等所有行到齐，借用 `row` 的 `Post` 活不过下一次 `next`，所以放进 map 的只能是拷贝。

这个优势有自己的适用范围。短行、逐行处理的负载两边差不多，SQLite 还略快；写事务里字符串照样要拷贝；TPC-C 这类负载看不出变化。聚合算子把借用的行留在手里，对应的页就要留在内存里，但这些页是 SQLite 读同样数据时也要进页缓存的干净文件页，内核可以随时回收；回收不掉的堆内存反而是零拷贝这边更少（表 3）。借用的结果活多久，只读事务就要开多久，而 LMDB 的旧页在有读者引用时不能回收，长时间持有结果又伴随大量写入，数据库文件会膨胀。

感谢阅读。文中的实现和测试代码都在 KiteSQL 仓库里：<https://github.com/KipData/KiteSQL>。

## 参考

[1] Athinagoras Skiadopoulos, Qian Li, Peter Kraft, Kostis Kaffes, Daniel Hong, Shana Mathew, David Bestor, Michael Cafarella, Vijay Gadepally, Goetz Graefe, Jeremy Kepner, Christos Kozyrakis, Tim Kraska, Michael Stonebraker, Lalith Suresh, Matei Zaharia. *DBOS: A DBMS-oriented Operating System*. PVLDB, 15(1): 21–30, 2022. doi:[10.14778/3485450.3485454](https://doi.org/10.14778/3485450.3485454). PDF: <https://www.vldb.org/pvldb/vol15/p21-skiadopoulos.pdf>

[2] GraphQL Specification, September 2025 Edition. GraphQL Foundation. <https://spec.graphql.org/September2025/>. 项目主页：<https://graphql.org/>

[3] Andrew Crotty, Viktor Leis, Andrew Pavlo. *Are You Sure You Want to Use MMAP in Your Database Management System?* CIDR 2022. 项目页：<https://db.cs.cmu.edu/mmap-cidr2022/>. PDF: <https://db.cs.cmu.edu/papers/2022/cidr2022-p13-crotty.pdf>。候选标题见 Andy Pavlo 的讲座录像 <https://www.youtube.com/watch?v=1BRGU_AS25c> 00:52 处。

[4] LMDB API 文档，Symas Corporation：<http://www.lmdb.tech/doc/group__mdb.html>。`MDB_val` 一节说明返回的值"valid only until a subsequent update operation, or the end of the transaction"，并且"commonly point into the database itself"；`MDB_WRITEMAP` 一节说明默认的只读映射能防止野指针写坏数据库。

[5] PingCAP. *TiDB Computing*，"Mapping table data to Key-Value" 一节：<https://docs.pingcap.com/tidb/stable/tidb-computing/>

[6] `examples/alloc_trace.rs`：<https://github.com/KipData/KiteSQL/blob/main/examples/alloc_trace.rs>。运行：`cargo run --release --example alloc_trace --features lmdb,orm`

[7] `examples/borrowed_parse.rs`：<https://github.com/KipData/KiteSQL/blob/main/examples/borrowed_parse.rs>。运行：`cargo run --release --example borrowed_parse --features lmdb,orm`
