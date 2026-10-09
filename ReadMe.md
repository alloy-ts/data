# Data

`@alloy-ts/data` a big-data analytics engine native addon for TypeScript
(Node.js) that exposes a Spark-compatible SQL / DataFrame API to JavaScript and
TypeScript runtimes through [NAPI‑RS](https://napi.rs). It lets you run
Spark(Sail)‑style queries **in‑process**, in Node.js, Bun, or Deno — with **no
JVM and no Python interpreter**.

The engine is built on [Apache DataFusion](https://datafusion.apache.org), a
Rust query engine that already implements a large slice of ANSI SQL plus Spark
semantics, wrapped in a thin NAPI layer (`src/sql.rs`, `src/data_frame.rs`) so
that the same `SessionContext` / `DataFrame` mental model you know from Spark is
available from TypeScript.

---

## Why NAPI instead of pyo3?

| Concern        | pyo3 (Python)      | NAPI (this repo)                    |
| -------------- | ------------------ | ----------------------------------- |
| Target runtime | CPython            | Node.js / Bun / Deno (TypeScript)   |
| Interop model  | Python C extension | Native `.node` addon                |
| Concurrency    | GIL                | No GIL; tokio runtime               |
| Distribution   | wheel              | Prebuilt platform binaries via napi |

We are explicitly **not** targeting only PySpark. The goal is to support Spark
scalar functions across the **Sail** ecosystem, and especially **tsail**
(TypeScript Sail) through a first‑class NAPI binding — which is what this repo
provides. pyo3 would bind to Python; NAPI is the correct native bridge for a
TypeScript runtime.

## Why Sail?

Sail is a re‑implementation of Spark's APIs that drops the JVM dependency.
Pairing Sail's SQL/DataFrame semantics with a NAPI frontend means:

- **One binary, many runtimes** — the same addon loads in Node, Bun, and Deno.
- **Embeddable analytics** — run SQL/DataFrame workloads inside an existing TS
  service without spinning up a separate cluster or JVM.
- **Spark muscle memory** — the DataFrame API and SQL dialect are intentionally
  Spark‑shaped, so migration is incremental.

---

## Installation

```bash
npm install @alloy-ts/data
```

```ts
import { queryResultToJson, SessionContext } from "@alloy-ts/data";

const ctx = new SessionContext();
await ctx.registerCsv("example", "data/example.csv", true);
const result = await ctx.execute("SELECT a, min(b) FROM example GROUP BY a");
console.log(queryResultToJson(result));
```

### Build from source

```bash
npm install
npm run build        # builds the native addon (napi) and packs the TS layer
npm test             # runs the test suite
```

> The native addon is produced by `npm run build:napi` into `./dist`; the
> TypeScript entry (`src/main.ts`) re‑exports it. Tests live in `src/*.test.ts`
> and run with `vite-plus/test`.

---

## Migrating from Spark

The API surface mirrors Spark's:

| Spark (PySpark / Scala)      | This binding (TS)                      |
| ---------------------------- | -------------------------------------- |
| `spark.read.csv(path)`       | `ctx.readCsv(path)`                    |
| `df.filter(...).select(...)` | `df.filter("a > 1").select(["a","b"])` |
| `df.agg(...)`                | `df.aggregate(["a"], ["min(b)"])`      |
| `spark.sql("SELECT ...")`    | `ctx.execute("SELECT ...")`            |
| `df.collect()`               | `await df.collect()` → `QueryResult`   |

Differences to note:

- **Expressions are SQL strings.** Every `filter` / `select` / `aggregate` /
  `withColumn` / join predicate takes a SQL expression string, parsed against
  the DataFrame's schema (`parse_sql_expr`). This keeps the TS API small while
  reusing DataFusion's full expression language.
- **Results are materialized as `string | null`.** A `QueryResult` carries
  `columns`, `rows` and `rowCount`; use `queryResultToJson` to get a JSON
  string. For typed Arrow data, inspect the schema directly.
- **Transformations are immutable and lazy.** Each transform returns a new
  `DataFrame`; nothing executes until `collect()` / `count()` / `write*()`.

---

## Benchmark Results

> _To be filled with concrete numbers._ A representative plan: TPC‑H‑like
> queries executed in‑process via the addon, compared against (a) PySpark on a
> local JVM and (b) the equivalent pure‑JS implementation, measuring cold‑start,
> query latency, and throughput. Contributions welcome.

---

## User Guide

### Spark DataFrame API

```ts
const ctx = new SessionContext();
const df = await ctx.readCsv("data/example.csv");

const result = await df
  .filter("a <= b")
  .aggregate(["a"], ["min(b)"])
  .sort([{ expr: "a", asc: true }])
  .limit(0, 100)
  .collect();

// result.columns -> ["a", "min(example.b)"]
// result.rows    -> [["1", "2"], ...]
```

Available `DataFrame` operations (all return a new lazy `DataFrame` unless
noted):

`select`, `selectColumns`, `selectExprs`, `withColumn`, `withColumnRenamed`,
`dropColumns`, `filter`, `aggregate`, `window`, `sort`, `sortBy`, `limit`,
`distinct`, `distinctOn`, `union`, `unionDistinct`, `unionByName`,
`unionByNameDistinct`, `intersect`, `intersectDistinct`, `except`,
`exceptDistinct`, `join`, `joinOn`, `repartition`, `unnestColumns`, `fillNull`,
`fillNan`, `alias`, `explain`, `schema`, `logicalPlan`, and the execution
methods `collect`, `collectPartitioned`, `count`, `describe`, `cache`, `show`,
`showLimit`, `toString`, `writeCsv`, `writeParquet`, `writeTable`.

### Spark SQL

```ts
const ctx = new SessionContext();
await ctx.registerCsv("example", "data/example.csv", true);

// Queries and DDL both go through execute():
await ctx.execute(
  "CREATE EXTERNAL TABLE t STORED AS PARQUET LOCATION 'data/t.parquet'",
);
const res = await ctx.execute("SELECT a, b FROM example WHERE a <= b");
```

### Functions and Operators

The reference below lists the **built‑in Spark scalar functions** that Sail
targets. ✅ supported, 🚧 planned/partial, ❌ not applicable (Java‑only). These
names are available today through `execute(sql)` (DataFusion's SQL planner) and
are progressively surfaced as first‑class DataFrame / TS helpers.

---

## Built‑in Spark Scalar Functions

Legend: ✅ supported · 🚧 planned/partial · ❌ Java‑only (not in Sail)

### Array

`array` ✅ · `array_append` ✅ · `array_compact` ✅ · `array_concat` ✅ ·
`array_contains` ✅ · `array_contains_all` ✅ (Sail exclusive) ·
`array_distinct` ✅ · `array_except` ✅ · `array_insert` ✅ · `array_intersect`
✅ · `array_join` ✅ (partial) · `array_max` ✅ · `array_min` ✅ ·
`array_position` ✅ · `array_prepend` ✅ · `array_remove` ✅ · `array_repeat` ✅
· `array_size` ✅ · `array_union` ✅ · `arrays_overlap` ✅ · `arrays_zip` ✅ ·
`flatten` ✅ · `get` ✅ · `sequence` ✅ (partial) · `slice` ✅ · `sort_array` ✅
· `shuffle` 🚧

### Bitwise

`&` ✅ · `^` ✅ · `<<` ✅ · `>>` ✅ · `>>>` ✅ · `|` ✅ · `~` ✅ · `bit_count`
✅ · `bit_get` ✅ · `bitwise_not` ✅ · `getbit` ✅ · `shiftleft` ✅ ·
`shiftright` ✅ · `shiftrightunsigned` ✅

### Collection

`aggregate` ✅ · `array_sort` ✅ · `cardinality` ✅ · `concat` ✅ · `element_at`
✅ · `exists` ✅ · `filter` ✅ · `forall` ✅ · `reduce` ✅ · `reverse` ✅ ·
`size` ✅ · `transform` ✅ · `try_element_at` ✅ · `map_filter` 🚧 ·
`map_zip_with` 🚧 · `transform_keys` 🚧 · `transform_values` 🚧 · `zip_with` 🚧

### Conditional

`coalesce` ✅ · `ifnull` ✅ · `nanvl` ✅ · `nullif` ✅ · `nullifzero` ✅ · `nvl`
✅ · `nvl2` ✅ · `when` ✅ · `zeroifnull` ✅

### Conversion

`bigint` ✅ · `binary` ✅ · `boolean` ✅ · `cast` ✅ · `date` ✅ · `decimal` ✅
· `double` ✅ · `float` ✅ · `int` ✅ · `smallint` ✅ · `string` ✅ ·
`timestamp` ✅ · `tinyint` ✅

### CSV

`from_csv` ✅ · `schema_of_csv` ✅ · `to_csv` ✅

### Datetime

`add_days` ✅ · `add_months` ✅ · `add_years` ✅ · `convert_timezone` ✅ ·
`curdate` ✅ · `current_date` ✅ · `current_timestamp` ✅ · `current_timezone`
✅ · `date_add` ✅ · `date_diff` ✅ · `date_format` ✅ · `date_from_unix_date`
✅ · `date_part` ✅ · `date_sub` ✅ · `date_trunc` ✅ · `dateadd` ✅ ·
`datediff` ✅ · `datepart` ✅ · `day` ✅ · `dayname` ✅ · `dayofmonth` ✅ ·
`dayofweek` ✅ · `dayofyear` ✅ · `extract` ✅ · `from_unixtime` ✅ ·
`from_utc_timestamp` ✅ · `hour` ✅ · `last_day` ✅ · `localtimestamp` ✅ ·
`make_date` ✅ · `make_dt_interval` ✅ · `make_interval` ✅ · `make_time` ✅ ·
`make_timestamp` ✅ · `make_timestamp_ltz` ✅ · `make_timestamp_ntz` ✅ ·
`make_ym_interval` ✅ · `minute` ✅ · `month` ✅ · `monthname` ✅ ·
`months_between` ✅ · `next_day` ✅ · `now` ✅ · `quarter` ✅ · `second` ✅ ·
`time_diff` ✅ · `time_trunc` ✅ · `timestamp_add` ✅ · `timestamp_diff` ✅ ·
`timestamp_micros` ✅ · `timestamp_millis` ✅ · `timestamp_seconds` ✅ ·
`to_date` ✅ · `to_timestamp` ✅ · `to_timestamp_ltz` ✅ · `to_timestamp_ntz` ✅
· `to_unix_timestamp` ✅ · `to_utc_timestamp` ✅ · `trunc` ✅ ·
`try_make_timestamp` ✅ · `try_make_timestamp_ltz` ✅ · `try_make_timestamp_ntz`
✅ · `try_to_timestamp` ✅ · `unix_date` ✅ · `unix_micros` ✅ · `unix_millis`
✅ · `unix_seconds` ✅ · `unix_timestamp` ✅ · `weekday` ✅ · `weekofyear` ✅ ·
`window` ✅ · `window_time` ✅ · `year` ✅ · `session_window` 🚧 ·
`try_make_interval` 🚧

### Geo

`st_asbinary` ✅ · `st_geogfromwkb` ✅ · `st_geomfromwkb` ✅ · `st_setsrid` 🚧 ·
`st_srid` 🚧

### Hash

`crc32` ✅ · `hash` ✅ · `md5` ✅ · `sha` ✅ · `sha1` ✅ · `sha2` ✅ ·
`xxhash64` ✅

### JSON

`from_json` ✅ · `get_json_object` ✅ · `json_array_length` ✅ ·
`json_object_keys` ✅ · `json_tuple` ✅ · `schema_of_json` ✅ · `to_json` ✅

### Map

`create_map` ✅ · `map` ✅ (Spark SQL only) · `map_concat` ✅ ·
`map_contains_key` ✅ · `map_entries` ✅ · `map_from_arrays` ✅ ·
`map_from_entries` ✅ · `map_keys` ✅ · `map_values` ✅ · `str_to_map` ✅

### Math

`-` ✅ · `*` ✅ · `/` ✅ · `%` ✅ · `+` ✅ · `abs` ✅ · `acos` ✅ · `acosh` ✅ ·
`asin` ✅ · `asinh` ✅ · `atan` ✅ · `atan2` ✅ · `atanh` ✅ · `bin` ✅ ·
`bround` ✅ · `cbrt` ✅ · `ceil` ✅ · `ceiling` ✅ · `conv` ✅ (Spark SQL only)
· `cos` ✅ · `cosh` ✅ · `cot` ✅ · `csc` ✅ · `degrees` ✅ · `div` ✅ (Spark
SQL only) · `e` ✅ · `exp` ✅ · `expm1` ✅ · `factorial` ✅ · `floor` ✅ ·
`greatest` ✅ · `hex` ✅ · `hypot` ✅ · `least` ✅ · `ln` ✅ · `log` ✅ ·
`log10` ✅ · `log1p` ✅ · `log2` ✅ · `mod` ✅ (Spark SQL only) · `negate` ✅ ·
`negative` ✅ · `pi` ✅ · `pmod` ✅ · `positive` ✅ · `pow` ✅ · `power` ✅ ·
`radians` ✅ · `rand` ✅ · `randn` ✅ · `random` ✅ · `random_poisson` ✅ ·
`rint` ✅ · `round` ✅ · `sec` ✅ · `sign` ✅ · `signum` ✅ · `sin` ✅ · `sinh`
✅ · `sqrt` ✅ · `tan` ✅ · `tanh` ✅ · `try_add` ✅ · `try_divide` ✅ ·
`try_mod` ✅ · `try_multiply` ✅ · `try_subtract` ✅ · `unhex` ✅ · `uniform` ✅
· `width_bucket` ✅

### Misc

`aes_decrypt` ✅ · `aes_encrypt` ✅ · `assert_true` ✅ · `bitmap_bit_position`
✅ · `bitmap_bucket_number` ✅ · `bitmap_count` ✅ · `current_catalog` ✅ ·
`current_database` ✅ · `current_schema` ✅ · `current_user` ✅ ·
`hll_sketch_estimate` ✅ · `hll_union` ✅ · `monotonically_increasing_id` ✅ ·
`raise_error` ✅ · `session_user` ✅ · `spark_partition_id` ✅ ·
`theta_difference` ✅ · `theta_intersection` ✅ · `theta_sketch_estimate` ✅ ·
`theta_union` ✅ · `try_aes_decrypt` ✅ · `try_aes_encrypt` ✅ (Sail exclusive)
· `typeof` ✅ · `user` ✅ · `uuid` ✅ · `version` ✅ · `from_avro` 🚧 (Spark SQL
only) · `from_protobuf` 🚧 (Spark SQL only) · `input_file_block_length` 🚧 ·
`input_file_block_start` 🚧 · `input_file_name` 🚧 · `schema_of_avro` 🚧 (Spark
SQL only) · `to_avro` 🚧 (Spark SQL only) · `to_protobuf` 🚧 (Spark SQL only) ·
`java_method` ❌ · `reflect` ❌ · `try_reflect` ❌

### Predicate

`!` ✅ · `!=` ✅ · `<` ✅ · `<=` ✅ · `<=>` ✅ · `=` ✅ · `==` ✅ · `>` ✅ ·
`>=` ✅ · `and` ✅ (Spark SQL only) · `equal_null` ✅ · `ilike` ✅ · `in` ✅
(Spark SQL only) · `isnan` ✅ · `isnotnull` ✅ · `isnull` ✅ · `like` ✅ · `not`
✅ (Spark SQL only) · `or` ✅ (Spark SQL only) · `regexp` ✅ · `regexp_like` ✅
· `rlike` ✅

### String

`ascii` ✅ · `base64` ✅ · `bit_length` ✅ · `btrim` ✅ · `char` ✅ ·
`char_length` ✅ · `character_length` ✅ · `chr` ✅ · `concat_ws` ✅ ·
`contains` ✅ · `decode` ✅ · `elt` ✅ · `encode` ✅ · `endswith` ✅ ·
`find_in_set` ✅ · `format_number` ✅ · `format_string` ✅ · `initcap` ✅ ·
`instr` ✅ · `is_valid_utf8` ✅ · `lcase` ✅ · `left` ✅ · `len` ✅ (Spark SQL
only) · `length` ✅ · `levenshtein` ✅ · `locate` ✅ · `lower` ✅ · `lpad` ✅ ·
`ltrim` ✅ · `luhn_check` ✅ (Spark SQL only) · `make_valid_utf8` ✅ · `mask` ✅
· `octet_length` ✅ · `overlay` ✅ · `position` ✅ · `printf` ✅ · `quote` ✅ ·
`randstr` ✅ · `regexp_count` ✅ · `regexp_extract` ✅ · `regexp_extract_all` ✅
· `regexp_instr` ✅ · `regexp_replace` ✅ · `regexp_substr` ✅ · `repeat` ✅ ·
`replace` ✅ · `right` ✅ · `rpad` ✅ · `rtrim` ✅ · `sentences` ✅ · `soundex`
✅ · `space` ✅ (Spark SQL only) · `split` ✅ · `split_part` ✅ · `startswith`
✅ · `strpos` ✅ · `substr` ✅ · `substring` ✅ · `substring_index` ✅ ·
`to_binary` ✅ · `to_char` ✅ · `to_number` ✅ · `to_varchar` ✅ · `translate`
✅ · `trim` ✅ · `try_to_binary` ✅ · `try_to_number` ✅ · `try_validate_utf8`
✅ · `ucase` ✅ · `unbase64` ✅ · `upper` ✅ · `validate_utf8` ✅ · `collate` 🚧
· `collation` 🚧

### Struct

`named_struct` ✅ · `struct` ✅

### URL

`parse_url` ✅ · `try_parse_url` ✅ · `try_url_decode` ✅ · `url_decode` ✅ ·
`url_encode` ✅

### Variant

`is_variant_null` ✅ · `parse_json` ✅ · `schema_of_variant` ✅ ·
`schema_of_variant_agg` ✅ · `to_variant_object` ✅ · `try_parse_json` ✅ ·
`try_variant_get` ✅ · `variant_get` ✅ · `variant_to_json` ✅ (Sail exclusive)

### XML

`from_xml` ✅ · `to_xml` ✅ · `xpath` ✅ · `xpath_boolean` ✅ · `xpath_double`
✅ · `xpath_float` ✅ · `xpath_int` ✅ · `xpath_long` ✅ · `xpath_number` ✅ ·
`xpath_short` ✅ · `xpath_string` ✅ · `schema_of_xml` 🚧

---

## Development

```bash
npm install
npm run build:napi   # compile the Rust addon (requires a Rust toolchain)
npm run build        # build + pack the TS layer
npm test             # vite-plus/test over src/**/*.test.ts
```

See `src/sql.rs` (SQL API: `SessionContext`, `execute`, `QueryResult`) and
`src/data_frame.rs` (DataFrame API) for the NAPI bindings.
