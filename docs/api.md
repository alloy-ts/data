# `@alloy-ts/data` API Reference

`@alloy-ts/data` is a native TypeScript/Node.js analytics engine providing Spark/DataFusion SQL and DataFrame semantics in-process via NAPI.

---

## SessionContext

The `SessionContext` manages tables, execution state, and query execution.

### `new SessionContext()`
Constructs a new SQL session context.

### `registerCsv(name: string, path: string, hasHeader?: boolean): Promise<void>`
Registers a CSV file as a named table in the session.

### `registerParquet(name: string, path: string): Promise<void>`
Registers a Parquet file as a named table in the session.

### `registerSql(ddlSql: string): Promise<void>`
Executes DDL registration SQL (e.g., `CREATE EXTERNAL TABLE ...`).

### `execute(sql: string): Promise<QueryResult>`
Executes an ANSI/Spark SQL query string and returns a materialized `QueryResult`.

### `readCsv(path: string): Promise<DataFrame>`
Reads a CSV file directly into a lazy `DataFrame`.

---

## DataFrame

A lazy, immutable handle to a dataset plan, mirroring PySpark / Scala DataFrame APIs.

### Transformations (Synchronous / Lazy)
- `select(exprs: string[]): DataFrame`
- `selectColumns(cols: string[]): DataFrame`
- `selectExprs(exprs: string[]): DataFrame`
- `filter(predicate: string): DataFrame`
- `withColumn(name: string, expr: string): DataFrame`
- `withColumnRenamed(oldName: string, newName: string): DataFrame`
- `dropColumns(cols: string[]): DataFrame`
- `aggregate(groupExprs: string[], aggs: string[]): DataFrame`
- `sort(sortExprs: Array<{ expr: string; asc?: boolean }>): DataFrame`
- `limit(skip: number, fetch: number): DataFrame`
- `distinct(): DataFrame`
- `union(other: DataFrame): DataFrame`
- `join(right: DataFrame, joinType: string, leftKeys: string[], rightKeys: string[], filter?: string | null): DataFrame`
- `fillNull(value: string, cols?: string[]): DataFrame`

### Actions (Asynchronous / Execution)
- `collect(): Promise<QueryResult>`
- `count(): Promise<number>`
- `schema(): string`
- `logicalPlan(): string`
- `writeCsv(path: string): Promise<QueryResult>`
- `writeParquet(path: string): Promise<QueryResult>`

---

## QueryResult & Helpers

### `QueryResult`
Object representing executed query outputs:
- `columns: string[]`
- `rows: Array<Array<string | null>>`
- `rowCount: number`

### `queryResultToJson(result: QueryResult): string`
Converts a `QueryResult` to a JSON string representation containing `{ columns, rows }`.

---

## CLI Thin Wrapper & Programmable API

Low-level functions forwarding subcommands to Sail/Spark CLI runners:

- `sail(args: string[]): number`
- `sailSparkServer(options?: SailSparkServerOptions): number`
- `sailSparkRun(options?: SailSparkRunOptions): number`
- `sailSparkShell(): number`
