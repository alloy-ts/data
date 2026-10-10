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

### `readParquet(path: string): Promise<DataFrame>`
Reads a Parquet file directly into a lazy `DataFrame`.

### `readAvro(path: string): Promise<DataFrame>`
Reads an Avro file directly into a lazy `DataFrame`.

### `table(name: string): Promise<DataFrame>`
Reads a previously registered table into a `DataFrame`.

### `sqlDf(sql: string): Promise<DataFrame>`
Executes a SQL query string and returns the resulting `DataFrame` (lazy evaluation).

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
- `window(windowExprs: string[]): DataFrame`
- `sort(sortExprs: Array<{ expr: string; asc?: boolean; nullsFirst?: boolean }>): DataFrame`
- `sortBy(exprs: string[]): DataFrame`
- `limit(skip: number, fetch: number): DataFrame`
- `distinct(): DataFrame`
- `distinctOn(on: string[], select: string[], sort?: Array<{ expr: string; asc?: boolean }>): DataFrame`
- `union(other: DataFrame): DataFrame`
- `unionDistinct(other: DataFrame): DataFrame`
- `unionByName(other: DataFrame): DataFrame`
- `unionByNameDistinct(other: DataFrame): DataFrame`
- `intersect(other: DataFrame): DataFrame`
- `intersectDistinct(other: DataFrame): DataFrame`
- `except(other: DataFrame): DataFrame`
- `exceptDistinct(other: DataFrame): DataFrame`
- `join(right: DataFrame, joinType: string, leftKeys: string[], rightKeys: string[], filter?: string | null): DataFrame`
- `joinOn(right: DataFrame, joinType: string, onExprs: string[]): DataFrame`
- `repartition(partitioning: { kind: string; num: number; columns?: string[] }): DataFrame`
- `unnestColumns(columns: string[]): DataFrame`
- `fillNull(value: string, columns: string[], dataType?: string): DataFrame`
- `fillNan(value: string, columns: string[], dataType?: string): DataFrame`
- `alias(aliasName: string): DataFrame`
- `explain(verbose: boolean, analyze: boolean): DataFrame`

### Schema & Metadata Accessors
- `columns(): string[]`
- `schemaFields(): Array<{ name: string; dataType: string; nullable: boolean }>`
- `schema(): string`
- `logicalPlan(): string`

### Actions (Asynchronous / Execution)
- `collect(): Promise<QueryResult>`
- `collectPartitioned(): Promise<QueryResult[]>`
- `count(): Promise<number>`
- `cache(): Promise<DataFrame>`
- `describe(): Promise<DataFrame>`
- `toArrowIpc(): Promise<Uint8Array>`
- `show(): Promise<void>`
- `showLimit(num: number): Promise<void>`
- `toString(): Promise<string>`
- `writeCsv(path: string): Promise<QueryResult>`
- `writeParquet(path: string): Promise<QueryResult>`
- `writeTable(tableName: string): Promise<QueryResult>`

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
