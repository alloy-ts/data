# API Reference (`@alloy-ts/data`)

`@alloy-ts/data` provides native TypeScript bindings to the DataFusion / Sail big-data analytics engine through NAPI-RS.

---

## `SessionContext`

The `SessionContext` represents the main execution context for running SQL queries, registering datasets, and creating lazy `DataFrame` instances.

### Methods

#### `new SessionContext()`
Constructs a new `SessionContext`.

#### `execute(sql: string): Promise<QueryResult>`
Executes a SQL query or DDL statement asynchronously and returns a `QueryResult`.

#### `registerCsv(name: string, path: string, hasHeader: boolean): Promise<void>`
Registers a CSV file as an in-memory table.

#### `registerSql(sql: string): Promise<QueryResult>`
Alias for executing DDL table creation statements.

#### `readCsv(path: string): Promise<DataFrame>`
Reads a CSV file lazy table into a `DataFrame`.

#### `readParquet(path: string): Promise<DataFrame>`
Reads a Parquet file into a `DataFrame`.

#### `readAvro(path: string): Promise<DataFrame>`
Reads an Avro file into a `DataFrame`.

#### `table(name: string): Promise<DataFrame>`
Loads a previously registered table into a `DataFrame`.

#### `sqlDf(sql: string): Promise<DataFrame>`
Executes a SQL query and returns the lazy `DataFrame` without evaluating rows.

---

## `DataFrame`

`DataFrame` represents a lazily-evaluated relational query plan over Apache Arrow record batches.

### Projection & Transformation
* `select(exprs: string[]): DataFrame`
* `selectColumns(columns: string[]): DataFrame`
* `selectExprs(exprs: string[]): DataFrame`
* `withColumn(name: string, expr: string): DataFrame`
* `withColumnRenamed(oldName: string, newName: string): DataFrame`
* `dropColumns(columns: string[]): DataFrame`
* `alias(alias: String): DataFrame`

### Filtering, Grouping & Sorting
* `filter(predicate: string): DataFrame`
* `aggregate(group: string[], aggr: string[]): DataFrame`
* `window(windowExprs: string[]): DataFrame`
* `sort(sorts: SortExpr[]): DataFrame`
* `sortBy(exprs: string[]): DataFrame`
* `limit(skip: number, fetch?: number): DataFrame`
* `distinct(): DataFrame`
* `distinctOn(on: string[], select: string[], sort?: SortExpr[]): DataFrame`

### Set Operations & Joins
* `union(right: DataFrame): DataFrame`
* `unionDistinct(right: DataFrame): DataFrame`
* `unionByName(right: DataFrame): DataFrame`
* `unionByNameDistinct(right: DataFrame): DataFrame`
* `intersect(right: DataFrame): DataFrame`
* `intersectDistinct(right: DataFrame): DataFrame`
* `except(right: DataFrame): DataFrame`
* `exceptDistinct(right: DataFrame): DataFrame`
* `join(right: DataFrame, joinType: string, leftCols: string[], rightCols: string[], filter?: string): DataFrame`
* `joinOn(right: DataFrame, joinType: string, onExprs: string[]): DataFrame`

### Cleaning & Reshaping
* `repartition(partitioning: Partitioning): DataFrame`
* `unnestColumns(columns: string[]): DataFrame`
* `fillNull(value: string, columns: string[], dataType?: string): DataFrame`
* `fillNan(value: string, columns: string[], dataType?: string): DataFrame`

### Introspection
* `schema(): string`
* `columns(): string[]`
* `schemaFields(): FieldInfo[]`
* `logicalPlan(): string`
* `explain(verbose: boolean, analyze: boolean): DataFrame`

### Execution & Materialization
* `cache(): Promise<DataFrame>`
* `describe(): Promise<DataFrame>`
* `count(): Promise<number>`
* `collect(): Promise<QueryResult>`
* `toArrowIpc(): Promise<Uint8Array>`
* `collectPartitioned(): Promise<QueryResult[]>`
* `show(): Promise<void>`
* `showLimit(num: number): Promise<void>`
* `toString(): Promise<string>`
* `writeCsv(path: string): Promise<QueryResult>`
* `writeParquet(path: string): Promise<QueryResult>`
* `writeTable(tableName: string): Promise<QueryResult>`

---

## CLI & Programmable NAPI API

The Sail CLI can be executed directly or programmatically through NAPI exported functions:

```ts
import {
  sail,
  sailSparkServer,
  sailSparkShell,
  sailSparkRun,
  sailSparkMcpServer,
  sailFlightServer,
} from "@alloy-ts/data";

// Direct command line runner
sail(["spark", "server", "--ip", "127.0.0.1", "--port", "50051"]);

// Programmable API entrypoints
sailSparkServer({ ip: "127.0.0.1", port: 50051 });
sailSparkShell();
sailSparkRun({ file: "script.py" });
sailSparkMcpServer({ host: "127.0.0.1", port: 8000, transport: "sse" });
sailFlightServer({ ip: "127.0.0.1", port: 32010 });
```
