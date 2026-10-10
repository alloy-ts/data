# `@alloy-ts/data` API Reference

`@alloy-ts/data` provides native NAPI bindings to the Sail/DataFusion query engine for TypeScript/Node.js, offering a Spark-compatible DataFrame and SQL API.

---

## SessionContext

`SessionContext` is the entry point for executing SQL queries and loading DataFrames.

### Constructor
- `new SessionContext()`: Creates a new isolated `SessionContext`.

### Methods
- `registerCsv(name: string, path: string, hasHeader?: boolean, delimiter?: string): Promise<void>`
- `registerParquet(name: string, path: string): Promise<void>`
- `registerAvro(name: string, path: string): Promise<void>`
- `registerSql(sql: string): Promise<QueryResult>`
- `execute(sql: string): Promise<QueryResult>`
- `readCsv(path: string): Promise<DataFrame>`
- `readParquet(path: string): Promise<DataFrame>`
- `readAvro(path: string): Promise<DataFrame>`
- `table(name: string): Promise<DataFrame>`
- `sqlDf(sql: string): Promise<DataFrame>`

---

## DataFrame

`DataFrame` represents a lazy relational DataFrame.

### Projection & Transformations
- `select(exprs: string[]): DataFrame`
- `selectColumns(columns: string[]): DataFrame`
- `selectExprs(exprs: string[]): DataFrame`
- `withColumn(name: string, expr: string): DataFrame`
- `withColumnRenamed(oldName: string, newName: string): DataFrame`
- `dropColumns(columns: string[]): DataFrame`
- `filter(predicate: string): DataFrame`
- `aggregate(group: string[], aggr: string[]): DataFrame`
- `window(windowExprs: string[]): DataFrame`
- `sort(sorts: SortExpr[]): DataFrame`
- `sortBy(exprs: string[]): DataFrame`
- `limit(skip: number, fetch?: number): DataFrame`
- `distinct(): DataFrame`
- `distinctOn(on: string[], select: string[], sort?: SortExpr[]): DataFrame`
- `alias(alias: String): DataFrame`

### Set Operations & Joins
- `union(right: DataFrame): DataFrame`
- `unionDistinct(right: DataFrame): DataFrame`
- `unionByName(right: DataFrame): DataFrame`
- `unionByNameDistinct(right: DataFrame): DataFrame`
- `intersect(right: DataFrame): DataFrame`
- `intersectDistinct(right: DataFrame): DataFrame`
- `except(right: DataFrame): DataFrame`
- `exceptDistinct(right: DataFrame): DataFrame`
- `join(right: DataFrame, joinType: string, leftCols: string[], rightCols: string[], filter?: string): DataFrame`
- `joinOn(right: DataFrame, joinType: string, onExprs: string[]): DataFrame`

### Repartitioning & Null Handling
- `repartition(partitioning: Partitioning): DataFrame`
- `unnestColumns(columns: string[]): DataFrame`
- `fillNull(value: string, columns: string[], dataType?: string): DataFrame`
- `fillNan(value: string, columns: string[], dataType?: string): DataFrame`

### Inspection & Metadata
- `explain(verbose: boolean, analyze: boolean): DataFrame`
- `schema(): string`
- `columns(): string[]`
- `schemaFields(): FieldInfo[]`
- `logicalPlan(): string`

### Execution
- `cache(): Promise<DataFrame>`
- `describe(): Promise<DataFrame>`
- `count(): Promise<number>`
- `collect(): Promise<QueryResult>`
- `toArrowIpc(): Promise<Uint8Array>`
- `collectPartitioned(): Promise<QueryResult[]>`
- `show(): Promise<void>`
- `showLimit(num: number): Promise<void>`
- `toString(): Promise<string>`
- `writeCsv(path: string): Promise<QueryResult>`
- `writeParquet(path: string): Promise<QueryResult>`
- `writeTable(tableName: string): Promise<QueryResult>`

---

## Sail CLI Functions

- `sail(args: string[]): number`
- `sailSparkServer(options?: SailSparkServerOptions): number`
- `sailSparkRun(options?: SailSparkRunOptions): number`
- `sailSparkShell(options?: SailSparkShellOptions): number`
