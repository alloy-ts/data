import { expect, test } from "vite-plus/test";
import { writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SessionContext } from "./main.ts";

// --- helpers ---------------------------------------------------------------

function writeTempCsv(name: string, content: string): string {
  const path = join(tmpdir(), name);
  writeFileSync(path, content, "utf8");
  return path;
}

const SAMPLE_CSV = "a,b,c\n1,2,3\n4,5,6\n7,8,9\n";

async function exampleDf(ctx: SessionContext) {
  const path = writeTempCsv("df_example.csv", SAMPLE_CSV);
  return ctx.readCsv(path);
}

// --- tests -----------------------------------------------------------------

test("readCsv + filter + select (sync transforms, async collect)", async () => {
  const ctx = new SessionContext();
  const df = await exampleDf(ctx);

  // filter/select are synchronous and return a new DataFrame; collect is async.
  const res = await df.filter("a > 1").select(["a", "b"]).collect();

  expect(res.columns).toEqual(["a", "b"]);
  expect(res.rows).toEqual([
    ["4", "5"],
    ["7", "8"],
  ]);
});

test("selectColumns keeps only the named columns", async () => {
  const ctx = new SessionContext();
  const df = await exampleDf(ctx);

  const res = await df.selectColumns(["b", "c"]).collect();
  expect(res.columns).toEqual(["b", "c"]);
  expect(res.rowCount).toBe(3);
});

test("selectExprs projects SQL expressions", async () => {
  const ctx = new SessionContext();
  const df = await exampleDf(ctx);

  const res = await df.selectExprs(["a", "b * 2 AS b2"]).collect();
  expect(res.columns).toEqual(["a", "b2"]);
  expect(res.rows[0]).toEqual(["1", "4"]);
});

test("withColumn adds a computed column", async () => {
  const ctx = new SessionContext();
  const df = await exampleDf(ctx);

  const res = await df.withColumn("ab", "a * b").collect();
  expect(res.columns).toContain("ab");
  // First row: a=1, b=2 -> ab="2"
  expect(res.rows[0]).toContain("2");
});

test("withColumnRenamed renames a column", async () => {
  const ctx = new SessionContext();
  const df = await exampleDf(ctx);

  const res = await df.withColumnRenamed("a", "col_a").collect();
  expect(res.columns).toEqual(["col_a", "b", "c"]);
});

test("dropColumns drops specified columns", async () => {
  const ctx = new SessionContext();
  const df = await exampleDf(ctx);

  const res = await df.dropColumns(["c"]).collect();
  expect(res.columns).toEqual(["a", "b"]);
});

test("aggregate + limit", async () => {
  const ctx = new SessionContext();
  const df = await exampleDf(ctx);

  const res = await df
    .aggregate(["a"], ["min(b)"])
    .limit(0, 2)
    .collect();

  expect(res.columns.length).toBe(2);
  expect(res.rows.length).toBeLessThanOrEqual(2);
});

test("sort orders rows (descending)", async () => {
  const ctx = new SessionContext();
  const df = await exampleDf(ctx);

  const res = await df.sort([{ expr: "a", asc: false }]).collect();
  // After descending sort by a, the last row holds the smallest a.
  expect(res.rows[res.rows.length - 1][0]).toBe("1");
});

test("distinct removes duplicate rows", async () => {
  const ctx = new SessionContext();
  const path = writeTempCsv(
    "df_dup.csv",
    "a,b\n1,2\n1,2\n3,4\n",
  );
  const df = await ctx.readCsv(path);

  const res = await df.distinct().collect();
  expect(res.rowCount).toBe(2);
});

test("union combines two DataFrames (preserving duplicates)", async () => {
  const ctx = new SessionContext();
  const left = await exampleDf(ctx);
  const right = await exampleDf(ctx);

  const res = await left.union(right).collect();
  expect(res.rowCount).toBe(6);
});

test("join combines two DataFrames on equi-columns", async () => {
  const ctx = new SessionContext();
  const left = await exampleDf(ctx);
  const right = await ctx.readCsv(
    writeTempCsv("df_right.csv", "a2,b2\n1,20\n4,50\n"),
  );

  const res = await left
    .join(right, "inner", ["a"], ["a2"], null)
    .collect();

  // left a in {1,4,7} matches right a2 in {1,4} -> 2 rows.
  expect(res.rowCount).toBe(2);
});

test("count returns the total number of rows", async () => {
  const ctx = new SessionContext();
  const df = await exampleDf(ctx);

  const n = await df.count();
  expect(n).toBe(3);
});

test("describe returns summary statistics DataFrame", async () => {
  const ctx = new SessionContext();
  const df = await exampleDf(ctx);

  const statsDf = await df.describe();
  const res = await statsDf.collect();
  expect(res.columns.length).toBeGreaterThan(0);
  expect(res.rowCount).toBeGreaterThan(0);
});

test("toArrowIpc yields Arrow IPC stream bytes", async () => {
  const ctx = new SessionContext();
  const df = await exampleDf(ctx);

  const bytes = await df.toArrowIpc();
  expect(bytes).toBeInstanceOf(Uint8Array);
  expect(bytes.length).toBeGreaterThan(0);
});

test("show, showLimit, and toString execute without errors", async () => {
  const ctx = new SessionContext();
  const df = await exampleDf(ctx);

  await df.show();
  await df.showLimit(2);
  const str = await df.toString();
  expect(typeof str).toBe("string");
  expect(str.length).toBeGreaterThan(0);
});

test("schema and logicalPlan return human-readable strings", async () => {
  const ctx = new SessionContext();
  const df = await exampleDf(ctx);

  expect(typeof df.schema()).toBe("string");
  expect(typeof df.logicalPlan()).toBe("string");
  expect(df.logicalPlan().length).toBeGreaterThan(0);
});

test("writeCsv and writeParquet execute and resolve", async () => {
  const ctx = new SessionContext();
  const df = await exampleDf(ctx);

  const csvOut = join(tmpdir(), "df_out.csv");
  const parquetOut = join(tmpdir(), "df_out.parquet");

  const csvRes = await df.writeCsv(csvOut);
  expect(csvRes.rowCount).toBeGreaterThanOrEqual(0);

  const parquetRes = await df.writeParquet(parquetOut);
  expect(parquetRes.rowCount).toBeGreaterThanOrEqual(0);
});

test("fillNull fills missing values in a column", async () => {
  const ctx = new SessionContext();
  const path = writeTempCsv("df_fill.csv", "a,b\n1,\n3,4\n");
  const df = await ctx.readCsv(path);

  const res = await df.fillNull("0", ["b"]).collect();
  expect(res.rows).toEqual([
    ["1", "0"],
    ["3", "4"],
  ]);
});
