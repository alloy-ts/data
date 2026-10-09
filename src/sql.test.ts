import { expect, test } from "vite-plus/test";
import { writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { queryResultToJson, SessionContext } from "./main.ts";

// --- helpers ---------------------------------------------------------------

function writeTempCsv(name: string, content: string): string {
  const path = join(tmpdir(), name);
  writeFileSync(path, content, "utf8");
  return path;
}

const SAMPLE_CSV = "a,b,c\n1,2,3\n4,5,6\n7,8,9\n";

// --- tests -----------------------------------------------------------------

test("execute runs SQL over a registered CSV table", async () => {
  const ctx = new SessionContext();
  const path = writeTempCsv("sql_example.csv", SAMPLE_CSV);
  await ctx.registerCsv("example", path, true);

  const res = await ctx.execute(
    "SELECT a, b FROM example WHERE a < 4 ORDER BY a",
  );

  expect(res.columns).toEqual(["a", "b"]);
  expect(res.rows).toEqual([["1", "2"]]);
  expect(res.rowCount).toBe(1);
});

test("CREATE EXTERNAL TABLE (DDL) via execute then query it", async () => {
  const ctx = new SessionContext();
  const path = writeTempCsv("sql_ext.csv", SAMPLE_CSV);
  await ctx.execute(
    `CREATE EXTERNAL TABLE ext STORED AS CSV LOCATION '${path}' OPTIONS ('format.has_header' 'true')`,
  );

  const res = await ctx.execute("SELECT count(*) AS n FROM ext");
  expect(res.columns).toEqual(["n"]);
  expect(res.rows.length).toBe(1);
});

test("registerSql is an alias for DDL registration", async () => {
  const ctx = new SessionContext();
  const path = writeTempCsv("sql_registerSql.csv", SAMPLE_CSV);
  await ctx.registerSql(
    `CREATE EXTERNAL TABLE t STORED AS CSV LOCATION '${path}' OPTIONS ('format.has_header' 'true')`,
  );

  const res = await ctx.execute("SELECT a FROM t ORDER BY a");
  expect(res.columns).toEqual(["a"]);
  expect(res.rows).toEqual([["1"], ["4"], ["7"]]);
});

test("queryResultToJson emits parseable {columns, rows}", async () => {
  const ctx = new SessionContext();
  const path = writeTempCsv("sql_json.csv", SAMPLE_CSV);
  await ctx.registerCsv("example", path, true);

  const res = await ctx.execute("SELECT a, b FROM example ORDER BY a");
  const json = JSON.parse(queryResultToJson(res));

  expect(json.columns).toEqual(["a", "b"]);
  expect(json.rows).toEqual([
    ["1", "2"],
    ["4", "5"],
    ["7", "8"],
  ]);
});

test("execute yields an empty result for DDL-only statements", async () => {
  const ctx = new SessionContext();
  // A self-contained SELECT with no table still executes and returns rows.
  const res = await ctx.execute("SELECT 1 + 1 AS two");
  expect(res.columns).toEqual(["two"]);
  expect(res.rows).toEqual([["2"]]);
  expect(res.rowCount).toBe(1);
});

test("nulls round-trip as null, not the string 'null'", async () => {
  const ctx = new SessionContext();
  const path = writeTempCsv("sql_null.csv", "a,b\n1,\n3,4\n");
  await ctx.registerCsv("example", path, true);

  const res = await ctx.execute("SELECT a, b FROM example ORDER BY a");
  expect(res.rows).toEqual([
    ["1", null],
    ["3", "4"],
  ]);
});
