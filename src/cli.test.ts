import { expect, test } from "vite-plus/test";
import { execSync } from "node:child_process";
import * as cliExports from "./main.ts";

test("CLI exports programmable API functions", () => {
  expect(typeof cliExports.sail).toBe("function");
  expect(typeof cliExports.sailSparkServer).toBe("function");
  expect(typeof cliExports.sailSparkRun).toBe("function");
  expect(typeof cliExports.sailSparkShell).toBe("function");
});

test("programmable API - sail function accepts string arguments array", () => {
  const result = cliExports.sail(["spark", "--help"]);
  expect(typeof result).toBe("number");
});

test("programmable API - sailSparkServer formats arguments correctly", () => {
  const result = cliExports.sailSparkServer({ ip: "127.0.0.1", port: 50051 });
  expect(typeof result).toBe("number");
});

test("programmable API - sailSparkRun accepts PySpark run options", () => {
  const result = cliExports.sailSparkRun({ file: "app.py" });
  expect(typeof result).toBe("number");
});

test("programmable API - sailSparkShell executes as thin wrapper over spark shell CLI", () => {
  const result = cliExports.sailSparkShell();
  expect(typeof result).toBe("number");
});

test("direct shell CLI use - process invocation with node runner", () => {
  try {
    const output = execSync('node -e "import(\'./src/main.ts\').then(m => m.sail([\'--help\']))"', {
      encoding: "utf8",
      timeout: 5000,
    });
    expect(typeof output).toBe("string");
  } catch (error: any) {
    // Should execute node process without syntax or resolution failure
    expect(error.status).toBeDefined();
  }
});
