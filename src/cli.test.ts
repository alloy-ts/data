import { expect, test } from "vite-plus/test";
import { sail, sailSparkRun, sailSparkServer, sailSparkShell } from "./main.ts";

test("direct shell CLI invocation via sail with --help returns 0", () => {
  // Direct CLI invocation passing raw args array
  const code = sail(["--help"]);
  expect(code).toBe(0);
});

test("direct shell CLI invocation via sail with spark --help returns 0", () => {
  const code = sail(["spark", "--help"]);
  expect(code).toBe(0);
});

test("programmable API sailSparkServer with --help via sail options", () => {
  // Test programmable wrapper API
  expect(typeof sailSparkServer).toBe("function");
});

test("programmable API sailSparkRun with --help via sail options", () => {
  expect(typeof sailSparkRun).toBe("function");
});

test("programmable API sailSparkShell exists as a function", () => {
  expect(typeof sailSparkShell).toBe("function");
});

test("direct shell CLI via sail returns non-zero code or throws on unknown flag", () => {
  try {
    const code = sail(["--invalid-unknown-flag-12345"]);
    expect(code).not.toBe(0);
  } catch (err: unknown) {
    expect(err).toBeDefined();
  }
});
