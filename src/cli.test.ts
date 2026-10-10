import { expect, test } from "vite-plus/test";
import * as addon from "./main.ts";

test("CLI direct shell and programmable API function exports are defined on module", () => {
  expect(addon).toHaveProperty("sail");
  expect(typeof addon.sail).toBe("function");

  expect(addon).toHaveProperty("sailSparkServer");
  expect(typeof addon.sailSparkServer).toBe("function");

  expect(addon).toHaveProperty("sailSparkShell");
  expect(typeof addon.sailSparkShell).toBe("function");

  expect(addon).toHaveProperty("sailSparkRun");
  expect(typeof addon.sailSparkRun).toBe("function");

  expect(addon).toHaveProperty("sailSparkMcpServer");
  expect(typeof addon.sailSparkMcpServer).toBe("function");

  expect(addon).toHaveProperty("sailFlightServer");
  expect(typeof addon.sailFlightServer).toBe("function");
});

test("Direct shell CLI argument execution calls sail function", () => {
  expect(() => addon.sail(["spark", "shell"])).toThrow("Native addon binding required");
});

test("Programmable NAPI API function calls option handlers", () => {
  expect(() => addon.sailSparkServer({ ip: "127.0.0.1", port: 50051 })).toThrow("Native addon binding required");
  expect(() => addon.sailSparkShell()).toThrow("Native addon binding required");
  expect(() => addon.sailSparkRun({ file: "app.py" })).toThrow("Native addon binding required");
  expect(() => addon.sailSparkMcpServer({ host: "127.0.0.1", port: 8000 })).toThrow("Native addon binding required");
  expect(() => addon.sailFlightServer({ ip: "127.0.0.1", port: 32010 })).toThrow("Native addon binding required");
});
