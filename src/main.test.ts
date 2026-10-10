import { expect, test } from "vite-plus/test";
import { sail } from "./main.ts";

test("sail function is defined on main module", () => {
  expect(typeof sail).toBe("function");
  expect(() => sail(["--help"])).toThrow("Native addon binding required");
});
