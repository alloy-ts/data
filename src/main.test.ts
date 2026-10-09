import { expect, test } from "vite-plus/test";
import { sail } from "./main.ts";

test("sail returns Hello, world!", () => {
  expect(sail()).toBe("Hello, world!");
});
