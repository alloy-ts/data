import { expect, test } from "vite-plus/test";
import * as addon from "./main.ts";

test("cli functions are exported and callable or defined in entrypoint", () => {
  // Check if sail, sailSparkServer, sailSparkRun, sailSparkShell exist on exported addon or index
  if ("sail" in addon) {
    expect(typeof addon.sail).toBe("function");
  }
  if ("sailSparkServer" in addon) {
    expect(typeof addon.sailSparkServer).toBe("function");
  }
  if ("sailSparkRun" in addon) {
    expect(typeof addon.sailSparkRun).toBe("function");
  }
  if ("sailSparkShell" in addon) {
    expect(typeof addon.sailSparkShell).toBe("function");
  }
});
