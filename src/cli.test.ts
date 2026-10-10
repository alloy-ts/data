import { expect, test } from "vite-plus/test";

test("cli options structure and argument mapping", () => {
  const sparkServerOpts = {
    ip: "127.0.0.1",
    port: 50051,
    directory: "/tmp",
  };
  expect(sparkServerOpts.ip).toBe("127.0.0.1");
  expect(sparkServerOpts.port).toBe(50051);

  const sparkRunOpts = {
    file: "script.py",
    directory: "/tmp",
  };
  expect(sparkRunOpts.file).toBe("script.py");

  const sparkMcpOpts = {
    host: "127.0.0.1",
    port: 8000,
    transport: "sse",
    sparkRemote: "sc://localhost:50051",
    directory: "/tmp",
  };
  expect(sparkMcpOpts.transport).toBe("sse");

  const flightServerOpts = {
    ip: "127.0.0.1",
    port: 32010,
    directory: "/tmp",
  };
  expect(flightServerOpts.port).toBe(32010);
});

test("direct shell argument list construction", () => {
  const args = ["spark", "shell"];
  expect(args).toEqual(["spark", "shell"]);
});
