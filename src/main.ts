import console from "node:console";

export interface SailSparkServerOptions {
  ip?: string;
  port?: number;
  directory?: string;
}

export interface SailSparkRunOptions {
  file?: string;
  directory?: string;
}

let nativeAddon: any = null;
try {
  // Attempt to load native addon if built
  nativeAddon = await import("../dist/index.js").catch(() => null);
} catch {
  // Fallback if native addon not yet built
}

export const main = () => {
  return "Hello, world!";
};

export function sail(args: string[]): number {
  if (typeof nativeAddon?.sail === "function") {
    return nativeAddon.sail(args);
  }
  return 0;
}

export function sailSparkServer(options?: SailSparkServerOptions): number {
  if (typeof nativeAddon?.sailSparkServer === "function") {
    return nativeAddon.sailSparkServer(options);
  }
  const args = ["spark", "server"];
  if (options?.ip) args.push("--ip", options.ip);
  if (options?.port) args.push("--port", String(options.port));
  if (options?.directory) args.push("--directory", options.directory);
  return sail(args);
}

export function sailSparkRun(options?: SailSparkRunOptions): number {
  if (typeof nativeAddon?.sailSparkRun === "function") {
    return nativeAddon.sailSparkRun(options);
  }
  const args = ["spark", "run"];
  if (options?.file) args.push("--file", options.file);
  if (options?.directory) args.push("--directory", options.directory);
  return sail(args);
}

export function sailSparkShell(): number {
  if (typeof nativeAddon?.sailSparkShell === "function") {
    return nativeAddon.sailSparkShell();
  }
  return sail(["spark", "shell"]);
}

console.log(main());
