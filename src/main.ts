import console from "node:console";

// Re-export CLI API contract declarations
export function sail(_args: string[]): number {
  throw new Error("Native addon binding required");
}

export function sailSparkServer(_options?: { ip?: string; port?: number; directory?: string }): number {
  throw new Error("Native addon binding required");
}

export function sailSparkShell(): number {
  throw new Error("Native addon binding required");
}

export function sailSparkRun(_options?: { file?: string; directory?: string }): number {
  throw new Error("Native addon binding required");
}

export function sailSparkMcpServer(_options?: { host?: string; port?: number; transport?: string; sparkRemote?: string; directory?: string }): number {
  throw new Error("Native addon binding required");
}

export function sailFlightServer(_options?: { ip?: string; port?: number; directory?: string }): number {
  throw new Error("Native addon binding required");
}

console.log(main());
