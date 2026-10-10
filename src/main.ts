// Re-export CLI API contract declarations
export function sail(_args: string[]): number {
  throw new Error("Native addon binding required");
}

export function sailSparkServer(_options?: { ip?: string; port?: number; directory?: string }): number {
  throw new Error("Native addon binding required");
}

export function sailSparkShell(_options?: { file?: string; directory?: string }): number {
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

export class SessionContext {
  constructor() {}
  async registerCsv(_name: string, _path: string, _hasHeader?: boolean, _delimiter?: string): Promise<void> {}
  async registerParquet(_name: string, _path: string): Promise<void> {}
  async registerAvro(_name: string, _path: string): Promise<void> {}
  async registerSql(_sql: string): Promise<any> {}
  async execute(_sql: string): Promise<any> {}
  async readCsv(_path: string): Promise<any> {}
  async readParquet(_path: string): Promise<any> {}
  async readAvro(_path: string): Promise<any> {}
  async table(_name: string): Promise<any> {}
  async sqlDf(_sql: string): Promise<any> {}
}

export function queryResultToJson(_result: any): string {
  return "";
}
