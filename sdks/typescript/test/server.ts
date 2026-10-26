// A cronoq-server subprocess for the tests, with fixed ports so it can be restarted in place.
import { spawn, type ChildProcess } from "node:child_process";
import { closeSync, existsSync, fstatSync, mkdtempSync, openSync, readSync, writeFileSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

export const CLIENT = "client-token-0123456789";
export const WORKER = "worker-token-0123456789";
export const ADMIN = "admin-token-0123456789";

const LAYOUT = `
[[auth.tokens]]
token = "${CLIENT}"
role = "client"
[[auth.tokens]]
token = "${WORKER}"
role = "worker"
[[auth.tokens]]
token = "${ADMIN}"
role = "admin"

[limits]
max_wait_s = 30
`;

export const DEFAULT_POOL = `
[[pools]]
name = "default"
max = 8
lease_ttl_s = 1.0
retry = { max_attempts = 3, initial_backoff_s = 0.05, jitter = false }
`;

/** $CRONOQ_SERVER_BIN, or the debug build in the repository's target directory. */
export function serverBinary(): string | undefined {
  const bin =
    process.env.CRONOQ_SERVER_BIN ??
    resolve(import.meta.dirname, "../../../../target/debug", process.platform === "win32" ? "cronoq-server.exe" : "cronoq-server");
  if (existsSync(bin)) return bin;
  if (process.env.CRONOQ_REQUIRE_SERVER) throw new Error(`cronoq-server not found at ${bin}`);
  return undefined;
}

export function freePort(): Promise<number> {
  return new Promise((ok, fail) => {
    const s = createServer();
    s.listen(0, "127.0.0.1", () => {
      const port = (s.address() as { port: number }).port;
      s.close(() => ok(port));
    });
    s.on("error", fail);
  });
}

export class Server {
  readonly dir = mkdtempSync(join(tmpdir(), "cronoq-ts-"));
  private proc?: ChildProcess;
  private exited?: Promise<void>;
  /** The first start may move to other ports if another process took these. */
  private newPorts = false;

  constructor(
    readonly bin: string,
    public grpcPort: number,
    public httpPort: number,
    /** Lines of the [server] table. */
    public serverLines = "",
    /** Tables after the tokens and limits. */
    public tables = DEFAULT_POOL,
  ) {}

  static async create(bin: string, serverLines = "", tables = DEFAULT_POOL): Promise<Server> {
    const s = new Server(bin, await freePort(), await freePort(), serverLines, tables);
    s.newPorts = true;
    return s;
  }

  get target(): string {
    return `http://127.0.0.1:${this.grpcPort}`;
  }

  /**
   * Start the server. Free ports can be taken by another process between choosing and binding
   * them, so a first start of a server from `create` that finds a port in use retries on new ones.
   */
  async start(): Promise<void> {
    for (let attempt = 1; ; attempt++) {
      const exitLog = await this.startOnce();
      if (exitLog === undefined) {
        this.newPorts = false;
        return;
      }
      if (!this.newPorts || attempt === 3 || !exitLog.includes("Address already in use")) {
        throw new Error(`cronoq-server exited during startup: ${exitLog}`);
      }
      [this.grpcPort, this.httpPort] = [await freePort(), await freePort()];
    }
  }

  /** Start the server and wait until it is ready; the server's log if it exits first. */
  private async startOnce(): Promise<string | undefined> {
    const config = join(this.dir, "cronoq.toml");
    writeFileSync(
      config,
      `[server]\ngrpc_addr = "127.0.0.1:${this.grpcPort}"\nhttp_addr = "127.0.0.1:${this.httpPort}"\nshutdown_grace_s = 0\n${this.serverLines}\n${LAYOUT}\n${this.tables}`,
    );
    // The log is read back through the descriptor the server writes to, not by path.
    const log = openSync(join(this.dir, "server.log"), "w+");
    const proc = spawn(this.bin, ["--config", config], {
      stdio: ["ignore", "ignore", log],
      env: { ...process.env, RUST_LOG: "warn" },
    });
    this.proc = proc;
    let gone = false;
    this.exited = new Promise((r) => proc.on("exit", () => ((gone = true), r())));
    const deadline = Date.now() + 20_000;
    while (Date.now() < deadline) {
      if (gone) {
        this.proc = undefined;
        const text = readAll(log);
        closeSync(log);
        return text;
      }
      try {
        const r = await fetch(`http://127.0.0.1:${this.httpPort}/readyz`);
        if (r.ok) return undefined;
      } catch {
        // not listening yet
      }
      await new Promise((r) => setTimeout(r, 50));
    }
    throw new Error("cronoq-server did not become ready");
  }

  async stop(): Promise<void> {
    if (!this.proc) return;
    this.proc.kill(process.platform === "win32" ? "SIGKILL" : "SIGINT");
    const t = setTimeout(() => this.proc?.kill("SIGKILL"), 10_000);
    await this.exited;
    clearTimeout(t);
    this.proc = undefined;
  }
}

/** The whole contents of an open file, from its start. */
function readAll(fd: number): string {
  const buf = Buffer.alloc(fstatSync(fd).size);
  readSync(fd, buf, 0, buf.length, 0);
  return buf.toString("utf8");
}
