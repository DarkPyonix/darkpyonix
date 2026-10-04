// Where ACME DNS-01 TXT records go (SPEC FR-H5): the Cloudflare DNS API for darkpyonix.dev.

export class DnsError extends Error {}

export interface DnsProvider {
  /** Replaces every TXT value at `fqdn` with `values`. */
  setTxt(fqdn: string, values: string[]): Promise<void>;
  /** Removes every TXT value at `fqdn`. */
  clearTxt(fqdn: string): Promise<void>;
}

const API = "https://api.cloudflare.com/client/v4";
/** ACME validators read the record right away; keep it short-lived. */
const TXT_TTL = 60;

interface CfRecord {
  id: string;
  content: string;
}

interface CfEnvelope<T> {
  success: boolean;
  errors?: { code: number; message: string }[];
  result: T;
}

/**
 * The Cloudflare DNS API. The token needs only `Zone → DNS → Edit` on the one zone.
 * Replacement is one atomic batch call (`POST /zones/{id}/dns_records/batch`): deletes run
 * before posts, and the whole batch fails or succeeds together.
 */
export class CloudflareDns implements DnsProvider {
  constructor(
    private readonly token: string,
    private readonly zoneId: string,
    private readonly fetcher: typeof fetch,
  ) {}

  private async call<T>(method: string, path: string, body?: unknown): Promise<T> {
    let response: Response;
    try {
      response = await this.fetcher(`${API}/zones/${this.zoneId}${path}`, {
        method,
        headers: {
          authorization: `Bearer ${this.token}`,
          ...(body === undefined ? {} : { "content-type": "application/json" }),
        },
        body: body === undefined ? undefined : JSON.stringify(body),
      });
    } catch (err) {
      throw new DnsError(`cloudflare unreachable: ${String(err)}`);
    }
    let envelope: CfEnvelope<T>;
    try {
      envelope = (await response.json()) as CfEnvelope<T>;
    } catch {
      throw new DnsError(`cloudflare answered ${response.status} without JSON`);
    }
    if (!response.ok || !envelope.success) {
      const why = (envelope.errors ?? []).map((e) => `${e.code} ${e.message}`).join("; ");
      throw new DnsError(`cloudflare refused: ${response.status} ${why}`);
    }
    return envelope.result;
  }

  private async existing(fqdn: string): Promise<CfRecord[]> {
    const query = new URLSearchParams({ type: "TXT", name: fqdn, per_page: "100" });
    return this.call<CfRecord[]>("GET", `/dns_records?${query}`);
  }

  async setTxt(fqdn: string, values: string[]): Promise<void> {
    const old = await this.existing(fqdn);
    await this.call("POST", "/dns_records/batch", {
      deletes: old.map((r) => ({ id: r.id })),
      posts: values.map((v) => ({
        type: "TXT",
        name: fqdn,
        content: `"${v}"`,
        ttl: TXT_TTL,
        comment: "darkpyonix-hub ACME DNS-01",
      })),
    });
  }

  async clearTxt(fqdn: string): Promise<void> {
    const old = await this.existing(fqdn);
    if (old.length === 0) return;
    await this.call("POST", "/dns_records/batch", { deletes: old.map((r) => ({ id: r.id })) });
  }
}

/** TXT records in memory, for tests. */
export class MemoryDns implements DnsProvider {
  readonly records = new Map<string, string[]>();
  failing = false;

  async setTxt(fqdn: string, values: string[]): Promise<void> {
    if (this.failing) throw new DnsError("memory dns set to fail");
    this.records.set(fqdn, [...values]);
  }

  async clearTxt(fqdn: string): Promise<void> {
    if (this.failing) throw new DnsError("memory dns set to fail");
    this.records.delete(fqdn);
  }
}
