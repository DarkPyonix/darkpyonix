export interface LiveCheckLink {
  link_id: string;
  user_code: string;
  verification_uri: string;
  verification_uri_complete: string;
  challenge: string;
  interval: number;
  expires_at: number;
}

export interface LiveCheckOptions {
  baseUrl: string;
  fetch?: (url: string, init?: RequestInit) => Promise<Response>;
  log?: (line: string) => void;
  sleep?: (ms: number) => Promise<void>;
  nowMs?: () => number;
  onLink?: (link: LiveCheckLink) => void | Promise<void>;
}

export interface LiveCheckResult {
  ok: boolean;
  steps: { step: string; ok: boolean; detail: string }[];
  endpointId: string;
}

export const STEPS: string[];
export function runLiveCheck(options: LiveCheckOptions): Promise<LiveCheckResult>;
