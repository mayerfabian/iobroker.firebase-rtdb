import { GoogleAuth } from 'google-auth-library';
import { currentPath, historyPath } from './history';
import type { FirebaseServiceAccount, FirebaseWriteResult } from './types';

export interface FirebaseRtdbOptions {
  databaseUrl: string;
  rootPath: string;
  serviceAccount: FirebaseServiceAccount;
  dryRun?: boolean;
}

export class FirebaseRtdbClient {
  private readonly databaseUrl: string;
  private readonly rootPath: string;
  private readonly dryRun: boolean;
  private readonly auth: GoogleAuth;

  public constructor(options: FirebaseRtdbOptions) {
    this.databaseUrl = options.databaseUrl.replace(/\/+$/g, '');
    this.rootPath = options.rootPath || 'home';
    this.dryRun = options.dryRun ?? false;
    this.auth = new GoogleAuth({
      credentials: options.serviceAccount,
      scopes: [
        'https://www.googleapis.com/auth/firebase.database',
        'https://www.googleapis.com/auth/userinfo.email'
      ]
    });
  }

  public async writeCurrent(snapshot: Record<string, unknown>): Promise<string> {
    const path = currentPath(this.rootPath);
    await this.request('PUT', path, snapshot);
    return path;
  }

  public async writeHistory(fieldPath: string, timestamp: number, value: number): Promise<string> {
    const path = historyPath(this.rootPath, fieldPath, timestamp);
    await this.request('PUT', path, value);
    return path;
  }

  public async writeCurrentAndHistory(
    snapshot: Record<string, unknown>,
    fieldPath: string,
    timestamp: number,
    value: number
  ): Promise<FirebaseWriteResult> {
    const firebaseCurrentPath = await this.writeCurrent(snapshot);
    const firebaseHistoryPath = await this.writeHistory(fieldPath, timestamp, value);

    return {
      currentPath: firebaseCurrentPath,
      historyPath: firebaseHistoryPath,
      dryRun: this.dryRun
    };
  }

  public async read(path: string): Promise<unknown> {
    return this.request<unknown>('GET', path);
  }

  public async write(path: string, value: unknown): Promise<void> {
    await this.request<void>('PUT', path, value);
  }

  public async delete(path: string): Promise<void> {
    await this.request<void>('PUT', path, null);
  }

  public async stream(
    path: string,
    onEvent: (event: string, payload: unknown) => Promise<void> | void,
    signal: AbortSignal
  ): Promise<void> {
    const accessToken = await this.getAccessToken();
    const encodedPath = encodeFirebasePath(path);
    const separator = this.databaseUrl.includes('?') ? '&' : '?';
    const url = `${this.databaseUrl}/${encodedPath}.json${separator}access_token=${encodeURIComponent(accessToken)}`;

    const response = await fetch(url, {
      method: 'GET',
      headers: {
        Accept: 'text/event-stream'
      },
      signal
    });

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Firebase RTDB stream ${path} failed with ${response.status}: ${text}`);
    }

    const reader = response.body?.getReader();
    if (!reader) {
      throw new Error(`Firebase RTDB stream ${path} has no readable body`);
    }

    const decoder = new TextDecoder();
    let buffer = '';

    while (!signal.aborted) {
      const { value, done } = await reader.read();
      if (done) {
        break;
      }
      buffer += decoder.decode(value, { stream: true });

      while (true) {
        const boundaryMatch = buffer.match(/\r?\n\r?\n/);
        if (!boundaryMatch || boundaryMatch.index === undefined) {
          break;
        }
        const boundaryIndex = boundaryMatch.index;
        const block = buffer.slice(0, boundaryIndex);
        buffer = buffer.slice(boundaryIndex + boundaryMatch[0].length);
        const parsed = parseSseBlock(block);
        if (!parsed) {
          continue;
        }
        await onEvent(parsed.event, parsed.payload);
      }
    }
  }

  private async request<T>(method: 'GET' | 'PUT' | 'PATCH', path: string, body?: unknown): Promise<T> {
    if (this.dryRun && method !== 'GET') {
      return undefined as T;
    }

    const payload = method === 'GET' ? undefined : body;
    if (method !== 'GET' && payload === undefined) {
      return undefined as T;
    }

    const accessToken = await this.getAccessToken();

    const response = await fetch(`${this.databaseUrl}/${encodeFirebasePath(path)}.json`, {
      method,
      headers: {
        Authorization: `Bearer ${accessToken}`,
        'Content-Type': 'application/json'
      },
      body: payload === undefined ? undefined : JSON.stringify(payload)
    });

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Firebase RTDB ${method} ${path} failed with ${response.status}: ${text}`);
    }

    if (method === 'GET') {
      return (await response.json()) as T;
    }

    return undefined as T;
  }

  private async getAccessToken(): Promise<string> {
    const client = await this.auth.getClient();
    const accessTokenResponse = await client.getAccessToken();
    const accessToken = typeof accessTokenResponse === 'string' ? accessTokenResponse : accessTokenResponse.token;

    if (!accessToken) {
      throw new Error('Could not obtain Firebase OAuth access token');
    }

    return accessToken;
  }
}

function encodeFirebasePath(path: string): string {
  return path
    .split('/')
    .filter(Boolean)
    .map((part) => encodeURIComponent(part))
    .join('/');
}

function parseSseBlock(block: string): { event: string; payload: unknown } | null {
  const normalized = block.replace(/\r/g, '');
  const lines = normalized.split('\n');
  let event = 'message';
  const dataLines: string[] = [];

  for (const line of lines) {
    if (line.startsWith('event:')) {
      event = line.slice(6).trim() || 'message';
      continue;
    }
    if (line.startsWith('data:')) {
      dataLines.push(line.slice(5).trim());
    }
  }

  if (!dataLines.length) {
    return null;
  }

  const rawData = dataLines.join('\n');
  let payload: unknown = rawData;
  try {
    payload = JSON.parse(rawData);
  } catch {
    // leave string payload
  }

  return { event, payload };
}
