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
      scopes: ['https://www.googleapis.com/auth/firebase.database']
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

  private async request(method: 'PUT' | 'PATCH', path: string, body: unknown): Promise<void> {
    if (this.dryRun) {
      return;
    }

    const client = await this.auth.getClient();
    const accessTokenResponse = await client.getAccessToken();
    const accessToken = typeof accessTokenResponse === 'string' ? accessTokenResponse : accessTokenResponse.token;

    if (!accessToken) {
      throw new Error('Could not obtain Firebase OAuth access token');
    }

    const response = await fetch(`${this.databaseUrl}/${encodeFirebasePath(path)}.json`, {
      method,
      headers: {
        Authorization: `Bearer ${accessToken}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(body)
    });

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Firebase RTDB ${method} ${path} failed with ${response.status}: ${text}`);
    }
  }
}

function encodeFirebasePath(path: string): string {
  return path
    .split('/')
    .filter(Boolean)
    .map((part) => encodeURIComponent(part))
    .join('/');
}
