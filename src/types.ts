export type ChannelMode = 'threshold' | 'change' | 'daily_only';
export type ChannelTransform = 'none' | 'positive_only' | 'boolean';

export interface ChannelConfig {
  key: string;
  stateId: string;
  enabled?: boolean;
  sync?: boolean;
  defaultValue?: number | null;
  mode: ChannelMode;
  minChange?: number;
  factor: number;
  transform: ChannelTransform;
  round: number;
  minSendIntervalMs?: number;
  maxSendIntervalMs?: number;
}

export interface AdapterNativeConfig {
  databaseUrl?: string;
  rootPath?: string;
  serviceAccountJson?: string;
  dryRun?: boolean;
  dailyWriteHour?: number;
  dailyWriteMinute?: number;
  channels?: Partial<ChannelConfig>[];
}

export interface FirebaseServiceAccount {
  client_email: string;
  private_key: string;
  project_id?: string;
}

export interface WriteContext {
  channel: ChannelConfig;
  value: number;
  timestamp: number;
  trigger: string;
  reason: string;
  delta?: number;
}

export interface FirebaseWriteResult {
  currentPath: string;
  historyPath: string;
  dryRun: boolean;
}

export interface ChannelRuntimeState {
  lastObservedValue?: number;
  lastObservedAtMs?: number;
  lastWrittenValue?: number;
  lastWrittenAtMs?: number;
  pendingTimer?: NodeJS.Timeout;
}

export interface CustomStateConfig {
  enabled?: boolean;
  sync?: boolean;
  key?: string;
  mode?: ChannelMode;
  minChange?: number;
  factor?: number;
  transform?: ChannelTransform;
  round?: number;
  minSendIntervalMs?: number;
  maxSendIntervalMs?: number;
  defaultValue?: number | null;
}
