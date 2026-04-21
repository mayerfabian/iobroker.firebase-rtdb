import type * as utils from '@iobroker/adapter-core';
import { FirebaseRtdbClient } from './firebaseRtdb';
import type { AdapterNativeConfig, ChannelConfig, ChannelRuntimeState, WriteContext } from './types';

type Adapter = utils.AdapterInstance;

export class SyncRuntime {
  private readonly adapter: Adapter;
  private readonly firebase: FirebaseRtdbClient;
  private readonly channels: ChannelConfig[];
  private readonly stateByKey = new Map<string, ChannelRuntimeState>();
  private readonly channelByStateId = new Map<string, ChannelConfig>();
  private readonly maxIntervalTimers: NodeJS.Timeout[] = [];
  private writeCount = 0;
  private skippedCount = 0;
  private dailyTimer?: NodeJS.Timeout;
  private snapshot: Record<string, unknown> = {};
  private stopped = false;

  public constructor(adapter: Adapter, firebase: FirebaseRtdbClient, channels: ChannelConfig[]) {
    this.adapter = adapter;
    this.firebase = firebase;
    this.channels = channels;

    for (const channel of channels) {
      this.stateByKey.set(channel.key, {});
      this.channelByStateId.set(channel.stateId, channel);
    }
  }

  public async start(): Promise<void> {
    for (const channel of this.channels) {
      await this.adapter.subscribeForeignStatesAsync(channel.stateId);
      const state = await this.adapter.getForeignStateAsync(channel.stateId);
      await this.processRawValue(channel, state?.val, 'startup-observe', false);
      this.startMaxIntervalTimer(channel);
    }

    for (const channel of this.channels.filter((item) => item.mode !== 'daily_only')) {
      const value = this.getRuntimeState(channel).lastObservedValue;
      if (value !== undefined) {
        await this.processValue(channel, value, 'startup');
      }
    }

    this.startDailyTimer();
    this.adapter.log.info(`Firebase history sync runtime started with ${this.channels.length} channels`);
  }

  public async handleStateChange(stateId: string, state: ioBroker.State | null | undefined): Promise<void> {
    const channel = this.channelByStateId.get(stateId);
    if (!channel || !state || state.val === null || state.val === undefined) {
      return;
    }

    await this.processRawValue(channel, state.val, 'state-change');
  }

  public async stop(): Promise<void> {
    this.stopped = true;

    for (const runtimeState of this.stateByKey.values()) {
      if (runtimeState.pendingTimer) {
        clearTimeout(runtimeState.pendingTimer);
      }
    }

    for (const timer of this.maxIntervalTimers) {
      clearInterval(timer);
    }

    if (this.dailyTimer) {
      clearInterval(this.dailyTimer);
    }

    for (const channel of this.channels) {
      try {
        await this.adapter.unsubscribeForeignStatesAsync(channel.stateId);
      } catch (error) {
        this.adapter.log.debug(`Could not unsubscribe ${channel.stateId}: ${(error as Error).message}`);
      }
    }
  }

  private async processRawValue(
    channel: ChannelConfig,
    rawValue: ioBroker.StateValue | undefined,
    trigger: string,
    allowWrite = true
  ): Promise<void> {
    const effectiveValue =
      (rawValue === undefined || rawValue === null) && channel.defaultValue !== undefined && channel.defaultValue !== null
        ? channel.defaultValue
        : rawValue;
    const value = transformValue(effectiveValue, channel);
    if (value === undefined) {
      this.adapter.log.debug(`Skipping ${channel.key}: value "${String(rawValue)}" cannot be transformed`);
      return;
    }

    if ((rawValue === undefined || rawValue === null) && effectiveValue !== rawValue) {
      this.adapter.log.debug(`Using default value for ${channel.key}: ${effectiveValue}`);
    }

    const runtimeState = this.getRuntimeState(channel);
    runtimeState.lastObservedValue = value;
    runtimeState.lastObservedAtMs = Date.now();
    setDeepValue(this.snapshot, channel.key, value);

    if (!allowWrite) {
      return;
    }

    await this.processValue(channel, value, trigger);
  }

  private async processValue(channel: ChannelConfig, value: number, trigger: string): Promise<void> {
    if (channel.mode === 'daily_only') {
      if (trigger === 'daily') {
        await this.writeChannel({ channel, value, timestamp: Date.now(), trigger, reason: 'daily write' });
      }
      return;
    }

    const decision = this.getWriteDecision(channel, value, trigger);
    if (!decision.shouldWrite) {
      this.adapter.log.debug(`Skip ${channel.key}: trigger=${trigger}, reason=${decision.reason}`);
      await this.recordSkipped(channel.key, trigger, decision.reason, decision.delta);
      return;
    }

    await this.writeOrSchedule({
      channel,
      value,
      timestamp: Date.now(),
      trigger,
      reason: decision.reason,
      delta: decision.delta
    });
  }

  private getWriteDecision(channel: ChannelConfig, value: number, trigger: string): { shouldWrite: boolean; reason: string; delta?: number } {
    const runtimeState = this.getRuntimeState(channel);
    const lastWrittenValue = runtimeState.lastWrittenValue;

    if (lastWrittenValue === value) {
      return { shouldWrite: false, reason: 'same value already written' };
    }

    if (lastWrittenValue === undefined) {
      return { shouldWrite: true, reason: 'initial value' };
    }

    const delta = Math.abs(value - lastWrittenValue);

    if (trigger === 'max-interval') {
      return { shouldWrite: true, reason: 'max interval reached', delta };
    }

    if (channel.mode === 'change') {
      if (delta >= (channel.minChange ?? 0)) {
        return { shouldWrite: true, reason: `value changed (delta ${delta} >= ${channel.minChange ?? 0})`, delta };
      }
      return { shouldWrite: false, reason: `delta below minChange (${delta} < ${channel.minChange ?? 0})`, delta };
    }

    if (channel.mode === 'threshold' && delta >= (channel.minChange ?? 0)) {
      return { shouldWrite: true, reason: `minChange reached (${delta} >= ${channel.minChange ?? 0})`, delta };
    }

    return { shouldWrite: false, reason: `delta below minChange (${delta} < ${channel.minChange ?? 0})`, delta };
  }

  private async writeOrSchedule(context: WriteContext): Promise<void> {
    const runtimeState = this.getRuntimeState(context.channel);
    const now = Date.now();
    const lastWrittenAtMs = runtimeState.lastWrittenAtMs ?? 0;
    const minSendIntervalMs = context.channel.minSendIntervalMs ?? 0;
    const nextAllowedAtMs = lastWrittenAtMs + minSendIntervalMs;

    if (now >= nextAllowedAtMs) {
      await this.writeChannel(context);
      return;
    }

    const delayMs = nextAllowedAtMs - now;
    if (runtimeState.pendingTimer) {
      clearTimeout(runtimeState.pendingTimer);
    }

    runtimeState.pendingTimer = setTimeout(() => {
      runtimeState.pendingTimer = undefined;
      const latestValue = runtimeState.lastObservedValue;
      if (latestValue === undefined || this.stopped) {
        return;
      }

      void this.writeChannel({
        ...context,
        value: latestValue,
        timestamp: Date.now(),
        reason: `${context.reason}; delayed by min interval`
      }).catch((error) => this.adapter.log.error(`Delayed write failed for ${context.channel.key}: ${error.message}`));
    }, delayMs);

    this.adapter.log.debug(`Delay ${context.channel.key}: trigger=${context.trigger}, reason=${context.reason}, wait=${delayMs}ms`);
  }

  private async writeChannel(context: WriteContext): Promise<void> {
    const runtimeState = this.getRuntimeState(context.channel);

    if (runtimeState.lastWrittenValue === context.value) {
      this.adapter.log.debug(`Skip ${context.channel.key}: same value already written before Firebase write`);
      await this.recordSkipped(context.channel.key, context.trigger, 'same value already written before Firebase write', context.delta);
      return;
    }

    setDeepValue(this.snapshot, context.channel.key, context.value);
    const snapshotWithTimestamp = {
      ts: context.timestamp,
      ...this.snapshot
    };

    let writeResult;
    try {
      writeResult = await this.firebase.writeCurrentAndHistory(
        snapshotWithTimestamp,
        context.channel.key,
        context.timestamp,
        context.value
      );
    } catch (error) {
      await this.adapter.setStateAsync('info.connection', false, true);
      await this.adapter.setStateAsync('debug.lastError', String((error as Error).message), true);
      throw error;
    }

    runtimeState.lastWrittenValue = context.value;
    runtimeState.lastWrittenAtMs = context.timestamp;
    this.writeCount += 1;

    await this.adapter.setStateAsync('info.connection', true, true);
    await this.adapter.setStateAsync('info.lastWrite', context.timestamp, true);
    await this.adapter.setStateAsync('debug.writeCount', this.writeCount, true);
    await this.adapter.setStateAsync('debug.lastError', '', true);
    await this.adapter.setStateAsync('debug.lastTrigger', context.trigger, true);
    await this.adapter.setStateAsync('debug.lastWriteReason', context.reason, true);
    await this.adapter.setStateAsync('debug.lastWritePath', writeResult.historyPath, true);

    const deltaText = context.delta === undefined ? 'n/a' : String(context.delta);
    const dryRunPrefix = writeResult.dryRun ? 'DRY RUN ' : '';
    this.adapter.log.info(
      `${dryRunPrefix}Write ${context.channel.key}: value=${context.value}, trigger=${context.trigger}, reason=${context.reason}, delta=${deltaText}, currentPath=${writeResult.currentPath}, historyPath=${writeResult.historyPath}`
    );
  }

  private async recordSkipped(channelKey: string, trigger: string, reason: string, delta?: number): Promise<void> {
    this.skippedCount += 1;
    const deltaText = delta === undefined ? 'n/a' : String(delta);
    await this.adapter.setStateAsync('debug.skippedCount', this.skippedCount, true);
    await this.adapter.setStateAsync('debug.lastTrigger', trigger, true);
    await this.adapter.setStateAsync('debug.lastSkipped', `${channelKey}: ${reason}; delta=${deltaText}`, true);
  }

  private startMaxIntervalTimer(channel: ChannelConfig): void {
    if (!channel.maxSendIntervalMs || channel.mode === 'daily_only') {
      return;
    }

    const timer = setInterval(() => {
      const runtimeState = this.getRuntimeState(channel);
      const value = runtimeState.lastObservedValue;
      const lastWrittenAtMs = runtimeState.lastWrittenAtMs ?? 0;

      if (value === undefined || Date.now() - lastWrittenAtMs < channel.maxSendIntervalMs!) {
        return;
      }

      const decision = this.getWriteDecision(channel, value, 'max-interval');
      if (!decision.shouldWrite) {
        return;
      }

      void this.writeOrSchedule({
        channel,
        value,
        timestamp: Date.now(),
        trigger: 'max-interval',
        reason: decision.reason,
        delta: decision.delta
      }).catch((error) => this.adapter.log.error(`Max interval write failed for ${channel.key}: ${error.message}`));
    }, Math.min(channel.maxSendIntervalMs, 60000));

    this.maxIntervalTimers.push(timer);
  }

  private startDailyTimer(): void {
    // Check every 30 seconds so per-channel daily schedules can trigger precisely without duplicate writes.
    this.dailyTimer = setInterval(() => {
      void this.runDailyWrites().catch((error) => this.adapter.log.error(`Daily write cycle failed: ${error.message}`));
    }, 30000);

    void this.runDailyWrites().catch((error) => this.adapter.log.error(`Initial daily write check failed: ${error.message}`));
  }

  private async runDailyWrites(): Promise<void> {
    for (const channel of this.channels.filter((item) => item.mode === 'daily_only')) {
      if (!this.shouldRunDailyWriteNow(channel)) {
        continue;
      }

      const state = await this.adapter.getForeignStateAsync(channel.stateId);
      await this.processRawValue(channel, state?.val, 'daily');
      this.getRuntimeState(channel).lastDailyWriteKey = this.getDailyWriteKey(new Date(), channel);
    }
  }

  private shouldRunDailyWriteNow(channel: ChannelConfig): boolean {
    const now = new Date();
    const { hour, minute } = this.getDailyTarget(channel);
    if (now.getHours() !== hour || now.getMinutes() !== minute) {
      return false;
    }

    const runtimeState = this.getRuntimeState(channel);
    return runtimeState.lastDailyWriteKey !== this.getDailyWriteKey(now, channel);
  }

  private getDailyTarget(channel: ChannelConfig): { hour: number; minute: number } {
    const config = this.adapter.config as AdapterNativeConfig;
    return {
      hour: clampInt(channel.dailyHour ?? config.dailyWriteHour ?? 0, 0, 23),
      minute: clampInt(channel.dailyMinute ?? config.dailyWriteMinute ?? 10, 0, 59)
    };
  }

  private getDailyWriteKey(now: Date, channel: ChannelConfig): string {
    const { hour, minute } = this.getDailyTarget(channel);
    return `${now.getFullYear()}-${now.getMonth() + 1}-${now.getDate()}-${hour}-${minute}`;
  }

  private getRuntimeState(channel: ChannelConfig): ChannelRuntimeState {
    const runtimeState = this.stateByKey.get(channel.key);
    if (!runtimeState) {
      throw new Error(`No runtime state for channel ${channel.key}`);
    }

    return runtimeState;
  }
}

function transformValue(rawValue: ioBroker.StateValue | undefined, channel: ChannelConfig): number | undefined {
  if (rawValue === undefined || rawValue === null) {
    return undefined;
  }

  let value: number;
  if (channel.transform === 'boolean') {
    value = rawValue === true || rawValue === 1 || rawValue === 'true' ? 1 : 0;
  } else {
    value = Number(rawValue);
  }

  if (!Number.isFinite(value)) {
    return undefined;
  }

  value *= channel.factor;

  if (channel.transform === 'positive_only') {
    value = Math.max(0, value);
  }

  const precision = 10 ** channel.round;
  return Math.round(value * precision) / precision;
}

function setDeepValue(target: Record<string, unknown>, path: string, value: unknown): void {
  const parts = path
    .split('/')
    .flatMap((part) => part.split('.'))
    .map((part) => part.trim())
    .filter(Boolean);
  if (!parts.length) {
    return;
  }
  let current: Record<string, unknown> = target;

  for (const part of parts.slice(0, -1)) {
    const next = current[part];
    if (!next || typeof next !== 'object' || Array.isArray(next)) {
      current[part] = {};
    }

    current = current[part] as Record<string, unknown>;
  }

  current[parts[parts.length - 1]] = value;
}

function clampInt(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, Math.trunc(value)));
}
