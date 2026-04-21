import * as utils from '@iobroker/adapter-core';
import { DEFAULT_CHANNELS, mergeChannelOverrides } from './channels';
import { FirebaseRtdbClient } from './firebaseRtdb';
import { SyncRuntime } from './runtime';
import type { AdapterNativeConfig, ChannelConfig, CustomStateConfig, FirebaseServiceAccount } from './types';

class FirebaseHistorySyncAdapter extends utils.Adapter {
  private runtime?: SyncRuntime;
  private reloadTimer?: NodeJS.Timeout;
  private configPollTimer?: NodeJS.Timeout;
  private isReloading = false;
  private isReconcilingStateCustoms = false;
  private lastChannelsSignature = '';
  private readonly adminWriteOptions = { user: 'system.user.admin' };

  public constructor(options: Partial<utils.AdapterOptions> = {}) {
    super({
      ...options,
      name: 'firebase-history-sync'
    });

    this.on('ready', () => void this.onReady());
    this.on('stateChange', (id, state) => void this.runtime?.handleStateChange(id, state));
    this.on('objectChange', (id, obj) => void this.onObjectChange(id, obj));
    this.on('message', (obj) => void this.onMessage(obj));
    this.on('unload', (callback) => this.onUnload(callback));
  }

  private async onReady(): Promise<void> {
    await this.setStateAsync('info.connection', false, true);
    await this.subscribeForeignObjectsAsync('*');
    await this.initializeRuntime();
    this.startConfigPolling();
  }

  private async initializeRuntime(): Promise<void> {
    const config = this.config as AdapterNativeConfig;
    const storedChannels = await this.cleanupStoredChannels(config);
    this.lastChannelsSignature = JSON.stringify(storedChannels);

    const databaseUrl = config.databaseUrl?.trim();
    const serviceAccountJson = await this.readServiceAccountJson(config);
    const rootPath = config.rootPath?.trim() || 'home';

    if (!databaseUrl) {
      this.log.warn('Adapter is not configured: databaseUrl is empty');
      return;
    }

    if (!serviceAccountJson) {
      this.log.warn('Adapter is not configured: serviceAccountJson is empty');
      return;
    }

    let serviceAccount: FirebaseServiceAccount;
    try {
      serviceAccount = JSON.parse(serviceAccountJson) as FirebaseServiceAccount;
      validateServiceAccount(serviceAccount);
    } catch (error) {
      this.log.error(`Invalid Firebase serviceAccountJson: ${(error as Error).message}`);
      return;
    }

    const channels = mergeChannelOverrides(storedChannels);
    const customChannels = await this.loadCustomChannels(config);
    const runtimeChannels = mergeChannelsByStateId(channels, customChannels);
    const firebase = new FirebaseRtdbClient({
      databaseUrl,
      rootPath,
      serviceAccount,
      dryRun: config.dryRun ?? false
    });

    this.runtime = new SyncRuntime(this, firebase, runtimeChannels);

    try {
      const instanceObject = await this.getForeignObjectAsync(`system.adapter.${this.namespace}`);
      await this.reconcileStateCustomsFromInstanceObject(instanceObject);
      await this.runtime.start();
      this.log.info(`Loaded ${customChannels.length} Firebase custom object channel(s)`);
    } catch (error) {
      await this.setStateAsync('info.connection', false, true);
      this.log.error(`Could not start Firebase history sync runtime: ${(error as Error).message}`);
    }
  }

  private onObjectChange(id: string, obj: ioBroker.Object | null | undefined): void {
    if (this.isReconcilingStateCustoms) {
      return;
    }

    if (id === `system.adapter.${this.namespace}`) {
      void this.reconcileStateCustomsFromInstanceObject(obj);
    }

    if (!this.isRelevantObjectChange(id, obj)) {
      return;
    }

    this.scheduleRuntimeReload(`object change detected for ${id}`);
  }

  private isRelevantObjectChange(id: string, obj: ioBroker.Object | null | undefined): boolean {
    if (id === `system.adapter.${this.namespace}`) {
      return true;
    }

    if (obj?.type !== 'state') {
      return false;
    }

    const customMap = obj.common?.custom as Record<string, CustomStateConfig | undefined> | undefined;
    return Boolean(customMap?.[this.namespace] || customMap?.[this.name]);
  }

  private scheduleRuntimeReload(reason: string): void {
    if (this.reloadTimer) {
      clearTimeout(this.reloadTimer);
    }

    this.reloadTimer = setTimeout(() => {
      this.reloadTimer = undefined;
      void this.reloadRuntime(reason);
    }, 500);
  }

  private async reloadRuntime(reason: string): Promise<void> {
    if (this.isReloading) {
      return;
    }

    this.isReloading = true;
    this.log.info(`Reloading Firebase runtime: ${reason}`);

    try {
      if (this.runtime) {
        await this.runtime.stop();
        this.runtime = undefined;
      }

      await this.initializeRuntime();
    } catch (error) {
      this.log.error(`Could not reload Firebase runtime: ${(error as Error).message}`);
    } finally {
      this.isReloading = false;
    }
  }

  private startConfigPolling(): void {
    if (this.configPollTimer) {
      clearInterval(this.configPollTimer);
    }

    this.configPollTimer = setInterval(() => {
      void this.pollInstanceConfig();
    }, 2000);
  }

  private async pollInstanceConfig(): Promise<void> {
    if (this.isReloading || this.isReconcilingStateCustoms) {
      return;
    }

    const instanceObject = await this.getForeignObjectAsync(`system.adapter.${this.namespace}`);
    if (!instanceObject?.native) {
      return;
    }

    const currentChannels = Array.isArray(instanceObject.native.channels) ? instanceObject.native.channels : [];
    const currentSignature = JSON.stringify(currentChannels);
    const configChanged = currentSignature !== this.lastChannelsSignature;

    if (configChanged) {
      this.lastChannelsSignature = currentSignature;
      this.log.info(`Detected Firebase channel config change via polling (${currentChannels.length} channels)`);
    }

    const reconciled = await this.reconcileStateCustomsFromInstanceObject(instanceObject);
    if (reconciled) {
      this.log.info('Reconciled stale Firebase custom state config from polling');
    }

    if (configChanged || reconciled) {
      this.scheduleRuntimeReload(configChanged ? 'instance channel config changed' : 'stale custom config reconciled');
    }
  }

  private onMessage(obj: ioBroker.Message | undefined): void {
    if (!obj?.command) {
      return;
    }

    if (obj.command === 'reconcileCustoms') {
      void this.handleReconcileCustomsMessage(obj);
    }
  }

  private async handleReconcileCustomsMessage(obj: ioBroker.Message): Promise<void> {
    try {
      const instanceObject = await this.getForeignObjectAsync(`system.adapter.${this.namespace}`);
      await this.reconcileStateCustomsFromInstanceObject(instanceObject);
      this.sendTo(obj.from, obj.command, { ok: true }, obj.callback);
    } catch (error) {
      this.log.error(`Could not reconcile Firebase custom state config: ${(error as Error).message}`);
      this.sendTo(obj.from, obj.command, { ok: false, error: (error as Error).message }, obj.callback);
    }
  }

  private async reconcileStateCustomsFromInstanceObject(instanceObject: ioBroker.Object | null | undefined): Promise<boolean> {
    if (this.isReconcilingStateCustoms) {
      return false;
    }

    if (!instanceObject || instanceObject.type !== 'instance') {
      return false;
    }

    const config = this.config as AdapterNativeConfig;
    const desiredChannels = mergeChannelOverrides(
      Array.isArray(instanceObject.native?.channels) ? instanceObject.native.channels : []
    );
    this.log.info(`Reconciling Firebase custom config for ${desiredChannels.length} desired channel(s)`);
    const desiredByStateId = new Map(desiredChannels.map((channel) => [channel.stateId, channel]));
    const result = await this.getObjectViewAsync('system', 'state', {
      startkey: '',
      endkey: '\u9999'
    });

    this.isReconcilingStateCustoms = true;
    let changed = false;
    try {
      for (const row of result.rows) {
        const object = row.value;
        if (!object?.common) {
          continue;
        }

        const stateId = object._id;
        const customMap = object.common.custom as Record<string, CustomStateConfig | undefined> | undefined;
        const hasFirebaseCustom = Boolean(customMap?.[this.namespace] || customMap?.[this.name]);
        const desiredChannel = desiredByStateId.get(stateId);

        if (!hasFirebaseCustom && !desiredChannel) {
          continue;
        }

        this.log.info(
          `Reconciling state ${stateId}: hasCustom=${hasFirebaseCustom} desired=${Boolean(desiredChannel)}`
        );

        const nextObject = JSON.parse(JSON.stringify(object)) as ioBroker.StateObject;
        nextObject.common.custom = nextObject.common.custom || {};

        if (desiredChannel) {
          nextObject.common.custom[this.namespace] = this.channelToCustomStateConfig(desiredChannel, config);
          delete nextObject.common.custom[this.name];
        } else {
          const disabledCustom = this.customStateConfigToDisabledStateConfig(
            stateId,
            customMap?.[this.namespace] ?? customMap?.[this.name],
            config
          );
          nextObject.common.custom[this.namespace] = disabledCustom;
          nextObject.common.custom[this.name] = { ...disabledCustom };
        }

        if (JSON.stringify(object.common.custom ?? null) === JSON.stringify(nextObject.common.custom ?? null)) {
          this.log.debug(`Skipping ${stateId}: custom config already matches desired state`);
          continue;
        }

        await this.setForeignObjectAsync(stateId, nextObject, this.adminWriteOptions);
        const verifyObject = await this.getForeignObjectAsync(stateId);
        const verifyMap = verifyObject?.common?.custom as Record<string, CustomStateConfig | undefined> | undefined;
        const verifyNamespaceCustom = verifyMap?.[this.namespace];
        const verifyLegacyCustom = verifyMap?.[this.name];
        const verified = desiredChannel
          ? Boolean(verifyNamespaceCustom?.enabled && verifyNamespaceCustom.sync !== false)
          : Boolean(
            verifyNamespaceCustom?.enabled === false &&
            verifyNamespaceCustom?.sync === false &&
            verifyLegacyCustom?.enabled === false &&
            verifyLegacyCustom?.sync === false
          );

        this.log.info(
          `Reconciled state ${stateId}: verified=${verified} namespaceEnabled=${verifyNamespaceCustom?.enabled ?? 'missing'} legacyEnabled=${verifyLegacyCustom?.enabled ?? 'missing'}`
        );

        if (verified) {
          changed = true;
        } else {
          this.log.warn(`State ${stateId} did not reach desired Firebase custom state after reconcile write`);
        }
      }
    } finally {
      this.isReconcilingStateCustoms = false;
    }

    return changed;
  }

  private async loadCustomChannels(config: AdapterNativeConfig): Promise<ChannelConfig[]> {
    const result = await this.getObjectViewAsync('system', 'state', {
      startkey: '',
      endkey: '\u9999'
    });

    const channels: ChannelConfig[] = [];
    for (const row of result.rows) {
      const object = row.value;
      if (!object) {
        continue;
      }

      const customMap = object?.common?.custom as Record<string, CustomStateConfig | undefined> | undefined;
      const custom = customMap?.[this.namespace] ?? customMap?.[this.name];
      if (!custom?.enabled) {
        continue;
      }

      const stateId = object._id;
      const key = custom.key?.trim() || objectIdToFirebaseKey(stateId);
      const channel: ChannelConfig = {
        key,
        stateId,
        enabled: true,
        sync: custom.sync ?? false,
        defaultValue: custom.defaultValue ?? null,
        mode: custom.mode ?? 'threshold',
        minChange: custom.minChange ?? 0,
        factor: custom.factor ?? 1,
        transform: custom.transform ?? 'none',
        round: custom.round ?? 1,
        minSendIntervalMs: custom.minSendIntervalMs ?? 10000,
        maxSendIntervalMs: custom.maxSendIntervalMs ?? 900000,
        dailyHour: custom.dailyHour ?? config.dailyWriteHour ?? 0,
        dailyMinute: custom.dailyMinute ?? config.dailyWriteMinute ?? 10
      };

      channels.push(channel);
    }

    return channels.filter((channel) => channel.sync !== false);
  }

  private async readServiceAccountJson(config: AdapterNativeConfig): Promise<string> {
    const configuredValue = config.serviceAccountJson?.trim() ?? '';
    if (looksLikeJson(configuredValue)) {
      return configuredValue;
    }

    const instanceObject = await this.getForeignObjectAsync(`system.adapter.${this.namespace}`);
    const rawValue = typeof instanceObject?.native?.serviceAccountJson === 'string'
      ? instanceObject.native.serviceAccountJson.trim()
      : '';

    if (!rawValue) {
      return configuredValue;
    }

    if (looksLikeJson(rawValue)) {
      this.log.warn('serviceAccountJson is stored as plain JSON. Open the adapter config and save once to let ioBroker encrypt it.');
      return rawValue;
    }

    try {
      const decryptedValue = this.decrypt(rawValue).trim();
      if (looksLikeJson(decryptedValue)) {
        return decryptedValue;
      }
    } catch (error) {
      this.log.debug(`Could not decrypt raw serviceAccountJson: ${(error as Error).message}`);
    }

    return configuredValue;
  }

  private async cleanupStoredChannels(config: AdapterNativeConfig): Promise<Partial<ChannelConfig>[]> {
    const instanceObject = await this.getForeignObjectAsync(`system.adapter.${this.namespace}`);
    if (!instanceObject) {
      return Array.isArray(config.channels) ? config.channels : [];
    }

    const existingChannels = Array.isArray(instanceObject.native?.channels)
      ? instanceObject.native.channels
      : Array.isArray(config.channels)
        ? config.channels
        : [];
    const defaultKeys = new Set(DEFAULT_CHANNELS.map((channel) => channel.key));
    const cleanedChannels = existingChannels.filter((channel) => {
      if (!channel?.key || !channel?.stateId) {
        return false;
      }

      if (channel.enabled === false || channel.sync === false) {
        return false;
      }

      return !defaultKeys.has(channel.key);
    });

    if (JSON.stringify(existingChannels) === JSON.stringify(cleanedChannels)) {
      config.channels = cleanedChannels;
      return cleanedChannels;
    }

    instanceObject.native = {
      ...instanceObject.native,
      channels: cleanedChannels
    };

    await this.setForeignObjectAsync(instanceObject._id, instanceObject, this.adminWriteOptions);
    config.channels = cleanedChannels;
    this.log.info('Removed built-in default channels from stored adapter configuration');
    return cleanedChannels;
  }

  private channelToCustomStateConfig(channel: ChannelConfig, config: AdapterNativeConfig): CustomStateConfig {
    return {
      enabled: true,
      sync: channel.sync !== false,
      key: channel.key,
      mode: channel.mode ?? 'threshold',
      minChange: channel.minChange ?? 0,
      factor: channel.factor ?? 1,
      transform: channel.transform ?? 'none',
      round: channel.round ?? 1,
      minSendIntervalMs: channel.minSendIntervalMs ?? 10000,
      maxSendIntervalMs: channel.maxSendIntervalMs ?? 900000,
      dailyHour: channel.dailyHour ?? config.dailyWriteHour ?? 0,
      dailyMinute: channel.dailyMinute ?? config.dailyWriteMinute ?? 10,
      defaultValue: channel.defaultValue ?? null
    };
  }

  private customStateConfigToDisabledStateConfig(
    stateId: string,
    currentCustom: CustomStateConfig | undefined,
    config: AdapterNativeConfig
  ): CustomStateConfig {
    return {
      enabled: false,
      sync: false,
      key: currentCustom?.key?.trim() || objectIdToFirebaseKey(stateId),
      mode: currentCustom?.mode ?? 'threshold',
      minChange: currentCustom?.minChange ?? 0,
      factor: currentCustom?.factor ?? 1,
      transform: currentCustom?.transform ?? 'none',
      round: currentCustom?.round ?? 1,
      minSendIntervalMs: currentCustom?.minSendIntervalMs ?? 10000,
      maxSendIntervalMs: currentCustom?.maxSendIntervalMs ?? 900000,
      dailyHour: currentCustom?.dailyHour ?? config.dailyWriteHour ?? 0,
      dailyMinute: currentCustom?.dailyMinute ?? config.dailyWriteMinute ?? 10,
      defaultValue: currentCustom?.defaultValue ?? null
    };
  }

  private onUnload(callback: () => void): void {
    void (async () => {
      try {
        if (this.configPollTimer) {
          clearInterval(this.configPollTimer);
          this.configPollTimer = undefined;
        }
        if (this.reloadTimer) {
          clearTimeout(this.reloadTimer);
          this.reloadTimer = undefined;
        }
        await this.runtime?.stop();
      } finally {
        callback();
      }
    })();
  }
}

function validateServiceAccount(serviceAccount: FirebaseServiceAccount): void {
  if (!serviceAccount.client_email) {
    throw new Error('client_email is missing');
  }

  if (!serviceAccount.private_key) {
    throw new Error('private_key is missing');
  }
}

function looksLikeJson(value: string): boolean {
  return value.trim().startsWith('{');
}

function objectIdToFirebaseKey(objectId: string): string {
  const normalizedPath = String(objectId)
    .split('.')
    .map((part) => part.trim().replace(/[.#$\[\]/]+/g, '_').replace(/^_+|_+$/g, ''))
    .filter(Boolean)
    .join('/');
  return normalizedPath ? `custom/${normalizedPath}` : 'custom/value';
}

function mergeChannelsByStateId(baseChannels: ChannelConfig[], customChannels: ChannelConfig[]): ChannelConfig[] {
  const byStateId = new Map(baseChannels.map((channel) => [channel.stateId, channel]));

  for (const customChannel of customChannels) {
    byStateId.set(customChannel.stateId, customChannel);
  }

  return [...byStateId.values()].filter((channel) => channel.enabled !== false && channel.sync !== false);
}

if (require.main !== module) {
  module.exports = (options: Partial<utils.AdapterOptions> | undefined): FirebaseHistorySyncAdapter =>
    new FirebaseHistorySyncAdapter(options);
} else {
  void new FirebaseHistorySyncAdapter();
}
