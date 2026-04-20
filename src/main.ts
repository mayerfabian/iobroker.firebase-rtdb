import * as utils from '@iobroker/adapter-core';
import { DEFAULT_CHANNELS, mergeChannelOverrides } from './channels';
import { FirebaseRtdbClient } from './firebaseRtdb';
import { SyncRuntime } from './runtime';
import type { AdapterNativeConfig, ChannelConfig, CustomStateConfig, FirebaseServiceAccount } from './types';

class FirebaseHistorySyncAdapter extends utils.Adapter {
  private runtime?: SyncRuntime;

  public constructor(options: Partial<utils.AdapterOptions> = {}) {
    super({
      ...options,
      name: 'firebase-history-sync'
    });

    this.on('ready', () => void this.onReady());
    this.on('stateChange', (id, state) => void this.runtime?.handleStateChange(id, state));
    this.on('unload', (callback) => this.onUnload(callback));
  }

  private async onReady(): Promise<void> {
    await this.setStateAsync('info.connection', false, true);

    const config = this.config as AdapterNativeConfig;
    await this.cleanupStoredChannels(config);

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

    const channels = mergeChannelOverrides(config.channels);
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
      await this.runtime.start();
      this.log.info(`Loaded ${customChannels.length} Firebase custom object channel(s)`);
    } catch (error) {
      await this.setStateAsync('info.connection', false, true);
      this.log.error(`Could not start Firebase history sync runtime: ${(error as Error).message}`);
    }
  }

  private async loadCustomChannels(config: AdapterNativeConfig): Promise<ChannelConfig[]> {
    const result = await this.getObjectViewAsync('system', 'state', {
      startkey: '',
      endkey: '\u9999'
    });

    const channels: ChannelConfig[] = [];
    for (const row of result.rows) {
      const object = row.value;
      const custom = object?.common?.custom?.[this.namespace] as CustomStateConfig | undefined;
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
        maxSendIntervalMs: custom.maxSendIntervalMs ?? 900000
      };

      channels.push(channel);
    }

    await this.mergeCustomChannelsIntoConfig(config, channels);
    return channels.filter((channel) => channel.sync !== false);
  }

  private async mergeCustomChannelsIntoConfig(config: AdapterNativeConfig, customChannels: ChannelConfig[]): Promise<void> {
    const instanceObject = await this.getForeignObjectAsync(`system.adapter.${this.namespace}`);
    if (!instanceObject) {
      return;
    }

    const existingChannels = Array.isArray(config.channels) ? config.channels : [];
    const manualChannels = existingChannels.filter((channel) => {
      if (!channel?.stateId || !channel?.key) {
        return false;
      }

      return !customChannels.some((customChannel) => customChannel.stateId === channel.stateId);
    });
    const storedChannels = [
      ...manualChannels.filter((channel) => channel.enabled !== false && channel.sync !== false),
      ...customChannels.filter((channel) => channel.enabled !== false && channel.sync !== false)
    ];

    if (JSON.stringify(existingChannels) === JSON.stringify(storedChannels)) {
      return;
    }

    instanceObject.native = {
      ...instanceObject.native,
      channels: storedChannels
    };

    await this.setForeignObjectAsync(instanceObject._id, instanceObject);
    config.channels = storedChannels;
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

  private async cleanupStoredChannels(config: AdapterNativeConfig): Promise<void> {
    const instanceObject = await this.getForeignObjectAsync(`system.adapter.${this.namespace}`);
    if (!instanceObject) {
      return;
    }

    const existingChannels = Array.isArray(config.channels) ? config.channels : [];
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
      return;
    }

    instanceObject.native = {
      ...instanceObject.native,
      channels: cleanedChannels
    };

    await this.setForeignObjectAsync(instanceObject._id, instanceObject);
    config.channels = cleanedChannels;
    this.log.info('Removed built-in default channels from stored adapter configuration');
  }

  private onUnload(callback: () => void): void {
    try {
      this.runtime?.stop();
      callback();
    } catch {
      callback();
    }
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
  return `custom.${objectId.replace(/[^a-zA-Z0-9]+/g, '_').replace(/^_+|_+$/g, '')}`;
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
