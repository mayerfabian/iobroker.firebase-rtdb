import * as utils from '@iobroker/adapter-core';
import { DEFAULT_CHANNELS, mergeChannelOverrides } from './channels';
import { FirebaseRtdbClient } from './firebaseRtdb';
import { SyncRuntime } from './runtime';
import type {
  AdapterNativeConfig,
  ChannelConfig,
  CustomStateConfig,
  FirebaseServiceAccount,
  ReadSubscriptionConfig
} from './types';

class FirebaseHistorySyncAdapter extends utils.Adapter {
  private runtime?: SyncRuntime;
  private reloadTimer?: NodeJS.Timeout;
  private configPollTimer?: NodeJS.Timeout;
  private isReloading = false;
  private isReconcilingStateCustoms = false;
  private lastChannelsSignature = '';
  private lastReadSubscriptionsSignature = '';
  private readonly adminWriteOptions = { user: 'system.user.admin' };
  private isShuttingDown = false;
  private readonly readStreamControllers = new Map<string, AbortController>();
  private readonly readStateMetaSignatures = new Map<string, string>();
  private readonly readStateDescriptorCache = new Map<
    string,
    { state?: unknown; type?: string; role?: string; name?: string; description?: string }
  >();

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
    await this.cleanupDisabledCustomEntriesOnStartup();
    this.startConfigPolling();
  }

  private async cleanupDisabledCustomEntriesOnStartup(): Promise<void> {
    const customView = await this.getObjectViewAsync('system', 'custom', {
      startkey: '',
      endkey: '\u9999'
    });

    let cleanedCount = 0;
    for (const row of customView.rows) {
      const customMap = row.value as Record<string, CustomStateConfig | undefined> | undefined;
      const namespaceCustom = customMap?.[this.namespace];
      const legacyCustom = customMap?.[this.name];

      if (!namespaceCustom && !legacyCustom) {
        continue;
      }

      const hasEnabledEntry = Boolean(namespaceCustom?.enabled || legacyCustom?.enabled);
      if (hasEnabledEntry) {
        continue;
      }

      const object = await this.getForeignObjectAsync(row.id);
      if (!object || object.type !== 'state' || !object.common?.custom) {
        continue;
      }

      const nextObject = JSON.parse(JSON.stringify(object)) as ioBroker.StateObject;
      delete nextObject.common.custom?.[this.namespace];
      delete nextObject.common.custom?.[this.name];
      if (nextObject.common.custom && !Object.keys(nextObject.common.custom).length) {
        delete nextObject.common.custom;
      }

      if (JSON.stringify(object.common.custom ?? null) === JSON.stringify(nextObject.common.custom ?? null)) {
        continue;
      }

      await this.setForeignObjectAsync(row.id, nextObject, this.adminWriteOptions);
      cleanedCount++;
    }

    if (cleanedCount > 0) {
      this.log.info(`Cleaned ${cleanedCount} disabled Firebase custom object entries on startup`);
    }
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
      const readSubscriptions = this.getReadSubscriptions(instanceObject, config);
      this.lastReadSubscriptionsSignature = JSON.stringify(readSubscriptions);
      await this.reconcileStateCustomsFromInstanceObject(instanceObject);
      await this.restartReadSubscriptions(readSubscriptions, config);
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
    return Boolean(customMap?.[this.namespace]?.enabled || customMap?.[this.name]?.enabled);
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
      await this.stopReadSubscriptions();

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
    }, 10000);
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
    const currentReadSubscriptions = this.getReadSubscriptions(instanceObject, this.config as AdapterNativeConfig);
    const currentReadSubscriptionsSignature = JSON.stringify(currentReadSubscriptions);
    const readSubscriptionsChanged = currentReadSubscriptionsSignature !== this.lastReadSubscriptionsSignature;

    if (configChanged) {
      this.lastChannelsSignature = currentSignature;
      this.log.info(`Detected Firebase channel config change via polling (${currentChannels.length} channels)`);
    }
    if (readSubscriptionsChanged) {
      this.lastReadSubscriptionsSignature = currentReadSubscriptionsSignature;
      this.log.info(`Detected RTDB read subscription config change via polling (${currentReadSubscriptions.length} items)`);
      await this.restartReadSubscriptions(currentReadSubscriptions, this.config as AdapterNativeConfig);
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
      return;
    }

    if (obj.command === 'listReadableRtdbPaths') {
      void this.handleListReadableRtdbPathsMessage(obj);
      return;
    }

    if (obj.command === 'createReadableRtdbDatapoint') {
      void this.handleCreateReadableRtdbDatapointMessage(obj);
      return;
    }

    if (obj.command === 'readReadableRtdbDatapoint') {
      void this.handleReadReadableRtdbDatapointMessage(obj);
      return;
    }

    if (obj.command === 'deleteReadableRtdbDatapoints') {
      void this.handleDeleteReadableRtdbDatapointsMessage(obj);
      return;
    }

    if (obj.command === 'listKnownRoles') {
      void this.handleListKnownRolesMessage(obj);
      return;
    }
  }

  private async handleReconcileCustomsMessage(obj: ioBroker.Message): Promise<void> {
    const messagePayload = obj.message as { stateIdsToRemove?: unknown } | undefined;
    const removalPayload = messagePayload?.stateIdsToRemove;
    const requestedRemovals = Array.isArray(removalPayload)
      ? removalPayload
        .map((id: unknown) => String(id || '').trim())
        .filter(Boolean)
      : [];
    const forcedRemovals = new Set(requestedRemovals);

    this.sendTo(obj.from, obj.command, { ok: true, queued: true }, obj.callback);

    void (async () => {
      try {
        const instanceObject = await this.getForeignObjectAsync(`system.adapter.${this.namespace}`);
        await this.reconcileStateCustomsFromInstanceObject(instanceObject, forcedRemovals);
      } catch (error) {
        this.log.error(`Could not reconcile Firebase custom state config: ${(error as Error).message}`);
      }
    })();
  }

  private async handleListReadableRtdbPathsMessage(obj: ioBroker.Message): Promise<void> {
    try {
      const payload = obj.message as { path?: unknown; maxEntries?: unknown; maxDepth?: unknown } | undefined;
      const relativePath = normalizeRelativePath(payload?.path, 'data');
      const maxEntries = clampNumber(payload?.maxEntries, 800, 50, 5000);
      const maxDepth = clampNumber(payload?.maxDepth, 10, 1, 16);

      const config = this.config as AdapterNativeConfig;
      const databaseUrl = config.databaseUrl?.trim();
      const serviceAccountJson = await this.readServiceAccountJson(config);
      const rootPath = normalizeRelativePath(config.rootPath, 'home');

      if (!databaseUrl) {
        throw new Error('databaseUrl is empty');
      }
      if (!serviceAccountJson) {
        throw new Error('serviceAccountJson is empty');
      }

      let serviceAccount: FirebaseServiceAccount;
      try {
        serviceAccount = JSON.parse(serviceAccountJson) as FirebaseServiceAccount;
        validateServiceAccount(serviceAccount);
      } catch (error) {
        throw new Error(`Invalid serviceAccountJson: ${(error as Error).message}`);
      }

      const firebase = new FirebaseRtdbClient({
        databaseUrl,
        rootPath,
        serviceAccount,
        dryRun: false
      });

      const fullPath = relativePath ? `${rootPath}/${relativePath}` : rootPath;
      const value = await firebase.read(fullPath);
      const scanResult = collectReadableDatapointObjects(value, fullPath, maxEntries, maxDepth);
      const rootPrefix = `${rootPath}/`;
      const entries = scanResult.entries.map((entry) => {
        const normalizedPath = entry.path.startsWith(rootPrefix)
          ? entry.path.slice(rootPrefix.length)
          : entry.path === rootPath
            ? ''
            : entry.path.replace(/^\/+/, '');
        return {
          ...entry,
          path: normalizedPath
        };
      });

      this.sendTo(
        obj.from,
        obj.command,
        {
          ok: true,
          path: fullPath,
          count: entries.length,
          truncated: scanResult.truncated,
          maxEntries,
          maxDepth,
          entries
        },
        obj.callback
      );
    } catch (error) {
      this.sendTo(
        obj.from,
        obj.command,
        {
          ok: false,
          error: (error as Error).message
        },
        obj.callback
      );
    }
  }

  private async handleCreateReadableRtdbDatapointMessage(obj: ioBroker.Message): Promise<void> {
    try {
      const payload = obj.message as { path?: unknown; data?: unknown } | undefined;
      const relativePath = normalizeRelativePath(payload?.path, '');
      if (!relativePath) {
        throw new Error('path is required');
      }
      const data = payload?.data;
      if (!data || typeof data !== 'object' || Array.isArray(data)) {
        throw new Error('data must be an object');
      }
      if (!Object.prototype.hasOwnProperty.call(data as Record<string, unknown>, 'state')) {
        throw new Error('data.state is required');
      }

      const config = this.config as AdapterNativeConfig;
      const firebaseClientInfo = await this.createFirebaseClientFromConfig(config);
      const fullPath = `${firebaseClientInfo.rootPath}/${relativePath}`;
      await firebaseClientInfo.firebase.write(fullPath, data);

      this.sendTo(obj.from, obj.command, { ok: true, path: fullPath }, obj.callback);
    } catch (error) {
      this.sendTo(
        obj.from,
        obj.command,
        { ok: false, error: (error as Error).message },
        obj.callback
      );
    }
  }

  private async handleReadReadableRtdbDatapointMessage(obj: ioBroker.Message): Promise<void> {
    try {
      const payload = obj.message as { path?: unknown } | undefined;
      const relativePath = normalizeRelativePath(payload?.path, '');
      if (!relativePath) {
        throw new Error('path is required');
      }

      const config = this.config as AdapterNativeConfig;
      const firebaseClientInfo = await this.createFirebaseClientFromConfig(config);
      const fullPath = `${firebaseClientInfo.rootPath}/${relativePath}`;
      const value = await firebaseClientInfo.firebase.read(fullPath);

      this.sendTo(
        obj.from,
        obj.command,
        { ok: true, path: fullPath, data: value },
        obj.callback
      );
    } catch (error) {
      this.sendTo(
        obj.from,
        obj.command,
        { ok: false, error: (error as Error).message },
        obj.callback
      );
    }
  }

  private async handleDeleteReadableRtdbDatapointsMessage(obj: ioBroker.Message): Promise<void> {
    try {
      const payload = obj.message as { paths?: unknown } | undefined;
      const relativePaths = Array.isArray(payload?.paths)
        ? payload?.paths
          .map((pathValue) => normalizeRelativePath(pathValue, ''))
          .filter(Boolean)
        : [];
      if (!relativePaths.length) {
        throw new Error('paths is required');
      }

      const config = this.config as AdapterNativeConfig;
      const firebaseClientInfo = await this.createFirebaseClientFromConfig(config);
      const deletedPaths: string[] = [];
      for (const relativePath of relativePaths) {
        const fullPath = `${firebaseClientInfo.rootPath}/${relativePath}`;
        await firebaseClientInfo.firebase.delete(fullPath);
        deletedPaths.push(fullPath);
      }

      this.sendTo(
        obj.from,
        obj.command,
        { ok: true, deleted: deletedPaths.length, paths: deletedPaths },
        obj.callback
      );
    } catch (error) {
      this.sendTo(
        obj.from,
        obj.command,
        { ok: false, error: (error as Error).message },
        obj.callback
      );
    }
  }

  private async handleListKnownRolesMessage(obj: ioBroker.Message): Promise<void> {
    try {
      const rolesFromDocs = await this.fetchKnownRolesFromOfficialDocs();
      const rolesFromStates = await this.collectKnownRolesFromStates();
      const mergedRoles = [...new Set([...rolesFromDocs, ...rolesFromStates])]
        .filter(Boolean)
        .filter((role) => isAllowedRtdbRole(role))
        .sort((a, b) => a.localeCompare(b));

      this.sendTo(
        obj.from,
        obj.command,
        { ok: true, roles: mergedRoles },
        obj.callback
      );
    } catch (error) {
      this.sendTo(
        obj.from,
        obj.command,
        { ok: false, error: (error as Error).message },
        obj.callback
      );
    }
  }

  private async createFirebaseClientFromConfig(
    config: AdapterNativeConfig
  ): Promise<{ firebase: FirebaseRtdbClient; rootPath: string }> {
    const databaseUrl = config.databaseUrl?.trim();
    const serviceAccountJson = await this.readServiceAccountJson(config);
    const rootPath = normalizeRelativePath(config.rootPath, 'home');

    if (!databaseUrl) {
      throw new Error('databaseUrl is empty');
    }
    if (!serviceAccountJson) {
      throw new Error('serviceAccountJson is empty');
    }

    let serviceAccount: FirebaseServiceAccount;
    try {
      serviceAccount = JSON.parse(serviceAccountJson) as FirebaseServiceAccount;
      validateServiceAccount(serviceAccount);
    } catch (error) {
      throw new Error(`Invalid serviceAccountJson: ${(error as Error).message}`);
    }

    return {
      firebase: new FirebaseRtdbClient({
        databaseUrl,
        rootPath,
        serviceAccount,
        dryRun: false
      }),
      rootPath
    };
  }

  private async fetchKnownRolesFromOfficialDocs(): Promise<string[]> {
    const url = 'https://raw.githubusercontent.com/ioBroker/ioBroker.docs/master/docs/en/dev/stateroles.md';
    const response = await fetch(url, { method: 'GET' });
    if (!response.ok) {
      throw new Error(`Could not fetch stateroles.md (${response.status})`);
    }
    const markdown = await response.text();
    return parseRolesFromMarkdown(markdown);
  }

  private async collectKnownRolesFromStates(): Promise<string[]> {
    const result = await this.getObjectViewAsync('system', 'state', {
      startkey: '',
      endkey: '\u9999'
    });

    const roles = new Set<string>();
    for (const row of result.rows) {
      const role = String((row.value as ioBroker.StateObject | undefined)?.common?.role ?? '').trim();
      if (role) {
        roles.add(role);
      }
    }
    return [...roles];
  }

  private async reconcileStateCustomsFromInstanceObject(
    instanceObject: ioBroker.Object | null | undefined,
    forcedRemovals?: Set<string>
  ): Promise<boolean> {
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
    this.log.debug(`Reconciling Firebase custom config for ${desiredChannels.length} desired channel(s)`);
    const desiredByStateId = new Map(desiredChannels.map((channel) => [channel.stateId, channel]));
    const customView = await this.getObjectViewAsync('system', 'custom', {
      startkey: '',
      endkey: '\u9999'
    });
    const removalSet = forcedRemovals ?? new Set<string>();
    const candidateStateIds = new Set<string>(desiredByStateId.keys());
    for (const row of customView.rows) {
      const customMap = row.value as Record<string, CustomStateConfig | undefined> | undefined;
      const namespaceCustom = customMap?.[this.namespace];
      const legacyCustom = customMap?.[this.name];
      if (namespaceCustom?.enabled || legacyCustom?.enabled) {
        candidateStateIds.add(row.id);
      }
    }

    this.isReconcilingStateCustoms = true;
    let changed = false;
    try {
      for (const stateId of candidateStateIds) {
        const object = await this.getForeignObjectAsync(stateId);
        if (!object?.common || object.type !== 'state') {
          continue;
        }

        const customMap = object.common.custom as Record<string, CustomStateConfig | undefined> | undefined;
        const namespaceCustom = customMap?.[this.namespace];
        const legacyCustom = customMap?.[this.name];
        const hasFirebaseCustom = Boolean(namespaceCustom?.enabled || legacyCustom?.enabled);
        const desiredChannelRaw = desiredByStateId.get(stateId);
        const desiredChannel = desiredChannelRaw && desiredChannelRaw.enabled !== false && desiredChannelRaw.sync !== false
          ? desiredChannelRaw
          : undefined;
        const isDisabledByChannel = Boolean(
          desiredChannelRaw && (desiredChannelRaw.enabled === false || desiredChannelRaw.sync === false)
        );
        const isForcedRemoval = removalSet.has(stateId) || isDisabledByChannel;

        if (!hasFirebaseCustom && !desiredChannel && !isForcedRemoval) {
          continue;
        }

        this.log.debug(
          `Reconciling state ${stateId}: hasCustom=${hasFirebaseCustom} desired=${Boolean(desiredChannel)}`
        );

        const nextObject = JSON.parse(JSON.stringify(object)) as ioBroker.StateObject;
        nextObject.common.custom = nextObject.common.custom || {};

        if (desiredChannel) {
          nextObject.common.custom[this.namespace] = this.channelToCustomStateConfig(desiredChannel, config);
          delete nextObject.common.custom[this.name];
        } else if (isForcedRemoval) {
          delete nextObject.common.custom[this.namespace];
          delete nextObject.common.custom[this.name];

          if (!Object.keys(nextObject.common.custom).length) {
            delete nextObject.common.custom;
          }
        } else {
          continue;
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
        let verified = desiredChannel
          ? Boolean(verifyNamespaceCustom?.enabled && verifyNamespaceCustom.sync !== false)
          : Boolean(!verifyNamespaceCustom && !verifyLegacyCustom);

        if (isForcedRemoval && !verified) {
          const fallbackObject = JSON.parse(JSON.stringify(object)) as ioBroker.StateObject;
          fallbackObject.common.custom = fallbackObject.common.custom || {};
          const disabledCustom: CustomStateConfig = {
            enabled: false,
            sync: false,
            key: namespaceCustom?.key?.trim() || legacyCustom?.key?.trim() || objectIdToFirebaseKey(stateId),
            mode: namespaceCustom?.mode ?? legacyCustom?.mode ?? 'threshold',
            minChange: namespaceCustom?.minChange ?? legacyCustom?.minChange ?? 0,
            factor: namespaceCustom?.factor ?? legacyCustom?.factor ?? 1,
            transform: namespaceCustom?.transform ?? legacyCustom?.transform ?? 'none',
            round: namespaceCustom?.round ?? legacyCustom?.round ?? 1,
            minSendIntervalMs: namespaceCustom?.minSendIntervalMs ?? legacyCustom?.minSendIntervalMs ?? 10000,
            maxSendIntervalMs: namespaceCustom?.maxSendIntervalMs ?? legacyCustom?.maxSendIntervalMs ?? 900000,
            dailyHour: namespaceCustom?.dailyHour ?? legacyCustom?.dailyHour ?? config.dailyWriteHour ?? 0,
            dailyMinute: namespaceCustom?.dailyMinute ?? legacyCustom?.dailyMinute ?? config.dailyWriteMinute ?? 10,
            defaultValue: namespaceCustom?.defaultValue ?? legacyCustom?.defaultValue ?? null
          };
          fallbackObject.common.custom[this.namespace] = disabledCustom;
          fallbackObject.common.custom[this.name] = { ...disabledCustom };
          await this.setForeignObjectAsync(stateId, fallbackObject, this.adminWriteOptions);

          const fallbackVerifyObject = await this.getForeignObjectAsync(stateId);
          const fallbackVerifyMap = fallbackVerifyObject?.common?.custom as Record<string, CustomStateConfig | undefined> | undefined;
          const fallbackNamespaceCustom = fallbackVerifyMap?.[this.namespace];
          const fallbackLegacyCustom = fallbackVerifyMap?.[this.name];
          verified = Boolean(
            fallbackNamespaceCustom?.enabled === false &&
            fallbackNamespaceCustom?.sync === false &&
            fallbackLegacyCustom?.enabled === false &&
            fallbackLegacyCustom?.sync === false
          );
        }

        this.log.debug(
          `Reconciled state ${stateId}: verified=${verified} namespaceEnabled=${verifyNamespaceCustom?.enabled ?? 'missing'} legacyEnabled=${verifyLegacyCustom?.enabled ?? 'missing'}`
        );

        if (verified) {
          changed = true;
        } else {
          this.log.debug(`State ${stateId} did not reach desired Firebase custom state after reconcile write`);
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

  private getReadSubscriptions(
    instanceObject: ioBroker.Object | null | undefined,
    config: AdapterNativeConfig
  ): ReadSubscriptionConfig[] {
    const source = Array.isArray(instanceObject?.native?.readSubscriptions)
      ? instanceObject?.native?.readSubscriptions
      : Array.isArray(config.readSubscriptions)
        ? config.readSubscriptions
        : [];

    const byPath = new Map<string, ReadSubscriptionConfig>();
    for (const item of source) {
      const path = normalizeRelativePath((item as Partial<ReadSubscriptionConfig> | undefined)?.path, '');
      if (!path) {
        continue;
      }
      const stateIdRaw = String((item as Partial<ReadSubscriptionConfig> | undefined)?.stateId ?? '').trim();
      const stateId = stateIdRaw || readPathToDefaultStateId(this.namespace, path);
      const enabled = (item as Partial<ReadSubscriptionConfig> | undefined)?.enabled !== false;
      if (!enabled) {
        continue;
      }
      byPath.set(path, { path, stateId, enabled: true });
    }

    return [...byPath.values()].sort((a, b) => a.path.localeCompare(b.path));
  }

  private async restartReadSubscriptions(
    subscriptions: ReadSubscriptionConfig[],
    config: AdapterNativeConfig
  ): Promise<void> {
    await this.stopReadSubscriptions();
    if (this.isShuttingDown || !subscriptions.length) {
      return;
    }

    const databaseUrl = config.databaseUrl?.trim();
    const serviceAccountJson = await this.readServiceAccountJson(config);
    const rootPath = normalizeRelativePath(config.rootPath, 'home');

    if (!databaseUrl || !serviceAccountJson) {
      this.log.warn('Skipping RTDB read subscriptions: adapter credentials are incomplete');
      return;
    }

    let serviceAccount: FirebaseServiceAccount;
    try {
      serviceAccount = JSON.parse(serviceAccountJson) as FirebaseServiceAccount;
      validateServiceAccount(serviceAccount);
    } catch (error) {
      this.log.warn(`Skipping RTDB read subscriptions: invalid serviceAccountJson (${(error as Error).message})`);
      return;
    }

    const firebase = new FirebaseRtdbClient({
      databaseUrl,
      rootPath,
      serviceAccount,
      dryRun: false
    });

    for (const subscription of subscriptions) {
      await this.ensureReadStateObject(subscription);
      const fullPath = normalizeRelativePath(`${rootPath}/${subscription.path}`, rootPath);
      this.startReadSubscriptionLoop(firebase, subscription, fullPath);
    }

    this.log.info(`Started ${subscriptions.length} RTDB read subscription(s)`);
  }

  private startReadSubscriptionLoop(
    firebase: FirebaseRtdbClient,
    subscription: ReadSubscriptionConfig,
    fullPath: string
  ): void {
    const loopKey = `${subscription.path}=>${subscription.stateId}`;
    const controller = new AbortController();
    this.readStreamControllers.set(loopKey, controller);

    void (async () => {
      while (!this.isShuttingDown && this.readStreamControllers.get(loopKey) === controller) {
        try {
          await firebase.stream(fullPath, async (event, payload) => {
            await this.handleReadStreamEvent(subscription, event, payload);
          }, controller.signal);
        } catch (error) {
          if (controller.signal.aborted || this.isShuttingDown) {
            return;
          }
          this.log.warn(`RTDB read stream failed for ${fullPath}: ${(error as Error).message}`);
        }

        if (controller.signal.aborted || this.isShuttingDown || this.readStreamControllers.get(loopKey) !== controller) {
          return;
        }

        await waitMs(3000);
      }
    })();
  }

  private async handleReadStreamEvent(
    subscription: ReadSubscriptionConfig,
    event: string,
    payload: unknown
  ): Promise<void> {
    if (event === 'keep-alive') {
      return;
    }

    if (event === 'cancel' || event === 'auth_revoked') {
      this.log.warn(`RTDB read stream revoked for ${subscription.path}: ${event}`);
      return;
    }

    if (event !== 'put' && event !== 'patch') {
      return;
    }

    const streamPayload = extractFirebaseStreamPayload(payload);
    if (!streamPayload) {
      return;
    }

    const cachedDescriptor = this.readStateDescriptorCache.get(subscription.stateId);
    const mergedDescriptor = mergeReadDescriptor(cachedDescriptor, streamPayload.path, streamPayload.data);
    if (!mergedDescriptor) {
      return;
    }
    this.readStateDescriptorCache.set(subscription.stateId, mergedDescriptor);

    const parsed = parseIncomingReadPayload(mergedDescriptor);
    if (!parsed) {
      return;
    }

    if (parsed.meta) {
      await this.updateReadStateObjectMetadata(subscription, parsed.meta);
    }

    await this.setForeignStateAsync(subscription.stateId, parsed.stateValue, true);
  }

  private async ensureReadStateObject(subscription: ReadSubscriptionConfig): Promise<void> {
    const existing = await this.getForeignObjectAsync(subscription.stateId);
    if (existing?.type === 'state') {
      return;
    }

    const stateObject: ioBroker.StateObject = {
      _id: subscription.stateId,
      type: 'state',
      common: {
        name: `Firebase read ${subscription.path}`,
        type: 'mixed',
        role: 'value',
        read: true,
        write: false
      },
      native: {
        firebaseReadPath: subscription.path
      }
    };
    await this.setForeignObjectAsync(subscription.stateId, stateObject, this.adminWriteOptions);
  }

  private async updateReadStateObjectMetadata(
    subscription: ReadSubscriptionConfig,
    meta: { type?: string; role?: string; name?: string; description?: string }
  ): Promise<void> {
    const cleanMeta: { type?: string; role?: string; name?: string; description?: string } = {};
    if (meta.type) {
      cleanMeta.type = meta.type;
    }
    if (meta.role) {
      cleanMeta.role = meta.role;
    }
    if (meta.name) {
      cleanMeta.name = meta.name;
    }
    if (meta.description) {
      cleanMeta.description = meta.description;
    }

    const signature = JSON.stringify(cleanMeta);
    const cachedSignature = this.readStateMetaSignatures.get(subscription.stateId);
    if (signature === cachedSignature) {
      return;
    }

    const existing = await this.getForeignObjectAsync(subscription.stateId);
    if (!existing || existing.type !== 'state') {
      await this.ensureReadStateObject(subscription);
      this.readStateMetaSignatures.set(subscription.stateId, signature);
      return;
    }

    const nextObject = JSON.parse(JSON.stringify(existing)) as ioBroker.StateObject;
    nextObject.common = nextObject.common || {};
    nextObject.common.read = true;
    nextObject.common.write = false;
    if (cleanMeta.type) {
      nextObject.common.type = cleanMeta.type as ioBroker.CommonType;
    }
    if (cleanMeta.role) {
      nextObject.common.role = cleanMeta.role;
    }
    if (cleanMeta.name) {
      nextObject.common.name = cleanMeta.name;
    }
    if (cleanMeta.description) {
      nextObject.common.desc = cleanMeta.description;
    }
    nextObject.native = nextObject.native || {};
    nextObject.native.firebaseReadPath = subscription.path;

    if (
      JSON.stringify(existing.common ?? null) === JSON.stringify(nextObject.common ?? null) &&
      JSON.stringify(existing.native ?? null) === JSON.stringify(nextObject.native ?? null)
    ) {
      this.readStateMetaSignatures.set(subscription.stateId, signature);
      return;
    }

    await this.setForeignObjectAsync(subscription.stateId, nextObject, this.adminWriteOptions);
    this.readStateMetaSignatures.set(subscription.stateId, signature);
  }

  private async stopReadSubscriptions(): Promise<void> {
    for (const controller of this.readStreamControllers.values()) {
      controller.abort();
    }
    this.readStreamControllers.clear();
    this.readStateMetaSignatures.clear();
    this.readStateDescriptorCache.clear();
  }

  private onUnload(callback: () => void): void {
    void (async () => {
      try {
        this.isShuttingDown = true;
        if (this.configPollTimer) {
          clearInterval(this.configPollTimer);
          this.configPollTimer = undefined;
        }
        if (this.reloadTimer) {
          clearTimeout(this.reloadTimer);
          this.reloadTimer = undefined;
        }
        await this.stopReadSubscriptions();
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

function normalizeRelativePath(value: unknown, fallback: string): string {
  const trimmed = String(value ?? '').trim();
  const normalized = trimmed.replace(/^\/+|\/+$/g, '');
  if (normalized) {
    return normalized;
  }
  return fallback.replace(/^\/+|\/+$/g, '');
}

function clampNumber(value: unknown, fallback: number, min: number, max: number): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return Math.max(min, Math.min(max, Math.floor(parsed)));
}

function readPathToDefaultStateId(namespace: string, path: string): string {
  const suffix = normalizeRelativePath(path, '')
    .split('/')
    .map((part) => part.trim().replace(/[^a-zA-Z0-9_-]+/g, '_').replace(/^_+|_+$/g, ''))
    .filter(Boolean)
    .join('.');
  return suffix ? `${namespace}.read.${suffix}` : `${namespace}.read.value`;
}

function extractFirebaseStreamPayload(payload: unknown): { path: string; data: unknown } | null {
  if (!payload || typeof payload !== 'object') {
    return null;
  }

  const typedPayload = payload as { path?: unknown; data?: unknown };
  if (!('data' in typedPayload)) {
    return null;
  }
  return {
    path: String(typedPayload.path ?? '/'),
    data: typedPayload.data
  };
}

function normalizeStateValue(value: unknown): ioBroker.StateValue {
  if (
    value === null ||
    typeof value === 'string' ||
    typeof value === 'number' ||
    typeof value === 'boolean'
  ) {
    return value;
  }
  return JSON.stringify(value);
}

function parseIncomingReadPayload(
  value: unknown
): { stateValue: ioBroker.StateValue; meta?: { type?: string; role?: string; name?: string; description?: string } } | null {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return { stateValue: normalizeStateValue(value) };
  }

  const payload = value as Record<string, unknown>;
  if (!Object.prototype.hasOwnProperty.call(payload, 'state')) {
    return { stateValue: normalizeStateValue(value) };
  }

  const declaredType = resolveDeclaredStateType(payload);
  const normalizedStateValue = normalizeStateValueWithDeclaredType(payload.state, declaredType);
  if (normalizedStateValue === undefined) {
    return null;
  }

  const role = readStringField(payload, ['role', 'zustandstyp', 'stateRole']);
  const name = readStringField(payload, ['name', 'label', 'title', 'titel']);
  const description = readStringField(payload, ['description', 'beschreibung', 'desc']);

  return {
    stateValue: normalizedStateValue,
    meta: {
      type: declaredType,
      role,
      name,
      description
    }
  };
}

function mergeReadDescriptor(
  current: { state?: unknown; type?: string; role?: string; name?: string; description?: string } | undefined,
  eventPath: string,
  eventData: unknown
): { state?: unknown; type?: string; role?: string; name?: string; description?: string } | null {
  const normalizedPath = String(eventPath || '/').trim();

  if (normalizedPath === '/' || normalizedPath === '') {
    if (!eventData || typeof eventData !== 'object' || Array.isArray(eventData)) {
      return null;
    }
    const objectValue = eventData as Record<string, unknown>;
    if (!Object.prototype.hasOwnProperty.call(objectValue, 'state')) {
      return null;
    }
    return {
      state: objectValue.state,
      type: resolveDeclaredStateType(objectValue),
      role: readStringField(objectValue, ['role', 'zustandstyp', 'stateRole']),
      name: readStringField(objectValue, ['name', 'label', 'title', 'titel']),
      description: readStringField(objectValue, ['description', 'beschreibung', 'desc'])
    };
  }

  const descriptor = { ...(current ?? {}) };
  const pathParts = normalizedPath
    .split('/')
    .map((part) => part.trim())
    .filter(Boolean);
  if (!pathParts.length) {
    return descriptor;
  }

  const field = pathParts[0].toLowerCase();
  if (field === 'state') {
    descriptor.state = eventData;
    return descriptor;
  }
  if (field === 'type') {
    descriptor.type = resolveDeclaredStateType({ type: eventData });
    return descriptor;
  }
  if (field === 'role') {
    descriptor.role = typeof eventData === 'string' ? eventData.trim() || undefined : descriptor.role;
    return descriptor;
  }
  if (field === 'name' || field === 'label' || field === 'title' || field === 'titel') {
    descriptor.name = typeof eventData === 'string' ? eventData.trim() || undefined : descriptor.name;
    return descriptor;
  }
  if (field === 'description' || field === 'beschreibung' || field === 'desc') {
    descriptor.description = typeof eventData === 'string' ? eventData.trim() || undefined : descriptor.description;
    return descriptor;
  }

  return descriptor;
}

function resolveDeclaredStateType(payload: Record<string, unknown>): string | undefined {
  const rawType = readStringField(payload, ['type', 'stateType', 'zustandstyp']);
  if (!rawType) {
    return undefined;
  }

  const normalized = rawType.trim().toLowerCase();
  if (normalized === 'bool') {
    return 'boolean';
  }
  if (normalized === 'int' || normalized === 'float' || normalized === 'double') {
    return 'number';
  }
  if (normalized === 'str') {
    return 'string';
  }
  if (normalized === 'obj') {
    return 'object';
  }
  if (normalized === 'arr') {
    return 'array';
  }
  if (
    normalized === 'string' ||
    normalized === 'number' ||
    normalized === 'boolean' ||
    normalized === 'array' ||
    normalized === 'object' ||
    normalized === 'mixed'
  ) {
    return normalized;
  }
  return undefined;
}

function normalizeStateValueWithDeclaredType(value: unknown, declaredType?: string): ioBroker.StateValue | undefined {
  if (!declaredType) {
    return normalizeStateValue(value);
  }

  if (declaredType === 'boolean') {
    if (typeof value === 'boolean') {
      return value;
    }
    if (typeof value === 'number') {
      return value !== 0;
    }
    if (typeof value === 'string') {
      const lowered = value.trim().toLowerCase();
      if (['true', '1', 'yes', 'ja', 'on'].includes(lowered)) {
        return true;
      }
      if (['false', '0', 'no', 'nein', 'off', ''].includes(lowered)) {
        return false;
      }
    }
    return Boolean(value);
  }

  if (declaredType === 'number') {
    if (value === null || value === undefined || value === '') {
      return null;
    }
    const numberValue = Number(value);
    if (!Number.isFinite(numberValue)) {
      return undefined;
    }
    return numberValue;
  }

  if (declaredType === 'string') {
    if (value === null || value === undefined) {
      return '';
    }
    return typeof value === 'string' ? value : JSON.stringify(value);
  }

  return normalizeStateValue(value);
}

function readStringField(payload: Record<string, unknown>, fieldNames: string[]): string | undefined {
  for (const fieldName of fieldNames) {
    if (!Object.prototype.hasOwnProperty.call(payload, fieldName)) {
      continue;
    }
    const rawValue = payload[fieldName];
    if (typeof rawValue !== 'string') {
      continue;
    }
    const trimmedValue = rawValue.trim();
    if (trimmedValue) {
      return trimmedValue;
    }
  }
  return undefined;
}

function waitMs(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function collectReadableDatapointObjects(
  value: unknown,
  basePath: string,
  maxEntries: number,
  maxDepth: number
): {
  entries: Array<{
    path: string;
    type: string;
    preview: string;
    role?: string;
    description?: string;
    name?: string;
  }>;
  truncated: boolean;
} {
  const entries: Array<{
    path: string;
    type: string;
    preview: string;
    role?: string;
    description?: string;
    name?: string;
  }> = [];
  let truncated = false;

  const walk = (node: unknown, currentPath: string, depth: number): void => {
    if (entries.length >= maxEntries) {
      truncated = true;
      return;
    }

    if (node === null || node === undefined) {
      return;
    }

    if (Array.isArray(node)) {
      if (depth >= maxDepth) {
        if (node.length > 0) {
          truncated = true;
        }
        return;
      }
      for (let index = 0; index < node.length; index++) {
        walk(node[index], `${currentPath}/${index}`, depth + 1);
        if (entries.length >= maxEntries) {
          return;
        }
      }
      return;
    }

    if (typeof node === 'object') {
      const objectValue = node as Record<string, unknown>;
      if (Object.prototype.hasOwnProperty.call(objectValue, 'state')) {
        const declaredType = resolveDeclaredStateType(objectValue) ?? typeof objectValue.state;
        const previewValue = normalizeStateValueWithDeclaredType(objectValue.state, declaredType);
        const role = readStringField(objectValue, ['role', 'zustandstyp', 'stateRole']);
        const description = readStringField(objectValue, ['description', 'beschreibung', 'desc']);
        const name = readStringField(objectValue, ['name', 'label', 'title', 'titel']);
        entries.push({
          path: currentPath,
          type: declaredType,
          preview: formatPreview(previewValue),
          role,
          description,
          name
        });
        return;
      }

      const keys = Object.keys(objectValue);
      if (depth >= maxDepth) {
        if (keys.length > 0) {
          truncated = true;
        }
        return;
      }
      for (const key of keys) {
        walk(objectValue[key], `${currentPath}/${key}`, depth + 1);
        if (entries.length >= maxEntries) {
          return;
        }
      }
      return;
    }
  };

  walk(value, basePath, 0);
  return { entries, truncated };
}

function formatPreview(value: unknown): string {
  if (value === null) {
    return 'null';
  }
  if (typeof value === 'string') {
    const text = `"${value}"`;
    return text.length > 120 ? `${text.slice(0, 117)}...` : text;
  }
  const text = String(value);
  return text.length > 120 ? `${text.slice(0, 117)}...` : text;
}

function parseRolesFromMarkdown(markdown: string): string[] {
  const roles = new Set<string>();
  const codeRoleRegex = /`{1,2}([a-z][a-z0-9]*(?:\.[a-z0-9*]+)*)`{1,2}/g;
  let match: RegExpExecArray | null;
  while ((match = codeRoleRegex.exec(markdown)) !== null) {
    const rawRole = String(match[1] ?? '').trim();
    if (!rawRole) {
      continue;
    }
    if (rawRole.includes(' ') || rawRole.includes('=') || rawRole.includes(':')) {
      continue;
    }
    roles.add(rawRole);
  }
  return [...roles];
}

function isAllowedRtdbRole(role: string): boolean {
  const normalized = String(role || '').trim().toLowerCase();
  return (
    normalized === 'sensor' ||
    normalized.startsWith('sensor.') ||
    normalized === 'button' ||
    normalized.startsWith('button.') ||
    normalized === 'indicator' ||
    normalized.startsWith('indicator.') ||
    normalized === 'switch' ||
    normalized.startsWith('switch.')
  );
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
