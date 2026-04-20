import type { ChannelConfig } from './types';

export const DEFAULT_CHANNELS: ChannelConfig[] = [
  {
    key: 'pv.mttp1Power',
    stateId: '0_userdata.0.PV.Mttp1Power',
    mode: 'threshold',
    minChange: 150,
    factor: 1,
    transform: 'none',
    round: 0,
    minSendIntervalMs: 5000,
    maxSendIntervalMs: 600000
  },
  {
    key: 'pv.mttp2Power',
    stateId: '0_userdata.0.PV.Mttp2Power',
    mode: 'threshold',
    minChange: 150,
    factor: 1,
    transform: 'none',
    round: 0,
    minSendIntervalMs: 5000,
    maxSendIntervalMs: 600000
  },
  {
    key: 'pv.produktionPower',
    stateId: 'modbus.0.inputRegisters.5016_Total_DC_Power',
    mode: 'threshold',
    minChange: 100,
    factor: 1,
    transform: 'none',
    round: 0,
    minSendIntervalMs: 5000,
    maxSendIntervalMs: 600000
  },
  {
    key: 'pv.verbrauchPower',
    stateId: 'modbus.0.inputRegisters.13007_Load_power_',
    mode: 'threshold',
    minChange: 80,
    factor: 1,
    transform: 'none',
    round: 0,
    minSendIntervalMs: 5000,
    maxSendIntervalMs: 600000
  },
  {
    key: 'pv.exportPower',
    stateId: 'modbus.0.inputRegisters.13009_Export_power',
    mode: 'threshold',
    minChange: 80,
    factor: 1,
    transform: 'none',
    round: 0,
    minSendIntervalMs: 5000,
    maxSendIntervalMs: 600000
  },
  {
    key: 'pv.batteryPower',
    stateId: 'modbus.0.inputRegisters.13021_Battery_power_',
    mode: 'threshold',
    minChange: 100,
    factor: 1,
    transform: 'none',
    round: 0,
    minSendIntervalMs: 5000,
    maxSendIntervalMs: 30000
  },
  {
    key: 'pv.batteryLevelPct',
    stateId: 'modbus.0.inputRegisters.13022_Battery_level_',
    mode: 'threshold',
    minChange: 1,
    factor: 1,
    transform: 'none',
    round: 0,
    minSendIntervalMs: 30000,
    maxSendIntervalMs: 300000
  },
  {
    key: 'pv.batterieAusEingang',
    stateId: '0_userdata.0.PV.Batterie_AUS/EINGANG',
    mode: 'threshold',
    minChange: 5,
    factor: 1,
    transform: 'positive_only',
    round: 1,
    minSendIntervalMs: 1000,
    maxSendIntervalMs: 300000
  },
  {
    key: 'pv.batterieEntladen',
    stateId: '0_userdata.0.PV.Batterie_entladen',
    mode: 'change',
    factor: 1,
    transform: 'boolean',
    round: 0,
    minSendIntervalMs: 1000,
    maxSendIntervalMs: 300000
  },
  {
    key: 'pv.batterieLaden',
    stateId: '0_userdata.0.PV.Batterie_laden',
    mode: 'change',
    factor: 1,
    transform: 'boolean',
    round: 0,
    minSendIntervalMs: 1000,
    maxSendIntervalMs: 300000
  },
  {
    key: 'weather.rainTodayMm',
    stateId: '0_userdata.0.Umwelt.Regen.rainToday',
    mode: 'threshold',
    minChange: 0.1,
    factor: 1,
    transform: 'none',
    round: 1,
    minSendIntervalMs: 10000,
    maxSendIntervalMs: 2592000000
  },
  {
    key: 'weather.rainMonthMm',
    stateId: '0_userdata.0.Umwelt.Regen.rainMonth',
    mode: 'daily_only',
    factor: 1,
    transform: 'none',
    round: 1
  },
  {
    key: 'weather.rainYesterdayMm',
    stateId: '0_userdata.0.Umwelt.Regen.rainYesterday',
    mode: 'daily_only',
    factor: 1,
    transform: 'none',
    round: 1
  },
  {
    key: 'weather.outdoorHumidityPct',
    stateId: 'hm-rpc.1.00185D89A9EFAE.1.HUMIDITY',
    mode: 'threshold',
    minChange: 1,
    factor: 1,
    transform: 'none',
    round: 0,
    minSendIntervalMs: 30000,
    maxSendIntervalMs: 300000
  },
  {
    key: 'weather.windSpeed',
    stateId: 'hm-rpc.1.00185D89A9EFAE.1.WIND_SPEED',
    mode: 'threshold',
    minChange: 0.5,
    factor: 1,
    transform: 'none',
    round: 1,
    minSendIntervalMs: 15000,
    maxSendIntervalMs: 120000
  },
  {
    key: 'weather.raining',
    stateId: 'hm-rpc.1.00185D89A9EFAE.1.RAINING',
    mode: 'change',
    factor: 1,
    transform: 'boolean',
    round: 0,
    minSendIntervalMs: 1000,
    maxSendIntervalMs: 600000
  },
  {
    key: 'weather.outdoorTempC',
    stateId: 'hm-rpc.1.00185D89A9EFAE.1.ACTUAL_TEMPERATURE',
    mode: 'threshold',
    minChange: 0.2,
    factor: 1,
    transform: 'none',
    round: 1,
    minSendIntervalMs: 30000,
    maxSendIntervalMs: 300000
  },
  {
    key: 'rooms.kleinesZimmer.tempC',
    stateId: 'hm-rpc.1.001CDA49912C3D.1.ACTUAL_TEMPERATURE',
    mode: 'threshold',
    minChange: 0.2,
    factor: 1,
    transform: 'none',
    round: 1,
    minSendIntervalMs: 300000,
    maxSendIntervalMs: 900000
  },
  {
    key: 'rooms.gang.tempC',
    stateId: 'hm-rpc.1.001CE0C9964354.1.ACTUAL_TEMPERATURE',
    mode: 'threshold',
    minChange: 0.2,
    factor: 1,
    transform: 'none',
    round: 1,
    minSendIntervalMs: 300000,
    maxSendIntervalMs: 900000
  },
  {
    key: 'rooms.grossesZimmer.tempC',
    stateId: 'hm-rpc.1.001CE0C9A5E2DD.1.ACTUAL_TEMPERATURE',
    mode: 'threshold',
    minChange: 0.2,
    factor: 1,
    transform: 'none',
    round: 1,
    minSendIntervalMs: 300000,
    maxSendIntervalMs: 900000
  },
  {
    key: 'rooms.schlafzimmer.tempC',
    stateId: 'hm-rpc.1.001D5F2992FC07.1.ACTUAL_TEMPERATURE',
    mode: 'threshold',
    minChange: 0.2,
    factor: 1,
    transform: 'none',
    round: 1,
    minSendIntervalMs: 300000,
    maxSendIntervalMs: 900000
  },
  {
    key: 'rooms.speis.tempC',
    stateId: 'hm-rpc.1.001D5F2992FC0B.1.ACTUAL_TEMPERATURE',
    mode: 'threshold',
    minChange: 0.2,
    factor: 1,
    transform: 'none',
    round: 1,
    minSendIntervalMs: 300000,
    maxSendIntervalMs: 900000
  },
  {
    key: 'rooms.bad.tempC',
    stateId: 'hm-rpc.1.001D5F2992FC17.1.ACTUAL_TEMPERATURE',
    mode: 'threshold',
    minChange: 0.2,
    factor: 1,
    transform: 'none',
    round: 1,
    minSendIntervalMs: 300000,
    maxSendIntervalMs: 900000
  },
  {
    key: 'rooms.bad.humidityPct',
    stateId: 'hm-rpc.1.001D5F2992FC17.1.HUMIDITY',
    mode: 'threshold',
    minChange: 1,
    factor: 1,
    transform: 'none',
    round: 0,
    minSendIntervalMs: 10000,
    maxSendIntervalMs: 900000
  },
  {
    key: 'rooms.wohnzimmer.tempC',
    stateId: 'hm-rpc.1.001D5F2992FCC8.1.ACTUAL_TEMPERATURE',
    mode: 'threshold',
    minChange: 0.2,
    factor: 1,
    transform: 'none',
    round: 1,
    minSendIntervalMs: 10000,
    maxSendIntervalMs: 900000
  }
];

export function mergeChannelOverrides(overrides: Partial<ChannelConfig>[] | undefined): ChannelConfig[] {
  if (!Array.isArray(overrides) || overrides.length === 0) {
    return [];
  }

  return overrides
    .filter((item): item is ChannelConfig => Boolean(item.key && item.stateId))
    .map((channel) => normalizeChannelConfig(channel))
    .filter((channel) => channel.enabled !== false && channel.sync !== false);
}

export function getDefaultChannelConfig(): ChannelConfig[] {
  return DEFAULT_CHANNELS.map((channel) => normalizeChannelConfig(channel));
}

function normalizeChannelConfig(channel: ChannelConfig): ChannelConfig {
  return {
    ...channel,
    enabled: channel.enabled ?? true,
    sync: channel.sync ?? true,
    defaultValue: channel.defaultValue ?? null
  };
}
