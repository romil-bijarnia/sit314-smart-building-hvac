'use strict';

const SENSOR_FIELDS = ['temperatureC', 'humidityPercent', 'occupancyCount', 'co2Ppm'];
const LIMITS = {
  temperatureC: [-20, 80],
  humidityPercent: [0, 100],
  co2Ppm: [400, 5000],
  batteryPercent: [0, 100],
};
const isObject = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const isIdentity = (value, maxLength = 128) => typeof value === 'string' && value.length <= maxLength && /^[A-Za-z0-9][A-Za-z0-9._:-]*$/.test(value);
const keyOf = (...parts) => JSON.stringify(parts);

// Date.parse alone normalizes impossible dates such as February 30.
function timestampMs(value) {
  if (typeof value !== 'string') return NaN;
  const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(Z|[+-]\d{2}:\d{2})$/.exec(value);
  if (!match) return NaN;
  const [, yearText, monthText, dayText, hourText, minuteText, secondText, offset] = match;
  const [year, month, day, hour, minute, second] = [yearText, monthText, dayText, hourText, minuteText, secondText].map(Number);
  const leap = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const days = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  if (month < 1 || month > 12 || day < 1 || day > days[month - 1] || hour > 23 || minute > 59 || second > 59) return NaN;
  if (offset !== 'Z' && (Number(offset.slice(1, 3)) > 23 || Number(offset.slice(4, 6)) > 59)) return NaN;
  return Date.parse(value);
}

function telemetryReason(payload) {
  if (!isObject(payload)) return 'payload must be an object';
  for (const field of ['deviceId', 'zoneId']) {
    if (!isIdentity(payload[field])) return `payload.${field} must be a non-empty MQTT-safe identity`;
  }
  for (const [field, [min, max]] of Object.entries(LIMITS)) {
    if (typeof payload[field] !== 'number' || !Number.isFinite(payload[field]) || payload[field] < min || payload[field] > max) {
      return `payload.${field} must be a finite number in [${min}, ${max}]`;
    }
  }
  if (!Number.isSafeInteger(payload.occupancyCount) || payload.occupancyCount < 0) {
    return 'payload.occupancyCount must be a non-negative safe integer';
  }
  if (payload.sequence !== undefined && (!Number.isSafeInteger(payload.sequence) || payload.sequence < 0)) {
    return 'payload.sequence must be a non-negative safe integer when supplied';
  }
  return null;
}

function envelopeReason(event) {
  if (!isObject(event)) return 'event must be an object';
  if (event.schemaVersion !== 1) return 'schemaVersion must be 1';
  for (const field of ['id', 'runId', 'correlationId']) {
    if (!isIdentity(event[field], field === 'id' ? 160 : 128)) return `${field} must be a non-empty MQTT-safe identity`;
  }
  if (!Number.isFinite(timestampMs(event.timestamp))) return 'timestamp must be a valid ISO 8601 timestamp with timezone';
  return null;
}

function validateRaw(event) {
  const reason = envelopeReason(event);
  if (reason) return reason;
  if (event.type !== 'device.telemetry.raw') return 'type must be device.telemetry.raw';
  if (event.id.length > 128) return 'raw id must be at most 128 characters';
  if (event.correlationId !== event.id) return 'raw correlationId must equal id';
  return telemetryReason(event.payload);
}

function deriveEvent(parent, type, suffix, payload, nowMs = Date.now()) {
  if (!isObject(parent) || !isIdentity(parent.id, 160) || !isIdentity(parent.runId)) throw new TypeError('parent needs id and runId');
  const rootId = parent.correlationId === undefined ? parent.id : parent.correlationId;
  if (!isIdentity(rootId)) throw new TypeError('parent correlationId must be a valid identity');
  if (typeof type !== 'string' || !/^[a-z][a-z0-9]*(?:\.[a-z][a-z0-9]*)+$/.test(type)) throw new TypeError('type must be a dotted event name');
  const normalizedSuffix = typeof suffix === 'string' ? suffix.replace(/^:/, '') : '';
  if (normalizedSuffix.length > 31 || !/^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(normalizedSuffix)) throw new TypeError('suffix must be a simple identity of 1 to 31 characters');
  if (!isObject(payload)) throw new TypeError('payload must be an object');
  if (!Number.isFinite(nowMs) || !Number.isFinite(new Date(nowMs).valueOf())) throw new TypeError('nowMs must be a finite timestamp');
  return {
    schemaVersion: 1,
    id: `${rootId}:${normalizedSuffix}`,
    type,
    timestamp: new Date(nowMs).toISOString(),
    runId: parent.runId,
    correlationId: rootId,
    sourceEventId: parent.id,
    payload,
  };
}

function readingReason(event) {
  const reason = envelopeReason(event);
  if (reason) return reason;
  const payloadReason = telemetryReason(event.payload);
  if (payloadReason) return payloadReason;
  if (event.payload.sourceTimestamp !== undefined && !Number.isFinite(timestampMs(event.payload.sourceTimestamp))) {
    return 'payload.sourceTimestamp must be a valid ISO 8601 timestamp with timezone';
  }
  return null;
}

const rounded = (value, places = 2) => {
  const result = Number(value.toFixed(places));
  return Object.is(result, -0) ? 0 : result;
};

class ZoneAggregator {
  constructor(windowSize = 10) {
    if (!Number.isSafeInteger(windowSize) || windowSize < 1) throw new RangeError('windowSize must be a positive safe integer');
    this.windowSize = windowSize;
    this.windows = new Map();
    this.seen = new Set();
  }

  update(readingEvent, instanceId = 'aggregator') {
    const reason = readingReason(readingEvent);
    if (reason) throw new TypeError(reason);
    if (!isIdentity(instanceId)) throw new TypeError('instanceId must be a valid identity');
    const eventKey = keyOf(readingEvent.runId, readingEvent.id);
    if (this.seen.has(eventKey)) return null;
    const payload = readingEvent.payload;
    const windowKey = keyOf(readingEvent.runId, payload.zoneId);
    let window = this.windows.get(windowKey);
    if (!window) {
      window = { runId: readingEvent.runId, zoneId: payload.zoneId, nextIndex: 0, samples: [], sums: Object.fromEntries(SENSOR_FIELDS.map(field => [field, 0])) };
      this.windows.set(windowKey, window);
    }
    const nextSample = Object.fromEntries(SENSOR_FIELDS.map(field => [field, payload[field]]));
    const replaced = window.samples[window.nextIndex];
    for (const field of SENSOR_FIELDS) window.sums[field] += nextSample[field] - (replaced ? replaced[field] : 0);
    window.samples[window.nextIndex] = nextSample;
    window.nextIndex = (window.nextIndex + 1) % this.windowSize;
    this.seen.add(eventKey);
    const sampleCount = window.samples.length;
    return {
      zoneId: payload.zoneId,
      sampleCount,
      averageTemperatureC: rounded(window.sums.temperatureC / sampleCount),
      averageHumidityPercent: rounded(window.sums.humidityPercent / sampleCount),
      averageOccupancyCount: rounded(window.sums.occupancyCount / sampleCount),
      averageCo2Ppm: rounded(window.sums.co2Ppm / sampleCount, 0),
      aggregatorId: instanceId,
      sourceTimestamp: payload.sourceTimestamp || readingEvent.timestamp,
    };
  }

  serialize() {
    return {
      version: 1,
      windowSize: this.windowSize,
      windows: [...this.windows.values()].map(window => ({
        runId: window.runId,
        zoneId: window.zoneId,
        nextIndex: window.nextIndex,
        samples: window.samples.map(sample => ({ ...sample })),
      })),
      seenEventIds: [...this.seen].map(key => JSON.parse(key)),
    };
  }

  restore(snapshot) {
    if (!isObject(snapshot) || snapshot.version !== 1 || !Number.isSafeInteger(snapshot.windowSize) || snapshot.windowSize < 1 || !Array.isArray(snapshot.windows) || !Array.isArray(snapshot.seenEventIds)) {
      throw new TypeError('invalid aggregator checkpoint');
    }
    const windows = new Map();
    const seen = new Set();
    for (const entry of snapshot.windows) {
      if (!isObject(entry) || !isIdentity(entry.runId) || !isIdentity(entry.zoneId) || !Array.isArray(entry.samples) || entry.samples.length < 1 || entry.samples.length > snapshot.windowSize || !Number.isSafeInteger(entry.nextIndex) || entry.nextIndex < 0 || entry.nextIndex >= snapshot.windowSize || (entry.samples.length < snapshot.windowSize && entry.nextIndex !== entry.samples.length)) {
        throw new TypeError('invalid checkpoint window');
      }
      const windowKey = keyOf(entry.runId, entry.zoneId);
      if (windows.has(windowKey)) throw new TypeError('duplicate checkpoint window');
      const samples = [];
      const sums = Object.fromEntries(SENSOR_FIELDS.map(field => [field, 0]));
      for (const sample of entry.samples) {
        if (!isObject(sample)) throw new TypeError('invalid checkpoint sample');
        for (const field of SENSOR_FIELDS) {
          const value = sample[field];
          const valid = field === 'occupancyCount' ? Number.isSafeInteger(value) && value >= 0 : typeof value === 'number' && Number.isFinite(value) && value >= LIMITS[field][0] && value <= LIMITS[field][1];
          if (!valid) throw new TypeError(`invalid checkpoint sample ${field}`);
          sums[field] += value;
        }
        samples.push(Object.fromEntries(SENSOR_FIELDS.map(field => [field, sample[field]])));
      }
      windows.set(windowKey, { runId: entry.runId, zoneId: entry.zoneId, nextIndex: entry.nextIndex, samples, sums });
    }
    for (const pair of snapshot.seenEventIds) {
      if (!Array.isArray(pair) || pair.length !== 2 || !isIdentity(pair[0]) || !isIdentity(pair[1], 160)) throw new TypeError('invalid checkpoint event identity');
      const key = keyOf(...pair);
      if (seen.has(key)) throw new TypeError('duplicate checkpoint event identity');
      seen.add(key);
    }
    // Swap only after the complete checkpoint has passed validation.
    this.windowSize = snapshot.windowSize;
    this.windows = windows;
    this.seen = seen;
    return this;
  }
}

function decideCommand(metrics, targetTemperature = 22) {
  if (!isObject(metrics) || !isIdentity(metrics.zoneId)) throw new TypeError('metrics needs zoneId');
  for (const field of ['averageTemperatureC', 'averageOccupancyCount', 'averageCo2Ppm']) {
    if (!Number.isFinite(metrics[field])) throw new TypeError(`metrics.${field} must be finite`);
  }
  if (!Number.isFinite(targetTemperature)) throw new TypeError('targetTemperature must be finite');
  const occupied = metrics.averageOccupancyCount >= 2;
  const highCo2 = metrics.averageCo2Ppm > 900;
  let mode = 'idle';
  let fanSpeed = 'low';
  let reason = occupied ? 'Temperature and CO2 are within control thresholds' : 'Average occupancy is below 2';
  if (metrics.averageTemperatureC > targetTemperature + 1.5 && occupied) {
    mode = 'cool';
    fanSpeed = highCo2 ? 'high' : 'medium';
    reason = highCo2 ? 'Occupied zone exceeds temperature and CO2 thresholds' : 'Occupied zone exceeds temperature threshold';
  } else if (highCo2 && occupied) {
    mode = 'ventilate';
    fanSpeed = 'high';
    reason = 'Occupied zone exceeds CO2 threshold';
  }
  return { zoneId: metrics.zoneId, mode, fanSpeed, reason, targetTemperatureC: targetTemperature };
}

function ownsZone(zoneId, index, count) {
  if (!Number.isSafeInteger(count) || count < 1 || !Number.isSafeInteger(index) || index < 0 || index >= count) return false;
  const match = typeof zoneId === 'string' && /^zone-([1-9]\d*)$/.exec(zoneId);
  if (!match) return false;
  const number = Number(match[1]);
  return Number.isSafeInteger(number) && (number - 1) % count === index;
}

class CepEngine {
  constructor({ sustainedMs = 10000, silentMs = 5000 } = {}) {
    if (!Number.isFinite(sustainedMs) || sustainedMs < 0 || !Number.isFinite(silentMs) || silentMs <= 0) throw new RangeError('sustainedMs must be non-negative and silentMs must be positive');
    this.sustainedMs = sustainedMs;
    this.silentMs = silentMs;
    this.devices = new Map();
    this.seen = new Set();
  }

  reading(event, nowMs = Date.now()) {
    if (readingReason(event) || !Number.isFinite(nowMs) || !Number.isFinite(new Date(nowMs).valueOf())) return [];
    const eventKey = keyOf(event.runId, event.id);
    if (this.seen.has(eventKey)) return [];
    const payload = event.payload;
    const sourceTimestamp = payload.sourceTimestamp || event.timestamp;
    const sourceMs = timestampMs(sourceTimestamp);
    const deviceKey = keyOf(event.runId, payload.deviceId);
    let state = this.devices.get(deviceKey);
    if (state && (sourceMs <= state.sourceMs || nowMs < state.lastSeenMs)) return [];
    this.seen.add(eventKey);
    if (!state) {
      state = { highSinceMs: null, highAlerted: false, silentAlerted: false };
      this.devices.set(deviceKey, state);
    } else if (sourceMs - state.sourceMs >= this.silentMs || nowMs - state.lastSeenMs >= this.silentMs || state.zoneId !== payload.zoneId) {
      state.highSinceMs = null;
      state.highAlerted = false;
    }
    Object.assign(state, {
      runId: event.runId,
      deviceId: payload.deviceId,
      zoneId: payload.zoneId,
      sourceEventId: event.id,
      correlationId: event.correlationId,
      sourceTimestamp,
      sourceMs,
      lastSeenMs: nowMs,
      silentAlerted: false,
    });
    if (!(payload.co2Ppm > 900 && payload.occupancyCount >= 2)) {
      state.highSinceMs = null;
      state.highAlerted = false;
      return [];
    }
    if (state.highSinceMs === null) state.highSinceMs = sourceMs;
    const durationMs = sourceMs - state.highSinceMs;
    if (durationMs < this.sustainedMs || state.highAlerted) return [];
    state.highAlerted = true;
    return [{
      ...this.alertIdentity(state, 'sustained_co2', nowMs),
      co2Ppm: payload.co2Ppm,
      occupancyCount: payload.occupancyCount,
      durationMs,
      thresholdCo2Ppm: 900,
    }];
  }

  alertIdentity(state, kind, nowMs) {
    return {
      kind,
      runId: state.runId,
      deviceId: state.deviceId,
      zoneId: state.zoneId,
      sourceEventId: state.sourceEventId,
      correlationId: state.correlationId,
      sourceTimestamp: state.sourceTimestamp,
      detectedAt: new Date(nowMs).toISOString(),
    };
  }

  sweep(nowMs = Date.now()) {
    if (!Number.isFinite(nowMs) || !Number.isFinite(new Date(nowMs).valueOf())) return [];
    const alerts = [];
    for (const state of this.devices.values()) {
      const silentForMs = nowMs - state.lastSeenMs;
      if (silentForMs < this.silentMs || state.silentAlerted) continue;
      state.silentAlerted = true;
      state.highSinceMs = null;
      state.highAlerted = false;
      alerts.push({ ...this.alertIdentity(state, 'silent_device', nowMs), silentForMs, lastSeenAt: new Date(state.lastSeenMs).toISOString() });
    }
    return alerts;
  }
}

module.exports = { validateRaw, deriveEvent, ZoneAggregator, decideCommand, ownsZone, CepEngine };
