'use strict';
const fs = require('node:fs');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
const { validateRaw } = require('./domain.cjs');

const TYPES = new Set(['device.telemetry.raw', 'sensor.reading', 'zone.metrics.updated', 'actuator.command.created', 'actuator.command.applied', 'gateway.telemetry.rejected', 'cep.alert.created']);
const RAW = 'device.telemetry.raw';
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const identity = (value, max = 128) => typeof value === 'string' && value.length <= max && /^[A-Za-z0-9][A-Za-z0-9._:-]*$/.test(value);
const finite = (value, min = -Infinity, max = Infinity) => typeof value === 'number' && Number.isFinite(value) && value >= min && value <= max;
const integer = (value, min = 0) => Number.isSafeInteger(value) && value >= min;
const keyFor = event => JSON.stringify([event.pipeline, event.runId, event.id]);
const rollupKey = row => JSON.stringify([row.pipeline, row.runId, row.zoneId, row.minute]);
const clone = value => JSON.parse(JSON.stringify(value));
const timeProbe = { schemaVersion: 1, id: 'clock', correlationId: 'clock', runId: 'clock', type: RAW, payload: { deviceId: 'clock', zoneId: 'clock', temperatureC: 20, humidityPercent: 50, occupancyCount: 0, co2Ppm: 400, batteryPercent: 100 } };
const validTime = value => validateRaw({ ...timeProbe, timestamp: value }) === null;
const requireValue = (condition, message) => { if (!condition) throw new TypeError(message); };

function validateEvent(event, pipeline) {
  requireValue(pipeline === 'main' || pipeline === 'flow', 'Invalid pipeline');
  requireValue(object(event) && event.schemaVersion === 1 && TYPES.has(event.type), 'Invalid journal envelope type/version');
  requireValue(identity(event.id, event.type === RAW ? 128 : 160) && identity(event.runId) && identity(event.correlationId), 'Invalid journal envelope identity');
  requireValue(validTime(event.timestamp), 'Invalid journal envelope timestamp');
  requireValue(Object.hasOwn(event, 'payload'), 'Missing journal payload');
  if (event.type === RAW) return; // Preserve invalid raw sensor data as evidence of gateway rejection.
  requireValue(identity(event.sourceEventId, 160) && object(event.payload), 'Invalid derived envelope lineage/payload');
  const p = event.payload;
  if (event.type === 'sensor.reading') {
    const reason = validateRaw({ ...event, id: event.correlationId, type: RAW, payload: p });
    requireValue(!reason, `Invalid normalized reading: ${reason}`);
  }
  if (p.sourceTimestamp !== undefined) requireValue(validTime(p.sourceTimestamp), 'Invalid payload sourceTimestamp');
  if (event.type === 'zone.metrics.updated') {
    requireValue(identity(p.zoneId) && identity(p.aggregatorId) && integer(p.sampleCount, 1) && validTime(p.sourceTimestamp), 'Invalid metrics identity/count/source time');
    for (const [field, min, max] of [['averageTemperatureC', -20, 80], ['averageHumidityPercent', 0, 100], ['averageOccupancyCount', 0, Number.MAX_SAFE_INTEGER], ['averageCo2Ppm', 400, 5000]]) {
      requireValue(finite(p[field], min, max), `Invalid metrics ${field}`);
    }
    requireValue(Number.isInteger(p.averageCo2Ppm), 'Invalid metrics CO2 precision');
  }
  if (event.type === 'actuator.command.created' || event.type === 'actuator.command.applied') {
    requireValue(identity(p.zoneId) && ['cool', 'ventilate', 'idle'].includes(p.mode) && ['high', 'medium', 'low'].includes(p.fanSpeed), 'Invalid actuator command');
    if (event.type === 'actuator.command.created') {
      requireValue(typeof p.reason === 'string' && p.reason.length > 0 && finite(p.targetTemperatureC), 'Invalid command reason/target');
    } else {
      requireValue(identity(p.sourceCommandId, 160) && validTime(p.appliedAt) && validTime(p.sourceTimestamp), 'Invalid applied command lineage/time');
    }
  }
  if (event.type === 'gateway.telemetry.rejected') {
    requireValue(identity(p.deviceId) && typeof p.reason === 'string' && p.reason.length > 0, 'Invalid rejection payload');
  }
  if (event.type === 'cep.alert.created') {
    requireValue(['sustained_co2', 'silent_device'].includes(p.kind) && identity(p.deviceId) && identity(p.zoneId) && p.runId === event.runId && p.correlationId === event.correlationId && p.sourceEventId === event.sourceEventId, 'Invalid CEP alert identity/lineage');
    requireValue(validTime(p.sourceTimestamp) && validTime(p.detectedAt), 'Invalid CEP alert timestamp');
    if (p.kind === 'sustained_co2') requireValue(finite(p.co2Ppm, 900, 5000) && p.co2Ppm > 900 && integer(p.occupancyCount, 2) && finite(p.durationMs, 0) && p.thresholdCo2Ppm === 900, 'Invalid sustained CEP alert');
    else requireValue(finite(p.silentForMs, 0) && validTime(p.lastSeenAt), 'Invalid silence CEP alert');
  }
}

function validateRollup(row) {
  requireValue(object(row) && ['main', 'flow'].includes(row.pipeline) && identity(row.runId) && identity(row.zoneId) && typeof row.id === 'string' && validTime(row.minute) && Date.parse(row.minute) % 60000 === 0 && integer(row.count, 1), 'Invalid stored rollup');
  for (const field of ['temperatureSum', 'humiditySum', 'occupancySum', 'co2Sum']) requireValue(finite(row[field]), `Invalid rollup ${field}`);
}

class EventStore {
  constructor(directory, { retentionMs = 30 * 86400000 } = {}) {
    requireValue(typeof directory === 'string' && directory.length > 0 && finite(retentionMs, 0), 'Invalid store directory/retention');
    this.directory = directory;
    this.retentionMs = retentionMs;
    this.duplicates = 0;
    this.lastSequence = 0;
    this.events = new Map();
    this.rollups = new Map();
    this.seen = new Set();
    this.faulted = false;
    fs.mkdirSync(directory, { recursive: true });
    this.journal = path.join(directory, 'events.jsonl');
    this.statePath = path.join(directory, 'state.json');
    if (fs.existsSync(this.statePath)) this.loadState(JSON.parse(fs.readFileSync(this.statePath, 'utf8')));
    this.replayJournal();
  }

  loadState(state) {
    requireValue(object(state) && state.version === 1 && integer(state.lastSequence) && integer(state.duplicates) && Array.isArray(state.events) && Array.isArray(state.rollups) && Array.isArray(state.seenKeys), 'Invalid store snapshot');
    const events = new Map();
    const rollups = new Map();
    const seen = new Set();
    for (const key of state.seenKeys) {
      requireValue(typeof key === 'string', 'Invalid snapshot dedup key');
      let tuple;
      try { tuple = JSON.parse(key); } catch { throw new TypeError('Invalid snapshot dedup key'); }
      requireValue(Array.isArray(tuple) && tuple.length === 3 && ['main', 'flow'].includes(tuple[0]) && identity(tuple[1]) && identity(tuple[2], 160) && JSON.stringify(tuple) === key && !seen.has(key), 'Invalid/duplicate snapshot dedup key');
      seen.add(key);
    }
    for (const event of state.events) {
      validateEvent(event, event.pipeline);
      const key = keyFor(event);
      requireValue(!events.has(key) && seen.has(key), 'Invalid snapshot event identity');
      events.set(key, event);
    }
    for (const row of state.rollups) {
      validateRollup(row);
      const key = rollupKey(row);
      requireValue(!rollups.has(key), 'Duplicate snapshot rollup');
      rollups.set(key, row);
    }
    requireValue(seen.size <= state.lastSequence, 'Invalid snapshot sequence/dedup state');
    this.lastSequence = state.lastSequence;
    this.duplicates = state.duplicates;
    this.events = events;
    this.rollups = rollups;
    this.seen = seen;
  }

  replayJournal() {
    if (!fs.existsSync(this.journal)) return;
    const bytes = fs.readFileSync(this.journal);
    let start = 0;
    let previousSequence = null;
    while (start < bytes.length) {
      const newline = bytes.indexOf(10, start);
      const end = newline === -1 ? bytes.length : newline;
      const line = bytes.subarray(start, end).toString('utf8');
      let record;
      try { record = JSON.parse(line); }
      catch (error) {
        // Only an unterminated malformed final record is a recoverable torn append.
        // A complete malformed line or any interior corruption is never discarded.
        const position = /at position (\d+)/.exec(error.message);
        const truncated = line.trimStart().startsWith('{') && (/Unexpected end of JSON input/.test(error.message) || (position && Number(position[1]) >= line.length));
        if (newline !== -1 || !truncated) throw new Error(`Corrupt journal record at byte ${start}: ${error.message}`);
        const quarantine = `events.jsonl.truncated-${Date.now()}-${randomUUID()}.bin`;
        this.writeAtomic(quarantine, bytes.subarray(start));
        this.writeAtomic('events.jsonl', bytes.subarray(0, start));
        return;
      }
      requireValue(object(record) && integer(record.sequence, 1) && object(record.event), `Invalid journal record at byte ${start}; legacy flat records require explicit migration`);
      validateEvent(record.event, record.event.pipeline);
      if (previousSequence !== null) requireValue(record.sequence === previousSequence + 1, 'Non-contiguous journal sequence');
      previousSequence = record.sequence;
      if (record.sequence > this.lastSequence) {
        requireValue(record.sequence === this.lastSequence + 1, 'Journal sequence gap');
        const key = keyFor(record.event);
        requireValue(!this.seen.has(key), 'Duplicate committed journal event');
        this.apply(record.event);
        this.lastSequence = record.sequence;
      }
      if (newline === -1) {
        // A full JSON record without its final newline is valid; complete the frame before appending.
        this.writeAtomic('events.jsonl', Buffer.concat([bytes, Buffer.from('\n')]));
        return;
      }
      start = newline + 1;
    }
  }

  rollupAfter(event) {
    if (event.type !== 'sensor.reading') return null;
    const p = event.payload;
    const minute = new Date(Math.floor(Date.parse(p.sourceTimestamp || event.timestamp) / 60000) * 60000).toISOString();
    const key = JSON.stringify([event.pipeline, event.runId, p.zoneId, minute]);
    const existing = this.rollups.get(key);
    const row = existing ? { ...existing } : { id: key, pipeline: event.pipeline, runId: event.runId, zoneId: p.zoneId, minute, count: 0, temperatureSum: 0, humiditySum: 0, occupancySum: 0, co2Sum: 0 };
    row.count++;
    row.temperatureSum += p.temperatureC;
    row.humiditySum += p.humidityPercent;
    row.occupancySum += p.occupancyCount;
    row.co2Sum += p.co2Ppm;
    validateRollup(row);
    return { key, row };
  }

  apply(event, preparedRollup = this.rollupAfter(event)) {
    const key = keyFor(event);
    this.events.set(key, event);
    this.seen.add(key);
    if (preparedRollup) this.rollups.set(preparedRollup.key, preparedRollup.row);
  }

  ingest(event, pipeline = 'main') {
    if (this.faulted) throw new Error('Store journal write failed; restart before further ingestion');
    validateEvent(event, pipeline);
    // Detach caller objects and validate the actual JSON representation before writing it.
    const stored = clone({ ...event, pipeline });
    validateEvent(stored, pipeline);
    const key = keyFor(stored);
    if (this.seen.has(key)) { this.duplicates++; return false; }
    const preparedRollup = this.rollupAfter(stored);
    const sequence = this.lastSequence + 1;
    requireValue(integer(sequence, 1), 'Journal sequence exhausted');
    const line = JSON.stringify({ sequence, event: stored }) + '\n';
    const existed = fs.existsSync(this.journal);
    let fd;
    try {
      fd = fs.openSync(this.journal, 'a', 0o600);
      fs.writeFileSync(fd, line);
      fs.fsyncSync(fd);
      if (!existed) this.syncDirectory();
    } catch (error) {
      this.faulted = true;
      throw error;
    } finally { if (fd !== undefined) fs.closeSync(fd); }
    this.apply(stored, preparedRollup);
    this.lastSequence = sequence;
    return true;
  }

  *retainedEvents(cutoff) {
    for (const event of this.events.values()) if (Date.parse(event.timestamp) >= cutoff) yield event;
  }

  *eventsOfType(type) {
    for (const event of this.events.values()) if (event.type === type) yield event;
  }

  *jsonArray(values) {
    let first = true;
    yield '[';
    for (const value of values) {
      if (!first) yield ',';
      yield JSON.stringify(value);
      first = false;
    }
    yield ']';
  }

  *stateChunks(cutoff) {
    yield `{"version":1,"lastSequence":${this.lastSequence},"duplicates":${this.duplicates},"events":`;
    yield* this.jsonArray(this.retainedEvents(cutoff));
    yield ',"rollups":';
    yield* this.jsonArray(this.rollups.values());
    yield ',"seenKeys":';
    yield* this.jsonArray(this.seen.values());
    yield '}\n';
  }

  maintenance(now = Date.now()) {
    if (this.faulted) throw new Error('Store journal write failed; restart before further maintenance');
    requireValue(finite(now) && Number.isFinite(new Date(now).valueOf()), 'Invalid maintenance clock');
    // Stream the unchanged store into its next committed snapshot. Neither retained
    // documents nor their serialized JSON are collected in whole-store arrays/strings.
    const cutoff = Math.floor((now - this.retentionMs) / 60000) * 60000;
    this.writeAtomic('state.json', this.stateChunks(cutoff));
    // Delete only after commit: a failed snapshot still leaves in-memory state intact.
    let removed = 0;
    for (const [key, event] of this.events) {
      if (Date.parse(event.timestamp) < cutoff) { this.events.delete(key); removed++; }
    }
    // A crash before clearing the WAL is safe: replay skips committed sequences.
    this.writeAtomic('events.jsonl', '');
    this.atomic('rollups.jsonl', this.rollups.values());
    for (const [name, type] of Object.entries({ telemetry: RAW, 'zone-metrics': 'zone.metrics.updated', commands: 'actuator.command.created', alerts: 'cep.alert.created' })) {
      this.atomic(name + '.jsonl', this.eventsOfType(type));
    }
    return { removed, retained: this.events.size, rollups: this.rollups.size };
  }

  syncDirectory() {
    let fd;
    try { fd = fs.openSync(this.directory, 'r'); fs.fsyncSync(fd); }
    catch (error) { if (!['EINVAL', 'ENOTSUP', 'ENOSYS', 'EBADF'].includes(error.code)) throw error; }
    finally { if (fd !== undefined) fs.closeSync(fd); }
  }

  writeAtomic(name, content) {
    const destination = path.join(this.directory, name);
    const temporary = destination + '.tmp';
    const chunks = typeof content === 'string' || Buffer.isBuffer(content) ? [content] : content;
    requireValue(chunks && typeof chunks[Symbol.iterator] === 'function', 'Atomic content must be bytes or an iterable of chunks');
    // A single reusable buffer bounds write aggregation and avoids a syscall for
    // every comma/key. One unusually large record is still serialized independently.
    const buffer = Buffer.allocUnsafe(64 * 1024);
    let used = 0;
    let fd;
    const flush = () => {
      let offset = 0;
      while (offset < used) {
        const written = fs.writeSync(fd, buffer, offset, used - offset);
        if (written === 0) throw new Error('Atomic write made no progress');
        offset += written;
      }
      used = 0;
    };
    try {
      fd = fs.openSync(temporary, 'w', 0o600);
      for (const chunk of chunks) {
        requireValue(typeof chunk === 'string' || Buffer.isBuffer(chunk), 'Invalid atomic content chunk');
        const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk, 'utf8');
        let offset = 0;
        while (offset < bytes.length) {
          const count = Math.min(buffer.length - used, bytes.length - offset);
          bytes.copy(buffer, used, offset, offset + count);
          used += count;
          offset += count;
          if (used === buffer.length) flush();
        }
      }
      flush();
      fs.fsyncSync(fd);
    } finally { if (fd !== undefined) fs.closeSync(fd); }
    fs.renameSync(temporary, destination);
    this.syncDirectory();
  }

  *jsonLines(events) {
    for (const event of events) yield JSON.stringify(event) + '\n';
  }

  atomic(name, events) {
    this.writeAtomic(name, this.jsonLines(events));
  }

  snapshot(runId, pipeline) {
    const observedMs = Date.now();
    const throughputCutoff = observedMs - 10000;
    let recentRaw = 0, recentApplied = 0;
    const events = [...this.events.values()].filter(event => (!runId || event.runId === runId) && (!pipeline || event.pipeline === pipeline));
    const counts = Object.create(null), zones = Object.create(null), aggregators = Object.create(null);
    for (const event of events) {
      counts[event.type] = (counts[event.type] || 0) + 1;
      const emittedMs = Date.parse(event.timestamp);
      if (emittedMs > throughputCutoff && emittedMs <= observedMs) {
        if (event.type === RAW) recentRaw++;
        if (event.type === 'actuator.command.applied') recentApplied++;
      }
      if (event.type === 'zone.metrics.updated') {
        zones[event.payload.zoneId] = { ...(zones[event.payload.zoneId] || {}), metrics: event.payload };
        aggregators[event.payload.aggregatorId] = (aggregators[event.payload.aggregatorId] || 0) + 1;
      }
      if (event.type === 'actuator.command.applied') zones[event.payload.zoneId] = { ...(zones[event.payload.zoneId] || {}), actuator: event.payload };
    }
    const raw = new Map(events.filter(event => event.type === RAW).map(event => [keyFor(event), event]));
    const latencies = events.filter(event => event.type === 'actuator.command.applied').flatMap(event => {
      const source = raw.get(JSON.stringify([event.pipeline, event.runId, event.correlationId]));
      return source ? [Date.parse(event.payload.appliedAt) - Date.parse(source.timestamp)] : [];
    }).sort((a, b) => a - b);
    const percentile = p => latencies.length ? latencies[Math.max(0, Math.ceil(p * latencies.length) - 1)] : null;
    // Snapshots are detached so callers cannot mutate durable data through a returned payload.
    return clone({ observedAt: new Date(observedMs).toISOString(), runId: runId || null, pipeline: pipeline || null, totalEvents: events.length, counts, zones, aggregators, duplicateDeliveries: this.duplicates,
      rollingThroughput: { windowMs: 10000, rawPerSecond: recentRaw / 10, appliedPerSecond: recentApplied / 10 },
      latencyMs: { count: latencies.length, min: latencies[0] ?? null, p50: percentile(.5), p95: percentile(.95), p99: percentile(.99), max: latencies.at(-1) ?? null },
      alerts: events.filter(event => event.type === 'cep.alert.created').map(event => event.payload),
      rollups: [...this.rollups.values()].filter(row => (!runId || row.runId === runId) && (!pipeline || row.pipeline === pipeline)).map(row => ({ ...row, averageTemperatureC: row.temperatureSum / row.count, averageCo2Ppm: row.co2Sum / row.count })),
      recent: events.slice(-8), runs: [...new Set([...this.events.values()].map(event => event.runId))].slice(-30),
      retention: { days: this.retentionMs / 86400000, rawEventsExpire: true, rollupsExpire: false } });
  }
}
module.exports = { EventStore };
