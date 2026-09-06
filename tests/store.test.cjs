'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { EventStore } = require('../src/store.cjs');
const { deriveEvent, ZoneAggregator, decideCommand, CepEngine } = require('../src/domain.cjs');
const BASE = Date.parse('2026-09-06T00:00:00Z');
const at = ms => new Date(BASE + ms).toISOString();
function directory(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'hvac-store-test-'));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  return root;
}
function raw(id = 'event-1', sourceMs = 10000, runId = 'run-1') {
  return { schemaVersion: 1, id, type: 'device.telemetry.raw', timestamp: at(sourceMs), runId, correlationId: id, payload: { deviceId: 'sensor-1', zoneId: 'zone-1', temperatureC: 24, humidityPercent: 50, occupancyCount: 2, co2Ppm: 1000, batteryPercent: 95 } };
}
function reading(id = 'event-1', sourceMs = 10000, emissionMs = sourceMs, runId = 'run-1') {
  const source = raw(id, sourceMs, runId);
  return deriveEvent(source, 'sensor.reading', 'reading', { ...source.payload, sourceTimestamp: source.timestamp }, BASE + emissionMs);
}
function pipeline(id = 'event-1', sourceMs = 10000, latencyMs = 30, runId = 'run-1') {
  const source = raw(id, sourceMs, runId);
  const normalized = reading(id, sourceMs, sourceMs + 5, runId);
  const metrics = deriveEvent(normalized, 'zone.metrics.updated', 'metrics', new ZoneAggregator().update(normalized), BASE + sourceMs + 10);
  const command = deriveEvent(metrics, 'actuator.command.created', 'command', { ...decideCommand(metrics.payload), sourceTimestamp: source.timestamp }, BASE + sourceMs + 15);
  const applied = deriveEvent(command, 'actuator.command.applied', 'applied', { ...command.payload, sourceCommandId: command.id, appliedAt: at(sourceMs + latencyMs), sourceTimestamp: source.timestamp }, BASE + sourceMs + latencyMs);
  return [source, normalized, metrics, command, applied];
}
const stable = snapshot => { const { observedAt, ...rest } = snapshot; return rest; };
const lines = file => fs.readFileSync(file, 'utf8').split('\n').filter(Boolean).map(JSON.parse);

test('store replays synced journal and preserves namespace/run deduplication', t => {
  const dir = directory(t);
  const store = new EventStore(dir);
  const first = reading();
  assert.equal(store.ingest(first, 'main'), true);
  assert.equal(store.ingest(first, 'main'), false);
  assert.equal(store.ingest(first, 'flow'), true);
  assert.equal(store.ingest(reading('event-1', 10000, 10000, 'other-run'), 'main'), true);
  assert.equal(store.events.size, 3);
  assert.deepEqual(lines(store.journal).map(row => row.sequence), [1, 2, 3]);
  const restarted = new EventStore(dir);
  assert.equal(restarted.snapshot().totalEvents, 3);
  assert.equal(restarted.snapshot('run-1', 'main').rollups[0].count, 1);
  assert.equal(restarted.snapshot('run-1', 'flow').rollups[0].count, 1);
  assert.equal(restarted.ingest(first, 'flow'), false);
  assert.equal(restarted.ingest(reading('next'), 'main'), true);
  assert.equal(lines(restarted.journal).at(-1).sequence, 4);
});

test('TTL retains complete rollups when a source minute straddles emission expiry', t => {
  const dir = directory(t);
  const store = new EventStore(dir, { retentionMs: 60000 });
  store.ingest(reading('first', 10000, 10000));
  store.ingest(reading('delayed', 20000, 70000));
  assert.equal(store.snapshot().rollups[0].count, 2);
  assert.deepEqual(store.maintenance(BASE + 120000), { removed: 1, retained: 1, rollups: 1 });
  assert.equal(fs.readFileSync(store.journal, 'utf8'), '');
  const restarted = new EventStore(dir, { retentionMs: 60000 });
  assert.equal(restarted.snapshot().rollups[0].count, 2);
  assert.equal(restarted.snapshot().totalEvents, 1);
  assert.equal(restarted.ingest(reading('first', 10000, 10000)), false, 'expired event tombstone prevents reaggregation');
  restarted.ingest(reading('even-later', 30000, 130000));
  assert.equal(restarted.snapshot().rollups[0].count, 3);
  restarted.maintenance(BASE + 240000);
  const again = new EventStore(dir, { retentionMs: 60000 });
  assert.equal(again.snapshot().totalEvents, 0);
  assert.equal(again.snapshot().rollups[0].count, 3);
  assert.equal(again.snapshot().rollups[0].averageTemperatureC, 24);
});

test('snapshot commits before journal clearing and replay ignores committed sequences', t => {
  const dir = directory(t);
  const store = new EventStore(dir, { retentionMs: 60000 });
  store.ingest(reading('first', 10000, 10000));
  store.ingest(reading('delayed', 20000, 70000));
  const original = store.writeAtomic.bind(store);
  store.writeAtomic = (name, contents) => {
    if (name === 'events.jsonl') throw new Error('simulated crash before WAL reset');
    return original(name, contents);
  };
  assert.throws(() => store.maintenance(BASE + 120000), /simulated crash/);
  assert.equal(lines(store.journal).length, 2);
  assert.equal(JSON.parse(fs.readFileSync(path.join(dir, 'state.json'))).lastSequence, 2);
  const restarted = new EventStore(dir, { retentionMs: 60000 });
  assert.equal(restarted.snapshot().totalEvents, 1);
  assert.equal(restarted.snapshot().rollups[0].count, 2);
  restarted.ingest(reading('new', 30000, 130000));
  const second = new EventStore(dir, { retentionMs: 60000 });
  assert.equal(second.snapshot().totalEvents, 2);
  assert.equal(second.snapshot().rollups[0].count, 3);
  second.maintenance(BASE + 150000);
  assert.equal(new EventStore(dir).snapshot().rollups[0].count, 3);
});

test('failed snapshot write leaves memory and journal untouched', t => {
  const dir = directory(t);
  const store = new EventStore(dir, { retentionMs: 60000 });
  store.ingest(reading());
  const originalState = stable(store.snapshot());
  const originalJournal = fs.readFileSync(store.journal, 'utf8');
  store.writeAtomic = () => { throw new Error('simulated snapshot failure'); };
  assert.throws(() => store.maintenance(BASE + 180000), /simulated snapshot failure/);
  assert.deepEqual(stable(store.snapshot()), originalState);
  assert.equal(fs.readFileSync(store.journal, 'utf8'), originalJournal);
  assert.equal(new EventStore(dir).snapshot().rollups[0].count, 1);
});

test('compacted journal resumes sequence and handles repeated maintenance/restarts', t => {
  const dir = directory(t);
  let store = new EventStore(dir);
  for (let round = 0; round < 4; round++) {
    for (let index = 0; index < 3; index++) store.ingest(reading(`event-${round}-${index}`, 10000 + index));
    store.maintenance(BASE + 20000);
    const before = stable(store.snapshot());
    store = new EventStore(dir);
    assert.deepEqual(stable(store.snapshot()), before);
    assert.equal(store.lastSequence, (round + 1) * 3);
  }
  assert.equal(store.snapshot().rollups[0].count, 12);
});

test('malformed normalized readings are rejected before journal append and cannot poison restart', t => {
  const dir = directory(t);
  const store = new EventStore(dir);
  store.ingest(reading('good'));
  const original = fs.readFileSync(store.journal, 'utf8');
  for (const [field, value] of [['sourceTimestamp', 'bad timestamp'], ['temperatureC', NaN], ['humidityPercent', 101], ['co2Ppm', 399], ['batteryPercent', -1], ['occupancyCount', 1.5]]) {
    const event = reading('bad-' + field);
    event.payload[field] = value;
    assert.throws(() => store.ingest(event), TypeError);
    assert.equal(fs.readFileSync(store.journal, 'utf8'), original);
  }
  assert.equal(new EventStore(dir).snapshot().rollups[0].count, 1);
});

test('raw invalid sensor payloads remain journalled for rejection evidence', t => {
  const dir = directory(t);
  const store = new EventStore(dir);
  const malformed = raw('bad-temperature');
  malformed.payload.temperatureC = 999;
  const noFields = raw('missing-fields');
  noFields.payload = {};
  const nullPayload = raw('null-payload');
  nullPayload.payload = null;
  for (const event of [malformed, noFields, nullPayload]) assert.equal(store.ingest(event), true);
  const restarted = new EventStore(dir);
  assert.equal(restarted.snapshot().counts['device.telemetry.raw'], 3);
  assert.deepEqual(restarted.snapshot().rollups, []);
  assert.equal(restarted.snapshot().recent[0].payload.temperatureC, 999);
});

test('envelope and downstream stage validation happens before durable append', t => {
  const store = new EventStore(directory(t));
  const [source, normalized, metrics, command, applied] = pipeline();
  const cases = [
    null, {}, { ...source, schemaVersion: 2 }, { ...source, timestamp: '2026-02-30T00:00:00Z' },
    { ...source, runId: '' }, { ...source, correlationId: null }, { ...source, type: 'unknown.event' },
    { ...normalized, sourceEventId: undefined },
    { ...metrics, payload: { ...metrics.payload, sampleCount: 0 } },
    { ...metrics, payload: { ...metrics.payload, averageCo2Ppm: NaN } },
    { ...metrics, payload: { ...metrics.payload, averageTemperatureC: 81 } },
    { ...metrics, payload: { ...metrics.payload, averageHumidityPercent: -1 } },
    { ...command, payload: { ...command.payload, mode: 'heat' } },
    { ...command, payload: { ...command.payload, fanSpeed: 'turbo' } },
    { ...command, payload: { ...command.payload, targetTemperatureC: Infinity } },
    { ...applied, payload: { ...applied.payload, appliedAt: 'invalid' } },
  ];
  for (const event of cases) assert.throws(() => store.ingest(event), TypeError);
  assert.throws(() => store.ingest(source, 'bad-pipeline'), TypeError);
  assert.equal(fs.existsSync(store.journal), false);
  assert.equal(store.lastSequence, 0);
  for (const event of [source, normalized, metrics, command, applied]) assert.equal(store.ingest(event), true);
  assert.equal(store.snapshot().totalEvents, 5);
});

test('truncated final journal append is quarantined without dropping prior records', t => {
  const dir = directory(t);
  const store = new EventStore(dir);
  store.ingest(reading('good'));
  const prefix = fs.readFileSync(store.journal);
  const torn = '{"sequence":2,"event":{"schemaVersion":1,"id":"torn';
  fs.appendFileSync(store.journal, torn);
  const restarted = new EventStore(dir);
  assert.equal(restarted.snapshot().totalEvents, 1);
  assert.equal(restarted.snapshot().rollups[0].count, 1);
  assert.deepEqual(fs.readFileSync(store.journal), prefix);
  const quarantines = fs.readdirSync(dir).filter(name => name.includes('.truncated-'));
  assert.equal(quarantines.length, 1);
  assert.equal(fs.readFileSync(path.join(dir, quarantines[0]), 'utf8'), torn);
  restarted.ingest(reading('next'));
  assert.equal(new EventStore(dir).snapshot().rollups[0].count, 2);
});

test('interior corruption, complete bad records, arbitrary tail garbage and semantic errors fail closed', t => {
  for (const mode of ['interior', 'complete-tail', 'garbage-tail', 'semantic-tail']) {
    const dir = path.join(directory(t), mode);
    const store = new EventStore(dir);
    store.ingest(reading('good'));
    if (mode === 'interior') fs.appendFileSync(store.journal, 'not-json\n' + JSON.stringify({ sequence: 2, event: { ...reading('next'), pipeline: 'main' } }) + '\n');
    if (mode === 'complete-tail') fs.appendFileSync(store.journal, '{"sequence":2,\n');
    if (mode === 'garbage-tail') fs.appendFileSync(store.journal, 'not-json');
    if (mode === 'semantic-tail') fs.appendFileSync(store.journal, JSON.stringify({ sequence: 2, event: {} }));
    const before = fs.readFileSync(store.journal);
    assert.throws(() => new EventStore(dir), /Corrupt journal|Invalid/);
    assert.deepEqual(fs.readFileSync(store.journal), before);
    assert.equal(fs.readdirSync(dir).some(name => name.includes('.truncated-')), false);
  }
});

test('valid final record without newline is framed before subsequent appends', t => {
  const dir = directory(t);
  const store = new EventStore(dir);
  store.ingest(reading('first'));
  fs.writeFileSync(store.journal, fs.readFileSync(store.journal, 'utf8').trimEnd());
  const restarted = new EventStore(dir);
  assert.equal(restarted.snapshot().totalEvents, 1);
  assert.ok(fs.readFileSync(store.journal, 'utf8').endsWith('\n'));
  restarted.ingest(reading('second'));
  assert.equal(new EventStore(dir).snapshot().totalEvents, 2);
});

test('journal sequence gaps, duplicates and corrupt snapshots fail closed', t => {
  for (const sequence of [1, 3]) {
    const dir = directory(t);
    const store = new EventStore(dir);
    store.ingest(reading('first'));
    fs.appendFileSync(store.journal, JSON.stringify({ sequence, event: { ...reading('second'), pipeline: 'main' } }) + '\n');
    assert.throws(() => new EventStore(dir), /sequence/);
  }
  const dir = directory(t);
  const store = new EventStore(dir);
  store.ingest(reading());
  store.maintenance(BASE + 20000);
  const state = JSON.parse(fs.readFileSync(path.join(dir, 'state.json')));
  state.seenKeys = [];
  fs.writeFileSync(path.join(dir, 'state.json'), JSON.stringify(state));
  assert.throws(() => new EventStore(dir), /snapshot event identity/);
});

test('snapshot counts, percentiles, zones, aggregators and namespace latency correlation stay compatible', t => {
  const dir = directory(t);
  const store = new EventStore(dir);
  for (const [id, sourceMs, latencyMs, namespace, run] of [
    ['same', 10000, 30, 'main', 'run-1'], ['same', 20000, 70, 'flow', 'run-1'], ['same', 30000, 50, 'main', 'run-2'],
  ]) for (const event of pipeline(id, sourceMs, latencyMs, run)) store.ingest(event, namespace);
  const all = store.snapshot();
  assert.equal(all.totalEvents, 15);
  for (const type of ['device.telemetry.raw', 'sensor.reading', 'zone.metrics.updated', 'actuator.command.created', 'actuator.command.applied']) assert.equal(all.counts[type], 3);
  assert.deepEqual(all.latencyMs, { count: 3, min: 30, p50: 50, p95: 70, p99: 70, max: 70 });
  assert.deepEqual(store.snapshot('run-1', 'flow').latencyMs, { count: 1, min: 70, p50: 70, p95: 70, p99: 70, max: 70 });
  assert.equal(all.zones['zone-1'].metrics.averageTemperatureC, 24);
  assert.equal(all.zones['zone-1'].actuator.mode, 'cool');
  assert.equal(all.aggregators.aggregator, 3);
  assert.equal(all.recent.length, 8);
  assert.deepEqual(all.runs, ['run-1', 'run-2']);
  assert.deepEqual(all.retention, { days: 30, rawEventsExpire: true, rollupsExpire: false });
  store.maintenance(BASE + 60000);
  assert.deepEqual(stable(new EventStore(dir).snapshot()), stable(store.snapshot()));
  assert.equal(lines(path.join(dir, 'telemetry.jsonl')).length, 3);
  assert.equal(lines(path.join(dir, 'zone-metrics.jsonl')).length, 3);
  assert.equal(lines(path.join(dir, 'commands.jsonl')).length, 3);
  assert.equal(lines(path.join(dir, 'rollups.jsonl')).length, 3);
});

test('CEP alerts and gateway rejections keep their published payloads', t => {
  const store = new EventStore(directory(t));
  const normalized = reading();
  const alert = new CepEngine({ sustainedMs: 0 }).reading(normalized, BASE + 10000)[0];
  const envelope = deriveEvent(normalized, 'cep.alert.created', 'alert', alert, BASE + 10000);
  assert.equal(store.ingest(envelope), true);
  const rejected = deriveEvent(raw('invalid'), 'gateway.telemetry.rejected', 'rejected', { deviceId: 'sensor-1', zoneId: '/invalid-zone-evidence', reason: 'invalid sensor range' }, BASE + 10000);
  assert.equal(store.ingest(rejected), true);
  assert.deepEqual(store.snapshot().alerts, [alert]);
  assert.equal(store.snapshot().counts['gateway.telemetry.rejected'], 1);
});

test('input objects and returned snapshots cannot mutate store state', t => {
  const store = new EventStore(directory(t));
  const event = reading();
  store.ingest(event);
  event.payload.temperatureC = 80;
  const snapshot = store.snapshot();
  snapshot.recent[0].payload.temperatureC = -20;
  snapshot.rollups[0].temperatureSum = -999;
  assert.equal(store.snapshot().recent[0].payload.temperatureC, 24);
  assert.equal(store.snapshot().rollups[0].temperatureSum, 24);
  assert.equal(new EventStore(store.directory).snapshot().recent[0].payload.temperatureC, 24);
});

test('retention aligns minute boundaries, zero-retention works, and bad clocks do not commit', t => {
  const dir = directory(t);
  const store = new EventStore(dir, { retentionMs: 0 });
  store.ingest(reading('previous', 59999));
  store.ingest(reading('boundary', 60000));
  assert.deepEqual(store.maintenance(BASE + 60000), { removed: 1, retained: 1, rollups: 2 });
  const before = stable(store.snapshot());
  for (const now of [NaN, Infinity, 'now', 9e15]) assert.throws(() => store.maintenance(now), TypeError);
  assert.deepEqual(stable(store.snapshot()), before);
  for (const retentionMs of [-1, NaN, Infinity, '30']) assert.throws(() => new EventStore(path.join(dir, 'invalid'), { retentionMs }), TypeError);
});

test('maintenance streams state and JSONL exports with writes bounded to 64 KiB', t => {
  const dir = directory(t);
  const store = new EventStore(dir);
  for (let i = 0; i < 12; i++) {
    for (const event of pipeline(`stream-${i}`, 10000 + i)) store.ingest(event);
  }
  const large = raw('large-unicode');
  large.payload.metadata = '漢字𐀀'.repeat(16000);
  store.ingest(large);
  const originalAtomic = store.writeAtomic.bind(store);
  const streamed = new Set();
  store.writeAtomic = (name, content) => {
    if (name !== 'events.jsonl') {
      assert.equal(typeof content[Symbol.iterator], 'function', name);
      assert.notEqual(typeof content, 'string', name);
      assert.equal(Array.isArray(content), false, name);
      streamed.add(name);
    }
    return originalAtomic(name, content);
  };
  const originalWrite = fs.writeSync;
  let largestWrite = 0, writes = 0;
  try {
    fs.writeSync = function(fd, buffer, offset, length, ...rest) {
      largestWrite = Math.max(largestWrite, length);
      writes++;
      assert.ok(length <= 64 * 1024, 'each buffered write is bounded');
      return originalWrite.call(fs, fd, buffer, offset, length, ...rest);
    };
    store.maintenance(BASE + 20000);
  } finally { fs.writeSync = originalWrite; }
  assert.deepEqual([...streamed].sort(), ['alerts.jsonl', 'commands.jsonl', 'rollups.jsonl', 'state.json', 'telemetry.jsonl', 'zone-metrics.jsonl']);
  assert.equal(largestWrite, 64 * 1024);
  assert.ok(writes > 5);
  const state = JSON.parse(fs.readFileSync(path.join(dir, 'state.json'), 'utf8'));
  assert.equal(state.events.length, 61);
  assert.equal(state.seenKeys.length, 61);
  assert.equal(state.events.at(-1).payload.metadata, large.payload.metadata);
  assert.equal(lines(path.join(dir, 'telemetry.jsonl')).at(-1).payload.metadata, large.payload.metadata);
  assert.deepEqual(stable(new EventStore(dir).snapshot()), stable(store.snapshot()));
});

test('failure partway through streamed snapshot leaves committed state and WAL recoverable', t => {
  const dir = directory(t);
  const store = new EventStore(dir, { retentionMs: 60000 });
  store.ingest(reading('old', 10000));
  store.maintenance(BASE + 20000);
  store.ingest(reading('new', 90000));
  const oldSnapshot = fs.readFileSync(store.statePath);
  const oldJournal = fs.readFileSync(store.journal);
  store.stateChunks = function* () {
    yield '{"events":["' + 'x'.repeat(128 * 1024);
    throw new Error('simulated mid-stream serialization failure');
  };
  assert.throws(() => store.maintenance(BASE + 120000), /mid-stream/);
  assert.deepEqual(fs.readFileSync(store.statePath), oldSnapshot);
  assert.deepEqual(fs.readFileSync(store.journal), oldJournal);
  assert.equal(store.events.size, 2, 'uncommitted expiry did not change memory');
  assert.ok(fs.statSync(store.statePath + '.tmp').size >= 64 * 1024, 'failure happened after a buffer flush');
  const restarted = new EventStore(dir, { retentionMs: 60000 });
  assert.equal(restarted.snapshot().totalEvents, 2);
  assert.equal(restarted.snapshot().rollups.reduce((count, row) => count + row.count, 0), 2);
});

test('rolling throughput uses scoped trailing ten seconds, excludes future and boundary timestamps', t => {
  const store = new EventStore(directory(t));
  for (const [id, sourceMs, latency, namespace, runId] of [
    ['boundary', 9000, 1000, 'main', 'run-1'],
    ['raw-boundary', 10000, 0, 'main', 'run-1'],
    ['after-boundary', 10001, 30, 'main', 'run-1'],
    ['recent', 15000, 30, 'main', 'run-1'],
    ['future', 21000, 30, 'main', 'run-1'],
    ['flow', 15000, 30, 'flow', 'run-1'],
    ['other-run', 15000, 30, 'main', 'run-2'],
  ]) for (const event of pipeline(id, sourceMs, latency, runId)) store.ingest(event, namespace);
  const originalNow = Date.now;
  try {
    Date.now = () => BASE + 20000;
    assert.deepEqual(store.snapshot('run-1', 'main').rollingThroughput, { windowMs: 10000, rawPerSecond: 0.2, appliedPerSecond: 0.2 });
    assert.deepEqual(store.snapshot('run-1', 'flow').rollingThroughput, { windowMs: 10000, rawPerSecond: 0.1, appliedPerSecond: 0.1 });
    assert.deepEqual(store.snapshot().rollingThroughput, { windowMs: 10000, rawPerSecond: 0.4, appliedPerSecond: 0.4 });
    assert.equal(store.snapshot().observedAt, at(20000));
    Date.now = () => BASE + 60000;
    assert.deepEqual(store.snapshot('run-1', 'main').rollingThroughput, { windowMs: 10000, rawPerSecond: 0, appliedPerSecond: 0 });
  } finally { Date.now = originalNow; }
});
