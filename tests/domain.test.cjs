'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { validateRaw, deriveEvent, ZoneAggregator, decideCommand, ownsZone, CepEngine } = require('../src/domain.cjs');

const START = Date.parse('2026-09-06T00:00:00.000Z');
function raw(index = 0, overrides = {}, envelope = {}) {
  const id = `sample-${index}`;
  return {
    schemaVersion: 1,
    id,
    type: 'device.telemetry.raw',
    timestamp: new Date(START + index * 1000).toISOString(),
    runId: 'test-run',
    correlationId: id,
    payload: { deviceId: 'sensor-1', zoneId: 'zone-1', temperatureC: 24, humidityPercent: 50, occupancyCount: 2, co2Ppm: 1000, batteryPercent: 95, ...overrides },
    ...envelope,
  };
}
function reading(index = 0, overrides = {}, envelope = {}) {
  const source = raw(index, overrides, envelope);
  return deriveEvent(source, 'sensor.reading', 'reading', { sourceTimestamp: source.timestamp, ...source.payload }, START + index * 1000 + 5);
}

test('validateRaw accepts a complete envelope and optional sequence', () => {
  assert.equal(validateRaw(raw()), null);
  assert.equal(validateRaw(raw(0, { sequence: 0, extraDeviceMetadata: 'preserved' })), null);
  assert.equal(validateRaw(raw(0, {}, { timestamp: '2024-02-29T11:15:16+10:00' })), null);
});

test('validateRaw handles non-object and malformed values without throwing', () => {
  for (const candidate of [null, undefined, 'event', 3, true, [], {}, { payload: null }]) {
    assert.equal(typeof validateRaw(candidate), 'string');
  }
  for (const payload of [null, undefined, [], '', 5]) assert.match(validateRaw(raw(0, {}, { payload })), /payload/);
});

test('validateRaw requires schema version, event type, correlation and safe identities', () => {
  for (const field of ['schemaVersion', 'id', 'type', 'timestamp', 'runId', 'correlationId']) {
    const event = raw();
    delete event[field];
    assert.equal(typeof validateRaw(event), 'string', field);
  }
  assert.match(validateRaw(raw(0, {}, { schemaVersion: '1' })), /schemaVersion/);
  assert.match(validateRaw(raw(0, {}, { type: 'other.type' })), /type/);
  assert.match(validateRaw(raw(0, {}, { correlationId: 'different-root' })), /correlationId/);
  for (const field of ['deviceId', 'zoneId']) {
    for (const value of ['', ' ', '/path', 'zone/1', 'wild#', 'wild+', null, 1, 'a'.repeat(129)]) {
      assert.match(validateRaw(raw(0, { [field]: value })), new RegExp(field));
    }
  }
  for (const field of ['id', 'runId', 'correlationId']) {
    for (const value of ['', 'has space', 'wild#', 'a'.repeat(129)]) {
      assert.equal(typeof validateRaw(raw(0, {}, { [field]: value })), 'string');
    }
  }
  const id = 'a'.repeat(128);
  assert.equal(validateRaw(raw(0, { deviceId: id, zoneId: id }, { id, correlationId: id, runId: id })), null);
});

test('validateRaw enforces all finite numeric sensor bounds inclusively', async t => {
  const bounds = { temperatureC: [-20, 80], humidityPercent: [0, 100], co2Ppm: [400, 5000], batteryPercent: [0, 100] };
  for (const [field, [min, max]] of Object.entries(bounds)) {
    await t.test(field, () => {
      for (const value of [min, max, (min + max) / 2]) assert.equal(validateRaw(raw(0, { [field]: value })), null);
      for (const value of [min - 0.001, max + 0.001, NaN, Infinity, -Infinity, String(min), null, undefined]) {
        assert.match(validateRaw(raw(0, { [field]: value })), new RegExp(field));
      }
    });
  }
});

test('occupancy and optional sequence require non-negative safe integers', () => {
  for (const field of ['occupancyCount', 'sequence']) {
    for (const value of [0, 1, Number.MAX_SAFE_INTEGER]) assert.equal(validateRaw(raw(0, { [field]: value })), null);
    for (const value of [-1, 0.5, NaN, Infinity, '1', null, Number.MAX_SAFE_INTEGER + 1]) assert.match(validateRaw(raw(0, { [field]: value })), new RegExp(field));
  }
  assert.match(validateRaw(raw(0, { occupancyCount: undefined })), /occupancyCount/);
});

test('validateRaw rejects unparseable, timezone-free and impossible timestamps', () => {
  const invalid = ['', null, 1, 'yesterday', '2026-09-06', '2026-09-06T00:00:00', '2026-02-29T00:00:00Z', '2026-02-30T00:00:00Z', '2026-04-31T00:00:00Z', '2026-00-01T00:00:00Z', '2026-13-01T00:00:00Z', '2026-09-06T24:00:00Z', '2026-09-06T00:60:00Z', '2026-09-06T00:00:60Z', '2026-09-06T00:00:00+24:00', '2026-09-06T00:00:00+10:60'];
  for (const timestamp of invalid) assert.match(validateRaw(raw(0, {}, { timestamp })), /timestamp/, String(timestamp));
});

test('deriveEvent produces deterministic root IDs and immediate-parent lineage', () => {
  const source = raw();
  const normalized = deriveEvent(source, 'sensor.reading', ':reading', { ...source.payload }, START + 10);
  const metrics = deriveEvent(normalized, 'zone.metrics.updated', 'metrics', { zoneId: 'zone-1' }, START + 20);
  const command = deriveEvent(metrics, 'hvac.command.requested', 'command', { zoneId: 'zone-1' }, START + 30);
  assert.equal(normalized.id, `${source.id}:reading`);
  assert.equal(metrics.id, `${source.id}:metrics`);
  assert.equal(command.id, `${source.id}:command`);
  assert.equal(command.correlationId, source.id);
  assert.equal(command.sourceEventId, metrics.id);
  assert.equal(command.timestamp, '2026-09-06T00:00:00.030Z');
  assert.equal(command.runId, source.runId);
  assert.equal(command.schemaVersion, 1);
  assert.equal(source.type, 'device.telemetry.raw');
  const fallback = { id: 'root', runId: 'run' };
  assert.equal(deriveEvent(fallback, 'test.event', 'child', {}, START).correlationId, 'root');
});

test('deriveEvent rejects invalid arguments and supports maximum raw identity length', () => {
  for (const parent of [null, {}, { id: 'id', runId: 'run', correlationId: null }]) assert.throws(() => deriveEvent(parent, 'test.event', 'suffix', {}, START), TypeError);
  for (const suffix of ['', ':', 'contains/slash', 'a'.repeat(32)]) assert.throws(() => deriveEvent(raw(), 'test.event', suffix, {}, START), TypeError);
  assert.throws(() => deriveEvent(raw(), 'bad type', 'child', {}, START), TypeError);
  assert.throws(() => deriveEvent(raw(), 'test.event', 'child', null, START), TypeError);
  for (const now of [NaN, Infinity, 'today', 9e15]) assert.throws(() => deriveEvent(raw(), 'test.event', 'child', {}, now), TypeError);
  const id = 'a'.repeat(128);
  const event = raw(0, {}, { id, correlationId: id });
  const normalized = deriveEvent(event, 'sensor.reading', 'reading', event.payload, START);
  assert.equal(new ZoneAggregator().update(normalized).sampleCount, 1);
});

test('ZoneAggregator ring evicts oldest readings and rounds metrics', () => {
  const aggregator = new ZoneAggregator(3);
  const values = [20, 21, 22, 24];
  let metrics;
  for (let index = 0; index < values.length; index++) {
    metrics = aggregator.update(reading(index, { temperatureC: values[index], humidityPercent: 40 + index, occupancyCount: index, co2Ppm: 800 + index }), 'agg-2');
  }
  assert.deepEqual(metrics, {
    zoneId: 'zone-1', sampleCount: 3, averageTemperatureC: 22.33, averageHumidityPercent: 42, averageOccupancyCount: 2, averageCo2Ppm: 802,
    aggregatorId: 'agg-2', sourceTimestamp: '2026-09-06T00:00:03.000Z',
  });
  assert.equal(aggregator.windows.values().next().value.samples.length, 3);
  assert.equal(aggregator.update(reading(0, { temperatureC: 20 })), null, 'old duplicate remains deduplicated after leaving the ring');
  assert.equal(aggregator.update(reading(4, { temperatureC: 26 })).averageTemperatureC, 24);
});

test('ZoneAggregator deduplication includes run and isolates zone windows', () => {
  const aggregator = new ZoneAggregator(2);
  assert.equal(aggregator.update(reading(0, { temperatureC: 10 })).sampleCount, 1);
  assert.equal(aggregator.update(reading(0, { temperatureC: 30, zoneId: 'zone-2' })), null, 'same event ID cannot move zones');
  assert.equal(aggregator.update(reading(1, { temperatureC: 30, zoneId: 'zone-2' })).averageTemperatureC, 30);
  const otherRun = reading(0, { temperatureC: 40 }, { runId: 'another-run' });
  assert.equal(aggregator.update(otherRun).averageTemperatureC, 40);
  assert.equal(aggregator.update(reading(2, { temperatureC: 14 })).averageTemperatureC, 12);
});

test('ZoneAggregator validates inputs and has a timestamp fallback', () => {
  for (const size of [0, -1, 1.5, NaN, Infinity, '10']) assert.throws(() => new ZoneAggregator(size), RangeError);
  const aggregator = new ZoneAggregator(1);
  for (const event of [null, {}, reading(0, { temperatureC: NaN }), reading(0, { batteryPercent: -1 })]) assert.throws(() => aggregator.update(event), TypeError);
  const event = reading();
  delete event.payload.sourceTimestamp;
  assert.equal(aggregator.update(event).sourceTimestamp, event.timestamp);
  assert.throws(() => aggregator.update(reading(1, { sourceTimestamp: 'bad' })), TypeError);
  assert.throws(() => aggregator.update(reading(1), ''), TypeError);
});

test('checkpoint JSON round trip preserves ring position and deduplication', () => {
  const original = new ZoneAggregator(3);
  for (let index = 0; index < 5; index++) original.update(reading(index, { temperatureC: 15 + index }));
  original.update(reading(10, { zoneId: 'zone-2' }));
  original.update(reading(0, { temperatureC: 70 }, { runId: 'different-run' }));
  const snapshot = JSON.parse(JSON.stringify(original.serialize()));
  const restored = new ZoneAggregator(99).restore(snapshot);
  assert.equal(restored.windowSize, 3);
  assert.deepEqual(restored.serialize(), snapshot);
  assert.equal(restored.update(reading(0)), null);
  for (let index = 5; index < 20; index++) assert.deepEqual(restored.update(reading(index, { temperatureC: index })), original.update(reading(index, { temperatureC: index })));
  snapshot.windows[0].samples[0].temperatureC = 80;
  assert.deepEqual(restored.serialize(), original.serialize(), 'restore does not retain checkpoint references');
  const exported = restored.serialize();
  exported.windows[0].samples[0].temperatureC = 80;
  assert.deepEqual(restored.serialize(), original.serialize(), 'serialize does not expose mutable ring samples');
});

test('checkpoint restore handles partial and empty windows and is atomic on error', () => {
  assert.deepEqual(new ZoneAggregator().restore(new ZoneAggregator().serialize()).serialize(), new ZoneAggregator().serialize());
  const aggregator = new ZoneAggregator(3);
  aggregator.update(reading());
  const good = aggregator.serialize();
  assert.deepEqual(new ZoneAggregator().restore(good).serialize(), good);
  const mutations = [
    s => { s.version = 2; }, s => { s.windowSize = 0; }, s => { s.windows = null; },
    s => { s.windows[0].nextIndex = 0; }, s => { s.windows[0].samples[0].co2Ppm = 0; },
    s => { s.windows.push(s.windows[0]); }, s => { s.seenEventIds = [['run', null]]; },
    s => { s.seenEventIds.push(s.seenEventIds[0]); },
  ];
  for (const mutate of mutations) {
    const invalid = JSON.parse(JSON.stringify(good));
    mutate(invalid);
    assert.throws(() => aggregator.restore(invalid), TypeError);
    assert.deepEqual(aggregator.serialize(), good);
  }
});

test('decideCommand covers temperature, occupancy and CO2 boundaries', () => {
  const base = { zoneId: 'zone-1', averageTemperatureC: 24, averageOccupancyCount: 2, averageCo2Ppm: 900 };
  const cases = [
    [{}, 'cool', 'medium'],
    [{ averageCo2Ppm: 901 }, 'cool', 'high'],
    [{ averageTemperatureC: 23.5 }, 'idle', 'low'],
    [{ averageTemperatureC: 23.5001 }, 'cool', 'medium'],
    [{ averageTemperatureC: 23.5, averageCo2Ppm: 901 }, 'ventilate', 'high'],
    [{ averageTemperatureC: 20, averageCo2Ppm: 900 }, 'idle', 'low'],
    [{ averageTemperatureC: 20, averageCo2Ppm: 900.01 }, 'ventilate', 'high'],
    [{ averageOccupancyCount: 1.99, averageCo2Ppm: 2000 }, 'idle', 'low'],
    [{ averageOccupancyCount: 0, averageTemperatureC: 50 }, 'idle', 'low'],
  ];
  for (const [overrides, mode, fanSpeed] of cases) {
    const command = decideCommand({ ...base, ...overrides });
    assert.equal(command.mode, mode);
    assert.equal(command.fanSpeed, fanSpeed);
    assert.equal(command.zoneId, base.zoneId);
    assert.equal(command.targetTemperatureC, 22);
    assert.ok(command.reason.length > 0);
  }
  assert.equal(decideCommand(base, 23).mode, 'idle');
  assert.equal(decideCommand(base, 20).targetTemperatureC, 20);
  assert.throws(() => decideCommand({ ...base, averageCo2Ppm: NaN }), TypeError);
  assert.throws(() => decideCommand(base, Infinity), TypeError);
});

test('ownsZone gives every valid zone exactly one shard and rejects invalid indices', () => {
  for (const count of [1, 2, 3, 7]) {
    for (let zone = 1; zone <= 30; zone++) {
      const owners = Array.from({ length: count }, (_, index) => ownsZone(`zone-${zone}`, index, count));
      assert.equal(owners.filter(Boolean).length, 1);
      assert.equal(owners[(zone - 1) % count], true);
    }
  }
  for (const id of [null, 'zone-0', 'zone--1', 'zone-01', 'zone-1.5', 'zone-1-extra', 'another-1', `zone-${Number.MAX_SAFE_INTEGER + 1}`]) assert.equal(ownsZone(id, 0, 2), false);
  for (const [index, count] of [[-1, 2], [2, 2], [0, 0], [0, 1.5], [0.5, 2], [0, NaN]]) assert.equal(ownsZone('zone-1', index, count), false);
});

test('CEP requires elapsed source duration, not just three fast readings', () => {
  const cep = new CepEngine();
  for (let index = 0; index < 10; index++) assert.deepEqual(cep.reading(reading(index), START + index * 1000), []);
  const alerts = cep.reading(reading(10), START + 10000);
  assert.equal(alerts.length, 1);
  assert.equal(alerts[0].kind, 'sustained_co2');
  assert.equal(alerts[0].durationMs, 10000);
  assert.equal(alerts[0].sourceEventId, 'sample-10:reading');
  assert.equal(alerts[0].correlationId, 'sample-10');
  assert.equal(alerts[0].deviceId, 'sensor-1');
  assert.equal(alerts[0].zoneId, 'zone-1');
  assert.equal(alerts[0].sourceTimestamp, '2026-09-06T00:00:10.000Z');
  assert.equal(alerts[0].detectedAt, '2026-09-06T00:00:10.000Z');
  assert.deepEqual(cep.reading(reading(11), START + 11000), []);
  const fast = new CepEngine();
  for (let index = 0; index < 3; index++) {
    const event = reading(index);
    event.payload.sourceTimestamp = new Date(START + index).toISOString();
    assert.deepEqual(fast.reading(event, START + index), []);
  }
});

test('CEP source time controls sustained duration while receipt time controls silence', () => {
  const cep = new CepEngine();
  for (let index = 0; index <= 10; index++) {
    const event = reading(index);
    event.timestamp = new Date(START + 50000 + index).toISOString();
    const alerts = cep.reading(event, START + 100000 + index);
    assert.equal(alerts.length, index === 10 ? 1 : 0);
  }
  assert.deepEqual(cep.sweep(START + 105009), []);
  const alerts = cep.sweep(START + 105010);
  assert.equal(alerts[0].kind, 'silent_device');
  assert.equal(alerts[0].silentForMs, 5000);
});

test('CEP low CO2 or low occupancy resets sustained episode and permits another alert', () => {
  for (const interruption of [{ co2Ppm: 900 }, { occupancyCount: 1 }]) {
    const cep = new CepEngine({ sustainedMs: 2000 });
    assert.deepEqual(cep.reading(reading(0), START), []);
    assert.deepEqual(cep.reading(reading(1), START + 1000), []);
    assert.deepEqual(cep.reading(reading(2, interruption), START + 2000), []);
    for (let index = 3; index < 5; index++) assert.deepEqual(cep.reading(reading(index), START + index * 1000), []);
    assert.equal(cep.reading(reading(5), START + 5000)[0].kind, 'sustained_co2');
    assert.deepEqual(cep.reading(reading(6), START + 6000), []);
    assert.deepEqual(cep.reading(reading(7, interruption), START + 7000), []);
    assert.deepEqual(cep.reading(reading(8), START + 8000), []);
    assert.deepEqual(cep.reading(reading(9), START + 9000), []);
    assert.equal(cep.reading(reading(10), START + 10000)[0].kind, 'sustained_co2');
  }
});

test('CEP silent alert occurs exactly at threshold, once per episode, then rearms', () => {
  const cep = new CepEngine();
  cep.reading(reading(0), START);
  assert.deepEqual(cep.sweep(START + 4999), []);
  assert.equal(cep.sweep(START + 5000)[0].kind, 'silent_device');
  assert.deepEqual(cep.sweep(START + 20000), []);
  assert.deepEqual(cep.reading(reading(20), START + 20000), []);
  assert.deepEqual(cep.sweep(START + 24999), []);
  assert.equal(cep.sweep(START + 25000).length, 1);
});

test('CEP ignores duplicate IDs, ID collisions, equal and older source timestamps', () => {
  const cep = new CepEngine({ sustainedMs: 2000 });
  cep.reading(reading(1), START);
  assert.deepEqual(cep.reading(reading(1), START + 1000), []);
  const collision = reading(5);
  collision.id = 'sample-1:reading';
  assert.deepEqual(cep.reading(collision, START + 2000), []);
  assert.deepEqual(cep.reading(reading(0), START + 3000), []);
  const equalTime = reading(4);
  equalTime.payload.sourceTimestamp = reading(1).payload.sourceTimestamp;
  assert.deepEqual(cep.reading(equalTime, START + 4000), []);
  const silent = cep.sweep(START + 5000);
  assert.equal(silent.length, 1, 'rejected messages do not refresh liveness');
  assert.equal(silent[0].sourceEventId, 'sample-1:reading');
});

test('CEP gaps and zone moves break continuity even if sweep was not called', () => {
  for (const gapSource of [true, false]) {
    const cep = new CepEngine({ sustainedMs: 2000, silentMs: 5000 });
    cep.reading(reading(0), START);
    cep.reading(reading(1), START + 1000);
    const event = reading(gapSource ? 6 : 2);
    assert.deepEqual(cep.reading(event, START + (gapSource ? 2000 : 6000)), []);
  }
  const cep = new CepEngine({ sustainedMs: 2000 });
  cep.reading(reading(0), START);
  cep.reading(reading(1), START + 1000);
  assert.deepEqual(cep.reading(reading(2, { zoneId: 'zone-2' }), START + 2000), []);
  assert.deepEqual(cep.reading(reading(3, { zoneId: 'zone-2' }), START + 3000), []);
  assert.equal(cep.reading(reading(4, { zoneId: 'zone-2' }), START + 4000)[0].zoneId, 'zone-2');
});

test('CEP devices and runs are independent; unseen devices are not invented', () => {
  const cep = new CepEngine();
  assert.deepEqual(cep.sweep(START), []);
  cep.reading(reading(0), START);
  cep.reading(reading(1, { deviceId: 'sensor-2' }), START + 1000);
  cep.reading(reading(0, {}, { runId: 'other-run' }), START + 2000);
  const first = cep.sweep(START + 5000);
  assert.equal(first.length, 1);
  assert.equal(first[0].runId, 'test-run');
  assert.equal(first[0].deviceId, 'sensor-1');
  assert.equal(cep.sweep(START + 6000)[0].deviceId, 'sensor-2');
  assert.equal(cep.sweep(START + 7000)[0].runId, 'other-run');
});

test('CEP rejects malformed readings and invalid clocks without state changes', () => {
  for (const options of [{ sustainedMs: -1 }, { sustainedMs: NaN }, { silentMs: 0 }, { silentMs: Infinity }]) assert.throws(() => new CepEngine(options), RangeError);
  const cep = new CepEngine();
  for (const event of [null, {}, reading(0, { co2Ppm: NaN }), reading(0, { sourceTimestamp: 'bad' })]) assert.deepEqual(cep.reading(event, START), []);
  for (const clock of [NaN, Infinity, 'now', 9e15]) {
    assert.deepEqual(cep.reading(reading(), clock), []);
    assert.deepEqual(cep.sweep(clock), []);
  }
  assert.equal(cep.devices.size, 0);
  cep.reading(reading(0), START);
  assert.deepEqual(cep.reading(reading(1), START - 1), []);
  assert.equal(cep.sweep(START + 5000).length, 1);
  const immediate = new CepEngine({ sustainedMs: 0 });
  assert.equal(immediate.reading(reading(), START)[0].durationMs, 0);
});
