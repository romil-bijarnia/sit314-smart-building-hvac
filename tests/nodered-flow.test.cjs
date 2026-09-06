'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const domain = require('../src/domain.cjs');
const root = path.join(__dirname, '..');
const flows = JSON.parse(fs.readFileSync(path.join(root, 'node-red/flows.json'), 'utf8'));
const byId = new Map(flows.map(node => [node.id, node]));
const store = () => { const values = new Map(); return { get: key => values.get(key), set: (key, value) => values.set(key, value) }; };
function harness() {
  const flow = store();
  const contexts = new Map();
  const invoke = (id, msg) => {
    if (!contexts.has(id)) contexts.set(id, store());
    const fn = new Function('msg', 'flow', 'context', 'global', 'node', byId.get(id).func);
    return fn(msg, flow, contexts.get(id), { get: key => key === 'domain' ? domain : undefined }, { status() {}, warn() {} });
  };
  const run = (raw, topic = `hvac/flow/${raw.runId}/raw/${raw.payload.deviceId}`) => {
    const first = invoke('gateway', { topic, payload: JSON.stringify(raw) });
    if (!first || !first[0]) return { gateway: first };
    const second = invoke('aggregate', first[0]);
    const command = second && invoke('decide', second[0]);
    return { gateway: first, aggregation: second, command, reading: first[0].payload, metrics: second[0].payload, event: JSON.parse(command.payload) };
  };
  return { flow, invoke, run };
}
function raw(sequence, overrides = {}) {
  const id = `node-red-test-${sequence}`;
  return { schemaVersion: 1, id, type: 'device.telemetry.raw', timestamp: new Date(Date.UTC(2026, 8, 6, 0, 0, sequence)).toISOString(), runId: 'nodered-test', correlationId: id, payload: { deviceId: 'device-a', zoneId: 'zone-a', temperatureC: 27, humidityPercent: 55, occupancyCount: 4, co2Ppm: 1100, batteryPercent: 95, sequence }, ...overrides };
}

test('Node-RED flow contains only real core MQTT/HTTP nodes and valid wires', () => {
  const allowed = new Set(['tab', 'mqtt-broker', 'tls-config', 'mqtt in', 'mqtt out', 'function', 'status', 'catch', 'http in', 'template', 'http response']);
  assert.equal(new Set(flows.map(node => node.id)).size, flows.length);
  for (const node of flows) {
    assert.ok(allowed.has(node.type), node.type);
    for (const output of node.wires || []) for (const destination of output) assert.ok(byId.has(destination), destination);
    if (node.type === 'function') assert.doesNotThrow(() => new Function('msg', 'flow', 'context', 'global', 'node', node.func), node.name);
  }
  assert.equal(byId.get('raw-in').topic, 'hvac/flow/+/raw/+');
  assert.equal(byId.get('raw-in').qos, '1');
  for (const id of ['reading-out', 'metrics-out', 'command-out', 'rejected-out']) {
    assert.equal(byId.get(id).type, 'mqtt out');
    assert.equal(byId.get(id).qos, '1');
    assert.equal(byId.get(id).retain, 'false');
  }
  assert.equal(byId.get('broker').port, '8883');
  assert.equal(byId.get('broker').usetls, true);
  assert.equal(byId.get('mqtt-tls').verifyservercert, true);
  assert.equal(byId.get('mqtt-tls').cert, '/certs/node-red.crt');
  assert.equal(byId.get('mqtt-tls').key, '/certs/node-red.key');
  assert.equal(byId.get('mqtt-tls').ca, '/certs/ca.crt');
});

test('Node-RED gateway, metrics and command preserve schema, correlation and publish each stage', () => {
  const h = harness();
  const input = raw(1);
  const output = h.run(input);
  assert.equal(output.reading.type, 'sensor.reading');
  assert.equal(output.reading.id, input.id + ':reading');
  assert.equal(output.reading.sourceEventId, input.id);
  assert.equal(output.reading.payload.sourceTimestamp, input.timestamp);
  assert.equal(output.reading.payload.normalizedBy, 'node-red');
  assert.ok(Number.isFinite(Date.parse(output.reading.payload.receivedAt)));
  assert.equal(output.metrics.type, 'zone.metrics.updated');
  assert.equal(output.metrics.id, input.id + ':metrics');
  assert.equal(output.metrics.sourceEventId, output.reading.id);
  assert.equal(output.event.type, 'actuator.command.created');
  assert.equal(output.event.id, input.id + ':command');
  assert.equal(output.event.sourceEventId, output.metrics.id);
  assert.equal(output.event.payload.sourceTimestamp, input.timestamp);
  for (const event of [output.reading, output.metrics, output.event]) {
    assert.equal(event.correlationId, input.id);
    assert.equal(event.runId, input.runId);
    assert.equal(event.schemaVersion, 1);
  }
  assert.equal(output.gateway[1].topic, 'hvac/flow/nodered-test/reading/zone-a');
  assert.equal(output.aggregation[1].topic, 'hvac/flow/nodered-test/metrics/zone-a');
  assert.equal(output.command.topic, 'hvac/flow/nodered-test/command/zone-a');
  assert.equal(output.event.payload.mode, 'cool');
  assert.deepEqual(h.flow.get('hvacState').counts, { received: 1, accepted: 1, rejected: 0, duplicates: 0, readings: 1, metrics: 1, commands: 1, errors: 0 });
});

test('Node-RED rolling metrics and decisions match the shared domain over 15 readings', () => {
  const h = harness();
  const expected = new domain.ZoneAggregator(10);
  for (let i = 1; i <= 15; i++) {
    const input = raw(i);
    input.payload.temperatureC = 19 + (i % 10);
    input.payload.occupancyCount = i % 5;
    input.payload.co2Ppm = 500 + i * 40;
    const output = h.run(input);
    const expectedPayload = expected.update(domain.deriveEvent(input, 'sensor.reading', 'reading', { ...input.payload, sourceTimestamp: input.timestamp }), 'node-red');
    assert.deepEqual(output.metrics.payload, expectedPayload);
    assert.deepEqual(output.event.payload, { ...domain.decideCommand(expectedPayload, 22), sourceTimestamp: input.timestamp });
    assert.equal(output.metrics.payload.sampleCount, Math.min(i, 10));
  }
});

test('Node-RED suppresses duplicate raw delivery before all downstream work', () => {
  const h = harness();
  h.run(raw(1));
  assert.equal(h.run(raw(1)).gateway, null);
  const state = h.flow.get('hvacState');
  assert.equal(state.counts.received, 2);
  assert.equal(state.counts.duplicates, 1);
  assert.equal(state.counts.readings, 1);
  assert.equal(state.counts.metrics, 1);
  assert.equal(state.counts.commands, 1);
});

test('Node-RED run/zone windows and deduplication are isolated', () => {
  const h = harness();
  h.run(raw(1));
  const second = raw(1, { runId: 'second-run' });
  second.payload.temperatureC = 20;
  assert.equal(h.run(second).metrics.payload.averageTemperatureC, 20);
  const third = raw(2);
  third.payload.zoneId = 'zone-b';
  third.payload.temperatureC = 18;
  assert.equal(h.run(third).metrics.payload.sampleCount, 1);
  assert.equal(Object.keys(h.flow.get('hvacState').zones).length, 3);
});

test('Node-RED rejects malformed JSON, invalid measurements and wrong source topics', () => {
  const h = harness();
  const malformed = h.invoke('gateway', { topic: 'hvac/flow/bad-run/raw/device-a', payload: '{not-json' });
  const badJsonEvent = JSON.parse(malformed[2].payload);
  assert.equal(malformed[0], null);
  assert.equal(badJsonEvent.type, 'gateway.telemetry.rejected');
  assert.equal(badJsonEvent.payload.reason, 'invalid JSON');
  assert.equal(malformed[2].topic, 'hvac/flow/bad-run/rejected/device-a');
  const invalid = raw(2);
  invalid.payload.humidityPercent = 101;
  const rejected = h.run(invalid).gateway;
  const invalidEvent = JSON.parse(rejected[2].payload);
  assert.equal(invalidEvent.id, invalid.id + ':rejected');
  assert.match(invalidEvent.payload.reason, /humidityPercent/);
  const wrongTopic = h.run(raw(3), 'hvac/flow/wrong-run/raw/device-a').gateway;
  assert.match(JSON.parse(wrongTopic[2].payload).payload.reason, /topic/);
  assert.equal(h.flow.get('hvacState').counts.rejected, 3);
  assert.equal(h.flow.get('hvacState').counts.commands, 0);
  const badIdentity = h.run(raw(4, { id: 'bad/identity', correlationId: 'bad/identity' })).gateway;
  const badIdentityEvent = JSON.parse(badIdentity[2].payload);
  assert.match(badIdentityEvent.id, /^node-red-malformed-[0-9]+-[0-9]+:rejected$/);
  assert.doesNotThrow(() => domain.deriveEvent(badIdentityEvent, 'test.event', 'child', {}));
});

test('Node-RED HTTP status is backed by live flow context and MQTT status', () => {
  const h = harness();
  const empty = h.invoke('api-status', {});
  assert.equal(empty.payload.zones.length, 0);
  assert.equal(empty.payload.counts.commands, 0);
  assert.equal(empty.payload.broker.connected, false);
  h.invoke('record-broker-status', { status: { fill: 'green', text: 'node-red:common.status.connected' } });
  h.run(raw(1));
  const live = h.invoke('api-status', {});
  assert.equal(live.payload.zones[0].command.payload.mode, 'cool');
  assert.equal(live.payload.broker.connected, true);
  assert.equal(live.payload.counts.commands, 1);
  assert.equal(live.headers['Cache-Control'], 'no-store');
  const dashboard = byId.get('dashboard-template').template;
  assert.match(dashboard, /fetch\('\/api\/status'/);
  assert.doesNotMatch(dashboard, /(?:src|href)=["']https?:\/\//);
  assert.doesNotMatch(dashboard, /innerHTML/);
  assert.equal(byId.get('api-in').url, '/api/status');
  assert.equal(byId.get('dashboard-in').url, '/dashboard');
});

test('Node-RED settings default to local HTTP, allow opt-in TLS, and expose the domain', () => {
  const settingsText = fs.readFileSync(path.join(root, 'node-red/settings.cjs'), 'utf8');
  const load = env => {
    const sandbox = { module: { exports: {} }, process: { env }, require: name => name === 'node:fs' ? { readFileSync: file => 'certificate:' + file } : domain };
    vm.runInNewContext(settingsText, sandbox);
    return sandbox.module.exports;
  };
  const normal = load({});
  assert.equal(normal.https, undefined);
  assert.equal(normal.httpAdminRoot, '/admin');
  assert.equal(normal.functionGlobalContext.domain, domain);
  assert.equal(normal.functionExternalModules, false);
  const secure = load({ NODE_RED_HTTPS: 'true' });
  assert.equal(secure.https.minVersion, 'TLSv1.2');
  assert.equal(secure.https.key, 'certificate:/certs/dashboard.key');
});


test('Node-RED Docker CMD supplies settings arguments to the official entrypoint', () => {
  const dockerfile = fs.readFileSync(path.join(root, 'node-red/Dockerfile'), 'utf8');
  const commandLine = dockerfile.split('\n').find(line => /^CMD\s/.test(line));
  assert.ok(commandLine, 'Dockerfile needs an explicit CMD');
  const args = JSON.parse(commandLine.replace(/^CMD\s+/, ''));
  assert.deepEqual(args, ['--settings', '/data/settings.cjs', '/data/flows.json']);
  assert.ok(!args.includes('npm'), 'Official entrypoint already invokes Node-RED');
});
