'use strict';
// Generates an importable flow using only the Node-RED core palette.
const fs = require('node:fs');
const path = require('node:path');
const tab = 'hvac-flow';
const body = fn => fn.toString().slice(fn.toString().indexOf('{') + 1, fn.toString().lastIndexOf('}')).trim();
const initialState = `let state = flow.get('hvacState');
if (!state) { state = { pipeline: 'node-red', startedAt: new Date().toISOString(), updatedAt: null, broker: { connected: false, text: 'connecting' }, counts: { received: 0, accepted: 0, rejected: 0, duplicates: 0, readings: 0, metrics: 0, commands: 0, errors: 0 }, zones: {}, lastRejected: null }; flow.set('hvacState', state); }\n`;
function functionNode(id, name, fn, outputs, wires, x, y, initialize = '') {
  return { id, type: 'function', z: tab, name, func: initialState + body(fn), outputs, timeout: 0, noerr: 0, initialize, finalize: '', libs: [], x, y, wires };
}
const flows = [
  { id: tab, type: 'tab', label: 'HVAC • independent MQTT pipeline', disabled: false, info: 'Independent mTLS MQTT raw → reading → zone window metrics → actuator command flow. Each derived stage is published in hvac/flow/<runId> only. Core HTTP nodes serve a live HTTPS dashboard; no synthetic inject nodes.' },
  { id: 'broker', type: 'mqtt-broker', name: 'Mosquitto mTLS :8883', broker: '${MQTT_HOST}', port: '8883', tls: 'mqtt-tls', clientid: 'hvac-node-red', autoConnect: true, usetls: true, protocolVersion: '4', keepalive: '30', cleansession: true, autoUnsubscribe: true, birthTopic: '', birthQos: '0', birthPayload: '', birthMsg: {}, closeTopic: '', closeQos: '0', closePayload: '', closeMsg: {}, willTopic: '', willQos: '0', willPayload: '', willMsg: {} },
  { id: 'mqtt-tls', type: 'tls-config', name: 'Verified broker + node-red client certificate', cert: '/certs/node-red.crt', key: '/certs/node-red.key', ca: '/certs/ca.crt', certname: '', keyname: '', caname: '', servername: 'mosquitto', verifyservercert: true, alpnprotocol: '' },
  { id: 'raw-in', type: 'mqtt in', z: tab, name: 'Raw telemetry / QoS 1 / mTLS', topic: 'hvac/flow/+/raw/+', qos: '1', datatype: 'utf8', broker: 'broker', nl: false, rap: true, rh: 0, inputs: 0, x: 190, y: 120, wires: [['gateway']] },
  functionNode('gateway', 'Validate, deduplicate and normalize', function () {
    const domain = global.get('domain');
    const now = Date.now();
    state.counts.received += 1;
    state.updatedAt = new Date(now).toISOString();
    let event;
    let reason;
    try { event = typeof msg.payload === 'string' ? JSON.parse(msg.payload) : JSON.parse(Buffer.isBuffer(msg.payload) ? msg.payload.toString('utf8') : JSON.stringify(msg.payload)); }
    catch (_) { reason = 'invalid JSON'; }
    if (!reason) reason = domain.validateRaw(event);
    const topicParts = String(msg.topic || '').split('/');
    if (!reason && (topicParts.length !== 5 || topicParts[0] !== 'hvac' || topicParts[1] !== 'flow' || topicParts[3] !== 'raw' || topicParts[2] !== event.runId || topicParts[4] !== event.payload.deviceId)) reason = 'raw topic does not match event runId/deviceId';
    if (reason) {
      const segment = value => typeof value === 'string' && /^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,127}$/.test(value) ? value : 'invalid';
      const runId = segment(topicParts[2]);
      const deviceId = segment(event && event.payload && event.payload.deviceId || topicParts[4]);
      const parentId = event && typeof event.id === 'string' && /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(event.id) ? event.id : 'node-red-malformed-' + now + '-' + state.counts.received;
      const rejected = { schemaVersion: 1, id: parentId + ':rejected', type: 'gateway.telemetry.rejected', timestamp: new Date(now).toISOString(), runId, correlationId: parentId, sourceEventId: parentId, payload: { reason, deviceId, zoneId: segment(event && event.payload && event.payload.zoneId), rejectedBy: 'node-red' } };
      state.counts.rejected += 1;
      state.lastRejected = rejected;
      node.status({ fill: 'red', shape: 'ring', text: 'rejected: ' + reason });
      flow.set('hvacState', state);
      return [null, null, { topic: 'hvac/flow/' + runId + '/rejected/' + deviceId, payload: JSON.stringify(rejected), qos: '1', retain: false }];
    }
    let seen = context.get('seenRawIds');
    if (!seen) { seen = new Map(); context.set('seenRawIds', seen); }
    const key = event.runId + '/' + event.id;
    if (seen.has(key)) {
      state.counts.duplicates += 1;
      node.status({ fill: 'yellow', shape: 'ring', text: 'duplicate suppressed' });
      flow.set('hvacState', state);
      return null;
    }
    seen.set(key, now);
    if (seen.size > 20000) seen.delete(seen.keys().next().value);
    const reading = domain.deriveEvent(event, 'sensor.reading', 'reading', { ...event.payload, receivedAt: new Date(now).toISOString(), normalizedBy: 'node-red', sourceTimestamp: event.timestamp }, now);
    state.counts.accepted += 1;
    state.counts.readings += 1;
    const zoneKey = event.runId + '/' + event.payload.zoneId;
    const zone = state.zones[zoneKey] || { runId: event.runId, zoneId: event.payload.zoneId };
    zone.reading = reading;
    zone.updatedAt = reading.timestamp;
    state.zones[zoneKey] = zone;
    flow.set('hvacState', state);
    node.status({ fill: 'green', shape: 'dot', text: state.counts.accepted + ' accepted' });
    return [{ payload: reading }, { topic: 'hvac/flow/' + reading.runId + '/reading/' + reading.payload.zoneId, payload: JSON.stringify(reading), qos: '1', retain: false }, null];
  }, 3, [['aggregate'], ['reading-out'], ['rejected-out']], 500, 120),
  functionNode('aggregate', 'Rolling zone window (10 readings)', function () {
    const domain = global.get('domain');
    let aggregator = context.get('zoneAggregator');
    if (!aggregator) { aggregator = new domain.ZoneAggregator(10); context.set('zoneAggregator', aggregator); }
    const reading = msg.payload;
    const payload = aggregator.update(reading, 'node-red');
    if (!payload) return null;
    const metrics = domain.deriveEvent(reading, 'zone.metrics.updated', 'metrics', payload);
    const zoneKey = metrics.runId + '/' + metrics.payload.zoneId;
    state.zones[zoneKey].metrics = metrics;
    state.zones[zoneKey].updatedAt = metrics.timestamp;
    state.updatedAt = metrics.timestamp;
    state.counts.metrics += 1;
    flow.set('hvacState', state);
    node.status({ fill: 'green', shape: 'dot', text: payload.zoneId + ': ' + payload.sampleCount + ' samples' });
    return [{ payload: metrics }, { topic: 'hvac/flow/' + metrics.runId + '/metrics/' + metrics.payload.zoneId, payload: JSON.stringify(metrics), qos: '1', retain: false }];
  }, 2, [['decide'], ['metrics-out']], 820, 120),
  functionNode('decide', 'HVAC decision / target 22 C', function () {
    const domain = global.get('domain');
    const metrics = msg.payload;
    const payload = { ...domain.decideCommand(metrics.payload, 22), sourceTimestamp: metrics.payload.sourceTimestamp };
    const command = domain.deriveEvent(metrics, 'actuator.command.created', 'command', payload);
    const zoneKey = command.runId + '/' + command.payload.zoneId;
    state.zones[zoneKey].command = command;
    state.zones[zoneKey].updatedAt = command.timestamp;
    state.updatedAt = command.timestamp;
    state.counts.commands += 1;
    flow.set('hvacState', state);
    node.status({ fill: 'green', shape: 'dot', text: payload.zoneId + ': ' + payload.mode });
    return { topic: 'hvac/flow/' + command.runId + '/command/' + command.payload.zoneId, payload: JSON.stringify(command), qos: '1', retain: false };
  }, 1, [['command-out']], 1120, 120),
  ...[['reading-out', 'Publish sensor.reading', 520, 220], ['metrics-out', 'Publish zone.metrics.updated', 820, 220], ['command-out', 'Publish actuator.command.created', 1150, 220], ['rejected-out', 'Publish validation rejection', 510, 280]].map(([id, name, x, y]) => ({ id, type: 'mqtt out', z: tab, name, topic: '', qos: '1', retain: 'false', respTopic: '', contentType: 'application/json', userProps: '', correl: '', expiry: '', broker: 'broker', x, y, wires: [] })),
  { id: 'broker-status', type: 'status', z: tab, name: 'Live MQTT connection state', scope: ['raw-in'], x: 200, y: 360, wires: [['record-broker-status']] },
  functionNode('record-broker-status', 'Track actual broker connection', function () {
    state.broker = { connected: msg.status.fill === 'green', text: msg.status.text || 'unknown', updatedAt: new Date().toISOString() };
    flow.set('hvacState', state);
    return null;
  }, 0, [], 510, 360),
  { id: 'flow-errors', type: 'catch', z: tab, name: 'Pipeline runtime faults', scope: ['gateway', 'aggregate', 'decide'], uncaught: false, x: 200, y: 420, wires: [['record-error']] },
  functionNode('record-error', 'Expose runtime fault (not a command)', function () {
    state.counts.errors += 1;
    state.lastError = { message: msg.error && msg.error.message || 'unknown pipeline fault', source: msg.error && msg.error.source && msg.error.source.name || 'pipeline', timestamp: new Date().toISOString() };
    flow.set('hvacState', state);
    node.warn(state.lastError.message);
    return null;
  }, 0, [], 520, 420),
  { id: 'dashboard-in', type: 'http in', z: tab, name: 'GET /dashboard', url: '/dashboard', method: 'get', upload: false, swaggerDoc: '', x: 190, y: 520, wires: [['dashboard-template']] },
  { id: 'dashboard-template', type: 'template', z: tab, name: 'Live dashboard / no external assets', field: 'payload', fieldType: 'msg', format: 'html', syntax: 'plain', template: fs.readFileSync(path.join(__dirname, 'dashboard.html'), 'utf8'), output: 'str', x: 500, y: 520, wires: [['dashboard-response']] },
  { id: 'dashboard-response', type: 'http response', z: tab, name: 'HTML response', statusCode: '200', headers: { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff' }, x: 820, y: 520, wires: [] },
  { id: 'api-in', type: 'http in', z: tab, name: 'GET /api/status', url: '/api/status', method: 'get', upload: false, swaggerDoc: '', x: 190, y: 580, wires: [['api-status']] },
  functionNode('api-status', 'Read live flow context', function () {
    msg.headers = { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff' };
    msg.payload = { ...state, checkedAt: new Date().toISOString(), windowSize: 10, targetTemperatureC: 22, namespace: 'hvac/flow', transport: 'MQTT 3.1.1 / TLS / client certificates / QoS 1', zones: Object.values(state.zones).sort((a, b) => a.runId.localeCompare(b.runId) || a.zoneId.localeCompare(b.zoneId)) };
    return msg;
  }, 1, [['api-response']], 500, 580),
  { id: 'api-response', type: 'http response', z: tab, name: 'JSON response', statusCode: '200', headers: {}, x: 820, y: 580, wires: [] }
];
fs.writeFileSync(path.join(__dirname, 'flows.json'), JSON.stringify(flows, null, 2) + '\n');
console.log('Generated ' + flows.length + ' core Node-RED nodes.');
