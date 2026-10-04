'use strict';
const { ownsZone } = require('./domain.cjs');
function aggregationFilters({ mode = 'broadcast', shardIndex = 0, shardCount = 1, zoneCount = 12 } = {}) {
  if (!['broadcast', 'selective'].includes(mode)) throw new TypeError('Unsupported aggregation routing mode');
  if (![shardIndex, shardCount, zoneCount].every(Number.isSafeInteger) || shardCount < 1 || shardIndex < 0 || shardIndex >= shardCount || zoneCount < shardCount || zoneCount > 500) throw new TypeError('Invalid static routing topology');
  if (mode === 'broadcast') return ['hvac/main/+/reading/+'];
  return Array.from({ length: zoneCount }, (_, i) => `zone-${i + 1}`)
    .filter(zone => ownsZone(zone, shardIndex, shardCount))
    .map(zone => `hvac/main/+/reading/${zone}`);
}
module.exports = { aggregationFilters };
