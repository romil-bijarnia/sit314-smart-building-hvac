'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { aggregationFilters } = require('../src/routing.cjs');
const { ownsZone } = require('../src/domain.cjs');
test('broadcast preserves the original wildcard subscription', () => {
  assert.deepEqual(aggregationFilters({mode:'broadcast',shardIndex:0,shardCount:2,zoneCount:12}), ['hvac/main/+/reading/+']);
});
test('selective routing covers each configured zone exactly once for all supported scales', () => {
  for (const zoneCount of [3,4,12,180,500]) for (const shardCount of [1,2,3]) {
    const all = [];
    for(let shardIndex=0;shardIndex<shardCount;shardIndex++) {
      const filters = aggregationFilters({mode:'selective',shardIndex,shardCount,zoneCount});
      for (const f of filters) {
        const zone = f.split('/')[4];
        assert.equal(ownsZone(zone,shardIndex,shardCount),true);
      }
      all.push(...filters);
    }
    assert.equal(all.length,zoneCount); assert.equal(new Set(all).size,zoneCount);
  }
});
test('selective routing uses exact zone topics and only the run namespace wildcard', () => {
  assert.deepEqual(aggregationFilters({mode:'selective',shardIndex:1,shardCount:2,zoneCount:4}), ['hvac/main/+/reading/zone-2','hvac/main/+/reading/zone-4']);
});
test('invalid routing mode and topology fail closed at startup', () => {
  for(const args of [{mode:'shared'},{shardCount:0},{shardIndex:-1},{shardIndex:2,shardCount:2},{zoneCount:0},{zoneCount:501},{zoneCount:2,shardCount:3},{zoneCount:2.5}]) assert.throws(()=>aggregationFilters(args),TypeError);
});
