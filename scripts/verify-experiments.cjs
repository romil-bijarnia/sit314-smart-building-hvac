'use strict';
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),crypto=require('node:crypto');
const root=path.resolve(__dirname,'..');const campaign=JSON.parse(fs.readFileSync(path.join(root,'evidence/campaign.json')));
assert.equal(campaign.records.length,16,'Expected complete final campaign');
const normal=campaign.records.filter(r=>r.category==='normal');assert.equal(normal.length,12);
const rows=[];
for(const r of campaign.records){
 const rawFile=path.join(root,'evidence/runs',r.runId,'raw-sent.jsonl');const raw=fs.readFileSync(rawFile,'utf8').trim().split('\n').map(JSON.parse);
 assert.equal(raw.length,r.uniqueRawOffered);assert.equal(new Set(raw.map(e=>e.id)).size,raw.length);
 for(const type of ['device.telemetry.raw','sensor.reading','zone.metrics.updated','actuator.command.created','actuator.command.applied'])assert.equal(r.final.counts[type],raw.length,r.runId+' '+type);
 assert.equal(r.final.latencyMs.count,raw.length);assert.ok(r.final.latencyMs.min>=0);assert.equal(r.finalBacklog,0);
 const latestSource=Math.max(...raw.map(e=>Date.parse(e.timestamp)));const deadline=Date.parse(r.offeringEndedAt);
 // This proves all acknowledgements precede the deadline without treating a delayed API observation as instantaneous.
 const upperBound=latestSource+r.final.latencyMs.max;const inWindowProven=upperBound<=deadline;
 const knownLateZones=Object.values(r.final.zones).filter(z=>Date.parse(z.actuator?.appliedAt)>deadline).length;
 rows.push({runId:r.runId,category:r.category,repetition:r.repetition,devices:r.config.devices,zones:r.config.zones,shards:r.config.shards,intervalMs:r.config.intervalMs,offered:raw.length,
  applied:r.final.counts['actuator.command.applied'],p50Ms:r.final.latencyMs.p50,p95Ms:r.final.latencyMs.p95,p99Ms:r.final.latencyMs.p99,maxMs:r.final.latencyMs.max,
  finalBacklog:r.finalBacklog,allAppliedWithinOfferWindowProven:inWindowProven,completionUpperBoundMarginMs:deadline-upperBound,knownLateAcknowledgementsAtLeast:knownLateZones,
  finalObservationDelayMs:Date.parse(r.final.observedAt)-deadline,rawSHA256:crypto.createHash('sha256').update(fs.readFileSync(rawFile)).digest('hex')});
}
const groups=[];for(const devices of[12,60,180])for(const shards of[1,2]){const rr=rows.filter(r=>r.category==='normal'&&r.devices===devices&&r.shards===shards);assert.equal(rr.length,2);groups.push({devices,shards,repetitions:2,offered:rr.reduce((a,r)=>a+r.offered,0),applied:rr.reduce((a,r)=>a+r.applied,0),p95MinMs:Math.min(...rr.map(r=>r.p95Ms)),p95MaxMs:Math.max(...rr.map(r=>r.p95Ms)),maxLatencyMs:Math.max(...rr.map(r=>r.maxMs)),allInWindowProven:rr.every(r=>r.allAppliedWithinOfferWindowProven),finalBacklog:rr.reduce((a,r)=>a+r.finalBacklog,0)});}
const summary={verifiedAt:new Date().toISOString(),measuredMainSourceCommit:'f3618c947fc56e5b003849499630ebaa5d5969eb',container:JSON.parse(fs.readFileSync(path.join(root,'evidence/environment/container.json'))),normalGroups:groups,
 allNormalShortRunsMeetTargets:rows.filter(r=>r.category==='normal').every(r=>r.allAppliedWithinOfferWindowProven&&r.maxMs<=2000),
 allTested1HzRunsMeetLatencyTarget:rows.filter(r=>r.intervalMs===1000).every(r=>r.maxMs<=2000),
 allRunsEventuallyAccounted:true,rows,
 measurementCaveat:'Original runner field names appliedAtOfferEnd/backlogAtOfferEnd actually refer to the first post-offer API response. Those fields are not used as exact-deadline evidence here. Short-run completion is conservatively established from latest raw timestamp plus maximum matched latency. Long-run observations and late acknowledgements are disclosed.',
 scope:'Synthetic local traffic and actuators. Two repetitions per short setting; one accelerated run per size and one soak. No AWS auto-scaling or production-capacity claim.'};
fs.writeFileSync(path.join(root,'evidence/campaign-summary.json'),JSON.stringify(summary,null,2)+'\n');const keys=Object.keys(rows[0]);fs.writeFileSync(path.join(root,'evidence/campaign-summary.csv'),keys.join(',')+'\n'+rows.map(r=>keys.map(k=>r[k]).join(',')).join('\n')+'\n');console.log(JSON.stringify({normalGroups:groups,shortTargets:summary.allNormalShortRunsMeetTargets,overall1HzLatencyTarget:summary.allTested1HzRunsMeetLatencyTarget,eventuallyAccounted:true},null,2));
