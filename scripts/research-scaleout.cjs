'use strict';
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto'),assert=require('node:assert/strict');
const {execFileSync}=require('node:child_process');
const {runFleet,sleep}=require('./experiment.cjs');
const {connect}=require('../src/bus.cjs');
const root=path.resolve(__dirname,'..'),out=path.resolve(root,'research/scaleout-results');fs.mkdirSync(out,{recursive:true});
process.env.MQTT_URL='mqtts://127.0.0.1:18883';process.env.STATUS_BASE_URL='http://127.0.0.1:13140';
const engine=process.env.CONTAINER_ENGINE||'podman';
const roles=['gateway','aggregator-a','aggregator-b','controller','actuator','storage','cep'];
const campaign={startedAt:new Date().toISOString(),protocol:'../scaleout-protocol.md',trials:[],failures:[]};
const write=()=>fs.writeFileSync(path.join(out,'campaign.json'),JSON.stringify(campaign,null,2)+'\n');
function stack(action,env={},log){const buffer=execFileSync(process.execPath,[path.join(__dirname,'research-stack.cjs'),action,String(env.SHARD_COUNT||2)],{cwd:root,env:{...process.env,...env},encoding:'utf8',maxBuffer:8e6});if(log)fs.appendFileSync(log,buffer);return buffer;}
function serviceMeta(id){const script="const fs=require('fs'),path=require('path');function walk(p){return fs.readdirSync(p,{withFileTypes:true}).flatMap(x=>x.isDirectory()?walk(path.join(p,x.name)):[{name:path.join(p,x.name),bytes:fs.statSync(path.join(p,x.name)).size}]);}fetch('http://127.0.0.1:8080/health').then(r=>r.json()).then(health=>console.log(JSON.stringify({health,files:walk('/data')}))).catch(e=>{console.error(e);process.exit(1)});";return JSON.parse(execFileSync(engine,['exec','sit314-eval-'+id,'node','-e',script],{encoding:'utf8',maxBuffer:2e6}));}
const digest=p=>crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex');
const percentile=(a,q)=>a.length?a[Math.min(a.length-1,Math.ceil(a.length*q)-1)]:null;
(async()=>{
 const workloads=[{label:'normal180',devices:180,zones:12,ticks:10,intervalMs:1000}];
 for(let repetition=0;repetition<2;repetition++){
  const ordered=repetition===0?workloads:[...workloads].reverse();
  for(const workload of ordered)for(const mode of (repetition===0?['single','scaled']:['scaled','single'])){
   const shards=mode==='single'?1:2;const aggregateIds=shards===1?['aggregator-a']:['aggregator-a','aggregator-b'];const activeRoles=roles.filter(id=>shards===2||id!=='aggregator-b');
   const id=`r${repetition}-${workload.label}-${mode}`,dir=path.join(out,id);fs.mkdirSync(dir,{recursive:true});const deploymentLog=path.join(dir,'deployment.log');
   console.log('BEGIN '+id);stack('clean',{},deploymentLog);stack('up',{AGGREGATION_ROUTING:mode==='single'?'broadcast':'selective',ZONE_COUNT:String(workload.zones),SHARD_COUNT:String(shards)},deploymentLog);await sleep(2000);
   const before=Object.fromEntries(activeRoles.map(id=>[id,serviceMeta(id)]));
   const acknowledgements=[];const observer=await connect('observer',{clean:true,properties:{sessionExpiryInterval:0},clientId:'sit314-eval-observer'});
   observer.on('message',(topic,payload)=>acknowledgements.push({receivedAt:new Date().toISOString(),topic,event:JSON.parse(payload.toString())}));
   await observer.subscribeAsync('hvac/main/+/applied/+',{qos:1});
   let result;try{result=await runFleet({...workload,shards,label:id,runId:`research-${id}-${Date.now()}`});await sleep(100);}finally{await observer.endAsync();}
   const after=Object.fromEntries(activeRoles.map(id=>[id,serviceMeta(id)]));
   const rawPath=path.join(root,'evidence/runs',result.runId,'raw-sent.jsonl');const inputs=fs.readFileSync(rawPath,'utf8').trim().split('\n').map(JSON.parse);const inputById=new Map(inputs.map(e=>[e.id,e]));
   const unique=new Map(acknowledgements.filter(a=>a.event.runId===result.runId).map(a=>[a.event.correlationId,a]));
   assert.equal(unique.size,result.uniqueRawOffered,'Every offered reading must have a captured applied acknowledgement');assert.equal(result.finalBacklog,0);
   const latencies=[...unique].map(([id,a])=>{assert.ok(inputById.has(id));return Date.parse(a.event.payload.appliedAt)-Date.parse(inputById.get(id).timestamp);}).sort((a,b)=>a-b);
   const end=Date.parse(result.offeringEndedAt),inWindow=[...unique.values()].filter(a=>Date.parse(a.event.payload.appliedAt)<=end).length;
   const aggregate=(key,field)=>aggregateIds.reduce((n,id)=>n+(after[id].health[key][field]-before[id].health[key][field]),0);
   const inbound=aggregateIds.reduce((n,id)=>n+after[id].health.inboundPublishes-before[id].health.inboundPublishes,0);
   const payloadBytes=aggregateIds.reduce((n,id)=>n+after[id].health.inboundPayloadBytes-before[id].health.inboundPayloadBytes,0);
   const ledgerBytes=aggregateIds.reduce((n,id)=>n+after[id].files.reduce((s,f)=>s+f.bytes,0)-before[id].files.reduce((s,f)=>s+f.bytes,0),0);
   assert.equal(inbound,result.uniqueRawOffered,'Observed routing multiplicity must match treatment');
   assert.equal(result.final.latencyMs.max,latencies.at(-1),'Independent actuator captures must match stored latency');
   const record={id,repetition,mode,shards,workload:{...workload},runId:result.runId,offered:result.uniqueRawOffered,applied:unique.size,inWindow,offeredRate:result.actualRawPerSecond,nominalRate:result.nominalRawPerSecond,offeringDurationMs:result.offeringDurationMs,p50Ms:percentile(latencies,.5),p95Ms:percentile(latencies,.95),p99Ms:percentile(latencies,.99),maxMs:latencies.at(-1),aggregatorInboundPublishes:inbound,aggregatorInboundPayloadBytes:payloadBytes,aggregatorLedgerBytes:ledgerBytes,aggregatorCpuMicroseconds:aggregate('cpuUsageMicroseconds','user')+aggregate('cpuUsageMicroseconds','system'),aggregatorRssAfterBytes:aggregateIds.reduce((n,id)=>n+after[id].health.memoryBytes.rss,0),rawSHA256:digest(rawPath),exactEndBacklog:result.uniqueRawOffered-inWindow};
   fs.copyFileSync(rawPath,path.join(dir,'raw-sent.jsonl'));fs.writeFileSync(path.join(dir,'actuator-acknowledgements.jsonl'),acknowledgements.map(a=>JSON.stringify(a)).join('\n')+'\n');fs.writeFileSync(path.join(dir,'resource-samples.json'),JSON.stringify({before,after},null,2)+'\n');fs.writeFileSync(path.join(dir,'summary.json'),JSON.stringify({comparison:record,experiment:result},null,2)+'\n');
   record.acknowledgementsSHA256=digest(path.join(dir,'actuator-acknowledgements.jsonl'));campaign.trials.push(record);write();console.log('RESULT '+JSON.stringify(record));
  }
 }
 campaign.completedAt=new Date().toISOString();write();stack('down');console.log('SCALEOUT_CAMPAIGN_COMPLETE '+campaign.trials.length);
})().catch(e=>{campaign.failures.push({at:new Date().toISOString(),message:e.message,stack:e.stack});write();console.error(e);process.exitCode=1;});
