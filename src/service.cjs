'use strict';
const fs=require('node:fs'),path=require('node:path'),http=require('node:http');
const {connect,publish,decodeTopic}=require('./bus.cjs');
const {validateRaw,deriveEvent,ZoneAggregator,decideCommand,ownsZone,CepEngine}=require('./domain.cjs');
const {EventStore}=require('./store.cjs');const {StageLedger}=require('./ledger.cjs');
const role=process.env.ROLE||'gateway',identity=process.env.IDENTITY||role;
const dataDir=process.env.DATA_DIR||path.join(__dirname,'../runtime',identity);fs.mkdirSync(dataDir,{recursive:true});
const agg=new ZoneAggregator(10);const cep=new CepEngine({sustainedMs:Number(process.env.SUSTAINED_MS||10000),silentMs:Number(process.env.SILENT_MS||5000)});
const store=role==='storage'?new EventStore(dataDir):null;const ledger=store?null:new StageLedger(dataDir);
let client,server,processing=false,stopping=false,errors=0,processed=0;let retryTimer,sweepTimer,maintenanceTimer;
const shardIndex=Number(process.env.SHARD_INDEX||0),shardCount=Number(process.env.SHARD_COUNT||1);
const log=(message,extra={})=>process.stdout.write(JSON.stringify({at:new Date().toISOString(),role,identity,message,...extra})+'\n');
function alertEvent(alert){return {schemaVersion:1,id:`${alert.runId}:${alert.deviceId}:${alert.kind}:${alert.sourceEventId}`,type:'cep.alert.created',timestamp:new Date().toISOString(),runId:alert.runId,correlationId:alert.correlationId,sourceEventId:alert.sourceEventId,payload:alert};}
function transform(input){const info=decodeTopic(input.topic),event=input.event;if(!info)return[];
 if(role==='gateway'){
  const reason=validateRaw(event)||(event.runId!==info.runId?'runId/topic mismatch':null)||(event.payload.deviceId!==info.owner?'device identity/topic mismatch':null);
  if(reason){const root=typeof event?.id==='string'&&/^[a-zA-Z0-9_.:-]{1,128}$/.test(event.id)?event.id:input.rawHash;
   return[{pipeline:info.pipeline,event:{schemaVersion:1,id:root+':rejected',type:'gateway.telemetry.rejected',timestamp:new Date().toISOString(),runId:info.runId,correlationId:root,sourceEventId:root,payload:{deviceId:info.owner,zoneId:event?.payload?.zoneId||'unknown',reason,rejectedBy:identity}}}];}
  return[{pipeline:info.pipeline,event:deriveEvent(event,'sensor.reading','reading',{...event.payload,receivedAt:new Date(input.receivedAt).toISOString(),normalizedBy:identity,sourceTimestamp:event.timestamp})}];
 }
 if(role==='aggregator') {if(!ownsZone(event.payload.zoneId,shardIndex,shardCount))return[];const metrics=agg.update(event,identity);return metrics?[{pipeline:info.pipeline,event:deriveEvent(event,'zone.metrics.updated','metrics',metrics)}]:[];}
 if(role==='controller')return[{pipeline:info.pipeline,event:deriveEvent(event,'actuator.command.created','command',{...decideCommand(event.payload,22),sourceTimestamp:event.payload.sourceTimestamp})}];
 if(role==='actuator')return[{pipeline:info.pipeline,event:deriveEvent(event,'actuator.command.applied','applied',{...event.payload,sourceCommandId:event.id,appliedAt:new Date().toISOString(),simulated:true})}];
 if(role==='cep')return cep.reading(event,input.receivedAt).map(a=>({pipeline:info.pipeline,event:alertEvent(a)}));
 return[];
}
// Reconstruct state from committed transformations, not from arbitrary wall-clock checkpoints.
if(ledger)for(const r of ledger.records){if(r.op!=='prepared')continue;const i=ledger.inputs.get(r.key);if(!i?.event)continue;
 if(role==='aggregator'&&ownsZone(i.event.payload.zoneId,shardIndex,shardCount))agg.update(i.event,identity);
 if(role==='cep'&&i.event.type==='sensor.reading')cep.reading(i.event,i.receivedAt);
}
async function drain(){if(processing||!client?.connected)return;processing=true;let failed=false;try{for(const input of ledger.pending()){
 let prepared=ledger.prepared.get(input.key);if(!prepared){const outputs=transform(input);try{ledger.prepare(input,outputs);}catch(err){log('fatal_outbox_commit',{error:err.message});process.exit(1);}prepared=ledger.prepared.get(input.key);}
 for(const item of prepared.outputs)await publish(client,item.event,item.pipeline);
 ledger.delivered(input.key);processed++;
}}catch(e){failed=true;errors++;log('dispatcher_retry',{error:e.message});}finally{processing=false;if(!failed&&ledger.pending().length&&!stopping)setImmediate(drain);}}
function receivePacket(packet,done){try{const info=decodeTopic(packet.topic);if(info){if(store){let e;try{e=JSON.parse(packet.payload.toString());}catch{fs.appendFileSync(path.join(dataDir,'dead-letter.jsonl'),JSON.stringify({timestamp:new Date().toISOString(),topic:packet.topic,raw:packet.payload.toString(),reason:'malformed JSON'})+'\n');done();return;}try{store.ingest(e,info.pipeline);processed++;}catch(err){if(!(err instanceof TypeError)||store.faulted)throw err;fs.appendFileSync(path.join(dataDir,'dead-letter.jsonl'),JSON.stringify({timestamp:new Date().toISOString(),topic:packet.topic,raw:packet.payload.toString(),reason:err.message})+'\n');errors++;log('quarantined_invalid_envelope',{reason:err.message});}}else ledger.receive(packet.topic,packet.payload);}done();if(ledger)setImmediate(drain);}catch(e){errors++;log('fatal_inbox_commit',{error:e.message});done(e);setImmediate(()=>process.exit(1));}}
async function main(){client=await connect(identity,{},c=>{c.handleMessage=receivePacket;});
 const filters={gateway:['hvac/main/+/raw/+'],aggregator:['hvac/main/+/reading/+'],controller:['hvac/main/+/metrics/+'],actuator:['hvac/+/+/command/+'],storage:['hvac/#'],cep:['hvac/main/+/reading/+']};if(!filters[role])throw Error('Unknown role');
 // Persist the inbox before MQTT acknowledges delivery. Outgoing PUBACKs are handled outside this callback.

 await client.subscribeAsync(filters[role],{qos:1});if(ledger){await drain();retryTimer=setInterval(drain,250);}
 if(role==='cep')sweepTimer=setInterval(()=>{if(processing||stopping)return;for(const a of cep.sweep(Date.now())){const event=alertEvent(a);const topic=`hvac/main/${event.runId}/alert/${a.zoneId}`;const key='main|'+event.runId+'|'+event.id;if(ledger.inputs.has(key))continue;ledger.receive(topic,Buffer.from(JSON.stringify(event)));const input=ledger.inputs.get(key);ledger.prepare(input,[{pipeline:'main',event}]);}drain();},500);
 maintenanceTimer=setInterval(()=>{if(store)store.maintenance();else if(!processing)ledger.compact();},5000);
 server=http.createServer((req,res)=>{if(req.method!=='GET'){res.writeHead(405);res.end();return;}const url=new URL(req.url,'http://localhost');res.setHeader('Cache-Control','no-store');res.setHeader('X-Content-Type-Options','nosniff');
  if(url.pathname==='/health'){res.setHeader('Content-Type','application/json');res.end(JSON.stringify({role,identity,connected:client.connected,processed,errors,pending:ledger?ledger.pending().length:0}));return;}
  if(store&&url.pathname==='/api/status'){res.setHeader('Content-Type','application/json');res.end(JSON.stringify(store.snapshot(url.searchParams.get('run'),url.searchParams.get('pipeline'))));return;}
  if(store&&url.pathname==='/'){res.setHeader('Content-Type','text/html; charset=utf-8');res.end(fs.readFileSync(path.join(__dirname,'../public/index.html')));return;}res.writeHead(404);res.end();
 });server.listen(Number(process.env.PORT||8080),'0.0.0.0');log('ready',{transport:'mutual TLS',qos:1,persistentSession:true,shardIndex,shardCount});
 process.once('SIGTERM',async()=>{stopping=true;clearInterval(retryTimer);clearInterval(sweepTimer);clearInterval(maintenanceTimer);while(processing)await new Promise(r=>setTimeout(r,20));if(store)store.maintenance();await client.endAsync();server.close(()=>process.exit(0));});
}
main().catch(e=>{log('fatal',{error:e.message});process.exit(1);});
