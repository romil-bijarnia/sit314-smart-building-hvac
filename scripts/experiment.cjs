'use strict';
const fs=require('node:fs'),path=require('node:path');
const {connect,publish}=require('../src/bus.cjs');
const root=path.resolve(__dirname,'..');
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
async function snapshot(runId,pipeline='main'){const r=await fetch(`${process.env.STATUS_BASE_URL||'http://127.0.0.1:3140'}/api/status?run=${encodeURIComponent(runId)}&pipeline=${pipeline}`);if(!r.ok)throw Error('Evidence API HTTP '+r.status);return r.json();}
function makeEvent(runId,deviceIndex,tick,{zones=4,scenario='wave',timestamp=Date.now()}={}){
 const deviceId='sensor-'+String(deviceIndex+1).padStart(4,'0'),zoneId='zone-'+(deviceIndex%zones+1);
 const cycle=Math.sin((tick+1+deviceIndex+1)/8),occupancy=Math.max(0,Math.round(8+6*Math.sin((tick+1+deviceIndex+1)/5)));
 const noise=((Math.sin((deviceIndex+1)*1000+(tick+1)*77)*10000)%1+1)%1*.8-.4;
 const round=x=>Math.round(x*100)/100;
 let payload={buildingId:'building-a',deviceId,zoneId,room:'F1-R'+String(deviceIndex%zones+1).padStart(2,'0'),board:'simulated ESP32',sequence:tick,
  temperatureC:round(22.5+cycle*3+occupancy*.11+noise),humidityPercent:round(48+cycle*8+noise),occupancyCount:occupancy,co2Ppm:Math.round(420+occupancy*38+noise*20),batteryPercent:round(Math.max(30,100-(tick+1)*.01))};
 if(scenario==='modes'){const cases=[{temperatureC:26,co2Ppm:1200,occupancyCount:6},{temperatureC:22,co2Ppm:1200,occupancyCount:6},{temperatureC:26,co2Ppm:1200,occupancyCount:0},{temperatureC:25,co2Ppm:700,occupancyCount:6}];payload={...payload,...cases[deviceIndex%4]};}
 if(scenario==='co2')payload={...payload,temperatureC:22,co2Ppm:1200,occupancyCount:6};
 const id=`${runId}:${deviceId}:${tick}`;
 return {schemaVersion:1,id,type:'device.telemetry.raw',timestamp:new Date(timestamp).toISOString(),runId,correlationId:id,payload};
}
async function runFleet({devices=12,zones=4,ticks=20,intervalMs=1000,shards=2,pipeline='main',label='load',scenario='wave',duplicate=false,onTick,runId=`${label}-${devices}-${shards}-${Date.now()}`}={}){
 if(![devices,zones,ticks,intervalMs].every(Number.isSafeInteger)||devices<1||devices>500||zones<1||ticks<1||ticks>3600||intervalMs<25)throw Error('Invalid experiment bounds');
 const dir=path.join(root,'evidence/runs',runId);fs.mkdirSync(dir,{recursive:true});const clients=[];const sourceLines=[];let publishCount=0;const lateness=[];
 const setupStart=Date.now();
 try{
  for(let base=0;base<devices;base+=20)clients.push(...await Promise.all(Array.from({length:Math.min(20,devices-base)},(_,j)=>connect('sensor-'+String(base+j+1).padStart(4,'0')))));
  const startedAt=Date.now();
  for(let tick=0;tick<ticks;tick++){
   await sleep(Math.max(0,startedAt+tick*intervalMs-Date.now()));lateness.push(Date.now()-(startedAt+tick*intervalMs));
   if(onTick)await onTick(tick);
   await Promise.all(clients.map(async(c,i)=>{const e=makeEvent(runId,i,tick,{zones,scenario});sourceLines.push(JSON.stringify(e));await publish(c,e,pipeline);publishCount++;if(duplicate&&tick===0){await publish(c,e,pipeline);publishCount++;}}));
  }
  await sleep(Math.max(0,startedAt+ticks*intervalMs-Date.now()));const offeringEndedAt=Date.now();const atEnd=await snapshot(runId,pipeline);let final=atEnd;
  const expected=devices*ticks;const deadline=Date.now()+60000;
  while((final.counts['actuator.command.applied']||0)<expected&&Date.now()<deadline){await sleep(250);final=await snapshot(runId,pipeline);}
  const result={runId,observedAt:new Date().toISOString(),config:{devices,zones,ticks,intervalMs,shards,pipeline,scenario,duplicate},setupMs:startedAt-setupStart,
   startedAt:new Date(startedAt).toISOString(),offeringEndedAt:new Date(offeringEndedAt).toISOString(),offeringDurationMs:offeringEndedAt-startedAt,
   nominalRawPerSecond:devices*1000/intervalMs,uniqueRawOffered:expected,mqttPublishes:publishCount,maximumSchedulingLatenessMs:Math.max(...lateness),
   appliedAtFirstPostOfferObservation:atEnd.counts['actuator.command.applied']||0,backlogAtFirstPostOfferObservation:expected-(atEnd.counts['actuator.command.applied']||0),
   completionFractionAtFirstPostOfferObservation:(atEnd.counts['actuator.command.applied']||0)/expected,firstPostOfferObservationAt:atEnd.observedAt,
   drainMs:Date.now()-offeringEndedAt,
   finalBacklog:expected-(final.counts['actuator.command.applied']||0),actualRawPerSecond:expected*1000/(offeringEndedAt-startedAt),
   final,transport:{protocol:'MQTT5',qos:1,tlsVerified:true,perDeviceCertificate:true},clock:'Host and local VM wall clocks; no cross-region clock synchronization claim'};
  fs.writeFileSync(path.join(dir,'raw-sent.jsonl'),sourceLines.join('\n')+'\n');fs.writeFileSync(path.join(dir,'summary.json'),JSON.stringify(result,null,2)+'\n');
  console.log(JSON.stringify({runId,devices,shards,pipeline,offered:expected,applied:final.counts['actuator.command.applied']||0,backlog:result.finalBacklog,p95Ms:final.latencyMs.p95,maxMs:final.latencyMs.max,lateMs:result.maximumSchedulingLatenessMs}));return result;
 }finally{await Promise.all(clients.map(c=>c.endAsync()));}
}
if(require.main===module){const args={};for(let i=2;i<process.argv.length;i+=2){const key=process.argv[i].replace(/^--/,'');const value=process.argv[i+1];const names={devices:'devices',zones:'zones',ticks:'ticks',interval:'intervalMs',shards:'shards',pipeline:'pipeline',label:'label',scenario:'scenario'};if(!names[key]||value===undefined)throw Error('Unknown/missing option '+key);args[names[key]]=['pipeline','label','scenario'].includes(key)?value:Number(value);}runFleet(args).catch(e=>{console.error(e);process.exitCode=1;});}
module.exports={runFleet,makeEvent,snapshot,sleep};
