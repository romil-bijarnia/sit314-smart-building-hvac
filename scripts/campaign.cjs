'use strict';
const fs=require('node:fs'),path=require('node:path'),{execFileSync}=require('node:child_process');
const {runFleet,sleep}=require('./experiment.cjs');const root=path.resolve(__dirname,'..');
const records=[];const write=()=>fs.writeFileSync(path.join(root,'evidence/campaign.json'),JSON.stringify({observedAt:new Date().toISOString(),normalRateHz:1,repetitions:2,records},null,2)+'\n');
(async()=>{
 for(const shards of [1,2]){
  execFileSync(process.execPath,[path.join(root,'scripts/stack.cjs'),'shards',String(shards)],{stdio:'inherit'});await sleep(1500);
  for(let repetition=0;repetition<2;repetition++){
   const sizes=repetition===0?[12,60,180]:[180,60,12];
   for(const devices of sizes){const zones=devices===12?3:devices===60?6:12;const r=await runFleet({devices,zones,ticks:20,intervalMs:1000,shards,label:`normal-r${repetition}`});records.push({category:'normal',repetition,...r});write();}
  }
 }
 // Same sizes/zones/sample interval and nominal six-second window as the submitted in-process baseline.
 for(const [devices,zones] of [[12,3],[60,6],[180,12]]){const r=await runFleet({devices,zones,ticks:60,intervalMs:100,shards:2,label:'legacy-rate'});records.push({category:'legacy-rate',repetition:0,...r});write();}
 const soak=await runFleet({devices:24,zones:4,ticks:60,intervalMs:1000,shards:2,label:'soak'});records.push({category:'soak',repetition:0,...soak});write();
 console.log('CAMPAIGN_COMPLETE '+records.length);
})().catch(e=>{write();console.error(e);process.exitCode=1;});
