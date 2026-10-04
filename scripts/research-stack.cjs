'use strict';
const fs=require('node:fs'),path=require('node:path'),{execFileSync}=require('node:child_process');
const root=path.resolve(__dirname,'..');const engine=process.env.CONTAINER_ENGINE||'podman';
const prefix='sit314-eval-';const network=prefix+'edge';const ingress=prefix+'ingress';const cert=path.join(root,'.private/certs');
const command=process.argv[2]||'up';const shards=Number(process.argv[3]||2);
if(![1,2].includes(shards))throw Error('Supported shard counts: 1 or 2');
const run=(args,quiet=false)=>execFileSync(engine,args,{cwd:root,encoding:'utf8',stdio:quiet?['ignore','pipe','pipe']:'inherit'});
const roles=[['gateway','gateway'],['aggregator-a','aggregator'],['aggregator-b','aggregator'],['controller','controller'],['actuator','actuator'],['cep','cep'],['storage','storage']];
function exists(name){try{run(['container','inspect',name],true);return true;}catch{return false;}}
function volume(name){try{run(['volume','inspect',name],true);}catch{run(['volume','create',name],true);}}
function remove(name){if(exists(name))run(['rm','-f',name]);}
function mounts(identity){return ['ca.crt',identity+'.crt',identity+'.key'].flatMap(file=>['-v',`${cert}/${file}:/certs/${file}:ro`]);}
if(command==='clean'){for(const [id] of roles)remove(prefix+id);remove(prefix+'node-red');remove(prefix+'mosquitto');for(const name of ['broker-data',...roles.map(([id])=>id+'-data')]){try{run(['volume','rm',prefix+name],true);}catch{}}process.exit(0);}
if(command==='down') {for(const [id] of roles)remove(prefix+id);remove(prefix+'node-red');remove(prefix+'mosquitto');process.exit(0);}
if(command==='build'){run(['build',...(engine==='podman'?['--format','docker']:[]),'-t',prefix+'service:0.8.0','.']);run(['build',...(engine==='podman'?['--format','docker']:[]),'-f','node-red/Dockerfile','-t',prefix+'node-red:0.8.0','.']);process.exit(0);}
if(command==='status'){run(['ps','--filter','name='+prefix,'--format','{{.Names}}\t{{.Status}}\t{{.Image}}']);process.exit(0);}
if(!['up','shards'].includes(command))throw Error('Use build/up/shards/status/down');
if(command==='up') {
  try{run(['network','inspect',network],true);}catch{run(['network','create','--internal',network]);}
  try{run(['network','inspect',ingress],true);}catch{run(['network','create',ingress]);}
  remove(prefix+'mosquitto');volume(prefix+'broker-data');
  run(['run','-d','--name',prefix+'mosquitto','--network',ingress,'-p','127.0.0.1:18883:8883',
    '-v',root+'/infra/mosquitto.conf:/mosquitto/config/mosquitto.conf:ro','-v',root+'/infra/acl.conf:/mosquitto/config/acl.conf:ro',
    ...['ca.crt','mosquitto.crt','mosquitto.key'].flatMap(f=>['-v',`${cert}/${f}:/mosquitto/certs/${f}:ro`]),
    '-v',prefix+'broker-data:/mosquitto/data','docker.io/eclipse-mosquitto:2.0.22']);
  run(['network','connect','--alias','mosquitto',network,prefix+'mosquitto']);
}
if(command==='shards'){
  execFileSync(process.execPath,['-e',"fetch('http://127.0.0.1:13140/api/status?pipeline=main').then(r=>r.json()).then(s=>{if((s.counts['sensor.reading']||0)!==(s.counts['actuator.command.applied']||0))throw Error('Refusing topology change: accepted readings still await actuator acknowledgements');}).catch(e=>{console.error(e.message);process.exit(1)});"],{cwd:root,stdio:'inherit'});
  for(const id of ['aggregator-a','aggregator-b'])remove(prefix+id);
  // Static topology changes are performed only between fully drained experiments.
  // Expire old owners' subscriber sessions so historical duplicates do not enter the new epoch.
  execFileSync(process.execPath,['-e',"const {connect}=require('./src/bus.cjs');(async()=>{for(const id of ['aggregator-a','aggregator-b']){const c=await connect(id,{clean:true,properties:{sessionExpiryInterval:0}});await c.endAsync();}})().catch(e=>{console.error(e);process.exit(1)});"],{cwd:root,stdio:'inherit'});
}
for(const [id,role] of roles) {
  if(command==='shards'&&role!=='aggregator')continue;
  remove(prefix+id);if(id==='aggregator-b'&&shards===1)continue;
  volume(prefix+id+'-data');
  const extra=role==='aggregator'?['-e','SHARD_INDEX='+(id.endsWith('a')?0:1),'-e','SHARD_COUNT='+shards,'-e','AGGREGATION_ROUTING='+(process.env.AGGREGATION_ROUTING||'broadcast'),'-e','ZONE_COUNT='+(process.env.ZONE_COUNT||12)]:[];
  if(role==='storage')extra.push('-p','127.0.0.1:13140:8080');
  run(['run','-d','--name',prefix+id,'--network',role==='storage'?ingress:network,'--cap-drop','ALL','--security-opt','no-new-privileges',
    '-e','ROLE='+role,'-e','IDENTITY='+id,'-e','MQTT_URL=mqtts://mosquitto:8883','-e','CERT_DIR=/certs','-e','DATA_DIR=/data',
    ...extra,...mounts(id),'-v',prefix+id+'-data:/data',prefix+'service:0.8.0']);
  if(role==='storage')run(['network','connect',network,prefix+id]);
}
if(command==='up') {
  remove(prefix+'node-red');
  run(['run','-d','--name',prefix+'node-red','--network',ingress,'--cap-drop','ALL','--security-opt','no-new-privileges',
    '-e','MQTT_HOST=mosquitto','-p','127.0.0.1:13180:1880',...mounts('node-red'),prefix+'node-red:0.8.0']);
  run(['network','connect',network,prefix+'node-red']);
  execFileSync(process.execPath,['-e',"(async()=>{for(let i=0;i<60;i++){try{const a=await(await fetch('http://127.0.0.1:13140/health')).json();const b=await(await fetch('http://127.0.0.1:13180/api/status')).json();if(a.connected&&b.broker.connected)return;}catch{}await new Promise(r=>setTimeout(r,500));}throw Error('Stack readiness timeout');})().catch(e=>{console.error(e);process.exit(1)});"],{stdio:'inherit'});
}
console.log(JSON.stringify({network,ingress,controlNetworkInternal:true,loopbackPortsOnly:true,aggregatorInstances:shards,dashboard:'http://localhost:13140',nodeRed:'http://localhost:13180/dashboard'}));
