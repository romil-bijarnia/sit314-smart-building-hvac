'use strict';
const fs = require('node:fs');
const path = require('node:path');
const mqtt = require('mqtt');

const stages = {
  'device.telemetry.raw':'raw', 'sensor.reading':'reading',
  'zone.metrics.updated':'metrics', 'actuator.command.created':'command',
  'actuator.command.applied':'applied', 'gateway.telemetry.rejected':'rejected',
  'cep.alert.created':'alert'
};
function topicFor(event, pipeline='main') {
  const stage = stages[event.type];
  if (!stage) throw new Error('Unknown event type: '+event.type);
  const owner = ['raw','rejected'].includes(stage) ? event.payload.deviceId : event.payload.zoneId;
  if (![pipeline,event.runId,owner].every(x=>typeof x==='string' && /^[a-zA-Z0-9_.:-]+$/.test(x))) throw new Error('Unsafe topic component');
  return `hvac/${pipeline}/${event.runId}/${stage}/${owner}`;
}
async function connect(identity, overrides={}, configure) {
  const certDir = process.env.CERT_DIR || path.join(__dirname,'../.private/certs');
  const client = mqtt.connect(process.env.MQTT_URL || 'mqtts://127.0.0.1:8883',{
    protocolVersion:5, clientId:'sit314-'+identity+(process.env.CLIENT_TAG?'-'+process.env.CLIENT_TAG:''),
    ca:fs.readFileSync(path.join(certDir,'ca.crt')),
    cert:fs.readFileSync(path.join(certDir,identity+'.crt')),
    key:fs.readFileSync(path.join(certDir,identity+'.key')),
    rejectUnauthorized:true, reconnectPeriod:1000,connectTimeout:15000,clean:false,properties:{sessionExpiryInterval:86400},
    ...overrides, manualConnect:true
  });
  client.on('error',err=>process.stderr.write(JSON.stringify({component:identity,error:err.message,code:err.code,causes:err.errors?.map(e=>({code:e.code,address:e.address,port:e.port}))})+'\n'));
  await new Promise((resolve,reject)=>{
    const timer=setTimeout(()=>{client.end(true);reject(new Error('MQTT connection timed out: '+identity));},16000);
    client.once('connect',()=>{if(client.stream.setNoDelay)client.stream.setNoDelay(true);clearTimeout(timer);resolve();});
    if(configure)configure(client);client.connect();
  });
  return client;
}
function publish(client,event,pipeline='main') {
  return client.publishAsync(topicFor(event,pipeline),JSON.stringify(event),{qos:1,retain:false});
}
function decodeTopic(topic) {
  const a=topic.split('/');
  return a.length===5 && a[0]==='hvac' ? {pipeline:a[1],runId:a[2],stage:a[3],owner:a[4]} : null;
}
module.exports={connect,publish,topicFor,decodeTopic,stages};
