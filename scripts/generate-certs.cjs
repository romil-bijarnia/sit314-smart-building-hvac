'use strict';
const fs=require('node:fs'),path=require('node:path'),{execFileSync}=require('node:child_process');
const root=path.resolve(__dirname,'..');const dir=path.join(root,'.private/certs');fs.mkdirSync(dir,{recursive:true,mode:0o700});
const max=Number(process.argv[2]||180);if(!Number.isInteger(max)||max<1||max>500)throw Error('device count must be1..500');
const openssl=args=>execFileSync('openssl',args,{stdio:['ignore','ignore','pipe']});
if(!fs.existsSync(path.join(dir,'ca.key'))){openssl(['ecparam','-genkey','-name','prime256v1','-out',path.join(dir,'ca.key')]);openssl(['req','-new','-x509','-sha256','-days','30','-key',path.join(dir,'ca.key'),'-out',path.join(dir,'ca.crt'),'-subj','/CN=SIT314 Local Experiment CA']);}
const roles=['gateway','aggregator-a','aggregator-b','controller','actuator','storage','cep','observer','node-red'];
const ids=[...roles,...Array.from({length:max},(_,i)=>'sensor-'+String(i+1).padStart(4,'0'))];
for(const name of ['mosquitto',...ids]){
 if(fs.existsSync(path.join(dir,name+'.crt')))continue;
 openssl(['ecparam','-genkey','-name','prime256v1','-out',path.join(dir,name+'.key')]);
 openssl(['req','-new','-key',path.join(dir,name+'.key'),'-out',path.join(dir,name+'.csr'),'-subj','/CN='+name]);
 const ext=path.join(dir,name+'.ext');fs.writeFileSync(ext,'basicConstraints=CA:FALSE\nkeyUsage=digitalSignature\nextendedKeyUsage='+(name==='mosquitto'?'serverAuth\nsubjectAltName=DNS:mosquitto,DNS:localhost,IP:127.0.0.1':'clientAuth')+'\n');
 openssl(['x509','-req','-in',path.join(dir,name+'.csr'),'-CA',path.join(dir,'ca.crt'),'-CAkey',path.join(dir,'ca.key'),'-CAcreateserial','-out',path.join(dir,name+'.crt'),'-days','14','-sha256','-extfile',ext]);
 fs.chmodSync(path.join(dir,name+'.key'),0o644);
}
fs.chmodSync(path.join(dir,'ca.key'),0o600);
const permissions={gateway:['read hvac/main/+/raw/+','write hvac/main/+/reading/+','write hvac/main/+/rejected/+'],
 'aggregator-a':['read hvac/main/+/reading/+','write hvac/main/+/metrics/+'],'aggregator-b':['read hvac/main/+/reading/+','write hvac/main/+/metrics/+'],
 controller:['read hvac/main/+/metrics/+','write hvac/main/+/command/+'],actuator:['read hvac/+/+/command/+','write hvac/+/+/applied/+'],
 storage:['read hvac/#'],cep:['read hvac/main/+/reading/+','write hvac/main/+/alert/+'],observer:['read hvac/#'],
 'node-red':['read hvac/flow/+/raw/+','write hvac/flow/+/reading/+','write hvac/flow/+/metrics/+','write hvac/flow/+/command/+','write hvac/flow/+/rejected/+']};
for(const name of ids.filter(x=>x.startsWith('sensor-')))permissions[name]=['write hvac/+/+/raw/'+name];
fs.writeFileSync(path.join(root,'infra/acl.conf'),Object.entries(permissions).map(([name,ps])=>'user '+name+'\n'+ps.map(p=>'topic '+p).join('\n')).join('\n\n')+'\n');
console.log(JSON.stringify({generatedAt:new Date().toISOString(),clientIdentities:ids.length,devices:max,privateDirectory:'.private/certs',caInstalledInSystemTrust:false,certificateLifetimeDays:14}));
