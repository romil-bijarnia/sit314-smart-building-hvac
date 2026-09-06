'use strict';
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
class StageLedger {
 constructor(directory){this.path=path.join(directory,'inbox-outbox.jsonl');this.inputs=new Map();this.prepared=new Map();this.sent=new Set();this.pendingKeys=new Set();this.records=[];this.nextExpiryAt=Infinity;
  if(fs.existsSync(this.path)){const text=fs.readFileSync(this.path,'utf8');const lines=text.split('\n');let valid='';for(let i=0;i<lines.length;i++){if(!lines[i])continue;let r;try{r=JSON.parse(lines[i]);}catch(e){if(i!==lines.length-1)throw e;fs.writeFileSync(this.path+'.truncated-'+Date.now(),lines[i]);fs.writeFileSync(this.path,valid);break;}valid+=lines[i]+'\n';this.apply(r);}if(text && !text.endsWith('\n') && valid.trimEnd().endsWith('}')){const current=fs.readFileSync(this.path,'utf8');if(current&&!current.endsWith('\n'))fs.appendFileSync(this.path,'\n');}}
 }
 apply(r){this.records.push(r);if(r.op==='input'){this.inputs.set(r.key,r);this.pendingKeys.add(r.key);this.nextExpiryAt=Math.min(this.nextExpiryAt,r.receivedAt+30*86400000);}if(r.op==='prepared')this.prepared.set(r.key,r);if(r.op==='sent'){this.sent.add(r.key);this.pendingKeys.delete(r.key);}}
 append(r){const fd=fs.openSync(this.path,'a');try{fs.writeSync(fd,JSON.stringify(r)+'\n');fs.fsyncSync(fd);}finally{fs.closeSync(fd);}this.apply(r);}
 receive(topic,buffer){let event;try{event=JSON.parse(buffer.toString());}catch{event=null;}
  const id=typeof event?.id==='string'?event.id:crypto.createHash('sha256').update(buffer).digest('hex');const a=topic.split('/');const key=[a[1],a[2],id].join('|');
  if(this.inputs.has(key))return false;
  this.append({op:'input',key,topic,event,receivedAt:Date.now(),rawHash:crypto.createHash('sha256').update(buffer).digest('hex')});return true;
 }
 prepare(input,outputs){this.append({op:'prepared',key:input.key,outputs,at:Date.now()});}
 delivered(key){this.append({op:'sent',key,at:Date.now()});}
 pending(){return [...this.pendingKeys].map(key=>this.inputs.get(key));}
 compact(now=Date.now()){if(now<this.nextExpiryAt)return;this.nextExpiryAt=now+60000;const cutoff=now-30*86400000;const keep=new Set([...this.inputs.values()].filter(i=>i.receivedAt>=cutoff||!this.sent.has(i.key)).map(i=>i.key));const records=this.records.filter(r=>keep.has(r.key));if(records.length===this.records.length)return;fs.writeFileSync(this.path+'.tmp',records.map(x=>JSON.stringify(x)).join('\n')+'\n');fs.renameSync(this.path+'.tmp',this.path);for(const k of this.inputs.keys())if(!keep.has(k)){this.inputs.delete(k);this.prepared.delete(k);this.sent.delete(k);}this.records=records;}
}
module.exports={StageLedger};
