from pathlib import Path
import json,hashlib,math
ROOT=Path(__file__).parent
r=ROOT/'scaleout-results'
campaign=json.loads((r/'campaign.json').read_text())
rows=[]
for t in campaign['trials']:
 d=r/t['id']
 raw=[json.loads(x) for x in (d/'raw-sent.jsonl').read_text().splitlines() if x]
 acks=[json.loads(x) for x in (d/'actuator-acknowledgements.jsonl').read_text().splitlines() if x]
 from datetime import datetime
 epoch=lambda x: round(datetime.fromisoformat(x.replace('Z','+00:00')).timestamp()*1000)
 by_id={e['id']:e for e in raw}
 by_ack={a['event']['correlationId']:a['event'] for a in acks if a['event']['runId']==t['runId']}
 assert len(raw)==len(by_id)==t['offered']==t['workload']['devices']*t['workload']['ticks']
 assert set(by_id)==set(by_ack)
 assert hashlib.sha256((d/'raw-sent.jsonl').read_bytes()).hexdigest()==t['rawSHA256']
 assert hashlib.sha256((d/'actuator-acknowledgements.jsonl').read_bytes()).hexdigest()==t['acknowledgementsSHA256']
 lat=sorted(epoch(by_ack[i]['payload']['appliedAt'])-epoch(e['timestamp']) for i,e in by_id.items())
 pct=lambda q:lat[math.ceil(len(lat)*q)-1]
 assert lat[0]>=0
 assert (pct(.5),pct(.95),pct(.99),max(lat))==(t['p50Ms'],t['p95Ms'],t['p99Ms'],t['maxMs'])
 summary=json.loads((d/'summary.json').read_text())['experiment']
 in_window=sum(epoch(a['payload']['appliedAt'])<=epoch(summary['offeringEndedAt']) for a in by_ack.values())
 assert in_window==t['inWindow']
 assert t['aggregatorInboundPublishes']==t['offered']
 assert summary['finalBacklog']==0
 for typ in ('device.telemetry.raw','sensor.reading','zone.metrics.updated','actuator.command.created','actuator.command.applied'):
  assert summary['final']['counts'][typ]==t['offered']
 res=json.loads((d/'resource-samples.json').read_text())
 for phase in ('before','after'):
  assert all(x['health']['errors']==0 for x in res[phase].values())
 assert all(x['health']['pending']==0 for x in res['after'].values())
 for e in by_ack.values():
  assert e['payload']['simulated'] is True
  assert e['payload']['mode'] in ('cool','idle','ventilate')
  assert e['payload']['sourceTimestamp']==by_id[e['correlationId']]['timestamp']
 rows.append(t)
comparisons=[]
for rep in (0,1):
 for workload in ('normal180',):
  pair={t['mode']:t for t in rows if t['repetition']==rep and t['workload']['label']==workload}
  if len(pair)!=2:continue
  a,b=pair['single'],pair['scaled']
  # Sensor values are identical, except for source-time/run identity.
  def payloads(t):
   return {(e['payload']['deviceId'],e['payload']['sequence']):e['payload'] for e in [json.loads(x) for x in (r/t['id']/'raw-sent.jsonl').read_text().splitlines()]}
  assert payloads(a)==payloads(b)
  q={'repetition':rep,'workload':workload}
  for metric in ('p95Ms','maxMs','aggregatorInboundPublishes','aggregatorInboundPayloadBytes','aggregatorLedgerBytes','aggregatorCpuMicroseconds','aggregatorRssAfterBytes'):
   q[metric]={'singleOwner':a[metric],'twoOwners':b[metric],'reductionPercent':100*(a[metric]-b[metric])/a[metric]}
  comparisons.append(q)
result={'verifiedAt':datetime.now().astimezone().isoformat(),'trialCount':len(rows),'complete':len(rows)==4 and 'completedAt' in campaign and not campaign['failures'],'allInputOutputIdsMatched':True,'allRawHashesMatch':True,'allAcknowledgementHashesMatch':True,'pairedSensorInputsIdentical':True,'allReportedLatencyPercentilesRecomputed':True,'allStageCountsBalanced':True,'allFinalStageLedgersDrained':True,'comparisons':comparisons}
(ROOT/'verified-scaleout.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2))
if not result['complete']:raise SystemExit('Campaign not yet complete')
