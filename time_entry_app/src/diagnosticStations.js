const stationKey=value=>String(value??'').trim().replace(/\s+/g,' ').replace(/\s*-\s*/g,'-').toLowerCase();
const unassigned='__unassigned__';
const centralDate=new Intl.DateTimeFormat('en-US',{timeZone:'America/Chicago',year:'numeric',month:'2-digit',day:'2-digit'});
const centralTime=new Intl.DateTimeFormat('en-US',{timeZone:'America/Chicago',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit',timeZoneName:'short'});
export function diagnosticDay(value) {
 const date=new Date(value);if(!value||!Number.isFinite(date.getTime()))return '';
 const parts=Object.fromEntries(centralDate.formatToParts(date).map(part=>[part.type,part.value]));
 return `${parts.year}-${parts.month}-${parts.day}`;
}
export function diagnosticTime(value) {
 const date=new Date(value);return value&&Number.isFinite(date.getTime())?centralTime.format(date):String(value??'');
}
export function diagnosticDateRange(from='',to='') {
 const valid=value=>!value||(/^\d{4}-\d{2}-\d{2}$/.test(value)&&Number.isFinite(Date.parse(value+'T00:00:00Z'))&&new Date(value+'T00:00:00Z').toISOString().slice(0,10)===value);
 const error=!valid(from)||!valid(to)?'Enter valid dates.':from&&to&&from>to?'From date must be on or before To date.':'';
 return {from,to,error};
}
export function diagnosticStationView(events, machines=[], selected='', filter='', dates={}) {
 const range=diagnosticDateRange(String(dates.from||''),String(dates.to||''));
 const search=String(filter).toLowerCase();
 let firstDay='',lastDay='';
 const dated=events.map(event=>{
  const day=diagnosticDay(event.received_at);
  if(day){if(!firstDay||day<firstDay)firstDay=day;if(!lastDay||day>lastDay)lastDay=day;}
  return {event,day};
 });
 const matching=range.error?[]:dated.filter(({event,day})=>(!(range.from||range.to)||(day&&(!range.from||day>=range.from)&&(!range.to||day<=range.to)))&&(!search||JSON.stringify(event).toLowerCase().includes(search))).map(({event})=>event);
 const counts=new Map();for(const event of matching){const key=stationKey(event.station)||unassigned;counts.set(key,(counts.get(key)||0)+1);}
 const options=new Map();
 for(const machine of machines){
  const key=stationKey(machine.machine_no);if(!key)continue;
  options.set(key,{key,label:String(machine.label||machine.machine_no),count:0});
 }
 for(const event of events){
  const key=stationKey(event.station)||unassigned;
  if(!options.has(key))options.set(key,{key,label:key===unassigned?'Unassigned':String(event.station),count:0});

 }
 for(const option of options.values())option.count=counts.get(option.key)||0;
 const selectedKey=stationKey(selected);
 if(selectedKey&&!options.has(selectedKey))options.set(selectedKey,{key:selectedKey,label:String(selected),count:0});
 const stations=[...options.values()].sort((a,b)=>a.key===unassigned?1:b.key===unassigned?-1:a.label.localeCompare(b.label,undefined,{numeric:true,sensitivity:'base'}));
 const stationEvents=selectedKey?matching.filter(event=>(stationKey(event.station)||unassigned)===selectedKey):matching;
 return {selected:selectedKey,stations,range,firstDay,lastDay,matchingCount:matching.length,rows:stationEvents.slice(-500).reverse()};
}
export function diagnosticStationHref(station,filter='',dates={}) {
 const query=new URLSearchParams();if(station)query.set('station',station);if(filter)query.set('filter',filter);if(dates.from)query.set('from',dates.from);if(dates.to)query.set('to',dates.to);
 return '/admin/click-diagnostics'+(query.size?'?'+query.toString():'');
}
