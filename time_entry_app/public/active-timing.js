const central=new Intl.DateTimeFormat('en-US',{timeZone:'America/Chicago',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',hourCycle:'h23'});
const parts=date=>Object.fromEntries(central.formatToParts(new Date(date)).map(part=>[part.type,part.value]));
export function centralDateTime(value){
 if(!Number.isFinite(Date.parse(value)))return '';
 const p=parts(value);return `${p.year}-${p.month}-${p.day}T${p.hour}:${p.minute}`;
}
export function correctedStart(value,at){
 if(!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(String(value)))throw new Error('Enter a valid start date and time in Central Time.');
 const base=Date.parse(value+'Z');
 const candidates=[5,6].map(hours=>new Date(base+hours*3600000)).filter(date=>Number.isFinite(date.getTime())&&centralDateTime(date.toISOString())===value);
 if(candidates.length!==1)throw new Error('That Central Time is missing or ambiguous because of the clock change. Contact a supervisor.');
 const start=candidates[0];const age=Date.parse(at)-start.getTime();
 if(age<0||age>72*3600000)throw new Error('Start time must be within the past 72 hours and cannot be in the future.');
 return start.toISOString();
}
export function missedLunchInterval(shift,at,day){
 const night=['41','night','night shift'].includes(String(shift).trim().toLowerCase());
 const p=parts(at);
 if(!day){
  day=`${p.year}-${p.month}-${p.day}`;
  if(night&&Number(p.hour)<12)day=new Date(Date.parse(day+'T12:00:00Z')-86400000).toISOString().slice(0,10);
 }
 const start=correctedStart(`${day}T${night?'22:00':'11:30'}`,at);
 return {day,start,end:new Date(Date.parse(start)+3600000).toISOString()};
}
export function startOnExistingDate(value,existing,at,period){
 const day=centralDateTime(existing).slice(0,10);
 let input=String(value||'').trim();
 if(period!==undefined&&period!==null){
  const match=input.match(/^(0?[1-9]|1[0-2]):?([0-5][0-9])$/);
  if(!match||!['AM','PM'].includes(period))throw new Error('Enter a valid time and select AM or PM.');
  input=`${String(Number(match[1])%12+(period==='PM'?12:0)).padStart(2,'0')}:${match[2]}`;
 }
 if(/^\d{4}$/.test(input))input=`${input.slice(0,2)}:${input.slice(2)}`;
 if(input.includes('T')&&input.slice(0,10)!==day)throw new Error('Only the start time can be changed. The entry date must stay the same.');
 if(!input.includes('T')&&!/^\d{2}:\d{2}$/.test(input))throw new Error('Enter a valid start time in Central Time.');
 return correctedStart(input.includes('T')?input:`${day}T${input}`,at);
}
export function applyTimingAdjustment(timer,kind,data,at,shift){
 if(timer.downtime)throw new Error('End downtime before adjusting time.');
 const intervals=[...(timer.breaks||[]).map(p=>({...p,end:p.end||at,deducts:p.type!=='With Relief'})),...(timer.downtimes||[]).map(p=>({...p,type:'Downtime',deducts:true}))];
 if(kind==='correct_start'){
  const start=startOnExistingDate(data.start_time,timer.start,at,data.start_period);
  if(intervals.some(p=>Date.parse(start)>Date.parse(p.start)))throw new Error('Start time cannot be later than an already recorded break or downtime.');
  timer.start=start;const date=centralDateTime(start).slice(0,10);timer.fields.LaborDate=date;timer.fields.Year=Number(date.slice(0,4));
 }else{
  if(timer.status!=='In Progress')throw new Error('Resume the current break before logging a missed lunch.');
  const lunch=missedLunchInterval(shift,at,data.lunch_day);
  if(Date.parse(lunch.end)>Date.parse(at)||Date.parse(lunch.start)<Date.parse(timer.start))throw new Error('The full scheduled lunch hour must already have elapsed within this entry. Correct start time first if needed.');
  if(intervals.some(p=>(p.deducts&&Date.parse(lunch.start)<Date.parse(p.end)&&Date.parse(lunch.end)>Date.parse(p.start))||(p.type==='Lunch'&&centralDateTime(p.start).slice(0,10)===lunch.day)))throw new Error('Lunch or another deduction is already recorded for that period. No extra hour was deducted.');
  timer.break_minutes+=60;timer.breaks.push({type:'Lunch',start:lunch.start,end:lunch.end,minutes:60,retroactive:true,lunch_day:lunch.day});
 }
}
const escape=value=>String(value??'').replace(/[&<>"']/g,char=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'})[char]);
export function renderActiveTiming(entry,tablet=false){
 const identity=`<input type="hidden" name="${tablet?'timer_id':'entry_id'}" value="${escape(entry.id)}">`;
 const time=centralDateTime(entry.start).slice(11),hour=Number(time.slice(0,2)),period=hour>=12?'PM':'AM';
 const display=`${hour%12||12}:${time.slice(3)}`;
 return `<details class="cnc-active-downtime"><summary>Edit Time</summary><form ${tablet?'data-action="correct_start"':'method="post" action="/time/active/edit"'} class="cnc-form">${identity}<div class="cnc-start-clock"><label><span>Start Time (Central)</span><input type="text" inputmode="numeric" name="start_time" value="${escape(display)}" pattern="(0?[1-9]|1[0-2]):?[0-5][0-9]" maxlength="5" placeholder="8:00" autocomplete="off" title="Enter a time such as 800 or 8:00, then choose AM or PM." required></label><label><span>AM/PM</span><select name="start_period" required><option value="AM" ${period==='AM'?'selected':''}>AM</option><option value="PM" ${period==='PM'?'selected':''}>PM</option></select></label></div><small>Enter 800 for 8:00, then choose AM or PM.</small><button class="cnc-button ghost" type="submit">Save Start Time</button></form><form ${tablet?'data-action="log_lunch"':'method="post" action="/time/active/log-lunch"'} class="cnc-form">${identity}<input type="hidden" name="lunch_day"><button class="cnc-button ghost" type="submit" ${entry.status!=='In Progress'||entry.downtime?'disabled':''}>Log Lunch (1 Hour)</button></form></details>`;
}
