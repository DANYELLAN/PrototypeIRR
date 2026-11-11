import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {timeNavigation,renderTimeNavigation,renderActiveTimerActions,activeDowntimeData} from '../public/time-entry-ui.js';
import {applyLocal} from '../public/tablet-time-store.js';
import {registerTabletTime} from '../src/tabletTime.js';
const timer={id:'test-timer',status:'In Progress',fields:{ProductionNo:'TEST-WO',OperationID:'10',DetailsType:'Production'}};
const context={downtime_reasons:[{code:'MECH',label:'Mechanical <repair>'}]};
test('running and paused timers retain full navigation and downtime while diagnostics remain IT-only',()=>{
 const session={employee:{emp_id:'1'},roles:['operator']};const hasRole=(user,role)=>user.roles.includes(role);
 const links=timeNavigation(session,hasRole);
 assert.deepEqual(links.map(item=>item.label),['Machine Overview','Time Entry','Daily Checklist','Notifications','Maintenance','IT Support']);
 assert.doesNotMatch(renderTimeNavigation(links),/Diagnostics|Admin Approvals/);
 assert.match(renderTimeNavigation(timeNavigation({...session,roles:['operator','it','approver']},hasRole)),/Button Diagnostics/);
 for(const status of ['In Progress','Paused']){
  const html=renderActiveTimerActions({...timer,status},context);
  assert.match(html,/>Log Downtime<\/summary>/);assert.match(html,/name="detail_type_ii" required/);
  assert.match(html,/Mechanical &lt;repair&gt;/);assert.match(html,/name="active_timer_id" value="test-timer"/);
  assert.match(html,/End Entry/);assert.match(html,/cnc-active-downtime/);
  assert.match(html,status==='Paused'?/Resume Production/:/Take Break/);
 }
});
test('active downtime is pinned to current timer, deducted once and retained in the offline event queue',()=>{
 const data=activeDowntimeData(timer,{active_timer_id:'test-timer',production_number:'FORGED',operation_id:'99',hours:'0',minutes:'15',detail_type_ii:'MECH'});
 assert.equal(data.production_number,'TEST-WO');assert.equal(data.operation_id,'10');
 assert.throws(()=>activeDowntimeData({...timer,id:'new-timer'},data),/timer changed/);
 const profile={timer:{...timer,downtime_minutes:0},events:[],drafts:{}};
 applyLocal(profile,{id:'downtime',kind:'misc',data,at:'2026-11-05T16:00:00Z'});
 assert.equal(profile.timer.downtime_minutes,15);assert.equal(profile.events.length,1);assert.equal(profile.timer.status,'In Progress');
});
test('tablet context returns role-checked navigation from the same source as traditional pages',async()=>{
 const routes=new Map();registerTabletTime({get:(path,...handlers)=>routes.set(path,handlers),post:()=>{}},{hasRole:(_,role)=>role==='operator',callBridge:async action=>{assert.equal(action,'get_time_page_context');return {context:{operations:[],details_step_two:[],downtime_reasons:[]},timer:null,review:{required:false}};}});
 const req={session:{cncUser:{employee:{emp_id:'1',full_name:'Test'},machine_no:'1',shift_id:'40'}}};let result;
 const res={json:value=>result=value,status(){return this;}};
 await routes.get('/api/tablet-time/context')[1](req,res);
 assert.equal(result.navigation.length,6);assert.equal(result.navigation.find(item=>item.key==='notifications').href,'/notifications');
});
test('offline release precaches shared controls without removing tablet storage',async()=>{
 const shell=await readFile(new URL('../public/tablet-time.html',import.meta.url),'utf8');
 const worker=await readFile(new URL('../public/cnc-time-sw.js',import.meta.url),'utf8');
 assert.match(shell,/id="time-navigation"/);assert.match(worker,/'\/public\/time-entry-ui.js'/);
 assert.match(worker,/tablet-time-store.js/);
});

test('unrelieved breaks offer arrival while covered breaks offer End Relieved Break',()=>{
 const without=renderActiveTimerActions({...timer,status:'Paused',breaks:[{type:'No Relief'}]},context);
 assert.match(without,/Relief Arrived/);assert.match(without,/Resume Production/);
 const covered=renderActiveTimerActions({...timer,status:'Paused',breaks:[{type:'With Relief',relief_adp:'123'}]},context);
 assert.match(covered,/End Relieved Break/);assert.doesNotMatch(covered,/Resume Production|Relief Arrived/);
 const lunch=renderActiveTimerActions({...timer,status:'Paused',breaks:[{type:'Lunch'}]},context);
 assert.doesNotMatch(lunch,/Relief Arrived/);
});
