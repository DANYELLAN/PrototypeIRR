import test from 'node:test';
import assert from 'node:assert/strict';
import {createReviewReturn} from '../public/time-view-return.js';
const normal='<form action="/time/start"></form><section id="recent-entries"></section>';
function storage(){const values=new Map();return {getItem:key=>values.get(key)??null,setItem:(key,value)=>values.set(key,value)};}
test('old-server redirects and offline fallback HTML cannot trigger a refresh loop',async()=>{
 let time=100000,requests=0,navigations=0;
 const check=createReviewReturn({now:()=>time,storage:storage(),request:async()=>{requests++;return {ok:true,text:async()=>'<html>Tablet Time Entry</html>'};},navigate:()=>navigations++});
 for(let i=0;i<5;i++){assert.equal(await check(),false);time+=15000;}
 assert.equal(requests,5);assert.equal(navigations,0);
});
test('normal connected page triggers exactly one return despite repeated health signals',async()=>{
 let navigations=0;
 const check=createReviewReturn({storage:storage(),request:async()=>({ok:true,text:async()=>normal}),navigate:path=>{assert.equal(path,'/time');navigations++;}});
 assert.equal(await check(),true);assert.equal(await check(),false);assert.equal(navigations,1);
});
test('offline failure, expired sign-in and embedded recovery never navigate',async()=>{
 for(const request of [async()=>{throw new Error('Offline');},async()=>({ok:false,status:401})]){
  const check=createReviewReturn({request,navigate:()=>assert.fail('Must stay on the saved worksheet')});assert.equal(await check(),false);
 }
 const embedded=createReviewReturn({embedded:true,request:()=>assert.fail('Must not probe from the embedded timer'),navigate:()=>assert.fail()});
 assert.equal(await embedded(),false);
});
test('a fresh offline fallback document respects the previous navigation cooldown',async()=>{
 let time=100000,navigations=0;const saved=storage();
 const options={storage:saved,now:()=>time,request:async()=>({ok:true,text:async()=>normal}),navigate:()=>navigations++};
 assert.equal(await createReviewReturn(options)(),true);
 time+=5000;assert.equal(await createReviewReturn(options)(),false);
 time+=26000;assert.equal(await createReviewReturn(options)(),true);
 assert.equal(navigations,2);
});
test('overlapping checks and active input cannot cause duplicate or disruptive navigation',async()=>{
 let complete,editing=false,navigations=0;
 const check=createReviewReturn({canNavigate:()=>!editing,request:()=>new Promise(resolve=>complete=resolve),navigate:()=>navigations++});
 const pending=check();assert.equal(await check(),false);
 editing=true;complete({ok:true,text:async()=>normal});assert.equal(await pending,false);
 assert.equal(navigations,0);
});
