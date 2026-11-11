import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import http from 'node:http';
import {gunzipSync} from 'node:zlib';
import {responseCompression} from '../src/responseCompression.js';

test('dynamic HTML and JSON transfer fewer bytes without changing content or HTTP behavior',async()=>{
 const html='<html><body>'+('<label>Production order — EWO26-00142</label>'.repeat(400))+'</body></html>';
 const app=express();app.use(responseCompression);
 app.get('/page',(_req,res)=>res.send(html));
 app.get('/json',(_req,res)=>res.json({operations:Array(200).fill({work_order:'EWO26-00142',description:'THREAD BOX END'})}));
 app.get('/small',(_req,res)=>res.send('Small response'));
 app.get('/untouched',(_req,res)=>res.set('Cache-Control','no-transform').send(html));
 app.get('/empty',(_req,res)=>res.status(204).send());
 const server=app.listen(0,'127.0.0.1');await new Promise(resolve=>server.once('listening',resolve));
 const request=(path,encoding='gzip',method='GET')=>new Promise((resolve,reject)=>{
  const call=http.request({hostname:'127.0.0.1',port:server.address().port,path,method,headers:{'Accept-Encoding':encoding}},res=>{
   const chunks=[];res.on('data',part=>chunks.push(part));res.on('end',()=>resolve({headers:res.headers,status:res.statusCode,body:Buffer.concat(chunks)}));
  });call.on('error',reject);call.end();
 });
 try{
  const compressed=await request('/page'),plain=await request('/page','identity');
  assert.equal(compressed.headers['content-encoding'],'gzip');
  assert.equal(gunzipSync(compressed.body).toString(),html);assert.equal(plain.body.toString(),html);
  assert.equal(Number(compressed.headers['content-length']),compressed.body.length);
  assert.match(plain.headers.vary,/Accept-Encoding/);assert.match(compressed.headers.vary,/Accept-Encoding/);
  assert.ok(compressed.body.length<plain.body.length/2);
  const json=await request('/json');assert.equal(JSON.parse(gunzipSync(json.body)).operations.length,200);
  for(const [path,encoding]of [['/page','gzip;q=0, identity'],['/small','gzip'],['/untouched','gzip']]){
   assert.equal((await request(path,encoding)).headers['content-encoding'],undefined);
  }
  const head=await request('/page','gzip','HEAD');assert.equal(head.body.length,0);
  const empty=await request('/empty');assert.equal(empty.status,204);assert.equal(empty.body.length,0);
 }finally{await new Promise(resolve=>server.close(resolve));}
});
