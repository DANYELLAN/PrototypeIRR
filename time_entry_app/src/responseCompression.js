import {gzip} from 'node:zlib';

// Compress dynamic text responses; static/offline files keep their existing cache behavior.
export function responseCompression(req,res,next){
 const send=res.send;
 res.send=function(body){
  const type=String(res.get('Content-Type')|| (typeof body==='string'?'text/html':''));
  if(/^(text\/|application\/(json|javascript))/i.test(type))res.vary('Accept-Encoding');
  if(!req.acceptsEncodings('gzip')||res.statusCode<200||res.statusCode>=300||
     res.get('Content-Encoding')||/no-transform/i.test(res.get('Cache-Control')||'')||
     !/^(text\/|application\/(json|javascript))/i.test(type)||
     !(typeof body==='string'||Buffer.isBuffer(body))||Buffer.byteLength(body)<1024){
   return send.call(this,body);
  }
  res.vary('Accept-Encoding');
  if(!res.get('Content-Type'))res.type('html');
  gzip(body,{level:4},(error,compressed)=>{
   if(res.destroyed||res.writableEnded)return;
   if(error){send.call(res,body);return;}
   res.set('Content-Encoding','gzip');res.removeHeader('Content-Length');
   send.call(res,compressed);
  });
  return this;
 };
 next();
}
