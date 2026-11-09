import https from 'node:https';
import fs from 'node:fs';
import path from 'node:path';

export function enableTabletHttps(app, {name,port}) {
  const directory=process.env.BENOIT_TLS_DIR || path.join(process.env.ProgramData || 'C:/ProgramData','Benoit','Apps','tls');
  const certificate=path.join(directory,'server.crt');
  const key=path.join(directory,'server.key');
  if(!fs.existsSync(certificate)||!fs.existsSync(key)) {
    console.log(`[${name}] Tablet HTTPS awaits certificate setup.`);return null;
  }
  const server=https.createServer({cert:fs.readFileSync(certificate),key:fs.readFileSync(key),minVersion:'TLSv1.2'},app);
  server.on('error',error=>{console.error(`[${name}] HTTPS failed: ${error.message}`);});
  server.listen(port,()=>console.log(`[${name}] Tablet HTTPS listening on port ${port}`));
  app.get('/tablet-ca.crt',(req,res)=>res.type('application/x-x509-ca-cert').sendFile(path.join(directory,'benoit-tablet-ca.crt')));
  return server;
}
