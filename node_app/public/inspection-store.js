let opening;
function database(){
 if(!opening)opening=new Promise((resolve,reject)=>{
  const request=indexedDB.open('benoit-tablet-inspection',1);
  request.onupgradeneeded=()=>{request.result.createObjectStore('profiles',{keyPath:'owner'});request.result.createObjectStore('meta');};
  request.onsuccess=()=>resolve(request.result);request.onerror=()=>reject(request.error);
 });
 return opening;
}
export async function readProfile(owner){
 const db=await database();return new Promise((resolve,reject)=>{
  const request=db.transaction('profiles').objectStore('profiles').get(owner);
  request.onsuccess=()=>resolve(request.result);request.onerror=()=>reject(request.error);
 });
}
export async function currentOwner(value){
 const db=await database();return new Promise((resolve,reject)=>{
  const tx=db.transaction('meta',value===undefined?'readonly':'readwrite');
  const store=tx.objectStore('meta');const request=value===undefined?store.get('current'):store.put(value,'current');
  let result;request.onsuccess=()=>{result=request.result;};tx.oncomplete=()=>resolve(result);tx.onerror=()=>reject(tx.error);
 });
}
export async function updateProfile(owner,change){
 const db=await database();return new Promise((resolve,reject)=>{
  const tx=db.transaction('profiles','readwrite');const store=tx.objectStore('profiles');const request=store.get(owner);let result,failure;
  request.onsuccess=()=>{try{result=change(request.result);store.put(result);}catch(error){failure=error;tx.abort();}};
  tx.oncomplete=()=>resolve(result);tx.onabort=()=>reject(failure||tx.error);tx.onerror=()=>reject(tx.error);
 });
}
export function eventId(){
 const bytes=crypto.getRandomValues(new Uint8Array(16));bytes[6]=(bytes[6]&15)|64;bytes[8]=(bytes[8]&63)|128;
 const hex=[...bytes].map(byte=>byte.toString(16).padStart(2,'0')).join('');return `${hex.slice(0,8)}-${hex.slice(8,12)}-${hex.slice(12,16)}-${hex.slice(16,20)}-${hex.slice(20)}`;
}
