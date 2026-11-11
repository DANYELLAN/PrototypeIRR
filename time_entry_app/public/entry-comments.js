export function entryComment(row){
 if(Object.hasOwn(row,'user_comments'))return String(row.user_comments??'');
 const raw=String(row.tran_description||'').trim();
 if([row.operation_description,row.description].some(value=>String(value||'').trim()===raw))return '';
 const reason=String(row.details_type_ii||'').trim();
 if(reason&&raw===reason)return '';
 if(reason&&raw.startsWith(reason+' - '))return raw.slice(reason.length+3);
 return raw;
}
