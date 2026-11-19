import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import vm from 'node:vm';

const window={};
const listeners=new Map();
const document={addEventListener(name,handler,capture){if(!capture)listeners.set(name,handler);}};
vm.runInNewContext(await readFile(new URL('../public/measurement-values.js',import.meta.url),'utf8'),{window,document});

test('nonzero range offsets require a sign; unsigned nonzero entries remain full measurements',()=>{
 for(const [entry,actual,offset] of [['.002',.002,false],[' .002 ',.002,false],['+.002',2.883,true],['-.002',2.879,true],['−.002',2.879,true],['2.883',2.883,false],['0.234',.234,false]]){
  const result=window.resolveInspectionMeasurement(entry,2.881);
  assert.equal(result.value,actual,entry);
  assert.equal(result.isOffset,offset,entry);
 }
 assert.equal(window.resolveInspectionMeasurement('.002',null).value,.002);
 for(const entry of ['.','+','NaN','Infinity','abc'])assert.ok(Number.isNaN(window.resolveInspectionMeasurement(entry,2.881).value));
});

test('unsigned zero entries resolve to nominal; zero without nominal remains zero',()=>{
 for(const entry of ['0','.000','0.000',' 0 ','+0','-0']){
  const result=window.resolveInspectionMeasurement(entry,2.881);
  assert.equal(result.value,2.881,entry);
  assert.equal(result.isOffset,true,entry);
  for(const nominal of [null,'']){
   const literal=window.resolveInspectionMeasurement(entry,nominal);
   assert.equal(Math.abs(literal.value),0,entry);
   assert.equal(literal.isOffset,false,entry);
  }
 }
 assert.equal(window.resolveInspectionMeasurement('1e-400',2.881).isOffset,false);
});

test('positive offset preview displays the actual measurement',()=>{
 const preview={matches:()=>true};
 const field={value:'+.002',dataset:{nominal:'2.881'},matches:()=>true,nextElementSibling:preview,setCustomValidity(message){this.validity=message;}};
 listeners.get('input')({target:field});
 assert.equal(field.validity,'');
 assert.equal(preview.hidden,false);
 assert.equal(preview.textContent,'Actual: 2.883');
 field.value='.000';
 listeners.get('input')({target:field});
 assert.equal(preview.hidden,false);
 assert.equal(preview.textContent,'Actual: 2.881');
});
