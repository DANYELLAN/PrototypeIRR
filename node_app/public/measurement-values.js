(() => {
 const decimalPlaces=value=>{
  const [coefficient,exponent='0']=String(value).toLowerCase().split('e');
  return Math.max(0,(coefficient.split('.')[1] || '').length-Number(exponent));
 };
 window.resolveInspectionMeasurement=(raw,nominal)=>{
  const text=String(raw ?? '').trim().replaceAll('−','-');
  if(!/^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:e[+-]?\d+)?$/i.test(text))return {value:NaN,isOffset:false};
  let value=Number(text);
  const baseText=String(nominal ?? '').trim();
  // Match exact zero text so tiny nonzero values that underflow stay full measurements.
  const isZero=/^[+-]?(?:0+(?:\.0*)?|\.0+)(?:e[+-]?\d+)?$/i.test(text);
  const isOffset=(/^[+-]/.test(text) || isZero) && baseText!=='' && Number.isFinite(Number(baseText));
  if(isOffset){
   const places=Math.min(15,Math.max(decimalPlaces(text),decimalPlaces(baseText)));
   value=Number((Number(baseText)+value).toFixed(places));
  }
  return {value,isOffset};
 };
 // Normalize legacy float arithmetic noise before the worksheet checks inclusive limits.
 // Capture runs before field-level validation, including on dynamically loaded pipes.
 const normalizeLimits=field=>{
  if(!field.matches?.('[data-measurement-offset]'))return;
  for(const key of ['min','max']){
   const text=String(field.dataset[key] ?? '').trim();
   if(text!=='' && Number.isFinite(Number(text)))field.dataset[key]=String(Number(Number(text).toPrecision(15)));
  }
 };
 for(const name of ['input','change','blur','focusin'])document.addEventListener(name,event=>normalizeLimits(event.target),true);
 const refresh=field=>{
  if(!field.matches?.('[data-measurement-offset]'))return;
  const text=field.value.trim();
  const result=window.resolveInspectionMeasurement(text,field.dataset.nominal);
  field.setCustomValidity(!text || Number.isFinite(result.value)?'':'Enter a full measurement or signed offset, for example 2.879 or -.002.');
  let preview=field.nextElementSibling;
  if(!preview?.matches('[data-measurement-actual]')){
   preview=document.createElement('small');preview.dataset.measurementActual='';preview.className='measurement-actual';preview.setAttribute('aria-live','polite');field.after(preview);
  }
  preview.hidden=!result.isOffset || !Number.isFinite(result.value);
  preview.textContent=preview.hidden?'':`Actual: ${result.value}`;
 };
 for(const name of ['input','change','focusout'])document.addEventListener(name,event=>refresh(event.target));
})();
