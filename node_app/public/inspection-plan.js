export function inspectionPlan(context,pipeNumber,scope,attemptNo=1){
 const elements=context.recipe.elements||[];
 if(scope==='full')return elements;
 const index=/^\d+$/.test(String(pipeNumber))?Number(pipeNumber):attemptNo;
 const mod=(value,divisor)=>((value%divisor)+divisor)%divisor;
 const sampling=context.recipe.sampling_plan||{};
 const always=sampling.alwaysMeasureItems||[],rotating=sampling.rotatingAuditItems||[],cycles=sampling.cycleMap||{};
 if(always.length||rotating.length){
  let picked=cycles[String(mod(index-1,Math.max(rotating.length,1))+1)];
  if(picked===undefined&&rotating.length)picked=rotating[mod(index-1,rotating.length)];
  const sequence=[...always,...(picked===undefined||picked===null?[]:Array.isArray(picked)?picked:[picked])];
  return sequence.map(number=>elements.find(element=>element.element_sequence===number)).filter(Boolean);
 }
 const count=Math.min(Math.max(context.always_count,0),elements.length);
 const result=elements.slice(0,count),other=elements.slice(count);
 if(other.length&&context.rotating_count>0){
  const start=mod((index-1)*context.rotating_count,other.length);
  for(let offset=0;offset<context.rotating_count;offset++)result.push(other[(start+offset)%other.length]);
 }
 return result;
}
export function nextPipe(value){const match=String(value).match(/^(.*?)(\d+)$/);return match?match[1]+String(Number(match[2])+1).padStart(match[2].length,'0'):'';}
