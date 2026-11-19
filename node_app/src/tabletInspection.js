export function registerTabletInspection(app,{callBridge,connectionLabel,nextPipeNumber}){
 const guard=(req,res,next)=>{
  res.set('Cache-Control','no-store');
  if(!req.session?.inspector||!req.session.sessionRecord)return res.status(401).json({error:'Sign in again to sync. Saved measurements remain on the tablet.'});
  if(!req.session.selection?.recipeName||!req.session.selection?.operationDescription)return res.status(409).json({error:'Select the work order, detail and Digital IRR in the inspection screen first.'});
  next();
 };
 const params=req=>{
  const selection=req.session.selection,station=req.session.sessionRecord;
  return {production_number:selection.productionNumber,operation_description:connectionLabel(selection),pipe_number:selection.pipeNumber||'1',
   branch:req.session.inspector.branch,session_id:station.id,inspector:req.session.inspector,
   cnc_operator:{item_id:station.cnc_operator_item_id,name:station.cnc_operator_name},recipe_name:selection.recipeName,inspection_scope:req.session.activeInspection?.inspection_scope||selection.inspectionScope||'standard'};
 };
 const ownerFor=p=>JSON.stringify([String(p.session_id),p.production_number,p.operation_description,p.recipe_name]);
 app.get('/api/tablet-inspection/context',guard,async(req,res)=>{
  try{res.json({...await callBridge('tablet_inspection_prepare',{params:params(req)}),machine_name:req.session.sessionRecord.location_name||'',active_pipe:Boolean(req.session.activeInspection)});}catch(error){res.status(409).json({error:error.message});}
 });
 app.get('/api/tablet-inspection/health',guard,(req,res)=>res.json({connected:true}));
 app.post('/api/tablet-inspection/draft',guard,async(req,res)=>{
  const context=params(req);
  if(req.body.owner!==ownerFor(context))return res.status(409).json({error:'Reselect the original work order and station before publishing this draft.'});
  try{
   const result=await callBridge('tablet_inspection_draft',{params:context,event:req.body.event});
   req.session.activeInspection=result.attempt;req.session.selection.pipeNumber=result.pipe_number;
   res.json({saved:true});
  }catch(error){res.status(409).json({error:error.message});}
 });
 app.post('/api/tablet-inspection/pipes',guard,async(req,res)=>{
  const context=params(req);
  if(req.body.owner!==ownerFor(context))return res.status(409).json({error:'Reselect the original work order and station to sync this worksheet. Saved measurements have been kept.'});
  try{
   const result=await callBridge('tablet_inspection_apply',{params:context,event:req.body.event});
   if(String(req.session.selection.pipeNumber)===String(result.pipe_number)){
    req.session.activeInspection=null;
    req.session.selection.pipeNumber=nextPipeNumber(result.pipe_number);
   }
   res.json(result);
  }catch(error){res.status(409).json({error:error.message});}
 });
}
