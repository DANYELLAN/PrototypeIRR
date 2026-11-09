import {timeNavigation} from "../public/time-entry-ui.js";
export function registerTabletTime(app, {callBridge, hasRole}) {
  const requireTabletUser=(req,res,next)=>{
    if(!req.session?.cncUser || !hasRole(req.session.cncUser,'operator'))return res.status(401).json({error:'Sign in again to sync this tablet. Saved entries remain on the tablet.'});
    res.set('Cache-Control','no-store');next();
  };
  app.post('/api/tablet-time/relief',requireTabletUser,async(req,res)=>{
    try { res.json(await callBridge('tablet_relief_operator',{adp_number:req.body.adp_number,original_adp:req.session.cncUser.employee.emp_id})); }
    catch(error) { res.status(400).json({error:error.message}); }
  });
  app.get('/api/tablet-time/health',requireTabletUser,(req,res)=>res.json({connected:true}));
  app.get('/api/tablet-time/context',requireTabletUser,async(req,res)=>{
    try {
      const session=req.session.cncUser;
      const context=await callBridge('get_dashboard_context',{emp_id:session.employee.emp_id,user_email:session.user_email});
      const timer=await callBridge('tablet_time_active',{session});
      const review=await callBridge('get_daily_time_review',{employee:session.employee});
      res.json({employee:{emp_id:String(session.employee.emp_id),full_name:session.employee.full_name},
        machine_no:session.machine_no,shift_id:session.shift_id,shift_title:session.shift_title,
        navigation:timeNavigation(session,hasRole),operations:context.operations,details:context.details_step_two,downtime_reasons:context.downtime_reasons,
        legacy_active:context.active_entry,timer,review,prepared_at:new Date().toISOString()});
    } catch(error) {res.status(503).json({error:error.message});}
  });
  app.post('/api/tablet-time/events',requireTabletUser,async(req,res)=>{
    const session=req.session.cncUser;
    const expected=JSON.stringify([String(session.employee.emp_id).replace(/^0+/,'')||'0',String(session.machine_no),String(session.shift_id)]);
    if(req.body.owner!==expected)return res.status(409).json({error:'Sign in as the operator, station and shift that recorded these entries. Their tablet copies have been kept.'});
    try {
      const result=await callBridge('tablet_time_event',{session,event:req.body.event});
      if(result.approval_id){
        try{
          result.review=await callBridge('get_daily_time_review',{employee:session.employee,approval_id:result.approval_id});
          if(result.review.required){req.session.dailyReviewDate=result.review.labor_date;req.session.dailyReviewEditing=false;}
        }catch{result.review_pending=true;}
      }
      res.json(result);
    } catch(error) {res.status(409).json({error:error.message});}
  });
}
