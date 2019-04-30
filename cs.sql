drop table cs_temp;

 CREATE TABLE if not exists cs_temp STORED AS ORC as select distinct m2.interaction_id,
 m2.sr,
 m2.interaction_type,
 m2.created_time,
 from_unixtime(cast((m2.created_time)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') interaction_timestamp,
 m2.call_start,
 m2.call_end,
 m2.acw_start,
 m2.acw_end,
 m2.customer_number,
 m2.call_recording,
 m2.post_interaction_data, 
 m2.chat_pi_start,
 m2.chat_pi_end,
 m2.org_id,
 xt.ac agents_assigned,
 case when se.org_id is not null then 'Seller' else 'Buyer' end as org_type,
 get_json_object(o2.unit_address,'$.city') as called_city,
 cast(ll1.lat as double) city_lat,
 cast(ll1.lon as double) city_lon,


 get_json_object(o2.unit_address,'$.state') as called_state,
 cast(ll2.lat as double) state_lat,
 cast(ll2.lon as double) state_lon,

 m2.agent_id,
 m2.chat_data,

case when mi.seller_org_id is not null and mi.seller_org_id=m2.org_id then 'Seller'
  when mi.buyer_org_id is not null and mi.buyer_org_id=m2.org_id then 'Buyer'
  when r.seller_id is not null and r.seller_id=m2.org_id then 'Seller'
  when r.buyer_id is not null and r.buyer_id=m2.org_id then 'Buyer'
end as caller_type,





 from_unixtime(cast((get_json_object(m2.chat_data,'$.startSequence')+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') as chat_startSequence,
 from_unixtime(cast((get_json_object(m2.chat_data,'$.endSequence')+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') as chat_endSequence,
 from_unixtime(cast((get_json_object(m2.chat_data,'$.firstMessageAt')+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') as chat_firstMessageAt,
 from_unixtime(cast((get_json_object(m2.chat_data,'$.lastMessageAt')+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') as chat_lastMessageAt,
 from_unixtime(cast((get_json_object(m2.chat_data,'$.firstReplyAt')+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') as chat_firstReplyAt,
 get_json_object(m2.chat_data,'$.lastMessageBy') as chat_lastMessageBy,
 m2.status,
 m2.interaction_outcome,
 m2.initiated_by,
 m2.v_call_id,
 m2.v_interaction_duration,
 m2.v_ht_duration,
 m2.v_pi_duration,
 m2.v_answering_time,
 m2.all_sr,
 m2.v_call_team_number,
 m2.agent_assigned_at,
 m2.interaction_team,
 m2.interaction_sub_team,
 m2.order_id,
 m2.return_id,
 m2.type,
 m2.subtype,
 m2.v_caller_operator,
 m2.v_caller_circle,



 case 
  when trim(m2.extra_data) not in ('[]','null') then 'DT' 
  else 'Not DT' 
 end as interaction_system,

round((m2.created_time-o1.created_at)/86400000,0) as org_age_in_days,
case 
  when round((m2.created_time-o1.created_at)/86400000,0)<7*2 then 'New'
  else 'Old'
end as org_new_old_tag,

from_unixtime(cast((o1.created_at+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') as org_registered_date,
m2.extra_data,
m2.sr_status,
m2.user_note,
m2.freshdesk_ticket,
m2.sr_created_at,
m2.issue_id,
sis.title dt_issue,
sis.solution dt_solution,
sis.context_types,
sis.order_statuses,
sis.return_statuses,
sis.escalate fcr,
sis.issue_type issue_type,
sis.issue_sub_type issue_sub_type,
from_unixtime(cast((sis.created_at+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') as issue_created_at,

al.shipment_group_id,
al.vendor,
al.current_status shipment_status,
mi.order_status order_status,
case when m2.order_id is not null then mi.category else r.category end as category,


case when m2.order_id is not null then
mi.payment_mode else r.payment_mode end as payment_mode,
case when m2.order_id is not null then al.lane else r.lane end as lane,

al.attempt attempt,
al.lm_attempt_count ue_attempt_count,
case when m2.order_id is not null then mi.seller_city else r.seller_city end as seller_city,
case when m2.order_id is not null then mi.seller_state else r.seller_state end as seller_state,
case when m2.order_id is not null then mi.buyer_city else r.buyer_city end as buyer_city,
case when m2.order_id is not null then mi.buyer_state else r.buyer_state end as buyer_state,
case when m2.order_id is not null then mi.seller_org_id else r.seller_id end as seller_org_id,
case when m2.order_id is not null then mi.buyer_org_id else r.buyer_id end as buyer_org_id,
-- logistics timestamp ---
mi.order_date,

al.rts_date,al.picked_up_time,al.first_shipped_date,al.fa_date,al.del_date,al.rto_date,al.rto_del_date,al.fwd_promise_date,
al.unit_qty order_qty,al.rts_qty,al.clean_ndr_reason,al.order_amount,al.shipment_amount,

--- returns timestamp ---
r.request_created_at,r.request_approved_at,r.request_cancelled_at,r.request_rejected_at, 
r.rev_shipment_id,r.reschedule_count, r.rev_current_status, r.rev_vendor,r.rev_rts_date,r.rev_shipped_date,r.rejected_reason,r.seller_dispute_date, r.dispute_reason, r.dispute_settled_on,
--- credit details ----


ca.id application_id,
ca.state application_state,
from_unixtime(cast((ca.created_at+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') applied_at,
ca.category shop_type,
ca.borrower_category borrower_category,
get_json_object(ca.tags,'$.tag') tag,
ca.enable is_credit_enabled,
cb.amount_blocked_paisa/1000 amount_blocked,
cb.amount_consumed_paisa/1000 amount_consumed,
cb.amount_released_paisa/1000 amount_released,

get_json_object(cb.context,'$.runningAccountOrder') is_running_account,


-----------------  feedback  ---------------------------
trim(fr.feedback_request_id) as feedback_request_id,
fr.org_id feedback_org_id,
trim(fr.support_request_id) feedback_sr,
fr.status feedback_status,
fr.ivr_initiated ivr_initiated_at,
substr(sf.answer,2,1) as feedback_answer,
from_unixtime(cast((sf.captured_at+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') as answered_at,
from_unixtime(cast((fr.created_at+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') as question_created_at,
from_unixtime(cast((fr.scheduled_at+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') scheduled_at



from 
(select m1.interaction_id,
 m1.sr,
 m1.interaction_type,
 m1.created_time,
 m1.call_start,
 m1.call_end,
 m1.acw_start,
 m1.acw_end,
 m1.customer_number,
 m1.call_recording,
 m1.post_interaction_data, 
 m1.chat_pi_start,
 m1.chat_pi_end,
 m1.org_id,
 m1.agent_id,
 m1.chat_data,
 m1.status,
 m1.interaction_outcome,
 m1.initiated_by,
 m1.v_call_id,
 m1.v_interaction_duration,
 m1.v_ht_duration,
 m1.v_pi_duration,
 m1.v_answering_time,
 m1.all_sr,
 m1.v_call_team_number,
 m1.agent_assigned_at,
 m1.v_caller_operator,
m1.v_caller_circle,
 m1.interaction_team,
 m1.interaction_sub_team,
 trim(s.request_id) req_id,  
case when substr(substr(s.extra_data,instr(s.extra_data,'orderId')+10,14),1,2)='OD' 
then substr(s.extra_data,instr(s.extra_data,'orderId')+10,14)
else null
end order_id,
case when substr(substr(s.extra_data,instr(s.extra_data,'returnId')+11,14),1,2)='RT' then substr(s.extra_data,instr(s.extra_data,'returnId')+11,14) else null end as return_id,
case when substr(substr(s.extra_data,instr(s.extra_data,'issueId')+10,14),1,2)='SI' then substr(s.extra_data,instr(s.extra_data,'issueId')+10,9) else null end as issue_id,
s.type type,
s.subtype subtype,
s.extra_data extra_data,
s.status sr_status,
s.user_note user_note,
s.freshdesk_ticket freshdesk_ticket,
from_unixtime(cast((s.created_at+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') sr_created_at



from 
(SELECT rid as interaction_id,
  trim(task_id) as sr,
 interaction_type,
 created_time,
 call_start,
 call_end,
 acw_start,
 acw_end,
 customer_number,
 call_recording,
 post_interaction_data, 
 chat_pi_start,
 chat_pi_end,
 org_id,
 agent_id,
 chat_data,
 status,
 all_sr,
 interaction_outcome,
 initiated_by,
 v_call_id,
 v_interaction_duration,
 v_ht_duration,
 v_pi_duration,
  v_answering_time,
 v_call_team_number,
 agent_assigned_at,
 interaction_team,
 interaction_sub_team,
 v_caller_operator,
 v_caller_circle

FROM (SELECT ab.interaction_id rid,
ab.interaction_id as interactionId,
ab.interaction_type interaction_type,
ab.created_at+19800000 as created_time,
from_unixtime(cast((ab.interaction_started_at+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') as call_start,
from_unixtime(cast((ab.interaction_ended_at+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') as  call_end,
from_unixtime(cast((get_json_object(ab.post_interaction_data,'$.startedAt')+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') as acw_start,
from_unixtime(cast((get_json_object(ab.post_interaction_data,'$.endedAt')+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') as acw_end,
get_json_object(ab.call_data,'$.customerPhoneNumber') as customer_number,
get_json_object(ab.call_data,'$.recordingUrl') as call_recording,
ab.post_interaction_data as post_interaction_data,
from_unixtime(cast((get_json_object(ab.post_interaction_data,'$.startedAt')+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') as chat_pi_start,
from_unixtime(cast((get_json_object(ab.post_interaction_data,'$.endedAt')+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') as chat_pi_end,
ab.org_id org_id,
ab.agent_id agent_id,
ab.chat_data chat_data,
ab.status status,
ab.interaction_outcome interaction_outcome,
ab.initiated_by initiated_by,
ab.v_call_id v_call_id,
ab.v_interaction_duration v_interaction_duration,
ab.v_pi_duration v_pi_duration,
ab.v_answering_time as v_answering_time,
ab.v_ht_duration v_ht_duration,
get_json_object(ab.post_interaction_data,'$.requests') as all_sr,
ab.v_call_team_number as v_call_team_number,
from_unixtime(cast((ab.agent_assigned_at+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') agent_assigned_at,
ab.interaction_team interaction_team,
ab.interaction_sub_team interaction_sub_team,
ab.v_caller_operator v_caller_operator,
ab.v_caller_circle v_caller_circle,
split(get_json_object(ab.post_interaction_data,'$.requests'),'"') abc FROM ds_sqls_support_interactions_v2 ab ) temp LATERAL VIEW explode(abc) adtable AS task_id 
where substr(task_id,1,2)='SR' 

union 

select 

s1.interaction_id interaction_id,
null as sr,
s1.interaction_type interaction_type,
s1.created_at+19800000 as created_time,
from_unixtime(cast((s1.interaction_started_at+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') as call_start,
from_unixtime(cast((s1.interaction_ended_at+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') as  call_end,
from_unixtime(cast((get_json_object(s1.post_interaction_data,'$.startedAt')+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') as acw_start,
from_unixtime(cast((get_json_object(s1.post_interaction_data,'$.endedAt')+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') as acw_end,
get_json_object(s1.call_data,'$.customerPhoneNumber') as customer_number,
get_json_object(s1.call_data,'$.recordingUrl') as call_recording,
s1.post_interaction_data as post_interaction_data,
from_unixtime(cast((get_json_object(s1.post_interaction_data,'$.startedAt')+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') as chat_pi_start,
from_unixtime(cast((get_json_object(s1.post_interaction_data,'$.endedAt')+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') as chat_pi_end,
s1.org_id org_id,
s1.agent_id agent_id,
s1.chat_data chat_data,
s1.status status,
null as all_sr,
s1.interaction_outcome interaction_outcome,
s1.initiated_by initiated_by,
s1.v_call_id v_call_id,
s1.v_interaction_duration v_interaction_duration,
s1.v_pi_duration v_pi_duration,
s1.v_answering_time as v_answering_time,
s1.v_ht_duration v_ht_duration,
s1.v_call_team_number as v_call_team_number,
from_unixtime(cast((s1.agent_assigned_at+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') agent_assigned_at,
s1.interaction_team interaction_team,
s1.interaction_sub_team interaction_sub_team,
s1.v_caller_operator v_caller_operator,
s1.v_caller_circle v_caller_circle

from ds_sqls_support_interactions_v2 s1 where get_json_object(s1.post_interaction_data,'$.requests') is null) m1

left join ds_sqls_support_requests s on trim(s.request_id)=m1.sr and s.deleted=0) m2 
left join ds_sqls_support_issues sis on sis.issue_id=m2.issue_id
left join all_data al on al.seller_order_id=m2.order_id
left join mis_table mi on mi.order_id=m2.order_id
left join returns_data r on r.return_id=m2.return_id
left join ds_sqls_support_feedback_requests fr on trim(m2.sr)=trim(fr.support_request_id)
left join ds_sqls_support_feedback sf on sf.feedback_request_id=fr.feedback_request_id 
left join ds_sqls_credit_application ca on ca.borrower_id=m2.org_id
left join ds_sqls_credit_block cb on cb.reference_id=mi.order_id
left join ds_sqls_orgs o1 on o1.org_id=m2.org_id
left join ds_sqls_org_units o2 on o2.org_unit_id=o1.head_office_org_unit_ref
left join ds_csv_lat_lon ll1 on ll1.city= get_json_object(o2.unit_address,'$.city')
left join ds_csv_lat_lon ll2 on ll2.city= get_json_object(o2.unit_address,'$.state')
left join ds_sqls_sellers se on se.org_id=m2.org_id and se.status='ENABLED'
left join (select s.interaction_id,count(distinct s.agent_id) as ac from ds_sqls_support_agent_interaction_details s group by s.interaction_id ) xt on xt.interaction_id=m2.interaction_id


where  m2.interaction_team='CustomerSupport' ;


drop table cs;
CREATE TABLE IF NOT EXISTS cs STORED AS ORC AS SELECT distinct * FROM cs_temp;
drop table cs_temp;