drop table returns_temp;

CREATE TABLE if not exists temp72 STORED AS ORC as select sg.shipment_group_id as id, max(sst1.value) as last_rad_at,min(sst1.value) as first_rad_at from ds_sqls_shipment_group sg left join ds_sqls_shipment_group_metadata sst1 on sg.shipment_group_id=sst1.shipment_group_id where sst1.state='SHIPMENT_RAD' and sst1.key_attrib='event_time' and sg.current_active=1 group by sst1.shipment_group_id, sg.shipment_group_id;

CREATE TABLE if not exists returns_temp STORED AS ORC as with 

temp1 as (select ri.return_id as id, min(ri.created_at) as created_at from ds_sqls_returns ri group by ri.return_id), 

temp2 as (select ri.return_id as id, min(ri.created_at) as approved_at from ds_sqls_return_state_transitions ri where ri.new_value='APPROVED' group by ri.return_id), 

temp3 as (select ri.return_id as id, min(ri.created_at) as approved_at from ds_sqls_return_state_transitions ri where ri.new_value='CANCELLED' group by ri.return_id), 

temp4 as (select ri.return_id as id, min(ri.created_at) as approved_at from ds_sqls_return_state_transitions ri where ri.new_value='REJECTED' group by ri.return_id), 

temp5 as (select shipment_group_id as id, shipment_status as status from ds_sqls_shipment_group where current_active=1), 

temp6 as (select sg.shipment_group_id as id, min(sst1.created_at) as first_rts, max(sst1.created_at) as last_rts from ds_sqls_shipment_group sg left join ds_sqls_shipment_group_state_transition sst1 on sg.shipment_group_id=sst1.shipment_group_id where sst1.new_state='SHIPMENT_RTS' and sg.current_active=1 group by sst1.shipment_group_id, sg.shipment_group_id),

temp7 as (select sg.shipment_group_id as id, min(sst1.value) as first_shipped, max(sst1.value) as last_shipped from ds_sqls_shipment_group sg left join ds_sqls_shipment_group_metadata sst1 on sg.shipment_group_id=sst1.shipment_group_id where sst1.state='SHIPMENT_IN_TRANSIT' and sst1.key_attrib='event_time' and sg.current_active=1 group by sst1.shipment_group_id, sg.shipment_group_id), 

temp8 as (select sg.shipment_group_id as id, min(sst1.value) as first_rto, max(sst1.value) as last_rto from ds_sqls_shipment_group sg left join ds_sqls_shipment_group_metadata sst1 on sg.shipment_group_id=sst1.shipment_group_id where sst1.state='SHIPMENT_RTO' and sg.current_active=1  and sst1.key_attrib='event_time' group by sst1.shipment_group_id, sg.shipment_group_id) ,

temp9 as (select sg.shipment_group_id as id, max(sst1.value) as del_at,max(sst1.created_at) as mark_date from ds_sqls_shipment_group sg left join ds_sqls_shipment_group_metadata sst1 on sg.shipment_group_id=sst1.shipment_group_id where sst1.state='SHIPMENT_DELIVERED' and sst1.key_attrib='event_time' and sg.current_active=1 group by sst1.shipment_group_id, sg.shipment_group_id),

temp10 as (select sg.shipment_group_id as id,sg.awb_number as awb, orgv.display_name as vendor from ds_sqls_shipment_group sg left join ds_sqls_orgs orgv on orgv.org_id=sg.third_party_provider_id where sg.current_active=1), 

temp11 as (select sg.shipment_group_id as id, max(sst1.value) as cancelled_at from ds_sqls_shipment_group sg left join ds_sqls_shipment_group_metadata sst1 on sg.shipment_group_id=sst1.shipment_group_id where sst1.state='SHIPMENT_CANCELLED' and sst1.key_attrib='event_time' and sg.current_active=1 group by sst1.shipment_group_id, sg.shipment_group_id) ,

temp1a as (select b.id as id,count(b.awb) as reschedule_count from (select distinct a.id as id1,a.id1 as id,vsd.awb as awb from (select distinct rit.return_shipment_id as id,rit.return_id as id1 from ds_sqls_return_items rit) as a left join ds_sqls_vendor_shipment_details vsd on vsd.shipment_group_id=a.id where vsd.status_internal='SHIPMENT_RTS') b group by b.id),

temp26 as (select sot.order_id as id, sot.new_state as order_status,row_number() over (partition by sot.order_id order by sot.created_at desc) as row_num from ds_sqls_seller_order_state_transition sot),

temp27 as (select temp26.id as id,temp26.order_status as order_status from temp26 where temp26.row_num=1),

temp28 as (select rit.return_shipment_id as id,min(vsd.date_time) as fpa_time,max(vsd.date_time) as lpa_time from  ds_sqls_return_items rit left join ds_sqls_vendor_shipment_details vsd on vsd.shipment_group_id=rit.return_shipment_id left join ds_sqls_returns ri on ri.return_id=rit.return_id left join ds_sqls_shipment_group sg on sg.shipment_group_id=vsd.shipment_group_id left join ds_sqls_orgs orgs on orgs.org_id=sg.third_party_provider_id where sg.current_active=1 and  ri.current_active=1 and vsd.status_external='PP_Dispatched' and orgs.display_name in ('Delhivery','Delhivery Express') group by rit.return_shipment_id),

temp28a as (select rit.return_shipment_id as id,min(vsd.date_time) as fpa_time,max(vsd.date_time) as lpa_time from ds_sqls_return_items rit left join ds_sqls_vendor_shipment_details vsd on vsd.shipment_group_id=rit.return_shipment_id left join ds_sqls_returns ri on ri.return_id=rit.return_id left join ds_sqls_shipment_group sg on sg.shipment_group_id=vsd.shipment_group_id left join ds_sqls_orgs orgs on orgs.org_id=sg.third_party_provider_id where ri.current_active=1 and sg.current_active=1 and vsd.status_external='014' and orgs.display_name ='ECOM Express' group by rit.return_shipment_id),

temp28b as (select a.id as id,vsd.awb as awb, row_number() over (partition by vsd.shipment_group_id order by vsd.date_time desc) as row_num from (select distinct rit.return_shipment_id as id from ds_sqls_return_items rit) as a left join ds_sqls_vendor_shipment_details vsd on vsd.shipment_group_id=a.id where vsd.status_internal='SHIPMENT_RTS'),

temp29 as (select a.id as id, count(vsd.shipment_group_id) as pickup_attempt from (select distinct rit.return_shipment_id as id from ds_sqls_return_items rit) as a left join temp28b x on a.id=x.id left join ds_sqls_vendor_shipment_details vsd on vsd.awb=x.awb where x.row_num=1 and vsd.status_external='PP_Dispatched' group by vsd.shipment_group_id,a.id),

temp40 as (select a.id as id, count(vsd.shipment_group_id) as pickup_attempt from (select distinct rit.return_shipment_id as id from ds_sqls_return_items rit) as a left join temp28b x on a.id=x.id left join ds_sqls_vendor_shipment_details vsd on vsd.awb=x.awb where x.row_num=1 and vsd.status_external='014' group by vsd.shipment_group_id,a.id),

temp38 as (select rit.return_shipment_id as id, vsd.comment as ndr_reason,row_number() over (partition by rit.return_shipment_id order by vsd.date_time desc) as row_num from ds_sqls_return_items rit left join ds_sqls_vendor_shipment_details vsd on vsd.shipment_group_id=rit.return_shipment_id where  vsd.status_external='PP_Scheduled'), 

temp38a as (select rit.return_shipment_id as id,temp38.ndr_reason as ndr_reason from ds_sqls_return_items rit left join temp38 on temp38.id=rit.return_shipment_id where temp38.row_num=1),

temp44 as (select ht.shipment_id as id, min(unix_timestamp(ht.updated_at,'yyyy-MM-dd HH:mm:ss')) as hub_inscan from ds_sqls_hub_task ht where ht.hub_task_state='IN_SCAN' group by ht.shipment_id),

temp41 as (select ht.shipment_id as id, min(unix_timestamp(ht.updated_at,'yyyy-MM-dd HH:mm:ss')) as first_ofd from ds_sqls_hub_task ht where ht.hub_task_state='OUT_SCAN' group by ht.shipment_id),

temp42 as (select ht.shipment_id as id, max(unix_timestamp(ht.updated_at,'yyyy-MM-dd HH:mm:ss')) as last_ofd from ds_sqls_hub_task ht where ht.hub_task_state='OUT_SCAN' group by ht.shipment_id),

temp43 as  (select ht.shipment_id as id,count(ht.shipment_id) as lm_attempt from ds_sqls_hub_task ht where ht.hub_task_state='OUT_SCAN' group by ht.shipment_id),

temp48 as (select ri.return_id as id, ri.created_by as created_by,ri.new_value as new_value,max(ri.created_at) as created_at  from ds_sqls_return_state_transitions ri where ri.new_value='REFUND' group by ri.return_id, ri.created_by,ri.new_value),

temp45 as (select abc.seller_id as id , case when abc.c>3 then 'Mature' else 'New' end as seller_maturity from (select ri.seller_id as seller_id,count(ri.return_id) as c from ds_sqls_returns ri where ri.current_active=1 and ri.state in ('REJECTED','COMPLETE','APPROVED') group by ri.seller_id) as abc),

temp46 as (select dt.shipment_id as id,collect_list(dt.comments) as undel_reasons from ds_sqls_delivery_task dt where dt.state='COMPLETE_REATTEMPT' group by dt.shipment_id),

temp47 as (select ri.return_id as id, max(ri.created_at) as dispute_date from ds_sqls_return_state_transitions ri where ri.attribute='dispute' and ri.new_value='true' group by ri.return_id)

select distinct ri.return_id as return_id, 
ri.state as current_status, 
ri.seller_id as seller_id,  
al.fm_service as fm_service,
orgs.display_name as seller_name, 
temp45.seller_maturity as seller_maturity,
al.seller_city,al.seller_state,
al.seller_pincode as seller_pincode,
al.seller_mobile_number, 
al.seller_address,
ri.buyer_id, orgb.display_name as buyer_name, 
al.buyer_city,al.buyer_state,
al.buyer_pincode as buyer_pincode,al.buyer_mobile_number, 
al.buyer_address as buyer_address,

concat('Account_Number- ',get_json_object(orgb.data,'$.bank_detail.account_number[0]'),', ', 'IFSC_Code- ',get_json_object(orgb.data,'$.bank_detail.ifsc_code'),', ','Bank Name- ',get_json_object(orgb.data,'$.bank_detail.bank_name'),', ','Account_Type- ',get_json_object(orgb.data,'$.bank_detail.acc_type')) as account_details,
get_json_object(orgb.data,'$.bank_detail.acc_holder_name[0]') as account_holder_name,
ri.repay_mode as repayment_mode, 

case 
	when temp48.new_value='REFUND' then 'REFUND' 
	else null 
end as current_repay_mode,


from_unixtime(cast((temp48.created_at+19800000)/1000 as bigint),'dd-MM-yyyy') as refund_conversion_date,
temp48.created_by as refund_converted_by,

ri.reverse_pickup_required as reverse_pickup_required,
(rl2.refund_amount_paise/100) as refund_amount,
(ri.return_goods_value_paise/100) as return_goods_value,
ri.repay_id as repay_id, ri.raised_dispute as dispute_flag, 

from_unixtime(cast((temp47.dispute_date+19800000)/1000 as bigint),'dd-MM-yyyy') as dispute_date,
ri.shipment_id as fwd_shipment_id,
al.seller_order_id as fwd_order_id,
al.vendor as fwd_vendor,
al.awb_number as fwd_awb_number, 
al.category,
al.sub_category,
al.vertical, 
al.lane as lane, 
ls.sla_max as max_sla,
al.weight,
al.shipment_amount,
al.logistics_amount,
al.listing_title as fwd_listing_titles,
al.payment_mode, 
al.order_date as fwd_order_date,
al.rts_date as fwd_rts,
al.first_shipped_date as fwd_shipped,
al.rto_date as fwd_rto ,
al.del_date as fwd_del,
al.fwd_promise_date as fwd_promise,
al.slc_promise_at as slc_promise,
ri.reason as return_reason, 
ri.non_listed_items as non_listed_items,
from_unixtime(cast((temp1.created_at+19800000)/1000 as bigint),'dd-MM-yyyy') as request_created_at,
from_unixtime(cast((temp2.approved_at+19800000)/1000 as bigint),'dd-MM-yyyy') as request_approved_at,
from_unixtime(cast((temp3.approved_at+19800000)/1000 as bigint),'dd-MM-yyyy') as request_cancelled_at,
from_unixtime(cast((temp4.approved_at+19800000)/1000 as bigint),'dd-MM-yyyy') as request_rejected_at, 
datediff(from_unixtime(unix_timestamp()+19800,'yyyy-MM-dd'),from_unixtime(cast((temp1.created_at+19800000)/1000 as bigint),'yyyy-MM-dd')) as request_aging,
case 
	when ri.state='APPROVED' then (((temp2.approved_at+19800000)-(temp1.created_at+19800000))/1000)/86400 else null 
end as resolution_time, 
rit.return_shipment_id as rev_shipment_id, 
temp1a.reschedule_count as reschedule_count,
temp10.vendor as rev_vendor,
temp10.awb as rev_awb_number, 
temp5.status as rev_current_status ,

round(((unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))-((temp6.last_rts+19800000)/1000))/86400),0) as rev_aging, 
case 
	when ri.state='CONFIRMATION_PENDING' then null else from_unixtime(cast((rit.created_at+19800000)/1000 as bigint),'dd-MM-yyyy') 
end as rev_created_at, 
from_unixtime(cast((temp6.last_rts+19800000)/1000 as bigint),'dd-MM-yyyy HH:mm:ss') as rev_rts_date,
from_unixtime(cast(unix_timestamp(from_unixtime(cast((temp6.last_rts+19800000)/1000 as bigint),'dd-MM-yyyy 00:00:00'),'dd-MM-yyyy HH:mm:ss')+(86400*4) as bigint),'dd-MM-yyyy') as rev_pickup_promise_date,
case 
	when temp10.vendor in ('Delhivery','Delhivery Express') and datediff(from_unixtime(cast(unix_timestamp(from_unixtime(cast((temp6.last_rts+19800000)/1000 as bigint),'dd-MM-yyyy HH:mm:ss'),'dd-MM-yyyy HH:mm:ss')+(86400*2) as bigint),'yyyy-MM-dd'), from_unixtime(cast((temp28.fpa_time+19800000)/1000 as bigint),'dd-MM-yyyy 00:00:00'))<=2 then 'within'	
	when temp10.vendor in ('Delhivery','Delhivery Express') and datediff(from_unixtime(cast(unix_timestamp(from_unixtime(cast((temp6.last_rts+19800000)/1000 as bigint),'dd-MM-yyyy HH:mm:ss'),'dd-MM-yyyy HH:mm:ss')+(86400*2) as bigint),'yyyy-MM-dd'), from_unixtime(cast((temp28.fpa_time+19800000)/1000 as bigint),'dd-MM-yyyy 00:00:00'))>2 then 'outside'
	when temp10.vendor='ECOM Express' and datediff(from_unixtime(cast(unix_timestamp(from_unixtime(cast((temp6.last_rts+19800000)/1000 as bigint),'dd-MM-yyyy HH:mm:ss'),'dd-MM-yyyy HH:mm:ss')+(86400*2) as bigint),'yyyy-MM-dd'), from_unixtime(cast((temp28a.fpa_time+19800000)/1000 as bigint),'dd-MM-yyyy 00:00:00'))<=2 then 'within'
	else 'outside'
end as pick_attempt_breach,

case 
when temp10.vendor in ('Delhivery','Delhivery Express') then from_unixtime(cast((temp28.fpa_time+19800000)/1000 as bigint),'dd-MM-yyyy HH:mm:ss') 
when temp10.vendor ='ECOM Express' then from_unixtime(cast((temp28a.fpa_time+19800000)/1000 as bigint),'dd-MM-yyyy HH:mm:ss') 
end as rev_fpa_date,

case 
when temp10.vendor in ('Delhivery','Delhivery Express') then from_unixtime(cast((temp28.lpa_time+19800000)/1000 as bigint),'dd-MM-yyyy HH:mm:ss') 
when temp10.vendor='ECOM Express' then from_unixtime(cast((temp28a.lpa_time+19800000)/1000 as bigint),'dd-MM-yyyy HH:mm:ss') 
end as rev_lpa_date,
ht.hub_task_state as rev_hub_status,

from_unixtime(cast((temp72.first_rad_at+19800000)/1000 as bigint),'dd-MM-yyyy HH:mm:ss') as rad_date,
temp46.undel_reasons as rvp_undel_reason,


case 
	when temp10.vendor in ('Delhivery','Delhivery Express') then temp29.pickup_attempt 
	when temp10.vendor = 'ECOM Express' then temp40.pickup_attempt
end as rev_attempt_count,

temp38a.ndr_reason as ndr_reason,
from_unixtime(cast((temp7.first_shipped+19800000)/1000 as bigint),'dd-MM-yyyy') as rev_shipped_date,
from_unixtime(cast((temp11.cancelled_at+19800000)/1000 as bigint),'dd-MM-yyyy') as cancelled_at, 
from_unixtime(cast((temp8.first_rto+19800000)/1000 as bigint),'dd-MM-yyyy') as rev_rto_date, 
from_unixtime((t44.hub_inscan+19800 ),'dd-MM-yyyy') as rev_inscan_date, 

from_unixtime((t41.first_ofd+19800 ),'dd-MM-yyyy') as rev_first_ofd_date, 
from_unixtime((t42.last_ofd+19800 ),'dd-MM-yyyy') as rev_last_ofd_date, 

from_unixtime(cast((temp9.del_at+19800000)/1000 as bigint),'dd-MM-yyyy') as rev_del_date, 
from_unixtime(cast((temp9.mark_date+19800000)/1000 as bigint),'dd-MM-yyyy') as rev_del_mark_date, 

case when t43.lm_attempt is null then 0 else t43.lm_attempt end as lm_rvp_attempt,

npr.npr_date as npr_action_date,
npr.comment as npr_action,
npr.type as npr_bucket_type,

temp12.shipment_group_id as return_shipment_id, 
temp12.shipment_amount as return_shipment_amount,
temp12.logistics_amount as return_logistics_amount,
get_json_object(so.extra_data,'$.hold_reason') as hold_reason,

from_unixtime(cast((get_json_object(so.extra_data,'$.hold_till_millis')+19800000)/1000 as bigint),'dd-MM-yyyy') as hold_till_date,

case 
	when ri.repay_mode='REPLACEMENT' then ri.repay_id 
	else null 
end as return_order_id,

case 
	when ri.repay_mode='REPLACEMENT' then temp27.order_status
	else null
end as return_order_current_status,

case 
	when sgr.shipment_status is null then 'order_status' 
	else sgr.shipment_status 
end as return_shipment_current_status,

temp12.awb_number as return_awb_number,
temp12.listing_title as return_listing_titles,
temp12.vendor as return_vendor,
temp12.order_type as return_order_type,
temp12.order_date as return_order_date,
temp12.rts_date as return_rts_date, 
temp12.first_shipped_date as return_shipped_date,
temp12.del_date as return_delivered_date, 

case 
	when ri.state='CONFIRMATION_PENDING' and (unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))-((temp1.created_at+19800000)/1000))/86400>=5 then '5+' 
	when ((ri.state='CONFIRMATION_PENDING' and (unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))-((temp1.created_at+19800000)/1000))/86400>=2) and (ri.state='CONFIRMATION_PENDING' and (unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))-((temp1.created_at+19800000)/1000))/86400<5)) then '2_5' 
	when ((ri.state='CONFIRMATION_PENDING' and (unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))-((temp1.created_at+19800000)/1000))/86400>=1) and (ri.state='CONFIRMATION_PENDING' and (unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))-((temp1.created_at+19800000)/1000))/86400<2)) then '1'
	when ri.state='CONFIRMATION_PENDING' and (unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))-((temp1.created_at+19800000)/1000))/86400<1 then '0' 
	else null 
end as pendency_spread,

case 
	when ri.state='APPROVED' and (((temp2.approved_at+19800000)/1000)-((temp1.created_at+19800000)/1000))/86400>=5 then '5+' 
	when ((ri.state='APPROVED' and (((temp2.approved_at+19800000)/1000)-((temp1.created_at+19800000)/1000))/86400>=2) and (ri.state='APPROVED' and (((temp2.approved_at+19800000)/1000)-((temp1.created_at+19800000)/1000))/86400<5)) then '2_5' 
	when ((ri.state='APPROVED' and (((temp2.approved_at+19800000)/1000)-((temp1.created_at+19800000)/1000))/86400>=1) and (ri.state='APPROVED' and (((temp2.approved_at+19800000)/1000)-((temp1.created_at+19800000)/1000))/86400<2)) then '1'
	when ri.state='APPROVED' and (((temp2.approved_at+19800000)/1000)-((temp1.created_at+19800000)/1000))/86400<1 then '0' 
	else null 
end as resolution_spread,

case 
	when temp9.del_at is not null and ((temp9.del_at+19800000)-(temp7.first_shipped+19800000))/86400000<=ls.sla_max then 'Within_SLA' 
	when temp9.del_at is not null and ((temp9.del_at+19800000)-(temp7.first_shipped+19800000))/86400000 > ls.sla_max then 'Outside_SLA' 
	else null 
end as rev_sla_met, 

case 
	when temp12.del_date is not null and (unix_timestamp(temp12.del_date,'dd-MM-yyyy')-unix_timestamp(temp12.first_shipped_date,'dd-MM-yyyy'))/86400 <= ls.sla_max then 'Within_SLA' 
	when temp12.del_date is not null and (unix_timestamp(temp12.del_date,'dd-MM-yyyy')-unix_timestamp(temp12.first_shipped_date,'dd-MM-yyyy'))/86400 > ls.sla_max then 'Outside_SLA' 
	else null 
end as return_sla_met, 

case 
	when temp9.del_at is not null then ((temp9.del_at+19800000)-(temp7.first_shipped+19800000))/86400000 
	else null 
end as rev_s2d, 

case 
	when temp7.first_shipped is not null and (temp7.first_shipped-temp6.first_rts)<0 then 0 when temp7.first_shipped is not null and (temp7.first_shipped-temp6.first_rts) >=0 then (temp7.first_shipped-temp6.first_rts)/86400000 
	else null 
end as rev_pick_up_time,

case 
	when temp12.del_date is not null then (unix_timestamp(temp12.del_date,'dd-MM-yyyy')-unix_timestamp(temp12.first_shipped_date,'dd-MM-yyyy'))/86400 
	else null 
end as return_s2d 

from ds_sqls_returns ri 
left join ds_sqls_orgs orgs on orgs.org_id=ri.seller_id 
left join ds_sqls_orgs orgb on orgb.org_id=ri.buyer_id 
left join temp1 on temp1.id=ri.return_id 
left join temp2 on temp2.id=ri.return_id 
left join temp3 on temp3.id=ri.return_id 
left join temp4 on temp4.id=ri.return_id 
left join ds_sqls_return_items rit on ri.return_id=rit.return_id 
left join all_data al on al.shipment_group_id=ri.shipment_id 
left join all_data al1 on al1.shipment_group_id=rit.return_shipment_id 
left join ds_sqls_lane_sla ls on ls.zone=al.lane 
left join temp5 on temp5.id=rit.return_shipment_id 
left join temp6 on temp6.id=rit.return_shipment_id 
left join temp7 on temp7.id=rit.return_shipment_id 
left join temp8 on temp8.id=rit.return_shipment_id 
left join temp9 on temp9.id=rit.return_shipment_id 
left join all_data temp12 on (temp12.seller_order_id=ri.repay_id and ri.repay_mode='REPLACEMENT') 
left join temp27 on (temp27.id=ri.repay_id and ri.repay_mode='REPLACEMENT')
left join temp10 on temp10.id=rit.return_shipment_id 
left join temp11 on temp11.id=rit.return_shipment_id 
left join ds_sqls_refund_line rl2 on (rl2.refund_line_id=ri.repay_id and ri.repay_mode='REFUND' and rl2.current_active=1) 
left join ds_sqls_shipment_group sgr on (sgr.shipment_group_id=temp12.shipment_group_id and sgr.current_active=1) 
left join temp28 on temp28.id=rit.return_shipment_id 
left join temp28b on temp28b.id=rit.return_shipment_id 
left join temp28a on temp28a.id=rit.return_shipment_id 
left join temp29 on temp29.id=rit.return_shipment_id
left join temp40 on temp40.id=rit.return_shipment_id
left join temp38a on temp38a.id=rit.return_shipment_id
left join temp1a on temp1a.id=ri.return_id
left join npr_data2 npr on ri.return_id=npr.return_id 
left join temp72 on temp72.id=rit.return_shipment_id
left join ds_sqls_seller_order so on ri.repay_id=so.order_id 
left join temp44 t44 on t44.id=rit.return_shipment_id
left join temp41 t41 on t41.id=rit.return_shipment_id
left join temp42 t42 on t42.id=rit.return_shipment_id
left join temp43 t43 on t43.id=rit.return_shipment_id
left join ds_sqls_hub_task ht on ht.shipment_id=rit.return_shipment_id and ht.current_active=1
left join temp48 on temp48.id=ri.return_id
left join temp45 on temp45.id=ri.seller_id
left join temp46 on temp46.id=rit.return_shipment_id 
left join temp47 on temp47.id=ri.return_id 

where ri.current_active=1;
drop table temp72;

drop table returns;
CREATE TABLE IF NOT EXISTS returns STORED AS ORC AS SELECT distinct * FROM returns_temp;
drop table returns_temp;