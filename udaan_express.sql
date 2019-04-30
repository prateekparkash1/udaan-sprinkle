--      set hive.tez.container.size=20000;
--      set hive.tez.java.opts=-Xmx15000M -Xms6000M -Djava.net.preferIPv4Stack=true -XX:NewRatio=8 -XX:+UseNUMA -XX:+UseParallelGC;
--      set hive.auto.convert.join.noconditionaltask.size=10240000000;
--      set hive.auto.convert.join.noconditionaltask.size=4718592000;

     drop table  udaan_express_temp;
     drop table  temp27;

 CREATE TABLE if not exists temp27 STORED AS ORC as with 
     tx1 as (SELECT y1.bid bid,y1.vid vid,y1.state state,y1.sid,
collect_set(y1.ctime)[0] ctime, 
collect_set(y1.otime)[0] otime, 
collect_set(y1.rtime)[0] rtime, 
collect_set(y1.vtime)[0] vtime

from 

(select

x1.bid bid,x1.vid vid,x1.state state,x1.sid,

case when x1.state='CREATED' then x1.ts2 end as ctime,
case when x1.state='OUTSCAN' then x1.ts2 end as otime,
case when x1.state='REGISTERED' then x1.ts2 end as rtime,
case when x1.state='VALIDATED' then x1.ts1 end as vtime

from  (select b1.bag_id bid,b1.vendor_id vid,b1.state state,b1.source_location_id sid,
     min(b1.updated_at) ts1,
     max(b1.updated_at) ts2 

     from ds_sqls_bag b1 
    group by b1.bag_id,b1.vendor_id,b1.state,b1.source_location_id) x1


) y1 group by y1.bid ,y1.vid ,y1.state ,y1.sid)


select 
x1tt.bag_id,x1tt.shipment_id,x1tt.consignment_id,x1tt.shipment_type,x1tt.shipment_validation_check,x1tt.vendor_id,
x1tt.src_id,x1tt.dest_id,x1tt.bag_weight,x1tt.seal_check,
from_unixtime(cast((collect_set(x1tt.bag_outscan_date)[0]+19800000)/1000 as bigint),'dd-MM-yyyy HH:mm:ss') as bag_outscan_date,
from_unixtime(cast((collect_set(x1tt.bag_registration_date)[0]+19800000)/1000 as bigint),'dd-MM-yyyy HH:mm:ss') bag_registration_date,
from_unixtime(cast((collect_set(x1tt.bag_validation_date)[0]+19800000)/1000 as bigint),'dd-MM-yyyy HH:mm:ss') bag_validation_date,
from_unixtime(cast((collect_set(x1tt.bag_creation_date)[0]+19800000)/1000 as bigint),'dd-MM-yyyy HH:mm:ss') bag_creation_date

from 
(select 
x1t.bag_id,x1t.shipment_id,x1t.consignment_id,x1t.shipment_type,x1t.shipment_validation_check,x1t.vendor_id,
x1t.src_id src_id,
x1t.dest_id dest_id,
x1t.bag_weight,x1t.seal_check,x1t.bag_outscan_date,x1t.bag_registration_date,x1t.bag_validation_date,x1t.bag_creation_date


from
     (select 
     *, row_number() over (partition by x1.bag_id,x1.shipment_id order by x1.ts asc) as row_num



from (select distinct
     bsm.bag_id bag_id,
     substr(bsm.shipment_id,0,14) as shipment_id,
     csm.consignment_id consignment_id,
     csm.shipment_type shipment_type,
     bsm.shipment_validated shipment_validation_check,
     b.vendor_id vendor_id,
     l2.org_unit_id dest_id,
     l1.org_unit_id src_id,
     split(get_json_object(b.properties,'$.crossDockingHops'),'"')[1] as cross_dock,
     get_json_object(b.properties,'$.bagWeight')/1000 as bag_weight,
     get_json_object(b.properties,'$.sealIntact') as seal_check,
     x.otime bag_outscan_date,
     x.rtime bag_registration_date,
     x.vtime bag_validation_date,
     x.ctime bag_creation_date,
     t1.ts ts,
     t1.cnt cnt

     from ds_sqls_bag b  
     left join ds_sqls_bag_shipment_mapping bsm on b.bag_id=bsm.bag_id and b.vendor_id=bsm.vendor_id and bsm.current_active = 1
     left join ds_sqls_consignment_shipment_mapping csm on csm.shipment_id=b.bag_id and csm.vendor_id=b.vendor_id and csm.current_active=1
     left join (select bm.bag_id,substr(bm.shipment_id,0,14) shipment_id, count(distinct cm.consignment_id) cnt,min(unix_timestamp(cm.updated_at,'yyyy-MM-dd HH:mm:ss')) ts from ds_sqls_bag_shipment_mapping bm join ds_sqls_consignment_shipment_mapping cm on cm.shipment_id=bm.bag_id and bm.vendor_id=cm.vendor_id group by bm.bag_id,bm.shipment_id) t1 on t1.bag_id=bsm.bag_id and t1.shipment_id=bsm.shipment_id
     left join ds_sqls_location l2 on l2.location_id=b.destination_location_id
     join ds_sqls_location l1 on l1.location_id=b.source_location_id
     
     left join tx1 x on x.bid=bsm.bag_id and x.sid=l1.location_id and x.vid=b.vendor_id   

     where b.current_active=1) x1) x1t) x1tt

group by x1tt.bag_id,x1tt.shipment_id,x1tt.consignment_id,x1tt.shipment_type,x1tt.shipment_validation_check,x1tt.vendor_id,x1tt.src_id,x1tt.dest_id,x1tt.bag_weight,x1tt.seal_check;

     CREATE TABLE if not exists udaan_express_temp STORED AS ORC as with 
     temp1 as (SELECT y1.leg_id leg_id, 
collect_set(y1.no_state_time)[0] no_state_time, 
collect_set(y1.can_time)[0] can_time, 
collect_set(y1.del_time)[0] del_time, 
collect_set(y1.int_time)[0] int_time, 
collect_set(y1.att_time)[0] att_time,
collect_set(y1.rto_time)[0] rto_time 
from 

(select  x1.leg_id leg_id, 

case when x1.state='NO_STATE' then x1.ti2 end as no_state_time,
case when x1.state='CANCELLED' then x1.ti2 end as can_time,
case when x1.state='DELIVERED' then x1.ti2 end as del_time,
case when x1.state='IN_TRANSIT' then x1.ti1 end as int_time,
case when x1.state='ATTEMPTED_UNDELIVERED' then x1.ti2 end as att_time,
case when x1.state='RTO' then x1.ti2 end as rto_time


from  (select sls.leg_id as leg_id,sls.internal_state state,
     max(get_json_object(sls.leg_status_metadata,'$.eventTime')) ti2,
     min(get_json_object(sls.leg_status_metadata,'$.eventTime')) ti1 from ds_sqls_shipment_leg_status sls  
     where sls.internal_state in ('NO_STATE','CANCELLED','DELIVERED','IN_TRANSIT','ATTEMPTED_UNDELIVERED','RTO') group by sls.leg_id,sls.internal_state) x1


) y1 group by y1.leg_id) ,
    
     temp7 as (select sls.leg_id as leg_id,max(get_json_object(sls.leg_status_metadata,'$.attemptCount')) as attempt_count from ds_sqls_shipment_leg_status sls group by sls.leg_id),
     temp8 as (select sls.leg_id as leg_id,collect_list(get_json_object(sls.leg_status_metadata,'$.vendorComments')) as undel_reason from ds_sqls_shipment_leg_status sls  where sls.internal_state='ATTEMPTED_UNDELIVERED' group by sls.leg_id),
     temp9a as (select sg.seller_order_id as id,mi.category as category from ds_sqls_shipment_group sg left join mis_table mi on mi.order_id=sg.seller_order_id where sg.current_active=1 and mi.order_line_created_date >= date_sub('${sprinkle.latest.partition}', ${window}) ),
     temp16 as (select pt.shipment_id as id, from_unixtime(cast((max(CAST(pt.updated_at/1000 as bigint))+19800) as bigint),'dd-MM-yyyy HH:mm:ss') as picked_up_time from ds_sqls_pickup_task pt where pt.state='PICKED_UP' group by pt.shipment_id),
     temp19 as (select se.shipment_id as id, from_unixtime(cast(max((se.eta+19800000))/1000 as bigint),'dd-MM-yyyy HH:mm:ss') as shipment_promise_date from ds_sqls_shipment_execution se where se.leg_type='FWD_DELIVERY' group by se.shipment_id),
     temp20 as (select dt.shipment_id as id,dt.hub_org_unit_id hub_org_unit_id ,max(CAST(dt.updated_at/1000 as bigint)) as sort_time from ds_sqls_hub_task dt where dt.hub_task_state='SORT' group by dt.shipment_id,dt.hub_org_unit_id),
     tsort as (select dt.shipment_id as id,dt.hub_org_unit_id hub_org_unit_id ,max(CAST(dt.updated_at/1000 as bigint)) as sort_time from ds_sqls_hub_task dt where dt.hub_task_state='RTO_SORT' group by dt.shipment_id,dt.hub_org_unit_id),
     temp23 as (select ht.shipment_id as id,ht.hub_org_unit_id as hub_id,ht.hub_task_state as state,from_unixtime((CAST(min(ht.updated_at)/1000 as bigint)+19800),'dd-MM-yyyy HH:mm:ss') as inscan_time from ds_sqls_hub_task ht left join ds_sqls_org_units orguh on orguh.org_unit_id=ht.hub_org_unit_id where ht.hub_task_state in ('IN_SCAN','RTO_IN_SCAN') and ht.third_party_org_id is not null  group by ht.shipment_id,ht.hub_org_unit_id,ht.hub_task_state),
     temp25 as (select ht.shipment_id as id,ht.hub_org_unit_id as hub_id,ht.hub_task_state as state,from_unixtime((CAST(min(ht.updated_at)/1000 as bigint)+19800),'dd-MM-yyyy HH:mm:ss') as outscan_time from ds_sqls_hub_task ht left join ds_sqls_org_units orguh on orguh.org_unit_id=ht.hub_org_unit_id where ht.hub_task_state in ('OUT_SCAN','RTO_OUT_SCAN') and ht.third_party_org_id is not null group by ht.shipment_id,ht.hub_org_unit_id,ht.hub_task_state),
     temp28 as (select csm.consignment_id as consignment_id,substr(csm.shipment_id,1,14) as shipment_id,csm.shipment_validated as shipment_validation_check, c.vendor_id vendor_id,ou1.org_unit_id dest_id,ou2.org_unit_id src_id

          from ds_sqls_consignment_shipment_mapping csm 
          join ds_sqls_consignment c on c.consignment_id=csm.consignment_id and c.state='CREATED' and c.vendor_id=csm.vendor_id
          join ds_sqls_location l1 on l1.location_id=c.destination_location_id
          join ds_sqls_org_units ou1 on ou1.org_unit_id=l1.org_unit_id
          join ds_sqls_location l2 on l2.location_id=c.source_location_id
          join ds_sqls_org_units ou2 on ou2.org_unit_id=l2.org_unit_id
          where csm.shipment_type='INDIVIDUAL_SHIPMENT'),

     temp29 as (select se1.shipment_id as id,dtx.state as state,from_unixtime(min(CAST(dtx.updated_at/1000 as bigint)+19800),'dd-MM-yyyy HH:mm:ss') as time1 from ds_sqls_shipment_execution se1
          left join ds_sqls_delivery_task dtx on dtx.shipment_id=se1.shipment_id and dtx.state in ('OUT_FOR_DELIVERY') group by se1.shipment_id,dtx.state),

     temp21 as (SELECT y1.id id, y1.vendor_id vendor_id, 
collect_set(y1.consignment_register_time)[0] consignment_register_time, 
collect_set(y1.consignment_outscan_time)[0] consignment_outscan_time, 
collect_set(y1.consignment_validation_time)[0] consignment_validation_time, 
collect_set(y1.consignment_validation_pending_time)[0] consignment_validation_pending_time, 
collect_set(y1.consignment_closure_time)[0] consignment_closure_time from 

(select  x1.id, x1.vendor_id, 

case when x1.state='REGISTERED' then x1.time2 end as consignment_register_time,
case when x1.state='OUTSCAN' then x1.time2 end as consignment_outscan_time,
case when x1.state='VALIDATED' then x1.time1 end as consignment_validation_time,
case when x1.state='VALIDATION_PENDING' then x1.time1 end as consignment_validation_pending_time,
case when x1.state='CREATED' then x1.time1 end as consignment_closure_time

from  (select cs.consignment_id as id,cs.vendor_id as vendor_id,cs.state state,
     min(CAST(cs.updated_at/1000 as bigint)) time1,
     max(CAST(cs.updated_at/1000 as bigint)) time2 
     from ds_sqls_consignment cs group by cs.consignment_id,cs.vendor_id,cs.state) x1) 
y1 group by y1.id,y1.vendor_id)

     select  DISTINCT
     se1.shipment_id as shipment_group_id, 
     t27.bag_id as bag_id,
     bx.state as bag_current_status,
     se1.leg_id as leg_id,
     sg1.seller_order_id as order_id,

     case when t28.consignment_id is not null then 'INDIVIDUAL_SHIPMENT' when t27.consignment_id is not null then 'PART_OF_BAG' end as shipment_type,
     case when t28.consignment_id is not null then t28.consignment_id when t27.consignment_id is not null then t27.consignment_id end as consignment_id,

     sg1.awb_number as awb,
     vn.name as leg_vendor,
     orgv.display_name as ovl_vendor,
     sg1.shipment_status as overall_shipment_status,
     se1.state as current_status,
     sls_t.internal_state as current_leg_status,
     se1.leg_type as leg_type,
     se1.sequence_number as sequence_number,
     os.unit_name as shipment_dispatched_from,
     get_json_object(os.unit_address,'$.pincode') as shipment_dispatched_from_pincode,
     ob.unit_name as shipment_dispatched_to,
     get_json_object(ob.unit_address,'$.pincode') as shipment_dispatched_to_pincode,

     t9.category as category,
    
     sg1.shipfrom_pincode as seller_pincode,
     get_json_object(orgus.unit_address,'$.city') as seller_city,
     get_json_object(orgus.unit_address,'$.state') as seller_state,
  
     sg1.shipto_pincode as buyer_pincode, 
     get_json_object(orgub.unit_address,'$.city') as buyer_city,
     get_json_object(orgub.unit_address,'$.state') as buyer_state,
     get_json_object(sg1.box_details,'$.weight_per_box_grams[0]')/1000 as weight, 
     t27.bag_weight as bag_weight,
     t27.seal_check as seal_check,



     from_unixtime(CAST(se1.created_at/1000 as bigint),'yyyy-MM-dd') as partition_date,
     t27.bag_creation_date  bag_creation_date,
     t27.bag_outscan_date as bag_outscan_date,
     t27.bag_registration_date as bag_registration_date,
     t27.bag_validation_date as bag_validation_date,


     case when from_unixtime(t21.consignment_register_time+19800,'dd-MM-yyyy HH:mm:ss') is not null then from_unixtime(t21.consignment_register_time+19800,'dd-MM-yyyy HH:mm:ss') 
     when from_unixtime(t21a.consignment_register_time+19800,'dd-MM-yyyy HH:mm:ss') is not null then from_unixtime(t21a.consignment_register_time+19800,'dd-MM-yyyy HH:mm:ss') 
     else null
     end as consignment_register_time,

     case when from_unixtime(t21.consignment_outscan_time+19800,'dd-MM-yyyy HH:mm:ss') is not null then from_unixtime(t21.consignment_outscan_time+19800,'dd-MM-yyyy HH:mm:ss') 
     when from_unixtime(t21a.consignment_outscan_time+19800,'dd-MM-yyyy HH:mm:ss') is not null then from_unixtime(t21a.consignment_outscan_time+19800,'dd-MM-yyyy HH:mm:ss')
     else null 
     end as consignment_outscan_time,

     case when from_unixtime(t21.consignment_validation_time+19800,'dd-MM-yyyy HH:mm:ss') is not null then from_unixtime(t21.consignment_validation_time+19800,'dd-MM-yyyy HH:mm:ss') 
     when from_unixtime(t21a.consignment_validation_time+19800,'dd-MM-yyyy HH:mm:ss') is not null then from_unixtime(t21a.consignment_validation_time+19800,'dd-MM-yyyy HH:mm:ss')
     else null 
     end as consignment_validation_time,

     case when from_unixtime(t21.consignment_validation_pending_time+19800,'dd-MM-yyyy HH:mm:ss') is not null then from_unixtime(t21.consignment_validation_pending_time+19800,'dd-MM-yyyy HH:mm:ss') 
     when from_unixtime(t21a.consignment_validation_pending_time+19800,'dd-MM-yyyy HH:mm:ss') is not null then from_unixtime(t21a.consignment_validation_pending_time+19800,'dd-MM-yyyy HH:mm:ss')
     else null 
     end as consignment_validation_pending_time,

     case when from_unixtime(t21.consignment_closure_time+19800,'dd-MM-yyyy HH:mm:ss') is not null then from_unixtime(t21.consignment_closure_time+19800,'dd-MM-yyyy HH:mm:ss') 
     when from_unixtime(t21a.consignment_closure_time+19800,'dd-MM-yyyy HH:mm:ss') is not null then from_unixtime(t21a.consignment_closure_time+19800,'dd-MM-yyyy HH:mm:ss')
     else null 
     end as consignment_closure_time,

     case 
          when t27.shipment_id is not null and t27.shipment_validation_check=1 then 'VALIDATED'
          when t27.shipment_id is not null and t27.shipment_validation_check!=1 then 'NOT_VALIDATED'
          when t28.shipment_id is not null and t28.shipment_validation_check=1 then 'VALIDATED'
          when t28.shipment_id is not null and t28.shipment_validation_check!=1 then 'NOT_VALIDATED'
          else null
     end as shipment_validation_check,

     case when se1.leg_type like '%RTO%' then from_unixtime((tsort.sort_time+19800),'dd-MM-yyyy HH:mm:ss') 
     else from_unixtime((temp20.sort_time+19800),'dd-MM-yyyy HH:mm:ss') 
     end as shipment_sort_time,



      -- vehicle details  -- 
     case when t28.consignment_id is not null then vda1.trip_id when t27.consignment_id is not null then vda2.trip_id end as trip_id,
     case when t28.consignment_id is not null then vda1.type when t27.consignment_id is not null then vda2.type end as trip_type,

     case when t28.consignment_id is not null then vda1.vehicle_id when t27.consignment_id is not null then vda2.vehicle_id end as vehicle_id,

     case when t28.consignment_id is not null then vda1.vehicle_number when t27.consignment_id is not null then vda2.vehicle_number end as vehicle_number,

     case when t28.consignment_id is not null then vda1.fleet_vendor when t27.consignment_id is not null then vda2.fleet_vendor end as fleet_vendor,

     case when t28.consignment_id is not null then vda1.scheduling_type when t27.consignment_id is not null then vda2.scheduling_type end as scheduling_type,

     case when t28.consignment_id is not null then vda1.mode when t27.consignment_id is not null then vda2.mode end as mode,

     case when t28.consignment_id is not null then vda1.source_arrived_time when t27.consignment_id is not null then vda2.source_arrived_time end as source_arrived_time,

     case when t28.consignment_id is not null then vda1.source_depart_time when t27.consignment_id is not null then vda2.source_depart_time end as source_depart_time,

     case when t28.consignment_id is not null then vda1.destination_arrived_time when t27.consignment_id is not null then vda2.destination_arrived_time end as destination_arrived_time,

     from_unixtime(cast((se1.eta+19800000)/1000 as bigint),'dd-MM-yyyy HH:mm:ss') as leg_sla_date,

     from_unixtime(cast((t1.no_state_time+19800000)/1000 as bigint),'dd-MM-yyyy HH:mm:ss') as no_state_date,
     from_unixtime(cast((t1.can_time+19800000)/1000 as bigint),'dd-MM-yyyy HH:mm:ss') as cancel_date,
     t16.picked_up_time as picked_up_time,

     case 
          when se1.leg_type='PICKUP' then t23x.inscan_time  
          when se1.leg_type not like '%RTO%' and dt1.ts is null then t23.inscan_time   
          when dt1.ts is not null and se1.leg_type not like '%RTO%' then from_unixtime((CAST((dt1.ts)/1000 as bigint)+19800),'dd-MM-yyyy HH:mm:ss')
          when se1.leg_type like '%RTO%' and se1.leg_type!='RTO_DELIVERY' and dt1.ts is null then t23a.inscan_time
          when se1.leg_type like '%RTO%' and se1.leg_type!='RTO_DELIVERY' and dt1.ts is not null then from_unixtime((CAST((dt1.ts)/1000 as bigint)+19800),'dd-MM-yyyy HH:mm:ss')
          when se1.leg_type='RTO_DELIVERY' then t29.time1 
     end as inscan_time,

     case  
          when se1.leg_type='PICKUP' then t25x.outscan_time  
          when se1.leg_type not like '%RTO%' and dt2.ts is null then t25.outscan_time
          when se1.leg_type not like '%RTO%' and dt2.ts is not null then from_unixtime((CAST((dt2.ts)/1000 as bigint)+19800),'dd-MM-yyyy HH:mm:ss')
          when se1.leg_type like '%RTO%' and se1.leg_type!='RTO_DELIVERY' and dt2.ts is null then t25a.outscan_time   
          when se1.leg_type like '%RTO%' and se1.leg_type!='RTO_DELIVERY' and dt2.ts is not null then from_unixtime((CAST((dt2.ts)/1000 as bigint)+19800),'dd-MM-yyyy HH:mm:ss')
          when se1.leg_type='RTO_DELIVERY' then t29a.time1 
     end as outscan_time,


     from_unixtime(cast((t1.int_time+19800000)/1000 as bigint),'dd-MM-yyyy HH:mm:ss') as leg_transit_date,

     case 
          when t7.attempt_count is null then from_unixtime(cast((t1.del_time+19800000)/1000 as bigint),'dd-MM-yyyy HH:mm:ss')
          else from_unixtime(cast((t1.att_time+19800000)/1000 as bigint),'dd-MM-yyyy HH:mm:ss') 
     end as la_time,

     case 
          when t7.attempt_count is null then from_unixtime(cast((t1.del_time+19800000)/1000 as bigint),'dd-MM-yyyy HH:mm:ss')
          else from_unixtime(cast((t1.att_time+19800000)/1000 as bigint),'dd-MM-yyyy HH:mm:ss') 
     end as fa_time,

     from_unixtime(cast((t1.rto_time+19800000)/1000 as bigint),'dd-MM-yyyy HH:mm:ss') as last_leg_rto_time,
     from_unixtime(cast((t1.del_time+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') as delivery_time,

     case 
          when t7.attempt_count is null then 0
          else t7.attempt_count
     end as attempt_count,
     t8.undel_reason as undel_reason,
     t19.shipment_promise_date as shipment_promise_date,
     get_json_object(get_json_object(get_json_object(vs.vendors,'$.[0]'),'$.vendorProperties'),'$.sla') as sla_input



     from (select *, from_unixtime(CAST(se1.created_at/1000 as bigint), 'yyyy-MM-dd') created_day from ds_sqls_shipment_execution se1 where from_unixtime(CAST(se1.created_at/1000 as bigint), 'yyyy-MM-dd') >= date_sub('${sprinkle.latest.partition}', ${window})) se1 
     left join temp1 t1 on t1.leg_id=se1.leg_id 






     left join temp7 t7 on t7.leg_id=se1.leg_id
     left join temp8 t8 on t8.leg_id=se1.leg_id

     left join ds_sqls_shipment_group sg1 on sg1.shipment_group_id=se1.shipment_id
     left join ds_sqls_orgs orgv on orgv.org_id=sg1.third_party_provider_id
     left join ds_sqls_org_units os on os.org_unit_id=se1.from_org_unit_id
     left join ds_sqls_org_units ob on ob.org_unit_id=se1.to_org_unit_id
   
     left join temp9a t9 on t9.id=sg1.seller_order_id 

     left join ds_sqls_vendor vn on vn.vendor_id=se1.vendor_org_id
     left join temp16 t16 on t16.id=sg1.shipment_group_id
     left join temp19 t19 on t19.id=se1.shipment_id
     left join temp20 on temp20.id=se1.shipment_id and temp20.hub_org_unit_id=se1.from_org_unit_id
     left join tsort on tsort.id=se1.shipment_id and tsort.hub_org_unit_id=se1.from_org_unit_id
     left join temp28 t28 on t28.shipment_id=se1.shipment_id and t28.vendor_id=se1.vendor_org_id and t28.dest_id=se1.to_org_unit_id and t28.src_id=se1.from_org_unit_id
     left join temp27 t27 on t27.shipment_id=se1.shipment_id and t27.vendor_id=se1.vendor_org_id and t27.dest_id=se1.to_org_unit_id and t27.src_id=se1.from_org_unit_id
    
     left join temp21 t21 on t21.id=t27.consignment_id and se1.vendor_org_id=t21.vendor_id 
     left join temp21 t21a on t21a.id=t28.consignment_id and se1.vendor_org_id=t21a.vendor_id

     left join temp23 t23x on t23x.id=se1.shipment_id and t23x.hub_id=se1.to_org_unit_id and t23x.state='IN_SCAN' and se1.leg_type='PICKUP'
     left join ds_sqls_bag bx on bx.bag_id=t27.bag_id and bx.current_active=1
     left join temp25 t25x on t25x.id=se1.shipment_id and t25x.hub_id=se1.to_org_unit_id and t25x.state='OUT_SCAN' and se1.leg_type='PICKUP'

     left join temp23 t23 on t23.id=se1.shipment_id and t23.hub_id=se1.from_org_unit_id and t23.state='IN_SCAN'
     left join temp23 t23a on t23a.id=se1.shipment_id and t23a.hub_id=se1.from_org_unit_id and t23a.state='RTO_IN_SCAN'
     left join temp29 t29 on t29.id=se1.shipment_id and t29.state='CREATED'

     left join temp25 t25 on t25.id=se1.shipment_id and t25.hub_id=se1.from_org_unit_id and t25.state='OUT_SCAN'
     left join temp25 t25a on t25a.id=se1.shipment_id and t25a.hub_id=se1.from_org_unit_id and t25a.state='RTO_OUT_SCAN'
     left join temp29 t29a on t29a.id=se1.shipment_id and t29a.state='OUT_FOR_DELIVERY'
     left join ds_sqls_shipment_leg_status sls_t on sls_t.shipment_id=se1.shipment_id and sls_t.leg_id=se1.leg_id and sls_t.current_active=1
     left join ds_sqls_org_units orgus on orgus.org_unit_id=sg1.shipfrom_org_unit_id
     left join ds_sqls_org_units orgub on orgub.org_unit_id=sg1.shipto_org_unit_id
     left join (select x.shipment_id, x.hub_org_unit_id,min(x.updated_at) ts from  ds_sqls_cross_dock_hub_task x where x.state='IN_SCAN' group by x.shipment_id, x.hub_org_unit_id) dt1 on dt1.shipment_id=t27.bag_id and se1.to_org_unit_id=dt1.hub_org_unit_id
     left join (select x.shipment_id, x.hub_org_unit_id,min(x.updated_at) ts from  ds_sqls_cross_dock_hub_task x where x.state='OUT_SCAN' group by x.shipment_id, x.hub_org_unit_id) dt2 on dt2.shipment_id=t27.bag_id and se1.to_org_unit_id=dt2.hub_org_unit_id
    


      -- vehicle details  --

     left join 
     (select distinct 
     m1.cid cid,
     m1.rid trip_id,
     m1.type type,
     vht.vehicle_id vehicle_id, 
     ste.from_org_unit_id from_org_unit_id,
     sv.vehicle_number vehicle_number, 
     sv.fleet_vendor fleet_vendor,
     sv.scheduling_type scheduling_type,
     sv.mode mode,
     from_unixtime(tx1.ts+19800,'yyyy-MM-dd HH:mm:ss') as source_arrived_time,
     from_unixtime(tx2.ts+19800,'yyyy-MM-dd HH:mm:ss') as source_depart_time,
     from_unixtime(tx3.ts+19800,'yyyy-MM-dd HH:mm:ss') as destination_arrived_time


     from (select split(t1.task_id,'"')[3] as cid,t1.rid rid,t1.leg_id leg_id,t1.type type from (SELECT task_id as task_id,rid as rid,leg_id as leg_id,type  FROM (SELECT ab.trip_id rid,ab.leg_id leg_id,ab.type type, split(substr(ab.tasks,2,length(ab.tasks)-2),',') abc FROM ds_sqls_trip_tasks ab ) temp LATERAL VIEW explode(abc) adtable AS task_id 

     where task_id like '%consignmentId%') t1) m1
     left join ds_sqls_vehicle_hub_task vht on vht.ref_id=m1.rid
     left join ds_sqls_vehicle sv on sv.vehicle_id=vht.vehicle_id 
     left join ds_sqls_trip_leg_status str on str.trip_id=m1.rid 
     left join ds_sqls_trip_execution ste on ste.trip_id=m1.rid 
     left join (select sts.trip_id trip_id,sts.state state, min(CAST(sts.updated_at/1000 as bigint)) ts from ds_sqls_trip_leg_status sts group by sts.trip_id ,sts.state) tx1 on tx1.state='ARRIVED_SOURCE' and tx1.trip_id=m1.rid 
     left join (select sts.trip_id trip_id,sts.state state, min(CAST(sts.updated_at/1000 as bigint)) ts from ds_sqls_trip_leg_status sts group by sts.trip_id ,sts.state) tx2 on tx2.state='DEPARTED_SOURCE' and tx2.trip_id=m1.rid 
     left join (select sts.trip_id trip_id,sts.state state, min(CAST(sts.updated_at/1000 as bigint)) ts from ds_sqls_trip_leg_status sts group by sts.trip_id ,sts.state) tx3 on tx3.state='ARRIVED_DESTINATION' and tx3.trip_id=m1.rid 
       ) vda1 on vda1.cid=t28.consignment_id and vda1.from_org_unit_id=se1.from_org_unit_id 


     left join 
     (select distinct 
     m1.cid cid,
     m1.rid trip_id,
     m1.type type,
     vht.vehicle_id vehicle_id, 
     ste.from_org_unit_id from_org_unit_id,
     sv.vehicle_number vehicle_number, 
     sv.fleet_vendor fleet_vendor,
     sv.scheduling_type scheduling_type,
     sv.mode mode,
     from_unixtime(tx1.ts+19800,'yyyy-MM-dd HH:mm:ss') as source_arrived_time,
     from_unixtime(tx2.ts+19800,'yyyy-MM-dd HH:mm:ss') as source_depart_time,
     from_unixtime(tx3.ts+19800,'yyyy-MM-dd HH:mm:ss') as destination_arrived_time

     from (select split(t1.task_id,'"')[3] as cid,t1.rid rid,t1.leg_id leg_id,t1.type type from (SELECT task_id as task_id,rid as rid,leg_id as leg_id,type  FROM (SELECT ab.trip_id rid,ab.leg_id leg_id,ab.type type, split(substr(ab.tasks,2,length(ab.tasks)-2),',') abc FROM ds_sqls_trip_tasks ab ) temp LATERAL VIEW explode(abc) adtable AS task_id 

     where task_id like '%consignmentId%') t1) m1 
     left join ds_sqls_vehicle_hub_task vht on vht.ref_id=m1.rid 
     left join ds_sqls_vehicle sv on sv.vehicle_id=vht.vehicle_id 
     left join ds_sqls_trip_leg_status str on str.trip_id=m1.rid
     left join ds_sqls_trip_execution ste on ste.trip_id=m1.rid 
     left join (select sts.trip_id trip_id,sts.state state, min(CAST(sts.updated_at/1000 as bigint)) ts from ds_sqls_trip_leg_status sts group by sts.trip_id ,sts.state) tx1 on tx1.state='ARRIVED_SOURCE' and tx1.trip_id=m1.rid
     left join (select sts.trip_id trip_id,sts.state state, min(CAST(sts.updated_at/1000 as bigint)) ts from ds_sqls_trip_leg_status sts group by sts.trip_id ,sts.state) tx2 on tx2.state='DEPARTED_SOURCE' and tx2.trip_id=m1.rid 
     left join (select sts.trip_id trip_id,sts.state state, min(CAST(sts.updated_at/1000 as bigint)) ts from ds_sqls_trip_leg_status sts group by sts.trip_id ,sts.state) tx3 on tx3.state='ARRIVED_DESTINATION' and tx3.trip_id=m1.rid)

     vda2 on vda2.cid=t27.consignment_id and vda2.from_org_unit_id=se1.from_org_unit_id


      -- vehicle details end  -- 

     left join ds_sqls_vendor vr1 on vr1.name=vn.name
     left join ds_sqls_vendor_serviceability vs on vr1.vendor_id=get_json_object(substr(vs.vendors,2,length(vs.vendors)-1),'$.vendorId') and vs.from_pincode=get_json_object(os.unit_address,'$.pincode') and vs.to_pincode=get_json_object(ob.unit_address,'$.pincode')

     where se1.current_active=1 and sg1.current_active=1 and orgv.display_name='Udaan_Express' and se1.created_day >= date_sub('${sprinkle.latest.partition}', ${window});


     drop table  temp27;

     