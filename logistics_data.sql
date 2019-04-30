  drop table all_data_temp;
  drop table temp1;
  drop table temp_t;
  drop table temp6;
  drop table temp2;
  drop table temp3;
  drop table temp4;
  drop table temp21;
  drop table temp21a;
  drop table temp25;
  drop table temp31;
  drop table temp38;
  DROP TABLE temp77;

  CREATE TABLE if not exists temp1 STORED AS ORC as select sst1.shipment_group_id as id, min(sst1.created_at) as first_rts,max(sst1.created_at) as last_rts from ds_sqls_shipment_group_metadata sst1 where sst1.state='SHIPMENT_RTS' and from_unixtime(cast((sst1.created_at+19800000)/1000 as bigint), 'yyyy-MM-dd') >= date_sub('${sprinkle.latest.partition}', ${window}) group by sst1.shipment_group_id;

  CREATE TABLE if not exists temp1p STORED AS ORC as select sst2.shipment_group_id as id, min(sst2.created_at) as last_packed_ts from ds_sqls_shipment_group_metadata sst2 where sst2.state='SHIPMENT_PACKED' and from_unixtime(cast((sst2.created_at+19800000)/1000 as bigint), 'yyyy-MM-dd') >= date_sub('${sprinkle.latest.partition}', ${window}) group by sst2.shipment_group_id;

  CREATE TABLE if not exists temp2 STORED AS ORC as select so.order_id as order_id,so.created_at as order_at, so.order_type as order_type, get_json_object(so.extra_data,'$.selected_payment_method') as payment_mode,om.type as type from ds_sqls_seller_order so left join ds_sqls_seller_order_line sol on so.order_id=sol.seller_order_id left join ds_csv_order_mapping om on om.order_id=so.order_id where (so.order_status!= 'SELLER_ORDER_DRAFT' AND so.order_status!= 'SELLER_ORDER_RESERVED') and from_unixtime(cast((so.created_at+19800000)/1000 as bigint), 'yyyy-MM-dd') >= date_sub('${sprinkle.latest.partition}', ${window}) group by so.order_id,so.created_at, so.order_type, get_json_object(so.extra_data,'$.selected_payment_method'),om.type;

  CREATE TABLE if not exists temp3 STORED AS ORC as select sg.shipment_group_id as id,inv.invoice_type as type,sum(inv.amount_in_paisa) as amount from ds_sqls_shipment_group sg left join ds_sqls_invoice_shipment_group_association isa on sg.shipment_group_id=isa.shipment_group_id left join ds_sqls_invoice inv on inv.invoice_id=isa.invoice_id where inv.invoice_type in ('GOODS','LOGISTIC_SERVICE') and sg.current_active=1 and from_unixtime(cast((inv.created_at+19800000)/1000 as bigint), 'yyyy-MM-dd') >= date_sub('${sprinkle.latest.partition}', ${window}) group by sg.shipment_group_id,inv.invoice_type;

  CREATE TABLE if not exists temp4 STORED AS ORC as select sg.seller_order_id as id,mi.order_status as order_status,mi.category as category, mi.sub_category as sub,mi.vertical as vertical,mi.buyer_overall_newrep as new_or_old ,row_number() over (partition by sg.seller_order_id order by mi.category) as row_num from ds_sqls_shipment_group sg left join mis_table mi on mi.order_id=sg.seller_order_id where sg.current_active=1 and from_unixtime(cast((sg.created_at+19800000)/1000 as bigint), 'yyyy-MM-dd') >= date_sub('${sprinkle.latest.partition}', ${window});

  CREATE TABLE if not exists temp6 STORED AS ORC as select sst1.shipment_group_id as id, min(sst1.value) as first_shipped, max(sst1.value) as last_shipped from ds_sqls_shipment_group_metadata sst1 where sst1.state='SHIPMENT_IN_TRANSIT' and sst1.key_attrib='event_time' and from_unixtime(cast((sst1.created_at+19800000)/1000 as bigint), 'yyyy-MM-dd') >= date_sub('${sprinkle.latest.partition}', ${window}) group by sst1.shipment_group_id;

  CREATE TABLE if not exists temp_t STORED AS ORC as select sst1.shipment_group_id as id, sst1.state as state, min(sst1.value) as first_time, max(sst1.value) as last_time from ds_sqls_shipment_group_metadata sst1 where sst1.key_attrib='event_time' and from_unixtime(cast((sst1.created_at+19800000)/1000 as bigint), 'yyyy-MM-dd') >= date_sub('${sprinkle.latest.partition}', ${window}) group by sst1.shipment_group_id,sst1.state;
 
  CREATE TABLE if not exists temp21 STORED AS ORC as select ht.shipment_id as id,ht.hub_task_state as state,from_unixtime((min(CAST(ht.updated_at/1000 as bigint)+19800)),'dd-MM-yyyy HH:mm:ss') as first_time , from_unixtime((max(CAST(ht.updated_at/1000 as bigint)+19800)),'dd-MM-yyyy HH:mm:ss') as last_time from ds_sqls_hub_task ht where ht.hub_task_state in ('IN_SCAN','OUT_SCAN') and ht.third_party_org_id is not null and from_unixtime(CAST(ht.updated_at/1000 as bigint), 'yyyy-MM-dd') >= date_sub('${sprinkle.latest.partition}', ${window}) group by ht.shipment_id,ht.hub_task_state;

  CREATE TABLE if not exists temp21a STORED AS ORC as select ht.shipment_id as id,ht.hub_task_state as state,from_unixtime((min(CAST(ht.updated_at/1000 as bigint)+19800)),'dd-MM-yyyy HH:mm:ss') as first_time , from_unixtime((max(CAST(ht.updated_at/1000 as bigint)+19800)),'dd-MM-yyyy HH:mm:ss') as last_time from ds_sqls_hub_task ht where ht.hub_task_state in ('RTO_IN_SCAN','RTO_OUT_SCAN') and from_unixtime(CAST(ht.updated_at/1000 as bigint), 'yyyy-MM-dd') >= date_sub('${sprinkle.latest.partition}', ${window}) group by ht.shipment_id,ht.hub_task_state;

  CREATE TABLE if not exists temp25 STORED AS ORC as select mi.order_id as id, sum(mi.unit_qty) as unit_qty,sum(mi.total_line_amount) as order_amount, collect_list(mi.listing_title) as title, collect_list(mi.listing_id) as listing_id,concat_ws(',',collect_list(get_json_object(s.config,'$.dtr_config'))) as dtr_tag,concat_ws(',',collect_list(wh.id)) as wh_tag from mis_table mi left join ds_stream_listing s on s.listing_id=mi.listing_id left join ds_sqls_wh_sku_reservation wh on split(wh.id,'-')[1]=mi.order_line_id and substr(split(wh.id,'-')[1],0,2)='OL'
 where mi.order_date >= date_sub('${sprinkle.latest.partition}', ${window})
  group by mi.order_id;

  CREATE TABLE if not exists temp31 STORED AS ORC as select vsd.shipment_group_id as id,min(vsd.date_time) as rad_time from ds_sqls_vendor_shipment_details vsd where from_unixtime(cast((vsd.date_time+19800000)/1000 as bigint), 'yyyy-MM-dd') >= date_sub('${sprinkle.latest.partition}', ${window}) and (vsd.comment='Consignment received at destination city' or vsd.comment='Received at destination city' or vsd.status_external='RAD') group by vsd.shipment_group_id;

  CREATE TABLE if not exists temp38 STORED AS ORC as select vsd.shipment_group_id as id, vsd.comment as ndr_reason,row_number() over (partition by vsd.shipment_group_id order by vsd.date_time desc) as row_num from  ds_sqls_vendor_shipment_details vsd  left join ds_sqls_orgs orgv on orgv.org_id=vsd.vendor_org_id  where from_unixtime(cast((vsd.date_time+19800000)/1000 as bigint), 'yyyy-MM-dd') >= date_sub('${sprinkle.latest.partition}', ${window}) and  ( vsd.status_internal not like '%RTO%' and ((orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') and vsd.status_external='UD_Pending') or (orgv.display_name in ('ECOM Express','Ecom Surface') and  vsd.status_external in ('200','201','202','206','207','208','209','210','211','212','213','214','215','216','217','221','222','223','224','225','226','227','228','229','231','232','233','234','235','236','237','238','240','300','311','302','302','317','321','325','331','332','333','340','343','344','345','666','888','218','219','220','2425','1225','2443')) or ((orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface') and vsd.status_external='UD'));

  CREATE TABLE if not exists temp77 STORED AS ORC as select sg.shipment_group_id as id,count(dt.state) as lm_attempt_count from ds_sqls_shipment_group sg left join ds_sqls_delivery_task dt on dt.shipment_id=sg.shipment_group_id where sg.current_active=1 and dt.state='OUT_FOR_DELIVERY' and from_unixtime(cast((sg.created_at+19800000)/1000 as bigint), 'yyyy-MM-dd') >= date_sub('${sprinkle.latest.partition}', ${window}) group by sg.shipment_group_id;

  CREATE TABLE if not exists all_data_temp STORED AS ORC as with
  temp45 as (select isa.shipment_group_id as id,inv.invoice_id as invoice_id, inv.invoice_ref_id as invoice_ref_id,inv.num_of_items as rts_qty from ds_sqls_invoice_shipment_group_association isa  left join ds_sqls_invoice inv on inv.invoice_id=isa.invoice_id where from_unixtime(cast((inv.created_at+19800000)/1000 as bigint), 'yyyy-MM-dd') >= date_sub('${sprinkle.latest.partition}', ${window}) and  inv.invoice_type='GOODS' and inv.current_active=1),

  vsx as (select v1.shipment_group_id as id, v1.comment as ndr_reason,row_number() over (partition by v1.shipment_group_id order by v1.date_time desc) as row_num from ds_sqls_vendor_shipment_details v1 join 
 (select vsd.shipment_group_id as id, vsd.comment as ndr_reason,row_number() over (partition by vsd.shipment_group_id order by vsd.date_time desc) as row_num 
  from  ds_sqls_vendor_shipment_details vsd left join ds_sqls_orgs orgv on orgv.org_id=vsd.vendor_org_id where vsd.status_internal not like '%RTO%'  and (orgv.display_name in ('ECOM Express','Ecom Surface') and vsd.comment like '%1225%'   ) or 
  (orgv.display_name in ('Delhivery Express','Delhivery') and 
    vsd.comment in ('%Reattempt - As per NDR instructions%','%Reached maximum attempt count%','%Returned as per Client Instructions%'))) x1 on x1.id = v1.shipment_group_id and v1.status_internal not like '%RTO%' and v1.comment not like '%RTO%'),

  topr as (select sos.order_id as od, max(sos.created_at) as pr_time from ds_sqls_seller_order_state_transition sos where sos.new_state='SELLER_ORDER_PROCESSING' and from_unixtime(cast((sos.created_at+19800000)/1000 as bigint), 'yyyy-MM-dd') >= date_sub('${sprinkle.latest.partition}', ${window}) group by sos.order_id),

  temp47 as (select pt.shipment_id as id , from_unixtime((max(CAST(pt.updated_at/1000 as bigint))+19800),'dd-MM-yyyy HH:mm:ss') as picked_up_time from ds_sqls_pickup_task pt where pt.state='PICKED_UP' and from_unixtime(CAST(pt.updated_at/1000 as bigint), 'yyyy-MM-dd') >= date_sub('${sprinkle.latest.partition}', ${window}) group by pt.shipment_id),

  temp48 as (select pt.shipment_id as id, from_unixtime((min(CAST(pt.updated_at/1000 as bigint))+19800),'dd-MM-yyyy HH:mm:ss') as first_pickup_time, from_unixtime((max(CAST(pt.updated_at/1000 as bigint))+19800),'dd-MM-yyyy HH:mm:ss') as last_pickup_time, count(pt.shipment_id) as pickup_count from ds_sqls_pickup_task pt where pt.state='OUT_FOR_PICKUP' and from_unixtime(CAST(pt.updated_at/1000 as bigint), 'yyyy-MM-dd') >= date_sub('${sprinkle.latest.partition}', ${window}) group by pt.shipment_id),

  temp50 as (select pt.shipment_id as id, pt.comments as pickup_failure_reason, row_number() over (partition by pt.shipment_id order by CAST(pt.updated_at/1000 as bigint) desc) as row_num from ds_sqls_pickup_task pt where pt.state='PICKUP_ATTEMPTED' and from_unixtime(CAST(pt.updated_at/1000 as bigint), 'yyyy-MM-dd') >= date_sub('${sprinkle.latest.partition}', ${window})),

  temp51a as (select vsd.shipment_group_id as id, min(vsd.date_time) as fa_time, max(vsd.date_time) as la_time from ds_sqls_vendor_shipment_details vsd where vsd.status_external in ('006','OFD','UD_Dispatched') and from_unixtime(cast((vsd.date_time+19800000)/1000 as bigint), 'yyyy-MM-dd') >= date_sub('${sprinkle.latest.partition}', ${window}) group by vsd.shipment_group_id),

  temp51ac as (select a.id as id,count(a.attempt_count) as attempt_count from
  (select distinct vsd.shipment_group_id as id,from_unixtime(cast((vsd.date_time+19800000)/1000 as bigint),'dd-MM-yyyy') as attempt_count

  from ds_sqls_vendor_shipment_details vsd where from_unixtime(cast((vsd.date_time+19800000)/1000 as bigint), 'yyyy-MM-dd') >= date_sub('${sprinkle.latest.partition}', ${window}) and  vsd.status_external in ('006','OFD','UD_Dispatched') and vsd.status_internal in ('SHIPMENT_IN_TRANSIT','SHIPMENT_ATTEMPTED_UNDELIVERED') ) a group by a.id),

  temp52 as (select sg.shipment_group_id as id,min(vsd.date_time) as rad_time from ds_sqls_shipment_group sg left join ds_sqls_orgs orgv on orgv.org_id=sg.third_party_provider_id left join ds_sqls_vendor_shipment_details vsd on vsd.shipment_group_id=sg.shipment_group_id left join ds_sqls_org_units orgub on orgub.org_unit_id=sg.shipto_org_unit_id
    left join ds_csv_ecom_city_map em on em.pincode=sg.shipto_pincode
    where sg.current_active=1 and lower(vsd.location) like '%service center%' and vsd.location like concat('%',em.city,'%') and from_unixtime(cast((vsd.date_time+19800000)/1000 as bigint), 'yyyy-MM-dd') >= date_sub('${sprinkle.latest.partition}', ${window})

    group by vsd.shipment_group_id,sg.shipment_group_id),

  temp67 as (select vsd.shipment_group_id as id, count(vsd.shipment_group_id) as rto_attempt_count from ds_sqls_vendor_shipment_details vsd where vsd.status_internal='SHIPMENT_RTO' and vsd.status_external in (6,'RT_Dispatched','RTO-OFD') and from_unixtime(cast((vsd.date_time+19800000)/1000 as bigint), 'yyyy-MM-dd') >= date_sub('${sprinkle.latest.partition}', ${window}) group by vsd.shipment_group_id),

  temp71 as (select sg.shipment_group_id AS id,count(sst.shipment_group_id) as ul_attempt_count FROM ds_sqls_shipment_group sg left join ds_sqls_shipment_group_state_transition sst on sg.shipment_group_id=sst.shipment_group_id where from_unixtime(cast((sg.created_at+19800000)/1000 as bigint), 'yyyy-MM-dd') >= date_sub('${sprinkle.latest.partition}', ${window}) and sg.current_active=1 and sst.new_state='SHIPMENT_ATTEMPTED_UNDELIVERED' GROUP BY sst.shipment_group_id,sg.shipment_group_id),

  temp1a as (select sg.shipment_group_id as id, case when get_json_object(sg.snapshot,'$.data.rto_time_epoch') is not null
    then from_unixtime(cast((get_json_object(sg.snapshot,'$.data.rto_time_epoch')+19799000)/1000 as bigint),'dd-MM-yyyy')
  when get_json_object(sg.snapshot,'$.data.rto_time_epoch') is null and (vm.zone='LU' or vm.zone='LU_MED' or vm.zone='LU_NEAR' or vm.zone='LU_FAR') and from_unixtime(cast((temp2.order_at+19800000)/1000 as bigint),'yyyy-MM-dd') < '2018-09-01' then from_unixtime(cast((temp6.first_shipped+19800000+(86400000*5))/1000 as bigint),'dd-MM-yyyy')
    when get_json_object(sg.snapshot,'$.data.rto_time_epoch') is null and  (vm.zone='ZR' or vm.zone='ZSU' or vm.zone='ZU') and from_unixtime(cast((temp2.order_at+19800000)/1000 as bigint),'yyyy-MM-dd') < '2018-09-01' then from_unixtime(cast((temp6.first_shipped+19800000+(86400000*12))/1000 as bigint),'dd-MM-yyyy')
    when get_json_object(sg.snapshot,'$.data.rto_time_epoch') is null and  (vm.zone='NR' or vm.zone='NSU' or vm.zone='NU' or vm.zone='NM') and from_unixtime(cast((temp2.order_at+19800000)/1000 as bigint),'yyyy-MM-dd') < '2018-09-01' then from_unixtime(cast((temp6.first_shipped+19800000+(86400000*17))/1000 as bigint),'dd-MM-yyyy')
    when get_json_object(sg.snapshot,'$.data.rto_time_epoch') is null and (vm.zone='LU' or vm.zone='LU_MED' or vm.zone='LU_NEAR' or vm.zone='LU_FAR') and from_unixtime(cast((temp2.order_at+19800000)/1000 as bigint),'yyyy-MM-dd') >= '2018-09-01' then from_unixtime(cast((temp6.first_shipped+19800000+(86400000*10))/1000 as bigint),'dd-MM-yyyy')
    when get_json_object(sg.snapshot,'$.data.rto_time_epoch') is null and  (vm.zone='ZR' or vm.zone='ZSU' or vm.zone='ZU'  or vm.zone='NM') and from_unixtime(cast((temp2.order_at+19800000)/1000 as bigint),'yyyy-MM-dd') >= '2018-09-01' then from_unixtime(cast((temp6.first_shipped+19800000+(86400000*17))/1000 as bigint),'dd-MM-yyyy')
    when get_json_object(sg.snapshot,'$.data.rto_time_epoch') is null and  (vm.zone='NR' or vm.zone='NSU' or vm.zone='NU') and from_unixtime(cast((temp2.order_at+19800000)/1000 as bigint),'yyyy-MM-dd') >= '2018-09-01' then from_unixtime(cast((temp6.first_shipped+19800000+(86400000*22))/1000 as bigint),'dd-MM-yyyy')
  end as slc_promise_at

    from ds_sqls_shipment_group sg
    left join temp6 on temp6.id=sg.shipment_group_id
    left join temp2 on sg.seller_order_id = temp2.order_id
     left join ds_sqls_logistics_pincode_cluster_master cm on cm.pincode=sg.shipfrom_pincode 
     join ds_sqls_logistics_route_vsl_master vm on (vm.pick_up_cluster=cm.pick_up_cluster and vm.dest_pincode=sg.shipto_pincode)
     where sg.current_active=1 and cm.current_active=1 and vm.current_active=1
and from_unixtime(cast((sg.created_at+19800000)/1000 as bigint), 'yyyy-MM-dd') >= date_sub('${sprinkle.latest.partition}', ${window})

     ),

  temp1b as (select v.shipment_group_id as id,v.status_external as ex_s,v.location as loc,v.comment as com,
    row_number() over (partition by v.shipment_group_id order by v.date_time desc) as row_num
   from ds_sqls_vendor_shipment_details v
where from_unixtime(cast((v.date_time+19800000)/1000 as bigint), 'yyyy-MM-dd') >= date_sub('${sprinkle.latest.partition}', ${window})
   ),
  temp1c as (select sm.shipment_group_id as id,collect_list(sm.value) as reason from ds_sqls_shipment_group_metadata sm where sm.key_attrib='ops_reason' group by sm.shipment_group_id),
 temp1d as ( select
    a.id as id,
    a.time1,
    datediff(a.time1,from_unixtime(cast((t6.last_rts+19800000)/1000 as bigint),'yyyy-MM-dd')) as sla
    from


    (select s.shipment_group_id as id,from_unixtime(cast(max(s.eta+19800000)/1000 as bigint),'yyyy-MM-dd') as time1 from ds_sqls_shipment_group s group by s.shipment_group_id) a
  left join temp1 t6 on t6.id=a.id
    ),
  temp2b as (select

  d.shipment_id as id,
  count(d.state) as rto_attempts,
  from_unixtime((min(CAST(d.updated_at/1000 as bigint))+19800),'dd-MM-yyyy HH:mm:ss') as rto_fofd,
  from_unixtime((max(CAST(d.updated_at/1000 as bigint))+19800),'dd-MM-yyyy HH:mm:ss') as rto_lofd,



  collect_list(d.comments) as failed_rto_comments


  from ds_sqls_delivery_task d
  where d.type='SELLER_RTO_DROP' and d.state='OUT_FOR_DELIVERY' and from_unixtime(CAST(d.updated_at/1000 as bigint), 'yyyy-MM-dd') >= date_sub('${sprinkle.latest.partition}', ${window})

  group by d.shipment_id),


  tl as (select sg.shipment_group_id id,vm.zone zone,row_number() over (partition by sg.shipment_group_id order by vm.zone) as rw

   from ds_sqls_shipment_group sg  
  left join ds_sqls_logistics_pincode_cluster_master cm on cm.pincode=sg.shipfrom_pincode and cm.current_active=1
  join ds_sqls_logistics_route_vsl_master vm on (vm.pick_up_cluster=cm.pick_up_cluster and vm.dest_pincode=sg.shipto_pincode) and vm.current_active=1
  left join ds_sqls_lane_sla ls on ls.zone=vm.zone
where from_unixtime(cast((sg.created_at+19800000)/1000 as bigint), 'yyyy-MM-dd') >= date_sub('${sprinkle.latest.partition}', ${window})

  )

  select
  -- shipment details --
  distinct sg.shipment_group_id,
  sg.seller_order_id,
  sg.awb_number,
  sg.shipment_status as current_status,
  pt.state as current_fm_status,
  temp1b.ex_s as 3pl_external_status,
  temp1b.loc as shipment_current_location,
  temp1b.com as 3pl_comment,
  case
    when (sg.shipment_status='SHIPMENT_IN_TRANSIT' OR sg.shipment_status='SHIPMENT_ATTEMPTED_UNDELIVERED' OR sg.shipment_status='SHIPMENT_RTS') THEN 'SHIPMENT_TRANSIT'
    WHEN sg.shipment_status='SHIPMENT_DELIVERED' THEN 'SHIPMENT_DELIVERED'
    ELSE 'SHIPMENT_RTO'
  END as terminal_state,
  temp4.order_status as order_status,
  temp4.category as category,
  temp4.sub as sub_category,
  temp4.vertical,
  temp25.title as listing_title,
  temp25.listing_id as listing_id,
  temp4.new_or_old,
  temp2.order_type,
  temp2.payment_mode,
  orgv.display_name as vendor,
  case
    when orgv.display_name='Udaan_Express' and u1.leg_vendor='Udaan Express' then 'Own_LM'
    when orgv.display_name='Udaan_Express' and u1.leg_vendor!='Udaan Express' then 'Regional_vendor'
    when orgv.display_name!='Udaan_Express' then 'Non_UE'
  end as ue_lm_vendor,
  case
    when get_json_object(sg.snapshot,'$.data.rto_back_to_seller_enabled')='true' then 'YES'
    else 'NO'
  end as rto_to_seller,
  case when (tl.zone='LU' or tl.zone='LU_MED' or tl.zone='LU_NEAR' or tl.zone='LU_FAR') then 'LU' else tl.zone end as lane,
  case when (tl.zone='LU' or tl.zone='LU_MED' or tl.zone='LU_NEAR' or tl.zone='LU_FAR') then 'Local' when (tl.zone='ZR' or tl.zone='ZSU' or tl.zone='ZU') then 'Zonal' else 'National' end as zone,
  t1d.sla as max_sla,
  get_json_object(sg.box_details,'$.weight_per_box_grams[0]')/1000 as weight,
  get_json_object(get_json_object(hsd.shipment_attributes,'$.weightChanges[0]'),'$.from') as seller_weight_input,
  get_json_object(get_json_object(hsd.shipment_attributes,'$.weightChanges[0]'),'$.to') as hub_weight_input,
  (get_json_object(sg.box_details,'$.weight_per_box_grams[0]')/1000)-((get_json_object(get_json_object(hsd.shipment_attributes,'$.weightChanges[0]'),'$.from'))/1000) as weight_diff,
  get_json_object(sg.box_details,'$.number_of_boxes') as number_of_boxes,
  temp25.unit_qty as unit_qty,
  temp45.invoice_id as invoice_id,
  temp45.invoice_ref_id as invoice_ref_id,
  temp45.rts_qty as rts_qty,
  case
    when cac.amount_in_paisa is null then 'No'
    else 'Yes'
  end as token_flag,
  sg.out_of_payment_policy as rto_policy_flag,

  temp25.order_amount as order_amount,
  temp3.amount/100 as shipment_amount,
  case 
    when il.payee_id=orgs.org_id then 'From_Pay' 
    else 'To_pay' 
  end as from_pay_tag,

  (cac.amount_in_paisa/100) as token_amount,
  t3a.amount/100 as logistics_amount,


  -- seller details --
  orgs.org_id as seller_id,
  sg.shipfrom_org_unit_id as seller_unit_id,
  orgs.display_name as seller_name,
  get_json_object(orgus.data,'$.mobile_number.value') as seller_mobile_number,
  concat(get_json_object(orgus.unit_address,'$.address_line1'),', ',get_json_object(orgus.unit_address,'$.address_line2'),', ',get_json_object(orgus.unit_address,'$.address_line3')) as seller_address,
  sg.shipfrom_pincode as seller_pincode,
  cm.pick_up_cluster as pick_up_cluster,
  get_json_object(orgus.unit_address,'$.city') as seller_city,
  get_json_object(orgus.unit_address,'$.state') as seller_state,
  case 
    when length(temp25.wh_tag)>10 and orgs.org_id not in ('ORG1BM4G86NG2Q3E4BPVZHEHBM8C8','ORGBZ4LES4YGN8W9G298H3MDRJZD0','ORGL7XKVB290ZQEXZCH5F6MRD1PEX') then 'Warehouse'
    when orgs.org_id in ('ORG1BM4G86NG2Q3E4BPVZHEHBM8C8','ORGBZ4LES4YGN8W9G298H3MDRJZD0','ORGL7XKVB290ZQEXZCH5F6MRD1PEX') then 'First Party'
    else 'Marketplace'
  end as fulfilment_type,


  case
    when length(temp25.dtr_tag)<10 then 'Not-DTR'
    else 'DTR'
  end as dtr_tag,

  case 
    when orgs.org_id in ('ORGE0J1ZB67LZC8046RSEK1BCWGBE','ORG61RBTH4V8SQTQFHCZQJK36XT14') then 'Yes'
    else 'No'
  end as seller_flex_flag, 


  -- buyer details --
  orgb.org_id as buyer_id,
  sg.shipto_org_unit_id as buyer_unit_id,
  orgb.display_name as buyer_name,
  round((((temp2.order_at+19800000)-(orgub.created_at+19800000))/1000)/86400,0)/7 as buyer_age_in_weeks,
  get_json_object(orgub.data,'$.mobile_number.value') as buyer_mobile_number,
  us.mobile_primary as buyer_verified_number,
  concat(get_json_object(orgub.unit_address,'$.address_line1'),', ',get_json_object(orgub.unit_address,'$.address_line2'),', ',get_json_object(orgub.unit_address,'$.address_line3')) as buyer_address,
  sg.shipto_pincode as buyer_pincode,
  get_json_object(orgub.unit_address,'$.city') as buyer_city,
  get_json_object(orgub.unit_address,'$.state') as buyer_state,


  -- logistics details --
  case
  when pt.state is not null then 'FM service' else '3pl service'
  end as fm_service,

  case
    when rit.return_shipment_id is not null then 'RVP'
    else 'FWD'
  end as rvp_flag,

  case
    when orgv.display_name='Udaan_Express' then u.shipment_dispatched_to
    else oh.unit_name
  end as hub_name,

  -- shipment timestamps --
  from_unixtime(cast((temp2.order_at+19800000)/1000 as bigint),'dd-MM-yyyy HH:mm:ss') as order_date,
  from_unixtime(cast((topr.pr_time+19800000)/1000 as bigint),'dd-MM-yyyy HH:mm:ss') as order_processing_time,
  from_unixtime(cast((temp1.last_rts+19800000)/1000 as bigint),'dd-MM-yyyy HH:mm:ss') as rts_date,
  from_unixtime(cast((temp1.first_rts+19800000)/1000 as bigint),'dd-MM-yyyy HH:mm:ss') as first_rts_date,
    from_unixtime(cast((sg.created_at+19800000)/1000 as bigint),'yyyy-MM-dd') as partition_date,
  from_unixtime(cast((temp1p.last_packed_ts+19800000)/1000 as bigint),'dd-MM-yyyy HH:mm:ss') as last_packed_date,
     ((temp1.last_rts+19800000)/1000) as rts_date_epoch,
  temp48.first_pickup_time as first_ofp_time,
  temp48.last_pickup_time as last_ofp_time,
  temp47.picked_up_time as picked_up_time,
  case when orgv.display_name='Udaan_Express' then u.inscan_time else temp21.first_time end as inscan_time,
  case when orgv.display_name='Udaan_Express' then u.outscan_time else t21a.first_time end as outscan_time,

  from_unixtime(cast((temp6.first_shipped+19800000)/1000 as bigint),'dd-MM-yyyy') as first_shipped_date,
  case
    when (orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') then from_unixtime(cast((temp31.rad_time+19800000)/1000 as bigint),'dd-MM-yyyy HH:mm:ss')
    when (orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface') then from_unixtime(cast((temp31.rad_time+19800000)/1000 as bigint),'dd-MM-yyyy HH:mm:ss')
    when orgv.display_name in ('ECOM Express','Ecom Surface') then from_unixtime(cast((temp52.rad_time+19800000)/1000 as bigint),'dd-MM-yyyy HH:mm:ss')
    when orgv.display_name='Udaan_Express' and u1.leg_vendor='Udaan Express' then u1.inscan_time
    else null
  end as rad_date,

  case
    when orgv.display_name in ('ECOM Express','Ecom Surface','Delhivery','Delhivery Express','Xpressbees','Xpressbees Surface') then from_unixtime(cast((temp51a.fa_time+19800000)/1000 as bigint),'dd-MM-yyyy HH:mm:ss')
    when orgv.display_name='Udaan_Express' and u1.leg_vendor='Udaan Express' then from_unixtime(xt.t1+19800,'dd-MM-yyyy HH:mm:ss')
    else null
  end as fa_date,

  case
    when orgv.display_name in ('ECOM Express','Ecom Surface','Delhivery','Delhivery Express','Xpressbees','Xpressbees Surface') then from_unixtime(cast((temp51a.la_time+19800000)/1000 as bigint),'dd-MM-yyyy HH:mm:ss')
    else null
  end as la_date,

  from_unixtime(cast((tt.first_time+19800000)/1000 as bigint),'dd-MM-yyyy') as rto_date,
  from_unixtime(cast((tt.last_time+19800000)/1000 as bigint),'dd-MM-yyyy') as last_rto_date,
  from_unixtime(unix_timestamp(t1d.time1,'yyyy-MM-dd'),'dd-MM-yyyy') as fwd_promise_date,
  from_unixtime(cast((tt1.last_time+19800000)/1000 as bigint),'dd-MM-yyyy') as del_date,
  from_unixtime(cast((tt2.last_time+19800000)/1000 as bigint),'dd-MM-yyyy') as rto_del_date,
  temp1a.slc_promise_at as slc_promise_at,
  from_unixtime(cast((tt4.last_time+19800000)/1000 as bigint),'dd-MM-yyyy') as rto_abs_date,
  from_unixtime(cast((tt5.last_time+19800000)/1000 as bigint),'dd-MM-yyyy') as lost_time,
  from_unixtime(cast((tt3.last_time+19800000)/1000 as bigint),'dd-MM-yyyy') as rto_to_seller_date,
  from_unixtime(cast((tt6.first_time+19800000)/1000 as bigint),'dd-MM-yyyy') as first_rad_date,
  from_unixtime(cast((tt6.last_time+19800000)/1000 as bigint),'dd-MM-yyyy') as last_rad_date,

  t21b.first_time as rto_inscan_date,
  t21c.last_time as rto_outscan_date,
  t2b.rto_attempts as rto_attempt_count,
  t2b.rto_fofd as rto_first_ofd,
  t2b.rto_lofd as rto_last_ofd,
  t2b.failed_rto_comments as rto_undel_comments,


  case
    when month(from_unixtime(cast((tt1.last_time+19800000)/1000 as bigint),'yyyy-MM-dd'))/10 < 1 and day(from_unixtime(cast((tt1.last_time+19800000)/1000 as bigint),'yyyy-MM-dd'))/10 < 1 then concat('0',month(from_unixtime(cast((tt1.last_time+19800000)/1000 as bigint),'yyyy-MM-dd')),'/ ','0',day(from_unixtime(cast((tt1.last_time+19800000)/1000 as bigint),'yyyy-MM-dd')))
    when month(from_unixtime(cast((tt1.last_time+19800000)/1000 as bigint),'yyyy-MM-dd'))/10 < 1 and day(from_unixtime(cast((tt1.last_time+19800000)/1000 as bigint),'yyyy-MM-dd'))/10 >= 1 then concat('0',month(from_unixtime(cast((tt1.last_time+19800000)/1000 as bigint),'yyyy-MM-dd')),'/ ',day(from_unixtime(cast((tt1.last_time+19800000)/1000 as bigint),'yyyy-MM-dd')))
    when day(from_unixtime(cast((tt1.last_time+19800000)/1000 as bigint),'yyyy-MM-dd'))/10 < 1 and month(from_unixtime(cast((tt1.last_time+19800000)/1000 as bigint),'yyyy-MM-dd'))/10 >= 1 then concat(month(from_unixtime(cast((tt1.last_time+19800000)/1000 as bigint),'yyyy-MM-dd')),'/ ','0',day(from_unixtime(cast((tt1.last_time+19800000)/1000 as bigint),'yyyy-MM-dd')))
    else concat(month(from_unixtime(cast((tt1.last_time+19800000)/1000 as bigint),'yyyy-MM-dd')),'/ ',day(from_unixtime(cast((tt1.last_time+19800000)/1000 as bigint),'yyyy-MM-dd')))
  end as delivered_day,

  -- attempt counts --
  temp48.pickup_count as pickup_count,
  temp67.rto_attempt_count as 3pl_rto_attempt_count,
  temp50.pickup_failure_reason as pickup_failure_reason,
  case
    when orgv.display_name in ('ECOM Express','Ecom Surface','Delhivery','Delhivery Express','Xpressbees','Xpressbees Surface') and t51ac.attempt_count is null then 0
    when orgv.display_name in ('ECOM Express','Ecom Surface','Delhivery','Delhivery Express','Xpressbees','Xpressbees Surface') and t51ac.attempt_count is not null then t51ac.attempt_count
  end as attempt,

  temp77.lm_attempt_count as lm_attempt_count,
  case
    when temp71.ul_attempt_count is null then 0
    else temp71.ul_attempt_count
  end as ul_attempt_count,


  -- process timelines --
  case
    when round((((temp1.last_rts+19800000)-(temp2.order_at+19800000))/1000)/86400,0)<0 then 0
    else round((((temp1.last_rts+19800000)-(temp2.order_at+19800000))/1000)/86400,0)
  end as o2rts,

  case
    when round((((temp6.first_shipped+19800000)-(temp1.last_rts+19800000))/1000)/86400,0) < 0 then 0
    else round((((temp6.first_shipped+19800000)-(temp1.last_rts+19800000))/1000)/86400,0)
  end as rts2s,

  round(((tt1.last_time-temp6.first_shipped)/1000)/86400,0) as s2d,

  case
    when (orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') then datediff(from_unixtime(cast(unix_timestamp(from_unixtime(cast((tt.first_time+19800000)/1000 as bigint),'dd-MM-yyyy'),'dd-MM-yyyy') as bigint),'yyyy-MM-dd'),from_unixtime(cast(unix_timestamp(from_unixtime(cast((temp31.rad_time+19800000)/1000 as bigint),'dd-MM-yyyy HH:mm:ss'),'dd-MM-yyyy HH:mm:ss') as bigint),'yyyy-MM-dd'))
    when (orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface') then datediff(from_unixtime(cast(unix_timestamp(from_unixtime(cast((tt.first_time+19800000)/1000 as bigint),'dd-MM-yyyy'),'dd-MM-yyyy') as bigint),'yyyy-MM-dd'),from_unixtime(cast(unix_timestamp(from_unixtime(cast((temp31.rad_time+19800000)/1000 as bigint),'dd-MM-yyyy HH:mm:ss'),'dd-MM-yyyy HH:mm:ss') as bigint),'yyyy-MM-dd'))
    when (orgv.display_name in ('ECOM Express','Ecom Surface')) then datediff(from_unixtime(cast(unix_timestamp(from_unixtime(cast((tt.first_time+19800000)/1000 as bigint),'dd-MM-yyyy'),'dd-MM-yyyy') as bigint),'yyyy-MM-dd'),from_unixtime(cast(unix_timestamp(from_unixtime(cast((temp52.rad_time+19800000)/1000 as bigint),'dd-MM-yyyy HH:mm:ss'),'dd-MM-yyyy HH:mm:ss') as bigint),'yyyy-MM-dd'))
    else null
  end as rto2rad,

  round(((unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))-(temp6.first_shipped/1000))/86400),0) as d2d_aging,

  case
    when (orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') and split(temp38.ndr_reason,':')[0]='Received at destination city' then null
    else temp38.ndr_reason
  end as ndr_reason,
  tec.reason as logistics_undel_reason,

  case
  when
  (orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') and (temp38.ndr_reason = 'Received at destination city') then null
  when (((orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface') and split(temp38.ndr_reason,':')[0]='Wrong Pincode by SR') or (orgv.display_name in ('ECOM Express','Ecom Surface') and temp38.ndr_reason like '%234%')) then 'Address - pincode mismatch'
  when (((orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface') and split(temp38.ndr_reason,':')[0]='Customer Out of Station by SR') or (orgv.display_name in ('ECOM Express','Ecom Surface') and temp38.ndr_reason like '%212%')) then 'Buyer Out Of Station'
  when (((orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface') and split(temp38.ndr_reason,':')[0]='Consignee Refused to Pay COD Amount by SR') or (orgv.display_name in ('ECOM Express','Ecom Surface') and temp38.ndr_reason like '%209%')) then 'Buyer Refused to Pay COD Amount'
  when (((orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') and temp38.ndr_reason='Cash not ready with customer') or ((orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface') and (split(temp38.ndr_reason,':')[0]='COD Amount Not Ready by SR' or split(temp38.ndr_reason,':')[1]='Currency not available by SR') ) or (orgv.display_name in ('ECOM Express','Ecom Surface') and  (temp38.ndr_reason like '%210%' or temp38.ndr_reason like '%2425%'))) then 'COD Amount Not Ready'
  when (((orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface') and split(temp38.ndr_reason,':')[0]='No Such Consignee At The Given Address by SR') or (orgv.display_name in ('ECOM Express','Ecom Surface') and temp38.ndr_reason like '%220%')) then 'No Such Buyer At Given Address'
  when (((orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface') and split(temp38.ndr_reason,':')[0]='Customer Wants Open Delivery by SR') or ((orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') and (temp38.ndr_reason ='Customer asked for open delivery'))) then 'Buyer wants Open box Delivery'
  when (((orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface') and split(temp38.ndr_reason,':')[0]='Delivery not attempted (time constraint) by SR') or ((orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') and (temp38.ndr_reason ='Not attempted'))) then 'Delivery not attempted'
  when (((orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface') and split(temp38.ndr_reason,':')[0]='Entry Not Permitted by SR') or ((orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') and (temp38.ndr_reason ='Entry restricted area'))) then 'Entry Restricted area'
  when (((orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface') and split(temp38.ndr_reason,':')[0]='Natural Calamity Cannot Reach by SR') or ((orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') and (temp38.ndr_reason ='Natural Disaster'))) then 'Natural Calamity'
  when ((orgv.display_name in ('ECOM Express','Ecom Surface') and temp38.ndr_reason like '%235%') or ((orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') and (temp38.ndr_reason ='Bulk update'))) then 'Bulk order'
  when ((orgv.display_name in ('ECOM Express','Ecom Surface') and temp38.ndr_reason like '%200%') or ((orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') and (temp38.ndr_reason ='Consignment seized by consignee'))) then 'Forcibly taken by Buyer'
  when ((orgv.display_name in ('ECOM Express','Ecom Surface') and temp38.ndr_reason like '%236%') or ((orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') and ((temp38.ndr_reason ='Self Collect requested by customer') or (temp38.ndr_reason ='Self Collect') or (temp38.ndr_reason ='Reached out to customer for Self Collect')))) then 'Self collect'
  when (((orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface') and split(temp38.ndr_reason,':')[0]='Consignee Refused To Accept by SR') or (orgv.display_name in ('ECOM Express','Ecom Surface') and temp38.ndr_reason like '%221%') or
  ((orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') and ((temp38.ndr_reason ='Cancelled the order') or (temp38.ndr_reason like '%Customer Cancelled the order%')))) then 'Buyer cancelled the order'
  when (((orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface') and split(temp38.ndr_reason,':')[0]='Consignee Not Available by SR') or (orgv.display_name in ('ECOM Express','Ecom Surface') and temp38.ndr_reason like '%219%') or
  ((orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') and (temp38.ndr_reason ='Consignee unavailable '))) then 'Buyer not available'
  when (((orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface') and split(temp38.ndr_reason,':')[0]='C\'nee shifted On Given Address by SR') or (orgv.display_name in ('ECOM Express','Ecom Surface') and temp38.ndr_reason like '%218%') or
  ((orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') and (temp38.ndr_reason ='Consignee moved/shifted'))) then 'Buyer Shifted from the Given Address'
  when (((orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface') and split(temp38.ndr_reason,':')[0]='Future Delivery ') or (orgv.display_name in ('ECOM Express','Ecom Surface') and temp38.ndr_reason like '%331%') or
  ((orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') and substr(temp38.ndr_reason,0,24) ='Asked for delay delivery')) then 'Future delivery requested'
  when (((orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface') and split(temp38.ndr_reason,':')[0]='Add Incomplete/ Incorrect by SR') or (orgv.display_name in ('ECOM Express','Ecom Surface') and (temp38.ndr_reason like '%222%' or temp38.ndr_reason like '%223%' or temp38.ndr_reason like '%224%' or
  temp38.ndr_reason like '%231%')) or ((orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') and ((temp38.ndr_reason ='Bad/Incomplete Address') or (temp38.ndr_reason ='Bad Address')))) then 'Incomplete address'
  when (((orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface') and substr(temp38.ndr_reason,0,27) ='Shipment marked as misroute') or (orgv.display_name in ('ECOM Express','Ecom Surface') and temp38.ndr_reason like '%207%') or
  ((orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') and (temp38.ndr_reason ='Misrouted'))) then 'Misrouted'
  when (((orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface') and ((split(temp38.ndr_reason,':')[0]='ODA (Out Of Delivery Area) by SR' or  split(temp38.ndr_reason,':')[0] like '%Shipment marked as ODA (Out Of Delivery Area), Distance%') or (split(temp38.ndr_reason,':')[0]='Out of Delivery Area - Hold At Location by SR'))) or (orgv.display_name in ('ECOM Express','Ecom Surface') and temp38.ndr_reason like '%228%') or
  ((orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') and ((temp38.ndr_reason ='Out of Delivery Area (ODA)') or (temp38.ndr_reason ='ODA Shipment')))) then 'ODA'
  when (((orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface') and ((split(temp38.ndr_reason,':')[0]='Residence/Office Closed ') or (split(temp38.ndr_reason,':')[0]='Door Closed by SR'))) or (orgv.display_name in ('ECOM Express','Ecom Surface') and temp38.ndr_reason like '%227%') or
  ((orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') and (temp38.ndr_reason ='Office/Institute closed'))) then 'Shop closed'
  when ((orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface') and split(temp38.ndr_reason,':')[0]='Address correct -Mobile Unanswered/Not Reachable by SR') then 'Buyer not reachable'
  when (orgv.display_name in ('ECOM Express','Ecom Surface') and temp38.ndr_reason like '%201%') then 'Awaiting Buyer\'s Response for Delivery'
  when (orgv.display_name in ('ECOM Express','Ecom Surface') and temp38.ndr_reason like '%208%') then 'Contents Missing'
  when (orgv.display_name in ('ECOM Express','Ecom Surface') and temp38.ndr_reason like '%217%') then 'Delivery Area Not Accessible'
  when (orgv.display_name in ('ECOM Express','Ecom Surface') and temp38.ndr_reason like '%232%') then 'Buyer requested delivery on another address'
  when (orgv.display_name in ('ECOM Express','Ecom Surface') and temp38.ndr_reason like '%1225%') then vsx.ndr_reason
  when (orgv.display_name in ('ECOM Express','Ecom Surface') and temp38.ndr_reason like '%215%') then 'Disturbance/Natural Disaster/Strike'
  when ((orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') and (temp38.ndr_reason ='Reached maximum attempt count')) then vsx.ndr_reason
  when ((orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') and ((temp38.ndr_reason ='Reattempt - As per NDR instructions') OR (temp38.ndr_reason ='Reattempt - As per NDR instructions '))) then vsx.ndr_reason
  when ((orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') and (temp38.ndr_reason ='Heavy Rain/ Fog')) then 'Heavy Rain/ Fog'
  when ((orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') and (temp38.ndr_reason ='Consignee opened the package and refused to accept')) then 'Buyer opened the package and refused to accept'
  when ((orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') and (temp38.ndr_reason ='Returned as per Client Instructions')) then vsx.ndr_reason
  when ((orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') and (temp38.ndr_reason ='Payment Mode / Amt Dispute')) then 'Payment mode dispute'
  when ((orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') and (temp38.ndr_reason ='Consignee asked for card/wallet on delivery payment')) then 'Consignee asked for card/wallet on delivery payment'
  else 'Miscellaneous'
  end as clean_ndr_reason,

  case
    when (temp1b.ex_s='DLVD' and (orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface')) then 'Buyer'
    when (temp1b.ex_s='DRC' and (orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface')) then 'Seller' when (temp1b.ex_s='IT' and (orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface')) then 'Transit'
    when (temp1b.ex_s='OFD' and (orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface')) then 'Dest_Hub'
    when (temp1b.ex_s='OFP' and (orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface')) then 'Seller'
    when (temp1b.ex_s='PKD' and (orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface')) then 'Src_Hub'
    when (temp1b.ex_s='PND' and (orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface')) then 'Seller'
    when (temp1b.ex_s='RAD' and (orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface')) then 'Dest_Hub'
    when (temp1b.ex_s='RAO' and (orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface')) then 'Src_Hub'
    when (temp1b.ex_s='RTD' and (orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface')) then 'Src_Hub'
    when (temp1b.ex_s='RTO' and (orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface')) then 'Dest_Hub'
    when (temp1b.ex_s='RTO-IT' and (orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface')) then 'Transit'
    when (temp1b.ex_s='RTON' and (orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface')) then 'Dest_Hub'
    when (temp1b.ex_s='RTU' and (orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface')) then 'Src_Hub'
    when (temp1b.ex_s='UD' and (orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface')) then 'Dest_Hub'
    when ((temp1b.ex_s='DL_Delivered' or temp1b.ex_s='DL_DTO') and (orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express')) then 'Buyer'
    when (temp1b.ex_s='DL_RTO' and (orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express')) then 'Seller'
    when (temp1b.ex_s='RT_Dispatched' and (orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express')) then 'Dest_Hub'
    when (temp1b.ex_s='RT_In Transit' and (orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express')) then 'Transit'
    when (temp1b.ex_s='RT_Pending' and (orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express')) then 'Src_Hub'
    when (temp1b.ex_s='UD_Dispatched' and (orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express')) then 'Dest_Hub'
    when (temp1b.ex_s='UD_In Transit' and (orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') and temp31.rad_time is not null)  then 'Dest_Hub'
    when (temp1b.ex_s='UD_In Transit' and (orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') and temp31.rad_time is null)  then 'Transit'
    when ((temp1b.ex_s='UD_Manifested' or temp1b.ex_s='RTO-OFD') and (orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express')) then 'Src_Hub'
    when (temp1b.ex_s='UD_Not Picked' and (orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express')) then 'Seller'
    when (temp1b.ex_s='UD_Pending' and (orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express')) then 'Dest_Hub'
    when sg.shipment_status in ('SHIPMENT_IN_TRANSIT','SHIPMENT_ATTEMPTED_UNDELIVERED') and orgv.display_name in ('ECOM Express','Ecom Surface') and (temp1b.ex_s in ('201','202','200','206','207','208','209','210','212','213','214','215','216','217','221','222','223','224','225','226','227','228','229','231','233','331','332','218','220','777','309') or lower(temp1b.loc) like concat('%',get_json_object(orgub.unit_address,'$.city'),'%')) then 'Dest_Hub'
    when sg.shipment_status in ('SHIPMENT_IN_TRANSIT','SHIPMENT_ATTEMPTED_UNDELIVERED') and orgv.display_name in ('ECOM Express','Ecom Surface') and lower(temp1b.loc) not like concat('%',get_json_object(orgub.unit_address,'$.city'),'%') and temp1b.ex_s in ('002','003','005','006','011','219','333','1224','1225','304','011') then 'Transit'
    when orgv.display_name in ('ECOM Express','Ecom Surface') and temp1b.ex_s='999' then 'Buyer'
    when orgv.display_name in ('ECOM Express','Ecom Surface') and sg.shipment_status='SHIPMENT_RTO' and (temp1b.ex_s in ('201','202','200','206','207','208','209','210','212','213','214','215','216','217','221','222','223','224','225','226','227','228','229','231','233','331','332','218','220') or lower(temp1b.loc) like concat('%',get_json_object(orgub.unit_address,'$.city'),'%')) then 'Src_Hub'
    when sg.shipment_status='SHIPMENT_RTO' and orgv.display_name in ('ECOM Express','Ecom Surface') and lower(temp1b.loc) not like concat('%',get_json_object(orgub.unit_address,'$.city'),'%') and temp1b.ex_s in ('002','003','005','006','011','219','333','1224','1225','304') then 'Transit'
  end as current_shipment_owner,




  -- process breaches sla --
  case when from_unixtime(cast(((temp6.first_shipped+19800000)+(t1d.sla*86400000))/1000 as bigint),'dd-MM-yyyy')=from_unixtime(unix_timestamp(),'dd-MM-yyyy') then 'Yes' else 'No' end as fwd_today_flag,

  case
    when (((unix_timestamp(temp47.picked_up_time,'dd-MM-yyyy HH:mm:ss')-(temp1.last_rts+19800000))/1000)/86400)<=1 then 'Within_SLA'
    when (((unix_timestamp(temp47.picked_up_time,'dd-MM-yyyy HH:mm:ss')-(temp1.last_rts+19800000))/1000)/86400)>1 then 'Outside_SLA'
  end as rts2p_SLA,

  case
    when ((unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))- cast((temp1.last_rts+19800000)/1000 as bigint))/86400 )>0.333333333 and (get_json_object(orgus.unit_address,'$.city')='Ahmedabad' or get_json_object(orgus.unit_address,'$.city')='Delhi' or get_json_object(orgus.unit_address,'$.city')='Bangalore' or get_json_object(orgus.unit_address,'$.city')='Ludhiana' or get_json_object(orgus.unit_address,'$.city')='Chennai' or get_json_object(orgus.unit_address,'$.city')='Gurgaon' or get_json_object(orgus.unit_address,'$.city')='Pune'  or  get_json_object(orgus.unit_address,'$.city')='Hyderabad' or get_json_object(orgus.unit_address,'$.city')='Jaipur' or get_json_object(orgus.unit_address,'$.city')='Kalyan' or get_json_object(orgus.unit_address,'$.city')='Kolkata' or get_json_object(orgus.unit_address,'$.city')='Mumbai' or get_json_object(orgus.unit_address,'$.city')='Surat' or get_json_object(orgus.unit_address,'$.city')='Secunderabad') then 'Breach'
    when ((unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))- cast((temp1.last_rts+19800000)/1000 as bigint))/86400)<=0.333333333 and (get_json_object(orgus.unit_address,'$.city')='Ahmedabad' or get_json_object(orgus.unit_address,'$.city')='Bangalore' or get_json_object(orgus.unit_address,'$.city')='Ludhiana' or get_json_object(orgus.unit_address,'$.city')='Delhi' or get_json_object(orgus.unit_address,'$.city')='Chennai' or get_json_object(orgus.unit_address,'$.city')='Gurgaon' or get_json_object(orgus.unit_address,'$.city')='Hyderabad' or get_json_object(orgus.unit_address,'$.city')='Jaipur' or get_json_object(orgus.unit_address,'$.city')='Kalyan' or get_json_object(orgus.unit_address,'$.city')='Kolkata' or get_json_object(orgus.unit_address,'$.city')='Mumbai' or get_json_object(orgus.unit_address,'$.city')='Surat' or get_json_object(orgus.unit_address,'$.city')='Secunderabad')  or get_json_object(orgus.unit_address,'$.city')='Pune' then 'Late'
    when  ((unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))- cast((temp1.last_rts+19800000)/1000 as bigint))/86400)>0.5 and (get_json_object(orgus.unit_address,'$.city')!='Ahmedabad' or get_json_object(orgus.unit_address,'$.city')!='Bangalore' or get_json_object(orgus.unit_address,'$.city')!='Ludhiana' or get_json_object(orgus.unit_address,'$.city')!='Chennai' or get_json_object(orgus.unit_address,'$.city')!='Gurgaon' or get_json_object(orgus.unit_address,'$.city')!='Delhi' or get_json_object(orgus.unit_address,'$.city')!='Hyderabad' or get_json_object(orgus.unit_address,'$.city')!='Jaipur' or get_json_object(orgus.unit_address,'$.city')!='Kalyan' or get_json_object(orgus.unit_address,'$.city')!='Kolkata' or get_json_object(orgus.unit_address,'$.city')!='Mumbai' or get_json_object(orgus.unit_address,'$.city')!='Surat' or get_json_object(orgus.unit_address,'$.city')!='Secunderabad') or get_json_object(orgus.unit_address,'$.city')='Pune'  then 'Breach'
    when ((unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))- cast((temp1.last_rts+19800000)/1000 as bigint))/86400)<=0.5 and (get_json_object(orgus.unit_address,'$.city')!='Ahmedabad' or get_json_object(orgus.unit_address,'$.city')!='Bangalore' or get_json_object(orgus.unit_address,'$.city')!='Ludhiana' or get_json_object(orgus.unit_address,'$.city')!='Chennai' or get_json_object(orgus.unit_address,'$.city')!='Gurgaon' or get_json_object(orgus.unit_address,'$.city')!='Delhi' or get_json_object(orgus.unit_address,'$.city')!='Hyderabad' or get_json_object(orgus.unit_address,'$.city')!='Jaipur' or get_json_object(orgus.unit_address,'$.city')!='Kalyan' or get_json_object(orgus.unit_address,'$.city')!='Kolkata' or get_json_object(orgus.unit_address,'$.city')!='Mumbai' or get_json_object(orgus.unit_address,'$.city')!='Surat' or get_json_object(orgus.unit_address,'$.city')!='Secunderabad') or get_json_object(orgus.unit_address,'$.city')='Pune'  then 'Late'
  end as rts_sla,

  case
    when (((temp1.last_rts+19800000)-(temp2.order_at+19800000))/1000)/86400 <=2 then 'Within_SLA'
    when (((temp1.last_rts+19800000)-(temp2.order_at+19800000))/1000)/86400>2 then 'Outside_SLA'
  end as o2rts_SLA,

  case
    when round((((temp6.first_shipped+19800000)-(temp1.last_rts+19800000))/1000)/86400,0)<=1 then 'Within_SLA'
    when round((((temp6.first_shipped+19800000)-(temp1.last_rts+19800000))/1000)/86400,0)>1 then 'Outside_SLA'
  end as rts2s_SLA,

  case
    when unix_timestamp(from_unixtime(cast(round(((tt1.last_time+19800000)/1000),0) as bigint),'yyyy-MM-dd 00:00:00'))<=unix_timestamp(from_unixtime(cast(round(((temp6.first_shipped+19800000+(t1d.sla*86400000))/1000),0) as bigint),'yyyy-MM-dd 00:00:00')) then 'Within_SLA'
    else 'Outside_SLA'
  end as s2d_SLA,

  case
    when tt2.last_time is not null and unix_timestamp(from_unixtime(cast(round(((tt2.last_time+19800000)/1000),0) as bigint),'yyyy-MM-dd 00:00:00'))<=unix_timestamp(from_unixtime(cast(round(((tt.last_time+19800000+(t1d.sla*86400000))/1000),0) as bigint),'yyyy-MM-dd 00:00:00')) then 'Within_SLA'
    when tt2.last_time is not null and unix_timestamp(from_unixtime(cast(round(((tt2.last_time+19800000)/1000),0) as bigint),'yyyy-MM-dd 00:00:00'))>unix_timestamp(from_unixtime(cast(round(((tt.last_time+19800000+(t1d.sla*86400000))/1000),0) as bigint),'yyyy-MM-dd 00:00:00')) then 'Outside_SLA'
    when tt2.last_time is null and (unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))-((tt.last_time+19800000)/1000))/86400 > t1d.sla then 'Outside_SLA'
    when tt2.last_time is null and (unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))-((tt.last_time+19800000)/1000))/86400 <= t1d.sla then 'Within_SLA'
  end as rto_sla,

  case
    when (unix_timestamp(from_unixtime(cast(round(((tt2.last_time+19800000)/1000),0) as bigint),'yyyy-MM-dd 00:00:00'))<=unix_timestamp(from_unixtime(unix_timestamp(temp1a.slc_promise_at,'dd-MM-yyyy'),'yyyy-MM-dd 00:00:00'))) and (tl.zone='LU' or tl.zone='LU_MED' or tl.zone='LU_NEAR' or tl.zone='LU_FAR') then 'Within_SLA'
    when (unix_timestamp(from_unixtime(cast(round(((tt2.last_time+19800000)/1000),0) as bigint),'yyyy-MM-dd 00:00:00'))>unix_timestamp(from_unixtime(unix_timestamp(temp1a.slc_promise_at,'dd-MM-yyyy'),'yyyy-MM-dd 00:00:00'))) and (tl.zone='LU' or tl.zone='LU_MED' or tl.zone='LU_NEAR' or tl.zone='LU_FAR') then 'Outside_SLA'
    when (unix_timestamp(from_unixtime(cast(round(((tt2.last_time+19800000)/1000),0) as bigint),'yyyy-MM-dd 00:00:00'))<=unix_timestamp(from_unixtime(unix_timestamp(temp1a.slc_promise_at,'dd-MM-yyyy'),'yyyy-MM-dd 00:00:00'))) and (tl.zone='ZU' or tl.zone='ZR' or tl.zone='ZSU') then 'Within_SLA'
    when (unix_timestamp(from_unixtime(cast(round(((tt2.last_time+19800000)/1000),0) as bigint),'yyyy-MM-dd 00:00:00'))>unix_timestamp(from_unixtime(unix_timestamp(temp1a.slc_promise_at,'dd-MM-yyyy'),'yyyy-MM-dd 00:00:00'))) and (tl.zone='ZU' or tl.zone='ZR' or tl.zone='ZSU') then 'Outside_SLA'
    when (unix_timestamp(from_unixtime(cast(round(((tt2.last_time+19800000)/1000),0) as bigint),'yyyy-MM-dd 00:00:00'))<=unix_timestamp(from_unixtime(unix_timestamp(temp1a.slc_promise_at,'dd-MM-yyyy'),'yyyy-MM-dd 00:00:00'))) and (tl.zone='NU' or tl.zone='NR' or tl.zone='NSU' or tl.zone='NM') then 'Within_SLA'
    when (unix_timestamp(from_unixtime(cast(round(((tt2.last_time+19800000)/1000),0) as bigint),'yyyy-MM-dd 00:00:00'))>unix_timestamp(from_unixtime(unix_timestamp(temp1a.slc_promise_at,'dd-MM-yyyy'),'yyyy-MM-dd 00:00:00'))) and (tl.zone='NU' or tl.zone='NR' or tl.zone='NSU' or tl.zone='NM') then 'Outside_SLA'
  end as slc_SLA,

  case
    when sg.shipment_status in ('SHIPMENT_IN_TRANSIT','SHIPMENT_ATTEMPTED_UNDELIVERED','SHIPMENT_RTO','SHIPMENT_RE_DISPATCHED') AND ((unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))-(temp6.first_shipped/1000)) < 6*86400 and (tl.zone='LU' or tl.zone='LU_MED' or tl.zone='LU_NEAR' or tl.zone='LU_FAR')) THEN 'Within_SLA'
    when sg.shipment_status in ('SHIPMENT_IN_TRANSIT','SHIPMENT_ATTEMPTED_UNDELIVERED','SHIPMENT_RTO','SHIPMENT_RE_DISPATCHED') AND ((unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))-(temp6.first_shipped/1000))  >=6*86400 and (tl.zone='LU' or tl.zone='LU_MED' or tl.zone='LU_NEAR' or tl.zone='LU_FAR')) then 'Outside_SLA'
    when sg.shipment_status in ('SHIPMENT_IN_TRANSIT','SHIPMENT_ATTEMPTED_UNDELIVERED','SHIPMENT_RTO','SHIPMENT_RE_DISPATCHED') AND ((unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))-(temp6.first_shipped/1000))  <13*86400 and (tl.zone='ZU' or tl.zone='ZR' or tl.zone='ZSU')) then 'Within_SLA'
    when sg.shipment_status in ('SHIPMENT_IN_TRANSIT','SHIPMENT_ATTEMPTED_UNDELIVERED','SHIPMENT_RTO','SHIPMENT_RE_DISPATCHED') AND ((unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))-(temp6.first_shipped/1000))  >=13*86400 and (tl.zone='ZU' or tl.zone='ZR' or tl.zone='ZSU')) then 'Outside_SLA'
    when sg.shipment_status in ('SHIPMENT_IN_TRANSIT','SHIPMENT_ATTEMPTED_UNDELIVERED','SHIPMENT_RTO','SHIPMENT_RE_DISPATCHED') AND ((unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))-(temp6.first_shipped/1000))  <18*86400 and (tl.zone='NU' or tl.zone='NR' or tl.zone='NSU' or tl.zone='NM')) then 'Within_SLA'
    when sg.shipment_status in ('SHIPMENT_IN_TRANSIT','SHIPMENT_ATTEMPTED_UNDELIVERED','SHIPMENT_RTO','SHIPMENT_RE_DISPATCHED') AND ((unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))-(temp6.first_shipped/1000))  >=18*86400 and (tl.zone='NU' or tl.zone='NR' or tl.zone='NSU' or tl.zone='NM')) then 'Outside_SLA'
    WHEN sg.shipment_status ='SHIPMENT_RTO_DELIVERED' AND  (unix_timestamp(from_unixtime(cast(round(((tt2.last_time+19800000)/1000),0) as bigint),'yyyy-MM-dd 00:00:00'))<=unix_timestamp(from_unixtime(unix_timestamp(temp1a.slc_promise_at,'dd-MM-yyyy'),'yyyy-MM-dd 00:00:00'))) and (tl.zone='LU' or tl.zone='LU_MED' or tl.zone='LU_NEAR' or tl.zone='LU_FAR') then 'Within_SLA'
    when sg.shipment_status ='SHIPMENT_RTO_DELIVERED' AND (unix_timestamp(from_unixtime(cast(round(((tt2.last_time+19800000)/1000),0) as bigint),'yyyy-MM-dd 00:00:00'))>unix_timestamp(from_unixtime(unix_timestamp(temp1a.slc_promise_at,'dd-MM-yyyy'),'yyyy-MM-dd 00:00:00'))) and (tl.zone='LU' or tl.zone='LU_MED' or tl.zone='LU_NEAR' or tl.zone='LU_FAR') then 'Outside_SLA'
    when sg.shipment_status ='SHIPMENT_RTO_DELIVERED' AND (unix_timestamp(from_unixtime(cast(round(((tt2.last_time+19800000)/1000),0) as bigint),'yyyy-MM-dd 00:00:00'))<=unix_timestamp(from_unixtime(unix_timestamp(temp1a.slc_promise_at,'dd-MM-yyyy'),'yyyy-MM-dd 00:00:00'))) and (tl.zone='ZU' or tl.zone='ZR' or tl.zone='ZSU') then 'Within_SLA'
    when sg.shipment_status ='SHIPMENT_RTO_DELIVERED' AND (unix_timestamp(from_unixtime(cast(round(((tt2.last_time+19800000)/1000),0) as bigint),'yyyy-MM-dd 00:00:00'))>unix_timestamp(from_unixtime(unix_timestamp(temp1a.slc_promise_at,'dd-MM-yyyy'),'yyyy-MM-dd 00:00:00'))) and (tl.zone='ZU' or tl.zone='ZR' or tl.zone='ZSU') then 'Outside_SLA'
    when sg.shipment_status ='SHIPMENT_RTO_DELIVERED' AND (unix_timestamp(from_unixtime(cast(round(((tt2.last_time+19800000)/1000),0) as bigint),'yyyy-MM-dd 00:00:00'))<=unix_timestamp(from_unixtime(unix_timestamp(temp1a.slc_promise_at,'dd-MM-yyyy'),'yyyy-MM-dd 00:00:00'))) and (tl.zone='NU' or tl.zone='NR' or tl.zone='NSU' or tl.zone='NM') then 'Within_SLA'
    when sg.shipment_status ='SHIPMENT_RTO_DELIVERED' AND (unix_timestamp(from_unixtime(cast(round(((tt2.last_time+19800000)/1000),0) as bigint),'yyyy-MM-dd 00:00:00'))>unix_timestamp(from_unixtime(unix_timestamp(temp1a.slc_promise_at,'dd-MM-yyyy'),'yyyy-MM-dd 00:00:00'))) and (tl.zone='NU' or tl.zone='NR' or tl.zone='NSU' or tl.zone='NM') then 'Outside_SLA'
    when sg.shipment_status ='SHIPMENT_RTO_ABSORBED' THEN 'Outside_SLA'
    when sg.shipment_status='SHIPMENT_RTO_TO_SELLER' THEN 'Within_SLA'
  end as 3pl_rto_sla,

  case
    when (unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))-(temp6.first_shipped/1000)) < 6*86400 and (tl.zone='LU' or tl.zone='LU_MED' or tl.zone='LU_NEAR' or tl.zone='LU_FAR') then 'Within_SLA' when (unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))-(temp6.first_shipped/1000))  >=6*86400 and (tl.zone='LU' or tl.zone='LU_MED' or tl.zone='LU_NEAR' or tl.zone='LU_FAR') then 'Outside_SLA'
    when (unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))-(temp6.first_shipped/1000))  <13*86400 and (tl.zone='ZU' or tl.zone='ZR' or tl.zone='ZSU') then 'Within_SLA'
    when (unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))-(temp6.first_shipped/1000))  >=13*86400 and (tl.zone='ZU' or tl.zone='ZR' or tl.zone='ZSU') then 'Outside_SLA'
    when (unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))-(temp6.first_shipped/1000))  <18*86400 and (tl.zone='NU' or tl.zone='NR' or tl.zone='NSU' or tl.zone='NM') then 'Within_SLA'
    when (unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))-(temp6.first_shipped/1000))  >=18*86400 and (tl.zone='NU' or tl.zone='NR' or tl.zone='NSU' or tl.zone='NM') then 'Outside_SLA'
  end as d2d_breach,

  case
    when (tl.zone='LU' or tl.zone='LU_MED' or tl.zone='LU_NEAR' or tl.zone='LU_FAR') and tt2.last_time<(temp6.first_shipped+6*86400000) then 'YES'
    when (tl.zone='LU' or tl.zone='LU_MED' or tl.zone='LU_NEAR' or tl.zone='LU_FAR') and tt2.last_time>=(temp6.first_shipped+6*86400000) then 'NO'
    when (tl.zone='ZU' or tl.zone='ZR' or tl.zone='ZSU') and tt2.last_time<(temp6.first_shipped+13*86400000) then 'YES'
    when (tl.zone='ZU' or tl.zone='ZR' or tl.zone='ZSU') and tt2.last_time >= (temp6.first_shipped+13*86400000) then 'NO'
    when (tl.zone='NR' or tl.zone='NU' or tl.zone='NSU' or tl.zone='NM') and tt2.last_time < (temp6.first_shipped+18*86400000) then 'YES'
    else 'NO'
  end as rto_del_flag,

  case
    when (tl.zone='LU' or tl.zone='LU_MED' or tl.zone='LU_NEAR' or tl.zone='LU_FAR') and (unix_timestamp(temp1a.slc_promise_at,'dd-MM-yyyy')*1000)-(unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))*1000)>=0 then 'Within_SLA'
    when (tl.zone='LU' or tl.zone='LU_MED' or tl.zone='LU_NEAR' or tl.zone='LU_FAR') and (unix_timestamp(temp1a.slc_promise_at,'dd-MM-yyyy')*1000)-(unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))*1000)<0 then 'Outside_SLA'
    when (tl.zone='ZU' or tl.zone='ZR' or tl.zone='ZSU') and (unix_timestamp(temp1a.slc_promise_at,'dd-MM-yyyy')*1000)-(unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))*1000) >=0 then 'Within_SLA'
    when (tl.zone='ZU' or tl.zone='ZR' or tl.zone='ZSU') and (unix_timestamp(temp1a.slc_promise_at,'dd-MM-yyyy')*1000)-(unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))*1000) < 0 then 'Outside_SLA'
    when (tl.zone='NR' or tl.zone='NU' or tl.zone='NSU' or tl.zone='NM') and (unix_timestamp(temp1a.slc_promise_at,'dd-MM-yyyy')*1000)-(unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))*1000) >=0 then 'Within_SLA'
    when (tl.zone='NR' or tl.zone='NU' or tl.zone='NSU' or tl.zone='NM') and (unix_timestamp(temp1a.slc_promise_at,'dd-MM-yyyy')*1000)-(unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))*1000) <0 then 'Outside_SLA'
  end as hub_breach,

  case
    when (sg.shipment_status='SHIPMENT_IN_TRANSIT' OR sg.shipment_status='SHIPMENT_ATTEMPTED_UNDELIVERED') and (unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00')))<=unix_timestamp(from_unixtime(cast(round(((temp6.first_shipped+19800000+(t1d.sla*86400000))/1000),0) as bigint),'yyyy-MM-dd 00:00:00')) then 'Within_SLA'
    when (sg.shipment_status='SHIPMENT_IN_TRANSIT' OR sg.shipment_status='SHIPMENT_ATTEMPTED_UNDELIVERED') and (unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00')))>unix_timestamp(from_unixtime(cast(round(((temp6.first_shipped+19800000+(t1d.sla*86400000))/1000),0) as bigint),'yyyy-MM-dd 00:00:00')) then 'Outside_SLA'
    else null
  end as pendency_fwd_sla,

  case
    when (((tl.zone='ZU' or tl.zone='ZSU' or tl.zone='ZR') and ((unix_timestamp(from_unixtime(cast(round(((temp6.first_shipped+19800000+((t1d.sla-2)*86400000))/1000),0) as bigint),'yyyy-MM-dd 00:00:00')))<=(unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))))) or ((tl.zone='NU' or tl.zone='NSU' or tl.zone='NR' or tl.zone='NM') and ((unix_timestamp(from_unixtime(cast(round(((temp6.first_shipped+19800000+((t1d.sla-3)*86400000))/1000),0) as bigint),'yyyy-MM-dd 00:00:00')))<=(unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00')))))) then 'Yes'
    else 'No'
  end as fwd_breach_flag,

  case
    when (((tl.zone='LU' or tl.zone='LU_MED' or tl.zone='LU_NEAR' or tl.zone='LU_FAR') and ((unix_timestamp(from_unixtime(cast(round(((temp6.first_shipped+19800000+((datediff(from_unixtime(unix_timestamp(temp1a.slc_promise_at,'dd-MM-yyyy'),'yyyy-MM-dd'),from_unixtime(cast((temp6.first_shipped+19800000)/1000 as bigint),'yyyy-MM-dd'))-2)*86400000))/1000),0) as bigint),'yyyy-MM-dd 00:00:00')))<=(unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))))) or ((tl.zone='ZU' or tl.zone='ZSU' or tl.zone='ZR') and ((unix_timestamp(from_unixtime(cast(round(((temp6.first_shipped+19800000+((datediff(from_unixtime(unix_timestamp(temp1a.slc_promise_at,'dd-MM-yyyy'),'yyyy-MM-dd'),from_unixtime(cast((temp6.first_shipped+19800000)/1000 as bigint),'yyyy-MM-dd'))-4)*86400000))/1000),0) as bigint),'yyyy-MM-dd 00:00:00')))<=(unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))))) or ((tl.zone='NU' or tl.zone='NSU' or tl.zone='NR' or tl.zone='NM') and ((unix_timestamp(from_unixtime(cast(round(((temp6.first_shipped+19800000+((datediff(from_unixtime(unix_timestamp(temp1a.slc_promise_at,'dd-MM-yyyy'),'yyyy-MM-dd'),from_unixtime(cast((temp6.first_shipped+19800000)/1000 as bigint),'yyyy-MM-dd'))-5)*86400000))/1000),0) as bigint),'yyyy-MM-dd 00:00:00')))<=(unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00')))))) then 'Yes'
    else 'No'
  end as rev_breach_flag,

  case
    when (orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') and (unix_timestamp(from_unixtime(cast(round(((temp51a.fa_time+19800000)/1000),0) as bigint),'yyyy-MM-dd 00:00:00'))<=unix_timestamp(from_unixtime(cast(round(((temp6.first_shipped+19800000+(t1d.sla*86400000))/1000),0) as bigint),'yyyy-MM-dd 00:00:00'))) then 'Within_SLA'
    when (orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') and (unix_timestamp(from_unixtime(cast(round(((temp51a.fa_time+19800000)/1000),0) as bigint),'yyyy-MM-dd 00:00:00'))> unix_timestamp(from_unixtime(cast(round(((temp6.first_shipped+19800000+(t1d.sla*86400000))/1000),0) as bigint),'yyyy-MM-dd 00:00:00'))) then 'Outside_SLA'
    when (orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface') and (unix_timestamp(from_unixtime(cast(round(((temp51a.fa_time+19800000)/1000),0) as bigint),'yyyy-MM-dd 00:00:00'))<=unix_timestamp(from_unixtime(cast(round(((temp6.first_shipped+19800000+(t1d.sla*86400000))/1000),0) as bigint),'yyyy-MM-dd 00:00:00'))) then 'Within_SLA'
    when (orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface') and (unix_timestamp(from_unixtime(cast(round(((temp51a.fa_time+19800000)/1000),0) as bigint),'yyyy-MM-dd 00:00:00'))> unix_timestamp(from_unixtime(cast(round(((temp6.first_shipped+19800000+(t1d.sla*86400000))/1000),0) as bigint),'yyyy-MM-dd 00:00:00'))) then 'Outside_SLA'
    when ((orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') and (temp51a.fa_time is null) and (unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00')) <= (((temp6.first_shipped+19800000)+((t1d.sla+1)*86400000))/1000))) then 'Within_SLA'
    when ((orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') and (temp51a.fa_time is null) and (unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00')) > (((temp6.first_shipped+19800000)+((t1d.sla+1)*86400000))/1000))) then 'Outside_SLA'
    when ((orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface') and (temp51a.fa_time is null) and (unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00')) <= (((temp6.first_shipped+19800000)+((t1d.sla+1)*86400000))/1000))) then 'Within_SLA'
    when ((orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface') and (temp51a.fa_time is null) and (unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00')) > (((temp6.first_shipped+19800000)+((t1d.sla+1)*86400000))/1000))) then 'Outside_SLA'
    when orgv.display_name in ('ECOM Express','Ecom Surface') and (unix_timestamp(from_unixtime(cast(round(((temp51a.fa_time+19800000)/1000),0) as bigint),'yyyy-MM-dd 00:00:00'))<=unix_timestamp(from_unixtime(cast(round(((temp6.first_shipped+19800000+(t1d.sla*86400000))/1000),0) as bigint),'yyyy-MM-dd 00:00:00'))) then 'Within_SLA'
    when orgv.display_name in ('ECOM Express','Ecom Surface') and (unix_timestamp(from_unixtime(cast(round(((temp51a.fa_time+19800000)/1000),0) as bigint),'yyyy-MM-dd 00:00:00'))> unix_timestamp(from_unixtime(cast(round(((temp6.first_shipped+19800000+(t1d.sla*86400000))/1000),0) as bigint),'yyyy-MM-dd 00:00:00'))) then 'Outside_SLA'
    when (orgv.display_name in ('ECOM Express','Ecom Surface') and (temp51a.fa_time is null) and (unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00')) <= (((temp6.first_shipped+19800000)+((t1d.sla+1)*86400000))/1000))) then 'Within_SLA'
    when (orgv.display_name in ('ECOM Express','Ecom Surface') and (temp51a.fa_time is null) and (unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00')) > (((temp6.first_shipped+19800000)+((t1d.sla+1)*86400000))/1000))) then 'Outside_SLA'
    else null
  end as attempt_promise_sla,

  case
    when (orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface') and (temp51a.fa_time is not null) then 'YES'
    when (orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface') and (temp51a.fa_time is null) then 'NO'
    when (orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') and (temp51a.fa_time is not null) then 'YES'
    when (orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') and (temp51a.fa_time is null) then 'NO'
    when orgv.display_name in ('ECOM Express','Ecom Surface') and temp51a.fa_time is not null then 'YES'
    when orgv.display_name in ('ECOM Express','Ecom Surface') and temp51a.fa_time is null then 'NO'
  end as attempt_flag,

  case
    when ((sg.shipment_status='SHIPMENT_IN_TRANSIT' OR sg.shipment_status='SHIPMENT_ATTEMPTED_UNDELIVERED') and (orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') and ((temp6.first_shipped+t1d.sla*86400000)-(unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))*1000) < 0) and (temp51a.fa_time is not null)) then 'YES'
    when ((sg.shipment_status='SHIPMENT_IN_TRANSIT' OR sg.shipment_status='SHIPMENT_ATTEMPTED_UNDELIVERED') and (orgv.display_name='Delhivery' or orgv.display_name='Delhivery Express') and ((temp6.first_shipped+t1d.sla*86400000)-(unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))*1000) < 0) and temp51a.fa_time is null) then 'NO'
    when ((sg.shipment_status='SHIPMENT_IN_TRANSIT' OR sg.shipment_status='SHIPMENT_ATTEMPTED_UNDELIVERED') and (orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface') and ((temp6.first_shipped+t1d.sla*86400000)-(unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))*1000) < 0) and (temp51a.fa_time is not null)) then 'YES'
    when ((sg.shipment_status='SHIPMENT_IN_TRANSIT' OR sg.shipment_status='SHIPMENT_ATTEMPTED_UNDELIVERED') and (orgv.display_name='Xpressbees' or orgv.display_name='Xpressbees Surface') and ((temp6.first_shipped+t1d.sla*86400000)-(unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))*1000) < 0) and temp51a.fa_time is null) then 'NO'
    when ((sg.shipment_status='SHIPMENT_IN_TRANSIT' OR sg.shipment_status='SHIPMENT_ATTEMPTED_UNDELIVERED') and orgv.display_name in ('ECOM Express','Ecom Surface') and ((temp6.first_shipped+t1d.sla*86400000)-(unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))*1000) < 0) and (temp51a.fa_time is not null)) then 'YES'
    when ((sg.shipment_status='SHIPMENT_IN_TRANSIT' OR sg.shipment_status='SHIPMENT_ATTEMPTED_UNDELIVERED') and orgv.display_name in ('ECOM Express','Ecom Surface') and ((temp6.first_shipped+t1d.sla*86400000)-(unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00'))*1000) < 0) and temp51a.fa_time is null) then 'NO'
    else null
  end as attempt_o_flag,
  from_unixtime(cast((seo.old_eta+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') as old_eta,
  from_unixtime(cast((seo.new_eta+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') as revised_eta,
  cm.pick_up_cluster as fm_cluster,

cs.total_interactions, cs.type,cs.subtype,cs.first_interaction_at



  -- table joins --
  from ds_sqls_shipment_group sg
  left join( select seo.shipment_id,min(old_eta) old_eta, max(new_eta) new_eta from  ds_sqls_shipment_eta_override seo group by seo.shipment_id) seo on sg.shipment_group_id = seo.shipment_id
  left join ds_sqls_orgs orgv on orgv.org_id=sg.third_party_provider_id
  left join temp1 on temp1.id=sg.shipment_group_id
  left join temp1p ON temp1p.id = sg.shipment_group_id
  left join ds_sqls_logistics_pincode_cluster_master cm 
  on cm.pincode=sg.shipfrom_pincode and cm.current_active=1
  join ds_sqls_logistics_route_vsl_master vm on (vm.pick_up_cluster=cm.pick_up_cluster and vm.dest_pincode=sg.shipto_pincode) and vm.current_active=1
  left join tl on tl.id=sg.shipment_group_id and tl.rw=1
  left join ds_sqls_lane_sla ls on ls.zone=tl.zone 
  left join ds_sqls_org_units orgus on orgus.org_unit_id=sg.shipfrom_org_unit_id
  join ds_sqls_orgs orgs on orgs.org_id=orgus.org_id
  left join ds_sqls_org_units orgub on orgub.org_unit_id=sg.shipto_org_unit_id
  join ds_sqls_orgs orgb on orgb.org_id=orgub.org_id
  left join temp2 on temp2.order_id=sg.seller_order_id
  left join temp3 on temp3.id=sg.shipment_group_id and temp3.type='GOODS'
  left join temp3 t3a on t3a.id=sg.shipment_group_id and t3a.type='LOGISTIC_SERVICE'
  left join temp4 on temp4.id=sg.seller_order_id and temp4.row_num=1
  left join temp6 on temp6.id=sg.shipment_group_id
  left join temp_t tt on tt.id=sg.shipment_group_id and tt.state='SHIPMENT_RTO'
  left join temp_t tt1 on tt1.id=sg.shipment_group_id and tt1.state='SHIPMENT_DELIVERED'
  left join temp_t tt2 on tt2.id=sg.shipment_group_id and tt2.state='SHIPMENT_RTO_DELIVERED'
  left join temp_t tt3 on tt3.id=sg.shipment_group_id and tt3.state='SHIPMENT_RTO_TO_SELLER'
  left join temp_t tt4 on tt4.id=sg.shipment_group_id and tt4.state='SHIPMENT_RTO_ABSORBED'
  left join temp_t tt5 on tt5.id=sg.shipment_group_id and tt5.state='SHIPMENT_LOST'
  left join temp_t tt6 on tt6.id=sg.shipment_group_id and tt6.state='SHIPMENT_RAD'
  left join topr on topr.od=sg.seller_order_id 
  left join temp21 on temp21.id=sg.shipment_group_id and temp21.state='IN_SCAN'
  left join temp21  as t21a on t21a.id=sg.shipment_group_id and t21a.state='OUT_SCAN'
  left join temp21a as t21b on t21b.id=sg.shipment_group_id and t21b.state='RTO_IN_SCAN'
  left join temp21a as t21c on t21c.id=sg.shipment_group_id and t21c.state='RTO_OUT_SCAN'
  left join temp25 on temp25.id=sg.seller_order_id
  left join temp31 on temp31.id=sg.shipment_group_id
  left join temp38 on temp38.id=sg.shipment_group_id  and temp38.row_num=1
  left join ds_sqls_collected_advance_consumable cac on cac.consumed_for=sg.shipment_group_id
  left join temp45 on temp45.id=sg.shipment_group_id
  left join temp47 on temp47.id=sg.shipment_group_id
  left join temp48 on temp48.id=sg.shipment_group_id
  left join temp50 on temp50.id=sg.shipment_group_id and temp50.row_num=1
  left join temp51a on temp51a.id=sg.shipment_group_id
  left join temp52 on temp52.id=sg.shipment_group_id
  left join temp67 on temp67.id=sg.shipment_group_id
  left join vsx on vsx.id=sg.shipment_group_id and vsx.row_num=2
  left join ds_sqls_logistics_pincode_hub_master hm1 on hm1.pincode=sg.shipfrom_pincode and hm1.service_type='PICKUP'
  left join  (select x1.shipment_id as shipment_id,x1.state as state,x1.hub_org_unit_id,row_number() over (partition by x1.shipment_id order by CAST(x1.updated_at/1000 as bigint) desc) as rwn from ds_sqls_pickup_task x1) pt on pt.shipment_id=sg.shipment_group_id and pt.rwn=1
  left join ds_sqls_org_units oh on oh.org_unit_id=pt.hub_org_unit_id
  left join temp71 on temp71.id=sg.shipment_group_id
  left join temp77 on temp77.id=sg.shipment_group_id
  left join ds_sqls_return_items rit on rit.return_shipment_id=sg.shipment_group_id
  left join ds_sqls_users us on us.user_id=orgb.user_id_owner
  left join temp1a on temp1a.id=sg.shipment_group_id
  left join udaan_express u on u.shipment_group_id=sg.shipment_group_id and u.leg_type='PICKUP'
  left join udaan_express u1 on u1.shipment_group_id=sg.shipment_group_id and u1.leg_type='FWD_DELIVERY'
  left join (select dtt.shipment_id id, min(CAST(dtt.updated_at/1000 as bigint)) t1 from ds_sqls_delivery_task dtt where dtt.state='OUT_FOR_DELIVERY' group by dtt.shipment_id) xt on xt.id=sg.shipment_group_id
  left join temp1b on temp1b.id=sg.shipment_group_id and temp1b.row_num=1
  left join ds_sqls_hub_shipment_details hsd on hsd.shipment_id=sg.shipment_group_id
  left join temp1c as tec on tec.id=sg.shipment_group_id
  left join temp51ac as t51ac on t51ac.id=sg.shipment_group_id
  left join temp1d as t1d on t1d.id=sg.shipment_group_id
  left join temp2b t2b on t2b.id=sg.shipment_group_id
  left join (select distinct il.shipment_group_id id,si.buyer_org_id payee_id from ds_sqls_invoice_shipment_group_association il join ds_sqls_invoice si where from_unixtime(CAST(il.created_at/1000 as bigint), 'yyyy-MM-dd') >= date_sub('${sprinkle.latest.partition}', ${window}) and si.invoice_type='LOGISTIC_SERVICE' ) il on il.id=sg.shipment_group_id
  left join (select cs.order_id,count(cs.interaction_id) total_interactions, collect_set(cs.type) type, collect_set(cs.subtype) as subtype,from_unixtime(min(cs.created_time),'yyyy-MM-dd HH:mm:ss') first_interaction_at from cs where cs.interaction_team='CustomerSupport' and cs.interaction_type in ('IncomingCall','Chat') group by cs.order_id) cs on cs.order_id=sg.seller_order_id

  where sg.current_active=1 and  from_unixtime(cast((sg.created_at+19800000)/1000 as bigint), 'yyyy-MM-dd') >= date_sub('${sprinkle.latest.partition}', ${window});

  drop table temp1;
  drop table temp_t;
  drop table temp6;
  drop table temp2;
  drop table temp3;
  drop table temp4;
  drop table temp21;
  drop table temp21a;
  drop table temp25;
  drop table temp31;
  drop table temp38;
  DROP TABLE temp77;

