drop TABLE wh_flow ;
drop TABLE rri;
---------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS rri STORED AS ORC AS 

SELECT stRj.receive_order_id AS receive_order_id, stRj.id AS reject_order_note, NULL AS inward_order_note, rj_sku_items.sku_item_id AS sku_item_id, isku_r.sku_id AS sku_id, rj_sku.name AS sku_name,
rj_sku.capacity_consumed AS sku_wt, isku_r.state sku_item_state, isku_r.created_at sku_item_created, isku_r.updated_at sku_item_updated,rj_sku.storage_profile sku_storage_profile

FROM ds_sqls_wh_stock_reject_note AS stRj 
LEFT JOIN ds_sqls_wh_document_skuitems AS rj_sku_items ON rj_sku_items.document_id = stRj.id 
LEFT JOIN ds_sqls_wh_inventory_skuitems AS isku_r ON isku_r.sid = rj_sku_items.sku_item_id 
LEFT JOIN ds_sqls_wh_inventory_sku AS rj_sku ON rj_sku.id = isku_r.sku_id 

--WHERE isku_r.sku_id LIKE 'TLF%' 

UNION 

SELECT inN.receive_order_id AS receive_order_id, NULL reject_order_note, inN.id AS inward_order_note, in_sku_items.sku_item_id AS sku_item_id, isku.sku_id AS sku_id, in_sku.name AS sku_name,
in_sku.capacity_consumed AS sku_wt, isku.state sku_item_state, isku.created_at sku_item_created, isku.updated_at sku_item_updated, in_sku.storage_profile sku_storage_profile

FROM ds_sqls_wh_stock_inward_note AS inN 
LEFT JOIN ds_sqls_wh_document_skuitems AS in_sku_items ON in_sku_items.document_id = inN.id
LEFT JOIN ds_sqls_wh_inventory_skuitems AS isku  ON isku.sid = in_sku_items.sku_item_id 
LEFT JOIN ds_sqls_wh_inventory_sku AS in_sku ON in_sku.id = isku.sku_id

--WHERE isku.sku_id LIKE 'TLF%' 
;

----------------------------------------------------------------------------------------------------------------

drop TABLE ipt;
CREATE TABLE IF NOT EXISTS ipt STORED AS ORC AS 
SELECT st_in.id  inward_note_id, st_in.created_at  inward_created_at, st_in.updated_at  inward_updated_at, st_in.state  inward_note_state,
inven_sku_item.sid  sku_item_id, inven_sku.id  sku_id, inven_sku.name  sku_name, inven_sku.capacity_consumed  sku_wt, pp1.document_id  putlist_id,
put.created_at  putlist_created_at, put.updated_at  putlist_processed_at, put.state  putlist_state, put.type  putlist_type,
row_number() OVER (PARTITION BY inven_sku_item.sid ORDER BY put.created_at DESC) AS row_numb

FROM ds_sqls_wh_stock_inward_note  st_in
LEFT JOIN ds_sqls_wh_document_skuitems  doc_sku_item ON st_in.id = doc_sku_item.document_id --will have sku_item_id against it
LEFT JOIN ds_sqls_wh_puttables_pickables  pp1 ON doc_sku_item.sku_item_id = pp1.skuitem_id AND pp1.document_id LIKE '%PUL%'
LEFT JOIN ds_sqls_wh_putlist  put ON pp1.document_id = put.putlist_id
LEFT JOIN ds_sqls_wh_inventory_skuitems  inven_sku_item ON doc_sku_item.sku_item_id = inven_sku_item.sid
LEFT JOIN ds_sqls_wh_inventory_sku  inven_sku ON inven_sku.id = inven_sku_item.sku_id ;

-----------------------------------------------------------------------------------------------------------------

drop TABLE rot;
CREATE TABLE IF NOT EXISTS rot STORED AS ORC AS
SELECT doc_sku_item.sku_item_id  sku_item_id, do.id dispatch_order, do.type do_type, do.state Do_state, do.created_at dispatch_order_created, do.updated_at dispatch_order_updated, reser.id  reservation_id, reser.state  reservation_state, reser.created_at  reservation_created, reser.updated_at  reservation_updated, 
pick.picklist_id  picklist_id, pick.created_at  picklist_created, pick.updated_at  picklist_updated, pick.type  picklist_type, pick.state  picklist_state, 
ot.id  outward_note, ot.type  outward_note_type, ot.created_at  outward_note_created, ot.updated_at  outward_note_updated, ot.state  outward_note_state,
row_number() OVER (PARTITION BY doc_sku_item.sku_item_id ORDER BY reser.created_at DESC)  row_num

FROM ds_sqls_wh_document_skuitems  doc_sku_item
LEFT JOIN ds_sqls_wh_sku_reservation reser ON doc_sku_item.document_id = reser.id AND doc_sku_item.document_id LIKE '%FRES%'
LEFT JOIN ds_sqls_wh_puttables_pickables  pp2 ON doc_sku_item.sku_item_id = pp2.skuitem_id AND pp2.document_id LIKE '%PIL%'
LEFT JOIN ds_sqls_wh_picklist  pick ON pp2.document_id = pick.picklist_id
LEFT JOIN ds_sqls_wh_sku_quantities  quant_ot ON doc_sku_item.document_id = quant_ot.reservation_id AND (quant_ot.document_id LIKE 'DOO%')
LEFT JOIN ds_sqls_wh_dispatch_order do ON do.id=quant_ot.document_id
LEFT JOIN ds_sqls_wh_stock_outward_note  ot ON quant_ot.document_id = ot.document_id

WHERE doc_sku_item.document_id LIKE '%FRES%' ;
--------------------------------------------------------------------------------------------------------------------

drop TABLE final1;
CREATE TABLE IF NOT EXISTS final1 STORED AS ORC AS 

SELECT DISTINCT 

---- receive order ----
ro_rj.id  receive_order_id, 
ro_rj.wh_id  warehouse_id, 
wh.name  warehouse_name, 
ro_rj.org_id  seller_org_id, 
dsg.display_name seller_name,


from_unixtime(cast((ro_rj.created_at+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss')  receive_order_created_at,
from_unixtime(cast((ro_rj.updated_at+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss')  receive_order_updated_at, 
ro_rj.state  receive_order_state, 
ro_rj.type  receive_order_type,
rri.reject_order_note  reject_order_note, 
rjt.state  reject_order_state, 
from_unixtime(cast((rjt.created_at+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') reject_order_created, 
from_unixtime(cast((rjt.updated_at+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') reject_note_updated,


--- inwarding process ---

itp.inward_note_id  inward_note_id, 
from_unixtime(cast((itp.inward_created_at+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') inward_note_created_at, 
from_unixtime(cast((itp.inward_updated_at+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') inward_updated_at, 
itp.inward_note_state  inward_note_state,



--- putlist ----

itp.putlist_id  putlist_id,
from_unixtime(cast((itp.putlist_created_at+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') putlist_created_at, 
from_unixtime(cast((itp.putlist_processed_at+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') putlist_processed_at, 
itp.putlist_type  putlist_type, 
itp.putlist_state  putlist_state,


--- sku reservation ---
rto.dispatch_order dispatch_order_id,
rto.do_type dispatch_order_type,
rto.Do_state dispatch_order_state,


from_unixtime(cast((rto.dispatch_order_created+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') dispatch_order_created,
from_unixtime(cast((rto.dispatch_order_updated+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss')  dispatch_order_updated,

rto.reservation_id  reservation_id, 
split(rto.reservation_id,'-')[1] order_line_id,
sol.seller_order_id  order_id,
sg.shipment_group_id shipment_group_id,
so.order_status order_status,
from_unixtime(cast((so.created_at+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss')  order_date,
o1.unit_name tc_hub,
o1.org_unit_id hub_org_unit_id,
pt1.picked_up_time tc_picked_up_time,
from_unixtime(cast((sd.del_date+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss')  del_date,

rri.sku_item_id  sku_item_id, 
rri.sku_item_state sku_item_state,
from_unixtime(cast((rri.sku_item_created+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') sku_item_created,
from_unixtime(cast((rri.sku_item_updated+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') sku_item_updated,

get_json_object(get_json_object(az.props["ListingDetails"],"$.[0]"),"$.listingId") ListingID,
sl.status Listing_status,
sl.unit_price_wth_tax Listing_per_unit_price_with_tax,
rri.sku_id  sku_id, 
rri.sku_storage_profile sku_storage_profile,
rri.sku_name  sku_name, 
sl.title Listing_details,
sl.category category,
sl.sub_category sub_category,
sl.vertical vertical,

rri.sku_wt  sku_wt, 
from_unixtime(cast((rto.reservation_created+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') reservation_created, 
from_unixtime(cast((rto.reservation_updated+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') reservation_updated, 
rto.reservation_state  reservation_state,

--- picklist ---

rto.picklist_id  picklist_id, 
from_unixtime(cast((rto.picklist_created+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') picklist_created, 
from_unixtime(cast((rto.picklist_updated+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') picklist_updated, 
rto.picklist_type  picklist_type, 
rto.picklist_state  picklist_state,


--- outward not ---
rto.outward_note  outward_note, 
from_unixtime(cast((rto.outward_note_created+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') outward_note_created, 
from_unixtime(cast((rto.outward_note_updated+19800000)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') outward_note_updated, 
rto.outward_note_type  outward_note_type,
rto.outward_note_state  outward_note_state,

CASE WHEN rto.outward_note IS NOT NULL AND rto.outward_note_state = 'DISPATCHED' THEN 'DISPATCHED' --1
  WHEN  rto.outward_note IS NOT NULL AND rto.outward_note_state = 'DISPATCHING' THEN 'DISPATCHING'
  ELSE CASE WHEN rto.picklist_id IS NOT NULL AND rto.picklist_state = 'PICKED' THEN 'PICKLIST_PICKED' --2
    WHEN rto.picklist_id IS NOT NULL AND rto.picklist_state = 'PICKING' THEN 'PICKLIST_PICKING'
    WHEN rto.picklist_id IS NOT NULL AND rto.picklist_state = 'DISPATCHED' THEN 'PICKLIST_DISPATCHED'
    WHEN rto.picklist_id IS NOT NULL AND rto.picklist_state = 'CREATED' THEN 'PICKLIST_CREATED'
    ELSE CASE WHEN rto.reservation_id IS NOT NULL AND rto.reservation_state = 'RESERVED' THEN 'RESERVED'  --3
      WHEN rto.reservation_id IS NOT NULL AND rto.reservation_state = 'REQUESTED' THEN 'RESERVATION_REQUESTED'
      WHEN rto.reservation_id IS NOT NULL AND rto.reservation_state = 'CANCELLED' THEN 'RESERVATION_CANCELLED'
      ELSE CASE WHEN itp.putlist_id IS NOT NULL AND itp.putlist_state = 'SUGGESTED' THEN 'PUTLIST_SUGGESTED' --4
        WHEN itp.putlist_id IS NOT NULL AND itp.putlist_state = 'STOCKED' THEN 'PUTLIST_STOCKED'
        ELSE CASE WHEN itp.inward_note_id IS NOT NULL AND itp.inward_note_state = 'STOCKED' THEN 'INWARD_NOTE_STOCKED' --5
          WHEN itp.inward_note_id IS NOT NULL AND itp.inward_note_state = 'STOCKING' THEN 'INWARD_NOTE_STOCKING'
          WHEN itp.inward_note_id IS NOT NULL AND itp.inward_note_state = 'ABANDONED' THEN 'INWARD_NOTE_ABANDONED'
          ELSE CASE WHEN rri.reject_order_note IS NOT NULL THEN 'REJECTED' --6
            ELSE CASE WHEN ro_rj.id IS NOT NULL AND ro_rj.state = 'PROCESSED' THEN 'RECEIVE_ORDER_PROCESSED' --7
              WHEN ro_rj.id IS NOT NULL AND ro_rj.state = 'ACCEPTED' THEN 'RECEIVED_ORDER_ACCEPTED'
              WHEN ro_rj.id IS NOT NULL AND ro_rj.state = 'CANCELLED' THEN 'RECEIVE_ORDER_CANCELLED'
                
              END
            END
          END
        END
      END
    END
  END AS sku_status


FROM rri
LEFT JOIN ds_sqls_wh_receive_order  ro_rj ON rri.receive_order_id = ro_rj.id
LEFT JOIN ds_sqls_wh_stock_reject_note  rjt ON rri.reject_order_note = rjt.id
LEFT JOIN ipt itp ON itp.sku_item_id = rri.sku_item_id AND itp.row_numb = 1
LEFT JOIN rot rto ON rri.sku_item_id = rto.sku_item_id AND rto.row_num = 1 
LEFT JOIN ds_sqls_wh_warehouse  wh ON wh.id = ro_rj.wh_id 
LEFT JOIN ds_sqls_orgs dsg on dsg.org_id= ro_rj.org_id
LEFT join ds_sqls_seller_order_line sol on get_json_object(sol.misc_info,'$.ff_res_id')=rto.reservation_id and sol.current_active=1
left join ds_sqls_seller_order so on so.order_id=sol.seller_order_id and so.current_active=1
left join ds_sqls_shipment_group sg on sg.seller_order_id=so.order_id and sg.current_active=1
left join (select pt.shipment_id as id , from_unixtime((max(unix_timestamp(pt.updated_at,'yyyy-MM-dd HH:mm:ss'))+19800),'yyyy-MM-dd HH:mm:ss') as picked_up_time 
          from ds_sqls_pickup_task pt 
          where pt.state='PICKED_UP' 
          group by pt.shipment_id) pt1 ON pt1.id= sg.shipment_group_id
left join (
select se.shipment_id id,se.from_org_unit_id hid,row_number() OVER (PARTITION BY se.shipment_id ORDER BY se.sequence_number asc) AS rwn from ds_sqls_shipment_execution se where se.leg_type='TRANSPORT')
z1 on z1.id=sg.shipment_group_id and z1.rwn=1
LEFT join (select sst1.shipment_group_id as id, sst1.state as state, max(sst1.value) as del_date 

from ds_sqls_shipment_group_metadata sst1 

where sst1.key_attrib='event_time' and sst1.state='SHIPMENT_DELIVERED'
group by sst1.shipment_group_id,sst1.state) sd
ON sd.id=sg.shipment_group_id

left join ds_sqls_org_units o1 on o1.org_unit_id=z1.hid
LEFT JOIN ds_azure_table_managedffskus_prod az ON az.row_key=rri.sku_id
LEFT JOIN ds_stream_listing sl ON get_json_object(get_json_object(az.props["ListingDetails"],"$.[0]"),"$.listingId")= sl.listing_id;

drop TABLE warehouse;
CREATE TABLE IF NOT EXISTS warehouse STORED AS ORC AS SELECT DISTINCT * FROM final1;

drop TABLE ipt;
drop TABLE rot;
drop TABLE final1;
drop TABLE final_sku_wt;
drop TABLE rri;
