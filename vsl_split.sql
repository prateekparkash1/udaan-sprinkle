drop table vsl_split_temp;
CREATE TABLE if not exists vsl_split_temp STORED AS ORC as 
select distinct vm.vsl_id,cm.pick_up_cluster as pickup_cluster, 
cm.pincode as pickup_pincode,
vm.dest_pincode as dest_pincode,
vm.dest_city as dest_city,
vm.dest_state as dest_state,
vm.dest_tier as dest_tier,
ca.city_class as dest_city_class,
vm.zone as lane, 
REGEXP_REPLACE(ls.service_name,'\\r|\\t|\\n','') as service, 
orgs.display_name as vendor, 

concat(cast(get_json_object(REGEXP_REPLACE(sp.service_provider_properties,'\\r|\\t|\\n',''),'$.properties.min_wt')/1000 as int),'-',cast(get_json_object(REGEXP_REPLACE(sp.service_provider_properties,'\\r|\\t|\\n',''),'$.properties.max_wt')/1000 as int)) as weight_range,

case when get_json_object(REGEXP_REPLACE(sp.service_provider_properties,'\\r|\\t|\\n',''),'$.properties.shipping_profiles') is not null then get_json_object(REGEXP_REPLACE(sp.service_provider_properties,'\\r|\\t|\\n',''),'$.properties.shipping_profiles') else 'Missing' end as shipping_details,

case when get_json_object(REGEXP_REPLACE(sp.service_provider_properties,'\\r|\\t|\\n',''),'$.properties.multi_box_enabled')='true' then 'true' else 'false' end as multi_box_enabled,



from_unixtime(cast(((vm.created_at)+19800000)/1000 as bigint),'dd-MM-yyyy') as created_at,
from_unixtime(cast(((vm.updated_at)+19800000)/1000 as bigint),'dd-MM-yyyy') as updated_at 

from ds_sqls_logistics_pincode_cluster_master cm 
left join ds_sqls_logistics_route_vsl_master vm on vm.pick_up_cluster=cm.pick_up_cluster 
left join ds_sqls_logistics_serviceability ls on ls.route_id=vm.vsl_id 
left join ds_sqls_service_providers sp on sp.serviceability_id=ls.serviceability_id 
left join ds_sqls_orgs orgs on orgs.org_id=sp.service_provider_org_id 
left join categorization ca on ca.pincode=vm.dest_pincode 

where cm.current_active=1 and vm.current_active=1 and ls.enabled=1 and sp.enabled=1;
drop table vsl_split;
CREATE TABLE IF NOT EXISTS vsl_split STORED AS ORC AS SELECT * FROM vsl_split_temp;
drop table vsl_split_temp;