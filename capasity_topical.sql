with par as (select w.clearing_id as warehouse_id
             from whc_go_crud_warehouse.warehouses w
             where w.clearing_id = 19262731541000),
     topology as (
         select distinct th1.id                as building_id
                       , th2.id                as zone_id
                       , th3.id                as rack_id
                       , th4.id                as cell_id
                       , ci.full_name
                       , ci.cell_type_id
                       , ci.closed_for_placing as cell_closed
                       , ri.closed_for_placing as rack_closed
                       , ci.max_avg_sku_dim
                       , ci.height
                       , ci.width
                       , ci."length"
         from wms_topology.topology_hierarchy th1
                  join wms_topology.topology_hierarchy th2 on th2.parent_id = th1.id and th2.is_deleted = 'false'
                  join wms_topology.topology_hierarchy th3 on th3.parent_id = th2.id and th3.is_deleted = 'false'
                  join wms_topology.topology_hierarchy th4 on th4.parent_id = th3.id and th4.is_deleted = 'false'
                  join wms_topology.cell_info ci on ci.id = th4.id
                  join wms_topology.rack_info ri on ri.id = th3.id
         where th1."type" = 1
           and th1.is_deleted = 'false'
           and ci.is_deleted = 'false'
     )
        ,
/* считаем объем товара, лежащего на местах хранения*/
     item_volume as (
         select iip.place_id
              , iip.item_volume
              , sum(iip.qty_item)           as qty_item
              , count(distinct iip.qty_sku) as qty_sku
         from (
                  select iip.place_id
                       , sum(ir.width * ir.height * ir.depth * 1000) as item_volume
                       , sum(iip.quantity)                           as qty_item
                       , iip.item_id                                 as qty_sku
                  from wms_csharp_service_storage_all.item_in_place iip
                           join (
                      select i.sourcekey as id
                           , w.Width     as width
                           , h.Height    as height
                           , d.Depth     as depth
                      from dwh_data.anc_item i
                               join dwh_data.Atr_Item_Height h using (itemid)
                               join dwh_data.Atr_Item_Depth d using (itemid)
                               join dwh_data.Atr_Item_Width w using (itemid)) ir on ir.id = iip.item_id
                  where iip.place_type = 2 /* ячейка*/
                  group by iip.place_id
                         , iip.item_id
                  union all
                  select inip.place_id
                       , sum(ir.width * ir.height * ir.depth * 1000) as item_volume
                       , count(inip.id)                              as qty_item
                       , inip.item_id                                as qty_item
                  from wms_csharp_service_storage_all.instance_in_place inip
                           join (
                      select i.sourcekey as id
                           , w.Width     as width
                           , h.Height    as height
                           , d.Depth     as depth
                      from dwh_data.anc_item i
                               join dwh_data.Atr_Item_Height h using (itemid)
                               join dwh_data.Atr_Item_Depth d using (itemid)
                               join dwh_data.Atr_Item_Width w using (itemid)) ir on ir.id = inip.item_id
                  where inip.place_type = 2 /* ячейка*/
                  group by inip.place_id
                         , inip.item_id
              ) iip
         group by iip.place_id
                , iip.item_volume
     )
select t.full_name
     , t.cell_id
     , t.cell_closed                                           as 'закрыт к размещению ячейка'
     , t.rack_closed                                           as 'закрыт к размещению шкаф'
     --, CAST( concat( concat( concat( zi.name , ' (' ) , zi.description ) , ')' ) as varchar(255) ) as zone_name
     --, CAST( ct.purpose as varchar(255) )                                                          as type_name
     , CAST(
        t.width * t.height * t."length" / 1e6 as float)        as 'полезный объем' /* за полезный объем принимаем макс_капасити на типе ячейки, если он null, то считаем объем из габаритов ячейки*/
     , CAST(round(isnull(sum(iv.item_volume), 0), 2) as float) as 'объем занимаемый item'
     , CAST(round(isnull(sum(iv.item_volume), 0) /
                  t.width * t.height * t."length" / 1e6
    , 2) as float)                                             as 'полезное хранение' /* полезное хранилище*/
     , sum(iv.qty_item)                                        as 'кол-во itm'
     , ct.sku_qty                                              as 'максимальное кол-во sku'
     , sum(iv.qty_sku)                                         as 'кол-во sku факт'
from topology t
         join wms_topology.cell_type ct on ct.id = t.cell_type_id
         join par on par.warehouse_id = ct.warehouse_id
         join wms_topology.zone_info zi on zi.id = t.zone_id and zi.zone_type = 3 and zi.is_deleted = 'false'
         join wms_topology.rack_info ri
              on ri.id = t.rack_id and ri.purpose_type = 0 and ri.rack_type = 0 and ri.is_deleted = 'false'
         left join item_volume iv on iv.place_id = t.cell_id
group by t.full_name
       , t.rack_id
       , t.cell_id
       , t.cell_closed
       , t.rack_closed
       , t.width
       , t.height
       , t."length"
       , ct.sku_qty
       --, concat( concat( concat( zi.name , ' (' ) , zi.description ) , ')' )
       , ct.purpose

