use zdp_acc_ctrip;
set mapreduce.reduce.memory.mb=5072;
alter table zdp_etl_ctrip.biz_hotel_order_ctrip_info ADD IF NOT EXISTS PARTITION  (dt=${hivevar:yesterday}) location '/wh/entity/ctrip/biz/biz_hotel_order_ctrip_info/${hivevar:yesterday}/';
insert overwrite table zdp_etl_ctrip.biz_hotel_order_ctrip_info partition(dt=${hivevar:yesterday})
select biz_hotel_order.order_id as order_id,
       biz_hotel_order.order_date as order_date,
       biz_hotel_order.checkin_date as checkin_date,
       biz_hotel_order.checkout_date as checkout_date,
       biz_hotel_order.room_id as room_id,
       biz_hotel_order.master_hotel_id as master_hotel_id,
       biz_hotel_order.sub_hotel_id as hotel_id,
       biz_hotel_order.user_id as user_id,
       biz_hotel_order.customer_id as customer_id,
       biz_hotel_order.order_status as order_status,
       biz_hotel_order.actually_checkout_date as actually_checkout_date,
       biz_hotel_order.is_room_reseverd as is_room_reseverd,
       biz_hotel_order.pre_room_night as pre_room_night,
       biz_hotel_order.real_room_night as real_room_night,
       biz_hotel_order.pre_order_amounts as pre_order_amounts,
       biz_hotel_order.order_source_type as order_source_type,
       biz_hotel_order.modify_type as modify_type,
       biz_hotel_order.before_modified_order_id as before_modified_order_id,
       biz_hotel_order.after_modified_order_id as after_modified_order_id,
       biz_hotel_order.is_validity as is_validity,
       biz_hotel_order.currency as currency,
       biz_hotel_order.pay_type as pay_type,
       biz_hotel_order.collection_type as collection_type,
       biz_hotel_order.customer_source as customer_source,
       biz_hotel_order.pre_order_room_num as pre_order_room_num,
       biz_hotel_order.pre_book_days as pre_book_days,
       biz_hotel_order.order_source as order_source,
       biz_hotel_order.acutally_room_num as acutally_room_num,
       biz_hotel_order.update_time as update_time,
       biz_hotel_order.host_head as host_head,
       biz_hotel_order.host_mid as host_mid,
       biz_hotel_order.host_last as host_last,
       biz_hotel_order.customer_type as customer_type,
       biz_hotel_order.is_checkin as is_checkin,
       biz_hotel_order.pay_time as pay_time,
       biz_hotel_order.gathering_type as gathering_type,
       biz_hotel_order.is_direct_order as is_direct_order,
       biz_hotel_order.room_night_delay as room_night_delay,
       biz_hotel_order.is_comment_confirm as is_comment_confirm,
       biz_hotel_order.sub_order_source as sub_order_source,
       biz_hotel_order.city_id as city_id,
       biz_hotel_order.is_vacation_order as is_vacation_order,
       biz_hotel_order.is_discount_order as is_discount_order,
       biz_hotel_order.is_corp_order as is_corp_order,
       biz_hotel_order.cancel_reason as cancel_reason,
       biz_hotel_order.real_price as real_price,
       biz_hotel_order.channel_id as channel_id,
       biz_hotel_order.alliance_id as alliance_id,
       biz_hotel_order.room_name as room_name ,
       case
            when biz_hotel_order.order_id  is not null  and hotel_order_merge.order_id is not null  then hotel_order_merge.source_tab
            when hotel_order_merge.order_id is null or   hotel_order_merge.order_id  = "" then 'origin_source'
            else 'origin_source' end as  source_tab
        from(
select order_hotel.*,
       room_info.room_name from
    (
        select hotelorder.* ,
               hotelinfo.master_hotel_id as master_hotel_id
            from (
         select  ordid as order_id,
                 orddate as order_date,
                 arrivaltime as checkin_date,
                 departuretime as checkout_date,
                 roomid as room_id,
                 subhotelid as sub_hotel_id,
                 userid as user_id,
                 customerid as customer_id,
                 ordstatus as order_status,
                 leavedtime as actually_checkout_date,
                 preorderpersons as pre_order_person_num,
                 holdroomtype as is_room_reseverd,
                 preroomnight as pre_room_night,
                 realroomnight as real_room_night,
                 preorderamounts as pre_order_amounts,
                 serverfrom as order_source_type,
                 referencetype as modify_type,
                 reference as before_modified_order_id,
                 alteredordid as after_modified_order_id,
                 isvalidity as is_validity,
                 currency,
                 paytype as pay_type,
                 balancetype as collection_type,
                 customerfrom as customer_source,
                  ordroomnum as pre_order_room_num,
                  prebookdays as pre_book_days,
                  ordsource as order_source,
                  realroomnum as acutally_room_num,
                  lastupdated as update_time,
                 ip1 as host_head,
                 ip2 as host_mid,
                 ip3 as host_last,
                 customertype as customer_type,
                 checkin as is_checkin,
                 paytime as pay_time,
                 gathertype as gathering_type,
                 isdirectorder as is_direct_order,
                 addsronight as room_night_delay,
                 commentconfirm as is_comment_confirm,
                 ordsubsource as sub_order_source,
                 cityid as city_id,
                 isvacationorder as is_vacation_order,
                 isdiscountorder as is_discount_order,
                 iscorporder as is_corp_order,
                 cancelreason as cancel_reason,
                 realprice as real_price,
                 channelid as channel_id,
                  allianceid as alliance_id from zdp_acc_ctrip.biz_hotelorder where dt=${hivevar:hotel_order_dt})hotelorder
            left join
               ( select hotelid as hotel_id,
                 case masterhotelid when 0 then hotelid
                 when -1 then hotelid else masterhotelid
                 end as master_hotel_id
                 from zdp_acc_ctrip.dict_hotelinfo where dt=${hivevar:hotel_info_dt})hotelinfo
               on hotelorder.sub_hotel_id = hotelinfo.hotel_id
               where hotelinfo.hotel_id  is not null
             ) order_hotel
             left join
              (select roomid as room_id, roomname as room_name from zdp_acc_ctrip.dict_roominfo where dt=${hivevar:hotel_order_dt})room_info
              on order_hotel.room_id = room_info.room_id where room_info.room_id  is not null

        ) biz_hotel_order
left join
( select distinct merge.order_id, merge.source_tab from
    (
      select  rapid_accommodation.order_id as order_id,
              'rapid_accommodation'  as source_tab
        from
            (select order_id,  external_pay_no,external_pay_hashcode,update_time
        from
            zdp_acc_ctrip.biz_order_water_no_info where dt=${hivevar:rapid_checkin_dt}
                and  unix_timestamp(update_time)>=${hivevar:date_before_yesterday_timestamp}
                and  unix_timestamp(update_time)<=${hivevar:yesterday_date_timstamp}) rapid_accommodation
        left join
                (select ordid as order_id,
                     roomid as room_id from zdp_acc_ctrip.biz_hotelorder where dt=${hivevar:hotel_order_dt}
                )hotel_order
        on rapid_accommodation.order_id = hotel_order.order_id where hotel_order.room_id is not null
union all
    select  hotel_order_pms.order_id as order_id,
            'pms_order' as source_tab
            from
            (select md5(extraordid) as order_id ,
                    hotelid as hotel_id
             from   zdp_acc_ctrip.biz_hotelorder_pms where dt=${hivevar:pms_order_dt} and unix_timestamp(createtime)>=${hivevar:date_before_yesterday_timestamp}  and  unix_timestamp(createtime)<=${hivevar:yesterday_date_timstamp} 
            ) hotel_order_pms
  left join
            (select ordid as order_id,roomid as room_id from
                    zdp_acc_ctrip.biz_hotelorder where dt=${hivevar:hotel_order_dt} )hotel_order
   on hotel_order_pms.order_id = hotel_order.order_id where hotel_order.room_id is not null

union all
     select  mapping_order.order_id as order_id,
            'mapping_order' as source_tab	
            from
            (select orderid as order_id ,
                    clientid as client_id from
                    zdp_acc_ctrip.biz_mapping_order_client where dt=${hivevar:mapping_order_dt}
            ) mapping_order
    left join
            (select ordid as order_id,
                    roomid as room_id from
                    zdp_acc_ctrip.biz_hotelorder where dt=${hivevar:hotel_order_dt})hotel_order
    on mapping_order.order_id = hotel_order.order_id where hotel_order.room_id is not null

union all
    select switch_order.order_id as order_id,
      'switch_order' as source_tab
      from
            (select ordid as order_id,
                    price  from
                    zdp_acc_ctrip.biz_order_switch where dt=${hivevar:switch_order_dt}
            ) switch_order
    left join
            (select ordid as order_id,
                    roomid as room_id from
                    zdp_acc_ctrip.biz_hotelorder where dt=${hivevar:hotel_order_dt})hotel_order
         on switch_order.order_id = hotel_order.order_id where hotel_order.room_id is not null
    ) merge
 ) hotel_order_merge
on biz_hotel_order.order_id = hotel_order_merge.order_id  
