use zdp_etl_ctrip;
insert overwrite table dict_third_part_hotel_info partition(dt=${hivevar:yesterday})
select distinct merge.master_hotel_id,merge.hotel_name_ctrip,merge.hotel_id,merge.hotel_name,merge.city_name,merge.city_name_en,
                merge.address, merge.contact,merge.hotel_type,merge.hotel_star,merge.longitude,merge.latitude,merge.open_year,
                merge.decoration_year,merge.status,merge.zone_name,merge.area_name,merge.hotel_detail,
                merge.hotel_service,merge.facilities,merge.hotel_desc,merge.fax,merge.brand_name,merge.source
  from (
    select
          mapinfo.master_hotel_id as master_hotel_id,
          mapinfo.hotel_name_ctrip as hotel_name_ctrip,
          mapinfo.hotel_id as hotel_id,
          qunar.hotel_name as hotel_name,
          qunar.city_name as city_name,
          qunar.city_name_en as city_name_en,
          qunar.address as address,
          qunar.contact as contact,
          qunar.hotel_type as hotel_type,
          qunar.hotel_star as hotel_star,
          qunar.longitude as longitude,
          qunar.latitude as latitude,
          qunar.open_year as open_year,
          qunar.decoration_year as decoration_year,
          '' as status,
          qunar.zone_name as zone_name,
          '' as area_name,
          '' as hotel_detail,
          '' as hotel_service,
          qunar.facilities as facilities,
          qunar.hotel_desc as hotel_desc,
          qunar.fax as fax,
          qunar.brand_name as brand_name,
         'qunar' as source
          from
          (select id,qunarid as qunar_id,
                     hotelid as hotel_id,
                     citynameen as city_name_en,
                     address,
                     citynamecn as city_name,
                     hotelname as hotel_name,
                     phone as contact,
                     hoteltype as hotel_type,
                     status,
                     brandname as brand_name,
                     fax,
                     openyear as open_year,
                     fityear as decoration_year,
                     cast(star as int) as hotel_star,
                     longitude,
                     latitude,
                     description as hotel_desc,
                     zonename as zone_name,
                     facilities
                      from zdp_acc_ctrip.dict_hotelinfo_qunar where dt=${hivevar:qunar_dt} ) qunar
                  left join
          (select masterhotelid as master_hotel_id,
                                      chotelname as hotel_name_ctrip,
                                      websiteid ,
                                      hotelid as hotel_id from zdp_acc_ctrip.dict_hotelmap
                                      where dt=${hivevar:yesterday} and websiteid = 400 ) mapinfo
                                       on mapinfo.hotel_id = qunar.hotel_id where qunar.hotel_id is not null and mapinfo.master_hotel_id is not null


union all
select
          mapinfo.master_hotel_id as master_hotel_id,
          mapinfo.hotel_name_ctrip as hotel_name_ctrip,
          mapinfo.hotel_id as hotel_id,
          ali.hotel_name as hotel_name,
          ali.city_name as city_name,
          '' as city_name_en,
          ali.address as address,
          '' as contact,
          '' as hotel_type,
          ali.hotel_star as hotel_star,
          ali.longitude as longitude,
          ali.latitude as latitude,
          ali.open_year as open_year,
          ali.decoration_year as decoration_year,
          '' as status,
          '' as zone_name,
          ali.area_name as area_name,
          ali.hotel_detail as hotel_detail,
          ali.hotel_service as hotel_service,
          ali.facilities as facilities,
          ali.hotel_desc as hotel_desc,
          ali.fax as fax ,
          '' as brand_name,
          'ali' as source
          from
          (select id,
                  hotelid as hotel_id,
                  cityid as city_id,
                  cityname as city_name,
                  hotelname as hotel_name,
                  fax,
                  openyear as open_year,
                  fitmentyear as decoration_year,
                  description as hotel_desc,
                  address,
                  telephone as contact,
                  '' as hotel_type,
                  '' as brand_name,
                  '' as hotel_star,
                  latitude,
                  longitude,
                  '' as area_name,
                  '' as hotel_detail,
                  '' as facilities,
                  '' as hotel_service
           from zdp_acc_ctrip.dict_hotelinfo_ali where dt=${hivevar:ali_dt}) ali
                  left join
          (select masterhotelid as master_hotel_id,
                                      chotelname as hotel_name_ctrip,
                                      hotelid as hotel_id from zdp_acc_ctrip.dict_hotelmap
                                      where dt=${hivevar:yesterday} and websiteid = 1200 ) mapinfo
                                      on mapinfo.hotel_id = ali.hotel_id where ali.hotel_id is not null and mapinfo.master_hotel_id is not null
union all
select
          mapinfo.master_hotel_id as master_hotel_id,
          mapinfo.hotel_name_ctrip as hotel_name_ctrip,
          mapinfo.hotel_id as hotel_id,
          meituan.hotel_name as hotel_name,
          meituan.city_name_py as city_name,
          '' as city_name_en,
          meituan.address as address,
          meituan.contact as contact,
          meituan.hotel_type as hotel_type,
          meituan.hotel_star as hotel_star,
          meituan.longitude as longitude,
          meituan.latitude as latitude,
          '' as open_year,
          '' as decoration_year,
          '' as status,
          '' as zone_name,
          meituan.area_name as area_name,
          meituan.hotel_detail as hotel_detail,
          meituan.hotel_service as hotel_service,
          meituan.facilities as facilities,
          '' as hotel_desc,
          '' as fax,
          meituan.brand_name as brand_name,
         'meituan' as source
          from
          (select id,
                  hotelid as hotel_id,
                  cityid as city_id,
                  citynamepy as city_name_py,
                  address,
                  hotelname as hotel_name,
                  telephone as contact,
                  hoteltype as hotel_type,
                  brandname as brand_name,
                  star as hotel_star,
	          case star when '7' then '0'
	          when '' then '0'
         		 else star 
                  end as hotel_star,
                  latitude,
                  longitude,
                  areaname as area_name,
                  hotelmess as hotel_detail,
                  hotelinstallation as facilities,
                  hotelservice as hotel_service
           from zdp_acc_ctrip.dict_hotelinfo_meituan where dt=${hivevar:meituan_dt} ) meituan
                  left join
                  (select masterhotelid as master_hotel_id,
                          chotelname as hotel_name_ctrip,
                         hotelid as hotel_id from zdp_acc_ctrip.dict_hotelmap
                                     where dt=${hivevar:yesterday} and websiteid = 1100 ) mapinfo
                                       on mapinfo.hotel_id = meituan.hotel_id where meituan.hotel_id is not null and mapinfo.master_hotel_id is not null

 union all
select
          mapinfo.master_hotel_id as master_hotel_id,
          mapinfo.hotel_name_ctrip as hotel_name_ctrip,
          mapinfo.hotel_id as hotel_id,
          booking.hotel_name as hotel_name,
          booking.city_name as city_name,
          '' as city_name_en,
          booking.address as address,
          '' as contact,
          booking.hotel_type as hotel_type,
          booking.hotel_star as hotel_star,
          booking.longitude as longitude,
          booking.latitude as latitude,
          '' as open_year,
          '' as decoration_year,
          '' as status,
          '' as zone_name,
          '' as area_name,
          '' as hotel_detail,
          '' as hotel_service,
          booking.facilities as facilities,
          booking.hotel_desc as hotel_desc,
          '' as fax,
          '' as brand_name,
          'booking' as source
          from
          (select    id,
                     hotelid as hotel_id,
                     hotelname as hotel_name,
                     address,
                     cityname as city_name,
                     hoteltype as hotel_type,
		     case star 
                     when '' then 0
                         else star
                     end as hotel_star,
                     longitude,
                     latitude,
                     description as hotel_desc,
                     zoneid as zone_id,
                     policy ,
                     facilities
                     from zdp_acc_ctrip.dict_hotelinfo_booking where dt=${hivevar:booking_dt} ) booking
                  left join
          (select masterhotelid as master_hotel_id,
                                      chotelname as hotel_name_ctrip,
                                      hotelid as hotel_id from zdp_acc_ctrip.dict_hotelmap
                                      where dt=${hivevar:yesterday} and websiteid = 200 ) mapinfo
                                       on mapinfo.hotel_id = booking.hotel_id where booking.hotel_id is not null and mapinfo.master_hotel_id is not null

union all
             select mapinfo.master_hotel_id as master_hotel_id,
                    mapinfo.hotel_name_ctrip as hotel_name_ctrip,
                    mapinfo.hotel_id as hotel_id,
                    elong.hotel_name as hotel_name,
                    elong.city_name as city_name,
                    elong.city_name_en as city_name_en,
                    elong.address as address,
                    elong.contact as contact,
                    elong.hotel_type as hotel_type,
                    elong.star as hotel_star,
                    elong.longitude as longitude,
                    elong.latitude as latitude,
                    elong.open_year as open_year,
                    elong.decoration_year as decoration_year,
                    '' as status ,
                    elong.zone_name as zone_name,
                    '' as area_name,
                    '' as hotel_detail,
                    '' as hotel_service,
                    elong.facilities as facilities,
                    elong.hotel_desc as hotel_desc,
                    '' as fax,
                    '' as brand_name,
                    'elong' as source
                    from (select masterhotelid as master_hotel_id,
                             chotelname as hotel_name_ctrip,
                             hotelid as hotel_id from zdp_acc_ctrip.dict_hotelmap
                             where dt=${hivevar:yesterday} and websiteid = 100 ) mapinfo
                     right join
                     (select id,hotelid as hotel_id,citynamecn as city_name,citynameen as city_name_en,
                              address,hotelname as hotel_name,phone as contact, href,hoteltype as hotel_type,status,openyear as open_year,
                              fityear as decoration_year,
			      case (star as int) as star,
			      longitude,latitude,description as hotel_desc,zonename as zone_name,facilities
                      from zdp_acc_ctrip.dict_hotelinfo_elong where dt=${hivevar:elong_dt} ) elong
                              on mapinfo.hotel_id = elong.hotel_id where elong.hotel_id is not null and mapinfo.master_hotel_id is not null  )merge

