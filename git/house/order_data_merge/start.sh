#! /bin/bash
source ~/.bash_profile
yesterday=`date -d "1 days ago" +%Y-%m-%d`
date_before_yesterday=`date -d "2 days ago" +%Y-%m-%d`

yesterday_date_timstamp=`date -d ${yesterday} +%s`
date_before_yesterday_timestamp=`date -d ${date_before_yesterday} +%s`

echo "hivevar.yesterday_date_timstamp=$yesterday_date_timstamp" >> order_data_merge.job
echo "hivevar.date_before_yesterday_timestamp=$date_before_yesterday_timestamp" >> order_data_merge.job
echo "hivevar.yesterday=`date -d "yesterday" +%Y%m%d`" >> order_data_merge.job

hive -e "show partitions zdp_acc_ctrip.biz_order_water_no_info " > rapid_checkin.dat
hive -e "show partitions zdp_acc_ctrip.biz_hotelorder " > hotel_order.dat
hive -e "show partitions zdp_acc_ctrip.biz_hotelorder_pms " > pms_order.dat
hive -e "show partitions zdp_acc_ctrip.biz_mapping_order_client" > mapping_order.dat
hive -e "show partitions zdp_acc_ctrip.biz_order_switch" > switch_order.dat  
hive -e "show partitions zdp_acc_ctrip.dict_roominfo" > room_info.dat
hive -e "show partitions zdp_acc_ctrip.dict_hotelinfo" > hotel_info.dat 


rapid_checkin_dt=` tail -1 rapid_checkin.dat | awk -F'=' '{print $2}' `
hotel_order_dt=` tail -1  hotel_order.dat | awk -F'=' '{print $2}' `
pms_order_dt=` tail -1  pms_order.dat | awk -F'=' '{print $2}' `
mapping_order_dt=` tail -1 mapping_order.dat | awk -F'=' '{print $2}' `
switch_order_dt=` tail -1  switch_order.dat | awk -F'=' '{print $2}' `
room_info_dt =` tail -1  room_info.dat | awk -F'=' '{print $2}' `
hotel_info_dt=` tail -1  hotel_info.dat | awk -F'=' '{print $2}' `




echo "hivevar.rapid_checkin_dt=$rapid_checkin_dt" >> order_data_merge.job
echo "hivevar.hotel_order_dt=$hotel_order_dt ">> order_data_merge.job
echo "hivevar.pms_order_dt=$pms_order_dt" >> order_data_merge.job
echo "hivevar.mapping_order_dt=$mapping_order_dt" >> order_data_merge.job
echo "hivevar.switch_order_dt=$switch_order_dt" >> order_data_merge.job
echo "hivevar.room_info_dt=$room_info_dt" >> order_data_merge.job
echo "hivevar.hotel_info_dt=$hotel_info_dt" >> order_data_merge.job

