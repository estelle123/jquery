#! /bin/bash
set -x 
source ~/.bash_profile

yesterday=`date -d "yesterday" +%Y%m%d`
echo "hivevar.yesterday=`date -d "yesterday" +%Y%m%d`" >> merge.job
hive -e "show partitions zdp_acc_ctrip.dict_hotelinfo_elong" > elong_partitions.dat
tail -1 elong_partitions.dat | awk -F'=' '{print $2}' > elong_newest_dt.dat
echo "hivevar.elong_dt=`cat elong_newest_dt.dat`" >> merge.job
hive -e "show partitions zdp_acc_ctrip.dict_hotelinfo_ali " > ali_partitions.dat
tail -1 ali_partitions.dat | awk -F'=' '{print $2}' > ali_newest_dt.dat
echo "hivevar.ali_dt=`cat ali_newest_dt.dat`" >> merge.job
hive -e "show partitions zdp_acc_ctrip.dict_hotelinfo_meituan " > meituan_partitions.dat
tail -1 meituan_partitions.dat | awk -F'=' '{print $2}' > meituan_newest_dt.dat
echo "hivevar.meituan_dt=`cat meituan_newest_dt.dat`" >> merge.job
hive -e "show partitions zdp_acc_ctrip.dict_hotelinfo_booking" > booking_partitions.dat
tail -1 booking_partitions.dat | awk -F'=' '{print $2}' > booking_newest_dt.dat
echo "hivevar.booking_dt=`cat booking_newest_dt.dat`" >> merge.job
hive -e "show partitions zdp_acc_ctrip.dict_hotelinfo_qunar " > qunar_partitions.dat
tail -1 qunar_partitions.dat | awk -F'=' '{print $2}' > qunar_newest_dt.dat
echo "hivevar.qunar_dt=`cat qunar_newest_dt.dat`" >> merge.job


hadoop fs -mkdir /wh/entity/ctrip/dict/dict_third_part_hotel_info/$yesterday/

hive -e " 
use zdp_etl_ctrip;
alter table dict_third_part_hotel_info ADD IF NOT EXISTS PARTITION  (dt=$yesterday) location '/wh/format/ctrip/dict/dict_third_part_hotel_info/$yesterday/' ;
"


