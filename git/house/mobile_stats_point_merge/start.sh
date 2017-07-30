
#! /bin/bash
source ~/.bash_profile
echo "hivevar.yesterday=`date -d "yesterday" +%Y%m%d`" >> mobile_stats_pointmerge_mobile.job


hive -e "show partitions app.trace_id" > mobile_trace_id.dat
hive -e "show partitions app.pv" > mobile_pv.dat
hive -e "show partitions app.hotel_search_result_show" > mobile_result_show.dat
hive -e "show partitions app.hotel_oversea_list_click" > mobile_oversea_list_click.dat
hive -e "show partitions app.hotel_oversea_list_basic" > mobile_oversea_list_basic.dat
hive -e "show partitions app.hotel_orderresult_trace" > mobile_order_result_trace.dat
hive -e "show partitions app.hotel_inquire_trace" > mobile_inquire_trace.dat
hive -e "show partitions app.hotel_inland_order_trace" > mobile_inland_order_trace.dat
hive -e "show partitions app.hotel_inland_list_click" > mobile_inland_list_click.dat
hive -e "show partitions app.hotel_inland_detail_basic" > mobile_inland_detail_basic.dat
hive -e "show partitions app.hotel_inland_list_basic" > mobile_inland_list_basic.dat
hive -e "show partitions app.hotel_inland_booking_basic" >mobile_inland_booking_basic.dat
hive -e "show partitions app.hotel_search_result_show_servelog" > mobile_search_result_show_servelog.dat


mobile_trace_id_dt=` tail -1 mobile_trace_id.dat | awk -F'=' '{print $2}' `
mobile_pv_dt=` tail -1 mobile_pv.dat  | awk -F'=' '{print $2}' `
mobile_result_show_dt=` tail -1 mobile_result_show.dat | awk -F'=' '{print $2}' `
mobile_oversea_list_click_dt=` tail -1 mobile_oversea_list_click.dat | awk -F'=' '{print $2}' `
mobile_oversea_list_basic_dt=` tail -1 mobile_oversea_list_basic.dat  | awk -F'=' '{print $2}' `
mobile_order_result_trace_dt=` tail -1 mobile_order_result_trace.dat | awk -F'=' '{print $2}' `
mobile_inquire_trace_dt=` tail -1 mobile_inquire_trace.dat | awk -F'=' '{print $2}' `
mobile_inland_order_trace_dt=` tail -1 mobile_inland_order_trace.dat  | awk -F'=' '{print $2}' `
mobile_inland_list_click_dt=` tail -1 mobile_inland_list_click.dat | awk -F'=' '{print $2}' `
mobile_inland_detail_basic_dt=` tail -1 mobile_inland_detail_basic.dat | awk -F'=' '{print $2}' `
mobile_inland_list_basic_dt=` tail -1 mobile_inland_list_basic.dat  | awk -F'=' '{print $2}' `
mobile_inland_booking_basic_dt=` tail -1 mobile_inland_booking_basic.dat | awk -F'=' '{print $2}' `
mobile_search_result_show_servelog_dt=` tail -1 mobile_search_result_show_servelogservelog.dat | awk -F'=' '{print $2}'`

echo "hivevar.mobile_trace_id_dt=$mobile_trace_id_dt" >> mobile_stats_pointmerge_mobile.job
echo "hivevar.mobile_pv_dt=$mobile_pv_dt ">> mobile_stats_pointmerge_mobile.job
echo "hivevar.mobile_result_show_dt=$mobile_result_show_dt" >> mobile_stats_pointmerge_mobile.job
echo "hivevar.mobile_oversea_list_click_dt=$mobile_oversea_list_click_dt" >> mobile_stats_pointmerge_mobile.job
echo "hivevar.mobile_oversea_list_basic_dt=$mobile_oversea_list_basic_dt ">> mobile_stats_pointmerge_mobile.job
echo "hivevar.mobile_order_result_trace_dt=$mobile_order_result_trace_dt" >> mobile_stats_pointmerge_mobile.job
echo "hivevar.mobile_inland_order_trace_dt=$mobile_inland_order_trace_dt" >> mobile_stats_pointmerge_mobile.job
echo "hivevar.mobile_inland_list_click_dt=$mobile_inland_list_click_dt">> mobile_stats_pointmerge_mobile.job
echo "hivevar.mobile_inland_detail_basic_dt=$mobile_inland_detail_basic_dt">> mobile_stats_pointmerge_mobile.job
echo "hivevar.mobile_inland_list_basic_dt=$mobile_inland_list_basic_dt">> mobile_stats_pointmerge_mobile.job
echo "hivevar.mobile_inland_booking_basic_dt=$mobile_inland_booking_basic_dt">> mobile_stats_pointmerge_mobile.job
echo "hivevar.mobile_search_result_show_servelog_dt=$mobile_search_result_show_servelog_dt">> mobile_stats_pointmerge_mobile.job
