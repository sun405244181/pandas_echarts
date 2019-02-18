#coding:utf-8
#!/usr/bin/python
import os

fun_sign = 'ecd3fed47446c971'

current_dir = os.path.dirname(__file__)
echarts_base_dir = os.path.abspath(os.path.dirname(current_dir))

filename_pre = echarts_base_dir + '/out/'
filename_mid = '/data/hive_run_data/'
task_top_dir = echarts_base_dir + '/out/tasks/'

tv_telecontrol_columns = ['rom_ver', 'app_type', 'mac', 'app_ver', 'h_mode', 'appmode', 'rc_type', 'ctype', 'voice_type', 'bt_pair_ui', 'bt_pair_status', 'bt_start_pk', 'bt_callback']
helper_cast_screen_columns = ["stm","sip","sid","app_type","ctime","app_ver","order","h_mode","h_sn","net_type","rom_ver","appmode","mac","opera_way","opera_time","opera_id","screen_type","protocal_id","status","err","year","mounth","day","hour"]
rom_recovery_columns = ['rom_ver', 'app_type', 'app_ver', 'h_mode', 'mac', 'fdisk', 'chip_type', 'recovery_info', 'emmc']
#20181129之前
#tv_upgrade_info_columns = ['stm',"sip","rom_ver","sid","app_type","ctime","app_ver","order","h_mode","h_sn","net_type","appmode","mac","mem","tdisk","fdisk","tver","ok","utime",'lng','lat',"year","mounth","day","hour"]
#20181129及以后
tv_upgrade_info_columns = ['stm',"sip","rom_ver","sid","app_type","ctime","app_ver","order","h_mode","h_sn","net_type","appmode","mac","mem","tdisk","fdisk","tver","ok","utime",'lng','lat','android_id','router_ssid',"year","mounth","day","hour"]
tv_download_apk_start_columns = ["stm","sip","rom_ver","sid","app_type","ctime","app_ver","order","h_mode","h_sn","net_type","appmode","mac","mem","tdisk","fdisk","rt","tver","ok","dtime","year","mounth","day","hour"]
tv_download_apk_end_columns = ["stm","sip","rom_ver","sid","app_type","ctime","app_ver","order","h_mode","h_sn","net_type","appmode","mac","mem","tdisk","fdisk","tver","ok",'d_addr','p_size',"dtime",'p_type',"year","mounth","day","hour"]
tv_download_apk_installed_columns = ["stm","sip","rom_ver","sid","app_type","ctime","app_ver","order","h_mode","h_sn","net_type","appmode","mac","mem","tdisk","fdisk",'rtime',"tver",'tsize','utype',"ok",'i_type','itime','p_type',"year","mounth","day","hour"]

