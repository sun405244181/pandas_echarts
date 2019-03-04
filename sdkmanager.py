#coding:utf-8
#!/usr/bin/python
from fun.fun_constant import *
from fun.fun_date_util import yesterday_str
from fun.fun_echarts_sql import mysql_inserted, mysql_query, mysql_execute
import os
import pandas as pd

def init():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    print(yesterday_str()) 

def save(task, filename, mac):
    try:
        target_file = task_top_dir + task + '/' + filename
        os.makedirs(task_top_dir + task, exist_ok = True)
        fo = open(target_file, 'a+')
        fo.write(mac + '\t' + yesterday_str() + '\n')
        fo.close()        
    except Exception as err:
        print('save1')
        print(task)
        print(mac)
        print('save2')
        print(err)
    """
    target_file = task_top_dir + task + '/' + filename
    print('save target_file:' + target_file)
    if (os.path.exists(target_file) == False):
        os.makedirs(task_top_dir + task, exist_ok = True)
        fo = open(task_top_dir + task + '/' + filename, 'a+')
        fo.close()
    data = pd.read_table(target_file, names = ['mac'])
    df_mac = pd.DataFrame(data)

    df_append = pd.DataFrame(df_distinct_mac['mac'])

    df_total = df_mac.append(df_append, ignore_index = True)
    df_total.drop_duplicates(subset = ['mac'], keep = 'last', inplace = True)
    df_total.to_csv(target_file, sep = '\n', header = False, index = False)
    """

def duplicates_mac(target_file):
    data = pd.read_table(target_file, names = ['mac', 'date'])
    df_total = pd.DataFrame(data)
    df_total.drop_duplicates(subset = ['mac'], keep = 'last', inplace = True)
    df_total.to_csv(target_file, sep = '\t', header = False, index = False)

def del_old_mac(df, filename):
    print('del_old_mac start df:' + str(len(df)))
    global df_source
    df_source = pd.DataFrame().append(df)

    df_tvers = df.groupby(['tver']).size().reset_index()
    #print(df_tvers)
    tvers = []
    for row in df_tvers.itertuples():
        tvers.append(row.tver)

    for tver in tvers:
        #print(tver + '------df_source len:' + str(len(df_source)))
        old_macs = []
        target_file = task_top_dir + tver + '/' + filename
        print(target_file)
        if (os.path.exists(target_file)):
            data = pd.read_table(target_file, names = ['mac', 'date'])
            df_mac = pd.DataFrame(data)
            for mac_row in df_mac.itertuples():
                old_macs.append(mac_row.mac)
            df_source = df_source[~((df_source['tver'] == tver) & (df_source.mac.isin(old_macs)))]
        else:
            pass
    print('del_old_mac end df:' + str(len(df_source)))
    return df_source

def txt_upgrade_info():
    tv_upgrade_info()   
    sdk_upgrade_info()


def tv_upgrade_info():
    print('tv_upgrade_info')
    if (mysql_inserted('tv_upgrade_info') > 0):
        return
    data = pd.read_table(filename_pre + yesterday_str() + filename_mid + 'out1.txt', names=['brand', 'rom_ver', 'count'])
    df = pd.DataFrame(data)
    for row in df.itertuples():
        print(row)
        tv_upgrade_info_sql = 'INSERT INTO tv_upgrade_info (brand, rom_ver, count, date) VALUES (\'' +  row.brand + '\',\'' + row.rom_ver + '\',\'' + str(row.count) + '\',\'' + yesterday_str() + '\');'
        mysql_execute(tv_upgrade_info_sql)

def sdk_upgrade_info():
    print('sdk_upgrade_info')
    if (mysql_inserted('sdk_upgrade_info') > 0):
        return
    data = pd.read_table(filename_pre + yesterday_str() + filename_mid + 'out2.txt', names = tv_upgrade_info_columns)
    df = pd.DataFrame(data)
    df.fillna(0, inplace = True)
    df.drop_duplicates(subset = ['app_ver', 'mac'], keep = 'last', inplace = True)
    #20181129 tv_upgrade_info新增两列，'android_id','router_ssid'导致下面问题
    #ValueError: invalid literal for int() with base 10: 'FD5551A-SU'
    #df['app_ver'] = df['app_ver'].astype('int')
    try:
        df['app_ver'] = df['app_ver'].astype('int')
    except Exception as err:
        print('sdk_upgrade_info error')
        print(err)
        return
    groups = df.groupby(['app_ver'])#####板卡rom版本混乱，后续不跟据rom版本分组，日活也不支持根据rom版本查询
    df_from_group = groups.size().reset_index(name = 'count')
    #print(df_from_group)
    for row in df_from_group.itertuples():
        print(row)
        sdk_upgrade_info_sql = 'INSERT INTO sdk_upgrade_info (app_ver, app_count, date) VALUES (\'' + str(row.app_ver) + '\',\'' + str(row.count) + '\',\'' + yesterday_str() + '\');'
        mysql_execute(sdk_upgrade_info_sql)
    ######不跟据rom版本分组
    """
    df_invalid = df[df['rom_ver'].str.contains('_s') == False]
    invalid_count = 0
    for row in df_invalid.itertuples():
        invalid_count += 1
    invalid_rom_ver_sql = 'INSERT INTO sdk_upgrade_info (rom_ver, app_ver, app_count, date) VALUES (\'otherRom\', \'1111111\',\'' + str(invalid_count) + '\',\'' + yesterday_str() + '\');'
    mysql_execute(invalid_rom_ver_sql)
    """

def txt_download_install():
    print('txt_download_install')
    txt_download_start()
    txt_download_sucess()
    txt_install_sucess()

def txt_download_start():
    print('txt_download_start update_date:' + yesterday_str())
    if (mysql_inserted('sdk_download_install') > 0):
       return
    data = pd.read_table(filename_pre + yesterday_str() + filename_mid + 'out3.txt', names = tv_download_apk_start_columns)
    df = pd.DataFrame(data)
    df.fillna(0, inplace = True)

    df.drop_duplicates(subset = ['mac', 'tver'], keep = 'last', inplace = True)
    df_valid = df[df['mac'] != 0]
    df_distinct_mac = del_old_mac(df_valid, 'downloadStart')###
    if (len(df_distinct_mac) == 0):
        return

    groups = df_distinct_mac.groupby(['tver'])
    df_from_group = groups.size().reset_index(name = 'count')
    #print(df_from_group)
    for row in df_from_group.itertuples():
        #print(row)
        txt_download_start_sql = 'INSERT INTO sdk_download_install (app, download_start, date) VALUES (\'' + row.tver + '\',\'' + str(row.count) + '\',\'' + yesterday_str() + '\');'
        mysql_execute(txt_download_start_sql)
    for row in df_distinct_mac.itertuples():
        try:
            save(row.tver, 'downloadStart', row.mac)###
        except Exception as err:
            print('downloadStart tver:' + row.tver)
            print('downloadStart mac:' + row.mac)
            print(err)
            continue
    for row in df_from_group.itertuples():
        #print(row)
        duplicates_mac(task_top_dir + row.tver + '/' + 'downloadStart')###

def txt_download_sucess():
    print('txt_download_sucess')
    data = pd.read_table(filename_pre + yesterday_str() + filename_mid + 'out4.txt', names = tv_download_apk_end_columns)
    df = pd.DataFrame(data)
    df.fillna(0, inplace = True)
    
    df.drop_duplicates(subset = ['mac', 'tver', 'ok'], keep = 'last', inplace = True)
    df_valid = df[(df.ok.isin([1, 1000, 200])) & (df['mac'] != 0)]

    df_distinct_mac = del_old_mac(df_valid, 'downloadSucess')###
    if (len(df_distinct_mac) == 0):
        return
        
    groups = df_distinct_mac.groupby(['tver'])
    df_from_group = groups.size().reset_index(name = 'count')
    #print(df_from_group)
    for row in df_from_group.itertuples():
        #print(row)
        txt_download_sucess_select_sql = 'SELECT * FROM sdk_download_install WHERE app=\'' + row.tver + '\' and date=\'' + yesterday_str() + '\';'
        txt_download_sucess_sql = ''
        if (mysql_query(txt_download_sucess_select_sql)):
            txt_download_sucess_sql = 'UPDATE sdk_download_install set download_sucess=\'' + str(row.count) + '\' where app=\'' + row.tver + '\' and date=\'' + yesterday_str() + '\';'
        else:
            txt_download_sucess_sql = 'INSERT INTO sdk_download_install (app, download_sucess, date) VALUES (\'' + row.tver + '\',\'' + str(row.count) + '\',\'' + yesterday_str() + '\');'
        mysql_execute(txt_download_sucess_sql)            
    for row in df_distinct_mac.itertuples():
        try:
            save(row.tver, 'downloadSucess', row.mac)###
        except Exception as err:
            print('downloadSucess tver:' + row.tver)
            print('downloadSucess mac:' + row.mac)
            print(err)
            continue
    for row in df_from_group.itertuples():
        #print(row)
        duplicates_mac(task_top_dir + row.tver + '/' + 'downloadSucess')###

def txt_install_sucess():
    print('txt_install_sucess')
    data = pd.read_table(filename_pre + yesterday_str() + filename_mid + 'out5.txt', names = tv_download_apk_installed_columns)
    df = pd.DataFrame(data)
    df.fillna(0, inplace = True)
    df.drop_duplicates(subset = ['mac', 'tver', 'ok'], keep = 'last', inplace = True)
    df['ok'] = df['ok'].astype('int')

    sucess_state = [1, -4, -25]
    df_valid = df[(df['ok'].isin(sucess_state)) & (df['mac'] != 0)]
    df_distinct_mac = del_old_mac(df_valid, 'installSucess')###
    if (len(df_distinct_mac) == 0):
        return    

    groups = df_distinct_mac.groupby(['tver'])
    df_from_group = groups.size().reset_index(name = 'count')
    #print(df_from_group)
    for row in df_from_group.itertuples():
        print(row)
        txt_install_sucess_select_sql = 'SELECT * FROM sdk_download_install WHERE app=\'' + row.tver + '\' and date=\'' + yesterday_str() + '\';'
        txt_install_sucess_sql = ''
        if (mysql_query(txt_install_sucess_select_sql)):
            txt_install_sucess_sql = 'UPDATE sdk_download_install set install_sucess=\'' + str(row.count) + '\' where app=\'' + row.tver + '\' and date=\'' + yesterday_str() + '\';'
        else:
            txt_install_sucess_sql = 'INSERT INTO sdk_download_install (app, install_sucess, date) VALUES (\'' + row.tver + '\',\'' + str(row.count) + '\',\'' + yesterday_str() + '\');'     
        mysql_execute(txt_install_sucess_sql)
    for row in df_distinct_mac.itertuples():
        try:
            save(row.tver, 'installSucess', row.mac)###
        except Exception as err:
            print('installSucess tver:' + row.tver)
            print('installSucess mac:' + row.mac)
            print(err)
            continue
    for row in df_from_group.itertuples():
        #print(row)
        duplicates_mac(task_top_dir + row.tver + '/' + 'installSucess')###    

def txt_p2p_error():
    print('txt_p2p_error')
    if (mysql_inserted('sdk_p2p_error') > 0):
        return
    data = pd.read_table(filename_pre + yesterday_str() + filename_mid + 'out3.txt', names = tv_download_apk_start_columns)
    df = pd.DataFrame(data)
    df.fillna(0, inplace = True)
    df.drop_duplicates(subset = ['mac', 'tver', 'ok'], keep = 'last', inplace = True)
    df['app_ver'] = df['app_ver'].astype('int')
    df['ok'] = df['ok'].astype('int')

    #df_valid = df[df['rom_ver'].str.contains('_s')]   

    groups = df.groupby(['app_ver', 'tver', 'ok'])
    df_from_group = groups.size().reset_index(name = 'count')
    #print(df_from_group)
    for row in df_from_group.itertuples():
        print(row)
        txt_p2p_error_sql = 'INSERT INTO sdk_p2p_error (sdk_ver, app, error_code, count, date) VALUES (\'' + str(row.app_ver) + '\',\'' + row.tver + '\',\'' + str(row.ok) + '\',\'' + str(row.count) + '\',\'' + yesterday_str() + '\');'
        mysql_execute(txt_p2p_error_sql)

def txt_install_fail():
    print('txt_install_fail')
    if (mysql_inserted('sdk_install_fail') > 0):
        return    
    data = pd.read_table(filename_pre + yesterday_str() + filename_mid + 'out5.txt', names = tv_download_apk_installed_columns)
    df = pd.DataFrame(data)
    df.fillna(0, inplace = True)
    df.drop_duplicates(subset = ['mac', 'tver', 'ok'], keep = 'last', inplace = True)
    df['app_ver'] = df['app_ver'].astype('int')
    df['ok'] = df['ok'].astype('int')

    df_valid = df[df['rom_ver'].str.contains('_s')]

    groups = df_valid.groupby(['rom_ver', 'app_ver', 'tver', 'ok'])
    df_from_group = groups.size().reset_index(name = 'count')
    #print(df_from_group)
    for row in df_from_group.itertuples():
        print(row)
        sdk_install_fail_sql = 'INSERT INTO sdk_install_fail (rom_ver, sdk_ver, app, error_code, count, date) VALUES (\'' +  row.rom_ver + '\',\'' + str(row.app_ver) + '\',\'' + row.tver + '\',\'' + str(row.ok) + '\',\'' + str(row.count) + '\',\'' + yesterday_str() + '\');'
        mysql_execute(sdk_install_fail_sql)
         

init()
txt_upgrade_info()
txt_download_install()
txt_p2p_error()
txt_install_fail()



