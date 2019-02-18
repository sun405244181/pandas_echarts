#coding:utf-8
#!/usr/bin/python3
from fun.fun_constant import *
from fun.fun_date_util import yesterday_str
from fun.fun_echarts_sql import mysql_inserted, mysql_execute, mysql_group_count
import os
import pandas as pd

def init():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    print(yesterday_str())

def txt_helper_cast_screen():
    print('txt_helper_cast_screen update_date:' + yesterday_str())
    if (mysql_inserted('cast_screen') > 0):
        return
    data = pd.read_table(filename_pre + yesterday_str() + filename_mid + 'out7.txt', names = helper_cast_screen_columns)
    df = pd.DataFrame(data)
    df.fillna(0, inplace = True)
    df['app_type'] = df['app_type'].astype('int')
    df = df[df['app_type'] == 5]
    df.drop_duplicates(subset = ['mac', 'protocal_id'], keep = 'last', inplace = True)

    groups = df.groupby(['h_mode', 'app_ver', 'protocal_id'])
    df_from_group = groups.size().reset_index(name = 'count')
    #print(df_from_group)
    for row in df_from_group.itertuples():
        print(row)
        if (row.protocal_id not in ('AIRPLAY', 'DLNA', 'MIRACAST')):
            continue
        helper_cast_screen_sql = 'INSERT INTO cast_screen (h_mode, app_ver, protocal_id, count, date) VALUES (\'' + row.h_mode + '\',\'' + row.app_ver + '\',\'' + row.protocal_id + '\',\'' + str(row.count) + '\',\'' + yesterday_str() + '\');'
        mysql_execute(helper_cast_screen_sql)

def txt_helper_cast_screen_status():
    print('txt_helper_cast_screen_status update_date:' + yesterday_str())
    if (mysql_inserted('cast_screen_status') > 0):
        return
    data = pd.read_table(filename_pre + yesterday_str() + filename_mid + 'out7.txt', names = helper_cast_screen_columns)
    df = pd.DataFrame(data)
    df.fillna('0', inplace = True)
    df['app_type'] = df['app_type'].astype('int')
    df = df[df['app_type'] == 5]
    df_valid = df[df['protocal_id'].isin(['DLNA', 'AIRPLAY', 'MIRACAST'])]
    groups = df_valid.groupby(['h_mode', 'rom_ver', 'app_ver', 'protocal_id', 'status'])
    df_from_group = groups.size().reset_index(name = 'count')
    for row in df_from_group.itertuples():
        print(row)
        cast_screen_status_sql = 'INSERT INTO cast_screen_status (h_mode, rom_ver, app_ver, protocal_id, status ,count , date) VALUES (\'' + row.h_mode + '\',\'' + row.rom_ver  + '\',\'' + row.app_ver + '\',\'' + row.protocal_id + '\',\'' + str(row.status) + '\',\'' + str(row.count) + '\',\'' + yesterday_str() + '\');'
        mysql_execute(cast_screen_status_sql)
    pass

def txt_helper_cast_screen_err():
    print('txt_helper_cast_screen_err update_date:' + yesterday_str())
    if (mysql_inserted('cast_screen_err') > 0):
        return
    data = pd.read_table(filename_pre + yesterday_str() + filename_mid + 'out7.txt', names = helper_cast_screen_columns)
    df = pd.DataFrame(data)
    df.fillna('0', inplace = True)
    df_valid = df[(df['protocal_id'].isin(['DLNA', 'AIRPLAY', 'MIRACAST'])) & (df['status'] == 2)]
    groups = df_valid.groupby(['h_mode', 'rom_ver', 'app_ver', 'protocal_id', 'err'])
    df_from_group = groups.size().reset_index(name = 'count')
    for row in df_from_group.itertuples():
        print(row)
        if ((row.protocal_id == 'DLNA') & (row.err.find(':') > 0)):
            IndexErr = row.err.find(':')
            subErr = row.err[0 : IndexErr]
            cast_screen_status_select_sql = 'SELECT count FROM cast_screen_err WHERE h_mode=\'' + row.h_mode + '\' and rom_ver=\'' + row.rom_ver + '\' and app_ver=\'' + row.app_ver + '\' and protocal_id=\'' + row.protocal_id + '\' and err=\'' + subErr + '\' and date=\'' + yesterday_str() + '\';'
            cast_screen_status_sub_sql = ''
            rs = mysql_group_count(cast_screen_status_select_sql)
            if (rs[0] > 0):
                cast_screen_status_sub_sql = 'UPDATE cast_screen_err set count=\'' + str(row.count + rs[1]) + '\' where h_mode=\'' + row.h_mode + '\' and rom_ver=\'' + row.rom_ver + '\' and app_ver=\'' + row.app_ver + '\' and protocal_id=\'' + row.protocal_id + '\' and err=\'' + subErr + '\' and date=\'' + yesterday_str() + '\';'
                pass
            else:
                cast_screen_status_sub_sql = 'INSERT INTO cast_screen_err (h_mode, rom_ver, app_ver, protocal_id, err ,count , date) VALUES (\'' + row.h_mode + '\',\'' + row.rom_ver + '\',\'' + row.app_ver + '\',\'' + row.protocal_id + '\',\'' + subErr + '\',\'' + str(row.count) + '\',\'' + yesterday_str() + '\');'
            mysql_execute(cast_screen_status_sub_sql)    
            continue
        cast_screen_status_sql = 'INSERT INTO cast_screen_err (h_mode, rom_ver, app_ver, protocal_id, err ,count , date) VALUES (\'' + row.h_mode + '\',\'' + row.rom_ver + '\',\'' + row.app_ver + '\',\'' + row.protocal_id + '\',\'' + row.err + '\',\'' + str(row.count) + '\',\'' + yesterday_str() + '\');'
        mysql_execute(cast_screen_status_sql)     

init()
txt_helper_cast_screen()
txt_helper_cast_screen_status()
txt_helper_cast_screen_err()