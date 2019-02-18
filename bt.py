#coding:utf-8
#!/usr/bin/python
from fun.fun_constant import *
from fun.fun_date_util import yesterday_str
from fun.fun_echarts_sql import mysql_inserted, mysql_execute
import os
import pandas as pd

'''
1. 蓝牙版本分布 （按平台，按厂商，按遥控器）
  饼图展示版本
2. 预测蓝牙失联情况 （按平台，按厂商， 按遥控器）   
  二维线性图，mac去重
   横轴每天，
   纵轴有4条线： 蓝牙版本(1: 蓝牙)， 蓝牙配对界面弹出，用户主动配对蓝牙(配对成功：1 、配对失败：0)， 失联回掉
3. 蓝牙失败 （按平台，按厂商， 按遥控器）
   二维线性图， （分mac 去重 ， 和 mac不去重两种情况）
   横轴每天：
   纵轴有2条线： 蓝牙配对总数(配对成功：1 、配对失败：0)，蓝牙配对失败
'''

def init():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    print(yesterday_str()) 

def bt_rom_ver():
    print('bt_rom_ver')
    if (mysql_inserted('bt_rom_ver') > 0):
        return
    data = pd.read_table(filename_pre + yesterday_str() + filename_mid + 'out9.txt', names = tv_telecontrol_columns)
    df = pd.DataFrame(data)
    df.fillna(0, inplace = True)
    df['rc_type'] = df['rc_type'].astype('int')
    df['ctype'] = df['ctype'].astype('int')
    df = df[(df['rc_type'] == 1)]

    df.drop_duplicates(subset = ['mac', 'h_mode', 'ctype'], keep = 'last', inplace = True)

    groups = df.groupby(['rom_ver', 'h_mode', 'ctype'])
    df_from_group = groups.size().reset_index(name = 'count')
    for row in df_from_group.itertuples():
        print(row)
        bt_rom_ver_sql = 'INSERT INTO bt_rom_ver (rom_ver, ctype, h_mode, count, date) VALUES (\'' + row.rom_ver + '\',\'' + str(row.ctype) + '\',\'' + row.h_mode + '\',\'' + str(row.count) + '\',\'' + yesterday_str() + '\');'
        mysql_execute(bt_rom_ver_sql)

def bt_pair_status():
    print('bt_pair_status')
    if (mysql_inserted('bt_pair_status') > 0):
        return
    data = pd.read_table(filename_pre + yesterday_str() + filename_mid + 'out9.txt', names = tv_telecontrol_columns)
    df = pd.DataFrame(data)
    df.fillna(-1, inplace = True)
    df['rc_type'] = df['rc_type'].astype('int')
    df['ctype'] = df['ctype'].astype('int')
    df['bt_pair_ui'] = df['bt_pair_ui'].astype('int')
    df['bt_pair_status'] = df['bt_pair_status'].astype('int')
    df['bt_start_pk'] = df['bt_start_pk'].astype('int')
    df['bt_callback'] = df['bt_callback'].astype('int')
    df = df[(df['rc_type'] == 1)]

    groups_macs = df.groupby(['rom_ver', 'h_mode', 'ctype', 'bt_pair_ui', 'bt_pair_status', 'bt_start_pk', 'bt_callback'])
    df_from_group_macs = groups_macs.size().reset_index(name = 'count')
    for row in df_from_group_macs.itertuples():
        print(row)
        bt_rom_ver_sql = 'INSERT INTO bt_pair_status (rom_ver, ctype, h_mode, bt_pair_ui, bt_pair_status, bt_start_pk, bt_callback, count, date, distinct_mac) VALUES (\'' + row.rom_ver + '\',\'' + str(row.ctype) + '\',\'' + row.h_mode + '\',\'' + str(row.bt_pair_ui) + '\',\'' + str(row.bt_pair_status) + '\',\'' + str(row.bt_start_pk) + '\',\'' + str(row.bt_callback) + '\',\'' + str(row.count) + '\',\'' + yesterday_str() + '\',\'' + str(0) + '\');'
        mysql_execute(bt_rom_ver_sql)

    df.drop_duplicates(subset = ['mac', 'h_mode', 'ctype', 'bt_pair_ui', 'bt_pair_status', 'bt_start_pk', 'bt_callback'], keep = 'last', inplace = True)
    groups_mac = df.groupby(['rom_ver', 'h_mode', 'ctype', 'bt_pair_ui', 'bt_pair_status', 'bt_start_pk', 'bt_callback'])
    df_from_group_mac = groups_mac.size().reset_index(name = 'count')
    for row in df_from_group_mac.itertuples():
        print(row)
        bt_rom_ver_sql = 'INSERT INTO bt_pair_status (rom_ver, ctype, h_mode, bt_pair_ui, bt_pair_status, bt_start_pk, bt_callback, count, date, distinct_mac) VALUES (\'' + row.rom_ver + '\',\'' + str(row.ctype) + '\',\'' + row.h_mode + '\',\'' + str(row.bt_pair_ui) + '\',\'' + str(row.bt_pair_status) + '\',\'' + str(row.bt_start_pk) + '\',\'' + str(row.bt_callback) + '\',\'' + str(row.count) + '\',\'' + yesterday_str() + '\',\'' + str(1) + '\');'
        mysql_execute(bt_rom_ver_sql)

init()
bt_rom_ver()
bt_pair_status()     