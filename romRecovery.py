#coding:utf-8
#!/usr/bin/python
from fun.fun_constant import *
from fun.fun_date_util import yesterday_str
from fun.fun_echarts_sql import mysql_inserted, mysql_execute
import os
import pandas as pd

def init():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    print(yesterday_str())   

def rom_recovery():
    try:
        print('rom_recovery_mac')
        if (mysql_inserted('rom_recovery') > 0):
            return
        data = pd.read_table(filename_pre + yesterday_str() + filename_mid + 'out8.txt', names = rom_recovery_columns)
        df = pd.DataFrame(data)
        df.fillna(0, inplace = True)

        ######不去重mac
        groups_macs = df.groupby(['rom_ver', 'h_mode', 'recovery_info'])
        df_from_group_macs = groups_macs.size().reset_index(name = 'count')
        for row in df_from_group_macs.itertuples():
            print(row)
            rom_recovery_macs = 'INSERT INTO rom_recovery (rom_ver, h_mode, recovery_info, count, date, distinct_mac) VALUES (\'' + row.rom_ver + '\',\'' + row.h_mode + '\',\'' + str(row.recovery_info) + '\',\'' + str(row.count) + '\',\'' + yesterday_str() + '\',\'' + str(0) + '\');'
            mysql_execute(rom_recovery_macs)

        ######去重mac
        df.drop_duplicates(subset = ['mac', 'recovery_info'], keep = 'last', inplace = True)
        groups_mac = df.groupby(['rom_ver', 'h_mode', 'recovery_info'])
        df_from_group_mac = groups_mac.size().reset_index(name = 'count')
        for row in df_from_group_mac.itertuples():
            print(row)
            rom_recovery_mac = 'INSERT INTO rom_recovery (rom_ver, h_mode, recovery_info, count, date, distinct_mac) VALUES (\'' + row.rom_ver + '\',\'' + row.h_mode + '\',\'' + str(row.recovery_info) + '\',\'' + str(row.count) + '\',\'' + yesterday_str() + '\',\'' + str(1) + '\');'
            mysql_execute(rom_recovery_mac)
    except Exception as err:
        print('rom_recovery error', err)
        pass

def import_test():
    print('from romRecovery...')      

init()
rom_recovery()