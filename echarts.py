#coding:utf-8
#!/usr/bin/python
import datetime
import numpy as np
import os
import pandas as pd
import pymysql as msql
from pathlib import Path;
import time
import requests
from fun.fun_date_util import yesterday_str

file_pre = '/home/sunwc/romshare/SDK_ECharts/out/'
file_mid = '/data/hive_run_data/'
task_top_dir = '/home/sunwc/romshare/SDK_ECharts/out/tasks/'

def init():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

def duplicates_mac(target_file):
    data = pd.read_table(target_file, names = ['mac'])
    df_total = pd.DataFrame(data)
    df_total.drop_duplicates(subset = ['mac'], keep = 'last', inplace = True)
    df_total.to_csv(target_file, sep = '\n', header = False, index = False)


def del_old_mac():
    global df_source
    df_source = pd.read_table('/home/sunwc/romshare/SDK_ECharts/temp/py_downloadSucess', names = ['mac'])
    #print(df_source)
    df_sql = pd.read_table('/home/sunwc/romshare/SDK_ECharts/temp/query_result.csv', names = ['mac'])
    #print(df_sql)
    df = df_source.append(df_sql)
    df.drop_duplicates(subset = ['mac'], keep = False, inplace = True)
    #print(df)
    df_1000 = pd.read_table('/home/sunwc/romshare/SDK_ECharts/temp/1000.csv', names = ['mac'])
    df_1 = pd.read_table('/home/sunwc/romshare/SDK_ECharts/temp/1.csv', names = ['mac'])
    mac1s = []
    for row in df_1.itertuples():
        mac1s.append(row.mac)
    df_1000 = df_1000[df_1000.mac.isin(mac1s)]
    print(df_1000)


def current_dir():
    print(os.path.abspath(os.path.dirname(__file__)))
    print(os.getcwd())

def date_test():
    time = yesterday_str()
    print(time)
    pass

init()
#del_old_mac()
#current_dir()
date_test()
#drop_test()
#timing_task_test()
#txt_download_start()
#txt_download_install()
