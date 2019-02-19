# coding:utf-8
#!/usr/bin/python
from fun.fun_constant import *
from fun.fun_date_util import yesterday_str
from fun.fun_echarts_sql import mysql_inserted, mysql_query, mysql_execute
import os
import pandas as pd

'''
select ok, count(distinct mac) from tv_upgrade_info where year='2019' and month='02' and day in ('16', '17', '18') and app_type='1010' and app_ver > '4030101' group by ok;
0   -1      57491
1   9999    815254
2   200     814877

select ok, count(distinct mac) from tv_upgrade_info where year='2019' and month='02' and day in ('15', '16', '17', '18') and app_type='1010' and app_ver > '4030101' group by ok;
0   200	    846088
2   9999    845882
1   -1      71907
'''


def init():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    print(yesterday_str())


'''
def tv_upgrade_info():
    print('debug_process')
    if (mysql_inserted('debug_process') > 0):
        return
    data = pd.read_table(filename_pre + yesterday_str() +
                         filename_mid + 'out1.txt', names=['brand', 'rom_ver', 'count'])
    df = pd.DataFrame(data)
    for row in df.itertuples():
        print(row)
        tv_upgrade_info_sql = 'INSERT INTO tv_upgrade_info (brand, rom_ver, count, date) VALUES (\'' + \
            row.brand + '\',\'' + row.rom_ver + '\',\'' + \
            str(row.count) + '\',\'' + yesterday_str() + '\');'
        mysql_execute(tv_upgrade_info_sql)
'''


# init()
# tv_upgrade_info()
