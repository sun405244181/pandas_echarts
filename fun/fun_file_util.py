# coding:utf-8
#!/usr/bin/python

from fun_constant import *
from fun_date_util import yesterday_str
import os
import pandas as pd
import time


def fun_format_file(path):
    files = os.listdir(path)
    for i in range(0, len(files)):
        sourcefile = os.path.join(path, files[i])
        if (os.path.isfile(sourcefile)):
            print(sourcefile)
            insert_column(sourcefile)
            # print_df(sourcefile)
            '''
            print(sourcefile)
            content = ''
            rfo = open(sourcefile, "r")
            for line in rfo.readlines():
                line = line.strip() + ',20190124'
                content = content + line + '\n'
            rfo.close()    
            wfo = open(sourcefile + '_bak', 'a+')
            wfo.write(content)
            wfo.close()
            os.remove(sourcefile)
            time.sleep(3)
            os.rename(sourcefile + '_bak', sourcefile)
            '''
        else:
            fun_format_file(sourcefile)


def insert_column(path):
    try:
        data = pd.read_table(path, names=['mac'])
        df = pd.DataFrame(data)
        df = df.reindex(columns=['mac', 'date'], fill_value='20190124')
        df.to_csv(path, sep='\t', header=False, index=False)
    except:
        print('column already exist')


def update_tasks_file_by_date(path, date):
    files = os.listdir(path)
    for i in range(0, len(files)):
        sourcefile = os.path.join(path, files[i])
        if (os.path.isfile(sourcefile)):
            print(sourcefile)
            del_mac_by_date(sourcefile, date)
            pass
        else:
            update_tasks_file_by_date(sourcefile, date)


def del_mac_by_date(target_file, date):
    data = pd.read_table(target_file, names=['mac', 'date'])
    df_total = pd.DataFrame(data)
    df_valid = df_total[df_total['date'] <= date]
    df_valid.to_csv(target_file, sep='\t', header=False, index=False)


if __name__ == '__main__':
    pass
    # fun_format_file('/home/sunwc/romshare/SDK_ECharts/out/tasks')
    # update_tasks_file_by_date(
    #    '/home/sunwc/romshare/SDK_ECharts/out/debug_tasks', 20190301)
