# coding:utf-8
#!/usr/bin/python
from fun.fun_constant import *
from fun.fun_date_util import yesterday_str
from fun.fun_echarts_sql import mysql_inserted, mysql_query, mysql_execute, mysql_create_table
import os
import pandas as pd

debug_task_top_dir = echarts_base_dir + '/out/debug_tasks/'
debug_p2p_download_top_dir = echarts_base_dir + '/out/debug_p2p_download/'

global save_macs


def init():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    print(yesterday_str())


def save(task, filename, fcontent):
    try:
        target_file = debug_task_top_dir + task + '/' + filename
        os.makedirs(debug_task_top_dir + task, exist_ok=True)
        fo = open(target_file, mode='a+', buffering=409600)
        fo.write(fcontent)
        fo.flush()
        fo.close()
    except Exception as err:
        print('save1')
        print(task)
        print('save2')
        print(err)


def save_batch(mac_dict, filename):
    if (len(mac_dict) == 0):
        return
    keys = mac_dict.keys()
    for key in keys:
        save(key, filename, mac_dict[key])


def duplicates_mac(target_file):
    data = pd.read_table(target_file, names=['mac', 'date'])
    df_total = pd.DataFrame(data)
    df_total.drop_duplicates(subset=['mac'], keep='last', inplace=True)
    df_total.to_csv(target_file, sep='\t', header=False, index=False)


def del_old_mac(df, filename):
    print('del_old_mac start df:' + str(len(df)))
    global df_source
    df_source = pd.DataFrame().append(df)

    df_tvers = df.groupby(['tver']).size().reset_index()
    # print(df_tvers)
    tvers = []
    for row in df_tvers.itertuples():
        tvers.append(row.tver)

    for tver in tvers:
        #print(tver + '------df_source len:' + str(len(df_source)))
        old_macs = []
        target_file = debug_task_top_dir + tver + '/' + filename
        print(target_file)
        if (os.path.exists(target_file)):
            data = pd.read_table(target_file, names=['mac', 'date'])
            df_mac = pd.DataFrame(data)
            for mac_row in df_mac.itertuples():
                old_macs.append(mac_row.mac)
            df_source = df_source[~((df_source['tver'] == tver) & (
                df_source.mac.isin(old_macs)))]
        else:
            pass
    print('del_old_mac end df:' + str(len(df_source)))
    return df_source


def txt_download_install():
    mysql_create_table('debug_sdk_download_install',
                       debug_sdk_download_install_create_sql)
    print('txt_download_install')
    txt_download_start()
    txt_add_task_sucess()
    txt_download_sucess()
    txt_install_sucess()


def txt_download_start():
    print('txt_download_start update_date:' + yesterday_str())
    if (mysql_inserted('debug_sdk_download_install') > 0):
        return
    data = pd.read_table(filename_pre + yesterday_str() +
                         filename_mid + 'out3.txt', names=tv_download_apk_start_columns)
    df = pd.DataFrame(data)
    df.fillna(0, inplace=True)

    df.drop_duplicates(subset=['mac', 'tver'], keep='last', inplace=True)
    df_valid = df[df['mac'] != 0]
    df_distinct_mac = del_old_mac(df_valid, 'downloadStart')
    if (len(df_distinct_mac) == 0):
        return

    groups = df_distinct_mac.groupby(['h_mode', 'app_ver', 'tver'])
    df_from_group = groups.size().reset_index(name='count')
    # print(df_from_group)
    for row in df_from_group.itertuples():
        # print(row)
        txt_download_start_sql = 'INSERT INTO debug_sdk_download_install (h_mode, sdk_ver, app, download_start, date) VALUES (\'' + row.h_mode + '\', \'' + str(row.app_ver) + '\', \'' + row.tver + '\', \'' + \
            str(row.count) + '\', \'' + yesterday_str() + '\');'
        mysql_execute(txt_download_start_sql)
    download_start_save = 0
    save_macs = {}
    for row in df_distinct_mac.itertuples():
        try:
            download_start_save += 1
            current_line = (row.mac + '\t' + yesterday_str() + '\n')
            if (row.tver in save_macs):
                save_macs[row.tver] = (save_macs[row.tver] + current_line)
            else:
                save_macs[row.tver] = current_line
            if (download_start_save % 10000 == 0):
                save_batch(save_macs, 'downloadStart')
                print('save downloadStart index:' + str(download_start_save))
                save_macs.clear()
        except Exception as err:
            print('downloadStart tver:' + row.tver)
            print('downloadStart mac:' + row.mac)
            print(err)
            continue
    save_batch(save_macs, 'downloadStart')
    save_macs.clear()
    for row in df_from_group.itertuples():
        # print(row)
        duplicates_mac(debug_task_top_dir + row.tver + '/' + 'downloadStart')


def txt_add_task_sucess():
    print('txt_add_task_sucess update_date:' + yesterday_str())
    data = pd.read_table(filename_pre + yesterday_str() +
                         filename_mid + 'out3.txt', names=tv_download_apk_start_columns)
    df = pd.DataFrame(data)
    df.fillna(0, inplace=True)

    df.drop_duplicates(subset=['mac', 'tver'], keep='last', inplace=True)
    df_valid = df[(df.ok.isin([0, 1, 100])) & (df['mac'] != 0)]
    df_distinct_mac = del_old_mac(df_valid, 'addTaskSucess')
    if (len(df_distinct_mac) == 0):
        return

    groups = df_distinct_mac.groupby(['h_mode', 'app_ver', 'tver'])
    df_from_group = groups.size().reset_index(name='count')
    for row in df_from_group.itertuples():
        # print(row)
        exist_select_sql = 'SELECT * FROM debug_sdk_download_install WHERE h_mode=\'' + row.h_mode + '\' and sdk_ver=\'' + \
            str(row.app_ver) + '\' and app=\'' + row.tver + \
            '\' and date=\'' + yesterday_str() + '\';'
        add_task_secuss_sql = ''
        if (mysql_query(exist_select_sql)):
            add_task_secuss_sql = 'UPDATE debug_sdk_download_install set add_task_secuss=\'' + \
                str(row.count) + '\' where h_mode=\'' + row.h_mode + '\' and sdk_ver=\'' + \
                str(row.app_ver) + '\' and app=\'' + row.tver + \
                '\' and date=\'' + yesterday_str() + '\';'
        else:
            add_task_secuss_sql = 'INSERT INTO debug_sdk_download_install (h_mode, sdk_ver, app, add_task_secuss, date) VALUES (\'' + row.h_mode + '\',\'' + str(row.app_ver) + '\',\'' + row.tver + '\',\'' + \
                str(row.count) + '\',\'' + yesterday_str() + '\');'
        mysql_execute(add_task_secuss_sql)
    add_task_sucess_save = 0
    save_macs = {}
    for row in df_distinct_mac.itertuples():
        try:
            add_task_sucess_save += 1
            current_line = (row.mac + '\t' + yesterday_str() + '\n')
            if (row.tver in save_macs):
                save_macs[row.tver] = (save_macs[row.tver] + current_line)
            else:
                save_macs[row.tver] = current_line
            if (add_task_sucess_save % 10000 == 0):
                save_batch(save_macs, 'addTaskSucess')
                print('save addTaskSucess index:' + str(add_task_sucess_save))
                save_macs.clear()
        except Exception as err:
            print('addTaskSucess tver:' + row.tver)
            print('addTaskSucess mac:' + row.mac)
            print(err)
            continue
    save_batch(save_macs, 'addTaskSucess')
    save_macs.clear()
    for row in df_from_group.itertuples():
        # print(row)
        duplicates_mac(debug_task_top_dir + row.tver + '/' + 'addTaskSucess')


def txt_download_sucess():
    print('txt_download_sucess')
    data = pd.read_table(filename_pre + yesterday_str() +
                         filename_mid + 'out4.txt', names=tv_download_apk_end_columns)
    df = pd.DataFrame(data)
    df.fillna(0, inplace=True)

    df.drop_duplicates(subset=['mac', 'tver', 'ok'], keep='last', inplace=True)
    df_valid = df[(df.ok.isin([1, 1000, 200])) & (df['mac'] != 0)]

    df_distinct_mac = del_old_mac(df_valid, 'downloadSucess')
    if (len(df_distinct_mac) == 0):
        return

    groups = df_distinct_mac.groupby(['h_mode', 'app_ver', 'tver'])
    df_from_group = groups.size().reset_index(name='count')
    # print(df_from_group)
    for row in df_from_group.itertuples():
        # print(row)
        txt_download_sucess_select_sql = 'SELECT * FROM debug_sdk_download_install WHERE h_mode=\'' + row.h_mode + '\' and sdk_ver=\'' + \
            str(row.app_ver) + '\' and app=\'' + row.tver + \
            '\' and date=\'' + yesterday_str() + '\';'
        txt_download_sucess_sql = ''
        if (mysql_query(txt_download_sucess_select_sql)):
            txt_download_sucess_sql = 'UPDATE debug_sdk_download_install set download_sucess=\'' + \
                str(row.count) + '\' where h_mode=\'' + row.h_mode + '\' and sdk_ver=\'' + \
                str(row.app_ver) + '\' and app=\'' + row.tver + \
                '\' and date=\'' + yesterday_str() + '\';'
        else:
            txt_download_sucess_sql = 'INSERT INTO debug_sdk_download_install (h_mode, sdk_ver, app, download_sucess, date) VALUES (\'' + row.h_mode + '\',\'' + str(row.app_ver) + '\',\'' + row.tver + '\',\'' + \
                str(row.count) + '\',\'' + yesterday_str() + '\');'
        mysql_execute(txt_download_sucess_sql)
    download_sucess_save = 0
    save_macs = {}
    for row in df_distinct_mac.itertuples():
        try:
            download_sucess_save += 1
            current_line = (row.mac + '\t' + yesterday_str() + '\n')
            if (row.tver in save_macs):
                save_macs[row.tver] = (save_macs[row.tver] + current_line)
            else:
                save_macs[row.tver] = current_line
            if (download_sucess_save % 10000 == 0):
                save_batch(save_macs, 'downloadSucess')
                print('save downloadSucess index:' + str(download_sucess_save))
                save_macs.clear()
        except Exception as err:
            print('downloadSucess tver:' + row.tver)
            print('downloadSucess mac:' + row.mac)
            print(err)
            continue
    save_batch(save_macs, 'downloadSucess')
    save_macs.clear()
    for row in df_from_group.itertuples():
        # print(row)
        duplicates_mac(debug_task_top_dir + row.tver + '/' + 'downloadSucess')


def txt_install_sucess():
    print('txt_install_sucess')
    data = pd.read_table(filename_pre + yesterday_str() + filename_mid +
                         'out5.txt', names=tv_download_apk_installed_columns)
    df = pd.DataFrame(data)
    df.fillna(0, inplace=True)
    df.drop_duplicates(subset=['mac', 'tver', 'ok'], keep='last', inplace=True)
    df['ok'] = df['ok'].astype('int')

    sucess_state = [1, -4, -25]
    df_valid = df[(df['ok'].isin(sucess_state)) & (df['mac'] != 0)]
    df_distinct_mac = del_old_mac(df_valid, 'installSucess')
    if (len(df_distinct_mac) == 0):
        return

    groups = df_distinct_mac.groupby(['h_mode', 'app_ver', 'tver'])
    df_from_group = groups.size().reset_index(name='count')
    # print(df_from_group)
    for row in df_from_group.itertuples():
        print(row)
        txt_install_sucess_select_sql = 'SELECT * FROM debug_sdk_download_install WHERE h_mode=\'' + row.h_mode + '\' and sdk_ver=\'' + \
            str(row.app_ver) + '\' and app=\'' + row.tver + \
            '\' and date=\'' + yesterday_str() + '\';'
        txt_install_sucess_sql = ''
        if (mysql_query(txt_install_sucess_select_sql)):
            txt_install_sucess_sql = 'UPDATE debug_sdk_download_install set install_sucess=\'' + \
                str(row.count) + '\' where h_mode=\'' + row.h_mode + '\' and sdk_ver=\'' + \
                str(row.app_ver) + '\' and app=\'' + row.tver + \
                '\' and date=\'' + yesterday_str() + '\';'
        else:
            txt_install_sucess_sql = 'INSERT INTO debug_sdk_download_install (h_mode, sdk_ver, app, install_sucess, date) VALUES (\'' + row.h_mode + '\',\'' + str(row.app_ver) + '\',\'' + row.tver + '\',\'' + \
                str(row.count) + '\',\'' + yesterday_str() + '\');'
        mysql_execute(txt_install_sucess_sql)
    install_sucess_save = 0
    save_macs = {}
    for row in df_distinct_mac.itertuples():
        try:
            install_sucess_save += 1
            current_line = (row.mac + '\t' + yesterday_str() + '\n')
            if (row.tver in save_macs):
                save_macs[row.tver] = (save_macs[row.tver] + current_line)
            else:
                save_macs[row.tver] = current_line
            if (install_sucess_save % 10000 == 0):
                save_batch(save_macs, 'installSucess')
                print('save installSucess index:' + str(install_sucess_save))
                save_macs.clear()
        except Exception as err:
            print('installSucess tver:' + row.tver)
            print('installSucess mac:' + row.mac)
            print(err)
            continue
    save_batch(save_macs, 'installSucess')
    save_macs.clear()
    for row in df_from_group.itertuples():
        # print(row)
        duplicates_mac(debug_task_top_dir + row.tver + '/' + 'installSucess')


############################################################P2P#######################################################################


init()
txt_download_install()
