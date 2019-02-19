# coding:utf-8
#!/usr/bin/python
from fun_constant import *
from fun_date_util import yesterday_str
import pymysql as msql

host = 'localhost'
user = 'rom'
password = '123456'
database = 'rom_charts'


def mysql_inserted(table_name):
    print('updated rom_table_name:' + table_name)
    db = msql.connect(host, user, password, database, charset='utf8')
    rowcount = 0
    sql = 'select * from ' + table_name + ' where date = ' + yesterday_str() + ';'
    cursor = db.cursor()
    try:
        rowcount = cursor.execute(sql)
        db.commit()
    except:
        db.rollback()
    db.close()
    print(rowcount)
    return rowcount


def mysql_query(sql):
    print('mysql_query sql:' + sql)
    db = msql.connect(host, user, password, database, charset='utf8')
    rowcount = 0
    cursor = db.cursor()
    try:
        rowcount = cursor.execute(sql)
        db.commit()
    except:
        db.rollback()
    db.close()
    if (rowcount > 0):
        return True
    return False


def mysql_fetchall(sql):
    print('mysql_fetchall sql:' + sql)
    db = msql.connect(host, user, password, database, charset='utf8')
    global results
    rowcount = 0
    cursor = db.cursor()
    try:
        rowcount = cursor.execute(sql)
        results = cursor.fetchall()
        db.commit()
    except:
        db.rollback()
    db.close()
    return results


def mysql_execute(sql):
    print('mysql_execute sql:' + sql)
    db = msql.connect(host, user, password, database, charset='utf8')
    cursor = db.cursor()
    try:
        cursor.execute(sql)
        db.commit()
    except:
        db.rollback()
    db.close()


def mysql_group_count(sql):
    print('mysql_group_count sql:' + sql)
    db = msql.connect(host, user, password, database, charset='utf8')
    rowcount = 0
    gcount = 0
    cursor = db.cursor()
    try:
        rowcount = cursor.execute(sql)
        result = cursor.fetchone()
        gcount = result[0]
        db.commit()
    except:
        db.rollback()
    db.close()
    print('rowcount , gcount :' + str(rowcount) + ',' + str(gcount))
    return rowcount, gcount


def mysql_create_table(tablename, create_sql):
    exist_sql = 'show tables like \'' + tablename + '\';'
    print(exist_sql)
    db = msql.connect(host, user, password, database, charset='utf8')
    cursor = db.cursor()
    try:
        cursor.execute(exist_sql)
        results = cursor.fetchall()
        print(len(results))
        if (len(results) == 0):
            cursor.execute(create_sql)
            print('create table ' + tablename + ' sucess!')
        else:
            print('table ' + tablename + ' already exist!')
        db.commit()
    except:
        print('create table ' + tablename + ' failed!')
    db.close()


if __name__ == '__main__':
    mysql_create_table('debug_process', debug_process_create_sql)
    mysql_create_table('debug_sdk_download_install',
                       debug_sdk_download_install_create_sql)
