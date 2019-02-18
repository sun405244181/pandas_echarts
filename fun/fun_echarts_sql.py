#coding:utf-8
#!/usr/bin/python
from fun.fun_date_util import yesterday_str
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