#coding:utf-8
#!/usr/bin/python
from fun.fun_constant import *
from fun.fun_date_util import yesterday_str
import difflib
import gzip
import hashlib
import os
import pymysql as msql
import requests
import zipfile

def downloadZip():
    print('downloadZip')
    #file_url = "http://yhds-admin.tv.funshion.com/api/log/v1/rom/data/romdata20181209.zip?sign=29a9006ff37844f075c3e149b871929d"

    file_url = 'http://yhds-admin.tv.funshion.com/api/log/v1/rom/data/' + getFileName() + '?sign=' + getSign()
    print('downloadZip ' + file_url)
    log_dir = echarts_base_dir + '/logs/'
    if (os.path.exists(log_dir) == False):
        os.makedirs(log_dir)    
    r = requests.get(file_url, stream=True)
    with open(log_dir + "/out.zip", "wb") as out:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                out.write(chunk)

    extractZipFile()

def getFileName():
    fileName = 'romdata' + yesterday_str() + '.zip'
    return fileName

def getSign():
    source = getFileName() + 'ecd3fed47446c971'
    md5 = hashlib.md5(source.encode(encoding='UTF-8')).hexdigest()
    return md5

def extractZipFile():
    zf = zipfile.ZipFile(os.path.join(echarts_base_dir, echarts_base_dir + '/logs/out.zip'))
    for file in zf.namelist():
        zf.extract(file, echarts_base_dir + '/out/logs/' + yesterday_str())
    zf.close()

def executeSql(sql):
    print(sql)
    db = msql.connect("localhost", "rom", "123456", "rom_charts", charset='utf8')
    rowcount = 0
    cursor = db.cursor()
    try:
        rowcount = cursor.execute(sql)
        db.commit()
    except:
        db.rollback()
    db.close()
    return rowcount;        

def getLogRootPath():
    return echarts_base_dir + '/out/logs/' +  yesterday_str() + '/data/master/upload/log'

def getTagFromFilename(filename):
    start = filename.find('@') + 1
    end = filename.rfind('@')
    return filename[start : end]

def listLogFiles():
    root = getLogRootPath()
    files = os.listdir(root)
    for filename in files:
        tag = getTagFromFilename(filename)
        path = root + '/' + filename
        if (tag == 'data_app_crash'):
            data_app_crash(path)
        elif (tag == 'system_server_crash'):
            system_server_crash(path)
        elif (tag == 'system_server_watchdog'):
            system_server_watchdog(path)
        else:
            print(tag)

def data_app_crash(path):
    print(path)

    romVer = ''
    chiptype = ''
    process = ''
    title = ''
    content = ''

    global dafo
    if (path.endswith('.txt')):
        dafo = open(path, "r")
    elif (path.endswith('.txt.gz')):
        dafo = gzip.open(path, "r")
    lineIndex = 0
    for line in dafo.readlines():
        try:
            line = line.decode('utf-8')
        except:
            pass    
        if (line.startswith('Version:')):
            romVer = line[(line.find(':') + 1) : ].strip()
        elif (line.startswith('ChipType:')):
            chiptype = line[(line.find(':') + 1) : ].strip()
        elif (line.startswith('Process:')):
            process = line[(line.find(':') + 1) : ].strip()
        lineIndex += 1
        if (lineIndex == 11):
            title = line.strip()
        if (lineIndex >= 11):    
            content += line
    dafo.close()

    '''
    print(romVer)
    print(chiptype)
    print(process)
    print(title)
    print(content)
    '''    

    if (content.strip() == ''):
        return
    querySql = 'select * from log where process=\'' + process + '\' and title=\'' + title + '\' and content=\'' + content +  '\';'
    updateSql = ''
    rowCount = executeSql(querySql)
    print(rowCount)
    if (rowCount <= 0):
        updateSql = 'INSERT INTO log (rom_ver, chip_type, tag, process, title, path, content, count, status, date) VALUES (\'' +  romVer + '\',\'' + chiptype + '\',\'data_app_crash\',\'' + process + '\',\'' + title + '\',\'' + path + '\',\'' + content + '\',1,0,\'' + yesterday_str() + '\');'
    else:
        updateSql = 'UPDATE log set count=\'' + str(rowCount + 1) + '\' where process=\'' + process + '\' and title=\'' + title + '\' and content=\'' + content + '\';'
    executeSql(updateSql)    

def system_server_crash(path):
    pass

def system_server_watchdog(path):
    print(path)

    romVer = ''
    chiptype = ''
    process = ''
    title = ''
    content = ''

    global sswfo
    if (path.endswith('.txt')):
        sswfo = open(path, "r")
    elif (path.endswith('.txt.gz')):
        sswfo = gzip.open(path, "r")
    global mainLine
    mainLine = False
    for line in sswfo.readlines():
        try:
            line = line.decode('utf-8')
        except:
            pass  
        if (line.startswith('Version:')):
            romVer = line[(line.find(':') + 1) : ].strip()
        elif (line.startswith('ChipType:')):
            chiptype = line[(line.find(':') + 1) : ].strip()
        elif (line.startswith('Process:')):
            process = line[(line.find(':') + 1) : ].strip()
        elif (line.startswith('Subject:')):
            title = line[(line.find(':') + 1) : ].strip()
        elif (line.startswith('\"main\"')):
            mainLine = True
        if (mainLine):
            content += line
        if (mainLine and (line.strip() == '')):
            mainLine = False            
    sswfo.close()
    
    '''
    print(romVer)
    print(chiptype)
    print(process)
    print(title)
    print(content)
    '''

    if (content.strip() == ''):
        return
    querySql = 'select * from log where process=\'' + process + '\' and title=\'' + title + '\' and content=\'' + content +  '\';'
    updateSql = ''
    rowCount = executeSql(querySql)
    print(rowCount)
    if (rowCount <= 0):
        updateSql = 'INSERT INTO log (rom_ver, chip_type, tag, process, title, path, content, count, status, date) VALUES (\'' +  romVer + '\',\'' + chiptype + '\',\'system_server_watchdog\',\'' + process + '\',\'' + title + '\',\'' + path + '\',\'' + content + '\',1,0,\'' + yesterday_str() + '\');'
    else:
        updateSql = 'UPDATE log set count=\'' + str(rowCount + 1) + '\' where process=\'' + process + '\' and title=\'' + title + '\' and content=\'' + content + '\';'
    executeSql(updateSql)       

def log_diff_ratio():############不准确
    a=""
    text1_lines = a.splitlines()
    b=""
    text2_lines = b.splitlines()

    #print(difflib.SequenceMatcher(None,a,b).ratio())
    d = difflib.Differ()
    diff = d.compare(text1_lines, text2_lines)
    print('\n'.join(diff))


def showgz(path):
    print(path)
    global testfoo
    if (path.endswith('.txt')):
        testfoo = open(path, "r")
    elif (path.endswith('.txt.gz')):
        print('gz')
        testfoo = gzip.open(path, "r")
    for line in testfoo.readlines():
        print(line)
    testfoo.close()



downloadZip()
listLogFiles()
#log_diff_ratio()
#showgz('/home/sunwc/romshare/SDK_ECharts/out/logs/20181218/data/master/upload/log/2876cd01d9e3_638_4.3.3_2018-10-18_16-55@data_app_crash@1545026174710.txt.gz')