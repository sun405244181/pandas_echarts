#coding:utf-8
#!/usr/bin/python

from fun.fun_echarts_sql import mysql_fetchall
from fun.fun_email import fun_sendmail
import time

DEBUG = False

def sdkmanager():

    install_list = ['com.audiorecoder.demo', \
                    'com.avc_mr.datacollectionandroid', \
                    'com.bestv.mishitong.gw', \
                    'com.example.com.bestv.ott.api', \
                    'com.example.dialogdemo', \
                    'com.funshion.publicity', \
                    'com.kuyun.common.androidtv', \
                    'com.kuyun.common.identifer', \
                    'com.qiyivideo.fengxing.tvapi', \
                    'tv.fun.orange', \
                    'tv.fun.sdkmanager', \
                    'com.gzads.tvac', \
                    'com.funshion.tvdlnarender']

    DOWNLOAD_RATE = 0.99
    INSTALL_RATE = 0.99

    subject = 'ROM统计上报预警-SDKManager'
    content = ''
    warning = False

    '''
    日活
    '''
    sql = 'SELECT sum(app_count) as app_count, date FROM sdk_upgrade_info group by date order by date desc limit 8'
    results = mysql_fetchall(sql)
    print(results)
    rs = send(results)
    if (rs[0] or DEBUG):
        warning = True
        content += 'SDKManager日活(暂定低于过去７天最低值为异常)</br></br>'
        content += '<table id=\'tab\' style="width: 50%; height: auto; table-layout: fixed;" border="1" bordercolor="black" cellspacing="0" cellpadding="6">\
                        <tr>\
                            <th width="20%"></th>\
                            <th width="10%">基准值</th>\
                            <th width="10%">实际值</th>\
                        </tr>\
                        <tr>\
                            <td align="center">SDKManager日活</td>\
                            <td align="center">' + str(rs[1]) + '</td>\
                            <td align="center">' + str(rs[2]) + '</td>\
                        </tr>\
                    </table></br></br></br>'

    '''
    下载安装成功率
    '''
    sql = 'select app, sum(download_start) as count, date from sdk_download_install where date=(select max(date) from sdk_download_install) group by app order by count desc;'
    results = mysql_fetchall(sql)
    download_install_rate_table = True
    download_install_table_head = False
    print(results)
    for result in results:
        task = result[0]
        download_start = result[1]
        date = result[2]
        if (download_start < 100):
            break

        sql = 'select app, sum(download_start), sum(download_sucess), sum(install_sucess) from sdk_download_install where app=\'' + task + '\';'
        counts = mysql_fetchall(sql)
        
        download_rate_str = ''
        install_rate_str = ''

        download_sucess_rate = round(counts[0][2]/counts[0][1], 4)
        print(task + '---' + str(download_sucess_rate))
        if (download_sucess_rate < DOWNLOAD_RATE):
            download_rate_str = '{:.2%}'.format(download_sucess_rate)

        if ((counts[0][3] is not None) and (task[:task.find('_')] in install_list)):
            install_sucess_rate = round(counts[0][3]/counts[0][2], 4)
            print(task + '---' + str(install_sucess_rate))
            if (install_sucess_rate < INSTALL_RATE):
                install_rate_str = '{:.2%}'.format(install_sucess_rate)

        if (download_sucess_rate < DOWNLOAD_RATE or install_sucess_rate < INSTALL_RATE or DEBUG):
            warning = True
            if (download_install_rate_table):
                content += '下载安装成功率(暂定下载安装成功率基准值为99%)</br></br>'
                content += table_head('下载任务', '下载成功率', '安装成功率')
                download_install_rate_table = False
                download_install_table_head = True
            content += '<tr>\
                            <td align="center">' + task + '</td>\
                            <td align="center">' + download_rate_str + '</td>\
                            <td align="center">' + install_rate_str + '</td>\
                        </tr>'    

    if (download_install_table_head):
        content += table_end()
    if (warning):
        content += '<a href="http://172.17.5.117:8080/ECharts/sdkmanager.html">http://172.17.5.117:8080/ECharts/sdkmanager.html</a>'
        fun_sendmail(subject, content)

def dlna():
    subject = 'ROM统计上报预警-投屏'
    content = ''
    warning = False

    '''
    日活
    '''
    sql = 'select sum(count), date from cast_screen where protocal_id=\'DLNA\' group by date order by date desc limit 8;'
    results = mysql_fetchall(sql)
    print(results)
    dlna_dau_rs = send(results)

    sql = 'select sum(count), date from cast_screen where protocal_id=\'AIRPLAY\' group by date order by date desc limit 8;'
    results = mysql_fetchall(sql)
    print(results)
    airlpay_dau_rs = send(results)

    if (dlna_dau_rs[0] or airlpay_dau_rs[0] or DEBUG):
        warning = True
        content += '投屏日活(暂定低于过去７天最低值为异常)</br></br>'
        content += '<table id=\'tab\' style="width: 50%; height: auto; table-layout: fixed;" border="1" bordercolor="black" cellspacing="0" cellpadding="6">\
                        <tr>\
                            <th width="20%"></th>\
                            <th width="10%">基准值</th>\
                            <th width="10%">实际值</th>\
                        </tr>'       

    if (dlna_dau_rs[0] or DEBUG):
        content += '<tr>\
                        <td align="center">DLNA</td>\
                        <td align="center">' + str(dlna_dau_rs[1]) + '</td>\
                        <td align="center">' + str(dlna_dau_rs[2]) + '</td>\
                    </tr>'
    if (airlpay_dau_rs[0] or DEBUG):
        content += '<tr>\
                        <td align="center">AIRPLAY</td>\
                        <td align="center">' + str(airlpay_dau_rs[1]) + '</td>\
                        <td align="center">' + str(airlpay_dau_rs[2]) + '</td>\
                    </tr>'                    
    if (dlna_dau_rs[0] or airlpay_dau_rs[0] or DEBUG):
        content += '</table></br></br></br>'
 

    '''
    投屏成功率
    '''
    sql = 'SELECT sum(count) as count, date FROM cast_screen_status where protocal_id=\'DLNA\' and status=1 group by date order by date desc limit 8;'
    sucessResults = mysql_fetchall(sql)
    sql = 'SELECT sum(count) as count, date FROM cast_screen_status where protocal_id=\'DLNA\' group by date order by date desc limit 8;'
    totalResults = mysql_fetchall(sql)
    results = []
    for (sucessResult, totalResult) in zip(sucessResults, totalResults):
        result = (round(sucessResult[0]/totalResult[0], 4), sucessResult[1])
        results.append(result)
    print(results)
    dlna_rate_rs = send(results)

    sql = 'SELECT sum(count) as count, date FROM cast_screen_status where protocal_id=\'AIRPLAY\' and status=1 group by date order by date desc limit 8;'
    sucessResults = mysql_fetchall(sql)
    sql = 'SELECT sum(count) as count, date FROM cast_screen_status where protocal_id=\'AIRPLAY\' group by date order by date desc limit 8;'
    totalResults = mysql_fetchall(sql)
    results = []
    for (sucessResult, totalResult) in zip(sucessResults, totalResults):
        result = (round(sucessResult[0]/totalResult[0], 4), sucessResult[1])
        results.append(result)
    print(results)
    airplay_rate_rs = send(results)

    if (dlna_rate_rs[0] or airplay_rate_rs[0] or DEBUG):
        warning = True
        content += '投屏成功/失败次数统计(暂定低于过去７天最低值为异常)</br></br>'
        content += '<table id=\'tab\' style="width: 50%; height: auto; table-layout: fixed;" border="1" bordercolor="black" cellspacing="0" cellpadding="6">\
                        <tr>\
                            <th width="20%"></th>\
                            <th width="10%">基准值</th>\
                            <th width="10%">实际值</th>\
                        </tr>'       

    if (dlna_rate_rs[0] or DEBUG):
        content += '<tr>\
                        <td align="center">DLNA</td>\
                        <td align="center">' + '{:.2%}'.format(dlna_rate_rs[1]) + '</td>\
                        <td align="center">' + '{:.2%}'.format(dlna_rate_rs[2]) + '</td>\
                    </tr>'
    if (airplay_rate_rs[0] or DEBUG):
        content += '<tr>\
                        <td align="center">AIRPLAY</td>\
                        <td align="center">' + '{:.2%}'.format(airplay_rate_rs[1]) + '</td>\
                        <td align="center">' + '{:.2%}'.format(airplay_rate_rs[2]) + '</td>\
                    </tr>'                    
    if (dlna_rate_rs[0] or airplay_rate_rs[0] or DEBUG):
        content += '</table></br></br></br>'    
    
    if (warning):
        content += '<a href="http://172.17.5.117:8080/ECharts/dlna.html">http://172.17.5.117:8080/ECharts/dlna.html</a>'
        fun_sendmail(subject, content)

def send(results):
    count = results[0][0]
    date = results[0][1]
    min_count = count
    for result in results:
        if (date == result[1]):
            continue
        if (result[0] < min_count):
            min_count = result[0]
    if (count < min_count):
        return True, min_count, count
    return False, min_count, count

def html_head():
    return '<!DOCTYPE html>\
            <html>\
            <head>\
            <meta charset="utf-8">\
            <title>LOG处理</title>\
            <style>\
            td {\
	            white-space: nowrap;\
	            overflow: hidden;\
	            text-overflow: ellipsis;\
            }\
            </style>\
            </head>\
            <body>'

def html_end():
    return '</body>\
            </html>'

def table_head(c1, c2, c3):
    return '<table id=\'tab\' style="width: 80%; height: auto; table-layout: fixed;" border="1" bordercolor="black" cellspacing="0" cellpadding="6">\
                <tr>\
                    <th width="40%">' + c1 + '</th>\
                    <th width="20%">' + c2 + '</th>\
                    <th width="20%">' + c3 + '</th>\
                </tr>'

def table_end():
    return '</table></br>'


if __name__ == '__main__':
    sdkmanager()
    dlna()
