# coding:utf-8
#!/usr/bin/python

import datetime


def yesterday_str():
    today = datetime.datetime.now()
    ddelay = datetime.timedelta(days=1)
    yesterday = today - ddelay
    # print(yesterday.strftime('%Y%m%d'))
    return yesterday.strftime('%Y%m%d')


if __name__ == '__main__':
    print('yesterday:' + yesterday_str())
