#!/bin/bash

basepath=$(cd `dirname $0`; pwd)

cd $basepath

if [ ! -d 'temp/' ];then
    mkdir 'temp/'
else
    echo "文件夹已经存在"
fi

#/opt/anaconda3/bin/python
#/home/sunwc/.pyenv/shims/python

/home/sunwc/.pyenv/shims/python httpDownload.py > temp/log.httpDownload
/home/sunwc/.pyenv/shims/python sdkmanager.py > temp/log.sdkmanager
/home/sunwc/.pyenv/shims/python dlna.py > temp/log.dlna
/home/sunwc/.pyenv/shims/python romRecovery.py > temp/log.romRecovery
/home/sunwc/.pyenv/shims/python bt.py > temp/log.bt
/home/sunwc/.pyenv/shims/python log.py > temp/log.log
#/home/sunwc/.pyenv/shims/python report.py > temp/report.log

