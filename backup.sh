#!/bin/bash
echo "backup echarts data"

basepath=$(cd `dirname $0`; pwd)
echo $basepath
yesterday=$(date +"%Y%m%d" -d "-1 days")
echo $yesterday
sqlfile='rom_charts.sql'
top='backup/'
separator='/'

init() {    
    backupDir $basepath$separator$top
    currentDir
}

currentDir() { 
    backupDir $basepath$separator$top$yesterday
    echo $basepath$separator$top$yesterday
}

backupDir() {
    path=$1
    if [ ! -d $path ];then
        mkdir $path
    else
        echo "文件夹已经存在"
    fi
}

backupSql() {
    echo "备份数据库"
    mysqldump -h localhost  -P 3306 -u rom -p123456 rom_charts > $basepath$separator$top$yesterday$separator$sqlfile
}

backupTasks() {
    echo "备份下载任务"
    tasksDir='/out/tasks'
    cp -rf $basepath$tasksDir $basepath$separator$top$yesterday
}


init
backupSql
backupTasks

