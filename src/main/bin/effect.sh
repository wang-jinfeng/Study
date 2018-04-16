#!/usr/bin/env bash

rm -rf /home/ec2-user/dmp/wangjf/data/effect.txt
hive -e 'SELECT app_key,name,platform,create_time FROM dmp.app_top;' >> /home/ec2-user/dmp/wangjf/data/effect.txt

data='/home/ec2-user/dmp/wangjf/data/effect.txt'

cat ${data} | while read LINE
do
    app_key=`echo ${LINE} |cut -d ' ' -f 1`
    app_name=`echo ${LINE} |cut -d ' ' -f 2`
    platform=`echo ${LINE} |cut -d ' ' -f 3`
    create_time=`echo ${LINE} |cut -d ' ' -f 4,5`
    create_time=`date -d "1 day ${create_time}" +%Y-%m-%d HH:mm:ss`
    if [ ${platform} == 'iOS' ]; then
        os_type="_idfa"
    else
        os_type="_imei"
    fi
    hive -hiveconf app_key=${app_key} -hiveconf os_type=${os_type} -hiveconf app_name=${app_name} -hiveconf platform=${platform} -hiveconf create_time=${create_time} -f /home/ec2-user/dmp/wangjf/sql/close_effect.sql

    if [ $? -eq 0 ];then
       echo "close effect is ok!!!"
    else
       echo "close effect is failure!!!"
       exit 0
    fi
done