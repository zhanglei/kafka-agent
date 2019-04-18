#!/bin/bash

echo '使用说明：sh hbase-batchdelete.sh 2018-01-22 17:11:52 2018-11-22 17:14:53 ccs:account info:ts'

echo '--------------程序从这里开始------------'
basepath=$(cd `dirname $0`; pwd)
#basepath=$(cd <code>dirname $0; pwd)
 
echo '---------------正在创建缓存文件夹--------------'
firstTime="_$1_$2"
mkdir $basepath/CacheOfdelete$firstTime
#touch $basepath/CacheOfdelete$firstTime/data$firstTime.txt
touch $basepath/CacheOfdelete$firstTime/record$firstTime.txt
touch $basepath/CacheOfdelete$firstTime/delete$firstTime.sh
 
#current1="2018-01-17 16:25:18"
#current2="2018-01-17 16:29:50"
current1="$1 $2"
current2="$3 $4"
 
tablename="$5"
 
echo 开始时间:$current1
echo 结束时间:$current2
startSec=`date -d "$current1" +%s`
endSec=`date -d "$current2" +%s`
 
startTimestamp=$((startSec*1000+`date "+%N"`/1000000))
endTimestamp=$((endSec*1000+`date "+%N"`/1000000))
 
echo $tablename
echo $startTimestamp
echo $endTimestamp
#echo $startTimestamp > $basepath/CacheOfdelete$firstTime/data$firstTime.txt
##echo $endTimestamp >> $basepath/CacheOfdelete$firstTime/data$firstTime.txt
 
# #######第一步：通过时间戳找到要删除的数据
# 注：这里只有rowkey和其中一列，因为目的是找到rowkey 
echo "scan '$tablename',{ COLUMNS => '$6',TIMERANGE => [$startTimestamp,$endTimestamp]}" | hbase shell > $basepath/CacheOfdelete$firstTime/record$firstTime.txt
# ######第二步：构建删除数据的shell
#echo "#!/bin/bash " >> $basepath/CacheOfdelete$firstTime/aa.sh
echo "#!/bin/bash " >> $basepath/CacheOfdelete$firstTime/delete$firstTime.sh
echo "exec hbase shell <<EOF " >> $basepath/CacheOfdelete$firstTime/delete$firstTime.sh
 
cat $basepath/CacheOfdelete$firstTime/record$firstTime.txt|awk '{print "deleteall '\'$tablename\''", ",", "'\''"$1"'\''"}' tName="$tablename" >> $basepath/CacheOfdelete$firstTime/delete$firstTime.sh
 
echo "EOF " >> $basepath/CacheOfdelete$firstTime/delete$firstTime.sh
 
# ########第三步：执行删除shell
#sh $basepath/CacheOfdelete$firstTime/delete$firstTime.sh
#echo '---------------正在删除缓存文件夹--------------'
#rm -rf $basepath/CacheOfdelete$firstTime
#echo '--------------程序到这里结束------------'
 
 
