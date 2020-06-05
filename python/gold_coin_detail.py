# coding: utf-8
from openpyxl import Workbook
import sys
import os
import time
import platform
import sys
import io
import MySQLdb
import datetime
from dateutil.relativedelta import relativedelta
 

# 打开数据库连接
db = MySQLdb.connect(host="1475177020277729-73404573.cn-shenzhen.datalakeanalytics.aliyuncs.com",port=10000, user="qinyanhong_s1475177020277729",passwd="wQOsikfKJLctvn1ctc",db="oss_gamelog2017",charset='utf8')

# 使用cursor()方法获取操作游标 
cursor = db.cursor()
 
datetime_now = datetime.datetime.now()
datetime_three_month_ago = datetime_now - relativedelta(months=6)
print (datetime_three_month_ago)


start_time_str = "2017-08-01"
end_time_str = "2020-04-01"
e_time = time.mktime(time.strptime(end_time_str,"%Y-%m-%d"))
start_t = datetime.datetime.strptime(start_time_str, "%Y-%m-%d")
end_t = datetime.datetime.strptime(end_time_str, "%Y-%m-%d")
print(e_time)
print(start_t)
next_month = start_t + relativedelta(months=1)
print(next_month)
print(end_t)
print(end_t > next_month)
print(end_t.strftime("%Y-%m-%d"))
print(end_t.year)
f = open("前100大R消耗明细.txt", "wb")
f.write("用户系统ID,昵称,消耗时间,消耗币种,消耗数量\n".encode('utf-8'))
tmp_t = start_t 
while tmp_t < end_t:
	year = tmp_t.year 
	if year > 2019:
		year = 2019
	for prefix in ["goldcoinlog", "roomcardlog"]:
		tname = "oss_zipai{}.{}_{}".format(year,prefix,tmp_t.strftime("%Y_%m"))
		sql = "select userid,nickname,time,goodsid,changecurrency,remarkid from {} where userid in (select userid from oss_zipai2018.paymenttop10000_2 order by sum desc limit 100)".format(tname)
		print(tname)
		print(sql)
		cursor.execute(sql)
		# 获取记录
		results = cursor.fetchall()
		for line in results:
			if line[5] != 11:
				goodsName = "其他"
				if line[3] == 0:
					goodsName = "金币"
				if line[3] == 107:
					goodsName = "房卡"
				f.write(("{},{},{},{},{}\n".format(line[0], line[1],line[2],goodsName,line[4])).encode('utf-8'))
			
	tmp_t = tmp_t + relativedelta(months=1)
f.close()