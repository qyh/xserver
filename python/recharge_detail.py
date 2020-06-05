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

f = open("前10000大R充值明细.txt", "wb")
f.write("用户系统ID||昵称||充值时间||充值金额||去向\n".encode('utf-8'))

sql = "select id, gaingoods from oss_zipai2019.mall"
cursor.execute(sql)
# 获取记录
results = cursor.fetchall()
mallInfo = {}
for line in results:
	gains = line[1].split("=")
	if len(gains) == 2:
		mallInfo[line[0]] = {
			"id": line[0],
			"gaingoods": {
				"itemId": int(gains[0]),
				"num": int(gains[1])
			}
		}
print(mallInfo)
sql = '''select userid, nickname, totalfee, mallid,notifytime from (
select * from oss_zipai2017.onlinepaynotify where outtradeno like 'glzp%' or outtradeno like 'glmj%' or outtradeno like 'glpp%' union 
select * from oss_zipai2018.onlinepaynotify where outtradeno like 'glzp%' or outtradeno like 'glmj%' or outtradeno like 'glpp%' union
select * from oss_zipai2019.onlinepaynotify where (outtradeno like 'glzp%' or outtradeno like 'glmj%' or outtradeno like 'glpp%') and notifytime < '2020-04-01 00:00:00'
) where userid in (select userid from oss_zipai2018.paymenttop10000_2)'''
print(sql)
cursor.execute(sql)
n = 1
while True:
	row = cursor.fetchone()
	if not row:
		break
	mallId = row[3]
	goodsInfo = mallInfo.get(mallId)
	print("row:", n)
	n = n + 1
	goodsName = "其他"
	if goodsInfo:
		if goodsInfo["gaingoods"]["itemId"] == 0:
			goodsName = "金币"
		if goodsInfo["gaingoods"]["itemId"] == 107:
			goodsName = "房卡"
	f.write(("{}||{}||{}||{}||{}\n".format(row[0], row[1], row[4], row[2], goodsName)).encode("utf-8"))
f.close()
