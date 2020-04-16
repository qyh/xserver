# coding: utf-8
import redis
from openpyxl import Workbook
import sys
import os
import time
import datetime
import platform
import rank_user
import sys
import io
import MySQLdb


#chr : 数字转 ascii
#ord : ascii 转数字

# 打开数据库连接
db = MySQLdb.connect(host="1475177020277729-73404573.cn-shenzhen.datalakeanalytics.aliyuncs.com",port=10000, user="qinyanhong_s1475177020277729",passwd="wQOsikfKJLctvn1ctc",db="oss_gamelog2017",charset='utf8')

# 使用cursor()方法获取操作游标 
cursor = db.cursor()

def getNextDay(str, n=1):
	_t = datetime.datetime.strptime(str, "%Y-%m-%d")
	out_date = (_t + datetime.timedelta(days=n)).strftime("%Y-%m-%d")
	return out_date
	
def getTimeByDate(str):
	_t = time.mktime(time.strptime(str,"%Y-%m-%d"))
	return int(_t)

def getDateByTime(t=time.time()):
	timeStamp = t 
	timeArray = time.localtime(timeStamp)
	return time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
def dayStr(t=time.time(), sep="-"):
	timeStamp = t 
	timeArray = time.localtime(timeStamp)
	return time.strftime("%Y{}%m{}%d".format(sep,sep), timeArray)

#@return tm_year=2020, tm_mon=4, tm_mday=15
def getDate(t=time.time()):
	timeStamp = t 
	timeArray = time.localtime(timeStamp)
	return timeArray

sql = "select userid from oss_zipai2018.paymenttop10000"
cursor.execute(sql)
# 获取记录并放入topList
results = cursor.fetchall()
users = {}
for line in results:
	userid = line[0]
	#print(userid)
	winCount = 0
	loseCount = 0
	users[userid] = {'win':winCount, 'lose':loseCount}
#print(users[509096])
print("users len:", len(users))

start_time = "2017-03-05"
end_time = "2020-04-01"
end_t = getTimeByDate(end_time)
val_time = getTimeByDate(start_time)
while val_time < end_t:
	d = getDate(val_time)
	year=d.tm_year
	if year > 2019:
		year = 2019
	sql = "select userid,state, count(*) as sum from oss_gamelog{}.gameuserinfolog_{} where (state='赢' or state='输') and userid in(select userid from oss_zipai2018.paymenttop10000 ) group by userid, state".format(year,dayStr(val_time, "_"))
	print("sql:", sql)
	cursor.execute(sql)
	results = cursor.fetchall()
	for line in results:
		userid = line[0]
		state = line[1]
		count = line[2]
		userinfo = users.get(userid)
		if userinfo:
			if state == '赢':
				userinfo['win'] = userinfo['win'] + count
			else:
				userinfo['lose'] = userinfo['lose'] + count
		else:
			print("no user {} found".format(userid))
	val_time = getTimeByDate(getNextDay(dayStr(val_time), 7))
	break
outfile = open("userwinlose.txt", "wb")
#写文件
for userid, info in users.items():
	print(userid, info['win'], info['lose'])
	txt = "{},{},{}\n".format(userid, info['win'], info['lose'])
	outfile.write(txt.encode('utf-8'))
	
outfile.close()
#写数据库
for userid, info in users.items():
	print(userid, info['win'], info['lose'])
	'''
	sql = "insert into oss_zipai2018.winlose(userid, win, lose) values({},{},{})".format(userid, info['win'], info['lose'])
	try:
		cursor.execute(sql)
		db.commit()
	except:
		db.rollback()
	'''

db.close()
print("all done !!")



