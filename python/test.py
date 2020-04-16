# -*- coding:utf8 -*
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

a = {'id':1, 'name':"jack", 'num':800}
b = {'id':2, 'name':"jack", 'num':700}
c = {'id':3, 'name':"jack", 'num':900}
d = {'id':4, 'name':"jack", 'num':1000}

l = []

l.append(a)
l.append(b)
l.append(c)
l.append(d)
def cmp(a, b):
	if a[2] < b[2]:
		return 1
	elif a[2] < b[2]:
		return -1
	else:
		return 0
l.sort(key=lambda item: item['num'],reverse=True)
print(l)
a = (1, 'hello', 100)
for v in a:
	print(v)

start_time = "2017-03-05"
end_time = "2020-04-01"
end_t = getTimeByDate(end_time)
val_time = getTimeByDate(start_time)
f = open("game_record_tables.txt", "wb")
if f:
	while val_time < end_t:
		d = getDate(val_time)
		year=d.tm_year
		if year > 2019:
			year = 2019
		print("year:" + str(d.tm_year) + " prefix:" + dayStr(val_time))
		txt = "select userid, state, starttime from oss_gamelog{}.gameuserinfolog_{} where starttime <'2020-04-01' union\n".format(year, dayStr(val_time, "_"))
		f.write(txt.encode("utf8"))
		txt = "select userid, state, starttime from oss_gamelog{}.matchuserinfolog_{} where starttime <'2020-04-01' union\n".format(year, dayStr(val_time, "_"))
		f.write(txt.encode("utf8"))
		txt = "select userid, state, starttime from oss_gamelog{}.newroomselfuserinfolog_{} where starttime <'2020-04-01' union\n".format(year, dayStr(val_time, "_"))
		f.write(txt.encode("utf8"))
		val_time = getTimeByDate(getNextDay(dayStr(val_time), 7))
	
f.close()
'''
f = open("test_detail.txt", "wb")
f.write("我们\n".encode('utf8'))
f.write("我们\n".encode('utf8'))

f.close()
'''
a = {}
a[1] = {'a':1}
aa = a.get(1)
aa['a'] = 2
print(a.get(1))
print("end")


