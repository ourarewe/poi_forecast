#coding:utf-8
import numpy as np
from numpy import *
import pandas as pd
import MySQLdb
import time
from scipy.spatial.kdtree import KDTree

df = pd.read_csv('/home/kaka/region_popul_static.csv'
               , parse_dates=True, index_col='time')
df = df.fillna(method='ffill')  # value=0 method='ffill'

c = ['体育中心时尚天河', '体育中心内场', '体育中心外场']
s = str(df[-1:].index[0])

print s

dr_1day = pd.date_range(end=s,periods=4*24+1,freq='15min') # s的前24小时
pred_df = pd.DataFrame(zeros((9,3)), index=pd.date_range(s, periods=9, freq='15min'))

for c_i in range(3):
	col_name = c[c_i]
	ts_a = df.loc[:,[col_name]]
	ts_temp = df.loc['2015-10-23':str(dr_1day[0]),[col_name]] # 2015-10-23 到当前时间的前一天为历史集
	ts = ts_temp.append(df.loc[ s, [col_name] ]) # 加上当前时刻人数
	ts_shift = ts
	# 与前十二小时人数作比较
	for i in range(1,4*12+1):
    		ts_shift = pd.merge(ts_shift, ts_a.shift(i), how='inner', left_index=True, right_index=True)
	ts_shift = ts_shift.dropna(how='any')
	X = ts_shift.values
	#kd-tree query----------2sec
	tree = KDTree(X)
	kn = 10  # 9 nearest points 
	dist, ind = tree.query(X[-1], k=kn)
	#在9个最近点里找后面有8个值的点
	for i in range(1,kn):
		idx = ind[i]
		idx_date = str(ts_shift.index[idx])
		dr_2hours = pd.date_range(start=idx_date,periods=4*2+1,freq='15min')
		pred_array = df.loc[str(dr_2hours[0]):str(dr_2hours[-1]),[col_name]].values
		if len(pred_array)==9: break
	#如果9个最近点后面都不够8个值，则用上个数代替缺失值
	if len(pred_array)<9:
		idx = ind[1]  #如果9个最近点都不满足要求，则取回最近的点
		idx_date = str(ts_shift.index[idx])
		dr_2hours = pd.date_range(start=idx_date,periods=4*2+1,freq='15min')
		temp_array = df.loc[str(dr_2hours[0]):str(dr_2hours[-1]),[col_name]].values
		pred_array = zeros((9,1))
		temp_values = temp_array[0,0]
		for i in range(9):
			try:
				pred_array[i,0] = temp_array[i,0]
				temp_values = temp_array[i,0]
			except:
				pred_array[i,0] = temp_values   
	print '相似时刻是：',idx_date
	a = ts_a.loc[s].values
	b = ts_a.loc[idx_date].values
	pred_df.loc[:,[c_i]] = pred_array*a/b
		
db = MySQLdb.connect(host="localhost", db="datamining", charset="utf8")
cursor = db.cursor()


# 更新poi_forecast的预测值 range(1,9)	
for i in range(1,9):
	t = str(pred_df[i:i+1].index[0])[0:15]
	sql = "update poi_forecast set forecast_count=%i where poi='体育中心时尚天河' and substring(time,1,15)='%s'" % (pred_df.values[i,0]*25/3, t)
	cursor.execute(sql)
	sql = "update poi_forecast set forecast_count=%i where poi='体育中心内场' and substring(time,1,15)='%s'" % (pred_df.values[i,1]*25/3, t)
	cursor.execute(sql)
	sql = "update poi_forecast set forecast_count=%i where poi='体育中心外场' and substring(time,1,15)='%s'" % (pred_df.values[i,2]*25/3, t)
	cursor.execute(sql)


# 更新gz_poi_area_forecast的预测值 range(1,9)	
for i in range(1,9):
	t = str(pred_df[i:i+1].index[0])[0:15]
	sql = "update gz_poi_area_forecast set count=%i where poi='体育中心时尚天河' and substring(f_time,1,15)='%s'" % (pred_df.values[i,0]*25/3, t)
	cursor.execute(sql)
	sql = "update gz_poi_area_forecast set count=%i where poi='体育中心内场' and substring(f_time,1,15)='%s'" % (pred_df.values[i,1]*25/3, t)
	cursor.execute(sql)
	sql = "update gz_poi_area_forecast set count=%i where poi='体育中心外场' and substring(f_time,1,15)='%s'" % (pred_df.values[i,2]*25/3, t)
	cursor.execute(sql)


#插入poi_forecast_log
sql = "insert into poi_forecast_log (actiontime,poi,t0,t1,t2,t3,t4,t5,t6,t7,t8) values ('%s', '体育中心时尚天河', %i, %i, %i, %i, %i, %i, %i, %i, %i)" \
% (s, pred_df.values[0,0], pred_df.values[1,0], pred_df.values[2,0], pred_df.values[3,0], pred_df.values[4,0], pred_df.values[5,0], pred_df.values[6,0], pred_df.values[7,0], pred_df.values[8,0])
cursor.execute(sql)
sql = "insert into poi_forecast_log (actiontime,poi,t0,t1,t2,t3,t4,t5,t6,t7,t8) values ('%s', '体育中心内场', %i, %i, %i, %i, %i, %i, %i, %i, %i)" \
% (s, pred_df.values[0,1], pred_df.values[1,1], pred_df.values[2,1], pred_df.values[3,1], pred_df.values[4,1], pred_df.values[5,1], pred_df.values[6,1], pred_df.values[7,1], pred_df.values[8,1])
cursor.execute(sql)
sql = "insert into poi_forecast_log (actiontime,poi,t0,t1,t2,t3,t4,t5,t6,t7,t8) values ('%s', '体育中心外场', %i, %i, %i, %i, %i, %i, %i, %i, %i)" \
% (s, pred_df.values[0,2], pred_df.values[1,2], pred_df.values[2,2], pred_df.values[3,2], pred_df.values[4,2], pred_df.values[5,2], pred_df.values[6,2], pred_df.values[7,2], pred_df.values[8,2])
cursor.execute(sql)


#更新poi_forecast_ph的值
# 更新真实值
t = str(pred_df[0:1].index[0])[11:16]
sql = "update poi_forecast_ph set real_count=%i where poi='体育中心时尚天河' and time='%s'" % (pred_df.values[0,0], t)
cursor.execute(sql)
sql = "update poi_forecast_ph set real_count=%i where poi='体育中心内场' and time='%s'" % (pred_df.values[0,1], t)
cursor.execute(sql)
sql = "update poi_forecast_ph set real_count=%i where poi='体育中心外场' and time='%s'" % (pred_df.values[0,2], t)
cursor.execute(sql)
# 更新预测值
for i in range(1,9):
	t = str(pred_df[i:i+1].index[0])[11:16]
	sql = "update poi_forecast_ph set forecast_count_%s=%i where poi='体育中心时尚天河' and time='%s'" \
	% (str(i), pred_df.values[i,0], t)
	cursor.execute(sql)
	sql = "update poi_forecast_ph set forecast_count_%s=%i where poi='体育中心内场' and time='%s'" \
	% (str(i), pred_df.values[i,1], t)
	cursor.execute(sql)
	sql = "update poi_forecast_ph set forecast_count_%s=%i where poi='体育中心外场' and time='%s'" \
	% (str(i), pred_df.values[i,2], t)
	cursor.execute(sql)

db.commit()
db.close()

