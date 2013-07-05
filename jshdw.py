# -*- coding:utf-8 -*-

import globalvars

def run():
  print globalvars.get_time(), '清空V3建设单位表'
  globalvars.trunc_jshdw()

  print globalvars.get_time(), '添加建设单位数据（黑龙江）'
  insert_data_hlj()
  print globalvars.get_time(), '添加建设单位数据（哈尔滨）'
  insert_data_hrb()

def insert_data_hlj():
  sql = 'SELECT * FROM tbs001_constructionunit'
  globalvars.cnx2_cursor.execute(sql)
  data = globalvars.cnx2_cursor.fetchall()

  for row in data:
    sql = ('INSERT INTO jianshe_danwei '
           '(MingCheng, DianYou, DiZhi, YouBian, FaRen, '
           'LianXiRen, DianHua, ChuanZhen) '
           'VALUES (%s, %s, %s, %s, %s, %s,'
           '%s, %s)')
    param = (row[1], row[10], row[2], row[4], row[8], row[7],
             row[3], row[9])
    globalvars.cnx1_cursor.execute(sql, param)
  globalvars.cnx1.commit()

def insert_data_hrb():
  sql = 'SELECT * FROM tbs001_constructionunit'
  globalvars.cnx3_cursor.execute(sql)
  data = globalvars.cnx3_cursor.fetchall()

  for row in data:
    sql = ('INSERT INTO jianshe_danwei '
           '(MingCheng, DianYou, DiZhi, YouBian, FaRen, '
           'LianXiRen, DianHua, ChuanZhen) '
           'VALUES (%s, %s, %s, %s, %s, %s,'
           '%s, %s)')
    param = (row[1], row[10], row[2], row[4], row[8], row[7],
             row[3], row[9])
    globalvars.cnx1_cursor.execute(sql, param)
  globalvars.cnx1.commit()
