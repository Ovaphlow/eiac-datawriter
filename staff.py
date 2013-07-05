# -*- coding:utf-8 -*-

import globalvars

def run():
  print globalvars.get_time(), '清空staff表'
  globalvars.trunc_staff()

  print globalvars.get_time(), '添加staff表数据（黑龙江）'
  insert_data_hlj()
  print globalvars.get_time(), '添加staff表数据（哈尔滨）'
  insert_data_hrb()

  print globalvars.get_time(), '添加authority表数据'
  authority()

def insert_data_hlj():
  cursor = globalvars.cnx2.cursor()
  sql = ('SELECT * '
         'FROM tbs001_user '
         'WHERE roleid!="建设单位" '
         'AND roleid!="环评单位"')
  cursor.execute(sql)
  data = cursor.fetchall()
  cursor.close()

  cursor = globalvars.cnx1.cursor()
  for row in data:
    sql = ('INSERT INTO staff '
           '(XingMing, ZhangHao, MiMa, '
           'DianHua, ZhiWu, DiQu, '
           'DanWei) '
           'VALUES (%s, %s, %s, %s, %s, %s,'
           '%s)')
    param = (row[1], row[2], row[3], row[4], row[5], '黑龙江',
             '环评中心')
    cursor.execute(sql, param)

  globalvars.cnx1.commit()

def insert_data_hrb():
  cursor = globalvars.cnx3.cursor()
  sql = ('SELECT * '
         'FROM tbs001_user '
         'WHERE roleid!="建设单位" '
         'AND roleid!="环评单位"')
  cursor.execute(sql)
  data = cursor.fetchall()
  cursor.close()

  cursor = globalvars.cnx1.cursor()
  for row in data:
    sql = ('INSERT INTO staff '
           '(XingMing, ZhangHao, MiMa, '
           'DianHua, ZhiWu, DiQu, '
           'DanWei) '
           'VALUES (%s, %s, %s, %s, %s, %s,'
           '%s)')
    param = (row[1], row[2], row[3], row[4], row[5], '哈尔滨',
             '环评中心')
    cursor.execute(sql, param)

  globalvars.cnx1.commit()

def authority():
  cursor = globalvars.cnx1.cursor()
  sql = 'SELECT id FROM staff'
  cursor.execute(sql)
  data = cursor.fetchall()

  for row in data:
    sql = ('INSERT INTO authority '
           '(staff_id) '
           'VALUES (%s)')
    param = (row[0],)
    cursor.execute(sql, param)

  globalvars.cnx1.commit()
