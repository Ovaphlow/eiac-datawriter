# -*- coding:utf-8 -*-

import globalvars

def run():
  print globalvars.get_time(), '清空V3建设单位表'
  globalvars.trunc_staff()

  print globalvars.get_time(), '添加staff表数据（黑龙江）'
  insert_data_hlj()
  print globalvars.get_time(), '添加staff表数据（哈尔滨）'
  insert_data_hrb()

  print globalvars.get_time(), '添加authority表数据'
  authority()

def insert_data_hlj():
  sql = ('SELECT * '
         'FROM tbs001_user '
         'WHERE roleid!="建设单位" '
         'AND roleid!="环评单位"')
  globalvars.cnx2_cursor.execute(sql)
  data = globalvars.cnx2_cursor.fetchall()

  for row in data:
    sql = ('INSERT INTO staff '
           '(XingMing, ZhangHao, MiMa, '
           'DianHua, ZhiWu, DiQu, '
           'DanWei) '
           'VALUES (%s, %s, %s, %s, %s, %s,'
           '%s)')
    param = (row[1], row[2], row[3], row[4], row[5], '黑龙江',
             '环评中心')
    globalvars.cnx1_cursor.execute(sql, param)
  globalvars.cnx1.commit()

def insert_data_hrb():
  sql = ('SELECT * '
         'FROM tbs001_user '
         'WHERE roleid!="建设单位" '
         'AND roleid!="环评单位"')
  globalvars.cnx3_cursor.execute(sql)
  data = globalvars.cnx3_cursor.fetchall()

  for row in data:
    sql = ('INSERT INTO staff '
           '(XingMing, ZhangHao, MiMa, '
           'DianHua, ZhiWu, DiQu, '
           'DanWei) '
           'VALUES (%s, %s, %s, %s, %s, %s,'
           '%s)')
    param = (row[1], row[2], row[3], row[4], row[5], '哈尔滨',
             '环评中心')
    globalvars.cnx1_cursor.execute(sql, param)
  globalvars.cnx1.commit()

def authority():
  sql = 'SELECT id FROM staff'
  globalvars.cnx1_cursor.execute(sql)
  data = globalvars.cnx1_cursor.fetchall()
  for row in data:
    sql = ('INSERT INTO authority '
           '(staff_id) '
           'VALUES (%s)')
    param = (row[0],)
    globalvars.cnx1_cursor.execute(sql, param)
  globalvars.cnx1.commit()

if __name__ == '__main__':
  print funx.get_time(), '连接V3数据库'
  cnx1 = mysql.connector.Connect(**cnx1_cfg)
  cnx1_cursor = cnx1.cursor()

  print funx.get_time(), '清空V3数据表'
  cnx1_sql = 'TRUNCATE TABLE staff'
  cnx1_cursor.execute(cnx1_sql)
  cnx1_sql = 'TRUNCATE TABLE authority'
  cnx1_cursor.execute(cnx1_sql)
  cnx1.commit()

  print funx.get_time(), '连接V2黑龙江数据库'
  cnx2 = mysql.connector.Connect(**cnx2_cfg)
  cnx2_cursor = cnx2.cursor()

  print funx.get_time(), '添加V2黑龙江数据'
  insert_data_hlj()

  print funx.get_time(), '连接V2哈尔滨数据库'
  cnx3 = mysql.connector.Connect(**cnx3_cfg)
  cnx3_cursor = cnx3.cursor()

  print funx.get_time(), '添加V2哈尔滨数据'
  insert_data_hrb()

  print funx.get_time(), '添加authority表数据'
  authority()

  print funx.get_time(), '所有数据添加完毕'
  cnx1_cursor.close()
  cnx1.close()
  cnx2_cursor.close()
  cnx2.close()
  cnx3_cursor.close()
  cnx3.close()
