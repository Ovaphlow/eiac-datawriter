# -*- coding:utf-8 -*-

import sys
import mysql.connector
import funx

reload(sys)
sys.setdefaultencoding("utf-8")

cnx1_cfg = {
  'user': 'root',
  'password': 'dsdfjk',
  'host': '127.0.0.1',
  'database': 'eiac-sys',
}

cnx2_cfg = {
  'user': 'root',
  'password': 'dsdfjk',
  'host': '127.0.0.1',
  'database': 'emsdatabase',
}

def init_data():
  cnx1_sql = 'TRUNCATE TABLE staff'
  cnx1_cursor.execute(cnx1_sql)
  cnx1_sql = 'TRUNCATE TABLE authority'
  cnx1_cursor.execute(cnx1_sql)
  cnx1.commit()
  print funx.get_time(), '数据表已清空'

def staff_hlj(row_data):
  cnx1_sql = ('INSERT INTO staff '
               '(XingMing, ZhangHao, MiMa, '
               'DianHua, ZhiWu, DiQu, '
               'DanWei) '
               'VALUES (%s, %s, %s, %s, %s, %s, '
               '%s)')
  cnx1_param = (row_data[1], row_data[2], row_data[3],
               row_data[4], row_data[5], '黑龙江',
               '环评中心')
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()
  return cnx1_cursor.lastrowid

def authority(lastid):
  cnx1_sql = ('INSERT INTO authority '
              '(staff_id) '
              'VALUES (%s)')
  cnx1_param = (lastid,)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

if __name__ == '__main__':
  print funx.get_time(), '初始化...'
  cnx1 = mysql.connector.Connect(**cnx1_cfg)
  cnx1_cursor = cnx1.cursor()
  cnx2 = mysql.connector.Connect(**cnx2_cfg)
  cnx2_cursor = cnx2.cursor()
  print funx.get_time(), '数据库已连接'

  init_data()

  cnx2_sql = ('SELECT * '
              'FROM tbs001_user '
              'WHERE roleid!="建设单位" '
              'AND roleid!="环评单位"')
  cnx2_cursor.execute(cnx2_sql)
  cnx2_data = cnx2_cursor.fetchall()

  for cnx2_row in cnx2_data:
    lastid = staff_hlj(cnx2_row)
    authority(lastid)
    print funx.get_time(), '编号', lastid, '添加完毕'

  print funx.get_time(), '所有数据添加完毕'
  cnx1_cursor.close()
  cnx1.close()
  cnx2_cursor.close()
  cnx2.close()
