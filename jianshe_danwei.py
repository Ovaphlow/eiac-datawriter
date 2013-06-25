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
  cnx1_sql = 'TRUNCATE TABLE jianshe_danwei'
  cnx1_cursor.execute(cnx1_sql)
  cnx1.commit()
  print funx.get_time(), '数据表已清空'

def jianshe_danwei(row_data):
  cnx1_sql = ('INSERT INTO jianshe_danwei '
               '(MingCheng, DianYou, DiZhi, YouBian, FaRen, '
               'LianXiRen, DianHua, ChuanZhen) '
               'VALUES (%s, %s, %s, %s, %s, %s, %s, %s)')
  cnx1_param = (row_data[1], row_data[10], row_data[2], 
               row_data[4], row_data[8], row_data[7], 
               row_data[3], row_data[9])
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()
  return cnx1_cursor.lastrowid

if __name__ == '__main__':
  print funx.get_time(), '初始化...'
  cnx1 = mysql.connector.Connect(**cnx1_cfg)
  cnx1_cursor = cnx1.cursor()
  cnx2 = mysql.connector.Connect(**cnx2_cfg)
  cnx2_cursor = cnx2.cursor()
  print funx.get_time(), '数据库已连接'

  init_data()

  cnx2_sql = 'SELECT * FROM tbs001_constructionunit'
  cnx2_cursor.execute(cnx2_sql)
  cnx2_data = cnx2_cursor.fetchall()

  for cnx2_row in cnx2_data:
    lastid = jianshe_danwei(cnx2_row)
    print funx.get_time(), '编号', lastid, '添加完毕'

  print funx.get_time(), '所有数据添加完毕'
  cnx1_cursor.close()
  cnx1.close()
  cnx2_cursor.close()
  cnx2.close()
