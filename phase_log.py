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

cnx3_cfg = {
  'user': 'root',
  'password': 'dsdfjk',
  'host': '127.0.0.1',
  'database': 'ems-hrb',
}

def init_data():
  cnx1_sql = 'TRUNCATE TABLE phase_b_log'
  cnx1_cursor.execute(cnx1_sql)
  cnx1.commit()

def insert_data_hlj():
  cnx2_sql = 'SELECT * FROM tbs001_developprojectbasicinfo'
  cnx2_cursor.execute(cnx2_sql)
  cnx2_data = cnx2_cursor.fetchall()

  for cnx2_row in cnx2_data:
    cnx1_sql = ('SELECT * FROM phase_a1 '
                'WHERE XiangMuMingCheng=%s')
    cnx1_param = (cnx2_row[0],)
    cnx1_cursor.execute(cnx1_sql, cnx1_param)
    cnx1_data = cnx1_cursor.fetchall()
    if cnx1_cursor.rowcount == 0:
      continue
    elif cnx1_cursor.rowcount == 1:
      pid = cnx1_data[0][0]
    else:
      print funx.get_time(), '数据出错！'
      pid = cnx1_data[0][0]
    log_yushouli(cnx2_row, pid)
    log_shouli(cnx2_row, pid)
    log_bumen(cnx2_row, pid)
    log_fuzeren(cnx2_row, pid)
    log_tacha(cnx2_row, pid)
    log_huiyi(cnx2_row, pid)
    log_pinggu(cnx2_row, pid)
    log_kaoping(cnx2_row, pid)
    log_shangbao(cnx2_row, pid)
    print funx.get_time(), '编号', pid,'添加完毕'

if __name__ == '__main__':
  print funx.get_time(), '连接V3数据库'
  cnx1 = mysql.connector.Connect(**cnx1_cfg)
  cnx1_cursor = cnx1.cursor()

  print funx.get_time(), '清空V3数据表'
  init_data()

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

  print funx.get_time(), '所有数据添加完毕，关闭数据库链接'
  cnx1_cursor.close()
  cnx1.close()
  cnx2_cursor.close()
  cnx2.close()
  cnx3_cursor.close()
  cnx3.close()
