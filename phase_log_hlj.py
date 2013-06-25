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
  cnx1_sql = 'TRUNCATE TABLE phase_b_log'
  cnx1_cursor.execute(cnx1_sql)
  cnx1.commit()
  print funx.get_time(), '目标数据表已清空'

def log_yushouli(row_data, id):
  cnx1_sql = ('INSERT INTO phase_b_log '
               'VALUES (%s,%s,%s,%s,%s,%s)')
  cnx1_param = (id, row_data[55], '',
                row_data[54], '1', '预受理')
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def log_shouli(row_data, id):
  cnx1_sql = ('INSERT INTO phase_b_log '
              'VALUES (%s,%s,%s,%s,%s,%s)')
  cnx1_param = (id, row_data[57], '',
                row_data[56], '1', '受理')
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def log_bumen(row_data, id):
  cnx1_sql = ('INSERT INTO phase_b_log '
              'VALUES (%s,%s,%s,%s,%s,%s)')
  cnx1_param = (id, row_data[65], '',
                row_data[64], '2', '确认')
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def log_fuzeren(row_data, id):
  cnx1_sql = ('INSERT INTO phase_b_log '
              'VALUES (%s,%s,%s,%s,%s,%s)')
  cnx1_param = (id, row_data[70], '',
                row_data[68], '3', '确认')
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def log_tacha(row_data, id):
  cnx1_sql = ('INSERT INTO phase_b_log '
              'VALUES (%s,%s,%s,%s,%s,%s)')
  cnx1_param = (id, row_data[78], '',
                row_data[77], '4', '确认')
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def log_huiyi(row_data, id):
  cnx1_sql = ('INSERT INTO phase_b_log '
              'VALUES (%s,%s,%s,%s,%s,%s)')
  cnx1_param = (id, row_data[86], '',
                row_data[85], '5', '确认')
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def log_pinggu(row_data, id):
  cnx1_sql = ('INSERT INTO phase_b_log '
              'VALUES (%s,%s,%s,%s,%s,%s)')
  cnx1_param = (id, row_data[88], '',
                row_data[87], '6', '确认')
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def log_kaoping(row_data, id):
  cnx1_sql = ('INSERT INTO phase_b_log '
              'VALUES (%s,%s,%s,%s,%s,%s)')
  cnx1_param = (id, row_data[95], '',
                row_data[94], '7', '确认')
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def log_shangbao(row_data, id):
  cnx1_sql = ('INSERT INTO phase_b_log '
              'VALUES (%s,%s,%s,%s,%s,%s)')
  cnx1_param = (id, row_data[115], '',
                row_data[112], '8', '确认')
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

if __name__ == '__main__':
  print funx.get_time(), '初始化...'
  cnx1 = mysql.connector.Connect(**cnx1_cfg)
  cnx1_cursor = cnx1.cursor()
  cnx2 = mysql.connector.Connect(**cnx2_cfg)
  cnx2_cursor = cnx2.cursor()
  funx.get_time()
  print funx.get_time(), '数据库已连接'

  init_data()

  cnx2_sql = 'SELECT * FROM tbs001_developprojectbasicinfo'
  cnx2_cursor.execute(cnx2_sql)
  cnx2_data = cnx2_cursor.fetchall()
  print funx.get_time(), '源数据数:', cnx2_cursor.rowcount, '开始添加数据到目标数据表'

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

  print funx.get_time(), '所有数据添加完毕', '准备删除无效数据'
  cnx1_sql = 'DELETE FROM phase_b_log WHERE account is NULL'
  cnx1_cursor.execute(cnx1_sql)
  cnx1.commit()
  print funx.get_time(), '无效数据清理完毕'
  cnx1_cursor.close()
  cnx1.close()
  cnx2_cursor.close()
  cnx2.close()
