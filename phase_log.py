#coding=utf-8

import mysql.connector
from get_time import get_time

cnx1_cfg = {
  'user': 'root',
  'password': '',
  'host': '127.0.0.1',
  'database': 'ems-hrb',
}

cnx2_cfg = {
  'user': 'root',
  'password': '',
  'host': '127.0.0.1',
  'database': 'eiac-sys',
}

def init_data():
  sql_text2 = 'DELETE FROM phase_b_log'
  cursor2.execute(sql_text2)
  cnx2.commit()
  print get_time(), '目标数据表已清空'

def log_yushouli(row, id):
  sql_text2 = ('INSERT INTO phase_b_log '
               'VALUES (%s,%s,%s,%s,%s,%s)')
  sql_data2 = (id,
               row[55],
               '',
               row[54],
               '1',
               '预受理')
  cursor2.execute(sql_text2, sql_data2)
  cnx2.commit()

def log_shouli(row, id):
  sqlText2 = ('INSERT INTO phase_b_log '
              'VALUES (%s,%s,%s,%s,%s,%s)')
  sqlData2 = (id,
              row[57],
              '',
              row[56],
              '1',
              '受理')
  cursor2.execute(sqlText2, sqlData2)
  cnx2.commit()

def log_bumen(row, id):
  sqlText2 = ('INSERT INTO phase_b_log '
              'VALUES (%s,%s,%s,%s,%s,%s)')
  sqlData2 = (id,
              row[65],
              '',
              row[64],
              '2',
              '确认')
  cursor2.execute(sqlText2, sqlData2)
  cnx2.commit()

def log_fuzeren(row, id):
  sqlText2 = ('INSERT INTO phase_b_log '
              'VALUES (%s,%s,%s,%s,%s,%s)')
  sqlData2 = (id,
              row[70],
              '',
              row[68],
              '3',
              '确认')
  cursor2.execute(sqlText2, sqlData2)
  cnx2.commit()

def log_tacha(row, id):
  sqlText2 = ('INSERT INTO phase_b_log '
              'VALUES (%s,%s,%s,%s,%s,%s)')
  sqlData2 = (id,
              row[78],
              '',
              row[77],
              '4',
              '确认')
  cursor2.execute(sqlText2, sqlData2)
  cnx2.commit()

def log_huiyi(row, id):
  sqlText2 = ('INSERT INTO phase_b_log '
              'VALUES (%s,%s,%s,%s,%s,%s)')
  sqlData2 = (id,
              row[86],
              '',
              row[85],
              '5',
              '确认')
  cursor2.execute(sqlText2, sqlData2)
  cnx2.commit()

def log_pinggu(row, id):
  sqlText2 = ('INSERT INTO phase_b_log '
              'VALUES (%s,%s,%s,%s,%s,%s)')
  sqlData2 = (id,
              row[88],
              '',
              row[87],
              '6',
              '确认')
  cursor2.execute(sqlText2, sqlData2)
  cnx2.commit()

def log_kaoping(row, id):
  sqlText2 = ('INSERT INTO phase_b_log '
              'VALUES (%s,%s,%s,%s,%s,%s)')
  sqlData2 = (id,
              row[95],
              '',
              row[94],
              '7',
              '确认')
  cursor2.execute(sqlText2, sqlData2)
  cnx2.commit()

def log_shangbao(row, id):
  sqlText2 = ('INSERT INTO phase_b_log '
              'VALUES (%s,%s,%s,%s,%s,%s)')
  sqlData2 = (id,
              row[115],
              '',
              row[112],
              '8',
              '确认')
  cursor2.execute(sqlText2, sqlData2)
  cnx2.commit()

if __name__ == '__main__':
  print get_time(), '初始化...'
  cnx1 = mysql.connector.Connect(**cnx1_cfg)
  cursor1 = cnx1.cursor()
  print get_time(), '源数据库已连接'

  cnx2 = mysql.connector.Connect(**cnx2_cfg)
  cursor2 = cnx2.cursor()
  get_time()
  print get_time(), '目标数据库已连接'

  init_data()

  sql_text1 = 'SELECT * FROM tbs001_developprojectbasicinfo'
  cursor1.execute(sql_text1)
  data1 = cursor1.fetchall()
  print get_time(), '源数据数:', cursor1.rowcount, '开始添加数据到目标数据表'

  for rowData in data1:
    sql_text2 = 'SELECT * FROM phase_a1 WHERE XiangMuMingCheng="' + rowData[0] + '"'
    cursor2.execute(sql_text2)
    data2 = cursor2.fetchall()
    pid = data2[0][0]
    if cursor2.rowcount != 1:
      print get_time(), p_name, '数据出错！'
    log_yushouli(rowData, pid)
    log_shouli(rowData, pid)
    log_bumen(rowData, pid)
    log_fuzeren(rowData, pid)
    log_tacha(rowData, pid)
    log_huiyi(rowData, pid)
    log_pinggu(rowData, pid)
    log_kaoping(rowData, pid)
    log_shangbao(rowData, pid)
    print get_time(), rowData[0], '处理完毕'

  print get_time(), '所有数据添加完毕', '准备删除无效数据'
  sqlText2 = 'DELETE FROM phase_b_log WHERE account is NULL'
  cursor2.execute(sqlText2)
  cnx2.commit()
  print get_time(), '无效数据清理完毕'
  cursor1.close()
  cnx1.close()
  cursor2.close()
  cnx2.close()
