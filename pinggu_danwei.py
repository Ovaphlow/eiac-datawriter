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

def insert_data_hlj():
  cnx2_sql = 'SELECT * FROM tbs001_evaluationunit'
  cnx2_cursor.execute(cnx2_sql)
  cnx2_data = cnx2_cursor.fetchall()

  for cnx2_row in cnx2_data:
    cnx1_sql = ('INSERT INTO pinggu_danwei '
                 '(MingCheng,ZhangHao,Mima,'
                 'DianYou,FaRen,LianXiRen,'
                 'DianHua,ChuanZhen,DiZhi,'
                 'YouBian,ZhengShuBianHao,PingGuFanWei) '
                 'VALUES (%s,%s,%s,%s,%s,%s,'
                 '%s,%s,%s,%s,%s,%s)')
    cnx1_param = (cnx2_row[0], '', '',
                 '', '', '',
                 cnx2_row[3], '', cnx2_row[1],
                 cnx2_row[4], cnx2_row[2], cnx2_row[5])
    cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def insert_data_hrb():
  cnx3_sql = 'SELECT * FROM tbs001_evaluationunit'
  cnx3_cursor.execute(cnx3_sql)
  cnx3_data = cnx3_cursor.fetchall()
  for cnx3_row in cnx3_data:
    cnx1_sql = ('INSERT INTO pinggu_danwei '
                 '(MingCheng,ZhangHao,Mima,'
                 'DianYou,FaRen,LianXiRen,'
                 'DianHua,ChuanZhen,DiZhi,'
                 'YouBian,ZhengShuBianHao,PingGuFanWei) '
                 'VALUES (%s,%s,%s,%s,%s,%s,'
                 '%s,%s,%s,%s,%s,%s)')
    cnx1_param = (cnx3_row[0], '', '',
                 cnx3_row[15], cnx3_row[13], '',
                 cnx3_row[3], cnx3_row[14], cnx3_row[1],
                 cnx3_row[4], cnx3_row[2], cnx3_row[5])
    cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def update_data_acc():
  cnx1_sql = 'SELECT id FROM pinggu_danwei'
  cnx1_cursor.execute(cnx1_sql)
  cnx1_data = cnx1_cursor.fetchall()
  for cnx1_row in cnx1_data:
    cnx1_sql = ('UPDATE pinggu_danwei '
                'SET '
                'ZhangHao=%s,MiMa=%s '
                'WHERE id=%s')
    cnx1_param = (cnx1_row[0], '123456', cnx1_row[0])
    cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

if __name__ == '__main__':
  print funx.get_time(), '连接V3数据库'
  cnx1 = mysql.connector.Connect(**cnx1_cfg)
  cnx1_cursor = cnx1.cursor()

  print funx.get_time(), '清空V3数据表'
  cnx1_sql = 'TRUNCATE TABLE pinggu_danwei'
  cnx1_cursor.execute(cnx1_sql)
  cnx1.commit()

  print funx.get_time(), '连接V2黑龙江数据库'
  cnx2 = mysql.connector.Connect(**cnx2_cfg)
  cnx2_cursor = cnx2.cursor()

  print funx.get_time(), '添加V2黑龙江数据'
  insert_data_hlj()
  cnx2_cursor.close()
  cnx2.close()

  print funx.get_time(), '连接V2哈尔滨数据库'
  cnx3 = mysql.connector.Connect(**cnx3_cfg)
  cnx3_cursor = cnx3.cursor()

  print funx.get_time(), '添加V2哈尔滨数据'
  insert_data_hrb()
  cnx3_cursor.close()
  cnx3.close()

  print funx.get_time(), '添加评估单位帐号和密码'
  update_data_acc()

  print funx.get_time(), '所有数据添加完毕，关闭数据库链接'
  cnx1_cursor.close()
  cnx1.close()
