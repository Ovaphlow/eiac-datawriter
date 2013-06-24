#coding=utf-8

import mysql.connector
import funx

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
  'database': 'ems-hrb',
}

def init_data():
  sql1_text = 'TRUNCATE TABLE pinggu_danwei'
  cursor1.execute(sql1_text)
  cnx1.commit()
  print funx.get_time(), '数据表已清空'

def pinggu_danwei(row_data):
  sql1_text = ('INSERT INTO pinggu_danwei '
               '(MingCheng,ZhangHao,Mima,'
               'DianYou,FaRenDaiBiao,LianXiRen,'
               'DianHua,ChuanZhen,DiZhi,'
               'YouBian,ZhengShuBianHao,PingGuFanWei) '
               'VALUES (%s,%s,%s,%s,%s,%s,'
               '%s,%s,%s,%s,%s,%s)')
  sql1_data = (row2[0], '', '',
               row2[15], row2[13], '',
               row2[3], row2[14], row2[1],
               row2[4], row2[2], row2[5])
  cursor1.execute(sql1_text, sql1_data)
  cnx1.commit()
  return cursor1.lastrowid

if __name__ == '__main__':
  print funx.get_time(), '初始化...'
  cnx1 = mysql.connector.Connect(**cnx1_cfg)
  cursor1 = cnx1.cursor()
  cnx2 = mysql.connector.Connect(**cnx2_cfg)
  cursor2 = cnx2.cursor()
  print funx.get_time(), '数据库已连接'

  init_data()

  sql2_text = 'SELECT * FROM tbs001_evaluationunit'
  cursor2.execute(sql2_text)
  rows2 = cursor2.fetchall()

  for row2 in rows2:
    lastid = pinggu_danwei(row2)
    print funx.get_time(), '编号', lastid,'添加完毕'

  print funx.get_time(), '所有数据添加完毕'
  cursor1.close()
  cnx1.close()
  cursor2.close()
  cnx2.close()
