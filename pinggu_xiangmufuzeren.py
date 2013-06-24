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
  'database': 'ems-hrb',
}

def init_data():
  sql1_text = 'TRUNCATE TABLE pinggu_xiangmufuzeren'
  cursor1.execute(sql1_text)
  cnx1.commit()
  print funx.get_time(), '数据表已清空'

def fuzeren(row_data):
  sql1_text = ('SELECT * FROM pinggu_danwei WHERE MingCheng="' + row_data[9] + '"')
  cursor1.execute(sql1_text)
  row1 = cursor1.fetchone()
  danwei_id = row1[0]
  sql1_text = ('INSERT INTO pinggu_xiangmufuzeren '
               '(DanWeiBianHao'
                ',XingMing'
                ',ZhengJianHao'
                ',HuanPingLeiBie'
                ',ZiGeZhengBianHao'
                ',ShangGangZhengBianHao)'
                'VALUES (%s,%s,%s,%s,%s,%s)')
  sql1_data = (danwei_id,
               row_data[1],
               row_data[3],
               row_data[4],
               row_data[7],
               row_data[8])
  cursor1.execute(sql1_text, sql1_data)
  cnx1.commit()
  return cursor1.lastrowid

def xiuzheng_leibie(row_data, lastid):
  sql2_text = ('SELECT * '
               'FROM tbs001_basicdatasub '
               'WHERE id='
               '%s')
  sql2_data = (row_data[4],)
  cursor2.execute(sql2_text, sql2_data)
  row = cursor2.fetchone()
  if row != None:
    sql1_text = ('UPDATE pinggu_xiangmufuzeren '
                 'SET HuanPingLeiBie='
                 '%s '
                 'WHERE id='
                 '%s')
    sql1_data = (row[2], lastid)
    cursor1.execute(sql1_text, sql1_data)
    cnx1.commit()

def xiuzheng_zigezheng(lastid):
  sql1_text = ('SELECT * '
               'FROM pinggu_xiangmufuzeren '
               'WHERE id=%s')
  sql1_data = (lastid,)
  cursor1.execute(sql1_text, sql1_data)
  row = cursor1.fetchone()
  if row[5] != '' and row[5] != None:
    text = row[5].upper().replace('附件\\', '')
    text = text.replace('CERTIFICATEPIC.JPG', '')
    text = text.replace('CERTIFICATEPIC.PNG', '')
    sql1_text = ('UPDATE pinggu_xiangmufuzeren '
                 'SET ZiGeZhengBianHao=%s'
                 'WHERE id=%s')
    sql1_data = (text, lastid)
    cursor1.execute(sql1_text, sql1_data)
    cnx1.commit()

def xiuzheng_shanggangzheng(lastid):
  sql1_text = ('SELECT * '
               'FROM pinggu_xiangmufuzeren '
               'WHERE id=%s')
  sql1_data = (lastid,)
  cursor1.execute(sql1_text, sql1_data)
  row = cursor1.fetchone()
  if row[6] != '' and row[6] != None:
    text = row[5].upper().replace('附件\\', '')
    text = text.replace('APPOINTMENTCARDPIC.JPG', '')
    text = text.replace('APPOINTMENTCARDPIC.PNG', '')
    sql1_text = ('UPDATE pinggu_xiangmufuzeren '
                 'SET ShangGangZhengBianHao=%s'
                 'WHERE id=%s')
    sql1_data = (text, lastid)
    cursor1.execute(sql1_text, sql1_data)
    cnx1.commit()

if __name__ == '__main__':
  print funx.get_time(), '初始化...'
  cnx1 = mysql.connector.Connect(**cnx1_cfg)
  cursor1 = cnx1.cursor()
  cnx2 = mysql.connector.Connect(**cnx2_cfg)
  cursor2 = cnx2.cursor()
  print funx.get_time(), '数据库已连接'

  init_data()

  sql2_text = ('SELECT * FROM tbs001_projectleader '
               'JOIN tbs001_evaluationunit '
               'ON (tbs001_projectleader.unitid='
               'tbs001_evaluationunit.id) '
               'ORDER BY tbs001_projectleader.id')
  cursor2.execute(sql2_text)
  rows2 = cursor2.fetchall()
  for row2 in rows2:
    lastid = fuzeren(row2)
    xiuzheng_leibie(row2, lastid)
    xiuzheng_zigezheng(lastid)
    xiuzheng_shanggangzheng(lastid)
    print funx.get_time(), '编号', lastid,'添加完毕'

  print funx.get_time(), '所有数据添加完毕'
  cursor1.close()
  cnx1.close()
  cursor2.close()
  cnx2.close()