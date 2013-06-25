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
  cnx1_sql = 'TRUNCATE TABLE pinggu_xiangmufuzeren'
  cnx1_cursor.execute(cnx1_sql)
  cnx1.commit()
  print funx.get_time(), '数据表已清空'

def fuzeren(row_data):
  cnx1_sql = ('SELECT * FROM pinggu_danwei '
              'WHERE MingCheng=%s')
  cnx1_param = (row_data[9],)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1_row = cnx1_cursor.fetchone()
  danwei_id = cnx1_row[0]
  cnx1_sql = ('INSERT INTO pinggu_xiangmufuzeren '
              '(DanWeiBianHao,XingMing,ZhengJianHao,'
              'HuanPingLeiBie,ZiGeZhengBianHao,ShangGangZhengBianHao) '
              'VALUES (%s,%s,%s,%s,%s,%s)')
  cnx1_param = (danwei_id, row_data[1], row_data[3],
                row_data[4], row_data[7], row_data[8])
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()
  return cnx1_cursor.lastrowid

def xiuzheng_leibie(row_data, rowid):
  cnx2_sql = ('SELECT * '
              'FROM tbs001_basicdatasub '
              'WHERE id=%s')
  cnx2_param = (row_data[4],)
  cnx2_cursor.execute(cnx2_sql, cnx2_param)
  cnx2_row = cnx2_cursor.fetchone()
  if cnx2_row != None:
    cnx1_sql = ('UPDATE pinggu_xiangmufuzeren '
                'SET HuanPingLeiBie=%s '
                'WHERE id=%s')
    cnx1_param = (cnx2_row[2], rowid)
    cnx1_cursor.execute(cnx1_sql, cnx1_param)
    cnx1.commit()

def xiuzheng_zigezheng(rowid):
  cnx1_sql = ('SELECT * '
              'FROM pinggu_xiangmufuzeren '
              'WHERE id=%s')
  cnx1_param = (rowid,)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1_row = cnx1_cursor.fetchone()
  if cnx1_row[5] != '' and cnx1_row[5] != None:
    text = cnx1_row[5].upper().replace('附件\\', '')
    text = text.replace('CERTIFICATEPIC.JPG', '')
    text = text.replace('CERTIFICATEPIC.PNG', '')
    cnx1_sql = ('UPDATE pinggu_xiangmufuzeren '
                'SET ZiGeZhengBianHao=%s '
                'WHERE id=%s')
    cnx1_param = (text, lastid)
    cnx1_cursor.execute(cnx1_sql, cnx1_param)
    cnx1.commit()

def xiuzheng_shanggangzheng(rowid):
  cnx1_sql = ('SELECT * '
              'FROM pinggu_xiangmufuzeren '
              'WHERE id=%s')
  cnx1_param = (rowid,)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1_row = cnx1_cursor.fetchone()
  if cnx1_row[6] != '' and cnx1_row[6] != None:
    text = cnx1_row[5].upper().replace('附件\\', '')
    text = text.replace('APPOINTMENTCARDPIC.JPG', '')
    text = text.replace('APPOINTMENTCARDPIC.PNG', '')
    cnx1_sql = ('UPDATE pinggu_xiangmufuzeren '
                'SET ShangGangZhengBianHao=%s'
                'WHERE id=%s')
    cnx1_param = (text, lastid)
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

  cnx2_sql = ('SELECT * FROM tbs001_projectleader '
              'JOIN tbs001_evaluationunit '
              'ON (tbs001_projectleader.unitid='
              'tbs001_evaluationunit.id) '
              'ORDER BY tbs001_projectleader.id')
  cnx2_cursor.execute(cnx2_sql)
  cnx2_data = cnx2_cursor.fetchall()
  for cnx2_row in cnx2_data:
    lastid = fuzeren(cnx2_row)
    xiuzheng_leibie(cnx2_row, lastid)
    xiuzheng_zigezheng(lastid)
    xiuzheng_shanggangzheng(lastid)
    print funx.get_time(), '编号', lastid,'添加完毕'

  print funx.get_time(), '所有数据添加完毕'
  cnx1_cursor.close()
  cnx1.close()
  cnx2_cursor.close()
  cnx2.close()