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
  cnx2_sql = ('SELECT * FROM tbs001_projectleader '
              'JOIN tbs001_evaluationunit '
              'ON (tbs001_projectleader.unitid='
              'tbs001_evaluationunit.id) '
              'ORDER BY tbs001_projectleader.id')
  cnx2_cursor.execute(cnx2_sql)
  cnx2_data = cnx2_cursor.fetchall()

  for cnx2_row in cnx2_data:
    cnx1_sql = ('SELECT * FROM pinggu_danwei '
                'WHERE MingCheng=%s')
    cnx1_param = (cnx2_row[9],)
    cnx1_cursor.execute(cnx1_sql, cnx1_param)
    cnx1_row = cnx1_cursor.fetchall()
    danwei_id = cnx1_row[0][0]
    cnx1_sql = ('INSERT INTO pinggu_xiangmufuzeren '
                '(DanWeiBianHao,XingMing,ZhengJianHao,'
                'HuanPingLeiBie,ZiGeZhengBianHao,ShangGangZhengBianHao) '
                'VALUES (%s,%s,%s,%s,%s,%s)')
    cnx1_param = (danwei_id, cnx2_row[1], cnx2_row[3],
                  cnx2_row[4], cnx2_row[7], cnx2_row[8])
    cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def insert_data_hrb():
  cnx3_sql = ('SELECT * FROM tbs001_projectleader '
              'JOIN tbs001_evaluationunit '
              'ON (tbs001_projectleader.unitid='
              'tbs001_evaluationunit.id) '
              'ORDER BY tbs001_projectleader.id')
  cnx3_cursor.execute(cnx3_sql)
  cnx3_data = cnx3_cursor.fetchall()

  for cnx3_row in cnx3_data:
    cnx1_sql = ('SELECT * FROM pinggu_danwei '
                'WHERE MingCheng=%s')
    cnx1_param = (cnx3_row[9],)
    cnx1_cursor.execute(cnx1_sql, cnx1_param)
    cnx1_row = cnx1_cursor.fetchall()
    danwei_id = cnx1_row[0][0]
    cnx1_sql = ('INSERT INTO pinggu_xiangmufuzeren '
                '(DanWeiBianHao,XingMing,ZhengJianHao,'
                'HuanPingLeiBie,ZiGeZhengBianHao,ShangGangZhengBianHao) '
                'VALUES (%s,%s,%s,%s,%s,%s)')
    cnx1_param = (danwei_id, cnx3_row[1], cnx3_row[3],
                  cnx3_row[4], cnx3_row[7], cnx3_row[8])
    cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def update_leibie():
  cnx1_sql = 'SELECT * FROM pinggu_xiangmufuzeren'
  cnx1_cursor.execute(cnx1_sql)
  cnx1_data = cnx1_cursor.fetchall()
  for cnx1_row in cnx1_data:
    cnx3_sql = ('SELECT * '
                'FROM tbs001_basicdatasub '
                'WHERE id=%s')
    cnx3_param = (cnx1_row[4],)
    cnx3_cursor.execute(cnx3_sql, cnx3_param)
    cnx3_row = cnx3_cursor.fetchone()
    if cnx3_row != None:
      cnx1_sql = ('UPDATE pinggu_xiangmufuzeren '
                  'SET HuanPingLeiBie=%s '
                  'WHERE id=%s')
      cnx1_param = (cnx3_row[2], cnx1_row[0])
      cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def update_zigezheng():
  cnx1_sql = 'SELECT * FROM pinggu_xiangmufuzeren'
  cnx1_cursor.execute(cnx1_sql)
  cnx1_data = cnx1_cursor.fetchall()
  for cnx1_row in cnx1_data:
    if cnx1_row[5] != '' and cnx1_row[5] != None:
      text = cnx1_row[5].upper().replace('附件\\', '')
      text = text.replace('CERTIFICATEPIC.JPG', '')
      text = text.replace('CERTIFICATEPIC.PNG', '')
      cnx1_sql = ('UPDATE pinggu_xiangmufuzeren '
                  'SET ZiGeZhengBianHao=%s '
                  'WHERE id=%s')
      cnx1_param = (text, cnx1_row[0])
      cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def update_shanggangzheng():
  cnx1_sql = 'SELECT * FROM pinggu_xiangmufuzeren'
  cnx1_cursor.execute(cnx1_sql)
  cnx1_data = cnx1_cursor.fetchall()
  for cnx1_row in cnx1_data:
    if cnx1_row[6] != '' and cnx1_row[6] != None:
      text = cnx1_row[6].upper().replace('附件\\', '')
      text = text.replace('APPOINTMENTCARDPIC.JPG', '')
      text = text.replace('APPOINTMENTCARDPIC.PNG', '')
      cnx1_sql = ('UPDATE pinggu_xiangmufuzeren '
                  'SET ShangGangZhengBianHao=%s'
                  'WHERE id=%s')
      cnx1_param = (text, cnx1_row[0])
      cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

if __name__ == '__main__':
  print funx.get_time(), '连接V3数据库'
  cnx1 = mysql.connector.Connect(**cnx1_cfg)
  cnx1_cursor = cnx1.cursor()

  print funx.get_time(), '清空V3数据表'
  cnx1_sql = 'TRUNCATE TABLE pinggu_xiangmufuzeren'
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

  print funx.get_time(), '修正评估类别'
  update_leibie()

  print funx.get_time(), '修正资格证编号'
  update_zigezheng()

  print funx.get_time(), '修正上岗证编号'
  update_shanggangzheng()

  print funx.get_time(), '所有数据添加完毕'
  cnx1_cursor.close()
  cnx1.close()
  cnx2_cursor.close()
  cnx2.close()
  cnx3_cursor.close()
  cnx3.close()
