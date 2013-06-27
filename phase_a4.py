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
  cnx1_sql = 'TRUNCATE TABLE phase_a4'
  cnx1_cursor.execute(cnx1_sql)
  cnx1.commit()
  print funx.get_time(), '数据表已清空'

def phase_a4_hlj_insert(row_data, project_id):
  cnx1_sql = ('INSERT INTO phase_a4 '
              'VALUES (%s,%s,%s,%s,%s,%s,'
              '%s,%s,%s,%s,%s,%s,'
              '%s,%s,%s,%s,%s,%s)')
  cnx1_param = (project_id, row_data[2], row_data[3], 
                row_data[4], row_data[5], row_data[6], 
                row_data[7], row_data[8], row_data[9], 
                row_data[10], row_data[11], row_data[12], 
                row_data[13], row_data[14], row_data[15], 
                row_data[16], row_data[17], row_data[18],)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()
  return cnx1_cursor.lastrowid

def phase_a4_hlj_update(row_data, project_id):
  cnx1_sql = ('UPDATE phase_a4 '
              'SET '
              'HJZhLDJ_1=%s,HJZhLDJ_2=%s,'
              'HJZhLDJ_3=%s,HJZhLDJ_4=%s,HJZhLDJ_5=%s,'
              'HJZhLDJ_6=%s,HJZhLDJ_7=%s,HJMGTZh=%s,'
              'HJYXQY_1=%s,HJYXQY_2=%s,HJYXQY_3=%s,'
              'HJYXQY_4=%s,HJYXQY_5=%s,HJYXQY_6=%s,'
              'HJYXQY_7=%s,HJYXQY_8=%s,HJYXQY_9=%s '
              'WHERE id=%s')
  cnx1_param = (row_data[2], row_data[3], 
                row_data[4], row_data[5], row_data[6], 
                row_data[7], row_data[8], row_data[9], 
                row_data[10], row_data[11], row_data[12], 
                row_data[13], row_data[14], row_data[15], 
                row_data[16], row_data[17], row_data[18],
                project_id)
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

  cnx2_sql = ('SELECT * '
              'FROM tbs001_huanjing '
              'ORDER BY id')
  cnx2_cursor.execute(cnx2_sql)
  cnx2_data = cnx2_cursor.fetchall()

  for cnx2_row in cnx2_data:
    cnx1_sql = ('SELECT * '
                'FROM phase_a1 '
                'WHERE XiangMuMingCheng=%s')
    cnx1_param = (cnx2_row[1],)
    cnx1_cursor.execute(cnx1_sql, cnx1_param)
    cnx1_data = cnx1_cursor.fetchall()
    if cnx1_cursor.rowcount == 0:
      continue
    else:
      project_id = cnx1_data[0][0]
      cnx1_sql = 'SELECT COUNT(*) FROM phase_a4 WHERE id=%s'
      cnx1_param = (project_id,)
      cnx1_cursor.execute(cnx1_sql, cnx1_param)
      cnx1_row = cnx1_cursor.fetchone()
      if cnx1_row[0] == 0:
        lastid = phase_a4_hlj_insert(cnx2_row, project_id)
      else:
        lastid = phase_a4_hlj_update(cnx2_row, project_id)
      print funx.get_time(), '编号', project_id, '添加完毕'

  print funx.get_time(), '所有数据添加完毕'
  cnx1_cursor.close()
  cnx1.close()
  cnx2_cursor.close()
  cnx2.close()