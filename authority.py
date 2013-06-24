#encoding=utf-8

import sys
import mysql.connector
import get_time

reload(sys)
sys.setdefaultencoding('utf-8')

cnx_cfg = {
  'user': 'root',
  'password': 'dsdfjk',
  'host': '127.0.0.1',
  'database': 'eiac-sys',
}

def init_data():
  sql_text = 'DELETE FROM authority'
  cursor.execute(sql_text)
  cnx.commit()
  print get_time.get_time(), '数据表已清空'

if __name__ == '__main__':
  print get_time.get_time(), '初始化...'
  cnx = mysql.connector.Connect(**cnx_cfg)
  cursor = cnx.cursor()
  print get_time.get_time(), '数据库已连接'

  init_data()

  sql_text = 'SELECT * FROM staff'
  cursor.execute(sql_text)
  rows = cursor.fetchall()
  for row in rows:
    sql_text = 'INSERT INTO authority (staff_id) VALUES (' + str(row[0]) + ')'
    cursor.execute(sql_text)
    cnx.commit()
    print get_time.get_time(), row[1].decode("utf8")

  cursor.close()
  cnx.close()