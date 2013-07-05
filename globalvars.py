# -*- coding:utf-8 -*-

import time
import mysql.connector

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

def get_time():
  return '[' + time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())) + ']'

def trunc_jshdw():
  sql = 'TRUNCATE TABLE jianshe_danwei'
  cnx1_cursor.execute(sql)
  cnx1.commit()

def trunc_staff():
  cnx1_sql = 'TRUNCATE TABLE staff'
  cnx1_cursor.execute(cnx1_sql)
  cnx1_sql = 'TRUNCATE TABLE authority'
  cnx1_cursor.execute(cnx1_sql)
  cnx1.commit()

print get_time(), '连接V3数据库'
cnx1 = mysql.connector.Connect(**cnx1_cfg)
cnx1_cursor = cnx1.cursor()

print get_time(), '连接V2黑龙江数据库'
cnx2 = mysql.connector.Connect(**cnx2_cfg)
cnx2_cursor = cnx2.cursor()

print get_time(), '连接V2哈尔滨数据库'
cnx3 = mysql.connector.Connect(**cnx3_cfg)
cnx3_cursor = cnx3.cursor()
