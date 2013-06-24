#coding=utf-8

import mysql.connector

cnx1_cfg = {
  'user': 'root',
  'password': '',
  'host': '127.0.0.1',
  'database': 'emsdatabase',
}

cnx2_cfg = {
  'user': 'root',
  'password': '',
  'host': '127.0.0.1',
  'database': 'eiac-sys',
}

def export():
  cnx1 = mysql.connector.Connect(**cnx1_cfg)
  cursor1 = cnx1.cursor()
  cnx2 = mysql.connector.Connect(**cnx2_cfg)
  cursor2 = cnx2.cursor()
  sql_text1 = 'SELECT * FROM tbs001_constructionunit'
  cursor1.execute(sql_text1)
  data1 = cursor1.fetchall()

  for i in range(0, cursor1.rowcount):
    sql_text2 = ('INSERT INTO jianshe_danwei '
                 '(MingCheng, DianYou, DiZhi, YouBian, FaRen, LianXiRen, DianHua, ChuanZhen) '
                 'VALUES (%s, %s, %s, %s, %s, %s, %s, %s)')
    sql_data2 = (data1[i][1], data1[i][10], data1[i][2], data1[i][4], data1[i][8], data1[i][7], data1[i][3], data1[i][9])
    cursor2.execute(sql_text2, sql_data2)
    cnx2.commit()
    print data1[i][1]
  cursor1.close()
  cnx1.close()
  cursor2.close()
  cnx2.close()
