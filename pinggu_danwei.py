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
  sql_text1 = 'SELECT * FROM emsdatabase.tbs001_evaluationunit'
  cursor1.execute(sql_text1)
  data1 = cursor1.fetchall()

  for i in range(0, cursor1.rowcount):
    var_mingcheng = data1[i][0]
    var_dizhi = data1[i][1]
    var_zhengshubianhao = data1[i][2]
    var_dianhua = data1[i][3]
    var_youbian = data1[i][4]
    var_pinggufanwei = data1[i][5]
    sql_text2 = 'INSERT INTO pinggu_danwei (MingCheng,DiZhi,ZhengShuBianHao,DianHua,YouBian,PingGuFanWei) VALUES ("' + var_mingcheng + '","'
    sql_text2 += var_dizhi + '","' + var_zhengshubianhao + '","' + var_dianhua + '","' + var_youbian + '","' + var_pinggufanwei + '")'
    cursor2.execute(sql_text2)
    cnx2.commit()
    print sql_text2
  cursor1.close()
  cnx1.close()
  cursor2.close()
  cnx2.close()
