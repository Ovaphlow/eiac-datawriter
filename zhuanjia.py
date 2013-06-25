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
  cnx1_sql = 'TRUNCATE TABLE zhuanjia'
  cnx1_cursor.execute(cnx1_sql)
  cnx1.commit()
  print funx.get_time(), '数据表已清空'

if __name__ == '__main__':
  print funx.get_time(), '初始化...'
  cnx1 = mysql.connector.Connect(**cnx1_cfg)
  cnx1_cursor = cnx1.cursor()
  cnx2 = mysql.connector.Connect(**cnx2_cfg)
  cnx2_cursor = cnx2.cursor()
  print funx.get_time(), '数据库已连接'

  init_data()

  cnx2_sql = 'SELECT * FROM tbs001_expert'
  cnx2_cursor.execute(cnx2_sql)
  cnx2_data = cnx2_cursor.fetchall()

  for cnx2_row in cnx2_data:
    cnx1_sql = ('INSERT INTO zhuanjia '
                '(XingMing, XingBie, ZhiWu, '
                'ShenFenZheng, ChuShengRiQi, DanWei, '
                'GangWei, YiDongDianHua, GuDingDianHua, '
                'ChuanZhen, DianYou, ZhuanJiaLeiBie, '
                'HangYeLeiBie, PingGuLeiBie, TuiJianRen, '
                'JianJie) '
                'VALUES (%s, %s, %s, %s, %s, %s, '
                '%s, %s, %s, %s, %s, %s, '
                '%s, %s, %s, %s)')
    cnx1_param = (cnx2_row[1], cnx2_row[2], cnx2_row[3],
                  cnx2_row[5], cnx2_row[4], cnx2_row[6],
                  cnx2_row[11], cnx2_row[7], cnx2_row[8],
                  cnx2_row[9], cnx2_row[10], cnx2_row[12],
                  cnx2_row[13], cnx2_row[14], cnx2_row[15],
                  cnx2_row[16])
    cnx1_cursor.execute(cnx1_sql, cnx1_param)
    cnx1.commit()
    print funx.get_time(), '编号', cnx1_cursor.lastrowid, '添加完毕'

  print funx.get_time(), '所有数据添加完毕'
  cnx1_cursor.close()
  cnx1.close()
  cnx2_cursor.close()
  cnx2.close()
