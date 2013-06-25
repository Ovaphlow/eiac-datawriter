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
  cnx1_sql = 'TRUNCATE TABLE phase_a1'
  cnx1_cursor.execute(cnx1_sql)
  cnx1_sql = 'TRUNCATE TABLE phase_a2'
  cnx1_cursor.execute(cnx1_sql)
  cnx1_sql = 'TRUNCATE TABLE phase_a3'
  cnx1_cursor.execute(cnx1_sql)
  cnx1_sql = 'TRUNCATE TABLE phase_a4'
  cnx1_cursor.execute(cnx1_sql)
  cnx1_sql = 'TRUNCATE TABLE phase_a5'
  cnx1_cursor.execute(cnx1_sql)
  cnx1_sql = 'TRUNCATE TABLE phase_a6'
  cnx1_cursor.execute(cnx1_sql)
  cnx1_sql = 'TRUNCATE TABLE phase_b1'
  cnx1_cursor.execute(cnx1_sql)
  cnx1_sql = 'TRUNCATE TABLE phase_b2'
  cnx1_cursor.execute(cnx1_sql)
  cnx1_sql = 'TRUNCATE TABLE phase_b3'
  cnx1_cursor.execute(cnx1_sql)
  cnx1_sql = 'TRUNCATE TABLE phase_b4'
  cnx1_cursor.execute(cnx1_sql)
  cnx1_sql = 'TRUNCATE TABLE phase_b4'
  cnx1_cursor.execute(cnx1_sql)
  cnx1_sql = 'TRUNCATE TABLE phase_b5'
  cnx1_cursor.execute(cnx1_sql)
  cnx1_sql = 'TRUNCATE TABLE phase_b6'
  cnx1_cursor.execute(cnx1_sql)
  cnx1_sql = 'TRUNCATE TABLE phase_b7'
  cnx1_cursor.execute(cnx1_sql)
  cnx1_sql = 'TRUNCATE TABLE phase_b8'
  cnx1_cursor.execute(cnx1_sql)
  cnx1_sql = 'TRUNCATE TABLE phase_config'
  cnx1_cursor.execute(cnx1_sql)
  cnx1.commit()
  print funx.get_time(), '目标数据表已清空'

def phase_a1_hlj(row_data):
  cnx1_sql = ('INSERT INTO phase_a1 '
               '(ZhuangTai,DiQu,XiangMuMingCheng,'
               'JianSheDiDian,DiQuDaiMa1,DiQuDaiMa2,'
               'XiangMuLianXiRen,LianXiRenDianHua,BaoGaoShuBiao,'
               'HuanBaoTouZi,ZongTouZi,SuoZhanBiLi,'
               'PingJiaJingFei,HangYeLeiBie1,HangYeLeiBie2,'
               'HangYeLeiBie3,HuanPingLeiBie,HuanTongLeiBie,'
               'JianSheXingZhi,JianSheNeirong,XiangMuMiaoShu) '
               'VALUES (%s,%s,%s,%s,%s,%s,'
               '%s,%s,%s,%s,%s,%s,'
               '%s,%s,%s,%s,%s,%s,'
               '%s,%s,%s)')
  cnx1_param = ('提交', '黑龙江', row_data[0],
               row_data[2], row_data[15], '',
               row_data[4], row_data[5], row_data[12],
               row_data[17], row_data[6], row_data[18],
               row_data[106], row_data[7], row_data[8],
               row_data[9], row_data[11], row_data[10],
               '', row_data[13], row_data[102])
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()
  return cnx1_cursor.lastrowid

if __name__ == '__main__':
  print funx.get_time(), '初始化...'
  cnx1 = mysql.connector.Connect(**cnx1_cfg)
  cnx1_cursor = cnx1.cursor()
  cnx2 = mysql.connector.Connect(**cnx2_cfg)
  cnx2_cursor = cnx2.cursor()
  funx.get_time()
  print funx.get_time(), '数据库已连接'

  init_data()

  cnx2_sql = 'SELECT * FROM tbs001_developprojectbasicinfo'
  cnx2_cursor.execute(cnx2_sql)
  cnx2_data = cnx2_cursor.fetchall()
  funx.get_time()
  print funx.get_time(), '源数据数:', cnx1_cursor.rowcount, '开始添加数据到目标数据表'

  for cnx2_row in cnx2_data:
    rowid_last = phase_a1_hlj()
    print funx.get_time(), '编号', rowid_last, '添加完毕'

  print funx.get_time(), '所有数据添加完毕'

  cnx1_cursor.close()
  cnx1.close()
  cnx1_cursor.close()
  cnx1.close()
