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

def insert_data_hlj():
  cnx2_sql = 'SELECT * FROM tbs001_developprojectbasicinfo'
  cnx2_cursor.execute(cnx2_sql)
  cnx2_data = cnx2_cursor.fetchall()

  for cnx2_row in cnx2_data:
    cnx1_sql = ('INSERT INTO phase_a1 '
                '(ZhuangTai, DiQu ,XiangMuMingCheng,'
                'JianSheDiDian, DiQuDaiMa1, DiQuDaiMa2,'
                'XiangMuLianXiRen, LianXiRenDianHua, BaoGaoShuBiao,'
                'HuanBaoTouZi, ZongTouZi, SuoZhanBiLi,'
                'PingJiaJingFei, HangYeLeiBie1, HangYeLeiBie2,'
                'HangYeLeiBie3, HuanPingLeiBie, HuanTongLeiBie,'
                'JianSheXingZhi, JianSheNeirong, XiangMuMiaoShu) '
                'VALUES (%s, %s, %s, %s, %s, %s,'
                '%s, %s, %s, %s, %s, %s,'
                '%s, %s, %s, %s, %s, %s,'
                '%s, %s, %s)')
    cnx1_param = ('提交', '黑龙江', cnx2_row[0],
                  cnx2_row[2], cnx2_row[15], '',
                  cnx2_row[4], cnx2_row[5], cnx2_row[12],
                  cnx2_row[17], cnx2_row[6], cnx2_row[18],
                  cnx2_row[106], cnx2_row[7], cnx2_row[8],
                  cnx2_row[9], cnx2_row[11], cnx2_row[10],
                  '', cnx2_row[13], cnx2_row[102])
    cnx1_cursor.execute(cnx1_sql, cnx1_param)
    pid = cnx1_cursor.lastrowid
    phase_a2(cnx2_row, pid)
    phase_a3(cnx2_row, pid)
    phase_b1_hlj(cnx2_row, pid)
    phase_b2(cnx2_row, pid)
    phase_b3(cnx2_row, pid)
    phase_b4(cnx2_row, pid)
    phase_b5_hlj(cnx2_row, pid)
    phase_b6(cnx2_row, pid)
    phase_b7(cnx2_row, pid)
    phase_b8(cnx2_row, pid)
    phase_config_hlj(cnx2_row, pid)
  cnx1.commit()

def insert_data_hrb():
  cnx3_sql = 'SELECT * FROM tbs001_developprojectbasicinfo'
  cnx3_cursor.execute(cnx3_sql)
  cnx3_data = cnx3_cursor.fetchall()

  for cnx3_row in cnx3_data:
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
    cnx1_param = ('提交', '哈尔滨', cnx3_row[0],
                  cnx3_row[2], cnx3_row[15], '',
                  cnx3_row[4], cnx3_row[5], cnx3_row[12],
                  cnx3_row[17], cnx3_row[6], cnx3_row[18],
                  cnx3_row[106], cnx3_row[7], cnx3_row[8],
                  cnx3_row[9], cnx3_row[11], cnx3_row[10],
                  '', cnx3_row[13], cnx3_row[102])
    cnx1_cursor.execute(cnx1_sql, cnx1_param)
    pid = cnx1_cursor.lastrowid
    phase_a2(cnx3_row, pid)
    phase_a3(cnx3_row, pid)
    phase_b1_hrb(cnx3_row, pid)
    phase_b2(cnx3_row, pid)
    phase_b3(cnx3_row, pid)
    phase_b4(cnx3_row, pid)
    phase_b5_hrb(cnx3_row, pid)
    phase_b6(cnx3_row, pid)
    phase_b7(cnx3_row, pid)
    phase_b8(cnx3_row, pid)
    phase_config_hrb(cnx3_row, pid)
  cnx1.commit()

def phase_a2(row_data, row_id):
  cnx1_sql = ('INSERT INTO phase_a2 '
              'VALUES (%s, %s, %s, %s, %s)')
  cnx1_param = (row_id, row_data[101], row_data[107],
                row_data[118], row_data[105])
  cnx1_cursor.execute(cnx1_sql, cnx1_param)

def phase_a3(row_data, row_id):
  cnx1_sql = ('INSERT INTO phase_a3 '
              'VALUES (%s,%s)')
  cnx1_param = (row_id, row_data[100])
  cnx1_cursor.execute(cnx1_sql, cnx1_param)

def phase_b1_hlj(row_data, row_id):
  cnx1_sql = ('INSERT INTO phase_b1 '
              'VALUES (%s, %s, %s, %s, %s, %s,'
              '%s, %s)')
  cnx1_param = (row_id, row_data[59], row_data[58],
                row_data[60], row_data[61], row_data[62],
                row_data[137], row_data[29])
  cnx1_cursor.execute(cnx1_sql, cnx1_param)

def phase_b1_hrb(row_data, row_id):
  cnx1_sql = ('INSERT INTO phase_b1 '
              'VALUES (%s, %s, %s, %s, %s, %s,'
              '%s, %s)')
  cnx1_param = (row_id, row_data[59], row_data[58],
                row_data[60], row_data[61], row_data[62],
                0, 0)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)

def phase_b2(row_data, row_id):
  cnx1_sql = ('INSERT INTO phase_b2 '
              'VALUES (%s, %s)')
  cnx1_param = (row_id, row_data[63])
  cnx1_cursor.execute(cnx1_sql, cnx1_param)

def phase_b3(row_data, row_id):
  cnx1_sql = ('INSERT INTO phase_b3 '
              'VALUES (%s, %s)')
  cnx1_param = (row_id, row_data[66])
  cnx1_cursor.execute(cnx1_sql, cnx1_param)

def phase_b4(row_data, row_id):
  cnx1_sql = ('INSERT INTO phase_b4 '
              'VALUES (%s, %s, %s, %s)')
  cnx1_param = (row_id, row_data[72], row_data[37],
                row_data[71])
  cnx1_cursor.execute(cnx1_sql, cnx1_param)

def phase_b5_hlj(row_data, row_id):
  cnx1_sql = ('INSERT INTO phase_b5 '
              'VALUES (%s, %s, %s, %s, %s)')
  cnx1_param = (row_id, row_data[80], row_data[38],
                row_data[43], row_data[79])
  cnx1_cursor.execute(cnx1_sql, cnx1_param)

def phase_b5_hrb(row_data, row_id):
  cnx1_sql = ('INSERT INTO phase_b5 '
              'VALUES (%s, %s, %s, %s, %s)')
  cnx1_param = (row_id, row_data[80], row_data[38],
                '', row_data[79])
  cnx1_cursor.execute(cnx1_sql, cnx1_param)

def phase_b6(row_data, row_id):
  cnx1_sql = ('INSERT INTO phase_b6 '
              'VALUES (%s, %s, %s, %s, %s, %s,'
              '%s, %s, %s, %s)')
  cnx1_param = (row_id, row_data[90], row_data[89],
                row_data[45], row_data[93], row_data[42],
                row_data[91], 0, row_data[92],
                row_data[108])
  cnx1_cursor.execute(cnx1_sql, cnx1_param)

def phase_b7(row_data, row_id):
  cnx1_sql = ('INSERT INTO phase_b7 '
              'VALUES (%s, %s, %s, %s, %s)')
  cnx1_param = (row_id, row_data[96], row_data[97],
                row_data[142], '0')
  cnx1_cursor.execute(cnx1_sql, cnx1_param)

def phase_b8(row_data, row_id):
  cnx1_sql = ('INSERT INTO phase_b8 '
              'VALUES (%s, %s, %s)')
  cnx1_param = (row_id, row_data[99], row_data[143])
  cnx1_cursor.execute(cnx1_sql, cnx1_param)

def phase_config_hlj(row_data, row_id):
  cnx1_sql = ('INSERT INTO phase_config '
              'VALUES (%s, %s, %s, %s, %s, %s,'
              '%s, %s, %s, %s, %s, %s,'
              '%s)')
  cnx1_param = (row_id, '', row_data[103],
                row_data[104], '0', '',
                '', row_data[26], row_data[27],
                row_data[28], row_data[29], row_data[144],
                0)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)

def phase_config_hrb(row_data, row_id):
  cnx1_sql = ('INSERT INTO phase_config '
              'VALUES (%s, %s, %s, %s, %s, %s,'
              '%s, %s, %s, %s, %s, %s,'
              '%s)')
  cnx1_param = (row_id, '', row_data[103],
                row_data[104], row_data[146], row_data[145],
                row_data[151], row_data[26], row_data[27],
                row_data[28], row_data[29], row_data[144],
                row_data[147])
  cnx1_cursor.execute(cnx1_sql, cnx1_param)

if __name__ == '__main__':
  print funx.get_time(), '连接V3数据库'
  cnx1 = mysql.connector.Connect(**cnx1_cfg)
  cnx1_cursor = cnx1.cursor()

  print funx.get_time(), '清空V3数据表'
  init_data()

  print funx.get_time(), '连接V2黑龙江数据库'
  cnx2 = mysql.connector.Connect(**cnx2_cfg)
  cnx2_cursor = cnx2.cursor()

  print funx.get_time(), '添加V2黑龙江数据'
  insert_data_hlj()

  print funx.get_time(), '连接V2哈尔滨数据库'
  cnx3 = mysql.connector.Connect(**cnx3_cfg)
  cnx3_cursor = cnx3.cursor()

  print funx.get_time(), '添加V2哈尔滨数据'
  insert_data_hrb()

  print funx.get_time(), '所有数据添加完毕，关闭数据库链接'
  cnx1_cursor.close()
  cnx1.close()
  cnx2_cursor.close()
  cnx2.close()
  cnx3_cursor.close()
  cnx3.close()
