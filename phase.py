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
  cnx2.commit()

def phase_b1_hrb(row_data, row_id):
  cnx1_sql = ('INSERT INTO phase_b1 '
               'VALUES (%s, %s, %s, %s, %s, %s,'
               '%s, %s)')
  cnx1_param = (row_id, hangye, mingan, biaozhun, heding, hsr, xishu, biangeng)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx2.commit()

def phase_b2_hrb(rowid, bumen):
  cnx1_sql = ('INSERT INTO phase_b2 '
               'VALUES (%s,%s)')
  cnx1_param = (rowid, bumen)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def phase_b3_hrb(rowid, fuzeren):
  cnx1_sql = ('INSERT INTO phase_b3 '
               'VALUES (%s, %s)')
  cnx1_param = (rowid, fuzeren)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx2.commit()

def phase_b4_hrb(rowid, zhuanjia, riqi, didian):
  cnx1_sql = ('INSERT INTO phase_b4 '
               'VALUES (%s,%s,%s,%s)')
  cnx1_param = (rowid, zhuanjia, riqi, didian)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx2.commit()

def phase_b5_hrb(rowid, zhuanjia, riqi, shangxiawu, didian):
  cnx1_sql = ('INSERT INTO phase_b5 '
               'VALUES (%s,%s,%s,%s,%s)')
  cnx1_param = (rowid, zhuanjia, riqi, shangxiawu, didian)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx2.commit()

def phase_b6_hrb(rowid, jieguo1, zhuangtai, bhg_shuoming, bhg_tiaokuan, fuhe, riqi, shangxiawu, didian, jieguo2):
  cnx1_sql = ('INSERT INTO phase_b6 '
               'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)')
  cnx1_param = (rowid, jieguo1, zhuangtai, bhg_shuoming, bhg_tiaokuan, fuhe, riqi, shangxiawu, didian, jieguo2)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx2.commit()

def phase_b7_hrb(rowid, fen1, fen2, fen3, fen_all):
  cnx1_sql = ('INSERT INTO phase_b7 '
               'VALUES (%s,%s,%s,%s,%s)')
  cnx1_param = (rowid, fen1, fen2, fen3, fen_all)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx2.commit()

def phase_b8_hrb(rowid, baosong1, baosong2):
  cnx1_sql = ('INSERT INTO phase_b8 '
               'VALUES (%s,%s,%s)')
  cnx1_param = (rowid, baosong1, baosong2)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx2.commit()

def phase_config_hrb(rowid):
  cnx1_sql = ('INSERT INTO phase_config '
               'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)')
  cnx1_param = (rowid,
               '',
               data_t[103],
               data_t[104],
               data_t[146],
               data_t[145],
               data_t[151],
               data_t[26],
               data_t[27],
               data_t[28],
               data_t[29],
               data_t[144],
               data_t[147])
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx2.commit()

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

  print funx.get_time(), '所有数据添加完毕，关闭数据库链接'
  cnx1_cursor.close()
  cnx1.close()
  cnx2_cursor.close()
  cnx2.close()
  #cnx3_cursor.close()
  #cnx3.close()
