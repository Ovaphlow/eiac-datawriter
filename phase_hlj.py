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

def phase_a2_hlj(rowid, pinggu_id, xmfzr_id, xmlxr, lxrdh):
  cnx1_sql = ('INSERT INTO phase_a2 '
               'VALUES (%s,%s,%s,%s,%s)')
  cnx1_param = (rowid, pinggu_id, xmfzr_id, xmlxr, lxrdh)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def phase_a3_hlj(rowid, jianshe_id):
  cnx1_sql = ('INSERT INTO phase_a3 '
               'VALUES (%s,%s)')
  cnx1_param = (rowid, jianshe_id)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def phase_b1_hlj(rowid, hangye, mingan, biaozhun, heding, hsr,
                 xishu, biangeng):
  cnx1_sql = ('INSERT INTO phase_b1 '
               'VALUES (%s,%s,%s,%s,%s,%s,'
               '%s,%s)')
  cnx1_param = (rowid, hangye, mingan, biaozhun, heding, hsr,
                xishu, biangeng)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def phase_b2_hlj(rowid, bumen):
  cnx1_sql = ('INSERT INTO phase_b2 '
               'VALUES (%s,%s)')
  cnx1_param = (rowid, bumen)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def phase_b3_hlj(rowid, fuzeren):
  cnx1_sql = ('INSERT INTO phase_b3 '
               'VALUES (%s, %s)')
  cnx1_param = (rowid, fuzeren)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def phase_b4_hlj(rowid, zhuanjia, riqi, didian):
  cnx1_sql = ('INSERT INTO phase_b4 '
               'VALUES (%s,%s,%s,%s)')
  cnx1_param = (rowid, zhuanjia, riqi, didian)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def phase_b5_hlj(rowid, zhuanjia, riqi, shangxiawu, didian):
  cnx1_sql = ('INSERT INTO phase_b5 '
               'VALUES (%s,%s,%s,%s,%s)')
  cnx1_param = (rowid, zhuanjia, riqi, shangxiawu, didian)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def phase_b6_hlj(rowid, jieguo1, zhuangtai,
                 bhg_shuoming, bhg_tiaokuan, fuhe,
                 riqi, shangxiawu, didian,
                 jieguo2):
  cnx1_sql = ('INSERT INTO phase_b6 '
               'VALUES (%s,%s,%s,%s,%s,%s,'
               '%s,%s,%s,%s)')
  cnx1_param = (rowid, jieguo1, zhuangtai,
                bhg_shuoming, bhg_tiaokuan, fuhe,
                riqi, shangxiawu, didian,
                jieguo2)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def phase_b7_hlj(rowid, fen1, fen2, fen3, fen_all):
  cnx1_sql = ('INSERT INTO phase_b7 '
               'VALUES (%s,%s,%s,%s,%s)')
  cnx1_param = (rowid, fen1, fen2, fen3, fen_all)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def phase_b8_hlj(rowid, baosong1, baosong2):
  cnx1_sql = ('INSERT INTO phase_b8 '
               'VALUES (%s,%s,%s)')
  cnx1_param = (rowid, baosong1, baosong2)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def phase_config_hlj(rowid, row_data):
  cnx1_sql = ('INSERT INTO phase_config '
               'VALUES (%s,%s,%s,%s,%s,%s,'
               '%s,%s,%s,%s,%s,%s,'
               '%s)')
  cnx1_param = (rowid, '', row_data[103],
               row_data[104], '0', '',
               '', row_data[26], row_data[27],
               row_data[28], row_data[29], row_data[144],
               0)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

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
  print funx.get_time(), '源数据数:', cnx2_cursor.rowcount,
        '开始添加数据到目标数据表'

  for cnx2_row in cnx2_data:
    rowid_last = phase_a1_hlj(cnx2_row)
    phase_a2_hlj(rowid_last, cnx2_row[101], cnx2_row[107], 
                 cnx2_row[118], cnx2_row[105])
    phase_a3_hlj(rowid_last, cnx2_row[100])
    phase_b1_hlj(rowid_last, cnx2_row[59], cnx2_row[58],
                 cnx2_row[60], cnx2_row[61], cnx2_row[62],
                 cnx2_row[137], cnx2_row[29])
    phase_b2_hlj(rowid_last, cnx2_row[63])
    phase_b3_hlj(rowid_last, cnx2_row[66])
    phase_b4_hlj(rowid_last, cnx2_row[72], cnx2_row[37],
                 cnx2_row[71])
    phase_b5_hlj(rowid_last, cnx2_row[80], cnx2_row[38],
                 cnx2_row[43], cnx2_row[79])
    phase_b6_hlj(rowid_last, cnx2_row[90], cnx2_row[89],
                 cnx2_row[45], cnx2_row[93], cnx2_row[42],
                 cnx2_row[91], 0, cnx2_row[92],
                 cnx2_row[108])
    phase_b7_hlj(rowid_last, cnx2_row[96], cnx2_row[97],
                 cnx2_row[142], '0')
    phase_b8_hlj(rowid_last, cnx2_row[99], cnx2_row[143])
    phase_config_hlj(rowid_last, cnx2_row)
    print funx.get_time(), '编号', rowid_last, '添加完毕'

  print funx.get_time(), '所有数据添加完毕'

  cnx1_cursor.close()
  cnx1.close()
  cnx1_cursor.close()
  cnx1.close()
