#coding=utf-8

import mysql.connector
import get_time

cnx1_cfg = {
  'user': 'root',
  'password': '',
  'host': '127.0.0.1',
  'database': 'ems-hrb',
}

cnx2_cfg = {
  'user': 'root',
  'password': '',
  'host': '127.0.0.1',
  'database': 'eiac-sys',
}

def init_data():
  sql_text2 = 'DELETE FROM phase_a1'
  cursor2.execute(sql_text2)
  sql_text2 = 'DELETE FROM phase_a2'
  cursor2.execute(sql_text2)
  sql_text2 = 'DELETE FROM phase_a3'
  cursor2.execute(sql_text2)
  sql_text2 = 'DELETE FROM phase_a4'
  cursor2.execute(sql_text2)
  sql_text2 = 'DELETE FROM phase_a5'
  cursor2.execute(sql_text2)
  sql_text2 = 'DELETE FROM phase_a6'
  cursor2.execute(sql_text2)
  sql_text2 = 'DELETE FROM phase_b1'
  cursor2.execute(sql_text2)
  sql_text2 = 'DELETE FROM phase_b2'
  cursor2.execute(sql_text2)
  sql_text2 = 'DELETE FROM phase_b3'
  cursor2.execute(sql_text2)
  sql_text2 = 'DELETE FROM phase_b4'
  cursor2.execute(sql_text2)
  sql_text2 = 'DELETE FROM phase_b4'
  cursor2.execute(sql_text2)
  sql_text2 = 'DELETE FROM phase_b5'
  cursor2.execute(sql_text2)
  sql_text2 = 'DELETE FROM phase_b6'
  cursor2.execute(sql_text2)
  sql_text2 = 'DELETE FROM phase_b7'
  cursor2.execute(sql_text2)
  sql_text2 = 'DELETE FROM phase_b8'
  cursor2.execute(sql_text2)
  sql_text2 = 'DELETE FROM phase_config'
  cursor2.execute(sql_text2)
  cnx2.commit()
  print get_time.get_time(), '目标数据表已清空'

def phase_a1_hrb():
  sql_text2 = ('INSERT INTO phase_a1 '
               '(ZhuangTai'
               ',DiQu'
               ',XiangMuMingCheng'
               ',JianSheDiDian'
               ',DiQuDaiMa1'
               ',DiQuDaiMa2' #地区代码2
               ',XiangMuLianXiRen'
               ',LianXiRenDianHua'
               ',BaoGaoShuBiao'
               ',HuanBaoTouZi'
               ',ZongTouZi'
               ',SuoZhanBiLi'
               ',PingJiaJingFei'
               ',HangYeLeiBie1'
               ',HangYeLeiBie2'
               ',HangYeLeiBie3'
               ',HuanPingLeiBie'
               ',HuanTongLeiBie'
               ',JianSheXingZhi' #建设性质
               ',JianSheNeirong'
               ',XiangMuMiaoShu'
               ') '
               'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)')
  sql_data2 = ('提交'
               , '哈尔滨'
               , data_t[0]
               , data_t[2]
               , data_t[15]
               , '' #地区代码2
               , data_t[4]
               , data_t[5]
               , data_t[12]
               , data_t[17]
               , data_t[6]
               , data_t[18]
               , data_t[106]
               , data_t[7]
               , data_t[8]
               , data_t[9]
               , data_t[11]
               , data_t[10]
               , '' #建设性质
               , data_t[13]
               , data_t[102]
               )
  cursor2.execute(sql_text2, sql_data2)
  cnx2.commit()
  return cursor2.lastrowid

def phase_a2_hrb(rowid, pinggu_id, xmfzr_id, xmlxr, lxrdh):
  sql_text2 = ('INSERT INTO phase_a2 '
               'VALUES (%s,%s,%s,%s,%s)')
  sql_data2 = (rowid, pinggu_id, xmfzr_id, xmlxr, lxrdh)
  cursor2.execute(sql_text2, sql_data2)
  cnx2.commit()

def phase_a3_hrb(rowid, jianshe_id):
#  建设单位编号100
  sql_text2 = ('INSERT INTO phase_a3 '
               'VALUES (%s,%s)')
  sql_data2 = (rowid, jianshe_id)
  cursor2.execute(sql_text2, sql_data2)
  cnx2.commit()

def phase_b1_hrb(rowid, hangye, mingan, biaozhun, heding, hsr, xishu, biangeng):
  sql_text2 = ('INSERT INTO phase_b1 '
               'VALUES (%s,%s,%s,%s,%s,%s,%s,%s)')
  sql_data2 = (rowid, hangye, mingan, biaozhun, heding, hsr, xishu, biangeng)
  cursor2.execute(sql_text2, sql_data2)
  cnx2.commit()

def phase_b2_hrb(rowid, bumen):
  sql_text2 = ('INSERT INTO phase_b2 '
               'VALUES (%s,%s)')
  sql_data2 = (rowid, bumen)
  cursor2.execute(sql_text2, sql_data2)
  cnx2.commit()

def phase_b3_hrb(rowid, fuzeren):
  sql_text2 = ('INSERT INTO phase_b3 '
               'VALUES (%s, %s)')
  sql_data2 = (rowid, fuzeren)
  cursor2.execute(sql_text2, sql_data2)
  cnx2.commit()

def phase_b4_hrb(rowid, zhuanjia, riqi, didian):
  sql_text2 = ('INSERT INTO phase_b4 '
               'VALUES (%s,%s,%s,%s)')
  sql_data2 = (rowid, zhuanjia, riqi, didian)
  cursor2.execute(sql_text2, sql_data2)
  cnx2.commit()

def phase_b5_hrb(rowid, zhuanjia, riqi, shangxiawu, didian):
  sql_text2 = ('INSERT INTO phase_b5 '
               'VALUES (%s,%s,%s,%s,%s)')
  sql_data2 = (rowid, zhuanjia, riqi, shangxiawu, didian)
  cursor2.execute(sql_text2, sql_data2)
  cnx2.commit()

def phase_b6_hrb(rowid, jieguo1, zhuangtai, bhg_shuoming, bhg_tiaokuan, fuhe, riqi, shangxiawu, didian, jieguo2):
  sql_text2 = ('INSERT INTO phase_b6 '
               'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)')
  sql_data2 = (rowid, jieguo1, zhuangtai, bhg_shuoming, bhg_tiaokuan, fuhe, riqi, shangxiawu, didian, jieguo2)
  cursor2.execute(sql_text2, sql_data2)
  cnx2.commit()

def phase_b7_hrb(rowid, fen1, fen2, fen3, fen_all):
  sql_text2 = ('INSERT INTO phase_b7 '
               'VALUES (%s,%s,%s,%s,%s)')
  sql_data2 = (rowid, fen1, fen2, fen3, fen_all)
  cursor2.execute(sql_text2, sql_data2)
  cnx2.commit()

def phase_b8_hrb(rowid, baosong1, baosong2):
  sql_text2 = ('INSERT INTO phase_b8 '
               'VALUES (%s,%s,%s)')
  sql_data2 = (rowid, baosong1, baosong2)
  cursor2.execute(sql_text2, sql_data2)
  cnx2.commit()

def phase_config_hrb(rowid):
  sql_text2 = ('INSERT INTO phase_config '
               'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)')
  sql_data2 = (rowid,
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
  cursor2.execute(sql_text2, sql_data2)
  cnx2.commit()

def phase_log_hrb(rowid):
  print get_time.get_time()

print get_time.get_time(), '初始化...'
cnx1 = mysql.connector.Connect(**cnx1_cfg)
cursor1 = cnx1.cursor()
print get_time.get_time(), '源数据库已连接'

cnx2 = mysql.connector.Connect(**cnx2_cfg)
cursor2 = cnx2.cursor()
get_time.get_time()
print get_time.get_time(), '目标数据库已连接'

init_data()

sql_text1 = 'SELECT * FROM tbs001_developprojectbasicinfo'
cursor1.execute(sql_text1)
data1 = cursor1.fetchall()
get_time.get_time()
print get_time.get_time(), '源数据数:', cursor1.rowcount, '开始添加数据到目标数据表'

for data_t in data1:
  rowid_t = phase_a1_hrb()
#  phase_a2_hrb(rowid_t, data_t[101], data_t[107], data_t[118], data_t[105])
#  phase_a3_hrb(rowid_t, data_t[100])
#  phase_b1_hrb(rowid_t, data_t[59], data_t[58], data_t[60], data_t[61], data_t[62],
#               0, 0)
#  phase_b2_hrb(rowid_t, data_t[63])
#  phase_b3_hrb(rowid_t, data_t[66])
#  phase_b4_hrb(rowid_t, data_t[72], data_t[37], data_t[71])
#  phase_b5_hrb(rowid_t, data_t[80], data_t[38], '', data_t[79])
#  phase_b6_hrb(rowid_t, data_t[90], data_t[89], data_t[45], data_t[93],
#               data_t[42], data_t[91], 0, data_t[92], data_t[108])
#  phase_b7_hrb(rowid_t, data_t[96], data_t[97], data_t[142], '0')
#  phase_b8_hrb(rowid_t, data_t[99], data_t[143])
  phase_config_hrb(rowid_t)
  print get_time.get_time(), '编号', rowid_t, '记录添加完毕'

print get_time.get_time(), '所有数据添加完毕'

cursor1.close()
cnx1.close()
cursor2.close()
cnx2.close()
