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
  'database': 'ems-hrb',
}

def init_data():
  sql_text2 = 'TRUNCATE TABLE phase_a1'
  cursor2.execute(sql_text2)
  sql_text2 = 'TRUNCATE TABLE phase_a2'
  cursor2.execute(sql_text2)
  sql_text2 = 'TRUNCATE TABLE phase_a3'
  cursor2.execute(sql_text2)
  sql_text2 = 'TRUNCATE TABLE phase_a4'
  cursor2.execute(sql_text2)
  sql_text2 = 'TRUNCATE TABLE phase_a5'
  cursor2.execute(sql_text2)
  sql_text2 = 'TRUNCATE TABLE phase_a6'
  cursor2.execute(sql_text2)
  sql_text2 = 'TRUNCATE TABLE phase_b1'
  cursor2.execute(sql_text2)
  sql_text2 = 'TRUNCATE TABLE phase_b2'
  cursor2.execute(sql_text2)
  sql_text2 = 'TRUNCATE TABLE phase_b3'
  cursor2.execute(sql_text2)
  sql_text2 = 'TRUNCATE TABLE phase_b4'
  cursor2.execute(sql_text2)
  sql_text2 = 'TRUNCATE TABLE phase_b4'
  cursor2.execute(sql_text2)
  sql_text2 = 'TRUNCATE TABLE phase_b5'
  cursor2.execute(sql_text2)
  sql_text2 = 'TRUNCATE TABLE phase_b6'
  cursor2.execute(sql_text2)
  sql_text2 = 'TRUNCATE TABLE phase_b7'
  cursor2.execute(sql_text2)
  sql_text2 = 'TRUNCATE TABLE phase_b8'
  cursor2.execute(sql_text2)
  sql_text2 = 'TRUNCATE TABLE phase_config'
  cursor2.execute(sql_text2)
  cnx2.commit()
  print funx.get_time(), '目标数据表已清空'

def phase_a1_hlj():
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
               , '黑龙江'
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

