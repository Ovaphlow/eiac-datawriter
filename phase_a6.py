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
  cnx1_sql = 'TRUNCATE TABLE phase_a6'
  cnx1_cursor.execute(cnx1_sql)
  cnx1.commit()
  print funx.get_time(), '数据表已清空'

def phase_a6_hlj_insert(row_data, project_id):
  cnx1_sql = ('INSERT INTO phase_a6 '
              '(id) '
              'VALUES (%s)')
  cnx1_param = (project_id,)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def phase_a6_hlj_1(row_data, project_id):
  cnx1_sql = ('UPDATE phase_a6 '
              'SET '
              'ZRBHQ_1=%s,ZRBHQ_2=%s,ZRBHQ_3=%s,'
              'ZRBHQ_4=%s,ZRBHQ_5=%s,ZRBHQ_6=%s,'
              'ZRBHQ_7=%s,ZRBHQ_8=%s,ZRBHQ_9=%s,'
              'ZRBHQ_10=%s '
              'WHERE id=%s')
  cnx1_param = (row_data[2], row_data[3], row_data[4],
                row_data[5], row_data[6], row_data[7],
                row_data[8], row_data[9], row_data[10],
                row_data[11], project_id)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def phase_a6_hlj_2(row_data, project_id):
  cnx1_sql = ('UPDATE phase_a6 '
              'SET '
              'ShYBHQ_1=%s,ShYBHQ_2=%s,ShYBHQ_3=%s,'
              'ShYBHQ_4=%s,ShYBHQ_5=%s,ShYBHQ_6=%s,'
              'ShYBHQ_7=%s,ShYBHQ_8=%s,ShYBHQ_9=%s,'
              'ShYBHQ_10=%s '
              'WHERE id=%s')
  cnx1_param = (row_data[12], row_data[13], row_data[14],
                row_data[15], row_data[16], row_data[17],
                row_data[18], row_data[19], row_data[20],
                row_data[21], project_id)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def phase_a6_hlj_3(row_data, project_id):
  cnx1_sql = ('UPDATE phase_a6 '
              'SET '
              'ZhYShD_1=%s,ZhYShD_2=%s,ZhYShD_3=%s,'
              'ZhYShD_4=%s,ZhYShD_5=%s,ZhYShD_6=%s,'
              'ZhYShD_7=%s,ZhYShD_8=%s,ZhYShD_9=%s,'
              'ZhYShD_10=%s '
              'WHERE id=%s')
  cnx1_param = (row_data[22], row_data[23], row_data[24],
                row_data[25], row_data[26], row_data[27],
                row_data[28], row_data[29], row_data[30],
                row_data[31], project_id)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def phase_a6_hlj_4(row_data, project_id):
  cnx1_sql = ('UPDATE phase_a6 '
              'SET '
              'FJMShQ_1=%s,FJMShQ_2=%s,FJMShQ_3=%s,'
              'FJMShQ_4=%s,FJMShQ_5=%s,FJMShQ_6=%s,'
              'FJMShQ_7=%s,FJMShQ_8=%s,FJMShQ_9=%s,'
              'FJMShQ_10=%s '
              'WHERE id=%s')
  cnx1_param = (row_data[32], row_data[33], row_data[34],
                row_data[35], row_data[36], row_data[37],
                row_data[38], row_data[39], row_data[40],
                row_data[41], project_id)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def phase_a6_hlj_5(row_data, project_id):
  cnx1_sql = ('UPDATE phase_a6 '
              'SET '
              'YChD_1=%s,YChD_2=%s,YChD_3=%s,'
              'YChD_4=%s,YChD_5=%s,YChD_6=%s,'
              'YChD_7=%s,YChD_8=%s,YChD_9=%s,'
              'YChD_10=%s '
              'WHERE id=%s')
  cnx1_param = (row_data[42], row_data[43], row_data[44],
                row_data[45], row_data[46], row_data[47],
                row_data[48], row_data[49], row_data[50],
                row_data[51], project_id)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def phase_a6_hlj_6(row_data, project_id):
  cnx1_sql = ('UPDATE phase_a6 '
              'SET '
              'ZhXTYDW_1=%s,ZhXTYDW_2=%s,ZhXTYDW_3=%s,'
              'ZhXTYDW_4=%s,ZhXTYDW_5=%s,ZhXTYDW_6=%s,'
              'ZhXTYDW_7=%s,ZhXTYDW_8=%s,ZhXTYDW_9=%s,'
              'ZhXTYDW_10=%s '
              'WHERE id=%s')
  cnx1_param = (row_data[52], row_data[53], row_data[54],
                row_data[55], row_data[56], row_data[57],
                row_data[58], row_data[59], row_data[60],
                row_data[61], project_id)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def phase_a6_hlj_7(row_data, project_id):
  cnx1_sql = ('UPDATE phase_a6 '
              'SET '
              'ZhXTYZhW_1=%s,ZhXTYZhW_2=%s,ZhXTYZhW_3=%s,'
              'ZhXTYZhW_4=%s,ZhXTYZhW_5=%s,ZhXTYZhW_6=%s,'
              'ZhXTYZhW_7=%s,ZhXTYZhW_8=%s,ZhXTYZhW_9=%s,'
              'ZhXTYZhW_10=%s '
              'WHERE id=%s')
  cnx1_param = (row_data[62], row_data[63], row_data[64],
                row_data[65], row_data[66], row_data[67],
                row_data[68], row_data[69], row_data[70],
                row_data[71], project_id)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def phase_a6_hlj_8(row_data, project_id):
  cnx1_sql = ('UPDATE phase_a6 '
              'SET '
              'ZhYTD_1_1=%s,ZhYTD_1_2=%s,ZhYTD_1_3=%s,'
              'ZhYTD_1_4=%s,ZhYTD_1_5=%s,ZhYTD_1_6=%s,'
              'ZhYTD_1_7=%s,ZhYTD_2_1=%s,ZhYTD_2_2=%s,'
              'ZhYTD_2_3=%s,ZhYTD_2_4=%s,ZhYTD_2_5=%s,'
              'ZhYTD_2_6=%s,ZhYTD_2_7=%s '
              'WHERE id=%s')
  cnx1_param = (row_data[72], row_data[73], row_data[74],
                row_data[75], row_data[76], row_data[77],
                row_data[78], row_data[79], row_data[80],
                row_data[81], row_data[82], row_data[83],
                row_data[84], row_data[85], project_id)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def phase_a6_hlj_9(row_data, project_id):
  cnx1_sql = ('UPDATE phase_a6 '
              'SET '
              'ZShZhL_1=%s,ZShZhL_2=%s,ZShZhL_3=%s,'
              'ZShZhL_4=%s,ZShZhL_5=%s,ZShZhL_6=%s,'
              'ChQRK_1=%s,ChQRK_2=%s,ChQRK_3=%s,'
              'ChQRK_4=%s,ChQRK_5=%s,ZhLShTLSh_1=%s,'
              'ZhLShTLSh_2=%s,ZhLShTLSh_3=%s '
              'WHERE id=%s')
  cnx1_param = (row_data[86], row_data[87], row_data[88],
                row_data[89], row_data[90], row_data[91],
                row_data[92], row_data[93], row_data[94],
                row_data[95], row_data[96], row_data[97],
                row_data[98], row_data[99],
                project_id)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

if __name__ == '__main__':
  print funx.get_time(), '初始化...'
  cnx1 = mysql.connector.Connect(**cnx1_cfg)
  cnx1_cursor = cnx1.cursor()
  cnx2 = mysql.connector.Connect(**cnx2_cfg)
  cnx2_cursor = cnx2.cursor()
  print funx.get_time(), '数据库已连接'

  init_data()

  cnx2_sql = ('SELECT * '
              'FROM tbx001_shengtai '
              'ORDER BY id')
  cnx2_cursor.execute(cnx2_sql)
  cnx2_data = cnx2_cursor.fetchall()

  for cnx2_row in cnx2_data:
    cnx1_sql = ('SELECT * '
                'FROM phase_a1 '
                'WHERE XiangMuMingCheng=%s')
    cnx1_param = (cnx2_row[1],)
    cnx1_cursor.execute(cnx1_sql, cnx1_param)
    cnx1_data = cnx1_cursor.fetchall()
    if cnx1_cursor.rowcount == 0:
      continue
    else:
      project_id = cnx1_data[0][0]
      cnx1_sql = 'SELECT COUNT(*) FROM phase_a6 WHERE id=%s'
      cnx1_param = (project_id,)
      cnx1_cursor.execute(cnx1_sql, cnx1_param)
      cnx1_row = cnx1_cursor.fetchone()
      if cnx1_row[0] == 0:
        phase_a6_hlj_insert(cnx2_row, project_id)
        phase_a6_hlj_1(cnx2_row, project_id)
        phase_a6_hlj_2(cnx2_row, project_id)
        phase_a6_hlj_3(cnx2_row, project_id)
        phase_a6_hlj_4(cnx2_row, project_id)
        phase_a6_hlj_5(cnx2_row, project_id)
        phase_a6_hlj_6(cnx2_row, project_id)
        phase_a6_hlj_7(cnx2_row, project_id)
        phase_a6_hlj_8(cnx2_row, project_id)
        phase_a6_hlj_9(cnx2_row, project_id)
      else:
        pass
      print funx.get_time(), '编号', project_id, '添加完毕'

  print funx.get_time(), '所有数据添加完毕'
  cnx1_cursor.close()
  cnx1.close()
  cnx2_cursor.close()
  cnx2.close()
