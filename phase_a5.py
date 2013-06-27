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
  cnx1_sql = 'TRUNCATE TABLE phase_a5'
  cnx1_cursor.execute(cnx1_sql)
  cnx1.commit()
  print funx.get_time(), '数据表已清空'

def phase_a5_hlj_insert(row_data, project_id):
  cnx1_sql = ('INSERT INTO phase_a5 '
              '(id) '
              'VALUES (%s)')
  cnx1_param = (project_id,) 
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def phase_a5_hlj_fs(row_data, project_id):
  cnx1_sql = ('UPDATE phase_a5 '
              'SET '
              'FS_1_1=%s,FS_1_2=%s,FS_1_3=%s,'
              'FS_1_4=%s,FS_2_1=%s,FS_2_2=%s,'
              'FS_2_3=%s,FS_2_4=%s,FS_2_5=%s,'
              'FS_2_6=%s,FS_3_1=%s,FS_3_2=%s,'
              'FS_3_3=%s,FS_3_4=%s,FS_3_5=%s '
              'WHERE id=%s')
  cnx1_param = (row_data[19], row_data[20], row_data[21], 
                row_data[22], row_data[23], row_data[24], 
                row_data[25], row_data[26], row_data[27], 
                row_data[28], row_data[29], row_data[30], 
                row_data[31], row_data[32], row_data[33], 
                project_id)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def phase_a5_hlj_hxxyl(row_data, project_id):
  cnx1_sql = ('UPDATE phase_a5 '
              'SET '
              'HXXYL_1_1=%s,HXXYL_1_2=%s,HXXYL_1_3=%s,'
              'HXXYL_1_4=%s,HXXYL_2_1=%s,HXXYL_2_2=%s,'
              'HXXYL_2_3=%s,HXXYL_2_4=%s,HXXYL_2_5=%s,'
              'HXXYL_2_6=%s,HXXYL_3_1=%s,HXXYL_3_2=%s,'
              'HXXYL_3_3=%s,HXXYL_3_4=%s,HXXYL_3_5=%s '
              'WHERE id=%s')
  cnx1_param = (row_data[34], row_data[35], row_data[36], 
                row_data[37], row_data[38], row_data[39], 
                row_data[40], row_data[41], row_data[42], 
                row_data[43], row_data[44], row_data[45], 
                row_data[46], row_data[47], row_data[48], 
                project_id)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def phase_a5_hlj_ad(row_data, project_id):
  cnx1_sql = ('UPDATE phase_a5 '
              'SET '
              'AD_1_1=%s,AD_1_2=%s,AD_1_3=%s,'
              'AD_1_4=%s,AD_2_1=%s,AD_2_2=%s,'
              'AD_2_3=%s,AD_2_4=%s,AD_2_5=%s,'
              'AD_2_6=%s,AD_3_1=%s,AD_3_2=%s,'
              'AD_3_3=%s,AD_3_4=%s,AD_3_5=%s '
              'WHERE id=%s')
  cnx1_param = (row_data[49], row_data[50], row_data[51], 
                row_data[52], row_data[53], row_data[54], 
                row_data[55], row_data[56], row_data[57], 
                row_data[58], row_data[59], row_data[60], 
                row_data[61], row_data[62], row_data[63], 
                project_id)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def phase_a5_hlj_sy(row_data, project_id):
  cnx1_sql = ('UPDATE phase_a5 '
              'SET '
              'SY_1_1=%s,SY_1_2=%s,SY_1_3=%s,'
              'SY_1_4=%s,SY_2_1=%s,SY_2_2=%s,'
              'SY_2_3=%s,SY_2_4=%s,SY_2_5=%s,'
              'SY_2_6=%s,SY_3_1=%s,SY_3_2=%s,'
              'SY_3_3=%s,SY_3_4=%s,SY_3_5=%s '
              'WHERE id=%s')
  cnx1_param = (row_data[64], row_data[65], row_data[66], 
                row_data[67], row_data[68], row_data[69], 
                row_data[70], row_data[71], row_data[72], 
                row_data[73], row_data[74], row_data[75], 
                row_data[76], row_data[77], row_data[78], 
                project_id)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def phase_a5_hlj_fq(row_data, project_id):
  cnx1_sql = ('UPDATE phase_a5 '
              'SET '
              'FQ_1_1=%s,FQ_1_2=%s,FQ_1_3=%s,'
              'FQ_1_4=%s,FQ_2_1=%s,FQ_2_2=%s,'
              'FQ_2_3=%s,FQ_2_4=%s,FQ_2_5=%s,'
              'FQ_2_6=%s,FQ_3_1=%s,FQ_3_2=%s,'
              'FQ_3_3=%s,FQ_3_4=%s,FQ_3_5=%s '
              'WHERE id=%s')
  cnx1_param = (row_data[79], row_data[80], row_data[81], 
                row_data[82], row_data[83], row_data[84], 
                row_data[85], row_data[86], row_data[87], 
                row_data[88], row_data[89], row_data[90], 
                row_data[91], row_data[92], row_data[93], 
                project_id)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def phase_a5_hlj_eyhl(row_data, project_id):
  cnx1_sql = ('UPDATE phase_a5 '
              'SET '
              'EYHL_1_1=%s,EYHL_1_2=%s,EYHL_1_3=%s,'
              'EYHL_1_4=%s,EYHL_2_1=%s,EYHL_2_2=%s,'
              'EYHL_2_3=%s,EYHL_2_4=%s,EYHL_2_5=%s,'
              'EYHL_2_6=%s,EYHL_3_1=%s,EYHL_3_2=%s,'
              'EYHL_3_3=%s,EYHL_3_4=%s,EYHL_3_5=%s '
              'WHERE id=%s')
  cnx1_param = (row_data[94], row_data[95], row_data[96], 
                row_data[97], row_data[98], row_data[99], 
                row_data[100], row_data[101], row_data[102], 
                row_data[103], row_data[104], row_data[105], 
                row_data[106], row_data[107], row_data[108], 
                project_id)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def phase_a5_hlj_ych(row_data, project_id):
  cnx1_sql = ('UPDATE phase_a5 '
              'SET '
              'YCh_1_1=%s,YCh_1_2=%s,YCh_1_3=%s,'
              'YCh_1_4=%s,YCh_2_1=%s,YCh_2_2=%s,'
              'YCh_2_3=%s,YCh_2_4=%s,YCh_2_5=%s,'
              'YCh_2_6=%s,YCh_3_1=%s,YCh_3_2=%s,'
              'YCh_3_3=%s,YCh_3_4=%s,YCh_3_5=%s '
              'WHERE id=%s')
  cnx1_param = (row_data[109], row_data[110], row_data[111], 
                row_data[112], row_data[113], row_data[114], 
                row_data[115], row_data[116], row_data[117], 
                row_data[118], row_data[119], row_data[120], 
                row_data[121], row_data[122], row_data[123], 
                project_id)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def phase_a5_hlj_gyfm(row_data, project_id):
  cnx1_sql = ('UPDATE phase_a5 '
              'SET '
              'GYFM_1_1=%s,GYFM_1_2=%s,GYFM_1_3=%s,'
              'GYFM_1_4=%s,GYFM_2_1=%s,GYFM_2_2=%s,'
              'GYFM_2_3=%s,GYFM_2_4=%s,GYFM_2_5=%s,'
              'GYFM_2_6=%s,GYFM_3_1=%s,GYFM_3_2=%s,'
              'GYFM_3_3=%s,GYFM_3_4=%s,GYFM_3_5=%s '
              'WHERE id=%s')
  cnx1_param = (row_data[124], row_data[125], row_data[126], 
                row_data[127], row_data[128], row_data[129], 
                row_data[130], row_data[131], row_data[132], 
                row_data[133], row_data[134], row_data[135], 
                row_data[136], row_data[137], row_data[138], 
                project_id)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def phase_a5_hlj_dyhhw(row_data, project_id):
  cnx1_sql = ('UPDATE phase_a5 '
              'SET '
              'DYHHW_1_1=%s,DYHHW_1_2=%s,DYHHW_1_3=%s,'
              'DYHHW_1_4=%s,DYHHW_2_1=%s,DYHHW_2_2=%s,'
              'DYHHW_2_3=%s,DYHHW_2_4=%s,DYHHW_2_5=%s,'
              'DYHHW_2_6=%s,DYHHW_3_1=%s,DYHHW_3_2=%s,'
              'DYHHW_3_3=%s,DYHHW_3_4=%s,DYHHW_3_5=%s '
              'WHERE id=%s')
  cnx1_param = (row_data[139], row_data[140], row_data[141], 
                row_data[142], row_data[143], row_data[144], 
                row_data[145], row_data[146], row_data[147], 
                row_data[148], row_data[149], row_data[150], 
                row_data[151], row_data[152], row_data[153], 
                project_id)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def phase_a5_hlj_gygtfw(row_data, project_id):
  cnx1_sql = ('UPDATE phase_a5 '
              'SET '
              'GYGTFW_1_1=%s,GYGTFW_1_2=%s,GYGTFW_1_3=%s,'
              'GYGTFW_1_4=%s,GYGTFW_2_1=%s,GYGTFW_2_2=%s,'
              'GYGTFW_2_3=%s,GYGTFW_2_4=%s,GYGTFW_2_5=%s,'
              'GYGTFW_2_6=%s,GYGTFW_3_1=%s,GYGTFW_3_2=%s,'
              'GYGTFW_3_3=%s,GYGTFW_3_4=%s,GYGTFW_3_5=%s '
              'WHERE id=%s')
  cnx1_param = (row_data[154], row_data[155], row_data[156], 
                row_data[157], row_data[158], row_data[159], 
                row_data[160], row_data[161], row_data[162], 
                row_data[163], row_data[164], row_data[165], 
                row_data[166], row_data[167], row_data[168], 
                project_id)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def phase_a5_hlj_qtxg1(row_data, project_id):
  cnx1_sql = ('UPDATE phase_a5 '
              'SET '
              'QTXG1_1_1=%s,QTXG1_1_2=%s,QTXG1_1_3=%s,'
              'QTXG1_1_4=%s,QTXG1_2_1=%s,QTXG1_2_2=%s,'
              'QTXG1_2_3=%s,QTXG1_2_4=%s,QTXG1_2_5=%s,'
              'QTXG1_2_6=%s,QTXG1_3_1=%s,QTXG1_3_2=%s,'
              'QTXG1_3_3=%s,QTXG1_3_4=%s,QTXG1_3_5=%s '
              'WHERE id=%s')
  cnx1_param = (row_data[169], row_data[170], row_data[171], 
                row_data[172], row_data[173], row_data[174], 
                row_data[175], row_data[176], row_data[177], 
                row_data[178], row_data[179], row_data[180], 
                row_data[181], row_data[182], row_data[183], 
                project_id)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def phase_a5_hlj_qtxg2(row_data, project_id):
  cnx1_sql = ('UPDATE phase_a5 '
              'SET '
              'QTXG2_1_1=%s,QTXG2_1_2=%s,QTXG2_1_3=%s,'
              'QTXG2_1_4=%s,QTXG2_2_1=%s,QTXG2_2_2=%s,'
              'QTXG2_2_3=%s,QTXG2_2_4=%s,QTXG2_2_5=%s,'
              'QTXG2_2_6=%s,QTXG2_3_1=%s,QTXG2_3_2=%s,'
              'QTXG2_3_3=%s,QTXG2_3_4=%s,QTXG2_3_5=%s '
              'WHERE id=%s')
  cnx1_param = (row_data[184], row_data[185], row_data[186], 
                row_data[187], row_data[188], row_data[189], 
                row_data[190], row_data[191], row_data[192], 
                row_data[193], row_data[194], row_data[195], 
                row_data[196], row_data[197], row_data[198], 
                project_id)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def phase_a5_hlj_qtxg3(row_data, project_id):
  cnx1_sql = ('UPDATE phase_a5 '
              'SET '
              'QTXG3_1_1=%s,QTXG3_1_2=%s,QTXG3_1_3=%s,'
              'QTXG3_1_4=%s,QTXG3_2_1=%s,QTXG3_2_2=%s,'
              'QTXG3_2_3=%s,QTXG3_2_4=%s,QTXG3_2_5=%s,'
              'QTXG3_2_6=%s,QTXG3_3_1=%s,QTXG3_3_2=%s,'
              'QTXG3_3_3=%s,QTXG3_3_4=%s,QTXG3_3_5=%s '
              'WHERE id=%s')
  cnx1_param = (row_data[199], row_data[200], row_data[201], 
                row_data[202], row_data[203], row_data[204], 
                row_data[205], row_data[206], row_data[207], 
                row_data[208], row_data[209], row_data[210], 
                row_data[211], row_data[212], row_data[213], 
                project_id)
  cnx1_cursor.execute(cnx1_sql, cnx1_param)
  cnx1.commit()

def phase_a5_hlj_qtxg4(row_data, project_id):
  cnx1_sql = ('UPDATE phase_a5 '
              'SET '
              'QTXG4_1_1=%s,QTXG4_1_2=%s,QTXG4_1_3=%s,'
              'QTXG4_1_4=%s,QTXG4_2_1=%s,QTXG4_2_2=%s,'
              'QTXG4_2_3=%s,QTXG4_2_4=%s,QTXG4_2_5=%s,'
              'QTXG4_2_6=%s,QTXG4_3_1=%s,QTXG4_3_2=%s,'
              'QTXG4_3_3=%s,QTXG4_3_4=%s,QTXG4_3_5=%s '
              'WHERE id=%s')
  cnx1_param = (row_data[214], row_data[215], row_data[216], 
                row_data[217], row_data[218], row_data[219], 
                row_data[220], row_data[221], row_data[222], 
                row_data[223], row_data[224], row_data[225], 
                row_data[226], row_data[227], row_data[228],
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
              'FROM tbs001_huanjing '
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
      cnx1_sql = 'SELECT COUNT(*) FROM phase_a5 WHERE id=%s'
      cnx1_param = (project_id,)
      cnx1_cursor.execute(cnx1_sql, cnx1_param)
      cnx1_row = cnx1_cursor.fetchone()
      if cnx1_row[0] == 0:
        phase_a5_hlj_insert(cnx2_row, project_id)
        phase_a5_hlj_fs(cnx2_row, project_id)
        phase_a5_hlj_hxxyl(cnx2_row, project_id)
        phase_a5_hlj_ad(cnx2_row, project_id)
        phase_a5_hlj_sy(cnx2_row, project_id)
        phase_a5_hlj_fq(cnx2_row, project_id)
        phase_a5_hlj_eyhl(cnx2_row, project_id)
        phase_a5_hlj_ych(cnx2_row, project_id)
        phase_a5_hlj_gyfm(cnx2_row, project_id)
        phase_a5_hlj_dyhhw(cnx2_row, project_id)
        phase_a5_hlj_gygtfw(cnx2_row, project_id)
        phase_a5_hlj_qtxg1(cnx2_row, project_id)
        phase_a5_hlj_qtxg2(cnx2_row, project_id)
        phase_a5_hlj_qtxg2(cnx2_row, project_id)
        phase_a5_hlj_qtxg4(cnx2_row, project_id)
      else:
        pass
      print funx.get_time(), '编号', project_id, '添加完毕'

  print funx.get_time(), '所有数据添加完毕'
  cnx1_cursor.close()
  cnx1.close()
  cnx2_cursor.close()
  cnx2.close()