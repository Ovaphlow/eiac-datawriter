# -*- coding:utf-8 -*-

import globalvars
import jshdw
import staff
import pgdw

if __name__ == '__main__':
  print globalvars.get_time(), '开始运行'

  jshdw.run()

  staff.run()

  pgdw.run()

  print globalvars.get_time(), '所有数据添加完毕'
