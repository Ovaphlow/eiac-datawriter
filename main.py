# -*- coding:utf-8 -*-

import sys
import globalvars
import jshdw
import staff

reload(sys)
sys.setdefaultencoding("utf-8")

if __name__ == '__main__':
  print globalvars.get_time(), '开始运行'

  jshdw.run()

  staff.run()

  print globalvars.get_time(), '所有数据添加完毕'
