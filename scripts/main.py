#!usr/bin/env python
# -*- coding: utf-8 -*-


## 脚本说明 ---------------------------------------------------------------------
# Author: jiar
# Name: bt demo
# Usage: a simple demo for BiTool projects
# Versions:
# ver_0.1, init, Jan 21th, 2018


## 模块导入 & 参数传递 -----------------------------------------------------------
# 导入模块
import os
import sys

# 导入公用工具箱模块
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from modules.modules import BiTool

# 若传参成功则为工具箱任务，失败则为测试脚本
try:
    __,data_path,job_id,arg1,arg2 = sys.argv
    is_bitool = True
except:
    data_path = os.path.join(os.path.dirname(__file__), 'test_data_path')
    job_id = 'test'
    arg1 = 'arg1'
    arg2 = 'arg2'
    is_bitool = False

## 函数定义 ---------------------------------------------------------------------
# 函数1
def fun1():
    bt.log_output(arg1)

# 函数2
def fun2():
    bt.log_output(arg2)

## 函数调用 ---------------------------------------------------------------------
# 主调用函数
def main():
    bt.log_debug('step1:')
    fun1()
    bt.log_debug('step2:')
    fun2()

if __name__ == '__main__':
    # 获取工作路径（以及其他必要的环境参数）
    work_path = os.path.abspath(os.path.join(os.path.abspath(__file__),'..'))   # 工作路径

    # 按是否为工具箱任务，调整参数
    if is_bitool:
        database = 'bitool'
    else:
        database = 'jiar'

    # 调用BiTool进行环境初始化
    bt = BiTool(database,work_path,data_path,job_id)

    # 自动初始化，返回统一操作路径、文件
    table_prefix = bt.table_prefix  # 统一表前缀
    tmp_path = bt.tmp_path  # 临时目录，用于"insert overwrite"等临时目录操作
    result_path = bt.result_path    # 结果目录，用于存储中间结果文件
    output_path = bt.output_path    # 输出目录，用于存储最终输出结果文件，一般不需要自己操作，可以调用output方法，统一打包输出
    log_debug_file_path = bt.log_debug_file_path    # debug日志文件，位于结果目录下，由log_debug方法写入，记录过程状态、报错等信息
    log_output_file_path = bt.log_output_file_path # 输出日志文件，位于输出目录下，有log_output方法写入，记录输出信息

    # 运行主函数
    main()

    # 调用output方法输出选定的结果文件
    output_files = []    # 需要自定义，由结果目录转入输出目录并打包的文件列表
    bt.output(output_files)

    # 执行清理函数（可自定义清理行为）并记录整个脚本运行时长
    bt.close()
