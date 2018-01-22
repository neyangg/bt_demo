#!usr/bin/env python
# -*- coding: utf-8 -*-


## 脚本说明 ---------------------------------------------------------------------
# Author: jiar
# Name: bt demo - setup
# Usage: auto gen az.job file
# Versions:
# ver_0.1, init, Jan 21th, 2018


## 模块导入 & 参数传递 -----------------------------------------------------------
# 导入模块
import os
import sys

try:
    import zipfile
except:
    print('please make sure you have zipfile pre-installed')
    sys.exit()


## 函数定义 ---------------------------------------------------------------------
# 自动生成az.job文件
def gen_az_job_file(script_file_path,az_job_file_path):
    with open(script_file_path, 'r') as f:
        lines = f.readlines()
    try:
        line_argv = [line for line in lines if 'sys.argv' in line][0].strip()
    except:
        pass

    argvs = line_argv.split('=')[0].split(',')[1:]
    argvs[:2] = ['dataFilePath','jobId']
    argvs_str = ''.join([' ${%s}'%argv.strip() for argv in argvs])
    # print(argvs_str)
    az_template_str = """type=command\ncommand=python2.7 scripts/main.py {}
    """.format(argvs_str)

    with open(az_job_file_path,'w') as f:
        f.write(az_template_str)

# 自动生成run.py文件
def gen_run_file(script_file_path,fun_file_path):
    with open(script_file_path, 'r') as f:
        lines = f.readlines()
    try:
        line_argv = [line for line in lines if 'sys.argv' in line][0].strip()
    except:
        pass
    argvs = line_argv.split('=')[0].split(',')[3:]
    comma_argvs_str = ','.join([argv.strip() for argv in argvs])
    flowOverride_argvs_str = ','.join(['flowOverride[%s]'%argv.strip() for argv in argvs])

    run_template_str = """import sys\n__,{comma_argvs_str} = sys.argv\ndef run({flowOverride_argvs_str}):\n    return\ndef main():\n    run({comma_argvs_str})\n    return\nmain()
    """.format(comma_argvs_str=comma_argvs_str,flowOverride_argvs_str=flowOverride_argvs_str)
    # print(run_template_str)

    with open(fun_file_path,'w') as f:
        f.write(run_template_str)

# 自动打包为azkaban工程
def zip_az_project(root_path):
    file_filter_list = ['.DS_Store','DS_Store','bt_demo.zip']

    root_name = os.path.basename(root_path)
    zf_path = os.path.join(root_path, root_name + ".zip")
    zf = zipfile.ZipFile(zf_path, 'w', zipfile.ZIP_DEFLATED)

    for cur_root_path, dir, files in os.walk(root_path):
        rel_path = os.path.relpath(cur_root_path, start=root_path)
        cur_root_name = os.path.basename(cur_root_path)
        if cur_root_name.startswith('.'):
            continue
        for file in files:
            if file in file_filter_list or file.startswith('.'):
                continue
            file_path = os.path.join(cur_root_path, file)
            zf.write(file_path, arcname=os.path.join(rel_path,file))

    zf.close()

## 主函数调用 -------------------------------------------------------------------
if __name__ == '__main__':
    root_path = os.path.abspath(os.path.dirname(__file__))
    script_file_path = os.path.abspath(os.path.join(root_path, 'scripts', 'main.py'))
    az_job_file_path = os.path.abspath(os.path.join(root_path, '%s.job'%os.path.basename(root_path)))
    run_file_path = os.path.abspath(os.path.join(root_path, 'run.py'))
    gen_az_job_file(script_file_path,az_job_file_path)
    gen_run_file(script_file_path,run_file_path)
    zip_az_project(root_path)
