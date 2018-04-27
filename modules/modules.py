#!usr/bin/env python
# -*- coding: utf-8 -*-

"""
Title: Modules for BiTool Templates
Description: free developers from repeated work
Author: Jerry Jia
Version: 0.1_beta
Created on 20170101
Updated on 20180417
"""


import os
import sys
import logging
import logging.config
import re
from datetime import datetime
import time


#==============================================================================
# BiTool模板 - 公用类
#==============================================================================
# 定义BiTool父类，所有自定义子类均继承自BiTool
class BiTool(object):
    """BiTool父类，抽象工具箱通用属性和方法，包括日志、环境部署、结果处理等"""
    def __init__(self,database,work_path,data_path,job_id):
        self.start_time = time.time()
        self.database = database
        self.work_path = work_path
        self.data_path = data_path
        self.job_id = job_id

        today_str = datetime.now().strftime('%m%d')
        self.table_prefix = '{database}.{today_str}_{job_id}'.format(
            database=database,today_str=today_str,job_id=job_id
        )

        try:
            self.__env_init()
            self.__log_init()
        except Exception as e:
            # print(e)
            pass

    # 环境初始化
    def __env_init(self):
        self.tmp_path = os.path.join(self.data_path,'tmp_%s' % self.job_id)
        self.result_path = os.path.join(self.data_path,'result_%s' % self.job_id)
        self.output_path = os.path.join(self.data_path,'output_%s' % self.job_id)

        for dir_path in [self.tmp_path,self.result_path,self.output_path]:
            if os.path.exists(dir_path):
                __import__('shutil').rmtree(dir_path)

            os.makedirs(dir_path)

    # 日志初始化
    def __log_init(self):
        # debug日志，在result文件夹下，只对开发者开放
        self.log_debug_file_path = os.path.join(self.result_path,'debug.log')

        # ouput日志，在output文件夹下，只存放包括debug日志路径等信息，对用户开放
        self.log_output_file = 'output.log'
        self.log_output_file_path = os.path.join(self.output_path,self.log_output_file)

        sh_str = "echo 'Info:\ndebug log path: %s\n\nResult:' >> %s" % (self.log_debug_file_path,self.log_output_file_path)
        os.system(sh_str)

        logging_config = LOGGING_CONFIG
        logging_config['handlers']['log']['filename'] = self.log_debug_file_path
        logging.config.dictConfig(logging_config)

        self.logger = logging.getLogger('default')

    # debug日志输出
    def log_debug(self,msg,level='debug'):
        if level == 'debug':
            sys.stderr.write(msg)
            self.logger.debug(msg)
        else:
            pass

    # ouput日志输出
    def log_output(self,msg):
        with open(self.log_output_file_path,'a+') as f:
            f.write(msg+'\n')

    # 检查模板表依赖
    def check_dependency(self,table_list=None):
        check_failed_list = []

        if table_list == None:
            table_list = []

        for table_name in table_list:
            sql_str = """hive -e "use {};
                desc {};
            "
            """.format(database, table_name)
            output = os.popen(sql_str)

            if 'Table not found' in output:
                check_failed_list.append(table_name)

        if len(check_failed_list) == 0:
            return True
        else:
            return False

    # 工作流
    def pipeline(self):
        pass

    # 打包压缩输出文件夹
    def __zip_output_dir(self):
        zipped_file_path = os.path.join(self.data_path,'bitool_result_%s.tar.gz' % self.job_id)

        sh_str = """
            if [[ -f {file_path} ]];then
                rm {file_path}
            fi
            tar -czvf {file_path} -C {output_path} .
            """.format(file_path=zipped_file_path,output_path=self.output_path)

        os.system(sh_str)

    # 将指定文件打包压缩（必须在result_path文件夹下）
    def output(self,file_list=None):
        if file_list is None:
            file_list = []

        for file in os.listdir(self.result_path):
            if file in file_list:
                file_path = os.path.join(self.result_path,file)
                sh_str = """cp -f {} {}
                """.format(file_path,self.output_path)

                os.system(sh_str)

        self.__zip_output_dir()

    # 清理
    def clear(self):
        pass

    # 关闭
    def close(self):
        self.clear()

        self.end_time = time.time()
        time_dura = self.end_time - self.start_time
        self.log_debug('time elapsed: %s' % time_dura)
        self.log_debug('all mission completed')

# 广告线子类
class BiToolAd(BiTool):
    pass


#==============================================================================
# BiTool模板 - 公用配置 & 函数
#==============================================================================
# 定义遍历类（支持对不存在键值的索引）
class cust_dict(dict):
    def __getitem__(self,item):
        try:
            return dict.__getitem__(self,item)
        except KeyError:
            value = self[item] = type(self)()
            return value


# 定义获取表最近可用分区函数（按hour/day/month等数字编码分区）
def get_latest_table_partition(full_table_name,part_type='day',low_thre=100):
    sql_str = """hive -e "show partitions %s;"
    """ % full_table_name
    output = os.popen(sql_str).read()
    lines = [line.strip() for line in output.split('\n') if len(line.strip())>0]

    pattern = re.compile(r'.*{}=(\d+)'.format(part_type))

    partitions = []

    for line in lines:
        try:
            partition = re.match(pattern,line).groups()[0]
            partitions.append(partition)
        except:
            continue

    partition_counts = len(set(partitions))

    if partition_counts == 0:
        return (0, None)
    else:
        partitions_sorted = sorted(partitions, reverse=True)

        for partition in partitions_sorted[:10]:
            sql_str = """hive -e "
                select *
                from {full_table_name}
                where {part_type}='{partition}'
                limit {low_thre};
            "
            """.format(full_table_name=full_table_name,part_type=part_type,
                partition=partition,low_thre=low_thre
            )
            output = os.popen(sql_str).read()
            row_num = len(output.split('\n'))

            if row_num >= low_thre:
                return (partition_counts, partition)
            else:
                continue

        return (partition_counts, None)


# 定义日志配置字典，默认为debug级别
LOGGING_CONFIG = cust_dict({
    'version':1, #日志级别
    'disable_existing_loggers':False, #是否禁用现有的记录器

    #日志格式集合
    'formatters':{
        'standard':{
            #[具体时间][线程名:线程ID][日志名字:日志级别名称(日志级别ID)] [输出的模块:输出的函数]:日志内容
            'format':'[%(asctime)s][%(name)s:%(levelname)s(%(lineno)d)]\n[%(module)s:%(funcName)s]:%(message)s'
        }
    },

    #过滤器
    'filters':{
    },

    #处理器集合
    'handlers':{
        #输出到文件
        'log':{
            'level':'DEBUG',
            'class':'logging.handlers.RotatingFileHandler',
            'formatter':'standard',
            'filename':'debug.log', #输出位置
            'maxBytes':1024*1024*5, #文件大小 5M
            'backupCount': 5, #备份份数
            'encoding': 'utf8', #文件编码
        },
    },

    #日志管理器集合
    'loggers':{
        #管理器
        'default':{
            'handlers':['log'],
            'level':'DEBUG',
            'propagate':True, #是否传递给父记录器
        },
    }
})
