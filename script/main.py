#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/10/21 10:00
# @Author  : Sean.deng
# @Site    :
# @File    : main.py
# @Software: PyCharm

import json
import os
import subprocess

# 首先读取迁移配置文件 转换成多个job
Basedir = os.path.split(os.path.realpath(__file__))[0]
DataxBindir = "{}/../bin".format(Basedir)
MigrateConfigdir = "{}/../migrate".format(Basedir)
JobConfigdir = "{}/../job".format(Basedir)

logDir = "{}/../log/migrate".format(Basedir)

migrate_config_file = "{}/migration-conf.json".format(MigrateConfigdir)

with open(migrate_config_file, "r") as f:
    migrate_config = json.load(f)
    
channel = migrate_config["channel"]
# source
mysql_host = migrate_config["source"]["mysql_host"]
mysql_port = migrate_config["source"]["mysql_port"]
mysql_username = migrate_config["source"]["mysql_username"]
mysql_password = migrate_config["source"]["mysql_password"]
mysql_schema = migrate_config["source"]["schema"]

# target
hg_host = migrate_config["target"]["hg_host"]
hg_port = migrate_config["target"]["hg_port"]
hg_username = migrate_config["target"]["hg_username"]
hg_password = migrate_config["target"]["hg_password"]
hg_schema = migrate_config["target"]["schema"]

hg_database = migrate_config["target"]["hg_database"]

try:
    # 检查database是否存在且不为空
    if "target" not in migrate_config and 'hg_database' not in migrate_config and not migrate_config["target"]["hg_database"]:
        raise ValueError("Please set hg_database in target and ensure it has a valid value")

except ValueError as e:
    print("error:", e)

mappings = migrate_config["mappings"]
try:
    # 检查mappings是否存在且不为空
    if "mappings" not in migrate_config or not migrate_config["mappings"]:
        raise ValueError("mappings do not exist or are empty")

except ValueError as e:
    print("error:", e)
    
# 拼接jdbc
mysql_jdbcUrl="jdbc:mysql://{}:{}/{}?useSSL=false&autoReconnect=true&autoReconnectForPools=true&useUnicode=true&characterEncoding=utf8&createDatabaseIfNotExist=true&allowMultiQueries=true&serverTimezone=UTC".format(mysql_host,mysql_port,mysql_schema)
hg_jdbcUrl="jdbc:postgresql://{}:{}/{}?currentSchema={}&autoReconnect=true&autoReconnectForPools=true&useUnicode=true&characterEncoding=utf8&createDatabaseIfNotExist=true&allowMultiQueries=true&zeroDateTimeBehavior=convertToNull".format(hg_host,hg_port,hg_database,hg_schema)

# 清理job目录
def clean_job_config_dir():
    # 遍历JobConfigdir目录中的所有文件
    for filename in os.listdir(JobConfigdir):
        # 获取文件的完整路径
        file_path = os.path.join(JobConfigdir, filename)
        # 如果文件是一个普通文件，则删除它
        if os.path.isfile(file_path):
            os.remove(file_path)
    print("job director clean done！")


# 定义一个函数，用于获取连接
def get_connection(table, model):
    # 如果model参数为"source"，则使用mysql的jdbcUrl
    if model == "source":
        jdbc = mysql_jdbcUrl
        # 构造连接配置
        connection_config = {
            "connection" : [
                {
                    "jdbcUrl": [jdbc],
                    "table": [table]
                }
            ]
        }
        # 将连接配置转换为json格式并返回
        return json.loads(json.dumps(connection_config, indent=4))
    # 如果model参数为"target"，则使用hg的jdbcUrl
    elif model == "target":
        jdbc = hg_jdbcUrl
        # 构造连接配置
        connection_config = {
            "connection" : [
                {
                    "jdbcUrl": jdbc,
                    "table": [table]
                }
            ]
        }
        # 将连接配置转换为json格式并返回
        return json.loads(json.dumps(connection_config, indent=4))
    # 如果model参数无效，则抛出ValueError异常
    else:
        raise ValueError("无效的model参数")
    
    
def get_base_parameter(connection, column, model):
    parameter = {}
    parameter["connection"] = connection["connection"]
    parameter["column"] = column
    
    if model == "source":
        username = mysql_username
        password = mysql_password
    elif model == "target":
        username = hg_username
        password = hg_password
    else:
        raise ValueError("无效的model参数")
    
    parameter["username"] = username
    parameter["password"] = password
    #print(json.dumps(parameter))
    return json.loads(json.dumps(parameter))

def generate_job_conf () :
    """
    生成迁移JOB配置文件
    """

    job_file_names = []
    # 遍历mappings数组
    for mapping in migrate_config["mappings"]:

        sourceTable = mapping["source"]["table"]
        targetTable = mapping["target"]["table"]

        # 加入判断，查看log/migrate 中是已经存在对应执行成功的log，如果存在则不生成log
        success_job_file_name = "{}.log".format(sourceTable)
        success_job_file_path = os.path.join(logDir, success_job_file_name)
        if os.path.exists(success_job_file_path):
            print("The file {} exists.".format(success_job_file_path))
            continue  # 跳过当前循环，开始下一次循环

        print("-" * 100)
        print("Generate table Source:[{}] -> Target:[{}] job conf start".format(sourceTable, targetTable))
        result = {}
        ready_job = {}
        ready_job_content_obj = {}
        ready_job_content = []

        # 开始拼接reader
        reader_name = "mysqlreader"
        reader = {}
        reader_content = {}
        reader_parameter = {}

        reader_connection = get_connection(sourceTable, "source")
        reader_parameter = get_base_parameter(reader_connection, mapping["source"]["column"], "source")
        # todo 其他的reader参数
        reader["name"] = reader_name
        reader["parameter"] = reader_parameter
        reader_content["reader"] = reader

        # 开始拼装writer
        writer_name = "postgresqlwriter"
        writer = {}
        writer_content = {}
        writer_parameter = {}

        writer_connection = get_connection(targetTable, "target")
        writer_parameter = get_base_parameter(writer_connection, mapping["target"]["column"], "target")

        # pre_sql处理
        if 'pre_sql' in mapping['target']:
            pre_sql = mapping["target"]["pre_sql"]
            writer_parameter["preSql"] = pre_sql

        # todo 其他的writer参数
        writer["name"] = writer_name
        writer["parameter"] = writer_parameter
        writer_content["writer"] = writer

        ready_job_content_obj = reader_content
        ready_job_content_obj.update(writer_content)
        #print(json.dumps(ready_job_content_obj))

        ready_job_content.append(ready_job_content_obj)

        ready_job["content"] = ready_job_content

        # 加入settings
        settings_config = {
            "setting" : {
                "speed": {
                    "channel": channel
                }
            }
        }
        ready_job["setting"] = settings_config["setting"]

        result["job"] = ready_job
        #print(json.dumps(result))

        job_file_name = "migrate-{}-job.json".format(sourceTable)
        job_file_path = os.path.join(JobConfigdir, job_file_name)
        #print(json.dumps(result, f, indent=4, sort_keys=False))

        #写入到文件
        with open(job_file_path, 'w') as file:
            file.write(json.dumps(result, f, indent=4, sort_keys=False))

        job_file_names.append(job_file_name)

        print("Generate table Source:[{}] -> Target:[{}] job conf end".format(sourceTable, targetTable))
        print("-" * 100)
    return job_file_names

# 对job目录做清理
clean_job_config_dir()

# 生成对应表的job配置文件
job_array = generate_job_conf()

datax_path = "{}/datax.py".format(DataxBindir)
# 遍历job_array
for job in job_array:
    # 执行job，并且输出到日志中
    job_file_path = os.path.join(JobConfigdir, job)

    # 使用split方法截取字符串
    job_parts = job.split('-')
    # 去掉migrate
    if job_parts[0] == 'migrate':
        job_parts = job_parts[1:]

    # 去掉后缀
    table_job_name = job_parts[0].rsplit('.', 1)[0]

    migrate_job_log = os.path.join(logDir, "{}.log".format(table_job_name))
    print(migrate_job_log)

    isError = 0
    # 打开标准输出文件和标准错误文件
    with open(migrate_job_log, 'w') as stdout_file :
        # 执行命令并将标准输出和标准错误分别重定向到文件
        result = subprocess.Popen(['python', datax_path, job_file_path], stdout=stdout_file, stderr=subprocess.PIPE)
        (out, err) = result.communicate()

        if err:
            print("#######Error executing#######")
            # 删除标准输出文件
            isError = 1

    # 判断 isError 是否为 1
    if isError == 1:
        try:
            # 删除文件
            os.remove(migrate_job_log)
            print("File {} has been deleted. please go to datax\log".format(migrate_job_log))
            # 停止程序
            break
        except OSError as e:
            print("Error!!")
