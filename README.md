# 项目名称
migrate
## 简介

基于datax的数据迁移工具（mysql->postgresql）

## 特性

- **功能1**: 支持多表迁移
- **功能2**: 支持全量数据同步，断点重试同步
- **功能3**: 支持postgresql的json字段类型

## 安装
   ```bash
   git clone http://gitlab.info.dbappsecurity.com.cn/ics-base/das-migrate.git
   ```

### 前提条件

确保你在开始之前已满足以下条件：

- 条件1 jdk1.8
- 条件2 python2.7.5

### 项目结构 
- **bin/**: 存放可执行文件和启动脚本。
- **conf/**: 配置文件目录
- **job/**: 执行job的配置文件目录
- **lib/**: jar赖库
- **log/**: datax 运行日志
- **migrate/**: 数据迁移配置文件目录
  - ***migrate-conf.json*** : 主要的配置文件
- **plugin/**: 读写插件
  - reader
    - mysqlreader
  - writer
    - postgresqlwriter
- **script/**: 主脚本目录
  - main.py

***migrate-conf.json*** 配置介绍

```json
{
  "channel" : "3", #通道数
  "source": {
    "mysql_host": "192.168.34.188", #数据库地址
    "mysql_port": "3306",  #数据库端口
    "mysql_username": "dbapp", #数据库用户名
    "mysql_password": "jY%kng8cc&", #数据库密码
    "schema" : "database" #数据库名
  },
  "target": {
    "hg_host": "192.168.34.224", #目标数据库地址
    "hg_port": "15866", #目标数据库端口
    "hg_username": "dbapp", #目标数据库用户名
    "hg_password": "jY%kng8cc&", #目标数据库密码
    "hg_database" : "database", #目标数据库名
    "schema" : "schema" #目标数据库schema
  },
  "mappings": [ # 迁移多表配置
    {
      "source":{
        "column": ["column1","column2"],
        "table":"table1"
      },
      "target":{
        "column": ["column1","column2"],
        "table":"table1",
        "pre_sql" : ["truncate table table1;"]
      },
      "remark" : "表名称"
    }
  ]
}
```
**注意：读表和写表的关键字**

### 操作步骤
```text
1: 修改migrate-conf.json配置文件
2: 执行main.py脚本
```

