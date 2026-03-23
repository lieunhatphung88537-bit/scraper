# -*- coding: utf-8 -*-
"""
AWS RDS 多数据库统一配置文件
所有爬虫只需导入这一个配置文件
"""
import os

# 从环境变量读取 RDS 连接信息host.docker.internal
#RDS_HOST = os.getenv('DB_HOST', '127.0.0.1')     #本地运行测试
#RDS_HOST = os.getenv('DB_HOST', 'host.docker.internal')  #本地测试docker
RDS_HOST = os.getenv('DB_HOST', 'YOUR_RDS_ENDPOINT_HERE')
RDS_PORT = int(os.getenv('DB_PORT', '5432'))
RDS_USER = os.getenv('DB_USER', 'postgres')
RDS_PASSWORD = os.getenv('DB_PASSWORD', 'YOUR_PASSWORD_HERE')

# 各数据库配置（同一 RDS 实例的不同数据库）
DATABASES = {
    'noaa': {
        'host': RDS_HOST,
        'port': RDS_PORT,
        'database': 'noaa_data',
        'user': RDS_USER,
        'password': RDS_PASSWORD,
        'connect_timeout': 120,
        'keepalives': 1,
        'keepalives_idle': 30,
        'keepalives_interval': 10,
        'keepalives_count': 5,
        'options': '-c statement_timeout=600000'
    },
    'metar_taf': {
        'host': RDS_HOST,
        'port': RDS_PORT,
        'database': 'metar_taf_synop_data',
        'user': RDS_USER,
        'password': RDS_PASSWORD,
        'connect_timeout': 120,
        'keepalives': 1,
        'keepalives_idle': 30,
        'keepalives_interval': 10,
        'keepalives_count': 5,
        'options': '-c statement_timeout=600000'
    },
    'typhoon': {
        'host': RDS_HOST,
        'port': RDS_PORT,
        'database': 'typhoon_data',
        'user': RDS_USER,
        'password': RDS_PASSWORD,
        'connect_timeout': 120,
        'keepalives': 1,
        'keepalives_idle': 30,
        'keepalives_interval': 10,
        'keepalives_count': 5,
        'options': '-c statement_timeout=600000'
    }
}

# 默认数据库配置（NOAA 爬虫使用）
DB_CONFIG = DATABASES['noaa']

def get_db_config(db_type='noaa'):
    """
    获取指定类型的数据库配置
    
    Args:
        db_type: 'noaa', 'metar_taf', 'typhoon', 'synop'
    
    Returns:
        数据库配置字典
    """
    return DATABASES.get(db_type, DATABASES['noaa'])

# 使用示例
"""
# NOAA 爬虫（默认）
from db_config import DB_CONFIG
conn = psycopg2.connect(**DB_CONFIG)

# METAR/TAF 爬虫
from db_config import DATABASES
conn = psycopg2.connect(**DATABASES['metar_taf'])

# 台风爬虫
from db_config import get_db_config
conn = psycopg2.connect(**get_db_config('typhoon'))
"""
