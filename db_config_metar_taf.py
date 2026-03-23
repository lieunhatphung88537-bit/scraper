# -*- coding: utf-8 -*-
"""
METAR/TAF 数据库配置（向后兼容）
实际配置从 db_config 读取
"""
from db_config import DATABASES
DB_CONFIG = DATABASES['metar_taf']
