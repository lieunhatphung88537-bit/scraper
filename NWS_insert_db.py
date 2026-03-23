# 检测站点地理坐标时，需要在文件内添加更多的站点
import psycopg2
from psycopg2 import OperationalError
import logging
import pandas as pd
from datetime import datetime, timezone, timedelta
import os
import requests
import schedule
import time
import numpy as np
import pytz
import concurrent.futures
import threading
from bs4 import BeautifulSoup
import re
import csv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import random
from metar import Metar
import json
import gc
import psutil
import sys
from typing import Dict, List, Set, Optional, Tuple
from apscheduler.schedulers.background import BackgroundScheduler

script_dir = os.path.dirname(os.path.abspath(__file__))
local_log_dir = os.path.join(script_dir, 'logs')
# Docker环境（Linux）使用 /logs，Windows本地使用脚本所在目录的 logs
if sys.platform == 'win32':
    log_dir = local_log_dir
else:
    log_dir = '/logs' if os.path.exists('/logs') and os.path.isdir('/logs') else local_log_dir
os.makedirs(log_dir, exist_ok=True)

logger = logging.getLogger('NWS_db')
logger.setLevel(logging.INFO)


class FlushFileHandler(logging.FileHandler):
    def emit(self, record):
        super().emit(record)
        self.flush()


if not logger.handlers:
    fh = FlushFileHandler(os.path.join(log_dir, 'NWS_db.log'), encoding='utf-8')
    fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(fh)
    sh = logging.StreamHandler()
    sh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(sh)

logging = logger  # 兼容原代码中的 logging.info() 调用


class NWS_METAR:
    def __init__(self, db_config=None, partition_days=10):
        if db_config is None:
            try:
                from db_config import get_db_config
                db_config = get_db_config('metar_taf')
            except ImportError:
                import os
                db_config = {
                    'host': os.getenv('DB_HOST', 'localhost'),
                    'port': int(os.getenv('DB_PORT', '5432')),
                    'database': 'metar',
                    'user': os.getenv('DB_USER', 'postgres'),
                    'password': os.getenv('DB_PASSWORD', '123456')
                }
        self.conn_params = db_config
        self.connection = None
        self.is_connected = False
        self.pattern = re.compile(r"(\d{4}/\d{2}/\d{2} \d{2}:\d{2})\s+(\w{4})\s+(.*)")
        self.partition_days = partition_days
        self._table_pattern = re.compile(r'nws_metar_data_(\d{8})_(\d{8})')

        # 内存管理
        self._memory_tracker = {}

        # 缓存表结构（带大小限制）
        self._table_columns_cache = {}
        self._table_exists_cache = {}
        self._created_tables_this_session = set()
        self._max_cache_size = 50  # 最多缓存50个表的信息

        self.headers_list = [
            'Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1',
            'Mozilla/5.0 (Linux; Android 8.0.0; SM-G955U Build/R16NW) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Mobile Safari/537.36',
            'Mozilla/5.0 (Linux; Android 10; SM-G981B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.162 Mobile Safari/537.36',
            'Mozilla/5.0 (iPad; CPU OS 13_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/87.0.4280.77 Mobile Safari/604.1',
            'Mozilla/5.0 (Linux; Android 8.0; Pixel 2 Build/OPD3.170816.012) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Mobile Safari/537.36',
            'Mozilla/5.0 (Linux; Android) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.109 Safari/537.36 CrKey/1.54.248666',
            'Mozilla/5.0 (X11; Linux aarch64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.188 Safari/537.36 CrKey/1.54.250320',
            'Mozilla/5.0 (BB10; Touch) AppleWebKit/537.10+ (KHTML, like Gecko) Version/10.0.9.2372 Mobile Safari/537.10+',
            'Mozilla/5.0 (PlayBook; U; RIM Tablet OS 2.1.0; en-US) AppleWebKit/536.2+ (KHTML like Gecko) Version/7.2.1.0 Safari/536.2+',
            'Mozilla/5.0 (Linux; U; Android 4.3; en-us; SM-N900T Build/JSS15J) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30',
            'Mozilla/5.0 (Linux; U; Android 4.1; en-us; GT-N7100 Build/JRO03C) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30',
            'Mozilla/5.0 (Linux; U; Android 4.0; en-us; GT-I9300 Build/IMM76D) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30',
            'Mozilla/5.0 (Linux; Android 7.0; SM-G950U Build/NRD90M) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.84 Mobile Safari/537.36',
            'Mozilla/5.0 (Linux; Android 8.0.0; SM-G965U Build/R16NW) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.111 Mobile Safari/537.36',
            'Mozilla/5.0 (Linux; Android 8.1.0; SM-T837A) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.80 Safari/537.36',
            'Mozilla/5.0 (Linux; U; en-us; KFAPWI Build/JDQ39) AppleWebKit/535.19 (KHTML, like Gecko) Silk/3.13 Safari/535.19 Silk-Accelerated=true',
            'Mozilla/5.0 (Linux; U; Android 4.4.2; en-us; LGMS323 Build/KOT49I.MS32310c) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/102.0.0.0 Mobile Safari/537.36',
            'Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 550) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2486.0 Mobile Safari/537.36 Edge/14.14263',
            'Mozilla/5.0 (Linux; Android 6.0.1; Moto G (4)) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Mobile Safari/537.36',
            'Mozilla/5.0 (Linux; Android 6.0.1; Nexus 10 Build/MOB31T) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Linux; Android 4.4.2; Nexus 4 Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Mobile Safari/537.36',
            'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Mobile Safari/537.36',
            'Mozilla/5.0 (Linux; Android 8.0.0; Nexus 5X Build/OPR4.170623.006) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Mobile Safari/537.36',
            'Mozilla/5.0 (Linux; Android 7.1.1; Nexus 6 Build/N6F26U) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Mobile Safari/537.36',
            'Mozilla/5.0 (Linux; Android 8.0.0; Nexus 6P Build/OPP3.170518.006) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Mobile Safari/537.36',
            'Mozilla/5.0 (Linux; Android 6.0.1; Nexus 7 Build/MOB30X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36',
            'Mozilla/5.0 (compatible; MSIE 10.0; Windows Phone 8.0; Trident/6.0; IEMobile/10.0; ARM; Touch; NOKIA; Lumia 520)',
            'Mozilla/5.0 (MeeGo; NokiaN9) AppleWebKit/534.13 (KHTML, like Gecko) NokiaBrowser/8.5.0 Mobile Safari/534.13',
            'Mozilla/5.0 (Linux; Android 9; Pixel 3 Build/PQ1A.181105.017.A1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.158 Mobile Safari/537.36',
            'Mozilla/5.0 (Linux; Android 10; Pixel 4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Mobile Safari/537.36',
            'Mozilla/5.0 (Linux; Android 11; Pixel 3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.181 Mobile Safari/537.36',
            'Mozilla/5.0 (Linux; Android 5.0; SM-G900P Build/LRX21T) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Mobile Safari/537.36',
            'Mozilla/5.0 (Linux; Android 8.0; Pixel 2 Build/OPD3.170816.012) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Mobile Safari/537.36',
            'Mozilla/5.0 (Linux; Android 8.0.0; Pixel 2 XL Build/OPD1.170816.004) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Mobile Safari/537.36',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 10_3_1 like Mac OS X) AppleWebKit/603.1.30 (KHTML, like Gecko) Version/10.0 Mobile/14E304 Safari/602.1',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1',
            'Mozilla/5.0 (iPad; CPU OS 11_0 like Mac OS X) AppleWebKit/604.1.34 (KHTML, like Gecko) Version/11.0 Mobile/15A5341f Safari/604.1'
        ]

        self.count_out = 0
        self.max_count_out = 50
        self.consecutive_no_new_data = 0
        self.max_consecutive_no_new_data = 20
        self.counter_lock = threading.Lock()

    # ============ 内存管理方法 ============

    def _track_memory(self, step_name: str) -> None:
        """跟踪内存使用情况"""
        try:
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            self._memory_tracker[step_name] = memory_mb
            logging.debug(f"内存使用跟踪 [{step_name}]: {memory_mb:.2f} MB")
        except ImportError:
            pass

    def _force_gc(self) -> None:
        """强制执行垃圾回收"""
        # 限制缓存大小，防止长期运行时无限增长
        if len(self._table_exists_cache) > self._max_cache_size:
            items = list(self._table_exists_cache.items())
            self._table_exists_cache = dict(items[-self._max_cache_size:])
            logging.debug(f"表存在缓存已清理，保留最近 {self._max_cache_size} 个")

        if len(self._table_columns_cache) > self._max_cache_size:
            items = list(self._table_columns_cache.items())
            self._table_columns_cache = dict(items[-self._max_cache_size:])
            logging.debug(f"表列缓存已清理，保留最近 {self._max_cache_size} 个")

        if len(self._created_tables_this_session) > self._max_cache_size:
            items = list(self._created_tables_this_session)
            self._created_tables_this_session = set(items[-self._max_cache_size:])
            logging.debug(f"创建表记录已清理，保留最近 {self._max_cache_size} 个")

        import gc
        collected = gc.collect()
        logging.debug(f"强制垃圾回收：回收了 {collected} 个对象")

    def _cleanup_large_objects(self, obj_dict: Dict) -> None:
        """清理大型对象"""
        for name, obj in list(obj_dict.items()):
            if isinstance(obj, (pd.DataFrame, list, dict)) and sys.getsizeof(obj) > 1024 * 1024:  # 大于1MB
                del obj_dict[name]
        self._force_gc()

    # ============ 分表管理方法 ============

    def _get_partition_table_name(self, observation_time: datetime) -> str:
        """
        根据观测时间计算分表名

        算法：
        1. 将时间转换为日期
        2. 计算从基准日期（2025-01-01）到该日期的天数
        3. 将天数除以分表间隔（10天）得到分区号
        4. 计算分区的起始和结束日期
        5. 格式化为表名后缀

        例如：2025-12-28 在 2025-12-25 到 2026-01-04 的分区中
        表名：nws_metar_data_20251225_20260104
        """
        # 兼容 pandas.Timestamp
        if isinstance(observation_time, pd.Timestamp):
            observation_time = observation_time.to_pydatetime()

        # 处理时区：先转换为 UTC，再去掉 tzinfo（保证分表按照 UTC 日期边界）
        if observation_time.tzinfo is not None:
            observation_time = observation_time.astimezone(timezone.utc).replace(tzinfo=None)

        base_date = observation_time.date()

        # 计算从基准日期到目标日期的天数
        base_reference = datetime(2025, 1, 1).date()
        days_from_reference = (base_date - base_reference).days

        # 计算分区信息
        partition_num = days_from_reference // self.partition_days
        partition_start_day = partition_num * self.partition_days
        partition_end_day = partition_start_day + self.partition_days - 1

        # 计算实际日期
        start_date = base_reference + timedelta(days=partition_start_day)
        end_date = base_reference + timedelta(days=partition_end_day)

        # 格式化为表名
        table_suffix = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
        return f"nws_metar_data_{table_suffix}"

    def _check_table_exists(self, table_name: str) -> bool:
        """检查表是否存在（带缓存）"""
        # 检查缓存
        if table_name in self._table_exists_cache:
            return self._table_exists_cache[table_name]

        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = %s
                    )
                """, (table_name,))
                exists = cursor.fetchone()[0]

                # 更新缓存
                self._table_exists_cache[table_name] = exists
                return exists
        except Exception as e:
            logging.error(f"检查表是否存在失败: {e}")
            return False

    def _create_partition_table(self, table_name: str) -> bool:
        """创建分表"""
        # 检查表是否已存在
        if self._check_table_exists(table_name):
            return True

        create_table_sql = f"""
        CREATE TABLE {table_name} (
            raw_text TEXT DEFAULT NULL,
            manually_corrected BOOLEAN DEFAULT FALSE,
            station_id VARCHAR(10) DEFAULT NULL,
            observation_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            latitude NUMERIC(9,6) DEFAULT NULL,
            longitude NUMERIC(9,6) DEFAULT NULL,
            temp NUMERIC(9,2) DEFAULT NULL,
            dewpoint NUMERIC(9,2) DEFAULT NULL,
            weather TEXT DEFAULT NULL,
            sky TEXT DEFAULT NULL,
            wind_variable BOOLEAN DEFAULT FALSE,
            wind_dir_degrees NUMERIC(9,1) DEFAULT NULL,
            wind_speed NUMERIC(9,2) DEFAULT NULL,
            wind_gust NUMERIC(9,2) DEFAULT NULL,
            peak_wind_drct NUMERIC(9,1) DEFAULT NULL,
            peak_wind_gust NUMERIC(9,2) DEFAULT NULL, 
            visibility_statute NUMERIC(15,2) DEFAULT NULL,
            altim_in_hg NUMERIC(9,2) DEFAULT NULL,
            sea_level_pressure NUMERIC(9,2) DEFAULT NULL,
            precip NUMERIC(9,2) DEFAULT NULL,
            updatetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(station_id, observation_time)
        );
        """

        create_index_sql = f"""
        CREATE INDEX IF NOT EXISTS idx_{table_name}_obs_time ON {table_name}(observation_time);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_station_id ON {table_name}(station_id);
        """

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(create_table_sql)
                cursor.execute(create_index_sql)
                self.connection.commit()
                logging.info(f"分表 {table_name} 创建成功")

                # 更新缓存
                self._table_exists_cache[table_name] = True
                self._table_columns_cache.pop(table_name, None)  # 清除旧的列缓存

                return True
        except Exception as e:
            logging.error(f"创建分表 {table_name} 失败：{e}")
            self.connection.rollback()
            return False

    def _get_table_columns(self, table_name: str) -> Set[str]:
        """获取指定表的字段列表（带缓存）"""
        # 检查缓存
        if table_name in self._table_columns_cache:
            return self._table_columns_cache[table_name]

        try:
            query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = %s
            ORDER BY ordinal_position
            """
            with self.connection.cursor() as cursor:
                cursor.execute(query, (table_name,))
                results = cursor.fetchall()
                columns = {row[0] for row in results}

                # 更新缓存
                self._table_columns_cache[table_name] = columns
                return columns
        except Exception as e:
            logging.error(f"获取表 {table_name} 字段失败: {e}")
            return set()

    def _get_all_partition_tables(self) -> List[str]:
        """获取所有已存在的分表"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name LIKE 'nws_metar_data_%'
                    ORDER BY table_name
                """)
                tables = [row[0] for row in cursor.fetchall()]
                # 更新缓存
                for table in tables:
                    self._table_exists_cache[table] = True
                return tables
        except Exception as e:
            logging.error(f"获取分表列表失败: {e}")
            return []

    def _batch_insert_to_table(self, data_list: List[Dict], table_name: str) -> int:
        """
        批量插入数据到指定表

        优化点：
        1. 使用VALUES列表一次性插入多条数据
        2. 使用ON CONFLICT处理重复
        3. 分离修正数据和普通数据，分别处理
        """
        if not data_list:
            return 0

        # 确保表存在
        if not self._check_table_exists(table_name):
            if not self._create_partition_table(table_name):
                return 0

        # 获取表的字段
        table_columns = self._get_table_columns(table_name)
        if not table_columns:
            logging.error(f"无法获取表 {table_name} 的字段列表")
            return 0

        # 准备数据：转换为正确的字段顺序
        processed_data = []
        for data_dict in data_list:
            # 只保留表中存在的字段
            filtered_data = {k: v for k, v in data_dict.items() if k in table_columns}
            # 确保manually_corrected字段存在
            if 'manually_corrected' not in filtered_data:
                filtered_data['manually_corrected'] = False
            # 检测手动修正
            if 'raw_text' in filtered_data:
                filtered_data['manually_corrected'] = self._is_manually_corrected(
                    filtered_data['raw_text']
                )
            processed_data.append(filtered_data)

        if not processed_data:
            return 0

        # 分离修正数据和普通数据
        corrected_data = [d for d in processed_data if d.get('manually_corrected')]
        normal_data = [d for d in processed_data if not d.get('manually_corrected')]

        success_count = 0

        try:
            with self.connection.cursor() as cursor:
                # 1. 插入修正数据（需要更新）
                if corrected_data:
                    success_count += self._insert_with_update(cursor, corrected_data, table_name, table_columns)

                # 2. 插入普通数据（仅插入新数据）
                if normal_data:
                    success_count += self._insert_without_update(cursor, normal_data, table_name, table_columns)

                self.connection.commit()

            logging.debug(f"表 {table_name} 插入完成: 成功 {success_count} 条")
            return success_count

        except Exception as e:
            logging.error(f"插入到表 {table_name} 失败: {e}")
            if self.connection:
                self.connection.rollback()
            return 0

    def _insert_with_update(self, cursor, data_list: List[Dict], table_name: str, table_columns: Set[str]) -> int:
        """插入并更新修正数据"""
        if not data_list:
            return 0

        # 构建插入语句
        columns = [col for col in data_list[0].keys() if col in table_columns]
        if not columns:
            return 0

        columns_str = ", ".join(columns)
        placeholders_str = ", ".join(["%s"] * len(columns))

        # 构建更新子句（更新所有字段）
        update_columns = [col for col in columns if col != 'manually_corrected']
        update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns])
        update_clause += ", manually_corrected = TRUE"

        insert_sql = f"""
        INSERT INTO {table_name} ({columns_str})
        VALUES ({placeholders_str})
        ON CONFLICT (station_id, observation_time) 
        DO UPDATE SET {update_clause}
        """

        success_count = 0
        for data_dict in data_list:
            try:
                values = [data_dict.get(col) for col in columns]
                cursor.execute(insert_sql, values)
                success_count += 1
            except Exception as e:
                logging.warning(f"更新修正数据失败: {e}")
                continue

        return success_count

    def _insert_without_update(self, cursor, data_list: List[Dict], table_name: str, table_columns: Set[str]) -> int:
        """仅插入新数据（不更新已存在的）"""
        if not data_list:
            return 0

        # 构建插入语句
        columns = [col for col in data_list[0].keys() if col in table_columns]
        if not columns:
            return 0

        columns_str = ", ".join(columns)
        placeholders_str = ", ".join(["%s"] * len(columns))

        insert_sql = f"""
        INSERT INTO {table_name} ({columns_str})
        VALUES ({placeholders_str})
        ON CONFLICT (station_id, observation_time) 
        DO NOTHING
        """

        success_count = 0
        for data_dict in data_list:
            try:
                values = [data_dict.get(col) for col in columns]
                cursor.execute(insert_sql, values)
                if cursor.rowcount > 0:
                    success_count += 1
            except Exception as e:
                logging.warning(f"插入普通数据失败: {e}")
                continue

        return success_count

    # ============ 原有的数据处理方法（修改为使用分表） ============

    def quick_stop_check(self, stop_event, station=None):
        """快速检查是否应该停止"""
        if stop_event.is_set():
            return True

        with self.counter_lock:
            if self.consecutive_no_new_data >= self.max_consecutive_no_new_data:
                if not stop_event.is_set():
                    logging.info(f"达到连续无新数据阈值 {self.max_consecutive_no_new_data}，触发停止")
                    stop_event.set()
                return True

        with self.counter_lock:
            if self.count_out >= self.max_count_out:
                if not stop_event.is_set():
                    logging.info(f"达到连续无新数据阈值 {self.max_consecutive_no_new_data}，触发停止")
                    stop_event.set()
                return True

        return False

    def process_and_insert_data(self, metar_link, session, stop_event, data_buffer: Dict[str, List[Dict]]):
        """处理并插入单个站点的数据（修改为缓冲模式）"""
        # 开始前检查
        if self.quick_stop_check(stop_event):
            return

        time.sleep(random.uniform(0.1, 0.3))

        metar_url = f"https://tgftp.nws.noaa.gov/data/observations/metar/stations/{metar_link}"

        metar_response = None  # 用于finally块引用

        try:
            # 网络请求前检查
            if self.quick_stop_check(stop_event):
                return

            metar_response = session.get(
                metar_url,
                headers={
                    'Cache-Control': 'no-cache',
                    'Pragma': 'no-cache',
                    'User-Agent': random.choice(self.headers_list)
                },
                timeout=(10, 30)
            )

            # 获取数据后检查
            if self.quick_stop_check(stop_event):
                return

            match = self.pattern.match(metar_response.text.strip())

            if match:
                bizDate_str = match.group(1)
                station = match.group(2)
                data = match.group(3)

                try:
                    # NWS 站点 METAR 文件时间通常为 UTC，按 UTC 语义处理并保持为 naive UTC
                    bizDate_dt = datetime.strptime(bizDate_str, '%Y/%m/%d %H:%M')
                    bizDate_dt = bizDate_dt.replace(tzinfo=None)
                    bizDate_formatted = bizDate_dt.strftime('%Y-%m-%d %H:%M:%S')
                except ValueError:
                    logging.error(f"无法将 bizDate 转换为 datetime: {bizDate_str}")
                    return

                utc_date_minus_one = self.get_utc_date_minus_one()

                if bizDate_dt > utc_date_minus_one:
                    if not self.check_existing_data_in_partition(bizDate_formatted, station):
                        parsed_data = self.parse_metar_data(station, data)

                        if parsed_data:
                            # 重置计数器
                            with self.counter_lock:
                                self.consecutive_no_new_data = 0

                            latitude, longitude = self.get_station_coordinates(station)
                            updatetime_dt = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)

                            if latitude and longitude and bizDate_dt <= updatetime_dt:
                                # 准备插入的数据
                                insert_data = {
                                    'raw_text': parsed_data['raw_data'],
                                    'station_id': parsed_data['station'],
                                    'observation_time': bizDate_dt.replace(microsecond=0),
                                    'latitude': float(latitude),
                                    'longitude': float(longitude),
                                    'temp': float(parsed_data['temperature']) if parsed_data['temperature'] else None,
                                    'dewpoint': float(parsed_data['dew_point']) if parsed_data['dew_point'] else None,
                                    'weather': parsed_data['weather'],
                                    'sky': parsed_data['sky'],
                                    'wind_variable': parsed_data['wind_variable'],
                                    'wind_dir_degrees': float(parsed_data['wind_dir']) if parsed_data[
                                        'wind_dir'] else None,
                                    'wind_speed': float(parsed_data['wind_speed']) if parsed_data[
                                        'wind_speed'] else None,
                                    'wind_gust': float(parsed_data['gust']) if parsed_data['gust'] else None,
                                    'peak_wind_drct': float(parsed_data['peak_wind_dir']) if parsed_data[
                                        'peak_wind_dir'] else None,
                                    'peak_wind_gust': float(parsed_data['peak_wind_speed']) if parsed_data[
                                        'peak_wind_speed'] else None,
                                    'visibility_statute': float(parsed_data['visibility']) if parsed_data[
                                        'visibility'] else None,
                                    'altim_in_hg': float(parsed_data['pressure']) if parsed_data['pressure'] else None,
                                    'sea_level_pressure': float(parsed_data['sea_level_pressure']) if parsed_data[
                                        'sea_level_pressure'] else None,
                                    'precip': float(parsed_data['1_hour_precipitation']) if parsed_data[
                                        '1_hour_precipitation'] else None,
                                    'updatetime': updatetime_dt
                                }

                                # 计算应该插入的表名
                                table_name = self._get_partition_table_name(bizDate_dt)

                                # 添加到缓冲区
                                if table_name not in data_buffer:
                                    data_buffer[table_name] = []
                                data_buffer[table_name].append(insert_data)

                                # 如果缓冲区达到阈值，立即插入
                                if len(data_buffer[table_name]) >= 50:  # 每50条插入一次
                                    inserted_count = self._batch_insert_to_table(data_buffer[table_name], table_name)
                                    if inserted_count > 0:
                                        logging.info(f"批量插入 {inserted_count} 条数据到表 {table_name}")
                                    # 清空缓冲区
                                    data_buffer[table_name] = []

                            else:
                                logging.warning(f"无法获取站点坐标: {station}")

                            # 清理解析的数据
                            del parsed_data
                        else:
                            logging.warning(f"解析METAR数据失败: {station}")
                    else:
                        with self.counter_lock:
                            self.count_out += 1
                        logging.info(f"数据已存在，跳过: {station} - {bizDate_formatted}")
                        self.quick_stop_check(stop_event, station)
                else:
                    with self.counter_lock:
                        self.consecutive_no_new_data += 1
                        current_count = self.consecutive_no_new_data

                    logging.info(f"数据时间过早，不插入: {station} - {bizDate_formatted} (计数: {current_count})")

                    # 立即检查是否达到阈值
                    self.quick_stop_check(stop_event, station)

            # 清理临时匹配对象
            del match

        except requests.exceptions.RequestException as e:
            logging.error(f"请求失败: {metar_url} - {e}")
        except Exception as e:
            logging.error(f"处理站点 {metar_link} 时发生错误: {e}")
        finally:
            # 及时释放response对象
            if metar_response:
                try:
                    metar_response.close()
                    del metar_response
                except:
                    pass

            # 清理parsed_data等大对象
            if 'parsed_data' in locals():
                del parsed_data
            if 'insert_data' in locals():
                del insert_data

            # 安全清理内存
            try:
                self._force_gc()
            except Exception as cleanup_error:
                logging.debug(f"内存清理时发生小错误: {cleanup_error}")

    def connect(self):
        try:
            self.connection = psycopg2.connect(**self.conn_params)
            self.is_connected = True
            logging.info("成功连接到 METAR 数据库")
            return True

        except OperationalError as e:
            logging.error(f"数据库连接失败：{e}")
            self.is_connected = False
            return False

    def ensure_connection(self):
        """确保数据库连接有效"""
        if not self.is_connected or self.connection is None or self.connection.closed:
            logging.warning("数据库连接已断开，尝试重新连接...")
            return self.connect()
        return True

    def close(self):
        if self.connection and not self.connection.closed:
            self.connection.close()
            self.is_connected = False
            logging.info("数据库连接已关闭")

        # 清理缓存
        self._table_columns_cache.clear()
        self._table_exists_cache.clear()
        self._created_tables_this_session.clear()

    def get_utc_date_minus_one(self):
        """获取UTC时间的前一天，返回datetime对象用于比较"""
        utc_timestamp = time.time() - 86400
        utc_struct = time.gmtime(utc_timestamp)
        utc_dt = datetime(
            year=utc_struct.tm_year,
            month=utc_struct.tm_mon,
            day=utc_struct.tm_mday,
            hour=utc_struct.tm_hour,
            minute=utc_struct.tm_min,
            second=utc_struct.tm_sec
        )
        return utc_dt

    def check_existing_data_in_partition(self, bizDate, station):
        """检查数据是否已存在（修改为检查对应的分表）"""
        if not self.ensure_connection():
            return False

        try:
            # 解析时间确定表名
            try:
                bizDate_dt = datetime.strptime(bizDate, '%Y-%m-%d %H:%M:%S')
                table_name = self._get_partition_table_name(bizDate_dt)
            except ValueError:
                logging.error(f"时间格式错误: {bizDate}")
                return False

            # 检查表是否存在
            if not self._check_table_exists(table_name):
                return False

            query = f"""
            SELECT COUNT(*) FROM {table_name} 
            WHERE station_id = %s AND observation_time = %s
            """
            with self.connection.cursor() as cursor:
                cursor.execute(query, (station, bizDate_dt.replace(microsecond=0)))
                count = cursor.fetchone()[0]
                return count > 0
        except Exception as e:
            logging.error(f"检查数据是否存在失败: {e}")
            return False

    def fetch_metar_data(self):
        """获取METAR数据链接列表"""
        url = "https://tgftp.nws.noaa.gov/data/observations/metar/stations/?C=M;O=D"
        try:
            response = requests.get(url, headers={
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache',
                'User-Agent': random.choice(self.headers_list)
            }, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            links = soup.find_all('a')
            metar_links = [link.get('href') for link in links if link.get('href') and link.get('href').endswith('.TXT')]

            # 清理响应对象和BeautifulSoup对象
            soup.decompose()  # 释放BeautifulSoup内部树结构
            response.close()
            del response, soup, links
            self._force_gc()

            return metar_links
        except requests.exceptions.RequestException as e:
            logging.error(f"获取 METAR 数据失败: {e}")
            return []

    def parse_metar_format(self, text):
        """解析METAR格式文本并转换为结构化数据"""
        data = {}
        lines = text.strip().split('\n')

        current_section = None
        sky_lines = []
        remarks_lines = []

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # 处理键值对
            if ':' in line:
                key, value = line.split(':', 1)
                key = key.strip()
                value = value.strip()

                if key == 'station':
                    data['station'] = value
                elif key == 'type':
                    data['type'] = value
                elif key == 'time':
                    data['time'] = value
                elif key == 'temperature':
                    data['temperature'] = value
                elif key == 'dew point':
                    data['dew_point'] = value
                elif key == 'wind':
                    data['wind'] = value
                elif key == 'visibility':
                    data['visibility'] = value
                elif key == 'pressure':
                    data['pressure'] = value
                elif key == 'weather':
                    data['weather'] = value
                elif key == 'sea-level pressure':
                    data['sea_level_pressure'] = value
                elif key == '1-hour precipitation':
                    data['1_hour_precipitation'] = value
                elif key == 'METAR':
                    data['METAR'] = value
                elif key == 'sky':
                    current_section = 'sky'
                    if value:
                        sky_lines.append(value)
                elif key == 'remarks':
                    current_section = 'remarks'
                else:
                    data[key.replace('-', '_').replace(' ', '_')] = value
                    current_section = None

            # 处理列表项（以-开头的行）
            elif line.startswith('- '):
                if current_section == 'remarks':
                    remarks_lines.append(line[2:])
                else:
                    if 'remarks' not in data:
                        data['remarks'] = []
                    data['remarks'].append(line[2:])

            # 处理天空状况的续行
            elif (current_section == 'sky' and
                  any(term in line for term in ['scattered', 'broken', 'overcast', 'clouds'])):
                sky_lines.append(line)

            # 处理其他续行
            elif current_section == 'remarks':
                remarks_lines.append(line)

        # 处理天空状况
        if sky_lines:
            data['sky'] = sky_lines

        # 处理备注
        if remarks_lines:
            data['remarks'] = remarks_lines

        return data

    def parse_metar_string(self, metar_string, metar_obj):
        """解析METAR字符串"""
        json_data = self.parse_metar_format(metar_string)
        parsed_data = {
            'station': None,
            'temperature': None,
            'dew_point': None,
            'wind_variable': False,
            'wind_dir': None,
            'wind_speed': None,
            'peak_wind_dir': None,
            'peak_wind_speed': None,
            'gust': None,
            'visibility': None,
            'pressure': None,
            'sea_level_pressure': None,
            '1_hour_precipitation': None,
            'weather': None,
            'sky': None,
            'raw_data': None,
        }

        try:
            # 如果输入是JSON字符串，先解析为字典
            if isinstance(json_data, str):
                data = json.loads(json_data)
            else:
                data = json_data

            # 解析温度
            if 'temperature' in data:
                temp_match = re.search(r'([-\d.]+)\s*C', data['temperature'])
                if temp_match:
                    parsed_data['temperature'] = temp_match.group(1)

            # 解析露点温度
            if 'dew_point' in data:
                dew_match = re.search(r'([-\d.]+)\s*C', data['dew_point'])
                if dew_match:
                    parsed_data['dew_point'] = dew_match.group(1)

            # 解析风向风速
            if 'wind' in data:
                wind_text = data['wind']
                if metar_obj.wind_dir:
                    original_wind_dir = metar_obj.wind_dir.value()
                    parsed_data['wind_dir'] = original_wind_dir

                if metar_obj.wind_speed:
                    original_wind_speed = metar_obj.wind_speed.value() * 0.51444
                    parsed_data['wind_speed'] = original_wind_speed

                if 'variable' in metar_obj.wind():
                    parsed_data['wind_variable'] = True

                gust_match = re.search(r'gusting to\s*(\d+)\s*knots', wind_text)
                if gust_match:
                    parsed_data['gust'] = float(gust_match.group(1)) * 0.51444
                gust_match2 = re.search(r'gusting to\s*(\d+)\s*meters', wind_text)
                if gust_match2:
                    parsed_data['gust'] = float(gust_match2.group(1)) * 0.51444

            if 'peak_wind' in data:
                if metar_obj.wind_dir_peak:
                    peak_wind_dir = metar_obj.wind_dir_peak.value()
                    parsed_data['peak_wind_dir'] = peak_wind_dir

                if metar_obj.wind_speed_peak:
                    peak_wind_speed = metar_obj.wind_speed_peak.value() * 0.51444
                    parsed_data['peak_wind_speed'] = peak_wind_speed

            # 修改海平面气压解析，增加异常处理
            if 'sea_level_pressure' in data:
                try:
                    sea_level_pressure_value = data['sea_level_pressure']
                    if sea_level_pressure_value:
                        # 尝试多种格式
                        patterns = [
                            r'([\d.]+)\s*mb',  # 百帕 (mb/hPa) - 无需转换
                            r'Q(\d+)',  # QNH格式 Q1013 (百帕) - 无需转换
                            r'A(\d{3,4})',  # 英寸汞柱 A2992 -> 需要转换
                            r'(\d{4})',  # 纯数字4位，可能是百帕
                            r'(\d{3}\.\d)',  # 带小数点的3位数字
                        ]

                        parsed_pressure = None
                        original_pattern = None

                        for pattern in patterns:
                            match = re.search(pattern, str(sea_level_pressure_value))
                            if match:
                                parsed_pressure = float(match.group(1))
                                original_pattern = pattern
                                break

                        # 根据匹配的模式进行单位转换
                        if parsed_pressure is not None:
                            if original_pattern == r'A(\d{3,4})':  # A格式：英寸汞柱
                                # A格式：最后3位是英寸汞柱，单位是0.01英寸汞柱
                                # 例如：A2992 = 29.92英寸汞柱
                                inches_hg = parsed_pressure / 100.0
                                # 转换为百帕: 1 inHg = 33.8639 hPa
                                parsed_data['sea_level_pressure'] = inches_hg * 33.8639
                                logging.debug(
                                    f"海平面气压A格式转换: {parsed_pressure} -> {inches_hg:.2f} inHg -> {parsed_data['sea_level_pressure']:.2f} hPa")

                            elif original_pattern == r'Q(\d+)':  # QNH格式
                                # Q格式：直接是百帕值
                                parsed_data['sea_level_pressure'] = parsed_pressure

                            elif original_pattern == r'(\d{4})':  # 纯4位数字
                                # 通常海平面气压是3-4位数字的百帕值
                                # 例如：1013 = 1013 hPa
                                if 950 <= parsed_pressure <= 1050:  # 合理的海平面气压范围
                                    parsed_data['sea_level_pressure'] = parsed_pressure
                                else:
                                    parsed_data['sea_level_pressure'] = None

                            elif original_pattern == r'(\d{3}\.\d)':  # 带小数点的格式
                                # 可能是其他单位，暂时按百帕处理
                                parsed_data['sea_level_pressure'] = parsed_pressure

                            else:  # mb格式或其他
                                parsed_data['sea_level_pressure'] = parsed_pressure
                        else:
                            parsed_data['sea_level_pressure'] = None

                except Exception as e:
                    logging.warning(f"解析海平面气压失败, 错误: {e}")
                    parsed_data['sea_level_pressure'] = None

            if '1_hour_precipitation' in data:
                snow_match = re.search(r'([\d.]+)\s*in', data['1_hour_precipitation'])
                if snow_match:
                    parsed_data['1_hour_precipitation'] = float(snow_match.group(1)) * 25.4

            # 解析能见度
            if 'visibility' in data:
                vis_match = re.search(r'([\d.]+)\s*meters', data['visibility'])
                if vis_match:
                    parsed_data['visibility'] = float(vis_match.group(1))
                else:
                    miles_match = re.search(r'([\d.]+)\s*miles', data['visibility'])
                    if miles_match:
                        parsed_data['visibility'] = float(miles_match.group(1)) * 1609.34

                    vis_match1 = re.search(r'greater\s*than\s*([\d.]+)\s*meters', data['visibility'])
                    if vis_match1:
                        parsed_data['visibility'] = vis_match1.group(1)

            if 'pressure' in data:
                pressure_match = re.search(r'([\d.]+)\s*mb', data['pressure'])
                if pressure_match:
                    parsed_data['pressure'] = pressure_match.group(1)

            # 解析天气现象
            if 'weather' in data:
                parsed_data['weather'] = data['weather']

            # 解析云层信息
            if 'sky' in data:
                # 如果sky是列表，转换为字符串
                if isinstance(data['sky'], list):
                    sky_text = ' '.join(data['sky'])
                else:
                    sky_text = data['sky']
                parsed_data['sky'] = sky_text

            # 添加其他直接字段
            direct_fields = ['station', 'type', 'time', 'remarks', 'METAR']
            for field in direct_fields:
                if field in data:
                    parsed_data[field] = data[field]

            return parsed_data

        except Exception as e:
            logging.error(f"从JSON解析METAR数据失败: {e}")
            return None

    def parse_metar_data(self, station, data):
        """解析METAR数据"""
        try:
            metar_str = f"METAR {station} {data}"
            metar_obj = Metar.Metar(metar_str)
            metar_string_output = metar_obj.string()

            parsed_data = self.parse_metar_string(metar_string_output, metar_obj)

            if parsed_data:
                parsed_data['station'] = station
                parsed_data['raw_data'] = data

            return parsed_data

        except Exception as e:
            logging.error(f"解析METAR数据失败: {station} - {e}")
            return None

    def get_station_coordinates(self, station_id, station_file='metar_station.csv'):
        """获取站点坐标"""
        try:
            with open(station_file, mode='r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    if row['station_id'] == station_id:
                        return row['latitude'], row['longitude']
            return None, None
        except Exception as e:
            logging.error(f"读取站点坐标文件时出错: {e}")
            return None, None

    def setup_retry_strategy(self):
        """设置重试策略"""
        retry_strategy = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session = requests.Session()
        session.mount("https://", adapter)
        return session

    def _is_manually_corrected(self, raw_text):
        """检查METAR数据是否为手动修正报告"""
        if not raw_text:
            return False

        try:
            raw_lower = str(raw_text).lower()
            manual_indicators = [
                'manually corrected',
                'corrected',
                'cor '
            ]
            return any(indicator in raw_lower for indicator in manual_indicators)

        except Exception as e:
            logging.debug(f"METAR解析失败，无法判断是否手动修正: {e}")
            return False

    def process_all_stations(self):
        """处理所有站点的数据（修改为使用缓冲区和分表）"""
        if not self.ensure_connection():
            logging.error("处理站点数据失败：数据库连接不可用")
            return

        logging.info(f"任务开始执行: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        self._track_memory("process_start")

        session = None
        try:
            session = self.setup_retry_strategy()
            metar_links = self.fetch_metar_data()

            if metar_links:
                stop_event = threading.Event()
                # 数据缓冲区，按表名分组
                data_buffer = {}

                # 分批处理链接，避免一次性处理太多
                batch_size = 100
                total_batches = (len(metar_links) + batch_size - 1) // batch_size

                for batch_num in range(total_batches):
                    if stop_event.is_set():
                        break

                    start_idx = batch_num * batch_size
                    end_idx = min((batch_num + 1) * batch_size, len(metar_links))
                    batch_links = metar_links[start_idx:end_idx]

                    logging.info(f"处理批次 {batch_num + 1}/{total_batches}, 链接数: {len(batch_links)}")

                    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                        futures = [
                            executor.submit(self.process_and_insert_data, metar_link, session, stop_event, data_buffer)
                            for metar_link in batch_links]

                        for future in concurrent.futures.as_completed(futures):
                            if stop_event.is_set():
                                for f in futures:
                                    f.cancel()
                                break

                            try:
                                result = future.result()
                                # 及时释放结果对象
                                del result
                            except Exception as e:
                                logging.error(f"任务执行异常: {e}")

                            # 每处理完一个任务就清理一次内存
                            self._force_gc()

                        # 清理futures列表
                        del futures

                    # 批次结束后，清理内存并插入缓冲区剩余数据
                    for table_name, data_list in data_buffer.items():
                        if data_list:
                            inserted_count = self._batch_insert_to_table(data_list, table_name)
                            if inserted_count > 0:
                                logging.info(f"批次结束插入 {inserted_count} 条数据到表 {table_name}")

                    # 重建字典而不是clear()，释放内部哈希表
                    del data_buffer
                    data_buffer = {}

                    # 清理批次数据
                    del batch_links

                    # 批次间内存清理
                    self._force_gc()

                    if self.consecutive_no_new_data >= self.max_consecutive_no_new_data:
                        logging.info(f"连续 {self.max_consecutive_no_new_data} 次没有新数据，停止处理后续数据")
                        stop_event.set()
                        break

            # 清理session
            if session:
                session.close()
                del session

        except Exception as e:
            logging.error(f"处理所有站点时发生错误: {e}")
        finally:
            # 强制垃圾回收
            self._force_gc()

            # 清理可能的大对象
            if 'metar_links' in locals():
                del metar_links
            if 'data_buffer' in locals():
                del data_buffer
            if 'stop_event' in locals():
                del stop_event
            if 'batch_size' in locals():
                del batch_size

            self._force_gc()

        self.consecutive_no_new_data = 0
        self.count_out = 0
        self._track_memory("process_end")
        logging.info(f"任务执行结束: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    def start_auto_update(self, interval_minutes=10):
        """启动自动更新任务 - 每10分钟的15秒执行一次"""
        if hasattr(self, 'is_running') and self.is_running:
            logging.warning("自动更新任务已经在运行")
            return

        try:
            # 创建调度器
            self.scheduler = BackgroundScheduler(daemon=True)

            # 使用cron触发器，每10分钟的15秒执行
            self.scheduler.add_job(
                func=self._memory_managed_update,
                trigger='cron',
                minute='*/10',  # 每10分钟
                second=5,  # 第0秒
                id='nws_metar_update',
                name='NWS_METAR每10分钟更新',
                misfire_grace_time=60,  # 错过执行的宽限时间60秒
                coalesce=True,  # 合并错过的执行
                max_instances=1  # 只允许一个实例运行
            )

            # 启动调度器
            self.scheduler.start()
            self.is_running = True

            # 立即执行一次（不等待下一个整点）
            logging.info("立即执行第一次数据更新...")
            self._memory_managed_update()
            self._keep_main_thread_alive()

        except Exception as e:
            logging.error(f"启动定时任务失败: {e}")
            if hasattr(self, 'scheduler') and self.scheduler:
                self.scheduler.shutdown()
            raise

    def _memory_managed_update(self):
        """内存管理包装函数"""
        try:
            logging.info(f"定时任务开始执行: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            # 执行前清理内存
            self._force_gc()
            self.process_all_stations()
            logging.info(f"定时任务执行完成: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        except Exception as e:
            logging.error(f"定时任务执行出错: {e}")
        finally:
            # 确保每次执行后都清理内存
            self._force_gc()
            # 清理可能残留的对象
            if hasattr(self, 'session_cache'):
                del self.session_cache
            self._force_gc()

    def _keep_main_thread_alive(self):
        """保持主线程运行"""
        try:
            last_cleanup_time = time.time()
            last_log_time = time.time()

            while hasattr(self, 'is_running') and self.is_running and \
                    hasattr(self, 'scheduler') and self.scheduler.running:
                current_time = time.time()

                # 每30秒记录一次状态（调试用）
                if current_time - last_log_time > 30:
                    try:
                        job = self.scheduler.get_job('nws_metar_update')
                        if job:
                            next_run = job.next_run_time
                            if next_run:
                                now = datetime.now()
                                time_until_next = (next_run - now).total_seconds()
                                if time_until_next > 0:
                                    logging.debug(f"距离下一次执行还有 {int(time_until_next)} 秒")
                    except Exception as e:
                        logging.debug(f"获取作业信息失败: {e}")
                    last_log_time = current_time

                # 定期内存清理（每5分钟）
                if current_time - last_cleanup_time > 300:  # 5分钟
                    self._cleanup_memory()
                    last_cleanup_time = current_time

                # 短暂休眠
                time.sleep(0.5)  # 0.5秒检查一次

        except KeyboardInterrupt:
            logging.info("程序被用户中断")
            self._shutdown()
        except Exception as e:
            logging.error(f"主线程循环出错: {e}")
            self._shutdown()

    def _shutdown(self):
        """关闭调度器和数据库连接"""
        logging.info("正在关闭程序...")

        # 停止调度器
        if hasattr(self, 'scheduler') and self.scheduler and self.scheduler.running:
            self.scheduler.shutdown(wait=False)
            logging.info("调度器已关闭")

        if hasattr(self, 'is_running'):
            self.is_running = False

        # 关闭数据库连接
        if hasattr(self, 'connection') and self.connection:
            self.connection.close()
            logging.info("数据库连接已关闭")

        # 最终内存清理
        self._cleanup_memory()

    def _cleanup_memory(self):
        """内存清理"""
        collected = gc.collect()
        logging.debug(f"垃圾回收收集了 {collected} 个对象")

        # 清理缓存
        if hasattr(self, '_table_columns_cache'):
            self._table_columns_cache.clear()
        if hasattr(self, '_table_exists_cache'):
            self._table_exists_cache.clear()
        if hasattr(self, '_created_tables_this_session'):
            self._created_tables_this_session.clear()

    def export_table_to_csv(self, table_name="nws_metar_data_20251217_20251226", file_name="here_1218.csv"):
        """导出表数据到CSV文件"""
        if not self.ensure_connection():
            logging.error("导出数据失败：数据库连接不可用")
            return False

        if file_name is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_name = f"{table_name}_{timestamp}.csv"

        try:
            query = f"COPY {table_name} TO STDOUT WITH CSV HEADER"
            with self.connection.cursor() as cursor, open(file_name, 'w', encoding='utf-8') as f:
                cursor.copy_expert(query, f)
            logging.info(f"表 {table_name} 导出到 {file_name} 成功")
            return True
        except Exception as e:
            logging.error(f"导出数据失败：{e}")
            return False


if __name__ == "__main__":
    nws_db = NWS_METAR(partition_days=10)

    if nws_db.connect():
        try:
            nws_db.start_auto_update()
            # nws_db.export_table_to_csv()
        except KeyboardInterrupt:
            logging.info("程序被用户中断")
        finally:
            nws_db.close()
    else:
        logging.error("数据库连接失败，程序退出")