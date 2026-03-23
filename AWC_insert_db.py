import psycopg2
from psycopg2 import OperationalError
import logging
import pandas as pd
from datetime import datetime, timezone, timedelta
import os
import requests
import schedule
import time
import gzip
import io
import numpy as np
import sys
import gc
import re
from typing import Optional, List, Dict, Tuple, Set
from apscheduler.schedulers.background import BackgroundScheduler

# 设置独立的 logger（兼容Docker和Windows本地环境）
script_dir = os.path.dirname(os.path.abspath(__file__))
local_log_dir = os.path.join(script_dir, 'logs')
if sys.platform == 'win32':
    log_dir = local_log_dir
else:
    log_dir = '/logs' if os.path.exists('/logs') and os.path.isdir('/logs') else local_log_dir
os.makedirs(log_dir, exist_ok=True)

logger = logging.getLogger('AWC_db')
logger.setLevel(logging.INFO)


class FlushFileHandler(logging.FileHandler):
    def emit(self, record):
        super().emit(record)
        self.flush()


if not logger.handlers:
    fh = FlushFileHandler(os.path.join(log_dir, 'AWC_db.log'), encoding='utf-8')
    fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(fh)
    sh = logging.StreamHandler()
    sh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(sh)

logging = logger


class AWC_METAR:
    def __init__(self, db_config=None, partition_days=10):
        if db_config is None:
            try:
                from db_config import get_db_config
                db_config = get_db_config('metar_taf')
            except ImportError:
                import os
                db_config = {
                    'host': os.getenv('DB_HOST', 'database-1.cxik82g8267p.us-east-2.rds.amazonaws.com'),
                    'port': int(os.getenv('DB_PORT', '5432')),
                    'database': 'metar_taf_synop_data',
                    'user': os.getenv('DB_USER', 'postgres'),
                    'password': os.getenv('DB_PASSWORD', 'oceantest1')
                }
        self.conn_params = db_config
        self.connection = None
        self._memory_tracker = {}
        self.partition_days = partition_days
        self._table_pattern = re.compile(r'awc_metar_data_(\d{8})_(\d{8})')

        # 缓存表结构
        self._table_columns_cache = {}
        self._table_exists_cache = {}
        self._created_tables_this_session = set()

        # APScheduler 实例
        self.scheduler = None
        self.is_running = False

    def start_auto_update(self):
        """启动自动更新任务 - 每分钟的0秒执行"""
        if self.is_running:
            logging.warning("自动更新任务已经在运行")
            return

        try:
            # 创建调度器
            self.scheduler = BackgroundScheduler(daemon=True)

            # 使用cron触发器，每分钟的0秒执行
            self.scheduler.add_job(
                func=self.download_and_save_data,
                trigger='cron',
                second=0,  # 每分钟的0秒
                id='awc_metar_update',
                name='AWC_METAR每分钟更新',
                misfire_grace_time=30,  # 错过执行的宽限时间30秒
                coalesce=True,  # 合并错过的执行
                max_instances=1  # 只允许一个实例运行
            )

            # 启动调度器
            self.scheduler.start()
            self.is_running = True

            # 立即执行一次（不等待下一个整分钟）
            logging.info("立即执行第一次数据更新...")
            self.download_and_save_data()

            # 计算下一次执行时间
            now = datetime.now()
            next_run = datetime(now.year, now.month, now.day, now.hour, now.minute + 1, 0)
            seconds_until_next = (next_run - now).total_seconds()

            # 保持主线程运行
            self._keep_main_thread_alive()

        except Exception as e:
            logging.error(f"启动定时任务失败: {e}")
            if self.scheduler:
                self.scheduler.shutdown()
            raise

    def _keep_main_thread_alive(self):
        """保持主线程运行"""
        try:
            last_cleanup_time = time.time()

            while self.is_running and self.scheduler.running:
                current_time = time.time()
                current_second = int(current_time) % 60

                # 日志显示当前秒数（调试用）
                if current_second == 0:
                    logging.debug(f"当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

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
        if self.scheduler and self.scheduler.running:
            self.scheduler.shutdown(wait=False)
            logging.info("调度器已关闭")

        self.is_running = False

        # 关闭数据库连接
        if self.connection:
            self.connection.close()
            logging.info("数据库连接已关闭")

        # 最终内存清理
        self._cleanup_memory()

    def _cleanup_memory(self):
        """内存清理"""
        collected = gc.collect()
        logging.debug(f"垃圾回收收集了 {collected} 个对象")

    def _track_memory(self, step_name: str) -> None:
        """跟踪内存使用情况"""
        try:
            import psutil
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            self._memory_tracker[step_name] = memory_mb
            logging.debug(f"内存使用跟踪 [{step_name}]: {memory_mb:.2f} MB")
        except ImportError:
            pass

    def connect(self):
        """连接数据库"""
        try:
            self.connection = psycopg2.connect(**self.conn_params)
            self.connection.autocommit = False
            logging.info("成功连接到 PostgreSQL 数据库")
            return True
        except OperationalError as e:
            logging.error(f"数据库连接失败：{e}")
            return False

    def close(self):
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            self.connection = None
            logging.info("数据库连接已关闭")
        self._cleanup_memory()
        # 清理缓存
        self._table_columns_cache.clear()
        self._table_exists_cache.clear()

    def _get_partition_table_name(self, observation_time: datetime) -> str:
        """
        根据观测时间计算分表名

        算法：
        1. 将时间转换为日期
        2. 计算从基准日期（2020-01-01）到该日期的天数
        3. 将天数除以分表间隔（10天）得到分区号
        4. 计算分区的起始和结束日期
        5. 格式化为表名后缀

        例如：2025-12-28 在 2025-12-25 到 2026-01-04 的分区中
        表名：awc_metar_data_20251225_20260104
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
        return f"awc_metar_data_{table_suffix}"

    def _create_partition_table(self, table_name: str) -> bool:
        """创建分表"""
        # 检查表是否已存在
        if self._check_table_exists(table_name):
            return True

        create_table_sql = f"""
        CREATE TABLE {table_name} (
            raw_text TEXT DEFAULT NULL,
            manually_corrected BOOLEAN DEFAULT FALSE,
            station_id VARCHAR(5) DEFAULT NULL,
            observation_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            latitude NUMERIC(9,6) DEFAULT NULL,
            longitude NUMERIC(9,6) DEFAULT NULL,
            temp NUMERIC(9,2) DEFAULT NULL,
            dewpoint NUMERIC(9,2) DEFAULT NULL,
            wind_dir_degrees NUMERIC(9,1) DEFAULT NULL,
            wind_speed NUMERIC(9,2) DEFAULT NULL,
            wind_gust NUMERIC(9,2) DEFAULT NULL,
            visibility_statute NUMERIC(15,2) DEFAULT NULL,
            altim_in_hg NUMERIC(9,2) DEFAULT NULL,
            sea_level_pressure NUMERIC(9,2) DEFAULT NULL,
            corrected VARCHAR(10) DEFAULT NULL,
            auto VARCHAR(10) DEFAULT NULL,
            auto_station VARCHAR(10) DEFAULT NULL,
            maintenance_indicator_on VARCHAR(10) DEFAULT NULL,
            no_signal VARCHAR(10) DEFAULT NULL,
            lightning_sensor_off VARCHAR(10) DEFAULT NULL,
            freezing_rain_sensor_off VARCHAR(10) DEFAULT NULL,
            present_weather_sensor_off VARCHAR(10) DEFAULT NULL,
            weather TEXT DEFAULT NULL,
            sky_cover1 VARCHAR(10) DEFAULT NULL,
            cloud1 VARCHAR(10) DEFAULT NULL,
            sky_cover2 VARCHAR(10) DEFAULT NULL,
            cloud2 VARCHAR(10) DEFAULT NULL,
            sky_cover3 VARCHAR(10) DEFAULT NULL,
            cloud3 VARCHAR(10) DEFAULT NULL,
            sky_cover4 VARCHAR(10) DEFAULT NULL,
            cloud4 VARCHAR(10) DEFAULT NULL,
            flight_category TEXT DEFAULT NULL,
            three_hr_pressure_tendency NUMERIC(9,2) DEFAULT NULL,
            maxT NUMERIC(9,2) DEFAULT NULL,
            minT NUMERIC(9,2) DEFAULT NULL,
            maxT24h NUMERIC(9,2) DEFAULT NULL,
            minT24h NUMERIC(9,2) DEFAULT NULL,
            precip NUMERIC(9,2) DEFAULT NULL,
            pcp3h NUMERIC(9,2) DEFAULT NULL,
            pcp6h NUMERIC(9,2) DEFAULT NULL,
            pcp24h NUMERIC(9,2) DEFAULT NULL,
            snow NUMERIC(9,2) DEFAULT NULL,
            vert_vis VARCHAR(10) DEFAULT NULL,
            metar_type VARCHAR(10) DEFAULT NULL,
            elevation_height NUMERIC(9,2) DEFAULT NULL,
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
                    AND table_name LIKE 'awc_metar_data_%'
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

    def _route_data_to_tables(self, df: pd.DataFrame) -> Dict[str, List[Dict]]:
        """
        核心路由函数：将数据按观测时间路由到不同的表

        算法：
        1. 遍历DataFrame中的每一行数据
        2. 根据observation_time计算应该插入的表名
        3. 将数据按表名分组

        内存优化：
        - 使用生成器方式处理，避免创建大的中间数据结构
        - 分批处理，限制内存使用
        """
        if df.empty or 'observation_time' not in df.columns:
            return {}

        # 确保 observation_time 是 UTC 时间语义（naive datetime，用于入库 TIMESTAMP 和分表）
        if not pd.api.types.is_datetime64_any_dtype(df['observation_time']):
            df['observation_time'] = pd.to_datetime(df['observation_time'], errors='coerce', utc=True)
        if hasattr(df['observation_time'].dt, 'tz') and df['observation_time'].dt.tz is not None:
            df['observation_time'] = df['observation_time'].dt.tz_convert('UTC').dt.tz_localize(None)

        # 使用字典分组数据
        table_groups = {}

        # 遍历DataFrame，按表分组
        for idx, row in df.iterrows():
            obs_time = row['observation_time']

            if pd.isna(obs_time):
                continue

            # 计算表名
            table_name = self._get_partition_table_name(obs_time)

            # 将数据转换为字典并添加到对应分组
            if table_name not in table_groups:
                table_groups[table_name] = []

            table_groups[table_name].append(row.to_dict())

        return table_groups

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

    def _is_manually_corrected(self, raw_text: str) -> bool:
        """检查是否为手动修正数据"""
        if not raw_text or pd.isna(raw_text):
            return False

        try:
            raw_lower = str(raw_text).lower()
            manual_indicators = ['manually corrected', 'corrected', 'cor ']
            return any(indicator in raw_lower for indicator in manual_indicators)
        except:
            return False

    def _request_with_retry(self, url, headers, retries=5, delay=5):
        """带重试的请求函数"""
        for attempt in range(retries):
            try:
                logging.info(f"尝试获取文件，尝试次数: {attempt + 1}/{retries}...")
                response = requests.get(url, headers=headers, timeout=30)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                logging.error(f"请求失败: {e}")
                if attempt < retries - 1:
                    sleep_time = delay * (2 ** attempt)  # 指数退避
                    time.sleep(sleep_time)
                else:
                    logging.error(f"重试失败，所有 {retries} 次尝试都未成功。")
                    raise

    # ============ 原有的数据处理方法 ============

    def _clean_dataframe(self, df):
        """清理数据框，处理特殊值和类型转换"""
        if df.empty:
            return df

        self._track_memory("before_clean_dataframe")

        try:
            df_clean = df.copy()

            # 处理文本布尔字段
            text_boolean_columns = [
                'corrected', 'auto', 'auto_station', 'maintenance_indicator_on',
                'no_signal', 'lightning_sensor_off', 'freezing_rain_sensor_off',
                'present_weather_sensor_off'
            ]

            for col in text_boolean_columns:
                if col in df_clean.columns:
                    df_clean[col] = df_clean[col].apply(
                        lambda x: None if pd.isna(x) or str(x).lower() in ['nan', 'none', 'null', '<null>',
                                                                           ''] else str(x)[:10]
                    )

            # 处理数值字段
            numeric_columns = [
                'latitude', 'longitude', 'temp', 'dewpoint', 'wind_dir_degrees',
                'wind_speed', 'wind_gust', 'altim_in_hg', 'sea_level_pressure',
                'three_hr_pressure_tendency', 'maxT', 'minT', 'maxT24h', 'minT24h',
                'precip', 'pcp3h', 'pcp6h', 'pcp24h', 'snow', 'elevation_height'
            ]

            for col in numeric_columns:
                if col in df_clean.columns:
                    df_clean[col] = df_clean[col].apply(
                        lambda x: None if pd.isna(x) or str(x).lower() in ['nan', 'none', 'null', '<null>',
                                                                           ''] else float(x)
                    )

            # 处理文本字段
            varchar_columns = [
                'visibility_statute', 'sky_cover1', 'cloud1', 'sky_cover2', 'cloud2',
                'sky_cover3', 'cloud3', 'sky_cover4', 'cloud4', 'vert_vis', 'metar_type'
            ]

            for col in varchar_columns:
                if col in df_clean.columns:
                    df_clean[col] = df_clean[col].apply(
                        lambda x: None if pd.isna(x) or str(x).lower() in ['nan', 'none', 'null', '<null>',
                                                                           ''] else str(x)[:10]
                    )

            # 处理长文本字段
            long_text_columns = ['raw_text', 'weather', 'flight_category']
            for col in long_text_columns:
                if col in df_clean.columns:
                    df_clean[col] = df_clean[col].apply(
                        lambda x: None if pd.isna(x) or str(x).lower() in ['nan', 'none', 'null', '<null>',
                                                                           ''] else str(x)
                    )

            # 处理站点ID
            if 'station_id' in df_clean.columns:
                df_clean['station_id'] = df_clean['station_id'].apply(
                    lambda x: None if pd.isna(x) or str(x).lower() in ['nan', 'none', 'null', '<null>', ''] else str(x)[
                                                                                                                 :5]
                )

            self._track_memory("after_clean_dataframe")
            return df_clean

        except Exception as e:
            logging.error(f"数据清理失败: {e}")
            raise

    def convert_units(self, df):
        """单位转换和字段映射函数"""
        if df.empty:
            return df

        df_copy = df.copy()

        try:
            # 风速转换：节(kt) → 米/秒(m/s)
            if 'wind_speed_kt' in df_copy.columns:
                df_copy.loc[:, 'wind_speed_kt'] = pd.to_numeric(df_copy['wind_speed_kt'], errors='coerce') * 0.514444
            if 'wind_gust_kt' in df_copy.columns:
                df_copy.loc[:, 'wind_gust_kt'] = pd.to_numeric(df_copy['wind_gust_kt'], errors='coerce') * 0.514444

            # 能见度转换：法定英里(mi) → 米(m)
            if 'visibility_statute_mi' in df_copy.columns:
                df_copy.loc[:, 'visibility_statute_mi'] = df_copy['visibility_statute_mi'].astype(str).str.replace('+',
                                                                                                                   '',
                                                                                                                   regex=False)
                df_copy.loc[:, 'visibility_statute_mi'] = pd.to_numeric(df_copy['visibility_statute_mi'],
                                                                        errors='coerce') * 1609.34

            # 气压转换：英寸汞柱(inHg) → 百帕(hPa)
            if 'altim_in_hg' in df_copy.columns:
                df_copy.loc[:, 'altim_in_hg'] = pd.to_numeric(df_copy['altim_in_hg'], errors='coerce') * 33.8639

            # 字段重命名
            field_mapping = {
                'temp_c': 'temp',
                'dewpoint_c': 'dewpoint',
                'wind_speed_kt': 'wind_speed',
                'wind_gust_kt': 'wind_gust',
                'visibility_statute_mi': 'visibility_statute',
                'altim_in_hg': 'altim_in_hg',
                'sea_level_pressure_mb': 'sea_level_pressure',
                'wx_string': 'weather',
                'cloud_base_ft_agl': 'cloud1',
                'cloud_base_ft_agl.1': 'cloud2',
                'cloud_base_ft_agl.2': 'cloud3',
                'cloud_base_ft_agl.3': 'cloud4',
                'three_hr_pressure_tendency_mb': 'three_hr_pressure_tendency',
                'maxT_c': 'maxT',
                'minT_c': 'minT',
                'maxT24hr_c': 'maxT24h',
                'minT24hr_c': 'minT24h',
                'precip_in': 'precip',
                'pcp3hr_in': 'pcp3h',
                'pcp6hr_in': 'pcp6h',
                'pcp24hr_in': 'pcp24h',
                'snow_in': 'snow',
                'vert_vis_ft': 'vert_vis',
                'elevation_m': 'elevation_height'
            }

            sky_cover_mapping = {
                'sky_cover': 'sky_cover1',
                'sky_cover.1': 'sky_cover2',
                'sky_cover.2': 'sky_cover3',
                'sky_cover.3': 'sky_cover4'
            }

            df_copy.rename(columns=field_mapping, inplace=True)
            df_copy.rename(columns=sky_cover_mapping, inplace=True)

            return df_copy

        except Exception as e:
            logging.error(f"单位转换失败: {e}")
            return df_copy

    # ============ 主数据下载和处理方法 ============

    def download_and_save_data(self):
        """
        下载数据并保存到对应的分表

        核心流程：
        1. 下载压缩的CSV文件
        2. 分块读取和解压（避免大内存占用）
        3. 对每块数据进行单位转换和清理
        4. 按观测时间将数据路由到不同的分表
        5. 批量插入到对应的表中
        """
        url = "https://aviationweather.gov/data/cache/metars.cache.csv.gz"
        headers = {
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        }

        response = None
        total_inserted = 0

        try:
            self._track_memory("before_download")

            # 下载数据
            response = self._request_with_retry(url, headers)
            logging.info("压缩包已成功下载，开始解压...")

            # 分块处理数据
            chunk_size = 5000  # 每块5000条记录
            chunk_count = 0

            with gzip.GzipFile(fileobj=io.BytesIO(response.content), mode='rb') as f_in:
                for chunk in pd.read_csv(f_in, chunksize=chunk_size, low_memory=False):
                    chunk_count += 1
                    logging.debug(f"处理数据块 {chunk_count}")

                    # 转换单位
                    chunk = self.convert_units(chunk)

                    # 统一时间格式：observation_time 按 UTC 解析并转为无时区（naive UTC），用于分表和入库
                    chunk['observation_time'] = pd.to_datetime(chunk['observation_time'], errors='coerce', utc=True)
                    chunk['observation_time'] = chunk['observation_time'].dt.tz_convert('UTC').dt.tz_localize(None)

                    # updatetime：按 UTC 存储到 TIMESTAMP（不要用字符串，避免隐式转换/格式不一致）
                    chunk['updatetime'] = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)

                    # 清理数据
                    chunk = self._clean_dataframe(chunk)

                    # 核心路由：将数据按时间路由到不同的表
                    table_groups = self._route_data_to_tables(chunk)

                    # 将数据插入到各自的表中
                    for table_name, data_list in table_groups.items():
                        if data_list:
                            inserted = self._batch_insert_to_table(data_list, table_name)
                            total_inserted += inserted
                            logging.debug(f"向表 {table_name} 插入了 {inserted} 条数据")

                    # 内存清理
                    del chunk, table_groups
                    if chunk_count % 3 == 0:  # 每处理3个块清理一次内存
                        self._cleanup_memory()

            logging.info(f"数据处理完成，总共插入 {total_inserted} 条数据")

        except Exception as e:
            logging.error(f"文件下载和处理过程中发生错误: {e}")
        finally:
            # 清理资源
            if response:
                response.close()
                del response
            self._cleanup_memory()

    # ============ 查询和统计方法 ============

    def get_total_data_count(self) -> Dict[str, int]:
        """获取所有分表的数据统计"""
        tables = self._get_all_partition_tables()
        counts = {}
        total = 0

        for table in tables:
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    counts[table] = count
                    total += count
            except Exception as e:
                logging.warning(f"统计表 {table} 数据失败: {e}")
                counts[table] = 0

        counts['TOTAL'] = total
        return counts

    def query_data(self, station_id: Optional[str] = None,
                   start_time: Optional[datetime] = None,
                   end_time: Optional[datetime] = None,
                   limit: int = 1000) -> pd.DataFrame:
        """查询数据（支持跨表查询）"""
        try:
            # 确定需要查询哪些表
            tables_to_query = self._get_tables_in_time_range(start_time, end_time)

            all_results = []
            for table_name in tables_to_query:
                if not self._check_table_exists(table_name):
                    continue

                query, params = self._build_query_sql(table_name, station_id, start_time, end_time, limit)

                try:
                    with self.connection.cursor() as cursor:
                        cursor.execute(query, params)
                        columns = [desc[0] for desc in cursor.description]
                        rows = cursor.fetchall()

                        if rows:
                            df = pd.DataFrame(rows, columns=columns)
                            all_results.append(df)
                except Exception as e:
                    logging.warning(f"查询表 {table_name} 失败: {e}")

            if all_results:
                return pd.concat(all_results, ignore_index=True).sort_values('observation_time', ascending=False).head(
                    limit)
            else:
                return pd.DataFrame()
        except Exception as e:
            logging.error(f"查询数据失败: {e}")
            return pd.DataFrame()

    def _get_tables_in_time_range(self, start_time: Optional[datetime], end_time: Optional[datetime]) -> List[str]:
        """获取时间范围内的所有分表"""
        all_tables = self._get_all_partition_tables()

        if not start_time and not end_time:
            return all_tables

        # 解析表名中的日期范围
        tables_in_range = []
        for table_name in all_tables:
            match = self._table_pattern.search(table_name)
            if match:
                start_str, end_str = match.groups()
                table_start = datetime.strptime(start_str, "%Y%m%d")
                table_end = datetime.strptime(end_str, "%Y%m%d") + timedelta(days=1) - timedelta(microseconds=1)

                # 检查时间范围是否有交集
                if start_time and end_time:
                    if not (end_time < table_start or start_time > table_end):
                        tables_in_range.append(table_name)
                elif start_time:
                    if not (start_time > table_end):
                        tables_in_range.append(table_name)
                elif end_time:
                    if not (end_time < table_start):
                        tables_in_range.append(table_name)

        return tables_in_range

    def _build_query_sql(self, table_name: str, station_id: Optional[str],
                         start_time: Optional[datetime], end_time: Optional[datetime],
                         limit: int) -> Tuple[str, List]:
        """构建查询SQL"""
        conditions = []
        params = []

        if station_id:
            conditions.append("station_id = %s")
            params.append(station_id)

        if start_time:
            conditions.append("observation_time >= %s")
            params.append(start_time)

        if end_time:
            conditions.append("observation_time <= %s")
            params.append(end_time)

        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
        query = f"SELECT * FROM {table_name} {where_clause} ORDER BY observation_time DESC LIMIT %s"
        params.append(limit)

        return query, params

    # ============ 自动更新和主程序 ============



if __name__ == "__main__":
    metar_db = AWC_METAR(partition_days=10)

    if metar_db.connect():
        try:

            # 启动自动更新
            metar_db.start_auto_update()

        except KeyboardInterrupt:
            logging.info("程序被用户中断")
        except Exception as e:
            logging.error(f"程序运行出错: {e}")
        finally:
            metar_db.close()
    else:
        logging.error("数据库连接失败，程序退出")