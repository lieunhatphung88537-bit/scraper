
import psycopg2
from psycopg2 import OperationalError
import logging
import pandas as pd
from datetime import datetime, timezone, timedelta
import os
import requests
import time
import io
import gc
import sys
import re
from typing import Optional, List, Dict, Tuple, Set
from apscheduler.schedulers.background import BackgroundScheduler
import concurrent.futures

# 设置独立的 logger（兼容Docker和Windows本地环境）
script_dir = os.path.dirname(os.path.abspath(__file__))
local_log_dir = os.path.join(script_dir, 'logs')
if sys.platform == 'win32':
    log_dir = local_log_dir
else:
    log_dir = '/logs' if os.path.exists('/logs') and os.path.isdir('/logs') else local_log_dir
os.makedirs(log_dir, exist_ok=True)

logger = logging.getLogger('IEM_db')
logger.setLevel(logging.INFO)


class FlushFileHandler(logging.FileHandler):
    def emit(self, record):
        super().emit(record)
        self.flush()


if not logger.handlers:
    fh = FlushFileHandler(os.path.join(log_dir, 'IEM_db.log'), encoding='utf-8')
    fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(fh)
    sh = logging.StreamHandler()
    sh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(sh)

logging = logger


class IEM_METAR:
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
        self.is_connected = False
        self.input_file = 'all_IEM.txt'
        self.partition_days = partition_days
        self._table_pattern = re.compile(r'iem_metar_data_(\d{8})_(\d{8})')

        # 缓存表结构
        self._table_columns_cache = {}
        self._table_exists_cache = {}
        self._created_tables_this_session = set()
        self._column_info_cache = {}  # 新增：存储字段类型信息

        # APScheduler 实例
        self.scheduler = None
        self.is_running = False

        # 内存跟踪
        self._memory_tracker = {}
        self._memory_baseline = None

    def _get_memory_usage(self) -> float:
        """获取当前内存使用量"""
        try:
            import psutil
            process = psutil.Process()
            return process.memory_info().rss / 1024 / 1024  # MB
        except ImportError:
            return 0.0

    def _track_memory(self, step_name: str) -> None:
        """跟踪内存使用情况"""
        try:
            memory_mb = self._get_memory_usage()
            if self._memory_baseline is None:
                self._memory_baseline = memory_mb

            delta = memory_mb - self._memory_baseline
            self._memory_tracker[step_name] = memory_mb
            logging.debug(f"内存使用跟踪 [{step_name}]: {memory_mb:.2f} MB (增量: {delta:+.2f} MB)")
        except Exception as e:
            logging.debug(f"内存跟踪失败: {e}")

    def _force_gc(self) -> None:
        """强制垃圾回收"""
        for i in range(3):  # 多次回收确保彻底
            collected = gc.collect()
            if collected == 0:
                break
            logging.debug(f"垃圾回收第{i + 1}次: 收集了 {collected} 个对象")

        # 清除可能的大对象引用
        gc.collect(2)  # 深度回收

    def _cleanup_dataframe_references(self, df_name: str = None):
        """清理DataFrame相关的引用"""
        try:
            # 清理pandas可能的缓存
            if hasattr(pd.core, 'algorithms'):
                for attr in ['_unique', '_mode', '_rank']:
                    if hasattr(pd.core.algorithms, attr):
                        setattr(pd.core.algorithms, attr, None)

            # 清理pandas的IO缓存
            try:
                import functools
                if hasattr(pd.io.common, '_get_filepath_or_buffer'):
                    if hasattr(pd.io.common._get_filepath_or_buffer, 'cache_clear'):
                        pd.io.common._get_filepath_or_buffer.cache_clear()
            except:
                pass

            # 清理本地命名空间
            if df_name:
                for scope in [locals(), globals()]:
                    if df_name in scope:
                        del scope[df_name]

        except Exception as e:
            logging.debug(f"清理DataFrame引用失败: {e}")

    def connect(self):
        try:
            self.connection = psycopg2.connect(**self.conn_params)
            self.is_connected = True
            logging.info("成功连接到 PostgreSQL 数据库")

            # 连接成功后立即创建表
            if self.create_metar_tables():
                return True
            else:
                logging.error("创建表失败")
                return False

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
        self._column_info_cache.clear()

        # 强制内存清理
        self._force_gc()
        self._cleanup_dataframe_references()

    def _get_partition_table_name(self, observation_time: datetime) -> str:
        """
        根据观测时间计算分表名
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
        return f"iem_metar_data_{table_suffix}"

    def _get_tables_for_data(self, data_dates: List[datetime]) -> List[str]:
        """获取数据涉及的所有表名"""
        table_names = set()
        for obs_time in data_dates:
            if pd.isna(obs_time):
                continue
            table_name = self._get_partition_table_name(obs_time)
            table_names.add(table_name)

        return sorted(list(table_names))

    def _ensure_partition_tables_exist(self, table_names: List[str]) -> bool:
        """确保指定的分表都存在"""
        existing_tables = self._get_all_partition_tables()

        created_count = 0
        for table_name in table_names:
            if table_name not in existing_tables:
                if self._create_partition_table(table_name):
                    created_count += 1
                    self._created_tables_this_session.add(table_name)

        if created_count > 0:
            logging.info(f"创建了 {created_count} 个新的分表")

        return True

    def _create_partition_table(self, table_name: str) -> bool:
        """创建分表"""
        # 检查表是否已存在
        if self._check_table_exists(table_name):
            return True

        # 从表名解析日期范围用于日志
        date_range = table_name.replace('iem_metar_data_', '')
        logging.info(f"正在创建分表: {table_name} (日期范围: {date_range})")

        create_table_sql = f"""
        CREATE TABLE {table_name} (
            raw_text TEXT DEFAULT NULL,
            manually_corrected BOOLEAN DEFAULT FALSE,
            station_id VARCHAR(20) DEFAULT NULL,
            observation_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            latitude NUMERIC(9,6) DEFAULT NULL,
            longitude NUMERIC(9,6) DEFAULT NULL,
            temp NUMERIC(9,2) DEFAULT NULL,
            dewpoint NUMERIC(9,2) DEFAULT NULL,
            feels_like_c NUMERIC(9,2) DEFAULT NULL,
            relh NUMERIC(9,2) DEFAULT NULL,
            wind_dir_degrees NUMERIC(9,1) DEFAULT NULL,
            wind_speed NUMERIC(9,2) DEFAULT NULL,
            wind_gust NUMERIC(9,2) DEFAULT NULL,
            peak_wind_drct NUMERIC(9,1) DEFAULT NULL,
            peak_wind_gust NUMERIC(9,2) DEFAULT NULL, 

            peak_wind_time TIMESTAMP DEFAULT NULL,
            visibility_statute NUMERIC(15,2) DEFAULT NULL,
            altim_in_hg NUMERIC(9,2) DEFAULT NULL,
            sea_level_pressure NUMERIC(9,2) DEFAULT NULL,
            sky_cover1 VARCHAR(50) DEFAULT NULL,
            cloud1 NUMERIC(9,2) DEFAULT NULL,
            sky_cover2 VARCHAR(50) DEFAULT NULL,
            cloud2 NUMERIC(9,2) DEFAULT NULL,
            sky_cover3 VARCHAR(50) DEFAULT NULL,
            cloud3 NUMERIC(9,2) DEFAULT NULL,
            sky_cover4 VARCHAR(50) DEFAULT NULL,
            cloud4 NUMERIC(9,2) DEFAULT NULL,
            precip NUMERIC(9,2) DEFAULT NULL,
            snow NUMERIC(9,2) DEFAULT NULL,
            weather TEXT DEFAULT NULL,
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
                self._table_columns_cache.pop(table_name, None)
                self._column_info_cache.pop(table_name, None)

                return True
        except Exception as e:
            logging.error(f"创建分表 {table_name} 失败：{e}")
            self.connection.rollback()
            return False

    def create_metar_tables(self):
        """检查并创建当前需要的分表"""
        if not self.ensure_connection():
            logging.error("创建表失败：数据库连接不可用")
            return False

        try:
            # 创建当前日期所在的表
            current_time = datetime.now(timezone.utc).replace(tzinfo=None)
            current_table = self._get_partition_table_name(current_time)

            # 只创建当前需要的表
            if not self._check_table_exists(current_table):
                self._create_partition_table(current_table)

            # 显示当前分表情况
            tables = self._get_all_partition_tables()
            logging.info(f"分表系统初始化完成，共 {len(tables)} 个分表")

            return True
        except Exception as e:
            logging.error(f"创建分表系统失败：{e}")
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
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = %s
            ORDER BY ordinal_position
            """
            with self.connection.cursor() as cursor:
                cursor.execute(query, (table_name,))
                results = cursor.fetchall()
                columns = {row[0] for row in results}

                # 同时记录字段类型信息
                column_info = {}
                for row in results:
                    column_info[row[0]] = {
                        'data_type': row[1],
                        'max_length': row[2]
                    }
                self._column_info_cache[table_name] = column_info

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
                    AND table_name LIKE 'iem_metar_data_%'
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

    def get_utc_date(self):
        """获取UTC日期"""
        utc_now = datetime.now(timezone.utc)
        year = utc_now.year
        month = utc_now.month
        day = utc_now.day
        return year, month, day

    def create_url(self, station_url):
        """创建IEM数据请求URL"""
        year, month, day = self.get_utc_date()

        # 创建当前日期的datetime对象
        current_date = datetime(year, month, day)

        # 计算前一天和后一天的日期
        previous_day = current_date - timedelta(days=1)
        next_day = current_date + timedelta(days=1)

        url_template = (
            "{station_url}&data=all&year1={year1}&month1={month1}&day1={day1}"
            "&year2={year2}&month2={month2}&day2={day2}&tz=Etc%2FUTC"
            "&format=onlycomma&latlon=yes&elev=yes&missing=null&trace=null"
            "&direct=no&report_type=3&report_type=4"
        )
        url = url_template.format(
            station_url=station_url,
            year1=previous_day.year, month1=previous_day.month, day1=previous_day.day,
            year2=next_day.year, month2=next_day.month, day2=next_day.day
        )
        return url

    def convert_units_to_metric(self, df):
        """
        将数据框中的英制单位转换为公制单位并映射到数据库字段
        内存优化版本 - 原地操作减少复制
        """
        if df.empty:
            return df

        self._track_memory("before_convert_units")
        start_memory = self._get_memory_usage()

        try:
            # 使用原地操作，避免不必要的复制
            result_df = pd.DataFrame()

            # 时间格式处理：IEM 请求参数带 tz=UTC，因此按 UTC 语义解析并转为 naive UTC（用于入库 TIMESTAMP/分表）
            if 'valid' in df.columns:
                result_df['observation_time'] = pd.to_datetime(
                    df['valid'],
                    format='%Y-%m-%d %H:%M',
                    errors='coerce',
                    utc=True
                ).dt.tz_convert('UTC').dt.tz_localize(None)
                if 'observation_time' in result_df.columns:
                    result_df['observation_time'] = result_df['observation_time'].dt.floor('s')

            # 站点ID映射
            if 'station' in df.columns:
                result_df['station_id'] = df['station']

            # 经纬度映射
            if 'lon' in df.columns:
                result_df['longitude'] = pd.to_numeric(df['lon'], errors='coerce')
            if 'lat' in df.columns:
                result_df['latitude'] = pd.to_numeric(df['lat'], errors='coerce')

            # 温度转换：华氏度(°F) → 摄氏度(°C)
            if 'tmpf' in df.columns:
                temp_series = pd.to_numeric(df['tmpf'], errors='coerce')
                result_df['temp'] = (temp_series - 32) * 5 / 9
                del temp_series

            if 'dwpf' in df.columns:
                dewpoint_series = pd.to_numeric(df['dwpf'], errors='coerce')
                result_df['dewpoint'] = (dewpoint_series - 32) * 5 / 9
                del dewpoint_series

            if 'feel' in df.columns:
                feels_series = pd.to_numeric(df['feel'], errors='coerce')
                result_df['feels_like_c'] = (feels_series - 32) * 5 / 9
                del feels_series

            # 相对湿度
            if 'relh' in df.columns:
                result_df['relh'] = pd.to_numeric(df['relh'], errors='coerce')

            # 风速转换：节(kt) → 米/秒(m/s)
            if 'sknt' in df.columns:
                wind_series = pd.to_numeric(df['sknt'], errors='coerce')
                result_df['wind_speed'] = wind_series * 0.514444
                del wind_series

            if 'gust' in df.columns:
                gust_series = pd.to_numeric(df['gust'], errors='coerce')
                result_df['wind_gust'] = gust_series * 0.514444
                del gust_series

            if 'peak_wind_gust' in df.columns:
                peak_gust_series = pd.to_numeric(df['peak_wind_gust'], errors='coerce')
                result_df['peak_wind_gust'] = peak_gust_series * 0.514444
                del peak_gust_series

            # 风向
            if 'drct' in df.columns:
                result_df['wind_dir_degrees'] = pd.to_numeric(df['drct'], errors='coerce')
            if 'peak_wind_drct' in df.columns:
                result_df['peak_wind_drct'] = pd.to_numeric(df['peak_wind_drct'], errors='coerce')

            # 峰值风时间：同样按 UTC 语义解析并转为 naive UTC
            if 'peak_wind_time' in df.columns:
                peak_ts = pd.to_datetime(df['peak_wind_time'], errors='coerce', utc=True)
                result_df['peak_wind_time'] = peak_ts.dt.tz_convert('UTC').dt.tz_localize(None)
                if 'peak_wind_time' in result_df.columns:
                    result_df['peak_wind_time'] = result_df['peak_wind_time'].dt.floor('s')
                del peak_ts

            # 能见度转换：英里(mile) → 米(m)
            if 'vsby' in df.columns:
                vsby_series = pd.to_numeric(df['vsby'], errors='coerce')
                result_df['visibility_statute'] = vsby_series * 1609.34
                del vsby_series

            # 气压转换：英寸汞柱(inHg) → 百帕(hPa)
            if 'alti' in df.columns:
                alti_series = pd.to_numeric(df['alti'], errors='coerce')
                result_df['altim_in_hg'] = alti_series * 33.8639
                del alti_series

            if 'mslp' in df.columns:
                result_df['sea_level_pressure'] = pd.to_numeric(df['mslp'], errors='coerce')

            # 云高转换：英尺(ft) → 米(m) 和 云量
            cloud_mapping = {
                'skyc1': 'sky_cover1', 'skyl1': 'cloud1',
                'skyc2': 'sky_cover2', 'skyl2': 'cloud2',
                'skyc3': 'sky_cover3', 'skyl3': 'cloud3',
                'skyc4': 'sky_cover4', 'skyl4': 'cloud4'
            }

            for old_col, new_col in cloud_mapping.items():
                if old_col in df.columns:
                    if 'skyc' in old_col:  # 云量
                        result_df[new_col] = df[old_col]
                    else:  # 云高
                        cloud_series = pd.to_numeric(df[old_col], errors='coerce')
                        result_df[new_col] = cloud_series * 0.3048
                        del cloud_series

            # 降水量转换：英寸(in) → 毫米(mm)
            if 'p01i' in df.columns:
                precip_series = pd.to_numeric(df['p01i'], errors='coerce')
                result_df['precip'] = precip_series * 25.4
                del precip_series

            if 'snowdepth' in df.columns:
                snow_series = pd.to_numeric(df['snowdepth'], errors='coerce')
                result_df['snow'] = snow_series * 25.4
                del snow_series

            # 天气现象
            if 'wxcodes' in df.columns:
                result_df['weather'] = df['wxcodes']
            if 'metar' in df.columns:
                result_df['raw_text'] = df['metar']

            # 海拔高度
            if 'elevation' in df.columns:
                result_df['elevation_height'] = pd.to_numeric(df['elevation'], errors='coerce')

            # 添加更新时间：UTC，且只到秒
            result_df['updatetime'] = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)

            # 清理原始DataFrame
            del df

            # 内存统计
            end_memory = self._get_memory_usage()
            memory_used = end_memory - start_memory
            self._track_memory(f"after_convert_units_used_{memory_used:.2f}MB")

            logging.info(f"单位转换完成: 处理 {len(result_df)} 条数据, 内存使用: {memory_used:+.2f} MB")
            return result_df

        except Exception as e:
            logging.error(f"单位转换失败: {e}")
            import traceback
            logging.error(traceback.format_exc())
            return pd.DataFrame()
        finally:
            self._force_gc()

    def _clean_dataframe(self, df):
        """清理数据框，处理特殊值和类型转换（内存优化版）"""
        if df.empty:
            return df

        self._track_memory("before_clean_dataframe")
        start_memory = self._get_memory_usage()

        try:
            # 使用批处理优化，避免多次复制
            result_df = pd.DataFrame()

            # 定义数值字段
            numeric_columns = [
                'latitude', 'longitude', 'temp', 'dewpoint', 'feels_like_c', 'relh',
                'wind_dir_degrees', 'wind_speed', 'wind_gust', 'peak_wind_drct', 'peak_wind_gust',
                'visibility_statute', 'altim_in_hg', 'sea_level_pressure', 'cloud1', 'cloud2',
                'cloud3', 'cloud4', 'precip', 'snow', 'elevation_height'
            ]

            # 处理数值字段
            for col in numeric_columns:
                if col in df.columns:
                    series = pd.to_numeric(df[col], errors='coerce')
                    # 使用向量化操作处理无效值
                    mask = ~series.notna() | series.astype(str).str.lower().isin(['nan', 'none', 'null', '<null>', ''])
                    result_df[col] = series.where(~mask, None)
                    result_df[col] = result_df[col].astype('float32')
                    del series, mask

            # 处理时间字段
            time_columns = ['observation_time', 'peak_wind_time', 'updatetime']
            for col in time_columns:
                if col in df.columns:
                    result_df[col] = df[col]
                    mask = result_df[col].astype(str).str.lower().isin(['nan', 'none', 'null', '<null>', ''])
                    result_df.loc[mask, col] = None
                    if pd.api.types.is_datetime64_any_dtype(result_df[col]):
                        result_df[col] = result_df[col].dt.floor('s')

            # 处理文本字段
            text_columns = ['sky_cover1', 'sky_cover2', 'sky_cover3', 'sky_cover4', 'weather', 'raw_text']
            for col in text_columns:
                if col in df.columns:
                    series = df[col].astype(str)
                    mask = series.str.lower().isin(['nan', 'none', 'null', '<null>', ''])
                    result_df[col] = series.where(~mask, None)
                    del series, mask

            # 处理站点ID
            if 'station_id' in df.columns:
                series = df['station_id'].astype(str)
                mask = series.str.lower().isin(['nan', 'none', 'null', '<null>', ''])
                result_df['station_id'] = series.where(~mask, None)
                del series, mask

            # 清理原始DataFrame
            del df

            # 内存统计
            end_memory = self._get_memory_usage()
            memory_used = end_memory - start_memory
            self._track_memory(f"after_clean_dataframe_used_{memory_used:.2f}MB")

            return result_df

        except Exception as e:
            logging.error(f"数据清理失败: {e}")
            import traceback
            logging.error(traceback.format_exc())
            return pd.DataFrame()
        finally:
            self._force_gc()

    def _sanitize_data_for_table(self, data_dict: Dict, table_name: str) -> Dict:
        """根据表的实际结构清理数据，防止字段超长"""
        sanitized = {}

        # 获取表的字段信息
        if table_name not in self._column_info_cache:
            self._get_table_columns(table_name)  # 这会更新缓存

        if table_name in self._column_info_cache:
            column_info = self._column_info_cache[table_name]
        else:
            column_info = {}

        for key, value in data_dict.items():
            if key in column_info:
                col_type = column_info[key]['data_type']
                max_length = column_info[key]['max_length']

                if pd.isna(value) or value is None:
                    sanitized[key] = None
                elif col_type.startswith('character varying') or col_type == 'varchar':
                    # VARCHAR字段，需要截断
                    if max_length and isinstance(value, str):
                        if len(value) > max_length:
                            logging.debug(f"截断字段 {key}: {value[:50]}... (长度: {len(value)} > {max_length})")
                            sanitized[key] = value[:max_length]
                        else:
                            sanitized[key] = value
                    else:
                        sanitized[key] = str(value)[:255] if isinstance(value, str) else value
                else:
                    sanitized[key] = value
            else:
                sanitized[key] = value

        return sanitized

    def _batch_insert_to_table(self, data_list: List[Dict], table_name: str) -> int:
        """批量插入数据到指定表（内存优化版）"""
        if not data_list:
            return 0

        self._track_memory(f"before_batch_insert_{table_name}")
        start_memory = self._get_memory_usage()

        # 确保表存在
        if not self._check_table_exists(table_name):
            if not self._create_partition_table(table_name):
                return 0

        # 获取表的字段
        table_columns = self._get_table_columns(table_name)
        if not table_columns:
            logging.error(f"无法获取表 {table_name} 的字段列表")
            return 0

        success_count = 0
        batch_size = 100  # 减小批处理大小，减少内存峰值
        total_batches = (len(data_list) + batch_size - 1) // batch_size

        for batch_idx in range(total_batches):
            batch_start = batch_idx * batch_size
            batch_end = min((batch_idx + 1) * batch_size, len(data_list))
            batch_data = data_list[batch_start:batch_end]

            try:
                with self.connection.cursor() as cursor:
                    # 处理当前批次
                    corrected_batch = []
                    normal_batch = []

                    for data_dict in batch_data:
                        # 只保留表中存在的字段
                        filtered_data = {k: v for k, v in data_dict.items() if k in table_columns}

                        # 清理数据，防止字段超长
                        sanitized_data = self._sanitize_data_for_table(filtered_data, table_name)

                        # 确保manually_corrected字段存在
                        if 'manually_corrected' not in sanitized_data:
                            sanitized_data['manually_corrected'] = False
                        # 检测手动修正
                        if 'raw_text' in sanitized_data:
                            sanitized_data['manually_corrected'] = self._is_manually_corrected(
                                sanitized_data['raw_text']
                            )

                        if sanitized_data.get('manually_corrected'):
                            corrected_batch.append(sanitized_data)
                        else:
                            normal_batch.append(sanitized_data)

                    # 1. 插入修正数据（需要更新）
                    if corrected_batch:
                        success_count += self._insert_batch_with_update(cursor, corrected_batch, table_name,
                                                                        table_columns)

                    # 2. 插入普通数据（仅插入新数据）
                    if normal_batch:
                        success_count += self._insert_batch_without_update(cursor, normal_batch, table_name,
                                                                           table_columns)

                    # 及时提交和清理批次数据
                    self.connection.commit()

                    # 清理当前批次数据
                    del corrected_batch, normal_batch, batch_data
                    self._force_gc()

                    logging.debug(f"表 {table_name} 批次 {batch_idx + 1}/{total_batches} 完成")

            except Exception as e:
                logging.error(f"插入到表 {table_name} 批次 {batch_idx + 1} 失败: {e}")
                if self.connection:
                    self.connection.rollback()
                # 继续处理下一批

        # 清理数据列表引用
        del data_list

        # 内存统计
        end_memory = self._get_memory_usage()
        memory_used = end_memory - start_memory
        self._track_memory(f"after_batch_insert_{table_name}_used_{memory_used:.2f}MB")

        logging.debug(f"表 {table_name} 插入完成: 成功 {success_count} 条, 内存使用: {memory_used:+.2f} MB")
        return success_count

    def _insert_batch_with_update(self, cursor, data_list: List[Dict], table_name: str, table_columns: Set[str]) -> int:
        """插入并更新修正数据（内存优化版）"""
        if not data_list:
            return 0

        success_count = 0
        insert_batch_size = 50  # 减小批插入大小

        for i in range(0, len(data_list), insert_batch_size):
            batch = data_list[i:i + insert_batch_size]

            # 构建插入语句
            columns = [col for col in batch[0].keys() if col in table_columns]
            if not columns:
                continue

            columns_str = ", ".join(columns)
            placeholders_str = ", ".join(["%s"] * len(columns))

            # 构建更新子句
            update_columns = [col for col in columns if col != 'manually_corrected']
            update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns])
            update_clause += ", manually_corrected = TRUE"

            insert_sql = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES ({placeholders_str})
            ON CONFLICT (station_id, observation_time) 
            DO UPDATE SET {update_clause}
            """

            try:
                # 批量执行
                values_list = []
                for data_dict in batch:
                    values = [data_dict.get(col) for col in columns]
                    values_list.append(values)

                cursor.executemany(insert_sql, values_list)
                success_count += len(batch)

                # 清理批次数据
                del values_list, batch

            except Exception as e:
                logging.warning(f"批量更新修正数据失败: {e}")
                # 回退到逐条插入
                for data_dict in batch:
                    try:
                        values = [data_dict.get(col) for col in columns]
                        cursor.execute(insert_sql, values)
                        success_count += 1
                    except Exception as e2:
                        logging.warning(f"单条更新修正数据失败: {e2}")
                        continue

        return success_count

    def _insert_batch_without_update(self, cursor, data_list: List[Dict], table_name: str,
                                     table_columns: Set[str]) -> int:
        """仅插入新数据（不更新已存在的，内存优化版）"""
        if not data_list:
            return 0

        success_count = 0
        insert_batch_size = 50  # 减小批插入大小

        for i in range(0, len(data_list), insert_batch_size):
            batch = data_list[i:i + insert_batch_size]

            # 构建插入语句
            columns = [col for col in batch[0].keys() if col in table_columns]
            if not columns:
                continue

            columns_str = ", ".join(columns)
            placeholders_str = ", ".join(["%s"] * len(columns))

            insert_sql = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES ({placeholders_str})
            ON CONFLICT (station_id, observation_time) 
            DO NOTHING
            """

            try:
                # 批量执行
                values_list = []
                for data_dict in batch:
                    values = [data_dict.get(col) for col in columns]
                    values_list.append(values)

                cursor.executemany(insert_sql, values_list)
                success_count += cursor.rowcount

                # 清理批次数据
                del values_list, batch

            except Exception as e:
                logging.warning(f"批量插入普通数据失败: {e}")
                # 回退到逐条插入
                for data_dict in batch:
                    try:
                        values = [data_dict.get(col) for col in columns]
                        cursor.execute(insert_sql, values)
                        if cursor.rowcount > 0:
                            success_count += 1
                    except Exception as e2:
                        logging.warning(f"单条插入普通数据失败: {e2}")
                        continue

        return success_count

    def _route_data_to_tables(self, df: pd.DataFrame) -> Dict[str, List[Dict]]:
        """将数据按观测时间路由到不同的表（内存优化版）"""
        if df.empty or 'observation_time' not in df.columns:
            return {}

        self._track_memory("before_route_data")
        start_memory = self._get_memory_usage()

        # 确保observation_time是datetime类型
        if not pd.api.types.is_datetime64_any_dtype(df['observation_time']):
            df['observation_time'] = pd.to_datetime(df['observation_time'], errors='coerce')

        # 使用分组替代iterrows，更高效
        table_groups = {}

        # 过滤掉无效时间
        valid_mask = df['observation_time'].notna()
        if not valid_mask.any():
            return {}

        # 创建新的DataFrame避免修改原数据
        valid_df = pd.DataFrame()
        valid_df['observation_time'] = df['observation_time'][valid_mask]

        # 复制其他字段
        for col in df.columns:
            if col != 'observation_time':
                valid_df[col] = df[col]

        # 按表名分组处理
        valid_df['_table_name'] = valid_df['observation_time'].apply(self._get_partition_table_name)

        for table_name, group in valid_df.groupby('_table_name'):
            # 转换为字典列表
            data_list = []
            for _, row in group.iterrows():
                row_dict = row.to_dict()
                # 删除临时字段
                row_dict.pop('_table_name', None)
                data_list.append(row_dict)

            table_groups[table_name] = data_list

            # 及时清理分组数据
            del group
            self._force_gc()

        # 清理原始DataFrame
        del df, valid_df

        # 内存统计
        end_memory = self._get_memory_usage()
        memory_used = end_memory - start_memory
        self._track_memory(f"after_route_data_used_{memory_used:.2f}MB")

        return table_groups

    def fetch_data_for_station(self, station):
        """获取单个站点的数据并插入数据库（内存优化版）"""
        if not self.ensure_connection():
            logging.error(f"获取站点 {station} 数据失败：数据库连接不可用")
            return

        url = self.create_url(station)
        response = None
        total_inserted = 0

        try:
            logging.info(f"正在请求数据: {url}")
            self._track_memory(f"before_fetch_{station}")

            response = requests.get(url, timeout=30)
            response.raise_for_status()
            logging.info(f"成功获取数据，站点：{station}")

            # 读取CSV数据，指定数据类型减少内存
            data = pd.read_csv(io.StringIO(response.text), low_memory=False)

            if data.empty:
                logging.info(f"站点 {station} 无数据")
                return

            # 处理数据并跟踪内存
            data_converted = self.convert_units_to_metric(data)
            data_cleaned = self._clean_dataframe(data_converted)

            # 按时间路由数据到不同的表
            table_groups = self._route_data_to_tables(data_cleaned)

            # 确保涉及的表都存在
            table_names = list(table_groups.keys())
            if table_names:
                self._ensure_partition_tables_exist(table_names)

            # 插入到各自的表中
            for table_name, data_list in table_groups.items():
                if data_list:
                    inserted = self._batch_insert_to_table(data_list, table_name)
                    total_inserted += inserted
                    if inserted > 0:
                        logging.debug(f"站点 {station} 向表 {table_name} 插入了 {inserted} 条数据")

                    # 及时清理数据列表
                    del data_list
                    self._force_gc()

            if total_inserted > 0:
                logging.info(f"站点 {station} 处理完成，共插入 {total_inserted} 条数据")
            else:
                logging.info(f"站点 {station} 无新数据需要插入")

        except requests.exceptions.RequestException as e:
            logging.error(f"请求失败，站点 {station}: {e}")
        except Exception as e:
            logging.error(f"处理站点 {station} 数据时发生错误: {e}")
            import traceback
            logging.error(traceback.format_exc())
        finally:
            # 彻底释放所有资源
            if response is not None:
                response.close()
                response = None

            # 强制清理所有可能的大对象
            self._force_gc()
            self._cleanup_dataframe_references()

            # 记录最终内存状态
            self._track_memory(f"after_fetch_{station}_complete")

            # 显示内存差异
            try:
                final_memory = self._get_memory_usage()
                if self._memory_baseline:
                    delta = final_memory - self._memory_baseline
                    if delta > 10:  # 如果内存增长超过10MB
                        logging.warning(f"站点 {station} 处理后内存增长: {delta:+.2f} MB")
            except:
                pass

    def process_stations(self):
        """处理所有站点的数据（内存优化版）"""
        if not self.ensure_connection():
            logging.error("处理站点数据失败：数据库连接不可用")
            return

        try:
            # 重置内存基线
            self._memory_baseline = self._get_memory_usage()
            logging.info(f"开始处理前内存基线: {self._memory_baseline:.2f} MB")

            # 确保当前日期所在的表存在
            current_time = datetime.now()
            current_table = self._get_partition_table_name(current_time)
            if not self._check_table_exists(current_table):
                self._create_partition_table(current_table)

            with open(self.input_file, 'r') as file:
                stations = [line.strip() for line in file if line.strip()]

            logging.info(f"开始处理 {len(stations)} 个站点")

            # 分批处理站点，避免同时加载太多数据
            batch_size = 3  # 进一步减少批处理大小
            total_batches = (len(stations) + batch_size - 1) // batch_size

            for batch_idx in range(total_batches):
                batch_start = batch_idx * batch_size
                batch_end = min((batch_idx + 1) * batch_size, len(stations))
                batch_stations = stations[batch_start:batch_end]

                logging.info(f"处理批次 {batch_idx + 1}/{total_batches}: {len(batch_stations)} 个站点")

                # 使用顺序处理，避免线程池内存累积
                for station in batch_stations:
                    try:
                        self.fetch_data_for_station(station)
                    except Exception as e:
                        logging.error(f"处理站点 {station} 失败: {e}")

                # 批次完成后彻底清理内存
                del batch_stations

                # 强制垃圾回收
                self._force_gc()
                self._cleanup_dataframe_references()

                # 清理pandas可能的缓存
                try:
                    import pandas.core.algorithms as alg
                    for attr in ['_unique', '_mode', '_rank']:
                        if hasattr(alg, attr):
                            setattr(alg, attr, None)
                except:
                    pass

                # 报告当前内存状态
                current_memory = self._get_memory_usage()
                if self._memory_baseline:
                    delta = current_memory - self._memory_baseline
                    logging.info(f"批次 {batch_idx + 1} 完成，内存状态: {current_memory:.2f} MB (增量: {delta:+.2f} MB)")

                    # 如果内存增长过多，执行深度清理
                    if delta > 20:
                        logging.warning(f"批次 {batch_idx + 1} 内存增长过多，执行深度清理")
                        self._perform_deep_cleanup()

            # 最终内存清理
            del stations
            self._force_gc()
            self._cleanup_dataframe_references()

            # 最终内存报告
            final_memory = self._get_memory_usage()
            if self._memory_baseline:
                total_delta = final_memory - self._memory_baseline
                logging.info(f"所有站点处理完成，最终内存: {final_memory:.2f} MB (总增量: {total_delta:+.2f} MB)")

                if total_delta > 20:  # 如果总内存增长超过20MB
                    logging.warning(f"内存泄漏警告: 处理结束后内存增长 {total_delta:.2f} MB")
                    # 执行更彻底的内存清理
                    self._perform_deep_cleanup()

        except FileNotFoundError:
            logging.error(f"文件 {self.input_file} 未找到！")
        except Exception as e:
            logging.error(f"处理站点数据时发生错误: {e}")
            import traceback
            logging.error(traceback.format_exc())
        finally:
            # 执行最终清理
            self._perform_deep_cleanup()

    def _perform_deep_cleanup(self):
        """执行深度内存清理"""
        logging.info("执行深度内存清理...")

        # 清理所有缓存
        self._table_columns_cache.clear()
        self._table_exists_cache.clear()
        self._created_tables_this_session.clear()
        self._column_info_cache.clear()
        self._memory_tracker.clear()

        # 清理pandas缓存
        self._cleanup_dataframe_references()

        # 清理requests会话缓存
        try:
            import requests
            requests.adapters.HTTPAdapter().close()
        except:
            pass

        # 多次强制垃圾回收
        for i in range(5):
            collected = gc.collect(2)  # 深度回收
            if collected == 0 and i > 1:
                break

        # 报告清理后内存
        final_memory = self._get_memory_usage()
        logging.info(f"深度清理后内存: {final_memory:.2f} MB")

    def _is_manually_corrected(self, raw_text):
        """检查METAR数据是否为手动修正报告"""
        if not raw_text or pd.isna(raw_text) or str(raw_text).lower() in ['nan', 'none', 'null', '<null>', '']:
            return False

        try:
            raw_lower = str(raw_text).lower()
            manual_indicators = ['manually corrected', 'corrected', 'cor ', 'cor.', 'corr']
            return any(indicator in raw_lower for indicator in manual_indicators)
        except Exception as e:
            logging.debug(f"METAR解析失败: {e}")
            return False

    def start_auto_update(self):
        """启动自动更新任务 - 每20分钟的30秒执行一次"""
        if self.is_running:
            logging.warning("自动更新任务已经在运行")
            return

        try:
            self.scheduler = BackgroundScheduler(daemon=True)

            # 使用cron触发器，每20分钟的30秒执行
            self.scheduler.add_job(
                func=self._safe_process_stations,
                trigger='cron',
                minute='*/20',
                second=30,
                id='iem_metar_update',
                name='IEM_METAR每20分钟更新',
                misfire_grace_time=30,
                coalesce=True,
                max_instances=1
            )

            # 启动调度器
            self.scheduler.start()
            self.is_running = True

            # 立即执行一次
            logging.info("立即执行第一次数据更新...")
            self._safe_process_stations()

            self._keep_main_thread_alive()

        except Exception as e:
            logging.error(f"启动定时任务失败: {e}")
            if self.scheduler:
                self.scheduler.shutdown()
            raise

    def _safe_process_stations(self):
        """安全处理站点数据，包含异常捕获和内存清理"""
        try:
            logging.info("开始执行定时任务...")

            # 重置内存基线
            self._memory_baseline = self._get_memory_usage()

            self.process_stations()
            logging.info("定时任务执行完成")

            # 任务完成后立即深度清理
            self._perform_deep_cleanup()

        except Exception as e:
            logging.error(f"定时任务执行失败: {e}")
            # 即使失败也执行清理
            self._perform_deep_cleanup()
        finally:
            # 确保每次运行后都进行深度清理
            self._perform_deep_cleanup()

    def _keep_main_thread_alive(self):
        """保持主线程运行"""
        try:
            last_cleanup_time = time.time()
            last_deep_cleanup_time = time.time()

            while self.is_running and self.scheduler.running:
                current_time = time.time()

                # 定期内存清理（每2分钟）
                if current_time - last_cleanup_time > 120:
                    self._force_gc()
                    last_cleanup_time = current_time
                    logging.debug("执行定期内存清理")

                # 深度内存清理（每15分钟）
                if current_time - last_deep_cleanup_time > 900:
                    self._perform_deep_cleanup()
                    last_deep_cleanup_time = current_time
                    logging.info("执行深度内存清理")

                # 短暂休眠
                time.sleep(0.5)

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
        self.close()

        # 最终内存清理
        self._perform_deep_cleanup()
        logging.info("程序关闭完成")


if __name__ == "__main__":
    iem_db = IEM_METAR(partition_days=10)

    if iem_db.connect():
        try:
            # 启动自动更新
            iem_db.start_auto_update()

        except Exception as e:
            logging.error(f"程序运行出错: {e}")
            iem_db._shutdown()
    else:
        logging.error("数据库连接失败，程序退出")
