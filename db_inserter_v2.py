#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据库插入模块 V2 - 支持METAR/TAF实时入库（字段与实际数据一致）
"""
import psycopg2
from psycopg2 import sql, extras
from psycopg2.extras import execute_values
from datetime import datetime, timezone, timedelta
import json
from typing import Dict, List, Optional
import threading
import gc
try:
    from db_config_metar_taf import DB_CONFIG
except ImportError:
    try:
        from db_config import DB_CONFIG
    except ImportError:
        try:
            import sys
            import os
            parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            if parent_dir not in sys.path:
                sys.path.insert(0, parent_dir)
            from db_config_metar_taf import DB_CONFIG
        except ImportError:
            try:
                from db_config import get_db_config
                DB_CONFIG = get_db_config('metar_taf')
            except ImportError:
                import os
                DB_CONFIG = {
                    'host': os.getenv('DB_HOST', 'database-1.cxik82g8267p.us-east-2.rds.amazonaws.com'),
                    'port': int(os.getenv('DB_PORT', '5432')),
                    'database': 'metar_taf_synop_data',
                    'user': os.getenv('DB_USER', 'postgres'),
                    'password': os.getenv('DB_PASSWORD', 'oceantest1')
                }


class DatabaseInserter:
    """数据库插入器 V2 - 线程安全，字段映射与实际JSON数据一致，支持分表"""
    @staticmethod
    def _handle_missing_value(value, field_type='numeric'):
        """
        处理缺失值
        - 数值类型（numeric）：None 或空字符串 -> -999
        - 整数类型（integer）：None 或空字符串 -> -999
        - 字符类型（text）：None 或空字符串 -> None (NULL)
        - 布尔类型：保持原样
        - 时间类型（timestamp）：None 或空字符串 -> None (NULL)
        """
        if value is None or (isinstance(value, str) and value.strip() == ''):
            if field_type == 'numeric' or field_type == 'integer':
                return -999
            else: 
                return None
        if field_type == 'integer' and isinstance(value, (int, float, str)):
            try:
                return int(float(value))
            except (ValueError, TypeError):
                return -999
        return value
    
    @staticmethod
    def _handle_timestamp_value(value):
        """
        处理时间戳值
        - None 或空字符串 -> None (NULL)
        - 其他值保持原样
        """
        if value is None or (isinstance(value, str) and value.strip() == ''):
            return None
        return value
    
    def __init__(self, db_config: Dict = None, table_prefix: str = 'ogimet', enable_partitioning: bool = True):
        """
        初始化数据库连接
        
        Args:
            db_config: 数据库配置字典
            table_prefix: 表名前缀，'ogimet' 或 'skyvector'
            enable_partitioning: 是否启用分表功能（默认True）
        """
        self.db_config = db_config or DB_CONFIG
        self.table_prefix = table_prefix
        self.base_metar_table = f'{table_prefix}_metar_data'
        self.base_taf_table = f'{table_prefix}_taf_data'
        self.base_synop_table = f'{table_prefix}_synop_data'
        self.enable_partitioning = enable_partitioning
        self.partition_start_date = datetime(2025, 1, 1, 0, 0, 0)  
        self.metar_table = self.base_metar_table
        self.taf_table = self.base_taf_table
        self.synop_table = self.base_synop_table
        self.conn = None
        self.lock = threading.Lock()
        self.partition_lock = threading.Lock()  
        self.created_partitions = set()  
        self._max_partition_cache = 100  
        self._connect()
        if self.enable_partitioning:
            if 'ogimet' in table_prefix:
                print(f"使用分表模式: {self.base_metar_table}_YYYYMMDD_YYYYMMDD (十天), {self.base_taf_table}_YYYYMM (月), {self.base_synop_table}_YYYYMMDD_YYYYMMDD (十天)")
            else:
                print(f"使用分表模式: {self.base_metar_table}_YYYYMMDD_YYYYMMDD (十天), {self.base_taf_table}_YYYYMMDD_YYYYMMDD (十天), {self.base_synop_table}_YYYYMMDD_YYYYMMDD (十天)")
        else:
            print(f"使用数据表: {self.metar_table}, {self.taf_table}, {self.synop_table}")
    
    def _connect(self):
        """建立数据库连接"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.conn.autocommit = False 
            print(f"✓ 已连接到数据库: {self.db_config['database']}")
        except Exception as e:
            print(f"✗ 数据库连接失败: {e}")
            raise
    
    def _reconnect(self):
        """重新连接数据库"""
        try:
            if self.conn:
                self.conn.close()
        except:
            pass
        self._connect()
    
    def _get_partition_table_name(self, base_table: str, obs_time_str: str, table_type: str = 'metar') -> str:
        """
        根据观测时间计算分表名称
        
        例如：2025-12-28 在 2025-12-25 到 2026-01-03 的分区中
        表名：ogimet_metar_data_20251225_20260103
        
        Args:
            base_table: 基础表名（如 ogimet_metar_data）
            obs_time_str: 观测时间字符串（格式：'YYYY-MM-DD HH:MM:SS'）
            table_type: 表类型，'metar'/'synop'/'taf'（都按十天分表，只有ogimet_taf按月分表）
        
        Returns:
            分表名称，如 ogimet_metar_data_20250101_20250110
        """
        if not self.enable_partitioning:
            return base_table
        
        if not obs_time_str:
            return base_table
        
        try:
            observation_time = datetime.strptime(obs_time_str, '%Y-%m-%d %H:%M:%S')
            if observation_time.tzinfo is not None:
                observation_time = observation_time.astimezone(timezone.utc).replace(tzinfo=None)
            if observation_time < self.partition_start_date:
                return base_table
            if table_type == 'taf' and 'ogimet' in base_table:
                month_str = observation_time.strftime('%Y%m')
                return f"{base_table}_{month_str}"
            else:
                base_date = observation_time.date()
                base_reference = self.partition_start_date.date()
                days_from_reference = (base_date - base_reference).days
                partition_days = 10  
                partition_num = days_from_reference // partition_days
                partition_start_day = partition_num * partition_days
                partition_end_day = partition_start_day + partition_days - 1
                start_date = base_reference + timedelta(days=partition_start_day)
                end_date = base_reference + timedelta(days=partition_end_day)
                table_suffix = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
                return f"{base_table}_{table_suffix}"
        
        except Exception as e:
            print(f"    解析观测时间失败: {e}，使用原表 {base_table}")
            return base_table
    
    def _get_partition_table_names_vectorized(self, base_table: str, observation_times: list, table_type: str = 'metar') -> dict:
        """
        向量化计算分表名称（性能优化：批量处理）
        
        Args:
            base_table: 基础表名
            observation_times: 观测时间字符串列表
            table_type: 表类型
        
        Returns:
            dict: {target_table: [indices]} 分表名到数据索引的映射
        """
        if not self.enable_partitioning or not observation_times:
            return {base_table: list(range(len(observation_times)))}
        import pandas as pd
        import gc
        times = None
        table_names = None
        try:
            times = pd.to_datetime(observation_times, format='%Y-%m-%d %H:%M:%S', errors='coerce')
            base_reference = pd.Timestamp(self.partition_start_date.date())
            result_map = {}
            is_ogimet_taf = (table_type == 'taf' and 'ogimet' in base_table)
            if is_ogimet_taf:
                suffixes = times.dt.year.astype(str) + times.dt.month.apply(lambda x: f'{x:02d}' if pd.notna(x) else '')
                table_names = base_table + '_' + suffixes
            else:
                days_from_reference = (times - base_reference).dt.days
                partition_num = days_from_reference // 10
                start_days = partition_num * 10
                end_days = start_days + 9
                start_dates = base_reference + pd.to_timedelta(start_days, unit='D')
                end_dates = base_reference + pd.to_timedelta(end_days, unit='D')
                suffixes = start_dates.dt.strftime('%Y%m%d') + '_' + end_dates.dt.strftime('%Y%m%d')
                table_names = base_table + '_' + suffixes
            invalid_mask = times.isna() | (times < self.partition_start_date)
            table_names[invalid_mask] = base_table
            for idx, table_name in enumerate(table_names):
                if table_name not in result_map:
                    result_map[table_name] = []
                result_map[table_name].append(idx)
            return result_map
        except Exception as e:
            print(f"  向量化计算分表名失败: {e}，使用原表")
            return {base_table: list(range(len(observation_times)))}
        finally:
            if times is not None:
                del times
            if table_names is not None:
                del table_names
            gc.collect()
    
    def _create_partition_table_if_not_exists(self, partition_table: str, base_table: str, table_type: str = 'metar'):
        """
        如果分表不存在，则创建分表（线程安全，并发冲突容错）
        
        并发安全策略：
        1. 使用内存缓存快速跳过已创建的分表
        2. 使用线程锁防止并发创建同一个分表
        3. 使用 CREATE TABLE IF NOT EXISTS - 数据库级别的并发控制
        4. 捕获所有"已存在"类型的错误
        5. 只记录警告不抛出 - 避免阻塞数据插入流程
        
        Args:
            partition_table: 分表名称
            base_table: 基表名称
            table_type: 表类型（'metar', 'taf', 'synop'）
        """
        if partition_table == base_table:
            return
        if partition_table in self.created_partitions:
            return
        with self.partition_lock:
            if partition_table in self.created_partitions:
                return
            cursor = None
            try:
                cursor = self.conn.cursor()
                try:
                    cursor.execute(f"""
                        CREATE TABLE IF NOT EXISTS {partition_table} (
                            LIKE {base_table} INCLUDING ALL
                        )
                    """)
                    self.conn.commit()
                    print(f"  ✓ 创建分表: {partition_table}")
                    self.created_partitions.add(partition_table)
                except Exception as e:
                    error_msg = str(e).lower()
                    self.conn.rollback()
                    is_exists_error = any(keyword in error_msg for keyword in [
                        'already exists',
                        'duplicate',
                        'pg_type_typname_nsp_index',
                        'unique constraint',
                        'relation' ])
                    if is_exists_error:
                        self.created_partitions.add(partition_table)
                        if len(self.created_partitions) > self._max_partition_cache:
                            items = list(self.created_partitions)
                            self.created_partitions = set(items[-self._max_partition_cache:])
                        print(f"  分表已存在: {partition_table}")
                    else:
                        print(f"  创建分表异常 {partition_table}: {e}")
            
            except Exception as e:
                if self.conn:
                    try:
                        self.conn.rollback()
                    except:
                        pass
                print(f"   创建分表失败 {partition_table}: {e}")
            finally:
                if cursor:
                    try:
                        if not cursor.closed:
                            cursor.close()
                    except:
                        pass
    
    def insert_metar(self, data: Dict) -> bool:
        """
        插入或更新METAR数据
        
        字段映射规则：
        1. 保留所有本地保存的字段
        2. 如果含义相同，使用"入库机场字段说明.md"中的字段名和类型
        3. 只入库公制单位（altimeter_hpa保留百帕，不转换为英寸汞柱）
        
        更新逻辑：
        - 唯一键：(station_id, observation_time)
        - 只有当新数据是修正报(AMD/COR)且原始文本不同时，才更新现有记录
        - 只对观测时间在最近两天内的记录进行比较和更新（历史数据不更新）
        - 如果记录已存在但新数据不是修正报或原始文本相同，则跳过更新（与本地保存逻辑一致）
        - crawl_time保留首次值，update_count递增
        
        Args:
            data: METAR数据字典（来自JSON文件的字段）
        
        Returns:
            bool: 成功返回True，失败返回False
        """
        if not data or not data.get('station_code'):
            return False
        
        with self.lock:
            try:
                cursor = self.conn.cursor()
                insert_data = self._prepare_metar_data(data)
                obs_time = insert_data.get('observation_time')
                target_table = self._get_partition_table_name(self.base_metar_table, obs_time, 'metar')
                if target_table != self.base_metar_table:
                    self._create_partition_table_if_not_exists(target_table, self.base_metar_table, 'metar')
                insert_sql = f"""
                    INSERT INTO {target_table} (
                        raw_text, corrected, auto, station_id, observation_time,
                        latitude, longitude, temp, dewpoint,
                        weather, sky, wind_direction_variation, wind_dir_degrees,
                        wind_speed, wind_gust, peak_wind_drct, peak_wind_gust,
                        visibility_statute, altimeter_hpa, sea_level_pressure,
                        pcp1h, pcp3h, pcp6h, pcp24h,
                        maxT6h, minT6h, maxT24h, minT24h,
                        metar_type, remarks, rvr,
                        country, crawl_time, updatetime, update_count, create_time
                    ) VALUES (
                        %(raw_text)s, %(corrected)s, %(auto)s, %(station_id)s, %(observation_time)s,
                        %(latitude)s, %(longitude)s, %(temp)s, %(dewpoint)s,
                        %(weather)s, %(sky)s, %(wind_direction_variation)s, %(wind_dir_degrees)s,
                        %(wind_speed)s, %(wind_gust)s, %(peak_wind_drct)s, %(peak_wind_gust)s,
                        %(visibility_statute)s, %(altimeter_hpa)s, %(sea_level_pressure)s,
                        %(pcp1h)s, %(pcp3h)s, %(pcp6h)s, %(pcp24h)s,
                        %(maxT6h)s, %(minT6h)s, %(maxT24h)s, %(minT24h)s,
                        %(metar_type)s, %(remarks)s, %(rvr)s,
                        %(country)s, %(crawl_time)s, %(updatetime)s, %(update_count)s, %(create_time)s
                    )
                    ON CONFLICT (station_id, observation_time)
                    DO UPDATE SET
                        raw_text = EXCLUDED.raw_text,
                        corrected = EXCLUDED.corrected,
                        auto = EXCLUDED.auto,
                        temp = EXCLUDED.temp,
                        dewpoint = EXCLUDED.dewpoint,
                        wind_dir_degrees = EXCLUDED.wind_dir_degrees,
                        wind_speed = EXCLUDED.wind_speed,
                        wind_gust = EXCLUDED.wind_gust,
                        wind_direction_variation = EXCLUDED.wind_direction_variation,
                        visibility_statute = EXCLUDED.visibility_statute,
                        altimeter_hpa = EXCLUDED.altimeter_hpa,
                        sea_level_pressure = EXCLUDED.sea_level_pressure,
                        weather = EXCLUDED.weather,
                        sky = EXCLUDED.sky,
                        pcp1h = EXCLUDED.pcp1h,
                        pcp3h = EXCLUDED.pcp3h,
                        pcp6h = EXCLUDED.pcp6h,
                        pcp24h = EXCLUDED.pcp24h,
                        peak_wind_drct = EXCLUDED.peak_wind_drct,
                        peak_wind_gust = EXCLUDED.peak_wind_gust,
                        maxT6h = EXCLUDED.maxT6h,
                        minT6h = EXCLUDED.minT6h,
                        maxT24h = EXCLUDED.maxT24h,
                        minT24h = EXCLUDED.minT24h,
                        metar_type = EXCLUDED.metar_type,
                        remarks = EXCLUDED.remarks,
                        rvr = EXCLUDED.rvr,
                        country = EXCLUDED.country,
                        crawl_time = COALESCE({target_table}.crawl_time, EXCLUDED.crawl_time),
                        updatetime = EXCLUDED.updatetime,
                        update_count = {target_table}.update_count + 1
                        -- create_time 不更新，保留首次入库时间
                    WHERE EXCLUDED.corrected = TRUE 
                      AND {target_table}.raw_text != EXCLUDED.raw_text
                """
                cursor.execute(insert_sql, insert_data)
                self.conn.commit()
                cursor.close()
                return True
            except Exception as e:
                self.conn.rollback()
                print(f"  ✗ METAR入库失败: {e}")
                try:
                    self._reconnect()
                except:
                    pass
                return False
    
    def insert_taf(self, data: Dict) -> bool:
        """
        插入或更新TAF数据
        
        更新逻辑：
        - 唯一键：(station_id, observation_time)
        - 只有当新数据是修正报(AMD/COR)且原始文本不同时，才更新现有记录
        - 只对发布时间在最近两天内的记录进行比较和更新（历史数据不更新）
        - 如果记录已存在但新数据不是修正报或原始文本相同，则跳过更新（与本地保存逻辑一致）
        - crawl_time保留首次值，update_count递增
        
        Args:
            data: TAF数据字典（JSON字段名：timestamp_utc, station_code）
        
        Returns:
            bool: 成功返回True，失败返回False
        """
        if not data or not data.get('station_code'):
            return False
        
        with self.lock:
            try:
                cursor = self.conn.cursor()
                insert_data = self._prepare_taf_data(data)
                obs_time = insert_data.get('observation_time')
                target_table = self._get_partition_table_name(self.base_taf_table, obs_time, 'taf')
                if target_table != self.base_taf_table:
                    self._create_partition_table_if_not_exists(target_table, self.base_taf_table, 'taf')
                insert_sql = f"""
                    INSERT INTO {target_table} (
                        raw_taf, observation_time, station_id,
                        latitude, longitude, country,
                        is_corrected, valid_from, valid_to,
                        wind_direction_deg, wind_speed_mps,
                        visibility_m, weather_phenomena, cloud_cover,
                        max_temp_c, min_temp_c, trends_descriptions,
                        crawl_time, update_time, update_count, create_time
                    ) VALUES (
                        %(raw_taf)s, %(observation_time)s, %(station_id)s,
                        %(latitude)s, %(longitude)s, %(country)s,
                        %(is_corrected)s, %(valid_from)s, %(valid_to)s,
                        %(wind_direction_deg)s, %(wind_speed_mps)s,
                        %(visibility_m)s, %(weather_phenomena)s, %(cloud_cover)s,
                        %(max_temp_c)s, %(min_temp_c)s, %(trends_descriptions)s,
                        %(crawl_time)s, %(update_time)s, %(update_count)s, %(create_time)s
                    )
                    ON CONFLICT (station_id, observation_time)
                    DO UPDATE SET
                        raw_taf = EXCLUDED.raw_taf,
                        is_corrected = EXCLUDED.is_corrected,
                        valid_from = EXCLUDED.valid_from,
                        valid_to = EXCLUDED.valid_to,
                        wind_direction_deg = EXCLUDED.wind_direction_deg,
                        wind_speed_mps = EXCLUDED.wind_speed_mps,
                        visibility_m = EXCLUDED.visibility_m,
                        weather_phenomena = EXCLUDED.weather_phenomena,
                        cloud_cover = EXCLUDED.cloud_cover,
                        max_temp_c = EXCLUDED.max_temp_c,
                        min_temp_c = EXCLUDED.min_temp_c,
                        trends_descriptions = EXCLUDED.trends_descriptions,
                        crawl_time = COALESCE({target_table}.crawl_time, EXCLUDED.crawl_time),
                        update_time = EXCLUDED.update_time,
                        update_count = {target_table}.update_count + 1
                    WHERE EXCLUDED.is_corrected = TRUE 
                      AND {target_table}.raw_taf != EXCLUDED.raw_taf
                """
                
                cursor.execute(insert_sql, insert_data)
                self.conn.commit()
                cursor.close()
                return True
                
            except Exception as e:
                self.conn.rollback()
                print(f"  ✗ TAF入库失败: {e}")
                try:
                    self._reconnect()
                except:
                    pass
                return False
    
    def insert_metar_batch(self, data_list: List[Dict], batch_size: int = 50) -> int:
        """
        批量插入METAR数据（性能优化版：使用execute_values，支持分表）
        
        Args:
            data_list: METAR数据字典列表
            batch_size: 每批插入的记录数（默认50，METAR字段较多，减小批次以节省内存）
        
        Returns:
            int: 成功插入的记录数
        """
        if not data_list:
            return 0
        success_count = 0
        with self.lock:
            try:
                cursor = self.conn.cursor()
                prepared_list = []
                obs_times = []
                for data in data_list:
                    if not data or not data.get('station_code'):
                        continue
                    prepared = self._prepare_metar_data(data)
                    if prepared.get('raw_text'):
                        prepared_list.append(prepared)
                        obs_times.append(prepared.get('observation_time'))
                table_indices_map = self._get_partition_table_names_vectorized(
                    self.base_metar_table, obs_times, 'metar'
                )
                idx_to_table = {}
                for table_name, indices in table_indices_map.items():
                    for idx in indices:
                        idx_to_table[idx] = table_name
                raw_text_map = {}  # raw_text -> (prepared_data, is_corrected, target_table)
                
                for idx, prepared in enumerate(prepared_list):
                    raw_text = prepared.get('raw_text')
                    is_corrected = prepared.get('corrected', False)
                    target_table = idx_to_table.get(idx, self.base_metar_table)
                    if raw_text in raw_text_map:
                        existing_prepared, existing_corrected, existing_table = raw_text_map[raw_text]
                        if is_corrected and not existing_corrected:
                            raw_text_map[raw_text] = (prepared, is_corrected, target_table)
                    else:
                        raw_text_map[raw_text] = (prepared, is_corrected, target_table)
                table_data_map = {}  # target_table -> [(prepared_data, ...), ...]
                for prepared, _, target_table in raw_text_map.values():
                    if target_table not in table_data_map:
                        table_data_map[target_table] = []
                    
                    table_data_map[target_table].append((
                        prepared['raw_text'],
                        prepared['corrected'],
                        prepared['auto'],
                        prepared['station_id'],
                        prepared['observation_time'],
                        prepared['latitude'],
                        prepared['longitude'],
                        prepared['temp'],
                        prepared['dewpoint'],
                        prepared['weather'],
                        prepared['sky'],
                        prepared['wind_direction_variation'],
                        prepared['wind_dir_degrees'],
                        prepared['wind_speed'],
                        prepared['wind_gust'],
                        prepared['peak_wind_drct'],
                        prepared['peak_wind_gust'],
                        prepared['visibility_statute'],
                        prepared['altimeter_hpa'],
                        prepared['sea_level_pressure'],
                        prepared['pcp1h'],
                        prepared['pcp3h'],
                        prepared['pcp6h'],
                        prepared['pcp24h'],
                        prepared['maxT6h'],
                        prepared['minT6h'],
                        prepared['maxT24h'],
                        prepared['minT24h'],
                        prepared['metar_type'],
                        prepared['remarks'],
                        prepared['rvr'],
                        prepared['country'],
                        prepared['crawl_time'],
                        prepared['updatetime'],
                        prepared['update_count'],
                        prepared['create_time']
                    ))
                
                if not table_data_map:
                    cursor.close()
                    return 0
                for target_table, batch_data in table_data_map.items():
                    if target_table != self.base_metar_table:
                        self._create_partition_table_if_not_exists(target_table, self.base_metar_table, 'metar')
                    insert_sql = f"""
                        INSERT INTO {target_table} (
                        raw_text, corrected, auto, station_id, observation_time,
                        latitude, longitude, temp, dewpoint,
                        weather, sky, wind_direction_variation, wind_dir_degrees,
                        wind_speed, wind_gust, peak_wind_drct, peak_wind_gust,
                        visibility_statute, altimeter_hpa, sea_level_pressure,
                        pcp1h, pcp3h, pcp6h, pcp24h,
                        maxT6h, minT6h, maxT24h, minT24h,
                        metar_type, remarks, rvr,
                            country, crawl_time, updatetime, update_count, create_time
                        ) VALUES %s
                        ON CONFLICT (raw_text)
                        DO NOTHING
                    """
                    for i in range(0, len(batch_data), batch_size):
                        batch = batch_data[i:i + batch_size]
                        
                        try:
                            execute_values(
                                cursor,
                                insert_sql,
                                batch,
                                template=None,
                                page_size=batch_size  )
                            self.conn.commit()
                            success_count += len(batch)
                        except Exception as e:
                            self.conn.rollback()
                            for record in batch:
                                try:
                                    cursor.execute(f"""
                                        INSERT INTO {target_table} (
                                        raw_text, corrected, auto, station_id, observation_time,
                                        latitude, longitude, temp, dewpoint,
                                        weather, sky, wind_direction_variation, wind_dir_degrees,
                                        wind_speed, wind_gust, peak_wind_drct, peak_wind_gust,
                                        visibility_statute, altimeter_hpa, sea_level_pressure,
                                        pcp1h, pcp3h, pcp6h, pcp24h,
                                        maxT6h, minT6h, maxT24h, minT24h,
                                        metar_type, remarks, rvr,
                                        country, crawl_time, updatetime, update_count, create_time
                                    ) VALUES (
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s
                                    )
                                        ON CONFLICT (raw_text) DO NOTHING
                                    """, record)
                                    self.conn.commit()
                                    success_count += 1
                                except Exception:
                                    self.conn.rollback()
                        del batch
                        gc.collect()
                
                cursor.close()
                return success_count
                
            except Exception as e:
                self.conn.rollback()
                print(f"  ✗ METAR批量入库失败: {e}")
                try:
                    self._reconnect()
                except:
                    pass
                return success_count
    
    def insert_taf_batch(self, data_list: List[Dict], batch_size: int = 50) -> int:
        """
        批量插入TAF数据（性能优化版：使用execute_values，支持分表）
        
        Args:
            data_list: TAF数据字典列表
            batch_size: 每批插入的记录数（默认50，减小批次以节省内存）
        
        Returns:
            int: 成功插入的记录数
        """
        if not data_list:
            return 0
        
        success_count = 0
        
        with self.lock:
            try:
                cursor = self.conn.cursor()
                prepared_list = []
                obs_times = []
                for data in data_list:
                    if not data or not data.get('station_code'):
                        continue
                    prepared = self._prepare_taf_data(data)
                    if prepared.get('raw_taf'):
                        prepared_list.append(prepared)
                        obs_times.append(prepared.get('observation_time'))
                table_indices_map = self._get_partition_table_names_vectorized(
                    self.base_taf_table, obs_times, 'taf'  )
                idx_to_table = {}
                for table_name, indices in table_indices_map.items():
                    for idx in indices:
                        idx_to_table[idx] = table_name
                raw_taf_map = {}  # raw_taf -> (prepared_data, is_corrected, target_table)
                
                for idx, prepared in enumerate(prepared_list):
                    raw_taf = prepared.get('raw_taf')
                    is_corrected = prepared.get('is_corrected', False)
                    target_table = idx_to_table.get(idx, self.base_taf_table)
                    if raw_taf in raw_taf_map:
                        existing_prepared, existing_corrected, existing_table = raw_taf_map[raw_taf]
                        if is_corrected and not existing_corrected:
                            raw_taf_map[raw_taf] = (prepared, is_corrected, target_table)
                    else:
                        raw_taf_map[raw_taf] = (prepared, is_corrected, target_table)
                table_data_map = {}  # target_table -> [(prepared_data, ...), ...]
                for prepared, _, target_table in raw_taf_map.values():
                    if target_table not in table_data_map:
                        table_data_map[target_table] = []
                    
                    table_data_map[target_table].append((
                        prepared['raw_taf'],
                        prepared['observation_time'],
                        prepared['station_id'],
                        prepared['latitude'],
                        prepared['longitude'],
                        prepared['country'],
                        prepared['is_corrected'],
                        prepared['valid_from'],
                        prepared['valid_to'],
                        prepared['wind_direction_deg'],
                        prepared['wind_speed_mps'],
                        prepared['visibility_m'],
                        prepared['weather_phenomena'],
                        prepared['cloud_cover'],
                        prepared['max_temp_c'],
                        prepared['min_temp_c'],
                        prepared['trends_descriptions'],
                        prepared['crawl_time'],
                        prepared['update_time'],
                        prepared['update_count'],
                        prepared['create_time']
                    ))
                
                if not table_data_map:
                    cursor.close()
                    return 0
                for target_table, batch_data in table_data_map.items():
                    if target_table != self.base_taf_table:
                        self._create_partition_table_if_not_exists(target_table, self.base_taf_table, 'taf')
                    insert_sql = f"""
                        INSERT INTO {target_table} (
                        raw_taf, observation_time, station_id,
                        latitude, longitude, country,
                        is_corrected, valid_from, valid_to,
                        wind_direction_deg, wind_speed_mps,
                        visibility_m, weather_phenomena, cloud_cover,
                        max_temp_c, min_temp_c, trends_descriptions,
                            crawl_time, update_time, update_count, create_time
                        ) VALUES %s
                        ON CONFLICT (raw_taf)
                        DO NOTHING
                    """
                    for i in range(0, len(batch_data), batch_size):
                        batch = batch_data[i:i + batch_size]
                        
                        try:
                            execute_values(
                                cursor,
                                insert_sql,
                                batch,
                                template=None,
                                page_size=batch_size
                            )
                            self.conn.commit()
                            success_count += len(batch)
                        except Exception as e:
                            self.conn.rollback()
                            for record in batch:
                                try:
                                    cursor.execute(f"""
                                        INSERT INTO {target_table} (
                                        raw_taf, observation_time, station_id,
                                        latitude, longitude, country,
                                        is_corrected, valid_from, valid_to,
                                        wind_direction_deg, wind_speed_mps,
                                        visibility_m, weather_phenomena, cloud_cover,
                                        max_temp_c, min_temp_c, trends_descriptions,
                                        crawl_time, update_time, update_count, create_time
                                    ) VALUES (
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                    )
                                        ON CONFLICT (raw_taf) DO NOTHING
                                    """, record)
                                    self.conn.commit()
                                    success_count += 1
                                except Exception:
                                    self.conn.rollback()
                        del batch
                        gc.collect()
                
                cursor.close()
                return success_count
                
            except Exception as e:
                self.conn.rollback()
                print(f"  ✗ TAF批量入库失败: {e}")
                # 尝试重连
                try:
                    self._reconnect()
                except:
                    pass
                return success_count
    
    def _prepare_metar_data(self, data: Dict) -> Dict:
        """
        准备METAR数据，进行字段映射（按照入库机场字段说明）
        
        字段映射规则：
        1. 保留所有本地字段
        2. 如果含义相同，使用说明文件中的字段名和类型
        3. 只入库公制单位（altimeter_hpa而不是altim_in_hg）
        """
        wind_direction_variation = data.get('wind_direction_variation')
        if wind_direction_variation and isinstance(wind_direction_variation, str):
            pass 
        elif wind_direction_variation:
            wind_direction_variation = str(wind_direction_variation)
        else:
            wind_direction_variation = None
        sky = data.get('sky_condition')
        if sky and isinstance(sky, str):
            if sky.strip() == '':
                sky = None
        elif sky:
            sky = json.dumps(sky, ensure_ascii=False)
        else:
            sky = None
        rvr = data.get('rvr')
        if rvr and isinstance(rvr, list):
            rvr = json.dumps(rvr, ensure_ascii=False)
        elif rvr and isinstance(rvr, str):
            if rvr.strip() == '':
                rvr = None
        else:
            rvr = None
        current_time = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        updatetime = current_time
        crawl_time = data.get('crawl_time')
        if not crawl_time or (isinstance(crawl_time, str) and crawl_time.strip() == ''):
            crawl_time = current_time
        create_time = current_time
        return {
            'raw_text': data.get('raw_metar'),
            'corrected': data.get('is_corrected', False),
            'auto': data.get('is_auto', False),
            'station_id': data.get('station_code'),
            'observation_time': self._handle_timestamp_value(data.get('observation_time')),
            'latitude': self._handle_missing_value(data.get('latitude'), 'numeric'),
            'longitude': self._handle_missing_value(data.get('longitude'), 'numeric'),
            'temp': self._handle_missing_value(data.get('temperature_c'), 'numeric'),
            'dewpoint': self._handle_missing_value(data.get('dewpoint_c'), 'numeric'),
            'weather': self._handle_missing_value(data.get('weather'), 'text'),
            'sky': sky,
            'wind_direction_variation': self._handle_missing_value(wind_direction_variation, 'text'),
            'wind_dir_degrees': self._handle_missing_value(data.get('wind_direction_deg'), 'numeric'),
            'wind_speed': self._handle_missing_value(data.get('wind_speed_mps'), 'numeric'),
            'wind_gust': self._handle_missing_value(data.get('wind_gust_mps'), 'numeric'),
            'peak_wind_drct': self._handle_missing_value(data.get('peak_wind_direction_deg'), 'numeric'),
            'peak_wind_gust': self._handle_missing_value(data.get('peak_wind_speed_mps'), 'numeric'),
            'visibility_statute': self._handle_missing_value(data.get('visibility_m'), 'numeric'),
            'altimeter_hpa': self._handle_missing_value(data.get('altimeter_hpa'), 'numeric'),
            'sea_level_pressure': self._handle_missing_value(data.get('sea_level_pressure_hpa'), 'numeric'),
            'pcp1h': self._handle_missing_value(data.get('precip_1hr_mm'), 'numeric'),
            'pcp3h': self._handle_missing_value(data.get('precip_3hr_mm'), 'numeric'),
            'pcp6h': self._handle_missing_value(data.get('precip_6hr_mm'), 'numeric'),
            'pcp24h': self._handle_missing_value(data.get('precip_24hr_mm'), 'numeric'),
            'maxT6h': self._handle_missing_value(data.get('max_temp_6hr_c'), 'numeric'),
            'minT6h': self._handle_missing_value(data.get('min_temp_6hr_c'), 'numeric'),
            'maxT24h': self._handle_missing_value(data.get('max_temp_24hr_c'), 'numeric'),
            'minT24h': self._handle_missing_value(data.get('min_temp_24hr_c'), 'numeric'),
            'metar_type': self._handle_missing_value(data.get('report_type'), 'text'),
            'remarks': self._handle_missing_value(data.get('remarks'), 'text'),
            'rvr': self._handle_missing_value(rvr, 'text'),
            'country': self._handle_missing_value(data.get('country'), 'text'),
            'crawl_time': self._handle_timestamp_value(crawl_time),
            'updatetime': self._handle_timestamp_value(updatetime),
            'update_count': 0,
            'create_time': self._handle_timestamp_value(create_time),
        }
    
    def _prepare_taf_data(self, data: Dict) -> Dict:
        """准备TAF数据（简化字段版本）"""
        # 获取当前UTC时间
        current_time = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        
        # update_time: 每次入库/更新时都用当前时间
        update_time = current_time
        crawl_time = data.get('crawl_time')
        if not crawl_time or (isinstance(crawl_time, str) and crawl_time.strip() == ''):
            crawl_time = current_time
        create_time = current_time
        return {
            'raw_taf': data.get('raw_taf'),
            'observation_time': self._handle_timestamp_value(data.get('observation_time') or data.get('timestamp_utc')),
            'station_id': data.get('station_code'),  # JSON中是station_code，映射到station_id
            'latitude': self._handle_missing_value(data.get('latitude'), 'numeric'),
            'longitude': self._handle_missing_value(data.get('longitude'), 'numeric'),
            'country': self._handle_missing_value(data.get('country'), 'text'),
            'is_corrected': data.get('is_corrected', False),
            'valid_from': self._handle_timestamp_value(data.get('valid_from')),
            'valid_to': self._handle_timestamp_value(data.get('valid_to')),
            'wind_direction_deg': self._handle_missing_value(data.get('wind_direction_deg'), 'numeric'),
            'wind_speed_mps': self._handle_missing_value(data.get('wind_speed_mps'), 'numeric'),
            'visibility_m': self._handle_missing_value(data.get('visibility_m'), 'numeric'),
            'weather_phenomena': self._handle_missing_value(data.get('weather_phenomena'), 'text'),
            'cloud_cover': self._handle_missing_value(data.get('cloud_cover'), 'text'),
            'max_temp_c': self._handle_missing_value(data.get('max_temp_c'), 'numeric'),
            'min_temp_c': self._handle_missing_value(data.get('min_temp_c'), 'numeric'),
            'trends_descriptions': self._handle_missing_value(data.get('trends_descriptions'), 'text'),
            'crawl_time': self._handle_timestamp_value(crawl_time),
            'update_time': self._handle_timestamp_value(update_time),
            'update_count': 0,
            'create_time': self._handle_timestamp_value(create_time),
        }
    
    def batch_insert_metar(self, data_list: List[Dict]) -> int:
        """批量插入METAR数据"""
        success_count = 0
        for data in data_list:
            if self.insert_metar(data):
                success_count += 1
        return success_count
    
    def batch_insert_taf(self, data_list: List[Dict]) -> int:
        """批量插入TAF数据"""
        success_count = 0
        for data in data_list:
            if self.insert_taf(data):
                success_count += 1
        return success_count
    
    def insert_synop(self, data: Dict) -> bool:
        """
        插入或更新SYNOP数据
        
        更新逻辑：
        - 唯一键：(station_id, observation_time)
        - 只有当新数据是修正报(raw_text包含COR)且原始文本不同时，才更新现有记录
        - 只对观测时间在最近两天内的记录进行比较和更新（历史数据不更新）
        - crawl_time保留首次值，update_count递增
        
        Args:
            data: SYNOP数据字典
        
        Returns:
            bool: 成功返回True，失败返回False
        """
        if not data:
            return False
        
        station_id = data.get('station_code', '') or data.get('station_id', '')
        if not station_id:
            return False
        with self.lock:
            try:
                cursor = self.conn.cursor()
                insert_data = self._prepare_synop_data(data)
                obs_time = insert_data.get('observation_time')
                target_table = self._get_partition_table_name(self.base_synop_table, obs_time, 'synop')
                if target_table != self.base_synop_table:
                    self._create_partition_table_if_not_exists(target_table, self.base_synop_table, 'synop')
                insert_sql = f"""
                    INSERT INTO {target_table} (
                        raw_text, station_id, observation_time, latitude, longitude, country,
                        report_type, station_name, altitude_m,
                        temp, dewpoint,
                        wind_indicator, wind_dir_degrees, wind_speed,
                        station_pressure, sea_level_pressure, pressure_tendency,
                        visibility_statute,
                        precipitation_mm, precipitation_hours,
                        present_weather_desc,
                        total_cloud_amount_oktas, low_cloud_type, mid_cloud_type, high_cloud_type,
                        crawl_time, updatetime, update_count, create_time
                    ) VALUES (
                        %(raw_text)s, %(station_id)s, %(observation_time)s, %(latitude)s, %(longitude)s, %(country)s,
                        %(report_type)s, %(station_name)s, %(altitude_m)s,
                        %(temp)s, %(dewpoint)s,
                        %(wind_indicator)s, %(wind_dir_degrees)s, %(wind_speed)s,
                        %(station_pressure)s, %(sea_level_pressure)s, %(pressure_tendency)s,
                        %(visibility_statute)s,
                        %(precipitation_mm)s, %(precipitation_hours)s,
                        %(present_weather_desc)s,
                        %(total_cloud_amount_oktas)s, %(low_cloud_type)s, %(mid_cloud_type)s, %(high_cloud_type)s,
                        %(crawl_time)s, %(updatetime)s, %(update_count)s, %(create_time)s
                    )
                    ON CONFLICT (station_id, observation_time)
                    DO UPDATE SET
                        raw_text = EXCLUDED.raw_text,
                        temp = EXCLUDED.temp,
                        dewpoint = EXCLUDED.dewpoint,
                        wind_dir_degrees = EXCLUDED.wind_dir_degrees,
                        wind_speed = EXCLUDED.wind_speed,
                        station_pressure = EXCLUDED.station_pressure,
                        sea_level_pressure = EXCLUDED.sea_level_pressure,
                        pressure_tendency = EXCLUDED.pressure_tendency,
                        visibility_statute = EXCLUDED.visibility_statute,
                        precipitation_mm = EXCLUDED.precipitation_mm,
                        precipitation_hours = EXCLUDED.precipitation_hours,
                        present_weather_desc = EXCLUDED.present_weather_desc,
                        total_cloud_amount_oktas = EXCLUDED.total_cloud_amount_oktas,
                        low_cloud_type = EXCLUDED.low_cloud_type,
                        mid_cloud_type = EXCLUDED.mid_cloud_type,
                        high_cloud_type = EXCLUDED.high_cloud_type,
                        crawl_time = COALESCE({target_table}.crawl_time, EXCLUDED.crawl_time),
                        updatetime = EXCLUDED.updatetime,
                        update_count = {target_table}.update_count + 1
                    WHERE {target_table}.raw_text != EXCLUDED.raw_text
                """
                
                cursor.execute(insert_sql, insert_data)
                self.conn.commit()
                cursor.close()
                return True
                
            except Exception as e:
                self.conn.rollback()
                print(f"  ✗ SYNOP入库失败: {e}")
                try:
                    self._reconnect()
                except:
                    pass
                return False
    
    def _prepare_synop_data(self, data: Dict) -> Dict:
        """准备SYNOP数据"""
        current_time = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        crawl_time = data.get('first_crawl_time') or data.get('crawl_time')
        if not crawl_time or (isinstance(crawl_time, str) and crawl_time.strip() == ''):
            crawl_time = current_time
        return {
            'raw_text': data.get('raw_synop', '') or data.get('synop_text', ''),
            'station_id': data.get('station_code', '') or data.get('station_id', ''),
            'observation_time': self._handle_timestamp_value(data.get('observation_time')),
            'latitude': self._handle_missing_value(data.get('latitude'), 'numeric'),
            'longitude': self._handle_missing_value(data.get('longitude'), 'numeric'),
            'country': data.get('country') or None,
            'report_type': data.get('report_type') or None,
            'station_name': data.get('station_name') or None,
            'altitude_m': self._handle_missing_value(data.get('altitude_m'), 'integer'),
            'temp': self._handle_missing_value(data.get('temperature_c'), 'numeric'),
            'dewpoint': self._handle_missing_value(data.get('dewpoint_c'), 'numeric'),
            'wind_indicator': data.get('wind_indicator') or None,
            'wind_dir_degrees': self._handle_missing_value(data.get('wind_direction_deg'), 'numeric'),
            'wind_speed': self._handle_missing_value(data.get('wind_speed_mps'), 'numeric'),
            'station_pressure': self._handle_missing_value(data.get('station_pressure_hpa'), 'numeric'),
            'sea_level_pressure': self._handle_missing_value(data.get('sea_level_pressure_hpa'), 'numeric'),
            'pressure_tendency': self._handle_missing_value(data.get('pressure_tendency_hpa'), 'numeric'),
            'visibility_statute': self._handle_missing_value(data.get('visibility_m'), 'numeric'),
            'precipitation_mm': self._handle_missing_value(data.get('precipitation_mm'), 'numeric'),
            'precipitation_hours': self._handle_missing_value(data.get('precipitation_hours'), 'integer'),
            'present_weather_desc': data.get('present_weather_desc') or None,
            'total_cloud_amount_oktas': self._handle_missing_value(data.get('total_cloud_amount_oktas'), 'integer'),
            'low_cloud_type': data.get('low_cloud_type') or None,
            'mid_cloud_type': data.get('mid_cloud_type') or None,
            'high_cloud_type': data.get('high_cloud_type') or None,
            'crawl_time': self._handle_timestamp_value(crawl_time),
            'updatetime': current_time,
            'update_count': 0,
            'create_time': current_time,
        }
    
    def batch_insert_synop(self, data_list: List[Dict]) -> int:
        """批量插入SYNOP数据（逐条插入，支持更新逻辑）"""
        success_count = 0
        for data in data_list:
            if self.insert_synop(data):
                success_count += 1
        return success_count
    
    def insert_synop_batch(self, data_list: List[Dict], batch_size: int = 50) -> int:
        """
        批量插入SYNOP数据（性能优化版：使用execute_values + ON CONFLICT处理，支持分表）
        
        Args:
            data_list: SYNOP记录列表
            batch_size: 批次大小
        
        Returns:
            成功插入或更新的记录数
        """
        if not data_list:
            return 0
        
        success_count = 0
        with self.lock:
            cursor = self.conn.cursor()
            try:
                obs_times = [self._handle_timestamp_value(r.get('observation_time')) for r in data_list]
                table_indices_map = self._get_partition_table_names_vectorized(
                    self.base_synop_table, obs_times, 'synop'
                )
                table_data_map = {}  # target_table -> [(record_tuple, ...), ...]
                
                for target_table, indices in table_indices_map.items():
                    if target_table not in table_data_map:
                        table_data_map[target_table] = []
                    for idx in indices:
                        record = data_list[idx]
                        raw_text = record.get('raw_synop', '') or record.get('synop_text', '')
                        station_id = record.get('station_code', '') or record.get('station_id', '')
                        observation_time = obs_times[idx]
                        latitude = self._handle_missing_value(record.get('latitude'), 'numeric')
                        longitude = self._handle_missing_value(record.get('longitude'), 'numeric')
                        country = record.get('country') or None   
                        report_type = record.get('report_type') or None
                        station_name = record.get('station_name') or None
                        altitude_m = self._handle_missing_value(record.get('altitude_m'), 'integer')
                        temp = self._handle_missing_value(record.get('temperature_c'), 'numeric')
                        dewpoint = self._handle_missing_value(record.get('dewpoint_c'), 'numeric')
                        wind_indicator = record.get('wind_indicator') or None
                        wind_dir_degrees = self._handle_missing_value(record.get('wind_direction_deg'), 'numeric')
                        wind_speed = self._handle_missing_value(record.get('wind_speed_mps'), 'numeric')
                        station_pressure = self._handle_missing_value(record.get('station_pressure_hpa'), 'numeric')
                        sea_level_pressure = self._handle_missing_value(record.get('sea_level_pressure_hpa'), 'numeric')
                        pressure_tendency = self._handle_missing_value(record.get('pressure_tendency_hpa'), 'numeric')
                        
                        visibility_statute = self._handle_missing_value(record.get('visibility_m'), 'numeric')
                        
                        precipitation_mm = self._handle_missing_value(record.get('precipitation_mm'), 'numeric')
                        precipitation_hours = self._handle_missing_value(record.get('precipitation_hours'), 'integer')
                        
                        present_weather_desc = record.get('present_weather_desc') or None
                        
                        total_cloud_amount_oktas = self._handle_missing_value(record.get('total_cloud_amount_oktas'), 'integer')
                        low_cloud_type = record.get('low_cloud_type') or None
                        mid_cloud_type = record.get('mid_cloud_type') or None
                        high_cloud_type = record.get('high_cloud_type') or None
                        
                        crawl_time = self._handle_timestamp_value(record.get('first_crawl_time') or record.get('crawl_time'))
                        update_count = self._handle_missing_value(record.get('update_count', 0), 'integer')
                        now_time = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                        updatetime = self._handle_timestamp_value(record.get('update_time')) or now_time
                        create_time = self._handle_timestamp_value(record.get('create_time')) or now_time
                        table_data_map[target_table].append((
                            raw_text, station_id, observation_time, latitude, longitude, country,
                            report_type, station_name, altitude_m,
                            temp, dewpoint,
                            wind_indicator, wind_dir_degrees, wind_speed,
                            station_pressure, sea_level_pressure, pressure_tendency,
                            visibility_statute,
                            precipitation_mm, precipitation_hours,
                            present_weather_desc,
                            total_cloud_amount_oktas, low_cloud_type, mid_cloud_type, high_cloud_type,
                            crawl_time, updatetime, update_count, create_time
                        ))
                for target_table, values in table_data_map.items():
                    if target_table != self.base_synop_table:
                        self._create_partition_table_if_not_exists(target_table, self.base_synop_table, 'synop')
                    if not values:
                        continue
                    for i in range(0, len(values), batch_size):
                        batch = values[i:i + batch_size]
                        insert_sql = f"""
                            INSERT INTO {target_table} (
                                raw_text, station_id, observation_time, latitude, longitude, country,
                                report_type, station_name, altitude_m,
                                temp, dewpoint,
                                wind_indicator, wind_dir_degrees, wind_speed,
                                station_pressure, sea_level_pressure, pressure_tendency,
                                visibility_statute,
                                precipitation_mm, precipitation_hours,
                                present_weather_desc,
                                total_cloud_amount_oktas, low_cloud_type, mid_cloud_type, high_cloud_type,
                                crawl_time, updatetime, update_count, create_time
                            ) VALUES %s
                            ON CONFLICT (raw_text)
                            DO NOTHING
                        """
                        
                        try:
                            extras.execute_values(
                                cursor,
                                insert_sql,
                                batch,
                                template=None,
                                page_size=batch_size
                            )
                            self.conn.commit()
                            success_count += len(batch)
                        except Exception:
                            self.conn.rollback()
                            for record in batch:
                                try:
                                    cursor.execute(f"""
                                        INSERT INTO {target_table} (
                                            raw_text, station_id, observation_time, latitude, longitude, country,
                                            report_type, station_name, altitude_m,
                                            temp, dewpoint,
                                            wind_indicator, wind_dir_degrees, wind_speed,
                                            station_pressure, sea_level_pressure, pressure_tendency,
                                            visibility_statute,
                                            precipitation_mm, precipitation_hours,
                                            present_weather_desc,
                                            total_cloud_amount_oktas, low_cloud_type, mid_cloud_type, high_cloud_type,
                                            crawl_time, updatetime, update_count, create_time
                                        ) VALUES (
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s
                                        )
                                        ON CONFLICT (raw_text) DO NOTHING
                                    """, record)
                                    self.conn.commit()
                                    success_count += 1
                                except Exception:
                                    self.conn.rollback()
                        del batch
                        gc.collect()

            except Exception as e:
                self.conn.rollback()
                print(f"✗ SYNOP批量插入失败: {e}")
                import traceback
                traceback.print_exc()
                return success_count
            finally:
                cursor.close()
                gc.collect()
        
        return success_count

    def get_existing_synop_raw_texts(self, station_id: str, days: int = 7) -> set:
        """
        查询某站点最近N天的已存在 raw_text 集合
        用于在爬虫端跳过已存在的报文，节省解码开销
        """
        if not self.conn:
            return set()
        
        try:
            cursor = self.conn.cursor()
            query = f"""
                SELECT raw_text FROM {self.synop_table}
                WHERE station_id = %s 
                  AND observation_time >= NOW() - INTERVAL '{days} days'
            """
            cursor.execute(query, (station_id,))
            rows = cursor.fetchall()
            cursor.close()
            return {row[0] for row in rows if row[0]}
        except Exception as e:
            return set()


