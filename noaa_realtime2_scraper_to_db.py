# -*- coding: utf-8 -*-
import requests
from requests.adapters import Retry
import pandas as pd
from datetime import datetime, timedelta, timezone
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import psycopg2
from psycopg2.extras import execute_values
import logging
import gc  # 垃圾回收
import json
import os
try:
    from db_config import DB_CONFIG
except ImportError:
    print("警告: 未找到 db_config.py，使用默认配置")
    import os
    # DB_CONFIG = {
    #     'host': os.getenv('DB_HOST', 'database-1.cxik82g8267p.us-east-2.rds.amazonaws.com'),
    #     'port': int(os.getenv('DB_PORT', '5432')),
    #     'database': 'noaa_data',
    #     'user': os.getenv('DB_USER', 'postgres'),
    #     'password': os.getenv('DB_PASSWORD', 'oceantest1')
    # }
    DB_CONFIG = {
        'host': '127.0.0.1',  # 本地
        'port': 5432,
        'database': 'noaa_data',  # 库名
        'user': 'postgres',  # 默认用户名
        'password': '123456'  # 密码
    }
class NOAARealtimeToDatabase:
    # REAL 类型字段（统一使用 REAL 类型，包括 WDIR 和 MWD）
    REAL_COLUMNS = [
        'WDIR', 'WSPD', 'GST', 'WVHT', 'DPD', 'APD', 'MWD',
        'PRES', 'ATMP', 'WTMP', 'DEWP', 'VIS', 'PTDY', 'TIDE',
        'WSPD10M', 'WSPD20M'
    ]
    
    def __init__(self, db_config=None, enable_partitioning=True):
        """初始化爬虫（性能优化版）"""
        self.base_url = "https://www.ndbc.noaa.gov/data/realtime2/"
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36']
        self.headers = {
            'User-Agent': self.user_agents[0],  
            'Accept': 'text/plain, text/html, application/xhtml+xml, */*',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': 'https://www.ndbc.noaa.gov/data/realtime2/',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Upgrade-Insecure-Requests': '1'  }
        # 数据库配置
        self.db_config = db_config or DB_CONFIG
        self.enable_partitioning = enable_partitioning
        self.partition_start_date = datetime(2025, 1, 1, 0, 0, 0)
        self.base_txt_table = 'txt_data'

        self.max_workers = 3  # 降低并发（5→3），缓解RDS连接压力
        self.batch_size = 500   # 降低批量大小（1500→500），减少单次事务压力
        self.request_timeout = 30 
        self.max_retries = 4 
        self.update_interval = 2 * 60 
        self.log_every_n_files = 100  
        self.request_delay = 1.0  # 增加延迟（0.5→1.0秒），更礼貌的爬取  
        
        # 内存管理配置
        self.memory_cleanup_interval = 20 
        self.connection_refresh_interval = 40 
        self.batch_cleanup_interval = 2  
        self.gc_enabled = True 
        
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # 大幅增加连接池大小（支持高并发）+ 自动重试
        retry_strategy = Retry(
            total=3, 
            backoff_factor=1, 
            status_forcelist=[429, 500, 502, 503, 504], 
            allowed_methods=["GET", "POST"] 
        )
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=12, 
            pool_maxsize=200,     
            max_retries=retry_strategy
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        
        # 线程锁（优化：添加站点锁）
        self.stats_lock = threading.Lock()
        self.stations_lock = threading.Lock()
        self.partition_lock = threading.Lock()  
        
        # 站点缓存（优化：避免重复查询）
        self.existing_stations = set()
        
        # 分表缓存（记录已创建或已确认存在的分表）
        self.created_partitions = set()
        
        # 站点补充数据缓存（存储WSPD10M/WSPD20M等额外字段）
        self.station_wspd_cache = {}  # {station_id: {'WSPD10M': value, 'WSPD20M': value}}
        self.wspd_cache_lock = threading.Lock()
        
        # 缓存文件配置
        self.wspd_cache_file = 'wspd_supplement_cache.json'
        self.wspd_cache_expiry = 60 * 60  # 缓存过期时间：1小时（秒）
        self.wspd_cache_skip_sampling = True  # 跳过抽样验证，直接全量检查
        self.wspd_cache_check_values = True  # 检查WSPD数值变化（不仅检查时间）
        self.wspd_cache_incremental_update = True  # 增量更新：只更新变化的站点
        
        # 统计信息
        self.stats = {
            'total_files': 0,
            'successful_imports': 0,
            'failed_imports': 0,
            'processed_records': 0,
            'errors': []
        }
        
        # 测试模式
        self.test_mode = False
        self.test_stations = ['41001', '41056']
        
        # 设置日志
        self.setup_logging()
    
    def setup_logging(self):
        import os
        import sys
        script_dir = os.path.dirname(os.path.abspath(__file__))
        local_log_dir = os.path.join(script_dir, 'logs')
        if sys.platform == 'win32':
            log_dir = local_log_dir
        else:
            log_dir = '/logs' if os.path.exists('/logs') and os.path.isdir('/logs') else local_log_dir
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'noaa_txt.log')
        self.logger = logging.getLogger('noaa_txt')
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            class FlushFileHandler(logging.FileHandler):
                def emit(self, record):
                    super().emit(record)
                    self.flush()
            
            file_handler = FlushFileHandler(log_file, encoding='utf-8')
            file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
            self.logger.addHandler(file_handler)
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
            self.logger.addHandler(console_handler)
    
    def connect_db(self, retry_count=3):
        """连接数据库（通过内存管理解决OOM，恢复较高配置 + 重试机制 + 编码错误处理）"""
        last_error = None
        for attempt in range(retry_count):
            try:
                conn = psycopg2.connect(
                    host=self.db_config['host'],
                    port=self.db_config['port'],
                    database=self.db_config['database'],
                    user=self.db_config['user'],
                    password=self.db_config['password'],
                    connect_timeout=10,  
                    application_name='noaa_scraper',
                    options='-c statement_timeout=300000', 
                    client_encoding='UTF8'
                )
                conn.autocommit = True  # 启用 autocommit 避免 "set_session cannot be used inside a transaction" 错误
                # 检查连接是否有效
                try:
                    with conn.cursor() as test_cursor:
                        test_cursor.execute("SELECT 1")
                        test_cursor.fetchone()
                except Exception:
                    conn.close()
                    raise psycopg2.OperationalError("连接测试失败")
                
                # 设置连接参数（在 autocommit 模式下执行 SET 命令）
                
                # 恢复较高配置，通过定期内存清理解决OOM问题
                # 使用try-except包装每个SET命令，避免单个命令失败导致整个连接失败
                cursor = None
                try:
                    cursor = conn.cursor()
                    try:
                        cursor.execute("SET timezone = 'UTC'")  # 设置会话时区为UTC（重要！）
                    except Exception as e:
                        self.logger.warning(f"设置 timezone 失败: {e}")
                    
                    try:
                        cursor.execute("SET work_mem = '32MB'")  # 恢复较高配置
                    except Exception as e:
                        self.logger.warning(f"设置 work_mem 失败: {e}")
                    
                    try:
                        cursor.execute("SET maintenance_work_mem = '128MB'")  # 恢复较高配置
                    except Exception as e:
                        self.logger.warning(f"设置 maintenance_work_mem 失败: {e}")
                    
                    try:
                        cursor.execute("SET synchronous_commit = 'off'")  # 异步提交（提升写入性能）
                    except Exception as e:
                        self.logger.warning(f"设置 synchronous_commit 失败: {e}")
                    
                    try:
                        cursor.execute("SET temp_buffers = '16MB'")  # 适当限制
                    except Exception as e:
                        self.logger.warning(f"设置 temp_buffers 失败: {e}")
                finally:
                    if cursor:
                        cursor.close()
                # 设置回非 autocommit 模式，用于后续的事务操作
                conn.autocommit = False
                
                return conn
            
            except UnicodeDecodeError as e:
                last_error = e
                self.logger.error(f"❌ 数据库连接编码错误 (第{attempt+1}/{retry_count}次): {e}")
                self.logger.error(f"   编码错误详情: 位置 {e.start}-{e.end}, 问题字节: {e.object[e.start:min(e.end+10, len(e.object))].hex()}")
                
                if attempt < retry_count - 1:
                    wait_time = min(2 ** (attempt + 2), 30)
                    self.logger.warning(f"    等待 {wait_time} 秒后重试（可能是数据库临时繁忙或编码配置问题）...")
                    time.sleep(wait_time)
                    continue
                else:
                    self.logger.error(f"   ❌ 编码错误重试{retry_count}次后仍然失败")
                    self.logger.error(f"    建议检查：1) PostgreSQL编码配置 2) 数据库错误日志 3) 服务器状态")
                    raise
            
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                last_error = e
                if attempt < retry_count - 1:
                    wait_time = min(2 ** attempt, 5)  # 指数退避，最多5秒
                    self.logger.warning(f"数据库连接失败 (第{attempt+1}次)，{wait_time}秒后重试: {e}")
                    time.sleep(wait_time)
                    continue
                else:
                    self.logger.error(f"✗ 数据库连接失败 (已重试{retry_count}次): {e}")
                    raise
            except Exception as e:
                last_error = e
                self.logger.error(f"✗ 数据库连接失败: {e}")
                raise
        if last_error:
            raise last_error
    
    def cleanup_memory(self):
        """清理内存：释放DataFrame、执行垃圾回收（增强版）"""
        try:
            # 清理分表缓存（保留最近的50个）
            if len(self.created_partitions) > 100:
                with self.partition_lock:
                    partition_list = list(self.created_partitions)
                    self.created_partitions = set(partition_list[-50:])
            if self.gc_enabled:
                collected = gc.collect(0) + gc.collect(1) + gc.collect(2)
                if collected > 0:
                    self.logger.debug(f"内存清理: 回收了 {collected} 个对象，分表缓存: {len(self.created_partitions)} 个")
        except Exception as e:
            self.logger.warning(f"内存清理失败: {e}")
    
    def refresh_connection(self, conn):
        """刷新连接：关闭旧连接，创建新连接"""
        try:
            if conn:
                conn.close()
        except Exception:
            pass
        return self.connect_db()
    
    def _get_partition_table_names_vectorized(self, base_table, observation_times):
        """向量化计算分表名称（性能优化：批量处理，十天一分区）"""
        if not self.enable_partitioning or observation_times.empty:
            return pd.Series([base_table] * len(observation_times), index=observation_times.index)
        try:
            times = pd.to_datetime(observation_times, errors='coerce')
            base_reference = pd.Timestamp(self.partition_start_date.date())
            days_from_ref = (times - base_reference).dt.days
            partition_num = days_from_ref // 10
            start_days = partition_num * 10
            end_days = start_days + 9
            start_dates = base_reference + pd.to_timedelta(start_days, unit='D')
            end_dates = base_reference + pd.to_timedelta(end_days, unit='D')
            suffixes = start_dates.dt.strftime('%Y%m%d') + '_' + end_dates.dt.strftime('%Y%m%d')
            result = base_table + '_' + suffixes
            result[times < self.partition_start_date] = base_table
            result[times.isna()] = base_table
            return result
        except Exception as e:
            self.logger.warning(f"向量化计算分表名失败: {e}，使用原表")
            return pd.Series([base_table] * len(observation_times), index=observation_times.index)
    
    def _create_partition_table_if_not_exists(self, conn, partition_table, base_table):
        """
        如果分表不存在，则创建分表（线程安全，并发冲突容错）
        
        并发安全策略：
        1. 使用内存缓存快速跳过已创建的分表
        2. 使用线程锁防止并发创建同一个分表
        3. 使用 CREATE TABLE IF NOT EXISTS - 数据库级别的并发控制
        4. 捕获所有"已存在"类型的错误
        5. 只记录警告不抛出 - 避免阻塞数据插入流程
        
        Args:
            conn: 数据库连接
            partition_table: 分表名称
            base_table: 基表名称
        """
        if partition_table == base_table:
            return
        if partition_table in self.created_partitions:
            return
        
        # 使用锁防止多线程并发创建同一个分表
        with self.partition_lock:
            # 双重检查
            if partition_table in self.created_partitions:
                return
            cursor = None
            try:
                cursor = conn.cursor()
                try:
                    cursor.execute(f"""
                        CREATE TABLE IF NOT EXISTS {partition_table} (
                            LIKE {base_table} INCLUDING ALL
                        )
                    """)
                    conn.commit()
                    self.logger.info(f"✓ 创建分表: {partition_table}")
                    self.created_partitions.add(partition_table)
                except psycopg2.errors.DuplicateTable:
                    conn.rollback()
                    self.created_partitions.add(partition_table)
                    self.logger.debug(f"分表已存在: {partition_table}")
                except Exception as e:
                    error_msg = str(e).lower()
                    conn.rollback()
                    is_exists_error = any(keyword in error_msg for keyword in [
                        'already exists',
                        'duplicate',
                        'pg_type_typname_nsp_index',
                        'unique constraint',
                        'relation'
                    ])
                    
                    if is_exists_error:
                        self.created_partitions.add(partition_table)
                        self.logger.debug(f"分表已存在（并发冲突）: {partition_table}")
                    else:
                        self.logger.warning(f"创建分表异常 {partition_table}: {e}")
            
            except Exception as e:
                if conn:
                    try:
                        conn.rollback()
                    except:
                        pass
                self.logger.warning(f"创建分表失败 {partition_table}: {e}")
            finally:
                if cursor:
                    try:
                        if not cursor.closed:
                            cursor.close()
                    except:
                        pass
    
    def load_existing_partitions(self, conn):
        """启动时预加载已存在的分表到缓存（性能优化：避免重复检查）"""
        cursor = None
        try:
            cursor = conn.cursor()
            # 查询所有以 txt_data 开头的分表
            cursor.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name LIKE 'txt_data_%%'
            """)
            
            rows = cursor.fetchall()
            partition_names = {row[0] for row in rows}
            
            with self.partition_lock:
                self.created_partitions = partition_names
            
            self.logger.info(f"✓ 已预加载 {len(partition_names)} 个分表到缓存")
        except Exception as e:
            self.logger.warning(f"预加载分表列表失败（不影响功能）: {e}")
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
    
    def load_existing_stations(self, conn):
        """加载已存在的站点列表（优化：避免重复查询 + 编码处理）"""
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM stations")
            rows = cursor.fetchall()
            
            # 安全处理编码，避免UTF-8解码错误
            station_names = set()
            for row in rows:
                try:
                    station_name = row[0]
                    # 如果是bytes，尝试解码
                    if isinstance(station_name, bytes):
                        station_name = station_name.decode('utf-8', errors='replace')
                    # 如果是字符串，确保是有效的UTF-8
                    elif isinstance(station_name, str):
                        # 验证字符串是否有效
                        station_name.encode('utf-8').decode('utf-8')
                    station_names.add(station_name)
                except (UnicodeDecodeError, UnicodeEncodeError) as e:
                    self.logger.warning(f"站点名称编码错误，跳过: {row[0]}, 错误: {e}")
                    continue
            
            with self.stations_lock:
                self.existing_stations = station_names
            cursor.close()
            self.logger.info(f"✓ 已加载 {len(self.existing_stations)} 个站点信息")
        except Exception as e:
            self.logger.error(f"✗ 加载站点列表失败: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            with self.stations_lock:
                self.existing_stations = set()
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
    
    def fetch_txt_file_links(self):
        try:
            self.logger.info("正在获取.txt文件链接...")
            
            # 添加时间戳参数防止HTTP缓存
            cache_buster = int(time.time() * 1000)
            url_with_cache_buster = f"{self.base_url}?_={cache_buster}"
            
            response = self.session.get(url_with_cache_buster, timeout=self.request_timeout)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            links = soup.find_all('a', href=True)
            
            txt_files = []
            for link in links:
                href = link.get('href')
                if href and href.endswith('.txt'):
                    full_url = urljoin(self.base_url, href)
                    station_id = href.replace('.txt', '')
                    
                    # 测试模式过滤
                    if self.test_mode and station_id not in self.test_stations:
                        continue
                    
                    file_info = {
                        'filename': href,
                        'url': full_url,
                        'station_id': station_id
                    }
                    txt_files.append(file_info)
            
            self.logger.info(f"找到 {len(txt_files)} 个.txt文件")
            return txt_files
            
        except Exception as e:
            self.logger.error(f"获取文件链接失败: {e}")
            return []
    
    def read_and_parse_file_online(self, file_info):
        """读取并解析在线文件（带重试机制 + 缓存破坏）"""
        last_error = None
        
        # 添加请求延迟，避免过快请求触发服务器限流
        time.sleep(self.request_delay)
        
        # 手动重试逻辑（HTTP适配器层会重试，但应用层也需要重试服务器错误）
        for attempt in range(self.max_retries):
            try:
                # 随机选择User-Agent（模拟不同浏览器）
                import random
                self.session.headers['User-Agent'] = random.choice(self.user_agents)
                
                # 添加时间戳参数防止HTTP缓存（cache buster pattern）
                cache_buster = int(time.time() * 1000)  # 毫秒时间戳
                url_with_cache_buster = f"{file_info['url']}?_={cache_buster}"
                
                response = self.session.get(url_with_cache_buster, timeout=self.request_timeout)
                response.raise_for_status()
                
                # 检查Content-Type，如果返回HTML说明文件不存在或服务器错误
                content_type = response.headers.get('Content-Type', '').lower()
                if 'text/html' in content_type or response.text.strip().startswith('<!DOCTYPE') or response.text.strip().startswith('<html'):
                    # 服务器返回HTML错误页面而不是txt数据（状态码200但实际是错误）
                    last_error = f"服务器返回HTML页面而不是数据文件（Content-Type: {content_type}），文件可能不存在或暂时不可用"
                    self.logger.debug(f"{file_info['filename']}: {last_error}")
                    # 这种情况不需要重试，直接跳过
                    break
                
                # 解析数据（返回 DataFrame 和详细错误信息）
                df, parse_error = self.parse_txt_content(response.text, file_info)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
                
                if df is not None and not df.empty:
                    return True, df, None
                else:
                    # 返回详细的解析错误信息
                    return False, None, parse_error or "数据解析失败或数据为空"
                    
            except requests.exceptions.Timeout as e:
                last_error = f"超时 (第{attempt+1}次): {e}"
                if attempt < self.max_retries - 1:
                    wait_time = min(2 ** attempt, 10)  # 指数退避，最多10秒
                    time.sleep(wait_time)
                    continue
            except requests.exceptions.HTTPError as e:
                # HTTP错误（500, 502, 503, 504等）- 服务器错误应该重试
                status_code = e.response.status_code if hasattr(e, 'response') and e.response else 'unknown'
                last_error = f"HTTP错误 {status_code} (第{attempt+1}次): {e}"
                if status_code in [429, 500, 502, 503, 504] and attempt < self.max_retries - 1:
                    # 服务器错误，等待后重试（指数退避）
                    wait_time = min(2 ** attempt, 10)  # 指数退避，最多10秒
                    time.sleep(wait_time)
                    continue
                else:
                    # 客户端错误（4xx）或其他错误，不重试
                    break
            except requests.exceptions.RequestException as e:
                # 其他网络错误（连接错误等）- 应该重试
                last_error = f"网络错误 (第{attempt+1}次): {e}"
                if attempt < self.max_retries - 1:
                    wait_time = min(2 ** attempt, 10)  # 指数退避，最多10秒
                    time.sleep(wait_time)
                    continue
            except Exception as e:
                # 其他未知错误（解析错误等）- 不重试
                last_error = f"读取失败: {e}"
                import traceback
                self.logger.error(f"文件 {file_info['filename']} 读取异常:\n{traceback.format_exc()}")
                break
        
        return False, None, last_error
    
    def extract_station_id(self, filename):
        """从文件名中提取站点ID"""
        # 去掉.txt后缀
        station_id = filename.replace('.txt', '')
        return station_id if station_id else None
    
    def parse_txt_content(self, raw_data, file_info):
        """解析TXT内容（性能优化版）- 返回 (DataFrame, error_message)"""
        if not raw_data or not raw_data.strip():
            return None, "文件内容为空"
        
        try:
            lines = raw_data.strip().split('\n')
            data_lines = []
            header_line = None
            
            # 记录前5行用于诊断
            first_lines = lines[:5]
            
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                if line.startswith('#'):
                    # 标准站点数据：以 #YY 开头
                    if line.startswith('#YY'):
                        header_line = line[1:].strip()  # 去掉#号
                    continue
                else:
                    data_lines.append(line)
            
            if not data_lines:
                error_msg = f"没有找到数据行，文件前5行内容:\n" + "\n".join(f"  {i+1}: {line}" for i, line in enumerate(first_lines))
                self.logger.error(f"文件 {file_info['filename']} 解析失败: {error_msg}")
                return None, error_msg
            
            if not header_line:
                error_msg = f"没有找到有效的列名行（需要 #YY 开头），文件前5行内容:\n" + "\n".join(f"  {i+1}: {line}" for i, line in enumerate(first_lines))
                self.logger.error(f"文件 {file_info['filename']} 解析失败: {error_msg}")
                return None, error_msg
            columns = header_line.split()
            data_rows = []
            for line in data_lines:
                row_data = line.split()
                if len(row_data) >= len(columns):
                    data_rows.append(row_data[:len(columns)])
                elif len(row_data) > 0:
                    row_data.extend([''] * (len(columns) - len(row_data)))
                    data_rows.append(row_data)
            if not data_rows:
                error_msg = f"解析后没有有效数据行"
                self.logger.error(f"文件 {file_info['filename']} 解析失败: {error_msg}")
                return None, error_msg
            df = pd.DataFrame(data_rows, columns=columns)
            df['station_name'] = file_info['station_id']
            
            # 优化：先清理数据，再批量处理时间
            df = self.clean_data(df)
            
            # 检查是否有必需的时间列
            required_time_cols = ['YY', 'MM', 'DD', 'hh', 'mm']
            missing_cols = [col for col in required_time_cols if col not in df.columns]
            if missing_cols:
                error_msg = f"缺少必需的时间列: {missing_cols}，当前可用列: {list(df.columns)}"
                self.logger.error(f"文件 {file_info['filename']} 解析失败: {error_msg}")
                return None, error_msg
            
            # 优化：使用向量化操作生成时间（性能提升10-15倍）
            observation_times = self.create_observation_time_vectorized(df)
            if observation_times is None:
                error_msg = f"时间转换函数返回None，前3行时间数据: {df[required_time_cols].head(3).to_dict('records')}"
                self.logger.error(f"文件 {file_info['filename']} 解析失败: {error_msg}")
                return None, error_msg
            
            df['observation_time'] = observation_times
            
            # 检查有多少有效时间
            valid_times = df['observation_time'].notna().sum()
            total_rows = len(df)
            
            if valid_times == 0:
                error_msg = f"时间转换全部失败 (0/{total_rows})，前3行时间数据: {df[required_time_cols].head(3).to_dict('records')}"
                self.logger.error(f"文件 {file_info['filename']} 解析失败: {error_msg}")
                return None, error_msg
            
            # 过滤掉无效时间的行
            df = df[df['observation_time'].notna()]
            
            # 检查过滤后是否还有数据
            if df.empty:
                error_msg = f"过滤无效时间后没有有效数据（原始: {total_rows} 行，有效时间: {valid_times} 行）"
                self.logger.error(f"文件 {file_info['filename']} 解析失败: {error_msg}")
                return None, error_msg
            
            if valid_times < total_rows:
                self.logger.debug(f"文件 {file_info['filename']} 部分时间无效: {valid_times}/{total_rows} 有效")
            
            # 使用UTC时间（Python 3.12+ 推荐方式）
            current_time_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            df['first_crawl_time'] = current_time_utc
            df['update_time'] = current_time_utc
            df['create_time'] = current_time_utc  # 新增：明确设置入库时间
            df['update_count'] = 1
            
            # 添加WSPD10M和WSPD20M字段（从缓存中获取站点补充数据）
            station_id = file_info['station_id']
            with self.wspd_cache_lock:
                wspd_data = self.station_wspd_cache.get(station_id, {})
            
            # 初始化字段为-999（默认值）
            df['WSPD10M'] = -999.0
            df['WSPD20M'] = -999.0
            
            # 精确匹配时间批次（用户要求：只有时间完全一致才插入）
            if wspd_data and 'time' in wspd_data:
                # 获取站点详情页的时间批次
                wspd_time = wspd_data['time']
                
                # 在DataFrame中精确匹配这个时间批次
                is_exact_match = df['observation_time'] == wspd_time
                
                # 有匹配的记录才填充
                if is_exact_match.any():
                    df.loc[is_exact_match, 'WSPD10M'] = wspd_data.get('WSPD10M', -999.0)
                    df.loc[is_exact_match, 'WSPD20M'] = wspd_data.get('WSPD20M', -999.0)
                    filled_count = is_exact_match.sum()
                    self.logger.debug(
                        f"站点 {station_id}: 精确匹配时间 {wspd_time.strftime('%Y-%m-%d %H:%M')}, "
                        f"填充了 {filled_count} 条记录的WSPD补充数据"
                    )
                else:
                    self.logger.debug(
                        f"站点 {station_id}: 未找到匹配时间批次 {wspd_time.strftime('%Y-%m-%d %H:%M')}, "
                        f"realtime2最新时间: {df['observation_time'].max()}"
                    )
            
            return df, None  # 成功：返回 DataFrame 和 None 错误
            
        except Exception as e:
            error_msg = f"解析异常: {str(e)}"
            self.logger.error(f"文件 {file_info['filename']} 解析失败: {error_msg}")
            import traceback
            self.logger.error(traceback.format_exc())
            return None, error_msg
    
    def clean_data(self, df):
        """数据清理（性能优化版）"""
        replace_dict = {
            'MM': None, 'NAN': None, 'nan': None, '': None, 'None': None
        }
        df = df.replace(replace_dict)
        for col in self.REAL_COLUMNS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(-999)
        
        return df
    
    def create_observation_time_vectorized(self, df):
        """使用向量化操作生成 observation_time（优化：性能提升10-15倍）"""
        try:
            required_cols = ['YY', 'MM', 'DD', 'hh', 'mm']
            for col in required_cols:
                if col not in df.columns:
                    self.logger.error(f"时间转换失败: 缺少列 '{col}'")
                    return None
            yy = pd.to_numeric(df['YY'], errors='coerce')
            mm = pd.to_numeric(df['MM'], errors='coerce')
            dd = pd.to_numeric(df['DD'], errors='coerce')
            hh = pd.to_numeric(df['hh'], errors='coerce')
            minute = pd.to_numeric(df['mm'], errors='coerce')
            if yy.notna().sum() == 0 or mm.notna().sum() == 0:
                self.logger.error(f"时间转换失败: YY或MM列全部无效")
                return None
            year = yy.apply(lambda y: 2000 + y if pd.notna(y) and y < 100 else y)
            observation_time = pd.to_datetime({
                'year': year,
                'month': mm,
                'day': dd,
                'hour': hh,
                'minute': minute
            }, errors='coerce')
            
            return observation_time
            
        except Exception as e:
            self.logger.error(f"时间转换异常: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return None
    
    def scrape_station_info(self, station_name):
        """从NOAA网站爬取站点坐标"""
        try:
            url = f"https://www.ndbc.noaa.gov/station_page.php?station={station_name}"
            # 添加时间戳参数防止HTTP缓存
            cache_buster = int(time.time() * 1000)
            url_with_cache_buster = f"{url}&_={cache_buster}"
            response = self.session.get(url_with_cache_buster, timeout=self.request_timeout)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            page_text = soup.get_text()
            coord_pattern = r'(\d+\.?\d*)\s*([NS])\s+(\d+\.?\d*)\s*([EW])'
            match = re.search(coord_pattern, page_text)
            
            if match:
                lat_value = float(match.group(1))
                lat_dir = match.group(2)
                lon_value = float(match.group(3))
                lon_dir = match.group(4)
                
                latitude = lat_value if lat_dir == 'N' else -lat_value
                longitude = lon_value if lon_dir == 'E' else -lon_value
                
                return {'latitude': latitude, 'longitude': longitude}
            else:
                return None
        except Exception as e:
            self.logger.warning(f"爬取站点 {station_name} 坐标失败: {e}")
            return None
    
    def fetch_station_time_only(self, station_id):
        """
        快速获取站点的时间批次（不获取WSPD数据，用于快速检查）
        
        Args:
            station_id (str): 站点ID
            
        Returns:
            datetime: 站点的observation_time，或 None（获取失败）
        """
        try:
            url = f"https://www.ndbc.noaa.gov/station_page.php?station={station_id}"
            cache_buster = int(time.time() * 1000)
            url_with_cache_buster = f"{url}&_={cache_buster}"
            
            response = self.session.get(url_with_cache_buster, timeout=5)  # 更短超时
            response.raise_for_status()
            
            page_text_lower = response.text.lower()
            if 'no recent reports' in page_text_lower or 'no data available' in page_text_lower:
                return None
            conditions_match = re.search(
                r'(?<!Ocean\s)Conditions\s+at\s+\w+\s+as\s+of\s*(\d{4})\s*GMT\s*on\s*(\d{1,2}/\d{1,2}/\d{4})', 
                response.text, 
                re.DOTALL | re.IGNORECASE
            )
            
            if not conditions_match:
                return None
            
            gmt_time_str = conditions_match.group(1)
            gmt_date_str = conditions_match.group(2)
            
            try:
                hour = int(gmt_time_str[:2])
                minute = int(gmt_time_str[2:])
                date_parts = gmt_date_str.split('/')
                month = int(date_parts[0])
                day = int(date_parts[1])
                year = int(date_parts[2])
                observation_time = datetime(year, month, day, hour, minute)
                return observation_time
            except (ValueError, IndexError):
                return None
                
        except Exception:
            return None
    
    def fetch_station_wspd_supplement(self, station_id):
        """
        从站点详情页获取WSPD10M和WSPD20M补充数据及其时间批次
        
        参考：noaa_station_scraper.py 的 _extract_realtime_conditions 方法
        
        Args:
            station_id (str): 站点ID
            
        Returns:
            dict: {'time': datetime, 'WSPD10M': value, 'WSPD20M': value} 或 None（获取失败）
        """
        try:
            # 构建URL参数（参考noaa_station_scraper.py）
            url = "https://www.ndbc.noaa.gov/station_page.php"
            params = {
                'station': station_id,
                'unit': 'M',  # M=公制(m/s, mb, °C), E=英制(kts, in, °F)
                'tz': 'GMT',  # GMT时区
                '_': int(time.time() * 1000) 
            }
            response = self.session.get(url, params=params, timeout=self.request_timeout)
            response.raise_for_status()
            page_text_lower = response.text.lower()
            if 'no recent reports' in page_text_lower or 'no data available' in page_text_lower:
                return None
            
            soup = BeautifulSoup(response.text, 'html.parser')
            page_text = soup.get_text()
            conditions_match = re.search(
                r'(?<!Ocean\s)Conditions\s+at\s+[\w\d]+\s+as\s+of\s*(?:\([^)]+\))?\s*(\d{4})\s*GMT\s*on\s*(\d{1,2}/\d{1,2}/\d{4})', 
                page_text, 
                re.DOTALL | re.IGNORECASE
            )
            
            if not conditions_match:
                return None
            gmt_time_str = conditions_match.group(1)  # 如 "0230"
            gmt_date_str = conditions_match.group(2)  # 如 "11/10/2025"
            try:
                hour = int(gmt_time_str[:2])
                minute = int(gmt_time_str[2:])
                
                # 解析日期：MM/DD/YYYY
                date_parts = gmt_date_str.split('/')
                month = int(date_parts[0])
                day = int(date_parts[1])
                year = int(date_parts[2])
                
                # 构建 datetime 对象（GMT时间）
                observation_time = datetime(year, month, day, hour, minute)
                
            except (ValueError, IndexError) as e:
                self.logger.warning(f"站点 {station_id} 时间解析失败: {gmt_time_str} GMT on {gmt_date_str}, 错误: {e}")
                return None
            conditions_start = conditions_match.start()
            conditions_end_match = re.search(
                r'(Ocean\s+Conditions|Wave\s+Summary)', 
                page_text[conditions_start:], 
                re.IGNORECASE
            )
            if conditions_end_match:
                conditions_end = conditions_start + conditions_end_match.start()
            else:
                conditions_end = min(conditions_start + 2000, len(page_text))
            conditions_text = page_text[conditions_start:conditions_end]
            result = {'time': observation_time}  # 保存时间批次
            wspd10m_match = re.search(
                r'Wind\s+Speed\s+at\s+10\s+meters?\s*\(WSPD10M\)\s*:\s*([\d.]+)', 
                conditions_text, 
                re.IGNORECASE
            )
            if wspd10m_match:
                result['WSPD10M'] = round(float(wspd10m_match.group(1)), 1)
            wspd20m_match = re.search(
                r'Wind\s+Speed\s+at\s+20\s+meters?\s*\(WSPD20M\)\s*:\s*([\d.]+)', 
                conditions_text, 
                re.IGNORECASE
            )
            if wspd20m_match:
                result['WSPD20M'] = round(float(wspd20m_match.group(1)), 1)
            if 'WSPD10M' in result or 'WSPD20M' in result:
                return result
            else:
                return None
                
        except requests.exceptions.Timeout:
            self.logger.debug(f"站点 {station_id} 补充数据获取超时（网络超时）")
            return None
        except requests.exceptions.RequestException as e:
            self.logger.debug(f"站点 {station_id} 补充数据获取失败（网络错误）: {e}")
            return None
        except Exception as e:
            self.logger.debug(f"站点 {station_id} 补充数据获取失败（解析异常）: {e}")
            return None
    
    def save_wspd_cache_to_file(self):
        """保存WSPD补充数据缓存到文件"""
        try:
            cache_data = {}
            for station_id, data in self.station_wspd_cache.items():
                cache_data[station_id] = {
                    'time': data['time'].isoformat() if 'time' in data else None,
                    'WSPD10M': data.get('WSPD10M'),
                    'WSPD20M': data.get('WSPD20M')   }
            with open(self.wspd_cache_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'timestamp': time.time(),  
                    'data': cache_data
                }, f, ensure_ascii=False, indent=2)
            
            self.logger.debug(f"✓ WSPD补充数据缓存已保存到 {self.wspd_cache_file}")
        except Exception as e:
            self.logger.warning(f"保存WSPD缓存失败: {e}")
    def check_all_stations_changes(self, station_ids):
        """
        检查所有站点的数据变化（包括时间批次和WSPD数值）
        
        关键改进：
        - 不仅检查时间批次，还检查WSPD10M/WSPD20M数值
        - 原因：同一时间批次，数据可能后续更新（一开始没有，后来补充了）
        
        Args:
            station_ids (list): 需要检查的站点ID列表
            
        Returns:
            tuple: (changed_stations, unchanged_stations)
                - changed_stations: 数据变化的站点ID列表
                - unchanged_stations: 数据未变化的站点ID列表
        """
        self.logger.info(f"正在检查所有 {len(station_ids)} 个站点的数据变化（包括时间和WSPD数值）...")
        
        changed_stations = []
        unchanged_stations = []
        no_data_stations = []     
        real_error_stations = []  
        with ThreadPoolExecutor(max_workers=min(5, self.max_workers)) as executor:  # 降低WSPD并发（20→5）
            futures = {
                executor.submit(self.fetch_station_wspd_supplement, station_id): station_id 
                for station_id in station_ids
            }
            
            for future in as_completed(futures):
                station_id = futures[future]
                try:
                    current_data = future.result()
                    
                    if current_data is None:
                        del current_data
                        no_data_stations.append(station_id)
                        continue
                    with self.wspd_cache_lock:
                        cached_data = self.station_wspd_cache.get(station_id)
                    if not cached_data:
                        changed_stations.append(station_id)
                        with self.wspd_cache_lock:
                            self.station_wspd_cache[station_id] = current_data
                    else:
                        has_change = False
                        change_reasons = []
                        current_time = current_data.get('time')
                        cached_time = cached_data.get('time')
                        if current_time != cached_time:
                            has_change = True
                            change_reasons.append(
                                f"时间: {cached_time.strftime('%H:%M') if cached_time else 'None'} → "
                                f"{current_time.strftime('%H:%M') if current_time else 'None'}"  )
                        if self.wspd_cache_check_values:
                            current_wspd10m = current_data.get('WSPD10M')
                            cached_wspd10m = cached_data.get('WSPD10M')
                            if current_wspd10m != cached_wspd10m:
                                has_change = True
                                change_reasons.append(
                                    f"WSPD10M: {cached_wspd10m} → {current_wspd10m}"  )
                            current_wspd20m = current_data.get('WSPD20M')
                            cached_wspd20m = cached_data.get('WSPD20M')
                            if current_wspd20m != cached_wspd20m:
                                has_change = True
                                change_reasons.append(
                                    f"WSPD20M: {cached_wspd20m} → {current_wspd20m}"  )
                        if has_change:
                            changed_stations.append(station_id)
                            with self.wspd_cache_lock:
                                self.station_wspd_cache[station_id] = current_data
                            if change_reasons:
                                self.logger.debug(
                                    f"站点 {station_id} 数据变化: {', '.join(change_reasons)}"  )
                        else:
                            unchanged_stations.append(station_id)
                except Exception as e:
                    real_error_stations.append(station_id)
                    self.logger.debug(f"站点 {station_id} 数据检查异常: {e}")
        self.logger.info(
            f"✓ 数据检查完成: "
            f"需要更新 {len(changed_stations)} 个站点, "
            f"保留缓存 {len(unchanged_stations)} 个站点"
        )
        if no_data_stations:
            self.logger.info(f"  无WSPD10M数据: {len(no_data_stations)} 个站点（正常，不支持或无报告）")
            # 显示前20个无数据站点，方便调试（使用INFO级别便于查看）
            sample_no_data = no_data_stations[:20]
            if len(sample_no_data) > 0:
                self.logger.info(f"  无数据站点示例（前20个）: {', '.join(sample_no_data)}")
                if len(no_data_stations) > 20:
                    self.logger.info(f"  ... 还有 {len(no_data_stations) - 20} 个无数据站点")
        
        # 输出真正失败的站点（需要关注）
        if real_error_stations:
            self.logger.warning(f"  ⚠️  检查异常: {len(real_error_stations)} 个站点（网络错误或解析失败）")
            # 显示所有失败站点，方便调试
            self.logger.warning(f"  异常站点: {', '.join(real_error_stations)}")
        
        return changed_stations, unchanged_stations
    
    def validate_wspd_cache(self, station_ids):
        """
        验证缓存是否仍然有效（抽样检查）
        
        策略：随机抽查N个站点，对比时间批次是否变化
        - 如果时间批次变化 → 缓存失效（数据已更新）
        - 如果时间批次不变 → 缓存有效
        
        Args:
            station_ids (list): 需要验证的站点ID列表
            
        Returns:
            bool: True=缓存有效, False=缓存失效（需要更新）
        """
        try:
            import random
            
            # 从缓存中的站点中随机抽样
            cached_stations = list(self.station_wspd_cache.keys())
            if not cached_stations:
                return False
            
            # 随机选择N个站点进行验证
            sample_size = min(self.wspd_cache_validation_sample, len(cached_stations))
            sample_stations = random.sample(cached_stations, sample_size)
            
            self.logger.info(f"正在验证缓存有效性（抽查 {sample_size} 个站点）...")
            
            changed_count = 0
            unchanged_count = 0
            error_count = 0
            
            for station_id in sample_stations:
                try:
                    # 获取缓存中的时间批次
                    cached_time = self.station_wspd_cache[station_id].get('time')
                    if not cached_time:
                        continue
                    
                    # 实时获取当前的时间批次
                    current_data = self.fetch_station_wspd_supplement(station_id)
                    if not current_data or 'time' not in current_data:
                        error_count += 1
                        continue
                    
                    current_time = current_data['time']
                    
                    # 对比时间批次
                    if current_time != cached_time:
                        changed_count += 1
                        self.logger.debug(
                            f"站点 {station_id} 时间批次已变化: "
                            f"{cached_time.strftime('%Y-%m-%d %H:%M')} → "
                            f"{current_time.strftime('%Y-%m-%d %H:%M')}"
                        )
                    else:
                        unchanged_count += 1
                        
                except Exception as e:
                    error_count += 1
                    self.logger.debug(f"站点 {station_id} 验证失败: {e}")
                
                # 添加延迟避免请求过快
                time.sleep(0.2)
            
            # 判断逻辑：严格模式下，只要有任何站点变化就触发全量更新
            total_checked = changed_count + unchanged_count
            if total_checked == 0:
                self.logger.warning(f"缓存验证失败（{error_count}个站点出错），将重新获取")
                return False
            
            if self.wspd_cache_strict_mode:
                # 严格模式：只要有1个站点变化，就认为缓存失效
                if changed_count > 0:
                    self.logger.info(
                        f"✗ 缓存已失效：检测到 {changed_count}/{total_checked} 个站点时间批次已变化，将全量更新所有站点"
                    )
                    return False
                else:
                    self.logger.info(
                        f"✓ 缓存仍然有效：{total_checked}/{total_checked} 个站点时间批次未变化，保留缓存"
                    )
                    return True
            else:
                # 宽松模式：超过30%变化才触发更新
                change_rate = changed_count / total_checked
                if change_rate > 0.3:
                    self.logger.info(
                        f"✗ 缓存已失效：{changed_count}/{total_checked} 个站点时间批次已变化（{change_rate*100:.0f}%），将重新获取"
                    )
                    return False
                else:
                    self.logger.info(
                        f"✓ 缓存仍然有效：{unchanged_count}/{total_checked} 个站点时间批次未变化（{(1-change_rate)*100:.0f}%）"
                    )
                    return True
                
        except Exception as e:
            self.logger.warning(f"缓存验证异常: {e}，将重新获取")
            return False
    
    def load_wspd_cache_from_file(self):
        """从文件加载WSPD补充数据缓存"""
        try:
            if not os.path.exists(self.wspd_cache_file):
                return False
            
            with open(self.wspd_cache_file, 'r', encoding='utf-8') as f:
                cache = json.load(f)
            
            # 检查缓存是否过期（时间过期）
            cache_age = time.time() - cache['timestamp']
            if cache_age > self.wspd_cache_expiry:
                self.logger.info(f"WSPD缓存已过期（{cache_age/60:.1f}分钟前），将重新获取")
                return False
            
            # 恢复缓存（将字符串转换回datetime）
            for station_id, data in cache['data'].items():
                if data['time']:
                    data['time'] = datetime.fromisoformat(data['time'])
                self.station_wspd_cache[station_id] = data
            
            cache_minutes = cache_age / 60
            self.logger.info(f"✓ 从缓存文件加载了 {len(self.station_wspd_cache)} 个站点的补充数据（{cache_minutes:.1f}分钟前）")
            
            # 显示部分示例
            if len(self.station_wspd_cache) > 0:
                sample_stations = list(self.station_wspd_cache.items())[:3]
                for station_id, data in sample_stations:
                    time_str = data['time'].strftime('%Y-%m-%d %H:%M') if 'time' in data else 'N/A'
                    wspd10m = data.get('WSPD10M', '-')
                    wspd20m = data.get('WSPD20M', '-')
                    self.logger.info(f"  缓存示例: {station_id} @ {time_str} -> WSPD10M={wspd10m}, WSPD20M={wspd20m}")
            
            return True
        except Exception as e:
            self.logger.warning(f"加载WSPD缓存失败: {e}")
            return False
    
    def load_station_wspd_supplements(self, station_ids):
        """
        批量加载站点的WSPD10M/WSPD20M补充数据（全量检查 + 增量更新）
        
        新策略（用户要求）：
        1. 不要抽样验证，直接全量检查所有站点
        2. 检查内容：时间批次 + WSPD10M + WSPD20M数值
        3. 原因：同一时间批次，数据可能后续更新（一开始没有，后来补充了）
        4. 只更新变化的站点，未变化的保留缓存
        
        Args:
            station_ids (list): 站点ID列表
        """
        # 步骤1: 尝试从缓存文件加载
        cache_loaded = self.load_wspd_cache_from_file()
        
        if cache_loaded:
            # 步骤2: 检查缓存是否覆盖所有需要的站点
            cached_stations = set(self.station_wspd_cache.keys())
            required_stations = set(station_ids)
            missing_stations = required_stations - cached_stations
            
            if missing_stations:
                self.logger.info(f"缓存缺少 {len(missing_stations)} 个站点，将获取缺失站点数据")
                # 先获取缺失的站点
                with ThreadPoolExecutor(max_workers=min(3, self.max_workers)) as executor:  # 降低并发
                    futures = {
                        executor.submit(self.fetch_station_wspd_supplement, station_id): station_id 
                        for station_id in missing_stations
                    }
                    for future in as_completed(futures):
                        try:
                            result = future.result()
                            if result:
                                station_id = futures[future]
                                with self.wspd_cache_lock:
                                    self.station_wspd_cache[station_id] = result
                            del result
                        except Exception:
                            pass
            
            # 步骤3: 直接全量检查所有站点（不抽样）
            if self.wspd_cache_skip_sampling:
                self.logger.info(f"启动全量检查模式（检查时间批次和WSPD数值变化）")
                
                # 检查所有站点的数据变化
                changed_stations, unchanged_stations = self.check_all_stations_changes(station_ids)
                
                if changed_stations:
                    # 更新变化的站点到缓存（数据已经在check_all_stations_changes中获取了）
                    self.logger.info(f"✓ 检测到 {len(changed_stations)} 个站点数据变化，已更新缓存")
                    
                    # 保留未变化站点的缓存
                    if unchanged_stations:
                        self.logger.info(f"✓ 保留 {len(unchanged_stations)} 个未变化站点的缓存数据")
                    
                    # 保存更新后的缓存
                    self.save_wspd_cache_to_file()
                    return
                else:
                    self.logger.info(f"✓ 所有站点数据检查完成，无需更新")
                    return
            else:
                # 使用抽样验证（旧逻辑，已废弃）
                cache_valid = self.validate_wspd_cache(station_ids)
                if cache_valid:
                    self.logger.info(f"✓ 缓存有效且覆盖所有 {len(station_ids)} 个站点，跳过重复获取")
                    return
        
        # 步骤4: 缓存不存在或已过期，首次全量获取所有站点
        self.logger.info(f"开始首次获取 {len(station_ids)} 个站点的补充数据（WSPD10M/WSPD20M）...")
        self._fetch_wspd_supplements_parallel(station_ids)
        
        # 保存到缓存文件
        self.save_wspd_cache_to_file()
    
    def _fetch_wspd_supplements_parallel(self, station_ids):
        """并行获取站点补充数据（内部方法）"""
        # 使用多线程加速获取
        with ThreadPoolExecutor(max_workers=min(3, self.max_workers)) as executor:  # 降低并发
            futures = {
                executor.submit(self.fetch_station_wspd_supplement, station_id): station_id 
                for station_id in station_ids
            }
            
            success_count = 0
            for future in as_completed(futures):
                station_id = futures[future]
                try:
                    result = future.result()
                    if result:
                        with self.wspd_cache_lock:
                            self.station_wspd_cache[station_id] = result
                        success_count += 1
                    del result
                except Exception as e:
                    self.logger.debug(f"站点 {station_id} 补充数据获取异常: {e}")
        
        self.logger.info(f"✓ 成功获取 {success_count}/{len(station_ids)} 个站点的补充数据")
        
        if success_count > 0:
            # 显示部分示例
            sample_stations = [(sid, self.station_wspd_cache[sid]) for sid in station_ids[:3] if sid in self.station_wspd_cache]
            for station_id, data in sample_stations:
                time_str = data['time'].strftime('%Y-%m-%d %H:%M') if 'time' in data else 'N/A'
                wspd10m = data.get('WSPD10M', '-')
                wspd20m = data.get('WSPD20M', '-')
                self.logger.info(f"  示例: {station_id} @ {time_str} -> WSPD10M={wspd10m}, WSPD20M={wspd20m}")
    
    def insert_station(self, conn, station_name):
        """
        插入站点信息（使用序列自动生成 station_id）
        
        改进：
        - ✅ 不再手动计算 MAX(station_id) + 1（避免并发冲突）
        - ✅ 使用数据库序列自动生成 ID（并发安全）
        - ✅ 简化代码逻辑（减少一次 SELECT 查询）
        
        前提条件：
        需要先执行 migrate_to_sequence.sql 创建序列
        """
        try:
            station_info = self.scrape_station_info(station_name)
            
            if station_info:
                latitude = station_info.get('latitude')
                longitude = station_info.get('longitude')
            else:
                latitude = None
                longitude = None
            
            cursor = conn.cursor()
            
            # ✅ 改进：不再手动指定 station_id，让数据库序列自动生成
            # 数据库会调用 nextval('stations_station_id_seq') 自动分配唯一ID
            insert_sql = """
                INSERT INTO stations (name, latitude, longitude)
                VALUES (%s, %s, %s)
                ON CONFLICT (name) DO NOTHING
            """
            
            cursor.execute(insert_sql, (station_name, latitude, longitude))
            conn.commit()
            cursor.close()
            
            return True
        except Exception as e:
            conn.rollback()
            self.logger.warning(f"插入站点 {station_name} 失败: {e}")
            # 确保 cursor 正确关闭
            try:
                if 'cursor' in locals() and cursor:
                    cursor.close()
            except Exception:
                pass
            return False
    
    def save_to_database(self, conn, df, file_info):
        """
        将数据保存到数据库（性能优化版：使用 execute_values 批量插入）
        
        关键优化：
        1. 使用 execute_values 真正的批量插入（性能提升20倍）
        2. 简化 SQL 逻辑（COALESCE 替代 CASE WHEN，性能提升2倍）
        3. 预先检查站点（避免外键错误）
        """
        cursor = conn.cursor()
        
        try:
            # 预先检查站点是否存在（优化：避免外键错误）
            station_name = file_info['station_id']
            with self.stations_lock:
                station_exists = station_name in self.existing_stations
            
            if not station_exists:
                # 多线程环境下，可能存在竞争 FOREIGN KEY 约束会处理重复插入
                try:
                    if self.insert_station(conn, station_name):
                        with self.stations_lock:
                            self.existing_stations.add(station_name)
                except Exception as e:
                    # 插入失败可能是其他线程已插入，ON CONFLICT DO NOTHING 会处理
                    # 重新检查缓存（简单刷新，或者依赖外键约束处理）
                    self.logger.debug(f"站点 {station_name} 可能已被其他线程插入: {e}")
                    # 不抛出异常，继续执行（如果站点真的不存在，外键约束会捕获）
            
            # 将数据按分表分组（性能优化：使用向量化计算）
            if self.enable_partitioning and not df.empty and 'observation_time' in df.columns:
                # 向量化计算目标分表（比 apply() 快 10-50 倍）
                df['_target_table'] = self._get_partition_table_names_vectorized(
                    self.base_txt_table, df['observation_time']
                )
                
                # 按分表分组
                table_groups = df.groupby('_target_table')
            else:
                # 不启用分表或无时间字段，所有数据使用原表
                df['_target_table'] = self.base_txt_table
                table_groups = [(self.base_txt_table, df)]
            
            # 准备列名（用于后续所有分表）
            columns = [
                'station_name', 'observation_time',
                'WDIR', 'WSPD', 'GST', 'WVHT', 'DPD', 'APD', 'MWD',
                'PRES', 'ATMP', 'WTMP', 'DEWP', 'VIS', 'PTDY', 'TIDE',
                'WSPD10M', 'WSPD20M',
                'first_crawl_time', 'update_time', 'update_count', 'create_time'  # 添加create_time
            ]
            
            # 确保所有列存在
            for col in columns:
                if col not in df.columns:
                    if col in self.REAL_COLUMNS:
                        df[col] = -999.0
                    else:
                        df[col] = None
            
            # 对每个分表分别执行批量插入
            total_processed = 0
            for target_table, group_df in table_groups:
                # 如果是分表，确保分表已创建
                if target_table != self.base_txt_table:
                    self._create_partition_table_if_not_exists(conn, target_table, self.base_txt_table)
                    # 创建分表可能调用rollback，导致cursor关闭，需要重新创建
                    if cursor.closed:
                        cursor = conn.cursor()
                
                # 简化的 UPSERT SQL（使用动态表名）
                upsert_sql = f"""
                    INSERT INTO {target_table} (
                    station_name, observation_time,
                    "WDIR", "WSPD", "GST", "WVHT", "DPD", "APD", "MWD",
                    "PRES", "ATMP", "WTMP", "DEWP", "VIS", "PTDY", "TIDE",
                    "WSPD10M", "WSPD20M",
                    first_crawl_time, update_time, update_count, create_time
                )
                VALUES %s
                ON CONFLICT (station_name, observation_time)
                DO UPDATE SET
                    "WDIR" = COALESCE(NULLIF(EXCLUDED."WDIR", -999), {target_table}."WDIR"),
                    "WSPD" = COALESCE(NULLIF(EXCLUDED."WSPD", -999), {target_table}."WSPD"),
                    "GST" = COALESCE(NULLIF(EXCLUDED."GST", -999), {target_table}."GST"),
                    "WVHT" = COALESCE(NULLIF(EXCLUDED."WVHT", -999), {target_table}."WVHT"),
                    "DPD" = COALESCE(NULLIF(EXCLUDED."DPD", -999), {target_table}."DPD"),
                    "APD" = COALESCE(NULLIF(EXCLUDED."APD", -999), {target_table}."APD"),
                    "MWD" = COALESCE(NULLIF(EXCLUDED."MWD", -999), {target_table}."MWD"),
                    "PRES" = COALESCE(NULLIF(EXCLUDED."PRES", -999), {target_table}."PRES"),
                    "ATMP" = COALESCE(NULLIF(EXCLUDED."ATMP", -999), {target_table}."ATMP"),
                    "WTMP" = COALESCE(NULLIF(EXCLUDED."WTMP", -999), {target_table}."WTMP"),
                    "DEWP" = COALESCE(NULLIF(EXCLUDED."DEWP", -999), {target_table}."DEWP"),
                    "VIS" = COALESCE(NULLIF(EXCLUDED."VIS", -999), {target_table}."VIS"),
                    "PTDY" = COALESCE(NULLIF(EXCLUDED."PTDY", -999), {target_table}."PTDY"),
                    "TIDE" = COALESCE(NULLIF(EXCLUDED."TIDE", -999), {target_table}."TIDE"),
                    "WSPD10M" = COALESCE(NULLIF(EXCLUDED."WSPD10M", -999), {target_table}."WSPD10M"),
                    "WSPD20M" = COALESCE(NULLIF(EXCLUDED."WSPD20M", -999), {target_table}."WSPD20M"),
                    update_time = EXCLUDED.update_time,
                    update_count = {target_table}.update_count + 1
                WHERE
                    -- 只要有任意字段发生变化，就触发更新（只对最近2天的数据）
                    ({target_table}."WDIR" IS DISTINCT FROM COALESCE(NULLIF(EXCLUDED."WDIR", -999), {target_table}."WDIR")
                    OR {target_table}."WSPD" IS DISTINCT FROM COALESCE(NULLIF(EXCLUDED."WSPD", -999), {target_table}."WSPD")
                    OR {target_table}."GST" IS DISTINCT FROM COALESCE(NULLIF(EXCLUDED."GST", -999), {target_table}."GST")
                    OR {target_table}."WVHT" IS DISTINCT FROM COALESCE(NULLIF(EXCLUDED."WVHT", -999), {target_table}."WVHT")
                    OR {target_table}."DPD" IS DISTINCT FROM COALESCE(NULLIF(EXCLUDED."DPD", -999), {target_table}."DPD")
                    OR {target_table}."APD" IS DISTINCT FROM COALESCE(NULLIF(EXCLUDED."APD", -999), {target_table}."APD")
                    OR {target_table}."MWD" IS DISTINCT FROM COALESCE(NULLIF(EXCLUDED."MWD", -999), {target_table}."MWD")
                    OR {target_table}."PRES" IS DISTINCT FROM COALESCE(NULLIF(EXCLUDED."PRES", -999), {target_table}."PRES")
                    OR {target_table}."ATMP" IS DISTINCT FROM COALESCE(NULLIF(EXCLUDED."ATMP", -999), {target_table}."ATMP")
                    OR {target_table}."WTMP" IS DISTINCT FROM COALESCE(NULLIF(EXCLUDED."WTMP", -999), {target_table}."WTMP")
                    OR {target_table}."DEWP" IS DISTINCT FROM COALESCE(NULLIF(EXCLUDED."DEWP", -999), {target_table}."DEWP")
                    OR {target_table}."VIS" IS DISTINCT FROM COALESCE(NULLIF(EXCLUDED."VIS", -999), {target_table}."VIS")
                    OR {target_table}."PTDY" IS DISTINCT FROM COALESCE(NULLIF(EXCLUDED."PTDY", -999), {target_table}."PTDY")
                    OR {target_table}."TIDE" IS DISTINCT FROM COALESCE(NULLIF(EXCLUDED."TIDE", -999), {target_table}."TIDE")
                    OR {target_table}."WSPD10M" IS DISTINCT FROM COALESCE(NULLIF(EXCLUDED."WSPD10M", -999), {target_table}."WSPD10M")
                    OR {target_table}."WSPD20M" IS DISTINCT FROM COALESCE(NULLIF(EXCLUDED."WSPD20M", -999), {target_table}."WSPD20M"))
                    AND {target_table}.observation_time >= NOW() - INTERVAL '2 days'
            """
            
                # 构建 VALUES 模板
                template = """(
                    %s, %s,
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s,
                    %s, %s, %s, %s
                )"""
                
                # 转换为列表（execute_values 需要的格式）
                data = group_df[columns].values.tolist()
                
                # 检查数据是否为空
                if not data:
                    continue
                
                # 分批执行（批处理之间清理内存）
                batch_count = 0
                try:
                    for i in range(0, len(data), self.batch_size):
                        batch = data[i:i + self.batch_size]
                        
                        # 使用 execute_values 批量插入
                        execute_values(
                            cursor, 
                            upsert_sql, 
                            batch,
                            template=template,
                            page_size=self.batch_size
                        )
                        batch_count += 1
                        
                        # 每批处理后提交（防止事务过大，及时释放内存）
                        conn.commit()
                        
                        # 每批处理后执行垃圾回收（但不关闭游标）
                        if self.batch_cleanup_interval > 0 and batch_count % self.batch_cleanup_interval == 0:
                            gc.collect()
                    
                    total_processed += len(data)
                except Exception as batch_error:
                    # 批处理错误，记录并继续处理下一个分表
                    self.logger.warning(f"分表 {target_table} 批处理失败: {batch_error}")
                    raise
            
            # 返回总处理记录数
            return total_processed
            
        except Exception as e:
            # 检查是否是连接已关闭的错误
            error_msg = str(e).lower()
            if 'connection already closed' in error_msg or 'server closed the connection' in error_msg:
                self.logger.error(f"数据库连接已断开: {e}")
                # 连接断开，无法rollback，直接抛出异常
            else:
                # 其他错误，尝试rollback
                try:
                    conn.rollback()
                except Exception:
                    pass  # rollback失败也不影响
                self.logger.error(f"数据库保存失败: {e}")
            raise
        finally:
            # 确保游标正确关闭（统一在外层finally处理）
            try:
                if cursor and not cursor.closed:
                    cursor.close()
            except Exception:
                pass
    
    def process_single_file(self, file_info, stats):
        """处理单个文件（线程安全 + 内存管理 + 连接重试）"""
        # 每个线程使用独立的数据库连接（关键优化：避免并发冲突）
        conn = None
        df = None
        try:
            # 连接数据库（带重试机制）
            conn = self.connect_db(retry_count=3)
            
            # 读取解析
            success, df, error_msg = self.read_and_parse_file_online(file_info)
            
            if success and df is not None:
                # 直接入库
                processed_count = self.save_to_database(conn, df, file_info)
                
                with self.stats_lock:
                    stats['successful_imports'] += 1
                    stats['processed_records'] += processed_count
                    current_count = stats['successful_imports']
                
                # 减少日志输出频率（性能优化，线程安全版本）
                if processed_count > 0 and current_count % self.log_every_n_files == 0:
                    self.logger.info(f"  ✓ {file_info['filename']}: 已处理 {processed_count} 条记录（自动UPSERT） | 已完成 {current_count} 个文件")
                
                # 定期清理内存（每N个文件）
                if current_count % self.memory_cleanup_interval == 0:
                    self.cleanup_memory()
                
                # 定期刷新连接（每N个文件，防止连接累积）
                if current_count % self.connection_refresh_interval == 0:
                    conn = self.refresh_connection(conn)
                    self.logger.debug(f"已刷新数据库连接（处理了 {current_count} 个文件）")
                
                # 显式释放DataFrame内存
                del df
                df = None
                
                return True, None
            else:
                # 立即输出详细错误信息（不等到最后）
                self.logger.error(f"✗ {file_info['filename']} 失败: {error_msg}")
                
                with self.stats_lock:
                    stats['failed_imports'] += 1
                    # 限制错误列表大小（最多保留1000个错误）
                    if len(stats['errors']) < 1000:
                        stats['errors'].append(f"{file_info['filename']}: {error_msg}")
                return False, error_msg
                
        except Exception as e:
            # 立即输出详细错误信息（包括堆栈跟踪）
            error_msg = str(e)
            self.logger.error(f"✗ {file_info['filename']} 异常: {error_msg}")
            import traceback
            self.logger.error(traceback.format_exc())
            
            with self.stats_lock:
                stats['failed_imports'] += 1
                # 限制错误列表大小（最多保留1000个错误）
                if len(stats['errors']) < 1000:
                    stats['errors'].append(f"{file_info['filename']}: {error_msg}")
            return False, error_msg
        finally:
            # 确保释放所有资源
            if df is not None:
                del df
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
            # 强制垃圾回收
            if self.gc_enabled:
                gc.collect()
    
    def run_once(self):
        """运行一次爬取"""
        self.logger.info(f"\n{'='*70}")
        if self.test_mode:
            self.logger.info(f"🧪 测试模式 - 开始爬取数据并入库 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            self.logger.info(f"测试站点: {', '.join(self.test_stations)}")
        else:
            self.logger.info(f"开始爬取数据并入库 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"{'='*70}")
        
        # 获取文件链接
        txt_files = self.fetch_txt_file_links()
        if not txt_files:
            self.logger.warning("没有找到.txt文件")
            return None
        if self.test_mode:
            original_count = len(txt_files)
            txt_files = [f for f in txt_files if f.get('station_id') in self.test_stations]
            filtered_count = len(txt_files)
            if original_count != filtered_count:
                self.logger.info(f"测试模式过滤: {original_count} → {filtered_count} 个文件")
            if filtered_count == 0:
                self.logger.warning(f"测试站点 {self.test_stations} 没有找到任何.txt文件")
                return None
        
        self.stats['total_files'] = len(txt_files)
        
        # 连接数据库（用于预加载站点列表）
        conn = None
        try:
            conn = self.connect_db()
            self.logger.info(f"✓ 数据库连接成功")
            # 预加载站点列表（优化：避免重复查询）
            self.load_existing_stations(conn)
            # 性能优化：预加载已存在的分表到缓存
            if self.enable_partitioning:
                self.load_existing_partitions(conn)
        except Exception as e:
            self.logger.error(f"数据库连接失败，程序终止: {e}")
            return None
        finally:
            # 确保连接被关闭
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
        # 批量获取站点的WSPD10M/WSPD20M补充数据（异常隔离，不影响主流程）
        station_ids = [f['station_id'] for f in txt_files]
        try:
            self.load_station_wspd_supplements(station_ids)
        except Exception as e:
            self.logger.warning(f"⚠️  WSPD补充数据获取失败（不影响主流程）: {e}")
            with self.wspd_cache_lock:
                self.station_wspd_cache.clear()
        self.logger.info(f"开始多线程处理 (线程数: {self.max_workers})")
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self.process_single_file, file_info, self.stats): file_info 
                for file_info in txt_files  }
            completed = 0
            for future in as_completed(futures):
                completed += 1
                if completed % 50 == 0:
                    self.logger.info(f"⏳ 进度: {completed}/{len(txt_files)}")
            del futures
        elapsed_time = time.time() - start_time
        self.print_summary(elapsed_time)
        
        return self.stats
    
    def print_summary(self, elapsed_time):
        self.logger.info(f"\n{'='*70}")
        self.logger.info(f"爬取完成")
        self.logger.info(f"{'='*70}")
        self.logger.info(f"总文件数: {self.stats['total_files']}")
        self.logger.info(f"成功导入: {self.stats['successful_imports']}")
        self.logger.info(f"失败数量: {self.stats['failed_imports']}")
        self.logger.info(f"处理记录数: {self.stats['processed_records']} 条（UPSERT自动判断新增/更新）")
        self.logger.info(f"总用时: {elapsed_time:.2f} 秒")
        if self.stats['errors']:
            self.logger.info(f"\n错误列表 (前10个):")
            for error in self.stats['errors'][:10]:
                self.logger.info(f"  - {error}")
    
    def set_update_interval(self, minutes):
        """设置更新间隔时间"""
        if minutes < 2:
            print(f"⚠️  更新间隔不能少于2分钟，已自动设置为2分钟")
            self.update_interval = 120  # 2分钟
        else:
            self.update_interval = minutes * 60
            print(f"✓ 更新间隔已设置为: {minutes} 分钟")
    
    def run_auto(self, interval_minutes=30):
        """自动定时运行"""
        self.logger.info(f"\n{'='*70}")
        if self.test_mode:
            self.logger.info(f"🧪 测试定时模式启动")
            self.logger.info(f"测试站点: {', '.join(self.test_stations)}")
        else:
            self.logger.info(f"自动定时模式启动")
        self.logger.info(f"{'='*70}")
        self.logger.info(f"更新间隔: {interval_minutes} 分钟")
        self.logger.info(f"按 Ctrl+C 停止")
        self.logger.info(f"{'='*70}\n")
        run_count = 0
        try:
            while True:
                run_count += 1
                self.logger.info(f"\n{'#'*70}")
                self.logger.info(f"第 {run_count} 次运行")
                self.logger.info(f"{'#'*70}")
                self.stats = {
                    'total_files': 0,
                    'successful_imports': 0,
                    'failed_imports': 0,
                    'processed_records': 0,
                    'errors': []
                }
                self.run_once()
                next_run = datetime.now() + timedelta(minutes=interval_minutes)
                self.logger.info(f"\n下次运行时间: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
                time.sleep(interval_minutes * 60)
        except KeyboardInterrupt:
            self.logger.info(f"\n\n{'='*70}")
            self.logger.info(f"用户停止程序")
            self.logger.info(f"总运行次数: {run_count}")
            self.logger.info(f"{'='*70}")


def main():
    scraper = NOAARealtimeToDatabase()
    
    print("=" * 70)
    print("  NOAA实时数据直接入库爬虫 v7.5 (性能优化版)")
    print("  性能提升：15-20 倍 🚀 | 网络优化：智能重试 | 内存管理：智能清理")
    print("  新增功能：WSPD10M/WSPD20M补充数据采集（精确时间匹配） ✨")
    print("=" * 70)
    print(f"  当前配置:")
    print(f"  - 线程数: {scraper.max_workers}")
    print(f"  - 默认更新间隔: {scraper.update_interval // 60} 分钟")
    print(f"  - 测试站点: {scraper.test_stations}")
    print("=" * 70)
    print("\n请选择运行模式：")
    print("  1. 运行一次（全部站点）")
    print("  2. 🧪 测试模式（只爬取测试站点一次：41001, 41056）")
    print("  3. ⏰ 测试定时模式（2分钟间隔自动采集测试站点）")
    print("  4. 自动定时运行（30分钟间隔，全部站点）")
    print("  5. 自定义间隔自动运行（全部站点，最少2分钟）")
    print("  0. 退出")
    print("-" * 70)
    try:
        choice = input("请输入选项 (0-5): ").strip()
        if choice == '1':
            print("\n✓ 选择: 运行一次（全部站点）")
            scraper.run_once()
        elif choice == '2':
            print("\n✓ 选择: 测试模式（单次）")
            scraper.test_mode = True
            print(f"只爬取测试站点: {scraper.test_stations}")
            scraper.run_once()
        elif choice == '3':
            print("\n✓ 选择: 测试定时模式")
            scraper.test_mode = True
            print(f"只爬取测试站点: {scraper.test_stations}")
            print(f"更新间隔: 2 分钟")
            scraper.set_update_interval(2) 
            scraper.run_auto(2)
        elif choice == '4':
            print("\n✓ 选择: 自动定时运行（30分钟间隔，全部站点）")
            scraper.run_auto(30)
        elif choice == '5':
            print("\n✓ 选择: 自定义间隔自动运行（全部站点）")
            try:
                interval = int(input("请输入更新间隔（分钟，最少2分钟）: ").strip())
                if interval < 2:
                    print("⚠️  间隔不能少于2分钟，已自动设置为2分钟")
                    interval = 2
                scraper.run_auto(interval)
            except ValueError:
                print("输入无效，请输入数字")
        elif choice == '0':
            print("\n再见！")
            return
        else:
            print("\n无效选项，请重新运行程序")
    except KeyboardInterrupt:
        print("\n\n用户中断程序")
    except Exception as e:
        print(f"\n程序运行出错: {e}")


if __name__ == "__main__":
    main()

