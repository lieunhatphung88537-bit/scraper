# -*- coding: utf-8 -*-
"""
NOAA实时数据海洋数据入库爬虫 v9.3 (Ocean Data Only)
======================================================================================
实时爬取NOAA海洋数据（.ocean）并直接存入PostgreSQL数据库

主要功能：
1. ✅ 支持 .ocean 文件（海洋数据 → ocean_data 表）
2. ✅ 智能UPSERT（避免重复，自动合并数据）
3. ✅ 自动站点注册（站点不存在时自动添加）
4. ✅ 多线程并行爬取（高性能）
5. ✅ 真正的批量插入（execute_values，性能提升20倍）
6. ✅ 向量化数据处理（Pandas优化）
7. ✅ 定时自动运行（持续监控）
8. ✅ 完整的错误处理和日志
9. ✅ 智能重试机制（网络超时自动重试5次，指数退避）
10. ✅ 请求限流保护（避免触发服务器反爬虫机制）
11. ✅ 智能内存管理（定期释放缓存，防止长时间运行OOM）

数据流程：
NOAA网站 → 爬取解析 → 直接入库到PostgreSQL（ocean_data）

内存管理策略：
- 每批处理后立即提交（防止事务累积）
- 每2批清理游标和Python对象（更积极地释放内存）
- 每20个文件执行垃圾回收（防止内存泄漏）
- 每40个文件刷新数据库连接（防止连接累积）

使用方法：
1. 单次运行：python noaa_realtime2_scraper_to_db_multi.py
2. 定时运行：选择自动模式
3. 测试模式：选择测试模式

创建时间：2025-11-03
最后更新：2025-11-07 (添加智能内存管理，解决长时间运行OOM)
基于版本：v8.0
"""

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
try:
    from db_config import DB_CONFIG
except ImportError:
    print("警告: 未找到 db_config.py，使用默认配置")
    import os
    DB_CONFIG = {
        'host': os.getenv('DB_HOST', 'database-1.cxik82g8267p.us-east-2.rds.amazonaws.com'),
        'port': int(os.getenv('DB_PORT', '5432')),
        'database': 'noaa_data',
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', 'oceantest1')
    }

class NOAARealtimeToDatabase:
    
    # OCEAN 数据的 REAL 类型字段
    OCEAN_REAL_COLUMNS = [
        'DEPTH', 'OTMP', 'COND', 'SAL', 'O2%', 'O2PPM', 'CLCON', 'TURB', 'PH', 'EH'
    ]
    
    def __init__(self, db_config=None, enable_partitioning=True):
        """
        初始化爬虫
        
        参数:
            db_config: 数据库配置
            enable_partitioning: 是否启用分表功能（默认True）
        """
        # NOAA设置
        self.base_url = "https://www.ndbc.noaa.gov/data/realtime2/"
        
        # User-Agent轮换列表（模拟不同浏览器）
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]
        
        self.headers = {
            'User-Agent': self.user_agents[0],  # 默认使用第一个
            'Cache-Control': 'no-cache, no-store, must-revalidate, max-age=0',
            'Pragma': 'no-cache',
            'Expires': '0',
            'If-Modified-Since': '',      # 强制不使用条件请求
            'If-None-Match': ''            # 强制不使用ETag缓存
        }
        
        # 数据库配置
        self.db_config = db_config or DB_CONFIG
        
        # 分表配置（ocean_data 按月分表）
        self.enable_partitioning = enable_partitioning
        self.partition_start_date = datetime(2025, 1, 1, 0, 0, 0)
        self.base_ocean_table = 'ocean_data'
        
        self.max_workers = 3  # 降低并发数（5→3），避免触发NOAA限流
        self.batch_size = 500  # 恢复较大批量（原8000，现4000，通过内存释放解决OOM）
        self.request_timeout = 30  # 增加超时时间（原20 → 30秒）
        self.max_retries = 5  # 增加重试次数（原3 → 5次）
        self.update_interval = 2 * 60
        self.log_every_n_files = 100
        self.request_delay = 1.0  # 增加延迟（0.1→1.0秒），更礼貌的爬取
        
        # 内存管理配置
        self.memory_cleanup_interval = 20  # 更频繁地做全局GC
        self.connection_refresh_interval = 40  # 降低刷新间隔避免长连接
        self.batch_cleanup_interval = 2  # 两批就清理游标/GC
        self.gc_enabled = True  # 启用垃圾回收
        
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.session.trust_env = False  # 禁用环境代理/缓存设置
        
        # 连接池 + 自动重试
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=20,
            pool_maxsize=100,
            max_retries=retry_strategy
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        
        # 线程锁
        self.stats_lock = threading.Lock()
        self.stations_lock = threading.Lock()
        self.partition_lock = threading.Lock()  # 分表创建锁
        
        # 站点缓存
        self.existing_stations = set()
        
        # 分表缓存（记录已创建或已确认存在的分表）
        self.created_partitions = set()
        
        # 统计信息
        self.stats = {
            'total_files': 0,
            'successful_imports': 0,
            'failed_imports': 0,
            'processed_records': 0,
            'ocean_files': 0,
            'errors': []
        }
        
        # 测试模式
        self.test_mode = False
        self.test_stations = ['41001', '41056', '32ST0']
        
        # 设置日志
        self.setup_logging()
    
    def setup_logging(self):
        import os
        import sys
        # 使用 logs 目录（兼容Docker和Windows本地环境）
        script_dir = os.path.dirname(os.path.abspath(__file__))
        local_log_dir = os.path.join(script_dir, 'logs')
        # Docker环境（Linux）使用 /logs，Windows本地使用脚本所在目录的 logs
        if sys.platform == 'win32':
            log_dir = local_log_dir
        else:
            log_dir = '/logs' if os.path.exists('/logs') and os.path.isdir('/logs') else local_log_dir
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'noaa_ocean.log')
        
        # 使用独立的 logger（不是 root logger）
        self.logger = logging.getLogger('noaa_ocean')
        self.logger.setLevel(logging.INFO)
        
        # 避免重复添加 handler
        if not self.logger.handlers:
            # 文件 handler（每条日志立即刷新）
            class FlushFileHandler(logging.FileHandler):
                def emit(self, record):
                    super().emit(record)
                    self.flush()
            
            file_handler = FlushFileHandler(log_file, encoding='utf-8')
            file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
            self.logger.addHandler(file_handler)
            
            # 控制台 handler
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
                    # 连接优化参数
                    connect_timeout=10,  # 增加连接超时时间
                    application_name='noaa_scraper_multi',
                    # 性能优化：减少往返次数
                    options='-c statement_timeout=300000',  # 5分钟超时
                    # 指定客户端编码为UTF-8，避免编码错误
                    client_encoding='UTF8'
                )
                
                # ⚠️ 关键：必须在任何操作前先设置 autocommit，避免事务冲突
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
                # ⚠️ 新增：专门处理编码错误（PostgreSQL返回非UTF-8数据）
                last_error = e
                self.logger.error(f"❌ 数据库连接编码错误 (第{attempt+1}/{retry_count}次): {e}")
                self.logger.error(f"   编码错误详情: 位置 {e.start}-{e.end}, 问题字节: {e.object[e.start:min(e.end+10, len(e.object))].hex()}")
                
                if attempt < retry_count - 1:
                    wait_time = min(2 ** (attempt + 2), 30)  # 更长的等待时间：4,8,16,30秒
                    self.logger.warning(f"   ⏳ 等待 {wait_time} 秒后重试（可能是数据库临时繁忙或编码配置问题）...")
                    time.sleep(wait_time)
                    continue
                else:
                    self.logger.error(f"   ❌ 编码错误重试{retry_count}次后仍然失败")
                    self.logger.error(f"   💡 建议检查：1) PostgreSQL编码配置 2) 数据库错误日志 3) 服务器状态")
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
        
        # 如果所有重试都失败
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
            
            # 执行三代垃圾回收
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
    
    def load_existing_partitions(self, conn):
        """启动时预加载已存在的分表到缓存（性能优化：避免重复检查）"""
        cursor = None
        try:
            cursor = conn.cursor()
            # 查询所有以 ocean_data 开头的分表
            cursor.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name LIKE 'ocean_data_%%'
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
    
    def fetch_file_links(self):
        """获取 ocean 文件链接（带重试机制）"""
        last_error = None
        
        # 重试机制
        for attempt in range(self.max_retries):
            try:
                if attempt > 0:
                    self.logger.info(f"正在获取 ocean 文件链接... (第{attempt+1}次尝试)")
                else:
                    self.logger.info("正在获取 ocean 文件链接...")
                
                # 随机选择User-Agent
                import random
                
                cache_buster = int(time.time() * 1000)
                url_with_cache_buster = f"{self.base_url}?_={cache_buster}"
                
                # 强制每次请求都附带最新的反缓存头
                fresh_headers = {
                    'User-Agent': random.choice(self.user_agents),
                    'Cache-Control': f'no-cache, no-store, must-revalidate, max-age=0, s-maxage=0',
                    'Pragma': 'no-cache',
                    'Expires': '0',
                    'If-Modified-Since': '',
                    'If-None-Match': '',
                    'X-Request-Time': str(cache_buster)  # 自定义时间戳头
                }
                
                response = self.session.get(
                    url_with_cache_buster, 
                    timeout=self.request_timeout,
                    headers=fresh_headers
                )
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, 'html.parser')
                links = soup.find_all('a', href=True)
                
                files = []
                # 过滤掉无效的文件名
                invalid_files = {'ship_obs.ocean', 'index.ocean', 'readme.ocean'}
                
                for link in links:
                    href = link.get('href')
                    if not href:
                        continue
                    
                    # 只处理 .ocean 文件
                    if href.endswith('.ocean'):
                        # 过滤掉无效文件
                        if href.lower() in invalid_files:
                            continue
                        
                        full_url = urljoin(self.base_url, href)
                        # 提取站点ID（去掉 .ocean 后缀）
                        station_id = href.replace('.ocean', '')
                        
                        # 测试模式过滤
                        if self.test_mode and station_id not in self.test_stations:
                            continue
                        
                        file_info = {
                            'filename': href,
                            'url': full_url,
                            'station_id': station_id,
                            'file_type': 'ocean'
                        }
                        files.append(file_info)
                
                self.logger.info(f"✓ 找到 {len(files)} 个 ocean 文件")
                return files
                
            except requests.exceptions.Timeout as e:
                last_error = f"请求超时 (第{attempt+1}/{self.max_retries}次): {e}"
                self.logger.warning(last_error)
                if attempt < self.max_retries - 1:
                    wait_time = 2 ** attempt  # 指数退避: 1秒, 2秒, 4秒
                    self.logger.info(f"等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                    continue
                    
            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code if hasattr(e, 'response') and e.response else 'unknown'
                last_error = f"HTTP错误 {status_code} (第{attempt+1}/{self.max_retries}次): {e}"
                self.logger.warning(last_error)
                if status_code in [429, 500, 502, 503, 504] and attempt < self.max_retries - 1:
                    wait_time = 2 ** attempt
                    self.logger.info(f"等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                    continue
                else:
                    break
                    
            except requests.exceptions.RequestException as e:
                last_error = f"网络错误 (第{attempt+1}/{self.max_retries}次): {e}"
                self.logger.warning(last_error)
                if attempt < self.max_retries - 1:
                    wait_time = 2 ** attempt
                    self.logger.info(f"等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                    continue
                    
            except Exception as e:
                last_error = f"未知错误: {e}"
                self.logger.error(last_error)
                break
        
        # 所有重试都失败
        self.logger.error(f"获取文件链接失败（已重试{self.max_retries}次）: {last_error}")
        return []
    
    def read_and_parse_file_online(self, file_info):
        """读取并解析在线文件（带重试机制）"""
        last_error = None
        
        # 添加请求延迟，避免过快请求触发服务器限流
        time.sleep(self.request_delay)
        
        for attempt in range(self.max_retries):
            try:
                # 随机选择User-Agent
                import random
                
                cache_buster = int(time.time() * 1000)
                url_with_cache_buster = f"{file_info['url']}?_={cache_buster}"
                
                # 强制每次请求都附带最新的反缓存头
                fresh_headers = {
                    'User-Agent': random.choice(self.user_agents),
                    'Cache-Control': f'no-cache, no-store, must-revalidate, max-age=0, s-maxage=0',
                    'Pragma': 'no-cache',
                    'Expires': '0',
                    'If-Modified-Since': '',
                    'If-None-Match': '',
                    'X-Request-Time': str(cache_buster)  # 自定义时间戳头
                }
                
                response = self.session.get(
                    url_with_cache_buster, 
                    timeout=self.request_timeout,
                    headers=fresh_headers
                )
                response.raise_for_status()
                
                # 检查Content-Type，如果返回HTML说明文件不存在或服务器错误
                content_type = response.headers.get('Content-Type', '').lower()
                if 'text/html' in content_type or response.text.strip().startswith('<!DOCTYPE') or response.text.strip().startswith('<html'):
                    # 服务器返回HTML错误页面而不是ocean数据（状态码200但实际是错误）
                    last_error = f"服务器返回HTML页面而不是数据文件（Content-Type: {content_type}），文件可能不存在或暂时不可用"
                    self.logger.debug(f"{file_info['filename']}: {last_error}")
                    break
                
                # 解析 ocean 文件
                df = self.parse_ocean_content(response.text, file_info)
                
                if df is not None and not df.empty:
                    return True, df, None
                else:
                    return False, None, "数据解析失败或数据为空"
                    
            except requests.exceptions.Timeout as e:
                last_error = f"超时 (第{attempt+1}次): {e}"
                if attempt < self.max_retries - 1:
                    wait_time = min(2 ** attempt, 10)  # 指数退避，最多10秒
                    time.sleep(wait_time)
                    continue
            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code if hasattr(e, 'response') and e.response else 'unknown'
                last_error = f"HTTP错误 {status_code} (第{attempt+1}次): {e}"
                
                # 404错误不重试（文件不存在）
                if status_code == 404:
                    last_error = f"文件不存在 (404): {file_info['filename']}"
                    break
                
                if status_code in [429, 500, 502, 503, 504] and attempt < self.max_retries - 1:
                    wait_time = min(2 ** attempt, 10)  # 指数退避，最多10秒
                    time.sleep(wait_time)
                    continue
                else:
                    break
            except requests.exceptions.RequestException as e:
                last_error = f"网络错误 (第{attempt+1}次): {e}"
                if attempt < self.max_retries - 1:
                    wait_time = min(2 ** attempt, 10)  # 指数退避，最多10秒
                    time.sleep(wait_time)
                    continue
            except Exception as e:
                last_error = f"读取失败: {e}"
                break
        
        return False, None, last_error
    
    def parse_ocean_content(self, raw_data, file_info):
        """解析OCEAN内容"""
        if not raw_data or not raw_data.strip():
            return None
        
        try:
            lines = raw_data.strip().split('\n')
            data_lines = []
            header_line = None
            
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                if line.startswith('#'):
                    if line.startswith('#YY'):
                        header_line = line[1:].strip()
                    continue
                else:
                    data_lines.append(line)
            
            if not data_lines or not header_line:
                return None
            
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
                return None
            
            df = pd.DataFrame(data_rows, columns=columns)
            df['station_name'] = file_info['station_id']
            df['file_type'] = 'ocean'
            
            # 清理数据
            df = self.clean_data(df, self.OCEAN_REAL_COLUMNS)
            
            # 生成观测时间
            observation_times = self.create_observation_time_vectorized(df)
            if observation_times is None:
                self.logger.warning(f"文件 {file_info['filename']} 时间转换失败，跳过")
                return None
            df['observation_time'] = observation_times
            df = df[df['observation_time'].notna()]
            
            if df.empty:
                self.logger.warning(f"文件 {file_info['filename']} 过滤后无有效数据")
                return None
            
            # 使用UTC时间（Python 3.12+ 推荐方式）
            current_time_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            df['first_crawl_time'] = current_time_utc
            df['update_time'] = current_time_utc
            df['create_time'] = current_time_utc  # 新增：明确设置入库时间
            df['update_count'] = 1
            
            return df
            
        except Exception as e:
            self.logger.error(f"解析OCEAN文件 {file_info['filename']} 时出错: {e}")
            return None
    
    def clean_data(self, df, real_columns):
        """数据清理"""
        replace_dict = {
            'MM': None, 'NAN': None, 'nan': None, '': None, 'None': None
        }
        df = df.replace(replace_dict)
        
        # 对 REAL 类型字段，将 None 替换为 -999
        for col in real_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(-999)
        
        # DEPTH 字段也是 REAL 类型，需要单独处理（它是唯一约束的一部分）
        if 'DEPTH' in df.columns:
            df['DEPTH'] = pd.to_numeric(df['DEPTH'], errors='coerce').fillna(-999)
        
        return df
    
    def create_observation_time_vectorized(self, df):
        """使用向量化操作生成 observation_time"""
        try:
            yy = pd.to_numeric(df['YY'], errors='coerce')
            mm = pd.to_numeric(df['MM'], errors='coerce')
            dd = pd.to_numeric(df['DD'], errors='coerce')
            hh = pd.to_numeric(df['hh'], errors='coerce')
            minute = pd.to_numeric(df['mm'], errors='coerce')
            
            year = yy.apply(lambda y: 2000 + y if y < 100 else y)
            
            observation_time = pd.to_datetime({
                'year': year,
                'month': mm,
                'day': dd,
                'hour': hh,
                'minute': minute
            }, errors='coerce')
            
            return observation_time
            
        except Exception as e:
            self.logger.error(f"时间转换失败: {e}")
            return None
    
    def _get_partition_table_name(self, base_table, obs_time_str):
        """根据观测时间计算分表名称（单条数据，ocean_data 按月分表）"""
        if not self.enable_partitioning or not obs_time_str:
            return base_table
        
        try:
            # 快速类型判断和转换
            if isinstance(obs_time_str, pd.Timestamp):
                observation_time = obs_time_str
            elif isinstance(obs_time_str, str):
                observation_time = pd.Timestamp(obs_time_str)
            else:
                observation_time = obs_time_str
            
            if observation_time < self.partition_start_date:
                return base_table
            
            # 按月分表
            return f"{base_table}_{observation_time.year}{observation_time.month:02d}"
        
        except Exception as e:
            self.logger.warning(f"解析观测时间失败: {e}，使用原表 {base_table}")
            return base_table
    
    def _get_partition_table_names_vectorized(self, base_table, observation_times):
        """向量化计算分表名称（性能优化：批量处理，按月分表）"""
        if not self.enable_partitioning or observation_times.empty:
            return pd.Series([base_table] * len(observation_times), index=observation_times.index)
        
        try:
            times = pd.to_datetime(observation_times, errors='coerce')
            
            # 按月分表
            suffixes = times.dt.year.astype(str) + times.dt.month.apply(lambda x: f'{x:02d}')
            result = base_table + '_' + suffixes
            
            # 早于起始时间的使用原表
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
        
        # 快速检查：如果分表已在缓存中，直接返回
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
                
                # 使用 CREATE TABLE IF NOT EXISTS 减少并发冲突
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
                    
                    # 识别所有"已存在"类型的错误
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
    
    def scrape_station_info(self, station_name):
        """从NOAA网站爬取站点坐标"""
        try:
            # 随机选择User-Agent
            import random
            self.session.headers['User-Agent'] = random.choice(self.user_agents)
            
            url = f"https://www.ndbc.noaa.gov/station_page.php?station={station_name}"
            cache_buster = int(time.time() * 1000)
            url_with_cache_buster = f"{url}&_={cache_buster}"
            response = self.session.get(url_with_cache_buster, timeout=20)
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
    
    def insert_station(self, conn, station_name):
        """插入站点信息（使用序列自动生成 station_id）"""
        try:
            station_info = self.scrape_station_info(station_name)
            
            if station_info:
                latitude = station_info.get('latitude')
                longitude = station_info.get('longitude')
            else:
                latitude = None
                longitude = None
            
            cursor = conn.cursor()
            
            # 使用数据库序列自动生成 station_id
            insert_sql = """
                INSERT INTO stations (station_id, name, latitude, longitude)
                VALUES (nextval('stations_station_id_seq'), %s, %s, %s)
                ON CONFLICT (name) DO NOTHING
            """
            
            cursor.execute(insert_sql, (station_name, latitude, longitude))
            conn.commit()
            cursor.close()
            
            return True
        except Exception as e:
            conn.rollback()
            self.logger.warning(f"插入站点 {station_name} 失败: {e}")
            try:
                if 'cursor' in locals() and cursor:
                    cursor.close()
            except Exception:
                pass
            return False
    
    def save_to_database(self, conn, df, file_info):
        """
        将 ocean 数据保存到数据库
        """
        cursor = conn.cursor()
        
        try:
            # 预先检查站点是否存在
            station_name = file_info['station_id']
            with self.stations_lock:
                station_exists = station_name in self.existing_stations
            
            if not station_exists:
                try:
                    if self.insert_station(conn, station_name):
                        with self.stations_lock:
                            self.existing_stations.add(station_name)
                except Exception as e:
                    self.logger.debug(f"站点 {station_name} 可能已被其他线程插入: {e}")
            
            # 保存 ocean 数据（内部已处理commit和cursor关闭）
            processed_count = self._save_ocean_data(conn, cursor, df, file_info)
            
            # _save_ocean_data 内部已经commit和close cursor，这里不需要重复
            return processed_count
            
        except Exception as e:
            conn.rollback()
            self.logger.error(f"数据库保存失败: {e}")
            raise
        finally:
            try:
                if cursor:
                    cursor.close()
            except Exception:
                pass
    
    def _generate_ocean_upsert_sql(self, target_table):
        """生成 OCEAN 数据的 UPSERT SQL（支持动态表名）"""
        # 注意：列名中的 % 需要转义为 %%，因为 execute_values 会将 % 当作格式化占位符
        return f"""
            INSERT INTO {target_table} (
                station_name, observation_time,
                "DEPTH", "OTMP", "COND", "SAL", "O2%%", "O2PPM", 
                "CLCON", "TURB", "PH", "EH",
                first_crawl_time, update_time, update_count, create_time
            )
            VALUES %s
            ON CONFLICT (station_name, observation_time, "DEPTH")
            DO UPDATE SET
                "OTMP" = COALESCE(NULLIF(EXCLUDED."OTMP", -999), {target_table}."OTMP"),
                "COND" = COALESCE(NULLIF(EXCLUDED."COND", -999), {target_table}."COND"),
                "SAL" = COALESCE(NULLIF(EXCLUDED."SAL", -999), {target_table}."SAL"),
                "O2%%" = COALESCE(NULLIF(EXCLUDED."O2%%", -999), {target_table}."O2%%"),
                "O2PPM" = COALESCE(NULLIF(EXCLUDED."O2PPM", -999), {target_table}."O2PPM"),
                "CLCON" = COALESCE(NULLIF(EXCLUDED."CLCON", -999), {target_table}."CLCON"),
                "TURB" = COALESCE(NULLIF(EXCLUDED."TURB", -999), {target_table}."TURB"),
                "PH" = COALESCE(NULLIF(EXCLUDED."PH", -999), {target_table}."PH"),
                "EH" = COALESCE(NULLIF(EXCLUDED."EH", -999), {target_table}."EH"),
                update_time = EXCLUDED.update_time,
                update_count = {target_table}.update_count + 1
            WHERE
                -- 只要有任意字段发生变化，就触发更新（只对最近2天的数据）
                ({target_table}."OTMP" IS DISTINCT FROM COALESCE(NULLIF(EXCLUDED."OTMP", -999), {target_table}."OTMP")
                OR {target_table}."COND" IS DISTINCT FROM COALESCE(NULLIF(EXCLUDED."COND", -999), {target_table}."COND")
                OR {target_table}."SAL" IS DISTINCT FROM COALESCE(NULLIF(EXCLUDED."SAL", -999), {target_table}."SAL")
                OR {target_table}."O2%%" IS DISTINCT FROM COALESCE(NULLIF(EXCLUDED."O2%%", -999), {target_table}."O2%%")
                OR {target_table}."O2PPM" IS DISTINCT FROM COALESCE(NULLIF(EXCLUDED."O2PPM", -999), {target_table}."O2PPM")
                OR {target_table}."CLCON" IS DISTINCT FROM COALESCE(NULLIF(EXCLUDED."CLCON", -999), {target_table}."CLCON")
                OR {target_table}."TURB" IS DISTINCT FROM COALESCE(NULLIF(EXCLUDED."TURB", -999), {target_table}."TURB")
                OR {target_table}."PH" IS DISTINCT FROM COALESCE(NULLIF(EXCLUDED."PH", -999), {target_table}."PH")
                OR {target_table}."EH" IS DISTINCT FROM COALESCE(NULLIF(EXCLUDED."EH", -999), {target_table}."EH"))
                AND {target_table}.observation_time >= NOW() - INTERVAL '2 days'
        """
    
    def _save_ocean_data(self, conn, cursor, df, file_info):
        """保存OCEAN数据到ocean_data表（支持分表，带内存管理）"""
        base_table = self.base_ocean_table
        
        # 将数据按分表分组（性能优化：使用向量化计算）
        if self.enable_partitioning and not df.empty and 'observation_time' in df.columns:
            # 向量化计算目标分表（比 apply() 快 10-50 倍）
            df['_target_table'] = self._get_partition_table_names_vectorized(
                base_table, df['observation_time']
            )
            table_groups = df.groupby('_target_table')
        else:
            df['_target_table'] = base_table
            table_groups = [(base_table, df)]
        
        template = """(
            %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s
        )"""
        
        columns = [
            'station_name', 'observation_time',
            'DEPTH', 'OTMP', 'COND', 'SAL', 'O2%', 'O2PPM',
            'CLCON', 'TURB', 'PH', 'EH',
            'first_crawl_time', 'update_time', 'update_count', 'create_time'
        ]
        
        # 确保所有列存在
        for col in columns:
            if col not in df.columns:
                if col in self.OCEAN_REAL_COLUMNS or col == 'DEPTH':
                    df[col] = -999.0  # DEPTH 也是 REAL 类型
                else:
                    df[col] = None
        
        # 对每个分表分别执行批量插入
        total_processed = 0
        batch_count = 0
        
        try:
            for target_table, group_df in table_groups:
                # 如果是分表，确保分表已创建
                if target_table != base_table:
                    self._create_partition_table_if_not_exists(conn, target_table, base_table)
                    # 创建分表可能调用rollback，导致cursor关闭，需要重新创建
                    if cursor.closed:
                        cursor = conn.cursor()
                
                # 生成 UPSERT SQL（使用动态表名）
                upsert_sql = self._generate_ocean_upsert_sql(target_table)
                
                # 转换为列表
                data = group_df[columns].values.tolist()
                
                if not data:
                    continue
                
                # 分批执行
                for i in range(0, len(data), self.batch_size):
                    batch = data[i:i + self.batch_size]
                    execute_values(
                        cursor, 
                        upsert_sql, 
                        batch,
                        template=template,
                        page_size=self.batch_size
                    )
                    batch_count += 1
                    conn.commit()
                    
                    # 更频繁地清理游标/GC
                    if self.batch_cleanup_interval > 0 and batch_count % self.batch_cleanup_interval == 0:
                        if cursor and not cursor.closed:
                            cursor.close()
                        cursor = conn.cursor()
                        gc.collect()
                
                total_processed += len(data)
            
            return total_processed
        finally:
            try:
                if cursor and not cursor.closed:
                    cursor.close()
            except Exception:
                pass
    
    def process_single_file(self, file_info, stats):
        """处理单个文件（线程安全 + 内存管理 + 连接重试）"""
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
                    stats['ocean_files'] += 1
                    current_count = stats['successful_imports']
                
                # 减少日志输出频率
                if processed_count > 0 and current_count % self.log_every_n_files == 0:
                    self.logger.info(f"  ✓ {file_info['filename']}: 已处理 {processed_count} 条记录 | 已完成 {current_count} 个文件")
                
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
                with self.stats_lock:
                    stats['failed_imports'] += 1
                    if len(stats['errors']) < 1000:
                        stats['errors'].append(f"{file_info['filename']}: {error_msg}")
                return False, error_msg
                
        except Exception as e:
            with self.stats_lock:
                stats['failed_imports'] += 1
                if len(stats['errors']) < 1000:
                    stats['errors'].append(f"{file_info['filename']}: {str(e)}")
            return False, str(e)
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
            self.logger.info(f"🧪 测试模式 - 开始爬取 OCEAN 数据并入库 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            self.logger.info(f"测试站点: {', '.join(self.test_stations)}")
        else:
            self.logger.info(f"开始爬取 OCEAN 数据并入库 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"{'='*70}")
        
        # 获取文件链接
        files = self.fetch_file_links()
        if not files:
            self.logger.warning("没有找到文件")
            return None
        
        self.stats['total_files'] = len(files)
        
        # 连接数据库
        conn = None
        try:
            conn = self.connect_db()
            self.logger.info(f"✓ 数据库连接成功")
            self.load_existing_stations(conn)
            # 性能优化：预加载已存在的分表到缓存
            if self.enable_partitioning:
                self.load_existing_partitions(conn)
        except Exception as e:
            self.logger.error(f"数据库连接失败，程序终止: {e}")
            return None
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
        
        # 多线程处理
        self.logger.info(f"开始多线程处理 (线程数: {self.max_workers})")
        
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self.process_single_file, file_info, self.stats): file_info 
                for file_info in files
            }
            
            completed = 0
            for future in as_completed(futures):
                completed += 1
                if completed % 50 == 0:
                    self.logger.info(f"⏳ 进度: {completed}/{len(files)}")
            
            # 删除futures列表释放内存
            del futures
        
        elapsed_time = time.time() - start_time
        self.print_summary(elapsed_time)
        
        return self.stats
    
    def print_summary(self, elapsed_time):
        self.logger.info(f"\n{'='*70}")
        self.logger.info(f"爬取完成")
        self.logger.info(f"{'='*70}")
        self.logger.info(f"总文件数: {self.stats['total_files']}")
        self.logger.info(f"OCEAN文件: {self.stats['ocean_files']}")
        self.logger.info(f"成功导入: {self.stats['successful_imports']}")
        self.logger.info(f"失败数量: {self.stats['failed_imports']}")
        self.logger.info(f"处理记录数: {self.stats['processed_records']} 条")
        self.logger.info(f"总用时: {elapsed_time:.2f} 秒")
        
        if self.stats['errors']:
            self.logger.info(f"\n错误列表 (前10个):")
            for error in self.stats['errors'][:10]:
                self.logger.info(f"  - {error}")
    
    def set_update_interval(self, minutes):
        """设置更新间隔时间"""
        if minutes < 2:
            print(f"⚠️  更新间隔不能少于2分钟，已自动设置为2分钟")
            self.update_interval = 120
        else:
            self.update_interval = minutes * 60
            print(f"✓ 更新间隔已设置为: {minutes} 分钟")
    
    def run_auto(self, interval_minutes=30):
        """自动定时运行"""
        self.logger.info(f"\n{'='*70}")
        if self.test_mode:
            self.logger.info(f"🧪 测试定时模式启动 - OCEAN 数据")
            self.logger.info(f"测试站点: {', '.join(self.test_stations)}")
        else:
            self.logger.info(f"自动定时模式启动 - OCEAN 数据")
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
                
                # 重置统计
                self.stats = {
                    'total_files': 0,
                    'successful_imports': 0,
                    'failed_imports': 0,
                    'processed_records': 0,
                    'ocean_files': 0,
                    'errors': []
                }
                
                # 运行爬取
                self.run_once()
                
                # 等待
                next_run = datetime.now() + timedelta(minutes=interval_minutes)
                self.logger.info(f"\n下次运行时间: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
                time.sleep(interval_minutes * 60)
                
        except KeyboardInterrupt:
            self.logger.info(f"\n\n{'='*70}")
            self.logger.info(f"用户停止程序")
            self.logger.info(f"总运行次数: {run_count}")
            self.logger.info(f"{'='*70}")


def main():
    print("=" * 70)
    print("  NOAA实时海洋数据入库爬虫 v9.3")
    print("  专注于 OCEAN 数据采集与入库")
    print("=" * 70)
    
    try:
        scraper = NOAARealtimeToDatabase()
        
        print("\n" + "=" * 70)
        print(f"  当前配置:")
        print(f"  - 数据类型: OCEAN (海洋数据)")
        print(f"  - 线程数: {scraper.max_workers}")
        print(f"  - 请求超时: {scraper.request_timeout} 秒")
        print(f"  - 重试次数: {scraper.max_retries} 次")
        print(f"  - 请求延迟: {scraper.request_delay} 秒")
        print(f"  - 测试站点: {', '.join(scraper.test_stations)}")
        print("=" * 70)
        print("\n请选择运行模式：")
        print("  1. 运行一次（全部站点）")
        print("  2. 🧪 测试模式（只爬取测试站点一次）")
        print("  3. ⏰ 测试定时模式（2分钟间隔自动采集测试站点）")
        print("  4. 自动定时运行（30分钟间隔，全部站点）")
        print("  5. 自定义间隔自动运行（全部站点，最少2分钟）")
        print("  0. 退出")
        print("-" * 70)
        
        choice = input("请输入选项 (0-5): ").strip()
        
        if choice == '1':
            print("\n✓ 选择: 运行一次（全部站点）")
            scraper.run_once()
            
        elif choice == '2':
            print("\n✓ 选择: 测试模式（单次）")
            scraper.test_mode = True
            print(f"只爬取测试站点: {', '.join(scraper.test_stations)}")
            scraper.run_once()
            
        elif choice == '3':
            print("\n✓ 选择: 测试定时模式")
            scraper.test_mode = True
            print(f"只爬取测试站点: {', '.join(scraper.test_stations)}")
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




