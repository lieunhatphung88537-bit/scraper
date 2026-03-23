# -*- coding: utf-8 -*-
"""
OGIMET SYNOP 数据爬虫
抓取各国家/站点的 SYNOP 报文，进行基础解码后以 JSON 保存
"""
import os
import re
import json
import time
import threading
import requests
import gc
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeoutError
from bs4 import BeautifulSoup
from pymetdecoder import synop as pymet_synop
import io
import contextlib
import signal
from functools import wraps

try:
    import schedule
    SCHEDULE_AVAILABLE = True
except ImportError:
    SCHEDULE_AVAILABLE = False
    print("安装命令: pip install schedule")

# 导入数据库插入模块
try:
    import sys
    sys.path.insert(0, os.path.dirname(__file__))
    from db_inserter_v2 import DatabaseInserter
    DB_INSERTER_AVAILABLE = True
except ImportError:
    DB_INSERTER_AVAILABLE = False
    print("警告: db_inserter_v2模块未找到，数据库功能将不可用")

# 超时装饰器（用于Windows系统）
def timeout_decorator(seconds):
    """超时装饰器，使用线程池实现超时控制（Windows兼容）"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(func, *args, **kwargs)
                try:
                    return future.result(timeout=seconds)
                except FutureTimeoutError:
                    raise TimeoutError(f"函数 {func.__name__} 执行超时（{seconds}秒）")
        return wrapper
    return decorator

class SynopScraper:
    def __init__(self,
                 base_url: str = "https://ogimet.com",
                 delay: float = 1.0,
                 save_format: str = "db",
                 max_workers: int = 10,
                 use_database: bool = False):
        self.base_url = base_url.rstrip("/")
        self.synop_url = f"{self.base_url}/usynops.phtml"
        self.delay = delay
        self.save_format = save_format.lower()
        if self.save_format not in ("json", "db"):
            self.save_format = "db"  # 默认只入库，不保存本地文件
        self.max_workers = max_workers

        # User-Agent轮换列表（模拟不同浏览器）
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": self.user_agents[0],  # 默认使用第一个
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0"
        })
        
        # 配置连接池以支持高并发（防止 "Connection pool is full" 警告）
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=max(30, max_workers * 2),  # 连接池数量
            pool_maxsize=max(50, max_workers * 3),      # 最大连接数
            max_retries=2,                               # 重试次数
            pool_block=False                             # 不阻塞
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        
        self.base_path = "data"
        self.synop_root = "synop_test"  
        os.makedirs(self.synop_root, exist_ok=True)
        self.file_lock = threading.Lock()
        self.log_lock = threading.Lock()
        self.stats_lock = threading.Lock() 
        self.print_lock = threading.Lock()  
        self.log_dir = "logs"
        os.makedirs(self.log_dir, exist_ok=True)
        self.log_file = os.path.join(self.log_dir, "synop_scrape.log")
        self.stats = {
            "total_lines": 0,
            "decoded": 0,
            "failed": 0,
            "start_time": None,
            "end_time": None,
        }
        self.synop_decoder = pymet_synop.SYNOP()
        self.station_info_cache = {} 
        self.country_list_cache = None  # 缓存国家列表
        self.session_lock = threading.Lock()  # Session 操作锁
        self.max_data_days = None 
        self.enable_auto_clean = False  
        self.update_stats = {
            "new": 0,
            "updated": 0,
            "skipped": 0,
            "cleaned": 0
        }
        self.last_memory_clean = time.time()  # 上次内存清理时间
        self.memory_clean_interval = 3600  # 内存清理间隔（秒），默认1小时  
        
        # 数据库配置
        self.use_database = use_database and DB_INSERTER_AVAILABLE
        self.db_inserter = None
        
        if self.use_database:
            try:
                self.db_inserter = DatabaseInserter(table_prefix='ogimet')
                print(f"✓ 数据库入库功能已启用（SYNOP专用表）")
            except Exception as e:
                print(f"✗ 数据库连接失败: {e}")
                self.use_database = False
    
    def __enter__(self):
        """上下文管理器入口"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出，确保资源释放"""
        self.close()
        return False
    
    def close(self):
        """关闭session和数据库连接，释放资源"""
        try:
            if hasattr(self, 'session') and self.session:
                self.session.close()
                self.session = None
                self.log("HTTP Session已关闭")
        except Exception as e:
            print(f"关闭Session时出错: {e}")
        
        try:
            if hasattr(self, 'db_inserter') and self.db_inserter:
                # 完整关闭数据库插入器
                if hasattr(self.db_inserter, 'close'):
                    self.db_inserter.close()
                elif hasattr(self.db_inserter, 'conn') and self.db_inserter.conn:
                    self.db_inserter.conn.close()
                # 删除对象引用
                self.db_inserter = None
                self.log("数据库连接已完全释放")
        except Exception as e:
            print(f"关闭数据库连接时出错: {e}")
        
        # 清理所有缓存
        try:
            if hasattr(self, 'station_info_cache'):
                self.station_info_cache.clear()
        except Exception as e:
            print(f"清理缓存时出错: {e}")
        
        # 强制垃圾回收
        import gc
        gc.collect()

    def parse_coordinate(self, coord_str: str, coord_type: str = 'lat') -> float:
        """
        解析坐标字符串为十进制度数（增强版，支持多种格式）
        支持格式：
        - 度-分-秒：52-58-14N, 122°31'49"E
        - 度-分：38-32N, 095-15.10W
        - 纯度数：38N, 122E
        """
        try:
            coord_str = coord_str.strip().upper()
            if not coord_str:
                return 0.0
            
            # 提取方向标识（最后一个字符）
            direction = coord_str[-1]
            if direction not in ['N', 'S', 'E', 'W']:
                return 0.0
            coord_str = coord_str[:-1]
            
            # 分割度分秒（支持多种分隔符：-、°、'、"）
            parts = re.split(r"[-°'\"]+", coord_str.strip())
            parts = [p for p in parts if p]  # 过滤空字符串
            
            if len(parts) >= 3:
                # 度-分-秒格式（如 38-32-20 或 095-15.10）
                degrees = float(parts[0])
                minutes = float(parts[1])
                seconds = float(parts[2])
                decimal = degrees + minutes/60.0 + seconds/3600.0
            elif len(parts) == 2:
                # 度-分格式（如 38-32 或 095-15.10）
                degrees = float(parts[0])
                minutes = float(parts[1])
                decimal = degrees + minutes/60.0
            elif len(parts) == 1:
                # 只有度
                decimal = float(parts[0])
            else:
                return 0.0
            
            # 处理方向（南纬和西经为负）
            if direction in ['S', 'W']:
                decimal = -decimal
            
            return round(decimal, 6)
        except Exception:
            return 0.0
    
    def _parse_html_timestamp(self, timestamp_str: str) -> str:
        """
        解析HTML时间戳格式
        输入格式：'17/11/2025 06:00->' 或 '17/11/2025 06:00'
        输出格式：'2025-11-17 06:00:00'
        """
        if not timestamp_str:
            return ""
        try:
            timestamp_str = timestamp_str.replace('->', '').strip()
            dt = datetime.strptime(timestamp_str, '%d/%m/%Y %H:%M')
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            return timestamp_str  

    def get_country_list(self, force_refresh: bool = False) -> List[Dict[str, str]]:
        """在 usynops 页面获取全部国家列表（带缓存）
        
        Args:
            force_refresh: 是否强制刷新缓存
        """
        # 使用缓存
        if not force_refresh and self.country_list_cache is not None:
            return self.country_list_cache
        
        print("正在获取国家列表...")
        params = {'_': int(datetime.now().timestamp() * 1000)}
        response = None
        soup = None
        try:
            # 随机选择User-Agent
            import random
            self.session.headers['User-Agent'] = random.choice(self.user_agents)
            
            response = self.session.get(self.synop_url, params=params, timeout=60)
            response.raise_for_status()
            html_text = response.text
            soup = BeautifulSoup(html_text, "html.parser")
            select = soup.find("select", {"name": "estado"})
            if not select:
                return []
            countries = []
            for option in select.find_all("option"):
                code = option.get("value", "").strip()
                name = option.text.strip()
                if code and name:
                    countries.append({"code": code, "name": name})
            self.country_list_cache = countries  # 缓存结果
            return countries
        finally:
            # 内存释放
            if soup:
                soup.decompose()
                del soup
            if response:
                response.close()
                del response
            if 'html_text' in locals():
                del html_text

    def get_country_stations(self, country_code: str) -> List[Dict[str, str]]:
        """获取指定国家的所有站点列表"""
        data_url = f"{self.base_url}/ultimos_synops2.php"
        params = {
            "lang": "en",
            "estado": country_code,
            '_': int(datetime.now().timestamp() * 1000),
        }
        response = None
        soup = None
        try:
            # 随机选择User-Agent
            import random
            self.session.headers['User-Agent'] = random.choice(self.user_agents)
            
            response = self.session.get(data_url, params=params, timeout=60)
            response.encoding = 'latin1'
            response.raise_for_status()
            html_text = response.text
            soup = BeautifulSoup(html_text, "html.parser")
            
            stations = []
            for row in soup.find_all("tr"):
                cells = row.find_all("td")
                if len(cells) < 4:
                    continue
                
                station_cell = cells[0]
                station_link = station_cell.find("a")
                if not station_link:
                    continue
                
                href = station_link.get("href", "")
                station_id_match = re.search(r'lugar=(\d+)', href)
                if not station_id_match:
                    continue
                
                station_id = station_id_match.group(1)
                station_name = station_link.text.strip()
                
                stations.append({
                    "station_id": station_id,
                    "station_name": station_name
                })
            
            return stations
        finally:
            # 内存释放
            if soup:
                soup.decompose()
                del soup
            if response:
                response.close()
                del response
            if 'html_text' in locals():
                del html_text
    
    def get_station_info(self, station_id: str, station_name: str, country_name: Optional[str] = None) -> Dict:
        """获取站点详细信息（经纬度、海拔、WMO等）- 使用缓存机制"""
        # 检查缓存
        if station_id in self.station_info_cache:
            return self.station_info_cache[station_id]
        
        detail_url = f"{self.base_url}/display_synops2.php"
        params = {
            "lang": "en",
            "lugar": station_id,
            "tipo": "ALL",
            "ord": "REV",
            "nil": "SI",
            "fmt": "html",
            '_': int(datetime.now().timestamp() * 1000),
        }
        
        response = None
        soup = None
        try:
            # 随机选择User-Agent
            import random
            self.session.headers['User-Agent'] = random.choice(self.user_agents)
            
            response = self.session.get(detail_url, params=params, timeout=30)
            response.encoding = 'latin1'
            response.raise_for_status()
            html_text = response.text
            soup = BeautifulSoup(html_text, "html.parser")
            
            station_info = {
                'station_id': station_id,
                'station_name': station_name,
                'icao_code': '',
                'wmo_index': '',
                'wigos_id': '',
                'latitude': 0.0,
                'longitude': 0.0,
                'altitude_m': None,
                'country': country_name or ''
            }
            
            # 查找站点信息表格
            info_table = soup.find("table", {"class": "border_brown"})
            if info_table:
                info_text = info_table.get_text()
                
                # 提取ICAO代码（排除"----"）
                icao_match = re.search(r'Indicativo OACI[:\s]+([A-Z0-9]{4})', info_text, re.IGNORECASE)
                if icao_match:
                    icao = icao_match.group(1).strip()
                    if icao != "----":
                        station_info["icao_code"] = icao
                
                # 提取WMO索引
                wmo_match = re.search(r'WMO index[:\s]+(\d+)', info_text, re.IGNORECASE)
                if wmo_match:
                    station_info['wmo_index'] = wmo_match.group(1)
                elif station_id.isdigit():
                    station_info['wmo_index'] = station_id
                
                # 提取WIGOS ID
                wigos_match = re.search(r'WIGOS Id[:\s]+([0-9\-]+)', info_text, re.IGNORECASE)
                if wigos_match:
                    station_info['wigos_id'] = wigos_match.group(1)
                
                # 提取纬度（支持多种格式）
                lat_match = re.search(r'Latitud[ei]?[^:]*:?\s+([0-9\-°\'\".]+[NS])', info_text, re.IGNORECASE)
                if lat_match:
                    station_info["latitude"] = self.parse_coordinate(lat_match.group(1), 'lat')
                
                # 提取经度（支持多种格式）
                lon_match = re.search(r'Longitud[ei]?[^:]*:?\s+([0-9\-°\'\".]+[EW])', info_text, re.IGNORECASE)
                if lon_match:
                    station_info["longitude"] = self.parse_coordinate(lon_match.group(1), 'lon')
                
                # 提取海拔
                alt_match = re.search(r'Altitud[ei]?[^:]*:?\s+([\d]+)', info_text, re.IGNORECASE)
                if alt_match:
                    station_info["altitude_m"] = int(alt_match.group(1))
            
            # 缓存结果
            self.station_info_cache[station_id] = station_info
            return station_info
            
        except Exception as e:
            # 异常时返回默认值（而非None）
            default_info = {
                'station_id': station_id,
                'station_name': station_name,
                'icao_code': '',
                'wmo_index': station_id if station_id.isdigit() else '',
                'wigos_id': '',
                'latitude': 0.0,
                'longitude': 0.0,
                'altitude_m': None,
                'country': country_name or ''
            }
            self.station_info_cache[station_id] = default_info
            return default_info
        finally:
            # 内存释放
            if soup:
                soup.decompose()
                del soup
            if response:
                response.close()
                del response
            if 'html_text' in locals():
                del html_text

    def fetch_station_synops(self, station_id: str, station_name: str) -> List[Dict[str, str]]:
        """
        获取单个站点的所有可用SYNOP数据（无时间限制，服务器返回多少获取多少）
        
        Args:
            station_id: 站点ID
            station_name: 站点名称
        
        Note:
            不设置时间参数，由OGIMET服务器决定返回多少历史数据
            通常服务器会返回最近几天到几周的数据
        """
        detail_url = f"{self.base_url}/display_synops2.php"
        
        params = {
            "lang": "en",
            "lugar": station_id,
            "tipo": "ALL",
            "ord": "REV",
            "nil": "SI",
            "fmt": "html",
            '_': int(datetime.now().timestamp() * 1000),
        }
        
        response = None
        soup = None
        try:
            # 随机选择User-Agent
            import random
            self.session.headers['User-Agent'] = random.choice(self.user_agents)
            
            response = self.session.get(detail_url, params=params, timeout=60)
            response.encoding = 'latin1'
            response.raise_for_status()
            html_text = response.text
            soup = BeautifulSoup(html_text, "html.parser")
            
            synop_records = []
            for row in soup.find_all("tr"):
                cells = row.find_all("td")
                if len(cells) < 4:
                    continue
                timestamp = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                header_cell = cells[2]
                header_pres = header_cell.find_all("pre")
                header_text = ""
                for pre in header_pres:
                    text = pre.text.strip()
                    if text:
                        header_text += text + " "
                header_text = header_text.strip()
                synop_cell = cells[3]
                synop_pres = synop_cell.find_all("pre")
                if not synop_pres:
                    continue
                data_text = ""
                for pre in synop_pres:
                    text = pre.text.strip()
                    if text:
                        data_text += text + " "
                data_text = data_text.strip()
                if header_text:
                    synop_text = header_text + " " + data_text
                else:
                    synop_text = data_text
                
                synop_text = synop_text.strip()
                
                if synop_text and any(c.isdigit() for c in synop_text):
                    synop_records.append({
                        "station_id": station_id,
                        "station_name": station_name,
                        "synop_text": synop_text,
                        "html_timestamp": timestamp  
                    })
            
            return synop_records
        except Exception as e:
            self.log(f"获取站点 {station_id} 数据失败: {e}")
            return []
        finally:
            if soup:
                soup.decompose()
                del soup
            if response:
                response.close()
                del response
            if 'html_text' in locals():
                del html_text

    def _decode_synop_core(self, cleaned_text: str, synop_text: str) -> Optional[Dict]:
        """核心解码逻辑（可能耗时的部分，需要超时控制）"""
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
            decoded = self.synop_decoder.decode(cleaned_text)
        return decoded

    def decode_synop(self, synop_text: str) -> Optional[Dict]:
        """基于 WMO FM-12 的主要编码段，进行简单解码（带超时保护）"""
        tokens = synop_text.split()
        if not tokens:
            return None

        if re.search(r'\bNIL\b', synop_text, re.IGNORECASE):
            return None
        cleaned_text = re.sub(r'=+\s*$', '', synop_text)
        cleaned_text = re.sub(r'([0-9/]+)==', r'\1', cleaned_text)

        record: Dict[str, Optional[object]] = {
            "raw_synop": synop_text,
            "report_type": "",
            "observation_time": "",
            "station_code": "",
            "station_name": "",
            "wind_indicator": "",
            "temperature_c": None,
            "dewpoint_c": None,
            "station_pressure_hpa": None,
            "sea_level_pressure_hpa": None,
            "pressure_tendency_code": None,
            "pressure_tendency_hpa": None,
            "precipitation_mm": None,
            "precipitation_hours": None,
            "visibility_m": None,
            "present_weather_code": "",
            "past_weather_code": "",
            "present_weather_desc": "",
            "total_cloud_amount_oktas": None,
            "low_cloud_type": "",
            "mid_cloud_type": "",
            "high_cloud_type": "",
            "wind_direction_deg": None,
            "wind_speed_mps": None,
        }

        def _decode_with_fallback(text: str, allow_retry: bool = True):
            """调用pymetdecoder解码，并在遇到无效分组时尝试截断尾部重试一次。"""
            try:
                # 使用超时控制（30秒），防止某些报文导致解码卡死
                @timeout_decorator(30)
                def decode_with_timeout():
                    return self._decode_synop_core(text, synop_text)
                
                return decode_with_timeout()

            except TimeoutError:
                error_msg = f"解码超时（30秒）: {synop_text[:100]}..."
                self.log(error_msg)
                return None
            except Exception as e:
                # 针对 “XXX is not a valid group for synop” 这类错误，做一次宽容重试
                msg = str(e)
                
                # 特殊处理：区域规则警告（如"Ground state not measured in region V")
                # 这类错误不影响核心气象要素，但pymetdecoder会抛异常，我们记录后继续按原流程处理
                if "not measured in region" in msg.lower():
                    self.log(f"区域规则警告（已忽略）: {str(e)[:80]}")
                    # 对于区域规则类警告，静默丢弃该报文，不再输出冗长错误信息
                    return None
                
                if allow_retry and ("not a valid group for synop" in msg or "Unable to decode group" in msg):
                    # 提取无效分组（通常为 5 位数字/字母或含 / 的组）
                    # 支持两种错误格式：1) "XXX is not a valid group" 2) "Unable to decode group XXX"
                    m1 = re.search(r"([0-9A-Z/]{1,5}) is not a valid group for synop", msg)
                    m2 = re.search(r"Unable to decode group ([0-9A-Z/]{1,5}) for synop", msg)
                    bad_group = (m1.group(1) if m1 else None) or (m2.group(1) if m2 else None)
                    tokens = text.split()
                    if bad_group and bad_group in tokens:
                        idx = tokens.index(bad_group)
                        # 只截断尾部，保留前面的标准段落
                        if idx > 0:
                            truncated = " ".join(tokens[:idx])
                            warn_msg = f"检测到无效 SYNOP 分组 {bad_group}，已截断尾部并重试解码"
                            self.log(warn_msg)
                            try:
                                return _decode_with_fallback(truncated, allow_retry=False)
                            except Exception:
                                # 重试仍失败则回落到原始错误处理
                                pass

                # 针对 "Unexpected precipitation group found in section 3"，截断第3段后重试
                if allow_retry and "Unexpected precipitation group" in msg:
                    tokens = text.split()
                    if "333" in tokens:
                        idx = tokens.index("333")
                        if idx > 0:
                            truncated = " ".join(tokens[:idx])
                            warn_msg = "检测到SYNOP第3段降水分组异常，已截断第3段并重试解码"
                            self.log(warn_msg)
                            try:
                                return _decode_with_fallback(truncated, allow_retry=False)
                            except Exception:
                                pass

                error_msg = f"pymetdecoder decode error: {e} for synop: {synop_text[:100]}..."
                self.log(error_msg)
                return None

        decoded = _decode_with_fallback(cleaned_text)
        if decoded is None:
            return None

        station_type = decoded.get("station_type", {})
        if isinstance(station_type, dict):
            record["report_type"] = station_type.get("value", "") or ""

        wind_indicator = decoded.get("wind_indicator", {})
        if isinstance(wind_indicator, dict):
            value = wind_indicator.get("value")
            record["wind_indicator"] = str(value) if value is not None else ""

        obs_time = decoded.get("obs_time", {})
        if isinstance(obs_time, dict):
            day = obs_time.get("day", {}).get("value")
            hour = obs_time.get("hour", {}).get("value")
            if isinstance(day, int) and isinstance(hour, int):
                record["observation_time"] = self._compose_datetime(day, hour)

        station_id = decoded.get("station_id", {})
        if isinstance(station_id, dict):
            value = station_id.get("value")
            if value:
                record["station_code"] = str(value)

        air_temperature = decoded.get("air_temperature", {})
        if isinstance(air_temperature, dict):
            value = air_temperature.get("value")
            if value is not None:
                record["temperature_c"] = float(value)

        dewpoint_temperature = decoded.get("dewpoint_temperature", {})
        if isinstance(dewpoint_temperature, dict):
            value = dewpoint_temperature.get("value")
            if value is not None:
                record["dewpoint_c"] = float(value)

        station_pressure = decoded.get("station_pressure", {})
        if isinstance(station_pressure, dict):
            value = station_pressure.get("value")
            if value is not None:
                record["station_pressure_hpa"] = float(value)

        sea_level_pressure = decoded.get("sea_level_pressure", {})
        if isinstance(sea_level_pressure, dict):
            value = sea_level_pressure.get("value")
            if value is not None:
                record["sea_level_pressure_hpa"] = float(value)

        pressure_tendency = decoded.get("pressure_tendency", {})
        tendency_code = None
        if isinstance(pressure_tendency, dict):
            tendency = pressure_tendency.get("tendency", {})
            if isinstance(tendency, dict):
                value = tendency.get("value")
                if value is not None:
                    record["pressure_tendency_code"] = str(value)
                    tendency_code = int(value)
            change = pressure_tendency.get("change", {})
            if isinstance(change, dict):
                value = change.get("value")
                if value is not None:
                    record["pressure_tendency_hpa"] = float(value)
        
        # 手动修正气压趋势符号：根据 WMO code table 0200，重新判断符号
        if tendency_code is not None and record["pressure_tendency_hpa"] is not None:
            a = tendency_code
            current_value = record["pressure_tendency_hpa"]
            
            if a in (0, 1, 2, 3):
                # 这些代码表示气压较 3 小时前更高，应为正值
                if current_value < 0:
                    record["pressure_tendency_hpa"] = abs(current_value)
            elif a == 4:
                # 稳定：视为 0
                record["pressure_tendency_hpa"] = 0.0
            elif a in (5, 6, 7, 8):
                # 这些代码表示气压较 3 小时前更低，应为负值
                if current_value > 0:
                    record["pressure_tendency_hpa"] = -abs(current_value)

        precipitation_s1 = decoded.get("precipitation_s1", {})
        if isinstance(precipitation_s1, dict):
            amount = precipitation_s1.get("amount", {})
            if isinstance(amount, dict):
                value = amount.get("value")
                if value is not None:
                    record["precipitation_mm"] = float(value)
            time_before = precipitation_s1.get("time_before_obs", {})
            if isinstance(time_before, dict):
                value = time_before.get("value")
                if value is not None:
                    record["precipitation_hours"] = int(value)

        visibility = decoded.get("visibility", {})
        if isinstance(visibility, dict):
            value = visibility.get("value")
            if value is not None:
                record["visibility_m"] = float(value)

        cloud_cover = decoded.get("cloud_cover", {})
        if isinstance(cloud_cover, dict):
            value = cloud_cover.get("value")
            if value is not None:
                record["total_cloud_amount_oktas"] = int(value)

        cloud_types = decoded.get("cloud_types", {})
        if isinstance(cloud_types, dict):
            low = cloud_types.get("low_cloud_type", {})
            if isinstance(low, dict):
                value = low.get("value")
                if value is not None:
                    record["low_cloud_type"] = str(value)
            mid = cloud_types.get("middle_cloud_type", {})
            if isinstance(mid, dict):
                value = mid.get("value")
                if value is not None:
                    record["mid_cloud_type"] = str(value)
            high = cloud_types.get("high_cloud_type", {})
            if isinstance(high, dict):
                value = high.get("value")
                if value is not None:
                    record["high_cloud_type"] = str(value)

        surface_wind = decoded.get("surface_wind", {})
        if isinstance(surface_wind, dict):
            direction = surface_wind.get("direction", {})
            if isinstance(direction, dict):
                value = direction.get("value")
                if value is not None:
                    record["wind_direction_deg"] = float(value)
            speed = surface_wind.get("speed", {})
            if isinstance(speed, dict):
                value = speed.get("value")
                unit = speed.get("unit")
                if value is not None:
                    v = float(value)
                    if unit == "KT":
                        v = v * 0.514444
                    record["wind_speed_mps"] = round(v, 2)

        for key in ("pressure_tendency_code", "present_weather_code", "past_weather_code", "icao_code"):
            if key in record:
                del record[key]

        return record

    def _compose_datetime(self, day: int, hour: int) -> str:
        """根据当月 UTC 时间推算 SYNOP 观测时间"""
        now = datetime.now(timezone.utc)
        candidates = []
        for delta_month in (0, -1, 1):
            year = now.year
            month = now.month + delta_month
            while month < 1:
                month += 12
                year -= 1
            while month > 12:
                month -= 12
                year += 1
            try:
                candidate = datetime(year, month, day, hour, tzinfo=timezone.utc)
                candidates.append(candidate)
            except ValueError:
                continue
        if not candidates:
            return ""
        best = min(candidates, key=lambda dt: abs(dt - now))
        return best.strftime("%Y-%m-%d %H:%M:%S")


    def get_record_key(self, record: Dict) -> str:
        """获取记录唯一键：站点ID + 观测时间"""
        station_code = record.get('station_code') or record.get('station_id') or 'UNKNOWN'
        obs_time = record.get('observation_time', '')
        return f"{station_code}_{obs_time}"

    def is_record_outdated(self, record: Dict) -> bool:
        obs_time_str = record.get('observation_time', '')
        if not obs_time_str:
            return True
        
        try:
            obs_time = datetime.strptime(obs_time_str, "%Y-%m-%d %H:%M:%S")
            obs_time = obs_time.replace(tzinfo=timezone.utc)
            cutoff_time = datetime.now(timezone.utc) - timedelta(days=self.max_data_days)
            return obs_time < cutoff_time
        except (ValueError, TypeError):
            return True

    def save_synop_record(self, country_name: str, record: Dict):
        """
        保存方法 - 支持两种模式：
        - save_format='db': 只入库，不保存本地文件（由数据库 ON CONFLICT 去重）
        - save_format='json': 保存本地JSON文件 + 入库
        """
        current_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        
        # 设置基本字段
        record["country"] = country_name
        record["crawl_time"] = current_time
        record["update_time"] = current_time
        record["update_count"] = 0  # 数据库会用 ON CONFLICT 处理实际的 update_count
        
        # save_format='db' 模式：直接返回记录供入库，不保存本地文件
        if self.save_format == 'db':
            with self.stats_lock:
                self.update_stats["new"] += 1
            record_copy = record.copy()
            return record_copy
        
        # save_format='json' 模式：保存本地JSON文件（原有逻辑）
        station = record.get("station_code") or record.get("station_id") or "UNKNOWN"
        safe_country = self._sanitize_filename(country_name)
        country_dir = os.path.join(self.synop_root, safe_country)
        os.makedirs(country_dir, exist_ok=True)
        file_path = os.path.join(country_dir, f"{station}.json")

        action_type = None
        should_save_db = False

        with self.file_lock:
            if os.path.exists(file_path):
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        existing = json.load(f)
                except (json.JSONDecodeError, IOError):
                    existing = []
            else:
                existing = []

            # 建立索引
            existing_map = {}
            start_index = max(0, len(existing) - 50)
            for i in range(start_index, len(existing)):
                rec = existing[i]
                key = self.get_record_key(rec)
                existing_map[key] = (i, rec)

            record_key = self.get_record_key(record)
            
            if record_key not in existing_map:
                record["first_crawl_time"] = current_time
                existing.append(record)
                action_type = 'new'
                should_save_db = True
                with self.stats_lock:
                    self.update_stats["new"] += 1
            else:
                idx, old_record = existing_map[record_key]
                if old_record.get("raw_synop") == record.get("raw_synop"):
                    action_type = 'skipped'
                    should_save_db = False
                    with self.stats_lock:
                        self.update_stats["skipped"] += 1
                else:
                    first_crawl = old_record.get("first_crawl_time") or old_record.get("crawl_time", current_time)
                    record["first_crawl_time"] = first_crawl
                    record["crawl_time"] = first_crawl
                    record["update_count"] = old_record.get("update_count", 0) + 1
                    existing[idx] = record
                    action_type = 'updated'
                    should_save_db = True
                    with self.stats_lock:
                        self.update_stats["updated"] += 1

            if action_type in ('new', 'updated'):
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(existing, f, ensure_ascii=False, indent=2)

        if should_save_db:
            record_copy = record.copy()
            record_copy["country"] = country_name
            if "crawl_time" not in record_copy and "first_crawl_time" in record_copy:
                record_copy["crawl_time"] = record_copy["first_crawl_time"]
            return record_copy
        return None

    def log(self, message: str):
        with self.log_lock:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            with open(self.log_file, "a", encoding="utf-8") as f:
                f.write(f"[{timestamp}] {message}\n")

    def _sanitize_filename(self, name: str) -> str:
        return re.sub(r'[<>:"/\\|?*]', "_", name).strip(" .") or "UNKNOWN"

    def clean_memory(self, force: bool = False):
        """清理内存缓存和执行垃圾回收（增强版）
        
        Args:
            force: 是否强制清理（忽略时间间隔）
        """
        current_time = time.time()
        
        # 检查是否到了清理时间
        if not force and (current_time - self.last_memory_clean) < self.memory_clean_interval:
            return
        
        cleaned_items = 0
        
        # 清理站点信息缓存（保留最近的50个，减少内存占用）
        if len(self.station_info_cache) > 50:
            old_count = len(self.station_info_cache)
            cache_items = list(self.station_info_cache.items())
            self.station_info_cache = dict(cache_items[-50:])
            cleaned_items += old_count - 50
        
        # 强制清理时完全清空缓存
        if force:
            cleaned_items += len(self.station_info_cache)
            self.station_info_cache.clear()
            # 清空国家列表缓存（下次会重新获取）
            if self.country_list_cache:
                cleaned_items += len(self.country_list_cache)
                self.country_list_cache = None
        
        # 清理 requests session 缓存
        try:
            if hasattr(self.session, 'adapters'):
                for adapter in self.session.adapters.values():
                    if hasattr(adapter, 'close'):
                        adapter.close()
        except:
            pass
        
        # 执行多轮垃圾回收确保彻底清理
        collected = 0
        for generation in range(3):
            collected += gc.collect(generation)
        
        self.last_memory_clean = current_time
    
    def _perform_deep_cleanup(self):
        """执行深度内存清理（参考IEM_METAR策略）"""
        # 完全清空所有缓存
        self.station_info_cache.clear()
        self.country_list_cache = None
        
        # 清理 BeautifulSoup 缓存
        try:
            from bs4 import BeautifulSoup
            if hasattr(BeautifulSoup, 'reset'):
                BeautifulSoup.reset()
        except:
            pass
        
        # 清理 requests 连接池（线程安全）
        try:
            with self.session_lock:
                if self.session:
                    self.session.close()
                    # 重新创建 session
                    import random
                    self.session = requests.Session()
                    self.session.headers.update({
                        "User-Agent": random.choice(self.user_agents),
                        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                        "Cache-Control": "no-cache, no-store, must-revalidate",
                        "Pragma": "no-cache",
                        "Expires": "0"
                    })
        except:
            pass
        
        # 多次深度垃圾回收
        for _ in range(5):
            collected = gc.collect(2)
            if collected == 0:
                break

    # ------------------------------------------------------------------ #
    # 主流程
    # ------------------------------------------------------------------ #
    def scrape_all(self,
                   start_country_index: int = 0,
                   max_countries: Optional[int] = None,
                   use_multithreading: bool = True):

        countries = self.get_country_list()
        if not countries:
            print("❌ 无法获取国家列表")
            return

        if max_countries is not None:
            countries = countries[start_country_index:start_country_index + max_countries]
        else:
            countries = countries[start_country_index:]

        self.stats["start_time"] = datetime.now()
        total_countries = len(countries)
        print(f"\n开始爬取 SYNOP: {total_countries} 个国家")
        self.log(f"Start scraping {total_countries} countries")
        if use_multithreading:
            print(f"使用两级并发模式：国家级别 + 站点级别，最大线程数: {self.max_workers}")
            self.log(f"Using two-level concurrency: country-level + station-level, workers={self.max_workers}")

        # 国家级别的executor（并发处理国家）
        # 增加国家级并发数，充分利用多线程
        country_executor = ThreadPoolExecutor(max_workers=min(4, self.max_workers)) if use_multithreading else None
        country_futures = []

        try:
            if country_executor:
                # 国家级别并发
                for i, country in enumerate(countries, 1):
                    country_futures.append(
                        country_executor.submit(
                            self._process_country,
                            country, i, total_countries, use_multithreading
                        )
                    )
                
                # 等待所有国家处理完成
                for future in as_completed(country_futures):
                    try:
                        future.result()
                    except Exception as e:
                        print(f"  国家处理线程出错: {e}")
                        self.log(f"Country thread error: {e}")
            else:
                # 单线程模式
                for i, country in enumerate(countries, 1):
                    self._process_country(country, i, total_countries, False)
        finally:
            if country_executor:
                country_executor.shutdown(wait=True)
            
            # 关闭 Session 和数据库连接
            self.close()

        self.stats["end_time"] = datetime.now()
        elapsed = (self.stats["end_time"] - self.stats["start_time"]).total_seconds()
        print("\n" + "="*80)
        print("爬取完成")
        print("="*80)
        print(f"  报文数: {self.stats['total_lines']}")
        print(f"  成功解码: {self.stats['decoded']}")
        print(f"  失败: {self.stats['failed']}")
        print(f"  耗时: {elapsed:.1f} 秒")
        self.log(f"Scraping completed. Total: {self.stats['total_lines']}, Decoded: {self.stats['decoded']}, Failed: {self.stats['failed']}, Time: {elapsed:.1f}s")
        print(f"  新增记录: {self.update_stats['new']}")
        print(f"  更新记录: {self.update_stats['updated']}")
        print(f"  跳过重复: {self.update_stats['skipped']}")
        print(f"  清理过期: {self.update_stats['cleaned']}")
        self.log(f"Update stats - New: {self.update_stats['new']}, Updated: {self.update_stats['updated']}, Skipped: {self.update_stats['skipped']}, Cleaned: {self.update_stats['cleaned']}")

    def _process_country(self, country: Dict, country_index: int, total_countries: int, use_multithreading: bool):
        """处理单个国家的所有站点（用于国家级别多线程）"""
        country_name = country['name']
        country_code = country['code']
        
        print(f"\n[{country_index}/{total_countries}] 处理国家: {country_name} ({country_code})")
        self.log(f"[{country_index}/{total_countries}] Processing country: {country_name} ({country_code})")
        
        try:
            # 获取站点列表
            stations = self.get_country_stations(country_code)
            if not stations:
                print(f"  ⛔ 未找到站点")
                self.log(f"Country {country_name} has no stations")
                return
            
            print(f"  找到 {len(stations)} 个站点")
            # 移除站点数量的详细日志
            # self.log(f"Country {country_name} has {len(stations)} stations")
            
            # 预加载该国所有站点的existing_raw_set（性能优化：并行查询）
            country_existing_raw_set = set()
            if self.save_format == 'db' and self.use_database and self.db_inserter:
                try:
                    # 并行批量查询该国所有站点的已存在raw_text（比逐个查询快 5-10 倍）
                    station_ids = [s['station_id'] for s in stations]
                    with ThreadPoolExecutor(max_workers=min(10, len(station_ids))) as executor:
                        futures = {executor.submit(
                            self.db_inserter.get_existing_synop_raw_texts, sid, 7
                        ): sid for sid in station_ids}
                        for future in as_completed(futures):
                            try:
                                existing = future.result(timeout=10)
                                country_existing_raw_set.update(existing)
                            except Exception:
                                pass
                except Exception as e:
                    self.log(f"预加载existing_raw_set失败: {e}")
            
            # 收集该国所有站点的入库数据（批量入库）
            country_db_records = []
            country_db_lock = threading.Lock()
            
            # 站点级别的executor（并发处理站点）
            station_executor = ThreadPoolExecutor(max_workers=self.max_workers) if use_multithreading else None
            station_futures = []
            
            try:
                if station_executor:
                    # 站点级别并发
                    for j, station in enumerate(stations, 1):
                        if use_multithreading:
                            future = station_executor.submit(
                                self._process_station, station, country, j, len(stations),
                                country_existing_raw_set, country_db_records, country_db_lock
                            )
                            station_futures.append(future)
                        else:
                            self._process_station(station, country, j, len(stations),
                                                country_existing_raw_set, country_db_records, country_db_lock)
                else:
                    # 单线程模式
                    for j, station in enumerate(stations, 1):
                        self._process_station(station, country, j, len(stations),
                                            country_existing_raw_set, country_db_records, country_db_lock)
            finally:
                if station_executor:
                    station_executor.shutdown(wait=True)
            
            # 国家级别批量入库（大幅减少数据库提交次数）
            if self.use_database and country_db_records:
                try:
                    inserted = self.db_inserter.batch_insert_synop(country_db_records)
                    print(f"  ✓ {country_name} 处理完成，批量入库 {inserted} 条")
                except Exception as e:
                    print(f"  ⚠️ {country_name} 批量入库失败: {e}")
                    self.log(f"Country {country_name} batch insert failed: {e}")
            else:
                print(f"  ✓ {country_name} 处理完成")
            
            # 处理完成后清理内存
            del country_db_records
            del country_existing_raw_set
            del stations
            self.clean_memory()
            
            # 每处理5个国家执行一次深度清理
            if country_index % 5 == 0:
                self._perform_deep_cleanup()
            
        except Exception as e:
            print(f"  ❌ 处理国家失败: {e}")
            self.log(f"Error processing country {country_name} ({country_code}): {e}")
        finally:
            # 确保资源释放
            gc.collect()

    def _process_station(self, station: Dict, country: Dict, station_index: int, total_stations: int,
                         existing_raw_set: set, country_db_records: list, country_db_lock: threading.Lock):
        """处理单个站点（用于多线程）
        
        Args:
            existing_raw_set: 该国所有站点的已存在raw_text集合（预加载）
            country_db_records: 该国所有站点的入库数据收集列表
            country_db_lock: 保护country_db_records的锁
        """
        station_code = station['station_id']
        station_name = station['station_name']
        country_name = country['name']
        
        # 获取站点信息（使用缓存）
        station_info = self.get_station_info(station_code, station_name, country_name)
        
        # 获取SYNOP数据
        synop_records = self.fetch_station_synops(station_code, station_name)
        
        summary_parts = []
        db_records = []  # 收集需要入库的记录
        
        if synop_records:
            # db模式：先查询数据库中已存在的raw_text，跳过相同报文的解码
            saved_count = 0
            skipped_count = 0
            failed_count = 0
            
            for idx, synop_rec in enumerate(synop_records, 1):
                raw_text = synop_rec.get("synop_text", "")
                
                # 如果报文已存在于数据库，跳过解码
                if raw_text in existing_raw_set:
                    skipped_count += 1
                    with self.stats_lock:
                        self.stats["total_lines"] += 1
                        self.update_stats["skipped"] += 1
                    continue
                
                try:
                    record = self.decode_synop(raw_text)
                    
                    if record:
                        record["station_code"] = station_code
                        record["station_name"] = station_name
                        if station_info:
                            record["latitude"] = station_info.get("latitude")
                            record["longitude"] = station_info.get("longitude")
                            record["altitude_m"] = station_info.get("altitude_m")
                        
                        # 使用HTML时间戳
                        if synop_rec.get("html_timestamp"):
                            html_time = self._parse_html_timestamp(synop_rec["html_timestamp"])
                            if html_time:
                                record["observation_time"] = html_time
                        
                        # 保存并收集需要入库的记录
                        db_record = self.save_synop_record(country_name, record)
                        if db_record:
                            # 添加到国家级别的入库队列（线程安全）
                            with country_db_lock:
                                country_db_records.append(db_record)
                        saved_count += 1
                        
                        with self.stats_lock:
                            self.stats["decoded"] += 1
                            self.stats["total_lines"] += 1
                except Exception as exc:
                    failed_count += 1
                    with self.stats_lock:
                        self.stats["failed"] += 1
                    error_preview = raw_text[:100] if raw_text else "N/A"
                    error_msg = f"{country_name} {station_code} 第{idx}条 decode error: {exc} | 报文: {error_preview}..."
                    self.log(error_msg)
            
            if saved_count > 0:
                summary_parts.append(f"新增: {saved_count}")
            if skipped_count > 0:
                summary_parts.append(f"跳过: {skipped_count}")
            if failed_count > 0:
                summary_parts.append(f"失败: {failed_count}")
        
        # 移除正常站点的处理进度日志，只保留错误日志
        # if summary_parts:
        #     summary = ", ".join(summary_parts)
        #     with self.print_lock:
        #         print(f"  [{station_index}/{total_stations}] {station_name} ({station_code}): {summary}")
        
        # 移除sleep，请求间隔由fetch中的self.delay控制，多线程中不需要额外延迟
        # time.sleep(0.3) # 已移除

    # ------------------------------------------------------------------ #
    # 云天气映射
    # ------------------------------------------------------------------ #
    @staticmethod
    def _present_weather_map() -> Dict[str, str]:
        return {
            "00": "Cloud development not observed",
            "01": "Clouds generally dissolving",
            "02": "State of sky unchanged",
            "04": "Visibility reduced by smoke or haze",
            "05": "Haze",
            "10": "Light rain",
            "11": "Showers",
            "20": "Drizzle",
            "21": "Rain",
            "22": "Snow",
            "23": "Rain and snow",
            "24": "Drizzle and rain",
            "25": "Freezing rain",
            "26": "Freezing drizzle",
            "27": "Blowing snow",
            "30": "Fog",
            "40": "Rain shower",
            "50": "Rain",
            "60": "Snow",
            "70": "Thunderstorm",
            "80": "Rain shower",
            "81": "Violent shower",
            "82": "Thunderstorm with rain",
            "83": "Thunderstorm with snow",
            "84": "Thunderstorm with hail",
            "90": "Thunderstorm with dust/sand",
            "91": "Thunderstorm slight",
            "92": "Thunderstorm heavy",
        }

    @staticmethod
    def _low_cloud_map() -> Dict[str, str]:
        return {
            "/": "",
            "0": "No clouds",
            "1": "Cumulus humilis",
            "2": "Cumulus mediocris",
            "3": "Cumulonimbus",
            "4": "Stratocumulus",
            "5": "Stratus",
            "6": "Cumulus congestus",
            "7": "Stratocumulus with cumulus",
            "8": "Stratocumulus with cumulus with bases at different levels",
            "9": "Cumulonimbus with anvil",
        }

    @staticmethod
    def _mid_cloud_map() -> Dict[str, str]:
        return {
            "/": "",
            "0": "No medium clouds",
            "1": "Altostratus translucidus",
            "2": "Altostratus opacus",
            "3": "Altocumulus translucidus",
            "4": "Altocumulus opacus",
            "5": "Altocumulus with double layer",
            "6": "Altocumulus from cumuliform clouds",
            "7": "Altocumulus with turrets",
            "8": "Altocumulus castellanus",
            "9": "Altocumulus chaotic",
        }

    @staticmethod
    def _high_cloud_map() -> Dict[str, str]:
        return {
            "/": "",
            "0": "No high clouds",
            "1": "Cirrus filaments",
            "2": "Cirrus dense patches",
            "3": "Cirrus dense",
            "4": "Cirrus with cumuliform tufts",
            "5": "Cirrus (hooked)",
            "6": "Cirrus spreading",
            "7": "Cirrostratus covering part of sky",
            "8": "Cirrostratus covering most of sky",
            "9": "Cirrostratus covering entire sky",
        }


def run_scheduled_scrape(scraper, selected_countries=None, start_country_index=0, max_countries=None):
    """执行定时爬取任务"""
    start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"\n[{start_time}] 开始定时爬取任务...")
    scraper.log(f"Scheduled scraping task started at {start_time}")
    
    # 预热：首次加载国家列表（后续会使用缓存）
    if scraper.country_list_cache is None:
        scraper.get_country_list()
    
    try:
        scraper.scrape_all(
            start_country_index=start_country_index,
            max_countries=max_countries,
            use_multithreading=True
        )
        end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{end_time}] 定时爬取任务完成")
        scraper.log(f"Scheduled scraping task completed at {end_time}")
        
        # 任务完成后清理内存
        scraper.clean_memory(force=True)
        
    except Exception as e:
        error_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{error_time}] 定时爬取任务出错: {e}")
        scraper.log(f"Scheduled scraping task failed at {error_time}: {e}")
        
        # 即使出错也尝试清理内存
        try:
            scraper.clean_memory(force=True)
        except:
            pass


def main():
    # 性能优化配置：
    # - delay=0.3: 请求间隔缩短到 0.3 秒
    # - max_workers=10: 每个国家最多 10 个站点并发
    # - 国家级并发=2: 最多 2 个国家同时处理
    # - 最大总并发=2×10=15 个线程
    # - use_database=True: 启用实时数据库入库
    scraper = SynopScraper(delay=0.2, max_workers=15, use_database=True)
    print("=" * 80)
    print("OGIMET SYNOP 数据爬虫")
    print("=" * 80)
    print("\n请选择模式：")
    print("  1. 爬取所有国家")
    print("  2. 测试模式（前 3 个国家）")
    print("  3. 测试中国 (China)")
    print("  4. 定时爬取模式")
    print("  5. 退出")

    choice = input("\n请输入选项 (1-5): ").strip()
    if choice == "1":
        scraper.scrape_all(use_multithreading=True)
    elif choice == "2":
        scraper.scrape_all(max_countries=3, use_multithreading=True)
    elif choice == "3":
        # 测试中国
        countries = scraper.get_country_list()
        china = None
        for idx, country in enumerate(countries):
            if 'china' in country['name'].lower() or 'chin' in country['code'].lower():
                china = country
                print(f"找到中国: {country['name']} ({country['code']})")
                break
        
        if china:
            scraper.scrape_all(start_country_index=idx, max_countries=1, use_multithreading=True)
        else:
            print("❌ 未找到中国")
    elif choice == "4":
        # 定时爬取模式
        if not SCHEDULE_AVAILABLE:
            print("\n❌ 定时任务功能需要安装 schedule 库")
            print("安装命令: pip install schedule")
            print("安装后请重新运行程序")
            return
        
        print("\n定时爬取模式")
        print("请选择定时任务配置：")
        print("  1. 每30分钟爬取一次")
        print("  2. 每小时爬取一次")
        print("  3. 每6小时爬取一次")
        print("  4. 每12小时爬取一次")
        print("  5. 每天爬取一次")
        print("  6. 返回")
        
        schedule_choice = input("\n请输入选项 (1-6): ").strip()
        
        if schedule_choice == "6":
            return
        
        # 询问是否爬取所有国家
        scope_choice = input("\n爬取所有国家? (y/n): ").strip().lower()
        if scope_choice == 'y':
            max_countries = None
            start_idx = 0
        else:
            max_countries = 3  # 默认测试模式
            start_idx = 0
        
        # 配置定时任务
        if schedule_choice == "1":
            schedule.every(30).minutes.do(run_scheduled_scrape, scraper, None, start_idx, max_countries)
            interval_desc = "每30分钟"
        elif schedule_choice == "2":
            schedule.every().hour.do(run_scheduled_scrape, scraper, None, start_idx, max_countries)
            interval_desc = "每小时"
        elif schedule_choice == "3":
            schedule.every(6).hours.do(run_scheduled_scrape, scraper, None, start_idx, max_countries)
            interval_desc = "每6小时"
        elif schedule_choice == "4":
            schedule.every(12).hours.do(run_scheduled_scrape, scraper, None, start_idx, max_countries)
            interval_desc = "每12小时"
        elif schedule_choice == "5":
            schedule.every().day.at("00:00").do(run_scheduled_scrape, scraper, None, start_idx, max_countries)
            interval_desc = "每天00:00"
        else:
            print("无效选项")
            return
        
        print(f"\n✓ 定时任务已配置: {interval_desc}")
        print("首次运行立即开始...")
        
        # 立即执行一次
        run_scheduled_scrape(scraper, None, start_idx, max_countries)
        
        print(f"\n定时任务运行中 ({interval_desc})...")
        print("按 Ctrl+C 停止")
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # 每分钟检查一次
        except KeyboardInterrupt:
            print("\n\n定时任务已停止")
    else:
        print("已退出")


if __name__ == "__main__":
    main()