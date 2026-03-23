# -*- coding: utf-8 -*-
"""
OGIMET METAR/TAF 数据爬虫
爬取所有国家的站点METAR和TAF数据，解码后保存为CSV
"""

import requests
from bs4 import BeautifulSoup
import re
import os
import csv
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import gc
try:
    import schedule
    SCHEDULE_AVAILABLE = True
except ImportError:
    SCHEDULE_AVAILABLE = False
    print("警告: schedule库未安装，定时任务功能将不可用")
    print("安装命令: pip install schedule")

try:
    from metar_taf_parser.parser.parser import MetarParser, TAFParser
    MIVEK_AVAILABLE = True
except ImportError:
    MIVEK_AVAILABLE = False
    print("警告: metar-taf-parser库未安装，将使用简化的正则表达式解析")
    print("安装命令: pip install metar-taf-parser")
try:
    import sys
    sys.path.insert(0, os.path.dirname(__file__))
    from db_inserter_v2 import DatabaseInserter
    DB_INSERTER_AVAILABLE = True
except ImportError:
    DB_INSERTER_AVAILABLE = False
    print("警告: db_inserter_v2模块未找到，数据库功能将不可用")

class OgimetScraper:
    def __init__(self, base_url="https://ogimet.com", delay=0.3, save_format='csv', max_workers=20, use_database=False):
        """
        初始化爬虫
        
        Args:
            base_url: ogimet网站基础URL
            delay: 请求间隔（秒），避免对服务器造成压力
            save_format: 保存格式，'csv'、'json' 或 'db'
            max_workers: 最大线程数（用于多线程爬取）
            use_database: 是否使用数据库入库（True时数据直接入库）
        """
        self.base_url = base_url
        self.delay = delay
        self.save_format = save_format.lower()
        if self.save_format not in ['csv', 'json']:
            self.save_format = 'csv'
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
        
        self.headers = {
            'User-Agent': self.user_agents[0],  # 默认使用第一个
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache, no-store, must-revalidate',
            'Pragma': 'no-cache',
            'Expires': '0',
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # 优化连接池配置以支持更高并发（动态配置）
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=max(30, max_workers * 2),  # 连接池数量
            pool_maxsize=max(50, max_workers * 3),      # 最大连接数
            max_retries=2,                               # 重试次数
            pool_block=False                             # 不阻塞
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        self.base_path = "data"
        self.airport_path = os.path.join(self.base_path, "airport")
        os.makedirs(self.airport_path, exist_ok=True)
        self.log_dir = "logs"
        os.makedirs(self.log_dir, exist_ok=True)
        self.log_file = os.path.join(self.log_dir, "ogimet_scrape.txt")
        
        # 缓存已处理的站点信息（限制大小防止内存泄漏）
        self.station_cache = {}
        self.max_cache_size = 200  # 最大缓存200个站点
        
        # 线程锁，用于保护文件写入操作
        self.file_lock = threading.Lock()
        self.log_lock = threading.Lock()
        self._error_lock = threading.Lock()  # 保护错误集合的锁
        self.session_lock = threading.Lock()  # Session 操作锁
        
        # 错误去重集合（线程安全，限制大小）
        self._shown_mivek_errors = set()
        self.max_error_cache_size = 100  # 最大缓存100个错误
        
        # 统计信息
        self.stats = {
            'total_stations': 0,
            'processed_stations': 0,
            'failed_stations': 0,
            'start_time': None,
            'end_time': None
        }
        
        # 内存清理配置（优化：更频繁清理以节省内存）
        self.memory_cleanup_interval = 20  # 每处理20个站点清理一次内存（更频繁）
        self.processed_count = 0  # 已处理站点计数（线程安全，通过stats_lock保护）
        self.stats_lock = threading.Lock()  # 保护统计信息的锁
        
        # 数据库配置
        self.use_database = use_database and DB_INSERTER_AVAILABLE
        self.db_inserter = None
        
        if self.use_database:
            try:
                self.db_inserter = DatabaseInserter(table_prefix='ogimet')
                print("✓ 数据库入库功能已启用（OGIMET专用表）")
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
            if hasattr(self, 'station_cache'):
                self.station_cache.clear()
            if hasattr(self, '_shown_mivek_errors'):
                self._shown_mivek_errors.clear()
        except Exception as e:
            print(f"清理缓存时出错: {e}")
        
        # 强制垃圾回收
        import gc
        gc.collect()
    
    def clean_memory(self, force: bool = False):
        """清理内存缓存和执行垃圾回收（线程安全，增强版）
        
        Args:
            force: 强制清理，忽略间隔限制
        """
        # 减少缓存大小（200 → 100）
        max_size = 100 if not force else 0
        if len(self.station_cache) > max_size:
            if force:
                self.station_cache.clear()
            else:
                cache_items = list(self.station_cache.items())
                self.station_cache = dict(cache_items[-max_size:])
        
        with self._error_lock:
            error_max = 50 if not force else 0
            if len(self._shown_mivek_errors) > error_max:
                if force:
                    self._shown_mivek_errors.clear()
                else:
                    error_list = list(self._shown_mivek_errors)
                    self._shown_mivek_errors = set(error_list[-error_max:])
        
        # 清理 session adapters 缓存
        try:
            if hasattr(self.session, 'adapters'):
                for adapter in self.session.adapters.values():
                    if hasattr(adapter, 'close'):
                        adapter.close()
        except:
            pass
        
        # 多代垃圾回收
        for generation in range(3):
            gc.collect(generation)
    
    def _perform_deep_cleanup(self):
        """执行深度内存清理（参考 ogimet_synop_scraper 策略）"""
        # 完全清空所有缓存
        self.station_cache.clear()
        with self._error_lock:
            self._shown_mivek_errors.clear()
        
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
                        'User-Agent': random.choice(self.user_agents),
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                        'Cache-Control': 'no-cache, no-store, must-revalidate',
                        'Pragma': 'no-cache',
                        'Expires': '0',
                    })
                    # 重新挂载适配器
                    adapter = requests.adapters.HTTPAdapter(
                        pool_connections=30,
                        pool_maxsize=30,
                        max_retries=2,
                        pool_block=False
                    )
                    self.session.mount('http://', adapter)
                    self.session.mount('https://', adapter)
        except:
            pass
        
        # 深度垃圾回收
        for _ in range(5):
            collected = gc.collect(2)
            if collected == 0:
                break

    def get_country_list(self) -> List[Dict[str, str]]:
        """获取所有国家列表"""
        url = f"{self.base_url}/umetars.phtml.en"
        params = {
            '_': int(datetime.now().timestamp() * 1000)    }
        response = None
        soup = None
        try:
            # 随机选择User-Agent
            import random
            self.session.headers['User-Agent'] = random.choice(self.user_agents)
            
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()
            html_text = response.text
            soup = BeautifulSoup(html_text, 'html.parser')
            country_select = soup.find('select', {'name': 'estado'})
            if not country_select:
                print("未找到国家选择框")
                return []
            countries = []
            for option in country_select.find_all('option'):
                value = option.get('value', '').strip()
                text = option.text.strip()
                if value and text:
                    countries.append({'code': value, 'name': text})
            
            return countries
            
        except Exception as e:
            print(f"获取国家列表失败: {e}")
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

    def log(self, message: str):
        """写入日志文件"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_line = f"[{timestamp}] {message}"
        try:
            with self.log_lock:
                with open(self.log_file, 'a', encoding='utf-8') as f:
                    f.write(log_line + '\n')
        except Exception:
            # 日志失败时避免影响主流程
            pass
    
    def get_stations_for_country(self, country_code: str, country_name: str) -> List[Dict[str, str]]:
        """获取指定国家的所有站点列表"""
        url = f"{self.base_url}/ultimos_metars2.php"
        params = {
            'lang': 'en',
            'estado': country_code,
            'fmt': 'html',
            'iord': 'yes',
            '_': int(datetime.now().timestamp() * 1000)   }
        try:
            # 随机选择User-Agent
            import random
            self.session.headers['User-Agent'] = random.choice(self.user_agents)
            
            response = self.session.get(url, params=params, timeout=15)
            response.encoding = 'latin1'
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            stations = []
            # 查找所有站点链接
            station_links = soup.find_all('a', href=re.compile(r'display_metars2\.php.*lugar='))
            
            for link in station_links:
                # 提取站点代码
                href = link.get('href', '')
                match = re.search(r'lugar=([A-Z0-9]{4})', href)
                if match:
                    station_code = match.group(1)
                    station_name = link.text.strip()
                    parent_row = link.find_parent('tr')
                    if parent_row:
                        cells = parent_row.find_all(['td', 'th'])
                        location_info = ""
                        for cell in cells:
                            text = cell.get_text(strip=True)
                            if re.search(r'\d+[NS]|.*\d+[EW]|.*\d+m', text):
                                location_info = text
                                break
                        stations.append({
                            'code': station_code,
                            'name': station_name,
                            'location': location_info,
                            'country_code': country_code,
                            'country_name': country_name
                        })
            
            # 去重
            seen = set()
            unique_stations = []
            for station in stations:
                if station['code'] not in seen:
                    seen.add(station['code'])
                    unique_stations.append(station)
            
            return unique_stations
            
        except Exception as e:
            print(f"获取 {country_name} 站点列表失败: {e}")
            return []
    
    def get_station_info(self, station_code: str, station_name: str, country_name: Optional[str] = None) -> Dict:
        """获取站点详细信息（经纬度、海拔等）"""
        # 检查缓存
        if station_code in self.station_cache:
            return self.station_cache[station_code]
        
        url = f"{self.base_url}/display_metars2.php"
        params = {
            'lang': 'en',
            'tipo': 'ALL',
            'ord': 'REV',
            'nil': 'SI',
            'fmt': 'html',
            'lugar': station_code,
            '_': int(datetime.now().timestamp() * 1000)  # 添加时间戳防止缓存
        }
        
        response = None
        soup = None
        try:
            # 随机选择User-Agent
            import random
            self.session.headers['User-Agent'] = random.choice(self.user_agents)
            
            response = self.session.get(url, params=params, timeout=15)
            response.encoding = 'latin1'
            response.raise_for_status()
            html_text = response.text
            soup = BeautifulSoup(html_text, 'html.parser')
            
            station_info = {
                'code': station_code,
                'name': station_name,
                'wmo_index': '',
                'wigos_id': '',
                'latitude': '',
                'longitude': '',
                'altitude_m': '',
                'country': country_name or ''
            }
            
            # 查找站点信息框（通常是绿色背景的div或table）
            info_text = soup.get_text()
            
            # 提取WMO索引
            wmo_match = re.search(r'WMO index[:\s]+(\d+)', info_text, re.IGNORECASE)
            if wmo_match:
                station_info['wmo_index'] = wmo_match.group(1)
            
            # 提取WIGOS ID
            wigos_match = re.search(r'WIGOS Id[:\s]+([0-9\-]+)', info_text, re.IGNORECASE)
            if wigos_match:
                station_info['wigos_id'] = wigos_match.group(1)
            
            # 提取经纬度
            # 支持多种格式：38-32-20N, 38°55'59"N, 095-15.10W, 38-32-20N 等
            # 支持英语（Latitude/Longitude）和西班牙语（Latitud/Longitud）
            lat_match = re.search(r'Latitud[ei]?\s+([0-9\-°\'".]+[NS])', info_text, re.IGNORECASE)
            if lat_match:
                station_info['latitude'] = self.parse_coordinate(lat_match.group(1), 'lat')
            
            lon_match = re.search(r'Longitud[ei]?\s+([0-9\-°\'".]+[EW])', info_text, re.IGNORECASE)
            if lon_match:
                station_info['longitude'] = self.parse_coordinate(lon_match.group(1), 'lon')
            
            # 提取海拔
            alt_match = re.search(r'Altitude\s+([0-9.]+)\s*m', info_text, re.IGNORECASE)
            if alt_match:
                station_info['altitude_m'] = float(alt_match.group(1))
            
            # 提取国家
            country_match = re.search(r'\(([^)]+)\)', station_name)
            if country_match:
                station_info['country'] = country_match.group(1)
            elif country_name:
                station_info['country'] = country_name
            
            # 缓存结果
            self.station_cache[station_code] = station_info
            
            time.sleep(self.delay)
            return station_info
            
        except Exception as e:
            print(f"  获取站点 {station_code} 信息失败: {e}")
            return {
                'code': station_code,
                'name': station_name,
                'wmo_index': '',
                'wigos_id': '',
                'latitude': '',
                'longitude': '',
                'altitude_m': '',
                'country': country_name or ''
            }
        finally:
            if soup:
                soup.decompose()
                del soup
            if response:
                response.close()
                del response
            if 'html_text' in locals():
                del html_text
    
    def get_cloud_cover_description(self, cloud_code: str) -> str:
        """Get cloud cover description in English (ICAO standard)"""
        descriptions = {
            'CLR': 'Clear',
            'SKC': 'Clear',
            'NSC': 'No significant clouds',
            'FEW': 'Few (1/8-2/8)',
            'SCT': 'Scattered (3/8-4/8)',
            'BKN': 'Broken (5/8-7/8)',
            'OVC': 'Overcast (8/8)',
            'VV': 'Vertical visibility'
        }
        return descriptions.get(cloud_code, cloud_code)
    
    def get_weather_description(self, weather_code: str) -> str:
        """Get weather phenomenon description in English (ICAO standard)"""
        descriptions = {
            # Precipitation types
            'DZ': 'Drizzle',
            'RA': 'Rain',
            'SN': 'Snow',
            'SG': 'Snow grains',
            'IC': 'Ice crystals',
            'PL': 'Ice pellets',
            'GR': 'Hail',
            'GS': 'Small hail',
            'UP': 'Unknown precipitation',
            # Fog and visibility
            'BR': 'Mist',
            'FG': 'Fog',
            'FU': 'Smoke',
            'VA': 'Volcanic ash',
            'DU': 'Dust',
            'SA': 'Sand',
            'HZ': 'Haze',
            # Storms
            'SQ': 'Squall',
            'SS': 'Sandstorm',
            'DS': 'Duststorm',
            'PO': 'Dust whirls',
            'FC': 'Funnel cloud/Tornado',
            'TS': 'Thunderstorm',
            # Modifiers
            'SH': 'Showers',
            'FZ': 'Freezing',
            'PRFG': 'Partial fog',
            'BCFG': 'Patchy fog',
            'MIFG': 'Shallow fog',
            'DR': 'Low drifting',
            'BL': 'Blowing',
            'VC': 'In vicinity',
        }
        return descriptions.get(weather_code, weather_code)
    
    def parse_taf_time_range(self, time_str: str) -> str:
        """Parse TAF time range format to readable format"""
        # Format: 1312/1314 or 1320/1405
        # Z means UTC (Zulu time)
        import re
        match = re.match(r'(\d{2})(\d{2})/(\d{2})(\d{2})', time_str)
        if match:
            day1 = int(match.group(1))
            hour1 = int(match.group(2))
            day2 = int(match.group(3))
            hour2 = int(match.group(4))
            return f"Day {day1}, {hour1:02d}:00 UTC - Day {day2}, {hour2:02d}:00 UTC"
        return time_str
    
    def decode_taf_trend(self, trend_text: str) -> str:
        """Decode TAF trend codes to English description"""
        import re
        
        # Trend type mapping
        trend_types = {
            'BECMG': 'Becoming',
            'TEMPO': 'Temporary',
            'PROB': 'Probability',
            'FM': 'From',
            'TL': 'Until',
            'AT': 'At',
        }
        
        # Parse trend type
        trend_type = None
        trend_type_code = None
        
        # Check PROB format (PROB30, PROB40, etc.)
        prob_match = re.match(r'PROB(\d+)', trend_text)
        if prob_match:
            prob_value = prob_match.group(1)
            trend_type = f'Probability {prob_value}%'
            trend_type_code = 'PROB'
            trend_text = trend_text.replace(f'PROB{prob_value}', '').strip()
        else:
            # Check other trend types
            for code, desc in trend_types.items():
                if trend_text.startswith(code):
                    trend_type = desc
                    trend_type_code = code
                    trend_text = trend_text.replace(code, '').strip()
                    break
        
        # Parse time range
        time_match = re.search(r'(\d{2})(\d{2})/(\d{2})(\d{2})', trend_text)
        time_range = ''
        if time_match:
            time_str = time_match.group(0)
            time_range = self.parse_taf_time_range(time_str)
            trend_text = trend_text.replace(time_str, '').strip()
        
        # Parse weather conditions
        conditions = []
        
        # 先提取风信息（避免风速中的数字干扰能见度解析）
        wind_info = None
        wind_match = re.search(r'(\d{3}|VRB)(\d{2,3})(G(\d{2,3}))?(KT|MPS)', trend_text)
        if wind_match:
            direction = wind_match.group(1)
            speed = int(wind_match.group(2))
            gust = int(wind_match.group(4)) if wind_match.group(4) else None
            unit = wind_match.group(5)
            
            if direction == 'VRB':
                wind_info = f'Variable wind, speed {speed}'
            else:
                wind_info = f'Wind {direction}deg, speed {speed}'
            if gust:
                wind_info += f', gust {gust}'
            wind_info += ' ' + unit.lower()
            
            # 暂时移除风信息，避免干扰后续解析
            trend_text = trend_text[:wind_match.start()] + trend_text[wind_match.end():]
            trend_text = trend_text.strip()
        
        # Parse visibility（现在风速已移除，不会误匹配）
        vis_match = re.search(r'\b(\d{4})\b', trend_text)
        if vis_match:
            vis_value = int(vis_match.group(1))
            if vis_value == 9999:
                conditions.append('Visibility >= 10km')
            else:
                conditions.append(f'Visibility {vis_value}m')
            trend_text = trend_text.replace(vis_match.group(1), '', 1).strip()
        
        # Check CAVOK
        if 'CAVOK' in trend_text:
            conditions.append('Visibility >= 10km, no clouds, no significant weather')
            trend_text = trend_text.replace('CAVOK', '').strip()
        
        # Parse weather phenomenon codes
        weather_codes = ['BCFG', 'MIFG', 'PRFG', 'BR', 'FG', 'RA', 'SN', 'TS', 'HZ', 'DZ', 'GR', 'GS', 'IC', 'PL', 'SG', 'UP', 'VA', 'DU', 'SA', 'FU', 'SQ', 'SS', 'DS', 'PO', 'FC', 'SH', 'FZ', 'DR', 'BL', 'VC']
        for code in weather_codes:
            if code in trend_text:
                conditions.append(self.get_weather_description(code))
                trend_text = trend_text.replace(code, '').strip()
        
        # Parse cloud codes
        cloud_codes = ['NSC', 'CLR', 'SKC', 'FEW', 'SCT', 'BKN', 'OVC', 'VV']
        for code in cloud_codes:
            if code in trend_text:
                conditions.append(self.get_cloud_cover_description(code))
                trend_text = trend_text.replace(code, '').strip()
        
        # 添加之前提取的风信息
        if wind_info:
            conditions.append(wind_info)
        
        # Build English description with explicit time
        parts = []
        if trend_type:
            parts.append(trend_type)
        if time_range:
            cleaned_range = time_range.replace(', ', ' ')
            parts.append(f"Time: {cleaned_range}")
        if conditions:
            parts.append(', '.join(conditions))
        
        return ' | '.join(parts) if parts else trend_text
    
    def parse_coordinate(self, coord_str: str, coord_type: str) -> float:
        """解析坐标字符串为十进制度数"""
        try:
            # 格式可能是: 39-55-59N, 39°55'59"N, 095-15.10W, 38-32-20N 等
            coord_str = coord_str.strip().upper()
            
            # 移除方向标识
            direction = coord_str[-1]
            coord_str = coord_str[:-1]
            
            # 分割度分秒（支持多种分隔符：-、°、'、"、.）
            # 注意：小数点可能出现在分或秒部分（如 095-15.10W）
            parts = re.split(r'[-°\'"]+', coord_str)
            if len(parts) >= 3:
                # 度-分-秒格式（如 38-32-20 或 095-15.10）
                degrees = float(parts[0])
                minutes = float(parts[1])
                seconds = float(parts[2])
                
                decimal = degrees + minutes/60.0 + seconds/3600.0
                
                # 处理方向
                if direction in ['S', 'W']:
                    decimal = -decimal
                
                return round(decimal, 6)
            elif len(parts) == 2:
                # 度-分格式（如 38-32 或 095-15.10）
                degrees = float(parts[0])
                minutes = float(parts[1])
                decimal = degrees + minutes/60.0
                if direction in ['S', 'W']:
                    decimal = -decimal
                return round(decimal, 6)
            else:
                # 只有度
                return float(parts[0]) if parts[0] else 0.0
        except:
            return 0.0
    
    def get_station_metar_taf(self, station_code: str, station_name: str) -> Tuple[List[Dict], List[Dict]]:
        """获取站点的METAR和TAF数据"""
        url = f"{self.base_url}/display_metars2.php"
        params = {
            'lang': 'en',
            'tipo': 'ALL',  # 获取所有类型（METAR + TAF）
            'ord': 'REV',
            'nil': 'SI',
            'fmt': 'html',
            'lugar': station_code,
            '_': int(datetime.now().timestamp() * 1000)  # 添加时间戳防止缓存
        }
        
        response = None
        soup = None
        try:
            # 随机选择User-Agent
            import random
            self.session.headers['User-Agent'] = random.choice(self.user_agents)
            
            response = self.session.get(url, params=params, timeout=15)
            response.encoding = 'latin1'
            response.raise_for_status()
            
            # 获取HTML文本后立即提取，避免保留response对象
            html_text = response.text
            soup = BeautifulSoup(html_text, 'html.parser')
            
            metar_data = []
            taf_data = []
            
            # 提取METAR数据
            metar_rows = soup.find_all('tr')
            for row in metar_rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 3:
                    # 检查是否是METAR数据行
                    first_cell = cells[0].get_text(strip=True)
                    if first_cell == 'SA':  # Surface Observation
                        timestamp_cell = cells[1].get_text(strip=True)
                        metar_cell = cells[2]
                        metar_text = metar_cell.get_text(strip=True)
                        
                        if 'METAR' in metar_text.upper() or 'SPECI' in metar_text.upper():
                            parsed = self.parse_metar(metar_text, timestamp_cell)
                            if parsed:
                                metar_data.append(parsed)
                    
                    # 检查是否是TAF数据行
                    elif first_cell == 'FT':  # Forecast Time
                        timestamp_cell = cells[1].get_text(strip=True)
                        taf_cell = cells[2]
                        # 使用separator=' '将换行符替换为空格，避免多行TAF导致解析失败
                        taf_text = taf_cell.get_text(separator=' ', strip=True)
                        
                        if 'TAF' in taf_text.upper():
                            parsed = self.parse_taf(taf_text, timestamp_cell)
                            if parsed:
                                taf_data.append(parsed)
            
            time.sleep(self.delay)
            return metar_data, taf_data
            
        except Exception as e:
            print(f"  获取站点 {station_code} METAR/TAF数据失败: {e}")
            return [], []
        finally:
            # 显式释放大对象
            if soup:
                soup.decompose()
                del soup
            if response:
                response.close()
                del response
            if 'html_text' in locals():
                del html_text
    
    def _parse_metar_with_mivek(self, metar_text: str, timestamp: str) -> Optional[Dict]:
        """使用mivek库解析METAR"""
        # 检查NIL报告 - 直接返回None跳过
        if 'NIL=' in metar_text.upper() or metar_text.upper().strip().endswith('NIL'):
            return None
        
        # 使用mivek解析（去掉METAR/SPECI前缀和AMD/COR修正标识）
        parser = MetarParser()
        
        # 先检测是否为修正报（AMD或COR标识）
        is_corrected = bool(re.search(r'\b(?:AMD|COR)\b', metar_text.upper()))
        
        clean_text = re.sub(r'^(METAR|SPECI)\s+', '', metar_text, flags=re.IGNORECASE)
        # 去掉AMD/COR修正报标识（可能在站点代码前或后），避免mivek解析错误
        clean_text = re.sub(r'\b(?:AMD|COR)\b\s*', '', clean_text, flags=re.IGNORECASE)
        
        # 修复TEMPO/BECMG中的时间格式问题
        # FM0 010 → FM0010 (修复FM时间)
        clean_text = re.sub(r'FM(\d)\s+(\d+)', r'FM\1\2', clean_text)
        # TL0 040 → TL0040 (修复TL时间)
        clean_text = re.sub(r'TL(\d)\s+(\d+)', r'TL\1\2', clean_text)
        
        # 清理趋势预报片段：从 TEMPO/BECMG 开始到行尾全部删除
        # 这些趋势信息对当前观测无关，而且容易触发 Mivek 解析错误
        clean_text = re.sub(r'\s+(TEMPO|BECMG)\b.*$', '', clean_text, flags=re.IGNORECASE)
        
        # 清理不完整的跑道信息（RWY后面缺少完整信息）
        # 匹配 RWY 后面没有完整数字的情况
        clean_text = re.sub(r'\s+R\d{0,2}[LCR]?/\s*$', '', clean_text)  # 末尾不完整的 R##/
        clean_text = re.sub(r'\s+R\d{0,2}[LCR]?\s*$', '', clean_text)  # 末尾不完整的 R##
        # 统一删除所有跑道能见度组（如 R02/0500、R02/////// 等），避免不规范格式触发解析错误
        clean_text = re.sub(r'\sR\d{2}[LCR]?[^ ]*', '', clean_text)
        
        # 清理其他常见不完整片段
        clean_text = re.sub(r'\s+(NOSIG|RMK|RE)\s*$', '', clean_text, flags=re.IGNORECASE)
        metar = parser.parse(clean_text)
        
        # 使用HTML提供的timestamp（更准确）
        dt = self.parse_timestamp(timestamp)
        obs_time = dt.strftime('%Y-%m-%d %H:%M:%S') if dt else ''
        
        # 构建结果字典（使用新格式）
        result = {
            'raw_metar': metar_text,
            'station_code': metar.station if hasattr(metar, 'station') else '',
            'observation_time': obs_time,
            'report_type': 'METAR',
            'is_auto': metar.auto if hasattr(metar, 'auto') else False,
            'is_corrected': is_corrected,
            'temperature_c': None,
            'dewpoint_c': None,
            'wind_direction_deg': None,
            'wind_speed_mps': None,
            'wind_gust_mps': None,
            'wind_direction_variation': None,
            'visibility_m': None,
            'altimeter_hpa': None,
            'sea_level_pressure_hpa': None,
            'weather': None,
            'sky_condition': None,
            'precip_1hr_mm': None,
            'precip_3hr_mm': None,
            'precip_6hr_mm': None,
            'precip_24hr_mm': None,
            'peak_wind_speed_mps': None,
            'peak_wind_time': None,
            'wind_shift_time': None,
            'max_temp_6hr_c': None,
            'min_temp_6hr_c': None,
            'max_temp_24hr_c': None,
            'min_temp_24hr_c': None,
            'remarks': '',
            'update_time': None
        }
        
        # 风信息
        if metar.wind:
            # 处理风向
            wind_deg = metar.wind.degrees
            if wind_deg != 'VRB' and wind_deg is not None:
                result['wind_direction_deg'] = int(wind_deg)
            
            # 风速（转换为米/秒）
            if metar.wind.speed is not None:
                result['wind_speed_mps'] = round(metar.wind.speed * 0.514444, 2) if metar.wind.unit == 'KT' else metar.wind.speed
            
            # 阵风
            if metar.wind.gust:
                gust_mps = round(metar.wind.gust * 0.514444, 2) if metar.wind.unit == 'KT' else metar.wind.gust
                result['wind_gust_mps'] = gust_mps
                result['peak_wind_speed_mps'] = gust_mps
            
            # 风向变化范围（使用库解析）
            if hasattr(metar.wind, 'min_variation') and metar.wind.min_variation and hasattr(metar.wind, 'max_variation') and metar.wind.max_variation:
                result['wind_direction_variation'] = f"{metar.wind.min_variation}-{metar.wind.max_variation}"
        
        # 能见度（优先使用库解析）
        if metar.cavok:
            result['visibility_m'] = 10000
        elif hasattr(metar, 'visibility') and metar.visibility and hasattr(metar.visibility, 'distance'):
            vis_distance = metar.visibility.distance
            if isinstance(vis_distance, (int, float)):
                vis_val = int(vis_distance)
                result['visibility_m'] = 10000 if vis_val == 9999 else vis_val
            elif isinstance(vis_distance, str):
                s = vis_distance.strip().upper()
                try:
                    if 'SM' in s:
                        # 英里转米
                        miles_str = s.replace('SM', '').replace('P', '').strip()
                        result['visibility_m'] = round(float(miles_str) * 1609.34)
                    elif '>' in s or 'KM' in s:
                        result['visibility_m'] = 10000
                    else:
                        val = int(float(s.replace('>', '')))
                        result['visibility_m'] = 10000 if val == 9999 else val
                except Exception:
                    result['visibility_m'] = None
        
        # 温度和露点
        if metar.temperature:
            result['temperature_c'] = metar.temperature
        if metar.dew_point:
            result['dewpoint_c'] = metar.dew_point
        
        # 气压
        # mivek库已经将A格式和Q格式都转换为百帕，直接使用库返回的值
        if metar.altimeter:
            result['altimeter_hpa'] = float(metar.altimeter)
        
        # 天气现象（使用库解析结果，不自定义解析）
        result['weather'] = None
        if hasattr(metar, 'weather_conditions') and metar.weather_conditions:
            weather_codes = []
            for wx in metar.weather_conditions:
                code_parts = []
                if hasattr(wx, 'intensity') and wx.intensity:
                    intensity_str = str(wx.intensity).replace('Intensity.', '')
                    if intensity_str == 'LIGHT':
                        code_parts.append('-')
                    elif intensity_str == 'HEAVY':
                        code_parts.append('+')
                if hasattr(wx, 'descriptive') and wx.descriptive:
                    desc_str = str(wx.descriptive).upper().replace('DESCRIPTIVE.', '')
                    descriptive_map = {'THUNDERSTORM': 'TS', 'SHOWERS': 'SH', 'FREEZING': 'FZ',
                                      'BLOWING': 'BL', 'DRIFTING': 'DR', 'SHALLOW': 'MI',
                                      'PARTIAL': 'PR', 'PATCHES': 'BC'}
                    code_parts.append(descriptive_map.get(desc_str, desc_str))
                if hasattr(wx, 'phenomenons') and wx.phenomenons:
                    for p in wx.phenomenons:
                        p_str = str(p).upper().replace('PHENOMENON.', '')
                        phenomenon_map = {'RAIN': 'RA', 'SNOW': 'SN', 'DRIZZLE': 'DZ', 'FOG': 'FG',
                                        'MIST': 'BR', 'HAZE': 'HZ', 'THUNDERSTORM': 'TS',
                                        'HAIL': 'GR', 'SMALL_HAIL': 'GS', 'ICE_PELLETS': 'PL',
                                        'ICE_CRYSTALS': 'IC', 'SNOW_GRAINS': 'SG', 'SMOKE': 'FU',
                                        'VOLCANIC_ASH': 'VA', 'SAND': 'SA', 'DUST': 'DU',
                                        'SQUALL': 'SQ', 'FUNNEL_CLOUD': 'FC', 'SANDSTORM': 'SS',
                                        'DUSTSTORM': 'DS'}
                        code_parts.append(phenomenon_map.get(p_str, p_str))
                if code_parts:
                    weather_codes.append(''.join(code_parts))
            result['weather'] = ' '.join(weather_codes) if weather_codes else None
        
        # 备注
        if hasattr(metar, 'remarks') and metar.remarks:
            result['remarks'] = ' '.join(metar.remarks)
        
        # 从备注中提取额外信息
        rmk_match = re.search(r'RMK\s+(.+?)(?=\s*=|$)', metar_text)
        if rmk_match:
            remarks_text = rmk_match.group(1).strip()
            if not result['remarks']:
                result['remarks'] = remarks_text
            
            # 提取自动站类型（AO1/AO2）
            if 'AO1' in remarks_text:
                result['auto_station_type'] = 'AO1'
            elif 'AO2' in remarks_text:
                result['auto_station_type'] = 'AO2'
            
            # 提取海平面气压（SLP）
            # METAR标准：SLP编码规则
            # - 如果第一位是0-2，则SLP = 1000 + 后两位/10
            # - 如果第一位是3-9，则SLP = 900 + 后两位/10，但如果结果<1000，则应该加100
            slp_match = re.search(r'\bSLP(\d{3})\b', remarks_text)
            if slp_match:
                slp_code = slp_match.group(1)
                if slp_code[0] in ['0', '1', '2']:
                    slp_value = 1000 + int(slp_code) / 10.0
                else:
                    slp_value = 900 + int(slp_code) / 10.0
                    # 如果计算出的值<1000，说明实际气压在1000-1099之间，需要加100
                    if slp_value < 1000:
                        slp_value += 100
                result['sea_level_pressure_hpa'] = round(slp_value, 1)
            
            # 提取1小时降水量
            precip_1hr_match = re.search(r'\bP(\d{4})\b', remarks_text)
            if precip_1hr_match:
                precip_inches = int(precip_1hr_match.group(1)) / 100.0
                result['precipitation_1hr_mm'] = round(precip_inches * 25.4, 2)
            
            # 提取6小时和24小时降水量
            remarks_parts = remarks_text.split()
            for part in remarks_parts:
                if re.match(r'^6\d{4}$', part):
                    precip_6hr = int(part[1:]) / 100.0
                    result['precipitation_6hr_mm'] = round(precip_6hr * 25.4, 2)
                elif re.match(r'^7\d{4}$', part):
                    precip_24hr = int(part[1:]) / 100.0
                    result['precipitation_24hr_mm'] = round(precip_24hr * 25.4, 2)
        else:
            result['remarks'] = ''
        
        # 如果既没有云层信息又存在CAVOK，按CAVOK编码云况
        if not result.get('sky_condition') and 'CAVOK' in metar_text:
            result['sky_condition'] = json.dumps([{'cover': 'CAVOK'}], ensure_ascii=False)
        
        # 补充解析VV（垂直能见度）- mivek可能漏掉
        if not result.get('cloud_layers') or result.get('cloud_layers') == '[]':
            vv_match = re.search(r'VV(\d{3})', metar_text)
            if vv_match:
                vv_height = int(vv_match.group(1)) * 100  # 英尺
                cloud_layers_list = [{
                    'type': 'VV',
                    'description': 'Vertical Visibility',
                    'oktas': 'obscured',
                    'base_m': round(vv_height * 0.3048, 2)
                }]
                result['cloud_layers'] = json.dumps(cloud_layers_list, ensure_ascii=False)
                result['cloud_cover'] = 'VV'
                result['cloud_base_m'] = round(vv_height * 0.3048, 2)
        
        # RVR (跑道视程) - JSON数组格式，包含趋势
        if hasattr(metar, 'runways_info') and metar.runways_info:
            rvr_list = []
            for runway in metar.runways_info:
                rvr_info = {}
                if hasattr(runway, 'name'):
                    rvr_info['runway'] = runway.name
                if hasattr(runway, 'min_range'):
                    rvr_info['visibility_m'] = runway.min_range
                elif hasattr(runway, 'max_range'):
                    rvr_info['visibility_m'] = runway.max_range
                
                # 从原始METAR提取趋势（mivek可能不提供）
                if hasattr(runway, 'name'):
                    trend_match = re.search(rf'R{runway.name}/[PM]?\d+(?:V[PM]?\d+)?([UDN])', metar_text)
                    if trend_match:
                        trend_code = trend_match.group(1)
                        trend_map = {
                            'U': {'code': 'U', 'description': 'Upward'},
                            'D': {'code': 'D', 'description': 'Downward'},
                            'N': {'code': 'N', 'description': 'No change'}
                        }
                        if trend_code in trend_map:
                            rvr_info['trend'] = trend_map[trend_code]['code']
                            rvr_info['trend_description'] = trend_map[trend_code]['description']
                
                if rvr_info:
                    rvr_list.append(rvr_info)
            if rvr_list:
                result['rvr'] = rvr_list  # 直接赋值数组，不转为字符串
        
        # 添加核心兼容字段（精简版）
        # 注意：mivek库已经将A格式和Q格式都转换为百帕，不需要altimeter_inhg转换
        
        result.setdefault('rvr', [])  # RVR默认为空数组
        result.setdefault('auto_station_type', '')
        result.setdefault('sea_level_pressure_hpa', None)
        result.setdefault('precipitation_1hr_mm', None)
        result.setdefault('precipitation_6hr_mm', None)
        result.setdefault('precipitation_24hr_mm', None)
        
        # 使用正则表达式解析REMARKS部分（mivek不支持）
        remarks_text = ''
        rmk_match = re.search(r'RMK\s+(.+?)(?=\s*=|$)', metar_text)
        if rmk_match:
            remarks_text = rmk_match.group(1).strip()
            result['remarks'] = remarks_text
            
            # 提取自动站类型（AO1/AO2）
            if 'AO1' in remarks_text:
                result['auto_station_type'] = 'AO1'
            elif 'AO2' in remarks_text:
                result['auto_station_type'] = 'AO2'
            
            # 提取海平面气压（SLP）
            # METAR标准：SLP编码规则
            # - 如果第一位是0-2，则SLP = 1000 + 后两位/10
            # - 如果第一位是3-9，则SLP = 900 + 后两位/10，但如果结果<1000，则应该加100
            slp_match = re.search(r'\bSLP(\d{3})\b', remarks_text)
            if slp_match:
                slp_code = slp_match.group(1)
                if slp_code[0] in ['0', '1', '2']:
                    slp_value = 1000 + int(slp_code) / 10.0
                else:
                    slp_value = 900 + int(slp_code) / 10.0
                    # 如果计算出的值<1000，说明实际气压在1000-1099之间，需要加100
                    if slp_value < 1000:
                        slp_value += 100
                result['sea_level_pressure_hpa'] = round(slp_value, 1)
            
            # 提取1小时降水量
            precip_1hr_match = re.search(r'\bP(\d{4})\b', remarks_text)
            if precip_1hr_match:
                precip_inches = int(precip_1hr_match.group(1)) / 100.0
                result['precipitation_1hr_mm'] = round(precip_inches * 25.4, 2)
            
            # 提取6小时和24小时降水量
            remarks_parts = remarks_text.split()
            for part in remarks_parts:
                if re.match(r'^6\d{4}$', part):
                    precip_6hr = int(part[1:]) / 100.0
                    result['precipitation_6hr_mm'] = round(precip_6hr * 25.4, 2)
                elif re.match(r'^7\d{4}$', part):
                    precip_24hr = int(part[1:]) / 100.0
                    result['precipitation_24hr_mm'] = round(precip_24hr * 25.4, 2)
        else:
            result['remarks'] = ''
        
        return result
    
    def parse_metar(self, metar_text: str, timestamp: str) -> Optional[Dict]:
        """解析METAR编码为结构化数据"""
        # 只使用mivek库解析，不再fallback到自定义解码
        if MIVEK_AVAILABLE:
            try:
                return self._parse_metar_with_mivek(metar_text, timestamp)
            except Exception as e:
                # 解析失败时记录日志并返回None
                # 使用集合记录已显示的错误类型，避免重复显示相同错误（线程安全）
                error_msg = str(e)[:50]
                error_key = f"{type(e).__name__}:{error_msg}"
                
                # 线程安全的错误去重检查和添加
                should_print = False
                with self._error_lock:
                    if error_key not in self._shown_mivek_errors:
                        self._shown_mivek_errors.add(error_key)
                        should_print = True
                
                if should_print:
                    with self.log_lock:
                        print(f"    ⚠️  Mivek解析失败: {e}")
                        print(f"        METAR: {metar_text[:100]}")
                return None
        
        # 如果库不可用，返回None（不再使用自定义解码）
        return None
    
    def _parse_taf_with_mivek(self, taf_text: str, timestamp: str) -> Optional[Dict]:
        """使用mivek库解析TAF"""
        try:
            from metar_taf_parser.parser.parser import TAFParser
            
            # 检查NIL报告 - 直接返回None跳过
            # 兼容多种形式：
            # - "TAF XXXX DDHHMMZ NIL="
            # - "TAF XXXX DDHHMMZ NIL RMK ..."（如 CWSA 271940Z NIL RMK ...）
            upper_taf = re.sub(r'\s+', ' ', taf_text.upper()).strip()
            if ('NIL=' in upper_taf
                or upper_taf.endswith(' NIL')
                or ' NIL ' in upper_taf):
                return None
            
            # 清理文本使其符合库的解析要求
            clean_text = taf_text
            
            # 1. 合并多行（将换行和多余空格替换为单个空格）
            clean_text = ' '.join(clean_text.split())
            
            # 2. 移除结尾的等号
            clean_text = clean_text.replace('=', '')
            
            # 3. 去掉 AMD/COR 修正标识，避免干扰库解析（是否为修正报由原始文本判断）
            clean_text = re.sub(r'\b(?:AMD|COR)\b\s*', '', clean_text, flags=re.IGNORECASE)
            
            # 4. 保留TAF前缀（库需要TAF前缀才能正确解析trends）
            # 只确保TAF前缀存在
            if not clean_text.strip().upper().startswith('TAF'):
                clean_text = 'TAF ' + clean_text
            
            # 5. 清理多余空格
            clean_text = re.sub(r'\s+', ' ', clean_text).strip()
            
            # 5. 清理不完整的关键字片段（末尾缺少必要信息）
            # 5.1 清理不完整的TEMPO/BECMG/FM/TL（缺少时间值）
            clean_text = re.sub(r'\s+(TEMPO|BECMG|FM|TL)\s*$', '', clean_text, flags=re.IGNORECASE)
            clean_text = re.sub(r'\s+(TEMPO|BECMG)\s+(FM|TL)\s*$', '', clean_text, flags=re.IGNORECASE)
            # 修复TEMPO/BECMG中的时间格式问题
            clean_text = re.sub(r'FM(\d)\s+(\d+)', r'FM\1\2', clean_text)
            clean_text = re.sub(r'TL(\d)\s+(\d+)', r'TL\1\2', clean_text)
            
            # 5.2 清理不完整的跑道信息（RWY后面缺少完整信息）
            clean_text = re.sub(r'\s+R\d{0,2}[LCR]?/\s*$', '', clean_text)  # 末尾不完整的 R##/
            clean_text = re.sub(r'\s+R\d{0,2}[LCR]?\s*$', '', clean_text)  # 末尾不完整的 R##
            
            # 5.3 清理其他常见不完整片段（尾部的 NOSIG / RMK / RE / PROBxx）
            clean_text = re.sub(r'\s+(NOSIG|RMK|RE|PROB\d*)\s*$', '', clean_text, flags=re.IGNORECASE)
            
            # 5.4 清理 PROB 开头的趋势段（例如：PROB30 2718/2800 7000 -RA FEW014CB BKN01，或 PROB 40 ...）
            # 这些概率性趋势目前不入库，而且容易因为不规范分组触发解析错误
            clean_text = re.sub(r'\s+PROB\b.*$', '', clean_text, flags=re.IGNORECASE)
            
            # 5.5 清理中间不完整的片段（关键字后面没有有效数字）
            # 匹配 TEMPO/BECMG 后面直接跟其他关键字的情况（缺少时间）
            clean_text = re.sub(r'\s+(TEMPO|BECMG)\s+(?=TEMPO|BECMG|PROB|TX|TN)', '', clean_text, flags=re.IGNORECASE)
            
            # 如果清理后文本过短，可能无效
            if len(clean_text) < 15:
                return None
            
            # 使用TAFParser解析
            parser = TAFParser()
            taf = parser.parse(clean_text)
            
            # 使用HTML提供的timestamp
            dt = self.parse_timestamp(timestamp)
            timestamp_str = dt.strftime('%Y-%m-%d %H:%M:%S') if dt else ''
            
            result = {
                'raw_taf': taf_text,
                'station_code': taf.station if hasattr(taf, 'station') else '',
                'timestamp_utc': timestamp_str,
                'is_corrected': bool(re.search(r'\b(?:AMD|COR)\b', taf_text.upper())),
            }
            
            # 主有效期
            valid_from_set = False
            valid_to_set = False
            
            if hasattr(taf, 'validity') and taf.validity and dt:
                # 库返回的validity包含start_day, start_hour, end_day, end_hour (整数)
                try:
                    if hasattr(taf.validity, 'start_day') and hasattr(taf.validity, 'start_hour'):
                        start_day = taf.validity.start_day
                        start_hour = taf.validity.start_hour
                        valid_from_dt = datetime(dt.year, dt.month, start_day, start_hour, 0, 0)
                        result['valid_from'] = valid_from_dt.strftime('%Y-%m-%d %H:%M:%S')
                        valid_from_set = True
                    
                    if hasattr(taf.validity, 'end_day') and hasattr(taf.validity, 'end_hour'):
                        end_day = taf.validity.end_day
                        end_hour = taf.validity.end_hour
                        # 处理跨月情况（如果end_day < start_day，说明跨月了）
                        end_month = dt.month
                        end_year = dt.year
                        if hasattr(taf.validity, 'start_day') and end_day < taf.validity.start_day:
                            end_month = dt.month + 1 if dt.month < 12 else 1
                            if end_month == 1:
                                end_year += 1
                        valid_to_dt = datetime(end_year, end_month, end_day, end_hour, 0, 0)
                        result['valid_to'] = valid_to_dt.strftime('%Y-%m-%d %H:%M:%S')
                        valid_to_set = True
                except (ValueError, AttributeError):
                    pass
            
            # 如果库解析失败，从原始报文中手动解析有效时间
            if not valid_from_set or not valid_to_set:
                # 清理文本以便匹配
                clean_taf = re.sub(r'\s+', ' ', taf_text.strip())
                
                # 尝试两种格式：
                # 1. 标准格式：DDHHMMZ DDHH/DDHH (例如: 252125Z 2600/2624)
                # 2. COR格式：TAF COR STATION DDHH/DDHH (例如: TAF COR SPYL 2500/2524，没有发布时间)
                pattern1 = r'(\d{6})Z\s+(\d{4})/(\d{4})'
                pattern2 = r'TAF\s+(?:COR\s+)?[A-Z0-9]{4}\s+(\d{4})/(\d{4})'
                
                time_match = re.search(pattern1, clean_taf)
                has_issue_time = True
                
                if not time_match:
                    # 尝试COR格式（没有发布时间）
                    time_match = re.search(pattern2, clean_taf)
                    has_issue_time = False
                
                if time_match and dt:
                    try:
                        if has_issue_time:
                            # 标准格式：有发布时间
                            valid_from_str = time_match.group(2)
                            valid_to_str = time_match.group(3)
                            issue_day = int(time_match.group(1)[0:2])
                        else:
                            # COR格式：没有发布时间
                            valid_from_str = time_match.group(1)
                            valid_to_str = time_match.group(2)
                            issue_day = None
                        
                        from_day = int(valid_from_str[0:2])
                        from_hour = int(valid_from_str[2:4])
                        to_day = int(valid_to_str[0:2])
                        to_hour = int(valid_to_str[2:4])
                        
                        # 处理24:00特殊情况
                        if to_hour == 24:
                            to_hour = 0
                            to_day += 1
                        
                        # 构建有效开始时间
                        if not valid_from_set:
                            year = dt.year
                            month = dt.month
                            if has_issue_time and issue_day is not None:
                                # 如果有效开始日期小于发布时间日期，可能是下个月
                                if from_day < issue_day:
                                    month += 1
                                    if month > 12:
                                        month = 1
                                        year += 1
                            else:
                                # 没有发布时间，从timestamp推断
                                if from_day < dt.day:
                                    month += 1
                                    if month > 12:
                                        month = 1
                                        year += 1
                            valid_from_dt = datetime(year, month, from_day, from_hour, 0, 0)
                            result['valid_from'] = valid_from_dt.strftime('%Y-%m-%d %H:%M:%S')
                        
                        # 构建有效结束时间
                        if not valid_to_set:
                            end_year = dt.year
                            end_month = dt.month
                            if has_issue_time and issue_day is not None:
                                if from_day < issue_day:
                                    end_month += 1
                                    if end_month > 12:
                                        end_month = 1
                                        end_year += 1
                            else:
                                if from_day < dt.day:
                                    end_month += 1
                                    if end_month > 12:
                                        end_month = 1
                                        end_year += 1
                            if to_day < from_day:
                                end_month += 1
                                if end_month > 12:
                                    end_month = 1
                                    end_year += 1
                            valid_to_dt = datetime(end_year, end_month, to_day, to_hour, 0, 0)
                            result['valid_to'] = valid_to_dt.strftime('%Y-%m-%d %H:%M:%S')
                    except (ValueError, AttributeError):
                        if not valid_from_set:
                            result['valid_from'] = None
                        if not valid_to_set:
                            result['valid_to'] = None
                else:
                    if not valid_from_set:
                        result['valid_from'] = None
                    if not valid_to_set:
                        result['valid_to'] = None
            
            # 风信息
            if hasattr(taf, 'wind') and taf.wind:
                wind_deg = taf.wind.degrees
                if wind_deg == 'VRB':
                    result['wind_direction_deg'] = None
                elif isinstance(wind_deg, int):
                    result['wind_direction_deg'] = wind_deg
                else:
                    result['wind_direction_deg'] = int(wind_deg) if wind_deg else None
                
                # 转换为m/s
                if taf.wind.speed:
                    result['wind_speed_mps'] = round(taf.wind.speed * 0.514444, 2) if taf.wind.unit == 'KT' else taf.wind.speed
                
                if taf.wind.gust:
                    result['wind_gust_mps'] = round(taf.wind.gust * 0.514444, 2) if taf.wind.unit == 'KT' else taf.wind.gust
            
            # CAVOK或能见度（优先使用库解析）
            if hasattr(taf, 'cavok') and taf.cavok:
                result['visibility_m'] = 10000
                result['cloud_cover'] = 'CAVOK'
            elif hasattr(taf, 'visibility') and taf.visibility and hasattr(taf.visibility, 'distance'):
                vis_distance = taf.visibility.distance
                if isinstance(vis_distance, (int, float)):
                    vis_val = int(vis_distance)
                    result['visibility_m'] = 10000 if vis_val == 9999 else vis_val
                elif isinstance(vis_distance, str):
                    s = vis_distance.strip().upper()
                    try:
                        if 'SM' in s:
                            miles_str = s.replace('SM', '').replace('P', '').strip()
                            result['visibility_m'] = round(float(miles_str) * 1609.34)
                        elif '>' in s or 'KM' in s:
                            result['visibility_m'] = 10000
                        else:
                            val = int(float(s.replace('>', '')))
                            result['visibility_m'] = 10000 if val == 9999 else val
                    except Exception:
                        result['visibility_m'] = None
            
            # 天气现象（使用库解析结果，不自定义解析）
            result['weather_phenomena'] = ''
            if hasattr(taf, 'weather_conditions') and taf.weather_conditions:
                weather_codes = []
                for wx in taf.weather_conditions:
                    # 构建天气代码：强度 + 描述符 + 现象
                    code_parts = []
                    if hasattr(wx, 'intensity') and wx.intensity:
                        intensity_str = str(wx.intensity).replace('Intensity.', '')
                        if intensity_str == 'LIGHT':
                            code_parts.append('-')
                        elif intensity_str == 'HEAVY':
                            code_parts.append('+')
                    if hasattr(wx, 'descriptive') and wx.descriptive:
                        desc_str = str(wx.descriptive).upper().replace('DESCRIPTIVE.', '')
                        # 转换描述符为标准代码
                        descriptive_map = {'THUNDERSTORM': 'TS', 'SHOWERS': 'SH', 'FREEZING': 'FZ',
                                          'BLOWING': 'BL', 'DRIFTING': 'DR', 'SHALLOW': 'MI',
                                          'PARTIAL': 'PR', 'PATCHES': 'BC'}
                        code_parts.append(descriptive_map.get(desc_str, desc_str))
                    if hasattr(wx, 'phenomenons') and wx.phenomenons:
                        for p in wx.phenomenons:
                            p_str = str(p).upper().replace('PHENOMENON.', '')
                            # 转换为标准代码
                            phenomenon_map = {'RAIN': 'RA', 'SNOW': 'SN', 'DRIZZLE': 'DZ', 'FOG': 'FG',
                                            'MIST': 'BR', 'HAZE': 'HZ', 'THUNDERSTORM': 'TS',
                                            'HAIL': 'GR', 'SMALL_HAIL': 'GS', 'ICE_PELLETS': 'PL',
                                            'ICE_CRYSTALS': 'IC', 'SNOW_GRAINS': 'SG', 'SMOKE': 'FU',
                                            'VOLCANIC_ASH': 'VA', 'SAND': 'SA', 'DUST': 'DU',
                                            'SQUALL': 'SQ', 'FUNNEL_CLOUD': 'FC', 'SANDSTORM': 'SS',
                                            'DUSTSTORM': 'DS'}
                            code_parts.append(phenomenon_map.get(p_str, p_str))
                    if code_parts:
                        weather_codes.append(''.join(code_parts))
                result['weather_phenomena'] = ' '.join(weather_codes) if weather_codes else ''
            
            # 云层
            if hasattr(taf, 'clouds') and taf.clouds:
                cloud_cover_map = {
                    'SKC': {'description': 'Sky Clear', 'oktas': '0/8'},
                    'CLR': {'description': 'Clear', 'oktas': '0/8'},
                    'NSC': {'description': 'No Significant Cloud', 'oktas': '0/8'},
                    'FEW': {'description': 'Few', 'oktas': '1-2/8'},
                    'SCT': {'description': 'Scattered', 'oktas': '3-4/8'},
                    'BKN': {'description': 'Broken', 'oktas': '5-7/8'},
                    'OVC': {'description': 'Overcast', 'oktas': '8/8'},
                    'VV': {'description': 'Vertical Visibility', 'oktas': 'obscured'}
                }
                
                cloud_layers_list = []
                for cloud in taf.clouds:
                    if hasattr(cloud.quantity, 'name'):
                        quantity_str = cloud.quantity.name
                    else:
                        quantity_str = str(cloud.quantity).replace('CloudQuantity.', '')
                    
                    cover_info = cloud_cover_map.get(quantity_str, {'description': quantity_str, 'oktas': ''})
                    
                    layer_info = {
                        'type': quantity_str,
                        'description': cover_info['description'],
                        'oktas': cover_info['oktas']
                    }
                    if hasattr(cloud, 'height') and cloud.height:
                        layer_info['base_m'] = round(cloud.height * 0.3048, 2)
                    cloud_layers_list.append(layer_info)
                
                result['cloud_layers'] = json.dumps(cloud_layers_list, ensure_ascii=False)
                if cloud_layers_list:
                    result['cloud_cover'] = cloud_layers_list[0]['type']
                    if 'base_m' in cloud_layers_list[0]:
                        result['cloud_base_m'] = cloud_layers_list[0]['base_m']
            
            # 温度信息（使用库的 max_temperature 和 min_temperature）
            max_temps = []
            min_temps = []
            
            # 最高温度
            if hasattr(taf, 'max_temperature') and taf.max_temperature:
                temp_info = taf.max_temperature
                temp_value = temp_info.temperature if hasattr(temp_info, 'temperature') else None
                temp_time = ''
                if hasattr(temp_info, 'day') and hasattr(temp_info, 'hour') and dt:
                    try:
                        temp_dt = datetime(dt.year, dt.month, temp_info.day, temp_info.hour, 0, 0)
                        temp_time = temp_dt.strftime('%Y-%m-%d %H:%M:%S')
                    except:
                        pass
                if temp_value is not None:
                    max_temps.append({'value': temp_value, 'time': temp_time})
            
            # 最低温度
            if hasattr(taf, 'min_temperature') and taf.min_temperature:
                temp_info = taf.min_temperature
                temp_value = temp_info.temperature if hasattr(temp_info, 'temperature') else None
                temp_time = ''
                if hasattr(temp_info, 'day') and hasattr(temp_info, 'hour') and dt:
                    try:
                        temp_dt = datetime(dt.year, dt.month, temp_info.day, temp_info.hour, 0, 0)
                        temp_time = temp_dt.strftime('%Y-%m-%d %H:%M:%S')
                    except:
                        pass
                if temp_value is not None:
                    min_temps.append({'value': temp_value, 'time': temp_time})
            
            # 保存温度结果
            result['max_temp_info'] = json.dumps(max_temps, ensure_ascii=False) if max_temps else None
            result['min_temp_info'] = json.dumps(min_temps, ensure_ascii=False) if min_temps else None
            result['max_temp_c'] = max_temps[0]['value'] if max_temps else None
            result['min_temp_c'] = min_temps[0]['value'] if min_temps else None
            
            # 趋势（BECMG, TEMPO, PROB等）
            trends = []
            trends_descriptions = []
            
            if hasattr(taf, 'trends') and taf.trends:
                # 临时调试：打印库解析的trends原始数据
                # print(f"    [DEBUG] 库解析得到 {len(taf.trends)} 个trends")
                
                for trend in taf.trends:
                    trend_type = str(trend.type) if hasattr(trend, 'type') else ''
                    trend_type = trend_type.replace('WeatherChangeType.', '')
                    
                    trend_info = {
                        'type': trend_type
                    }
                    
                    # 安全提取有效期（处理整数day/hour，包括24时特殊情况）
                    if hasattr(trend, 'validity') and trend.validity and dt:
                        try:
                            if hasattr(trend.validity, 'start_day') and hasattr(trend.validity, 'start_hour'):
                                start_day = trend.validity.start_day
                                start_hour = trend.validity.start_hour
                                # 处理24时：转换为次日00时
                                if start_hour == 24:
                                    from_dt = datetime(dt.year, dt.month, start_day, 0, 0, 0) + timedelta(days=1)
                                else:
                                    from_dt = datetime(dt.year, dt.month, start_day, start_hour, 0, 0)
                                trend_info['from'] = from_dt.strftime('%Y-%m-%d %H:%M:%S')
                            else:
                                trend_info['from'] = ''
                            
                            if hasattr(trend.validity, 'end_day') and hasattr(trend.validity, 'end_hour'):
                                end_day = trend.validity.end_day
                                end_hour = trend.validity.end_hour
                                # 处理24时：转换为次日00时
                                if end_hour == 24:
                                    to_dt = datetime(dt.year, dt.month, end_day, 0, 0, 0) + timedelta(days=1)
                                else:
                                    # 处理跨天跨月
                                    end_month = dt.month
                                    end_year = dt.year
                                    if hasattr(trend.validity, 'start_day') and end_day < trend.validity.start_day:
                                        end_month = dt.month + 1 if dt.month < 12 else 1
                                        if end_month == 1:
                                            end_year += 1
                                    to_dt = datetime(end_year, end_month, end_day, end_hour, 0, 0)
                                trend_info['to'] = to_dt.strftime('%Y-%m-%d %H:%M:%S')
                            else:
                                trend_info['to'] = ''
                        except (ValueError, AttributeError):
                            trend_info['from'] = ''
                            trend_info['to'] = ''
                    else:
                        trend_info['from'] = ''
                        trend_info['to'] = ''
                    
                    # 风信息
                    if hasattr(trend, 'wind') and trend.wind:
                        wind_deg = trend.wind.degrees if hasattr(trend.wind, 'degrees') else None
                        wind_speed = trend.wind.speed if hasattr(trend.wind, 'speed') else None
                        
                        trend_info['wind_direction_deg'] = wind_deg
                        # 转换为m/s（如果是节）
                        if wind_speed and hasattr(trend.wind, 'unit'):
                            if trend.wind.unit == 'KT':
                                trend_info['wind_speed_mps'] = round(wind_speed * 0.514444, 2)
                            else:
                                trend_info['wind_speed_mps'] = wind_speed
                        elif wind_speed:
                            trend_info['wind_speed_mps'] = wind_speed
                    
                    # 能见度信息（统一转换为米）
                    if hasattr(trend, 'visibility') and trend.visibility:
                        vis_distance = None
                        if hasattr(trend.visibility, 'distance'):
                            vis_distance = trend.visibility.distance
                        
                        # 处理不同类型的能见度值
                        if vis_distance is not None:
                            if isinstance(vis_distance, str):
                                s = vis_distance.strip().upper()
                                try:
                                    if 'SM' in s:
                                        # 英里转米：3SM -> 4828m, P6SM -> 9656m
                                        miles_str = s.replace('SM', '').replace('P', '').strip()
                                        miles = float(miles_str)
                                        trend_info['visibility_m'] = round(miles * 1609.34)
                                    elif '>' in s or 'KM' in s:
                                        trend_info['visibility_m'] = 10000
                                    elif 'M' in s and s.replace('M', '').isdigit():
                                        # 处理 "4000m" 格式
                                        vis_num = int(s.replace('M', '').strip())
                                        trend_info['visibility_m'] = 10000 if vis_num == 9999 else vis_num
                                    elif s == '9999':
                                        trend_info['visibility_m'] = 10000
                                    else:
                                        # 尝试直接解析数字
                                        val = int(float(s))
                                        trend_info['visibility_m'] = 10000 if val == 9999 else val
                                except:
                                    trend_info['visibility_m'] = None
                            elif isinstance(vis_distance, (int, float)):
                                vis_val = int(vis_distance)
                                trend_info['visibility_m'] = 10000 if vis_val == 9999 else vis_val
                    
                    trends.append(trend_info)
                    
                    # 生成描述
                    desc_parts = [trend_type]
                    if trend_info.get('from') and trend_info.get('to'):
                        desc_parts.append(f"Time: {trend_info['from']} - {trend_info['to']}")
                    if 'wind_direction_deg' in trend_info:
                        desc_parts.append(f"Wind: {trend_info['wind_direction_deg']}° {trend_info.get('wind_speed_mps', 0)} m/s")
                    if 'visibility_m' in trend_info:
                        desc_parts.append(f"Visibility: {trend_info['visibility_m']}m")
                    
                    trends_descriptions.append(' | '.join(desc_parts))
            
            if trends:
                result['trends'] = json.dumps(trends, ensure_ascii=False)
                result['trends_descriptions'] = '\n'.join(trends_descriptions)
            else:
                result['trends'] = ''
                result['trends_descriptions'] = ''
            
            return result
            
        except Exception as e:
            # 解析失败时记录日志并返回None
            # 使用集合记录已显示的错误类型，避免重复显示相同错误
            if not hasattr(self, '_shown_taf_errors'):
                self._shown_taf_errors = set()
            # 使用错误类型+错误消息前30字符作为键，区分不同错误
            error_msg = str(e)[:50]
            error_key = f"{type(e).__name__}:{error_msg}"
            if error_key not in self._shown_taf_errors:
                print(f"    ⚠️  Mivek TAF解析失败: {e}")
                print(f"        TAF: {taf_text[:100]}")
                self._shown_taf_errors.add(error_key)
            return None
    
    def parse_taf(self, taf_text: str, timestamp: str) -> Optional[Dict]:
        """解析TAF编码为结构化数据"""
        # 只使用库解析，不再fallback到自定义解码
        if MIVEK_AVAILABLE:
            result = self._parse_taf_with_mivek(taf_text, timestamp)
            if result:
                return result
        
        # 如果库解析失败或库不可用，直接返回None
        return None
    
    def parse_timestamp(self, timestamp_str: str) -> Optional[datetime]:
        """解析时间戳字符串"""
        try:
            # 格式: 11/11/2025 23:00-> 或 12/11/2025 03:00->
            timestamp_str = timestamp_str.replace('->', '').strip()
            return datetime.strptime(timestamp_str, '%d/%m/%Y %H:%M')
        except:
            try:
                # 尝试其他格式
                return datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
            except:
                return None
    
    def station_info_exists(self, station_code: str) -> bool:
        """检查站点信息是否已存在"""
        file_ext = 'json' if self.save_format == 'json' else 'csv'
        file_path = os.path.join(self.airport_path, f"{station_code}.{file_ext}")
        
        if not os.path.exists(file_path):
            return False
        
        if self.save_format == 'json':
            try:
                with self.file_lock:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        existing_data = json.load(f)
                        if not isinstance(existing_data, list):
                            existing_data = [existing_data]
                        # 检查是否已有该站点代码的记录（文件存在且有数据）
                        if len(existing_data) > 0:
                            return any(
                                item.get('station_code') == station_code 
                                for item in existing_data
                            )
                        return False
            except (json.JSONDecodeError, IOError, Exception):
                # 文件损坏或格式错误，认为不存在，允许重新保存
                return False
        else:
            # CSV格式：检查文件是否存在且有内容
            try:
                with self.file_lock:
                    with open(file_path, 'r', encoding='utf-8-sig') as f:
                        reader = csv.DictReader(f)
                        rows = list(reader)
                        # 检查是否已有该站点代码的记录
                        if len(rows) > 0:
                            return any(row.get('station_code') == station_code for row in rows)
                        return False
            except (IOError, Exception):
                # 文件损坏或格式错误，认为不存在，允许重新保存
                return False
    
    def save_station_info(self, station_info: Dict):
        """保存站点信息到CSV或JSON（如果已存在则跳过）"""
        station_code = station_info['code']
        
        # 检查站点信息是否已存在
        if self.station_info_exists(station_code):
            return
        
        # 根据保存格式选择文件扩展名
        file_ext = 'json' if self.save_format == 'json' else 'csv'
        file_path = os.path.join(self.airport_path, f"{station_code}.{file_ext}")
        
        # 获取当前爬取时间（UTC）
        crawl_time = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        
        station_data = {
            'station_code': station_info['code'],
            'station_name': station_info['name'],
            'wmo_index': station_info.get('wmo_index', ''),
            'wigos_id': station_info.get('wigos_id', ''),
            'latitude_deg': station_info.get('latitude', ''),
            'longitude_deg': station_info.get('longitude', ''),
            'altitude_m': station_info.get('altitude_m', ''),
            'country': station_info.get('country', ''),
            'crawl_time': crawl_time
        }
        
        if self.save_format == 'json':
            # 保存为JSON格式
            file_exists = os.path.exists(file_path)
            if file_exists:
                try:
                    with self.file_lock:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            existing_data = json.load(f)
                            if not isinstance(existing_data, list):
                                existing_data = [existing_data]
                except:
                    existing_data = []
            else:
                existing_data = []
            
            existing_data.append(station_data)
            
            with self.file_lock:
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(existing_data, f, ensure_ascii=False, indent=2)
        else:
            # 保存为CSV格式
            file_exists = os.path.exists(file_path)
            
            with self.file_lock:
                with open(file_path, 'a', newline='', encoding='utf-8-sig') as f:
                    writer = csv.DictWriter(f, fieldnames=[
                        'station_code', 'station_name', 'wmo_index', 'wigos_id',
                        'latitude_deg', 'longitude_deg', 'altitude_m', 'country', 'crawl_time'
                    ])
                    
                    if not file_exists:
                        writer.writeheader()
                    
                    writer.writerow(station_data)
    
    def sanitize_filename(self, filename: str) -> str:
        """清理文件名，移除非法字符"""
        # Windows文件名非法字符
        illegal_chars = r'[<>:"/\\|?*]'
        filename = re.sub(illegal_chars, '_', filename)
        # 移除首尾空格和点
        filename = filename.strip(' .')
        return filename
    
    def save_metar_taf_data(self, data: List[Dict], data_type: str, country_name: str, station_code: str):
        """保存METAR或TAF数据到CSV/JSON或数据库"""
        if not data:
            return
        
        # 如果启用数据库，使用批量插入（性能优化 + 内存优化）
        if self.use_database and self.db_inserter:
            try:
                if data_type == 'METAR':
                    # METAR使用单条插入（保证每条独立事务）
                    success_count = self.db_inserter.batch_insert_metar(data)
                else:  # TAF - 使用单条插入
                    success_count = self.db_inserter.batch_insert_taf(data)
                
                if success_count > 0:
                    # 批量插入会智能去重，所以success_count可能小于len(data)
                    # 只有当成功数显著低于预期时（<80%）才认为是异常
                    if success_count < len(data) * 0.8:
                        # 部分插入失败（成功率<80%）
                        error_msg = f"部分插入失败: 准备插入{len(data)}条，实际成功{success_count}条"
                        print(f"    ⚠ {data_type} {error_msg}")
                        self.log(f"{country_name} {station_code} {data_type} {error_msg}")
                    else:
                        # 成功率≥80%，认为正常（去重导致的少量差异）
                        # 移除正常入库的日志
                        # print(f"    {data_type}: +{success_count}条入库")
                        pass  # 不记录正常入库日志
                elif len(data) > 0:
                    # 有数据但成功数为0，说明批量插入失败
                    error_msg = f"批量插入失败: 准备插入{len(data)}条，但成功数为0"
                    print(f"    ✗ {data_type} {error_msg}")
                    self.log(f"{country_name} {station_code} {data_type} {error_msg}")
            except Exception as e:
                error_msg = f"批量入库失败: {e}"
                print(f"    ✗ {data_type} {error_msg}")
                self.log(f"{country_name} {station_code} {data_type} {error_msg}")
            return

        # NIL 报告已在解析阶段返回 None 被过滤，这里无需额外处理

        crawl_time = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        
        # 清理国家名称，确保可以作为文件夹名
        safe_country_name = self.sanitize_filename(country_name)
        
        # 创建目录结构
        country_dir = os.path.join(self.base_path, safe_country_name)
        data_dir = os.path.join(country_dir, data_type)
        os.makedirs(data_dir, exist_ok=True)
        
        # 根据保存格式选择文件扩展名
        file_ext = 'json' if self.save_format == 'json' else 'csv'
        file_path = os.path.join(data_dir, f"{station_code}.{file_ext}")
        
        # 为每条数据添加爬取时间，并处理JSON字段
        for row in data:
            # 添加更新时间字段（北京时间）
            row['update_time'] = crawl_time
            # 同时保留爬取时间字段，方便与SYNOP保持一致语义
            row['crawl_time'] = crawl_time
            # 初始化更新次数（对于历史数据，迁移脚本会补为0；此处保证新数据也有该字段）
            if 'update_count' not in row or row.get('update_count') is None:
                row['update_count'] = 0
            # 将JSON字符串字段转换为字典（如果保存为JSON格式）
            if self.save_format == 'json':
                # 处理云层信息字段
                if row.get('sky_condition') and isinstance(row['sky_condition'], str):
                    try:
                        row['sky_condition'] = json.loads(row['sky_condition'])
                    except:
                        pass
        
        if self.save_format == 'json':
            # 保存为JSON格式
            file_exists = os.path.exists(file_path)
            if file_exists:
                # 读取现有数据
                try:
                    with self.file_lock:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            existing_data = json.load(f)
                            if not isinstance(existing_data, list):
                                existing_data = []
                except:
                    existing_data = []
            else:
                existing_data = []
            
            # 创建已存在数据的唯一标识集合（用于去重）
            # 对于METAR：使用 station_code + observation_time（因为COR修正报会替换原数据）
            # 对于TAF：使用 station_code + issue_time（因为AMD修正报会替换原数据）
            # 同时记录原始文本，用于检测修正报
            existing_keys = set()  # 用于去重：station_code|time
            existing_raw_map = {}  # 用于检测修正报：key -> raw_text
            existing_index_map = {}  # 用于更新：key -> index in existing_data
            
            # 仅对最近100条记录建立索引，降低大文件上的去重/更新开销
            start_index = max(0, len(existing_data) - 100)
            for idx in range(start_index, len(existing_data)):
                item = existing_data[idx]
                if data_type == 'METAR':
                    station_code = item.get('station_code', '')
                    time_key = item.get('observation_time', '')
                    raw_key = item.get('raw_metar', '')
                    if station_code and time_key:
                        key = f"{station_code}|{time_key}"
                        existing_keys.add(key)
                        existing_raw_map[key] = raw_key
                        existing_index_map[key] = idx
                else:  # TAF
                    station_code = item.get('station_code', '')
                    time_key = item.get('timestamp_utc', '')  # 使用timestamp_utc代替issue_time
                    raw_key = item.get('raw_taf', '')
                    if station_code and time_key:
                        key = f"{station_code}|{time_key}"
                        existing_keys.add(key)
                        existing_raw_map[key] = raw_key
                        existing_index_map[key] = idx
            
            # 处理新数据：去重 + 检测修正报
            new_data = []
            updated_count = 0
            skipped_count = 0
            for item in data:
                if data_type == 'METAR':
                    station_code = item.get('station_code', '')
                    time_key = item.get('observation_time', '')
                    raw_key = item.get('raw_metar', '')
                    # 检查是否是修正报（AMD或COR）
                    is_correction = bool(re.search(r'\b(?:AMD|COR)\b', raw_key.upper())) if raw_key else False
                else:  # TAF
                    station_code = item.get('station_code', '')
                    time_key = item.get('timestamp_utc', '')  # 使用timestamp_utc代替issue_time
                    raw_key = item.get('raw_taf', '')
                    # 检查是否是修正报（AMD或COR）
                    is_correction = bool(re.search(r'\b(?:AMD|COR)\b', raw_key.upper())) if raw_key else False
                
                vis_value = item.get('visibility_m')
                if isinstance(vis_value, (int, float)) and int(vis_value) == 9999:
                    item['visibility_m'] = 10000
                
                if station_code and time_key:
                    key = f"{station_code}|{time_key}"
                    
                    if key in existing_keys:
                        # 记录已存在，检查是否是修正报
                        existing_raw = existing_raw_map.get(key, '')
                        
                        # 如果新数据是修正报，且原始文本不同，则更新原记录
                        if is_correction and existing_raw and raw_key != existing_raw:
                            # 更新现有记录
                            existing_idx = existing_index_map.get(key)
                            if existing_idx is not None:
                                old_record = existing_data[existing_idx]
                                # 保留首次爬取时间：crawl_time 继承旧记录的值
                                if isinstance(old_record, dict) and old_record.get('crawl_time'):
                                    item['crawl_time'] = old_record.get('crawl_time')
                                # 更新次数：在旧值基础上 +1
                                item['update_count'] = old_record.get('update_count', 0) + 1
                                item['update_time'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                                existing_data[existing_idx] = item  # 替换为修正后的数据
                                updated_count += 1
                            else:
                                # 如果找不到索引，添加为新记录
                                new_data.append(item)
                        else:
                            # 完全相同，跳过
                            skipped_count += 1
                    else:
                        # 新记录
                        new_data.append(item)
                        existing_keys.add(key)  # 添加到集合，避免新数据内部重复
                        if raw_key:
                            existing_raw_map[key] = raw_key
                else:
                    # 如果没有站点代码或时间，也保存（可能是特殊情况）
                    new_data.append(item)
            
            # 追加新数据
            if new_data:
                existing_data.extend(new_data)
            
            # 保存为JSON（使用文件锁保护）- 当有新数据或有修正报更新时都需要保存
            if new_data or updated_count > 0:
                with self.file_lock:
                    with open(file_path, 'w', encoding='utf-8') as f:
                        json.dump(existing_data, f, ensure_ascii=False, indent=2)
                
                # 只在有新增数据或更新时输出
                if len(new_data) > 0 or updated_count > 0:
                    status_parts = []
                    if len(new_data) > 0:
                        status_parts.append(f"+{len(new_data)}")
                    if updated_count > 0:
                        status_parts.append(f"更新{updated_count}")
                    if skipped_count > 0:
                        status_parts.append(f"跳过{skipped_count}")
                    summary = f"{data_type}: {', '.join(status_parts)}"
                    # 移除正常保存的日志
                    # print(f"    {summary}")
                    pass  # 不记录正常保存日志
        else:
            # 保存为CSV格式
            # 获取字段名（包含合并的JSON字段和单独字段）
            if data_type == 'METAR':
                fieldnames = [
                    'station_code', 'report_type', 'observation_time',
                    'is_auto', 'is_corrected', 
                    'wind_info',  # JSON格式：合并风向风速信息（统一公制单位）
                    'wind_direction_deg', 'wind_speed_mps', 'wind_gust_mps',  # 单独字段（向后兼容，公制单位）
                    'wind_variable', 'wind_direction_variation', 
                    'visibility_m', 'visibility_direction',
                    'weather_phenomena', 'weather_phenomena_all', 'weather_phenomena_descriptions',  # Weather phenomena and English descriptions
                    'cloud_layers',  # JSON格式：合并所有云层信息（统一公制单位）
                    'cloud_cover', 'cloud_base_m',  # 单独字段（向后兼容，公制单位）
                    'rvr_runway', 'rvr_value_m', 'rvr_trend', 'rvr_trend_description',  # RVR及趋势 
                    'temperature_c', 'dewpoint_c', 'temperature_precise_c', 'dewpoint_precise_c',
                    'altimeter_hpa', 'pressure_tendency', 'temperature_trend', 
                    'remarks', 'extended_info',
                    # 备注中提取的字段
                    'auto_station_type',  # 自动站类型（AO1/AO2）
                    'sea_level_pressure_hpa',  # 海平面气压（百帕）
                    'precipitation_1hr_mm',  # 1小时降水量（毫米）
                    'precipitation_6hr_mm',  # 6小时降水量（毫米）
                    'precipitation_24hr_mm',  # 24小时降水量（毫米）
                    'snow_depth_mm',  # 雪深（毫米）
                    'peak_wind_direction_deg',  # 峰值阵风风向（度）
                    'peak_wind_speed_kt',  # 峰值阵风风速（节）
                    'peak_wind_time',  # 峰值阵风时间
                    'pressure_change',  # 气压变化（PRESFR/PRESRR）
                    'raw_metar', 'crawl_time'
                ]
            else:  # TAF
                fieldnames = [
                    'station_code', 'timestamp_utc', 'valid_from', 'valid_to',
                    'wind_direction_deg', 'wind_speed_mps', 'wind_gust_mps',
                    'visibility_m', 'weather_phenomena', 'cloud_cover', 'cloud_base_m', 'cloud_layers',
                    'max_temp_c', 'min_temp_c', 'trends', 'trends_descriptions',
                    'is_corrected',
                    'raw_taf', 'crawl_time'
                ]
            
            # 检查文件是否存在
            file_exists = os.path.exists(file_path)
            
            # 读取现有数据用于去重（仅比较最近100条记录）
            existing_keys = set()
            if file_exists:
                try:
                    with self.file_lock:
                        with open(file_path, 'r', encoding='utf-8-sig') as f:
                            reader = csv.DictReader(f)
                            rows = list(reader)
                            # 仅使用最近100条记录来构建去重键
                            for row in rows[-100:]:
                                if data_type == 'METAR':
                                    raw_key = row.get('raw_metar', '')
                                    time_key = row.get('observation_time', '')
                                else:  # TAF
                                    raw_key = row.get('raw_taf', '')
                                    time_key = row.get('timestamp_utc', '')  # 使用timestamp_utc代替issue_time
                                
                                if raw_key and time_key:
                                    existing_keys.add(f"{raw_key}|{time_key}")
                except Exception:
                    existing_keys = set()
            
            # 过滤新数据，只保留不存在的记录
            new_data = []
            skipped_count = 0
            for row in data:
                if data_type == 'METAR':
                    raw_key = row.get('raw_metar', '')
                    time_key = row.get('observation_time', '')
                else:  # TAF
                    raw_key = row.get('raw_taf', '')
                    time_key = row.get('timestamp_utc', '')  # 使用timestamp_utc代替issue_time
                
                if raw_key and time_key:
                    key = f"{raw_key}|{time_key}"
                    if key not in existing_keys:
                        new_data.append(row)
                        existing_keys.add(key)  # 添加到集合，避免新数据内部重复
                    else:
                        skipped_count += 1
                else:
                    # 如果没有原始文本或时间，也保存（可能是特殊情况）
                    new_data.append(row)
            
            # 只保存新数据
            if new_data:
                with self.file_lock:
                    with open(file_path, 'a', newline='', encoding='utf-8-sig') as f:
                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                        
                        if not file_exists:
                            writer.writeheader()
                        
                        # 写入新数据
                        for row in new_data:
                            writer.writerow(row)
                
                # 只在有新增数据时输出
                if len(new_data) > 0:
                    status_parts = []
                    status_parts.append(f"+{len(new_data)}")
                    if skipped_count > 0:
                        status_parts.append(f"跳过{skipped_count}")
                    summary = f"{data_type}: {', '.join(status_parts)}"
                    # 移除正常保存的日志
                    # print(f"    {summary}")
                    pass  # 不记录正常保存日志
    
    def _process_station(self, station: Dict, country: Dict, station_index: int, total_stations: int):
        """处理单个站点（用于多线程）"""
        station_code = station['code']
        station_name = station['name']
        
        try:
            # 移除正常站点的处理日志，只保留错误日志
            # print(f"  [{station_index}/{total_stations}] 处理站点: {station_code} - {station_name}")
            # self.log(f"[{station_index}/{total_stations}] Processing station: {station_code} - {station_name}")
            
            # 获取站点信息
            station_info = self.get_station_info(station_code, station_name, country['name'])
            self.save_station_info(station_info)
            
            # 获取METAR和TAF数据
            metar_data, taf_data = self.get_station_metar_taf(station_code, station_name)
            
            # 将经纬度和国家信息附加到每条记录（字段名与 SYNOP 对齐）
            latitude = station_info.get('latitude')
            longitude = station_info.get('longitude')
            country_name = station_info.get('country') or country['name']
            if metar_data:
                for rec in metar_data:
                    if latitude is not None:
                        rec['latitude'] = latitude
                    if longitude is not None:
                        rec['longitude'] = longitude
                    rec['country'] = country_name
            if taf_data:
                for rec in taf_data:
                    if latitude is not None:
                        rec['latitude'] = latitude
                    if longitude is not None:
                        rec['longitude'] = longitude
                    rec['country'] = country_name

            # 保存数据
            if metar_data:
                self.save_metar_taf_data(
                    metar_data, 'METAR', country['name'], station_code
                )
            
            if taf_data:
                self.save_metar_taf_data(
                    taf_data, 'TAF', country['name'], station_code
                )
            
            # 线程安全的统计更新和内存清理
            should_gc = False
            current_count = 0
            with self.stats_lock:  # 使用专门的stats_lock保护统计信息
                self.processed_count += 1
                
                # 定期清理内存（快速清理）
                if self.processed_count % self.memory_cleanup_interval == 0:
                    should_gc = True
                    current_count = self.processed_count
            
            # 在锁外执行GC和日志（避免长时间持锁）
            if should_gc:
                self.clean_memory()
                # 移除内存清理的详细日志
                # with self.log_lock:
                #     print(f"  [内存清理] 已处理 {current_count} 个站点")
            
            time.sleep(self.delay)
            # 移除正常站点的处理完成日志
            # self.log(
            #     f"Station {station_code} ({station_name}) processed "
            #     f"(METAR records: {len(metar_data)}, TAF records: {len(taf_data)})"
            # )
            return True, station_code
            
        except Exception as e:
            print(f"  处理站点 {station_code} 时出错: {e}")
            # 线程安全的统计更新
            with self.log_lock:
                self.stats['failed_stations'] += 1
            self.log(f"Station {station_code} ({station_name}) failed: {e}")
            return False, station_code

    def _process_country(self, country: Dict, country_index: int, total_countries: int, use_multithreading: bool):
        """处理单个国家的所有站点（用于国家级别多线程）"""
        country_name = country['name']
        country_code = country['code']

        print(f"\n{'='*80}")
        print(f"[{country_index}/{total_countries}] 处理国家: {country_name} ({country_code})")
        print('='*80)
        self.log(f"[{country_index}/{total_countries}] Processing country: {country_name} ({country_code})")
        self.log(f"Start country {country_name} ({country_code}) [{country_index}/{total_countries}]")

        try:
            # 获取站点列表
            stations = self.get_stations_for_country(country_code, country_name)
            time.sleep(self.delay)

            if not stations:
                print(f"  未找到站点，跳过")
                self.log(f"No stations found for {country_name} ({country_code}), skipping")
                return

            # 线程安全的统计更新
            with self.log_lock:
                self.stats['total_stations'] += len(stations)
            # 移除站点数量的详细日志
            # self.log(f"Country {country_name} has {len(stations)} stations")

            # 站点级别的executor（并发处理站点）
            station_executor = ThreadPoolExecutor(max_workers=self.max_workers) if use_multithreading else None
            station_futures = []

            try:
                if station_executor:
                    # 站点级别并发
                    for j, station in enumerate(stations, 1):
                        station_futures.append(
                            station_executor.submit(
                                self._process_station,
                                station,
                                country,
                                j,
                                len(stations)
                            )
                        )

                    # 等待所有站点处理完成
                    for future in as_completed(station_futures):
                        try:
                            future.result()
                        except Exception as e:
                            print(f"  站点线程出错: {e}")
                            self.log(f"Station thread error in {country_name}: {e}")
                else:
                    # 单线程模式
                    for j, station in enumerate(stations, 1):
                        self._process_station(station, country, j, len(stations))
            finally:
                if station_executor:
                    station_executor.shutdown(wait=True)

            print(f"  ✓ {country_name} 处理完成")
            self.log(f"Country {country_name} processing completed")
            
            # 处理完成后清理内存
            del stations
            self.clean_memory()
            
            # 每处理5个国家执行一次深度清理
            if country_index % 5 == 0:
                self._perform_deep_cleanup()

        except Exception as e:
            print(f"处理国家 {country_name} 时出错: {e}")
            self.log(f"Error processing country {country_name} ({country_code}): {e}")
        finally:
            # 确保资源释放
            gc.collect()

    def scrape_all(self, start_country_index=0, max_countries=None, selected_countries=None, use_multithreading=True):
        """爬取所有国家的数据（两级并发：国家级 + 站点级）"""
        # 重置统计信息
        self.stats = {
            'total_stations': 0,
            'processed_stations': 0,
            'failed_stations': 0,
            'start_time': datetime.now(),
            'end_time': None
        }
        
        # 重置处理计数（用于内存清理）
        self.processed_count = 0

        if selected_countries:
            # 使用选定的国家列表
            countries = selected_countries
        else:
            # 获取所有国家
            countries = self.get_country_list()

            if not countries:
                print("无法获取国家列表")
                return

            # 应用范围限制
            if max_countries:
                countries = countries[start_country_index:start_country_index + max_countries]
            else:
                countries = countries[start_country_index:]

        total_countries = len(countries)

        print(f"\n开始爬取 {total_countries} 个国家的数据...")
        self.log(f"开始爬取 {total_countries} 个国家的数据")
        if use_multithreading:
            print(f"使用两级并发模式：国家级别 + 站点级别，最大线程数: {self.max_workers}")

        # 国家级别的executor（并发处理国家）
        # 增加到4个国家并发，提升整体速度（性能优化）
        country_executor = ThreadPoolExecutor(max_workers=min(2, self.max_workers)) if use_multithreading else None
        country_futures = []

        try:
            if country_executor:
                # 国家级别并发
                for i, country in enumerate(countries, 1):
                    country_futures.append(
                        country_executor.submit(
                            self._process_country,
                            country,
                            i,
                            total_countries,
                            use_multithreading
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

        self.stats['end_time'] = datetime.now()
        elapsed_time = (self.stats['end_time'] - self.stats['start_time']).total_seconds()

        # 全局内存清理（爬取结束后）
        print("\n执行最终内存清理...")
        self.clean_memory(force=True)
        # 移除内存清理的详细日志

        print(f"\n{'='*80}")
        print("爬取完成！")
        print(f"总站点数: {self.stats['total_stations']}")
        print(f"成功处理: {self.stats['processed_stations']}")
        print(f"失败: {self.stats['failed_stations']}")
        print(f"耗时: {elapsed_time:.2f} 秒")
        print('='*80)
        self.log(
            f"爬取完成: 总站点数={self.stats['total_stations']}, "
            f"成功={self.stats['processed_stations']}, 失败={self.stats['failed_stations']}, "
            f"耗时={elapsed_time:.2f}秒"
        )

def run_scheduled_scrape(scraper, selected_countries=None, start_country_index=0, max_countries=None):
    """执行定时爬取任务"""
    start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"\n[{start_time}] 开始定时爬取任务...")
    scraper.log(f"Scheduled scraping task started at {start_time}")
    try:
        if selected_countries:
            scraper.scrape_all(selected_countries=selected_countries, use_multithreading=True)
        else:
            scraper.scrape_all(
                start_country_index=start_country_index, 
                max_countries=max_countries, 
                use_multithreading=True
            )
        end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{end_time}] 定时爬取任务完成")
        scraper.log(f"Scheduled scraping task completed at {end_time}")
    except Exception as e:
        error_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{error_time}] 定时爬取任务出错: {e}")
        scraper.log(f"Scheduled scraping task failed at {error_time}: {e}")

def main():
    print("="*80)
    print("OGIMET METAR/TAF 数据爬虫")
    print("="*80)
    
    # 询问保存方式
    print("\n请选择保存方式:")
    print("  1. 保存到本地文件（JSON格式）")
    print("  2. 实时入库到PostgreSQL数据库")
    save_choice = input("\n请输入选项 (1-2): ").strip()
    
    use_database = (save_choice == '2')
    save_format = 'json' if save_choice == '1' else 'db'
    
    # 创建爬虫实例
    # max_workers: 多线程数量
    # 优化后配置: 15线程, 0.5秒延迟
    # 如果遇到频繁失败，可以降低到12线程和0.8秒延迟
    scraper = OgimetScraper(
        delay=0.3, 
        save_format=save_format, 
        max_workers=15,
        use_database=use_database
    )
    
    print("\n请选择爬取模式：")
    print("  1. 爬取所有国家")
    print("  2. 测试模式（只爬取前2个国家）")
    print("  3. 定时爬取模式")
    print("  4. 退出")
    
    while True:
        choice = input("\n请输入选项 (1-4): ").strip()
        
        if choice == '1':
            # 爬取所有国家
            confirm = input("\n确认爬取所有国家？这可能需要很长时间 (y/n): ").strip().lower()
            if confirm in ['y', 'yes']:
                scraper.scrape_all(use_multithreading=True)
            break
        elif choice == '2':
            # 测试模式
            print("\n测试模式：只爬取前2个国家")
            scraper.scrape_all(start_country_index=0, max_countries=2, use_multithreading=True)
            break
        elif choice == '3':
            # 定时爬取模式
            if not SCHEDULE_AVAILABLE:
                print("\n❌ 定时任务功能需要安装 schedule 库")
                print("安装命令: pip install schedule")
                print("安装后请重新运行程序")
                continue
            
            print("\n定时爬取模式")
            print("请选择定时任务配置：")
            print("  1. 每小时爬取一次")
            print("  2. 每6小时爬取一次")
            print("  3. 每天爬取一次（指定时间）")
            print("  4. 自定义分钟间隔（例如：每10分钟）")
            print("  5. 返回主菜单")
            
            schedule_choice = input("\n请输入选项 (1-5): ").strip()
            
            if schedule_choice == '1':
                # 每小时爬取
                print("\n已设置：每小时爬取一次")
                # 立即执行一次
                run_scheduled_scrape(scraper)
                # 设置定时任务
                schedule.every().hour.do(run_scheduled_scrape, scraper)
                print("\n定时任务已启动，按 Ctrl+C 停止")
                while True:
                    schedule.run_pending()
                    time.sleep(60)  # 每分钟检查一次
            elif schedule_choice == '2':
                # 每6小时爬取
                print("\n已设置：每6小时爬取一次")
                run_scheduled_scrape(scraper)
                schedule.every(6).hours.do(run_scheduled_scrape, scraper)
                print("\n定时任务已启动，按 Ctrl+C 停止")
                while True:
                    schedule.run_pending()
                    time.sleep(60)
            elif schedule_choice == '3':
                # 每天指定时间爬取
                print("\n已设置：每天指定时间爬取")
                time_str = input("请输入时间 (格式: HH:MM，例如 02:00): ").strip()
                try:
                    hour, minute = map(int, time_str.split(':'))
                    run_scheduled_scrape(scraper)
                    schedule.every().day.at(f"{hour:02d}:{minute:02d}").do(
                        run_scheduled_scrape, scraper
                    )
                    print(f"\n定时任务已启动，每天 {time_str} 执行，按 Ctrl+C 停止")
                    while True:
                        schedule.run_pending()
                        time.sleep(60)
                except ValueError:
                    print("❌ 时间格式错误，请使用 HH:MM 格式")
            elif schedule_choice == '4':
                # 自定义分钟间隔
                print("\n自定义分钟间隔爬取")
                print("提示：建议间隔不少于10分钟，以避免频繁请求")
                try:
                    minutes = int(input("请输入间隔分钟数（例如：10）: ").strip())
                    if minutes < 1:
                        print("❌ 间隔必须大于0分钟")
                        continue
                    if minutes < 10:
                        confirm = input(f"⚠️  间隔 {minutes} 分钟可能过于频繁，确认继续？(y/n): ").strip().lower()
                        if confirm not in ['y', 'yes']:
                            continue
                    
                    print(f"\n立即执行第一次爬取...")
                    run_scheduled_scrape(scraper)
                    schedule.every(minutes).minutes.do(run_scheduled_scrape, scraper)
                    print(f"\n✓ 定时任务已启动，每 {minutes} 分钟执行一次")
                    print("按 Ctrl+C 停止")
                    
                    while True:
                        schedule.run_pending()
                        time.sleep(30)  # 每30秒检查一次
                except ValueError:
                    print("❌ 请输入有效的数字")
                except KeyboardInterrupt:
                    print("\n\n定时任务已停止")
                    break
            elif schedule_choice == '5':
                continue
            else:
                print("❌ 无效选项")
        elif choice == '4':
            print("已退出")
            break
        else:
            print("❌ 无效选项，请输入 1-4")

if __name__ == "__main__":
    main()
