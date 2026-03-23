"""
SkyVector 优化覆盖爬虫 - 去除冗余区域
合并小区域，删除无数据区域，确保完整覆盖但减少请求次数
添加METAR/TAF解析功能，保存为JSON格式
"""
import requests
import json
import time
import os
import re
import gc
import threading
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

AIRPORT_INDEX: Dict[str, Dict[str, str]] = {}


def _load_airport_index() -> None:
    global AIRPORT_INDEX
    if AIRPORT_INDEX:
        return
    base_dir = os.path.join(os.path.dirname(__file__), 'data', 'airport')
    if not os.path.isdir(base_dir):
        return
    try:
        for filename in os.listdir(base_dir):
            if not filename.lower().endswith('.json'):
                continue
            full_path = os.path.join(base_dir, filename)
            try:
                with open(full_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            except Exception:
                continue
            if isinstance(data, list) and data:
                rec = data[0]
            elif isinstance(data, dict):
                rec = data
            else:
                continue
            if not isinstance(rec, dict):
                continue
            code = rec.get('station_code')
            if not code:
                continue
            AIRPORT_INDEX[code] = {'country': rec.get('country')}
    except Exception:
        AIRPORT_INDEX = {}


def get_station_country(icao: str) -> Optional[str]:
    if not icao:
        return None
    _load_airport_index()
    info = AIRPORT_INDEX.get(icao)
    if not info:
        return None
    return info.get('country')
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

# 导入数据库插入模块
try:
    import sys
    sys.path.insert(0, os.path.dirname(__file__))
    from db_inserter_v2 import DatabaseInserter
    DB_INSERTER_AVAILABLE = True
except ImportError:
    DB_INSERTER_AVAILABLE = False
    print("警告: db_inserter_v2模块未找到，数据库功能将不可用")


class SkyVectorParser:
    """SkyVector数据解析器，参考ogimet_scraper.py的解析逻辑"""

    def __init__(self, use_database=False, max_workers=4):
        self.base_path = "skyvector"
        self.metar_path = os.path.join(self.base_path, "metar")
        self.taf_path = os.path.join(self.base_path, "taf")

        # 创建目录
        os.makedirs(self.metar_path, exist_ok=True)
        os.makedirs(self.taf_path, exist_ok=True)
        
        # 日志路径
        self.log_dir = "logs"
        os.makedirs(self.log_dir, exist_ok=True)
        self.log_file = os.path.join(self.log_dir, "skyvector_scrape.txt")
        
        # 并发配置
        self.max_workers = max_workers
        
        # 线程锁，用于保护共享资源
        self.log_lock = threading.Lock()
        self.stats_lock = threading.Lock()
        
        # 线程本地存储：每个线程独立的数据库连接和Session
        self.thread_local = threading.local()
        
        # 创建全局 Session 并配置连接池
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        
        self.session = requests.Session()
        
        # 配置重试策略
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        # 配置连接池：pool_connections是连接池数量，pool_maxsize是每个连接池的最大连接数
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=max_workers * 2,
            pool_maxsize=max_workers * 4
        )
        
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        print(f"✓ HTTP连接池已配置：pool_connections={max_workers * 2}, pool_maxsize={max_workers * 4}")
        
        # 数据库配置
        self.use_database = use_database and DB_INSERTER_AVAILABLE
        self.db_inserter = None  # 主线程的连接（用于非并行模式）
        
        if self.use_database:
            try:
                self.db_inserter = DatabaseInserter(table_prefix='skyvector')
                print(f"✓ 数据库入库功能已启用（SkyVector专用表，并发数={max_workers}）")
            except Exception as e:
                print(f"✗ 数据库连接失败: {e}")
                self.use_database = False
        
        # 解析统计
        self.parse_stats = {
            'metar_total': 0,
            'metar_success': 0,
            'metar_simple': 0,
            'taf_total': 0,
            'taf_success': 0,
            'taf_simple': 0
        }
        
        # 错误去重集合（线程安全）
        self._shown_metar_errors = set()
        self._shown_taf_errors = set()
        self._error_lock = threading.Lock()  # 保护错误集合的锁
        
        # 内存清理配置（优化：更频繁清理以节省内存）
        self.memory_cleanup_interval = 20  # 每处理20个站点清理一次内存（与ogimet一致）
        self.processed_count = 0  # 已处理站点计数
        
        # 错误缓存大小限制
        self.max_error_cache_size = 100
    
    def get_db_inserter(self):
        """获取线程本地的数据库连接（用于多线程模式）"""
        if not self.use_database:
            return None
        
        # 如果当前线程还没有数据库连接，创建一个
        if not hasattr(self.thread_local, 'db_inserter'):
            try:
                self.thread_local.db_inserter = DatabaseInserter(table_prefix='skyvector')
                thread_name = threading.current_thread().name
                with self.log_lock:
                    print(f"  [线程 {thread_name}] 创建独立数据库连接")
            except Exception as e:
                print(f"  [线程 {threading.current_thread().name}] 数据库连接失败: {e}")
                self.thread_local.db_inserter = None
        
        return getattr(self.thread_local, 'db_inserter', None)
    
    def close(self):
        """关闭所有数据库连接，释放资源"""
        try:
            # 关闭主线程的连接
            if hasattr(self, 'db_inserter') and self.db_inserter:
                if hasattr(self.db_inserter, 'close'):
                    self.db_inserter.close()
                elif hasattr(self.db_inserter, 'conn') and self.db_inserter.conn:
                    self.db_inserter.conn.close()
                self.db_inserter = None
                self.log("主线程数据库连接已关闭")
        except Exception as e:
            print(f"关闭主线程数据库连接时出错: {e}")
        
        # 清理ThreadLocal中所有线程的db_inserter
        try:
            if hasattr(self, 'thread_local'):
                import threading
                for thread in threading.enumerate():
                    if hasattr(thread, 'db_inserter'):
                        try:
                            if hasattr(thread.db_inserter, 'close'):
                                thread.db_inserter.close()
                            elif hasattr(thread.db_inserter, 'conn'):
                                thread.db_inserter.conn.close()
                        except:
                            pass
                # 删除thread_local对象
                del self.thread_local
                self.log("ThreadLocal数据已清理")
        except Exception as e:
            print(f"清理ThreadLocal时出错: {e}")
        
        # 清理所有缓存
        try:
            if hasattr(self, '_shown_metar_errors'):
                self._shown_metar_errors.clear()
            if hasattr(self, '_shown_taf_errors'):
                self._shown_taf_errors.clear()
        except Exception as e:
            print(f"清理缓存时出错: {e}")
        
        # 清理全局AIRPORT_INDEX
        try:
            global AIRPORT_INDEX
            AIRPORT_INDEX.clear()
        except:
            pass
        
        # 强制垃圾回收
        import gc
        gc.collect()
    
    def clean_memory(self, force: bool = False):
        """清理内存缓存和执行垃圾回收（线程安全）
        
        Args:
            force: 强制清理，忽略间隔限制
        """
        # 清理错误缓存（保留最近的max_error_cache_size个）
        with self._error_lock:
            if len(self._shown_metar_errors) > self.max_error_cache_size:
                error_list = list(self._shown_metar_errors)
                self._shown_metar_errors = set(error_list[-self.max_error_cache_size:])
            
            if len(self._shown_taf_errors) > self.max_error_cache_size:
                error_list = list(self._shown_taf_errors)
                self._shown_taf_errors = set(error_list[-self.max_error_cache_size:])
        
        # 执行垃圾回收
        collected = gc.collect()
        
        self.log(f"内存清理完成：回收 {collected} 个对象，METAR错误缓存: {len(self._shown_metar_errors)} 个，TAF错误缓存: {len(self._shown_taf_errors)} 个")
        

    def parse_metar(self, metar_text: str, station_data: Dict) -> Optional[Dict]:
        """解析METAR数据，只使用metar-taf-parser库"""
        if not metar_text or not metar_text.strip():
            return None
        
        # 线程安全的统计更新
        with self.stats_lock:
            self.parse_stats['metar_total'] += 1
            
        # 检查NIL报告
        if 'NIL=' in metar_text.upper() or metar_text.upper().strip().endswith('NIL'):
            with self.stats_lock:
                self.parse_stats['metar_success'] += 1
            return None
        
        if MIVEK_AVAILABLE:
            result = self._parse_metar_with_mivek(metar_text, station_data)
            if result:
                with self.stats_lock:
                    self.parse_stats['metar_success'] += 1
            return result
        else:
            # 库不可用，返回None
            return None
    
    def _parse_metar_with_mivek(self, metar_text: str, station_data: Dict) -> Optional[Dict]:
        """使用mivek库解析METAR"""
        try:
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
            
            # 清理不完整的关键字片段（末尾缺少必要信息）
            # 1. 清理不完整的TEMPO/BECMG/FM/TL（缺少时间值）
            clean_text = re.sub(r'\s+(TEMPO|BECMG|FM|TL)\s*$', '', clean_text, flags=re.IGNORECASE)
            clean_text = re.sub(r'\s+(TEMPO|BECMG)\s+(FM|TL)\s*$', '', clean_text, flags=re.IGNORECASE)
            # 清理TEMPO/BECMG后面跟不完整FM的所有情况（如：TEMPO FM 2040，注意不是只在末尾）
            clean_text = re.sub(r'\s+(TEMPO|BECMG)\s+FM\s*\d{0,4}.*$', '', clean_text, flags=re.IGNORECASE)

            # 清理不完整的跑道信息（RWY后面缺少完整信息）
            clean_text = re.sub(r'\s+R\d{0,2}[LCR]?/\s*$', '', clean_text)  # 末尾不完整的 R##/
            clean_text = re.sub(r'\s+R\d{0,2}[LCR]?\s*$', '', clean_text)  # 末尾不完整的 R##
            # 统一删除所有跑道能见度组（如 R02/0500、R02/////// 等），避免不规范格式触发解析错误
            clean_text = re.sub(r'\sR\d{2}[LCR]?[^ ]*', '', clean_text)

            # 3. 清理其他常见不完整片段
            clean_text = re.sub(r'\s+(NOSIG|RMK|RE)\s*$', '', clean_text, flags=re.IGNORECASE)
            metar = parser.parse(clean_text)

            # 优先使用API返回的观测时间(d字段)，比从METAR原文解析更准确（避免跨月问题）
            obs_time = station_data.get('d') if station_data.get('d') else self._extract_metar_time(metar_text)
            
            station_code = metar.station if hasattr(metar, 'station') else station_data.get('s', '')
            result = {
                'raw_metar': metar_text,
                'station_code': station_code,
                'observation_time': obs_time,
                'report_type': 'METAR',
                'is_auto': metar.auto if hasattr(metar, 'auto') else False,
                'is_corrected': is_corrected,
                'latitude': station_data.get('lat'),
                'longitude': station_data.get('lon'),
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
                'update_time': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            }
            country = get_station_country(station_code)
            if country is not None:
                result['country'] = country
            
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
            
            # 云层
            if hasattr(metar, 'clouds') and metar.clouds:
                cloud_layers_list = []
                for cloud in metar.clouds:
                    if hasattr(cloud.quantity, 'name'):
                        quantity_str = cloud.quantity.name
                    else:
                        quantity_str = str(cloud.quantity).replace('CloudQuantity.', '')
                    
                    layer_data = {'cover': quantity_str}
                    if hasattr(cloud, 'height') and cloud.height:
                        layer_data['height_m'] = round(cloud.height * 0.3048, 2)
                    cloud_layers_list.append(layer_data)
                
                result['sky_condition'] = json.dumps(cloud_layers_list, ensure_ascii=False)
            else:
                cloud_match = re.search(r'\b(CLR|SKC|NSC|FEW\d{3}|SCT\d{3}|BKN\d{3}|OVC\d{3}|VV\d{3})\b', metar_text)
                if cloud_match:
                    cloud_code = cloud_match.group(1)
                    if cloud_code in ['CLR', 'SKC', 'NSC']:
                        result['sky_condition'] = json.dumps([{'cover': cloud_code}], ensure_ascii=False)
                    else:
                        cloud_type = cloud_code[:3]
                        if len(cloud_code) > 3:
                            height_ft = int(cloud_code[3:]) * 100
                            height_m = round(height_ft * 0.3048, 2)
                            result['sky_condition'] = json.dumps([{'cover': cloud_type, 'height_m': height_m}], ensure_ascii=False)
                        else:
                            result['sky_condition'] = json.dumps([{'cover': cloud_type}], ensure_ascii=False)
            
            # 如果既没有云层信息又存在CAVOK，按CAVOK编码云况
            if not result.get('sky_condition') and 'CAVOK' in metar_text:
                result['sky_condition'] = json.dumps([{'cover': 'CAVOK'}], ensure_ascii=False)
            
            # 备注
            if hasattr(metar, 'remarks') and metar.remarks:
                result['remarks'] = ' '.join(metar.remarks)
            
            # 从备注中提取额外信息
            rmk_match = re.search(r'RMK\s+(.+?)(?=\s*=|$)', metar_text)
            if rmk_match:
                remarks_text = rmk_match.group(1).strip()
                if not result['remarks']:
                    result['remarks'] = remarks_text
                
                # 优先从库解析的remarks中提取海平面气压（更准确）
                # 如果库已经解析出SLP文本（如 "sea level pressure of 1038.4 HPa"），直接提取
                if hasattr(metar, 'remarks') and metar.remarks:
                    slp_text_match = re.search(r'sea level pressure of ([\d.]+)\s*H?Pa', ' '.join(metar.remarks), re.IGNORECASE)
                    if slp_text_match:
                        result['sea_level_pressure_hpa'] = round(float(slp_text_match.group(1)), 1)
                    else:
                        # 如果库没有解析出SLP文本，从原始报文中提取SLP编码
                        # METAR标准：SLP编码规则
                        # - 如果第一位是0-2，则SLP = 1000 + 后两位/10
                        # - 如果第一位是3-9，则SLP = 900 + 后两位/10
                        #   但如果第一位是3-5，且后两位<50，则实际气压可能在1000-1099之间，需要加100
                        slp_match = re.search(r'\bSLP(\d{3})\b', remarks_text)
                        if slp_match:
                            slp_code = slp_match.group(1)
                            if slp_code[0] in ['0', '1', '2']:
                                slp_value = 1000 + int(slp_code) / 10.0
                            else:
                                slp_value = 900 + int(slp_code) / 10.0
                                # 如果第一位是3-5，且后两位<50，说明实际气压在1000-1099之间，需要加100
                                # 例如：SLP384 -> 900+38.4=938.4 -> 1038.4
                                if slp_code[0] in ['3', '4', '5'] and int(slp_code[1:3]) < 50:
                                    slp_value += 100
                            result['sea_level_pressure_hpa'] = round(slp_value, 1)
                else:
                    # 如果库没有解析remarks，从原始报文中提取
                    slp_match = re.search(r'\bSLP(\d{3})\b', remarks_text)
                    if slp_match:
                        slp_code = slp_match.group(1)
                        if slp_code[0] in ['0', '1', '2']:
                            slp_value = 1000 + int(slp_code) / 10.0
                        else:
                            slp_value = 900 + int(slp_code) / 10.0
                            # 如果第一位是3-5，且后两位<50，说明实际气压在1000-1099之间，需要加100
                            if slp_code[0] in ['3', '4', '5'] and int(slp_code[1:3]) < 50:
                                slp_value += 100
                        result['sea_level_pressure_hpa'] = round(slp_value, 1)
                
                # 提取1小时降水量
                precip_1hr_match = re.search(r'\bP(\d{4})\b', remarks_text)
                if precip_1hr_match:
                    precip_inches = int(precip_1hr_match.group(1)) / 100.0
                    result['precip_1hr_mm'] = round(precip_inches * 25.4, 2)
            elif 'NOSIG' in metar_text:
                result['remarks'] = 'NOSIG'
            else:
                result['remarks'] = ''
            
            return result
            
        except Exception as e:
            # 解析失败时记录日志并返回None
            # 使用集合记录已显示的错误类型，避免重复显示相同错误（线程安全）
            error_msg = str(e)[:50]
            error_key = f"{type(e).__name__}:{error_msg}"
            
            # 线程安全的错误去重检查和添加
            should_print = False
            if "Unparsed groups" not in error_msg:
                with self._error_lock:
                    if error_key not in self._shown_metar_errors:
                        self._shown_metar_errors.add(error_key)
                        should_print = True
            
            if should_print:
                with self.log_lock:
                    print(f"    ⚠️  Mivek解析失败: {e}")
                    print(f"        METAR: {metar_text[:100]}")
            return None
    
    def _extract_metar_time(self, metar_text: str) -> str:
        """从METAR报文中提取观测时间（修复跨月问题）"""
        # 匹配格式：DDHHMMZ (如 210930Z = 21日09:30 UTC)
        time_match = re.search(r'\s(\d{2})(\d{2})(\d{2})Z', metar_text)
        if time_match:
            day = int(time_match.group(1))
            hour = int(time_match.group(2))
            minute = int(time_match.group(3))
            
            # 验证时间的合理性
            if day >= 1 and day <= 31 and hour <= 23 and minute <= 59:
                now = datetime.now(timezone.utc)
                try:
                    # 先尝试使用当前年月
                    obs_dt = datetime(now.year, now.month, day, hour, minute, 0, tzinfo=timezone.utc)
                    
                    # 如果观测时间在未来（超过2天），说明是上个月的数据
                    if obs_dt > now + timedelta(days=2):
                        # 回退到上个月
                        if now.month == 1:
                            obs_dt = datetime(now.year - 1, 12, day, hour, minute, 0, tzinfo=timezone.utc)
                        else:
                            obs_dt = datetime(now.year, now.month - 1, day, hour, minute, 0, tzinfo=timezone.utc)
                    
                    return obs_dt.strftime('%Y-%m-%d %H:%M:%S')
                except ValueError:
                    # 日期无效（如2月30日），使用当前时间
                    pass
        
        # 如果解析失败，返回当前时间
        return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    
    def _extract_taf_time(self, taf_text: str, station_data: Dict = None) -> str:
        """从TAF报文中提取发布时间（增强跨日/月/年处理）
        
        优先级：
        1. 从TAF原文解析发布时间 (DDHHMMZ 或 DDHHMM)
        2. 使用API返回的d字段（station_data['d']）
        3. 当前UTC时间
        """
        now = datetime.now(timezone.utc)
        
        # 方式1：匹配标准格式 TAF [AMD/COR] STATION DDHHMMZ 或 DDHHMM (如 TAF AMD ZBAA 210900Z 或 TAF OPLA 190500)
        time_match = re.search(r'TAF(?:\s+(?:AMD|COR))?\s+[A-Z0-9]{4}\s+(\d{2})(\d{2})(\d{2})Z?(?:\s|$)', taf_text, re.IGNORECASE)
        if time_match:
            day = int(time_match.group(1))
            hour = int(time_match.group(2))
            minute = int(time_match.group(3))
            
            result = self._resolve_taf_datetime(now, day, hour, minute)
            if result:
                return result
        
        # 方式2：使用API返回的d字段
        if station_data and station_data.get('d'):
            api_time = station_data.get('d')
            # 验证格式是否正确 (YYYY-MM-DD HH:MM:SS)
            if re.match(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', api_time):
                return api_time
        
        # 方式3：如果解析失败，返回当前UTC时间
        return now.strftime('%Y-%m-%d %H:%M:%S')
    
    def _resolve_taf_datetime(self, now: datetime, day: int, hour: int, minute: int) -> Optional[str]:
        """根据日时分解析完整的TAF时间，选择早于当前UTC且最接近的时间"""
        if not (1 <= day <= 31 and 0 <= hour <= 23 and 0 <= minute <= 59):
            return None
        
        candidates = []
        
        # 当前月
        try:
            dt = datetime(now.year, now.month, day, hour, minute, 0, tzinfo=timezone.utc)
            candidates.append(dt)
        except ValueError:
            pass
        
        # 上个月
        prev_year = now.year if now.month > 1 else now.year - 1
        prev_month = now.month - 1 if now.month > 1 else 12
        try:
            dt = datetime(prev_year, prev_month, day, hour, minute, 0, tzinfo=timezone.utc)
            candidates.append(dt)
        except ValueError:
            pass
        
        # 下个月
        next_year = now.year if now.month < 12 else now.year + 1
        next_month = now.month + 1 if now.month < 12 else 1
        try:
            dt = datetime(next_year, next_month, day, hour, minute, 0, tzinfo=timezone.utc)
            candidates.append(dt)
        except ValueError:
            pass
        
        # 选择早于当前UTC时间且最接近的候选
        best_dt = None
        min_diff = None
        for dt in candidates:
            if dt > now:
                continue
            diff = (now - dt).total_seconds()
            if min_diff is None or diff < min_diff:
                min_diff = diff
                best_dt = dt
        
        if best_dt:
            return best_dt.strftime('%Y-%m-%d %H:%M:%S')
        return None
    
    
    def _format_sky_condition(self, sky_data) -> str:
        """格式化云层数据为JSON字符串"""
        if not sky_data:
            return None
        
        clouds = []
        for item in sky_data:
            if isinstance(item, tuple) and len(item) >= 2:
                cloud_dict = {
                    'cover': item[0],  # SKC, FEW, SCT, BKN, OVC
                }
                # 云高（转换为米）
                if item[1] and hasattr(item[1], 'value'):
                    cloud_dict['height_m'] = int(item[1].value() * 0.3048)
                
                # 云类型
                if len(item) > 2 and item[2]:
                    cloud_dict['type'] = item[2]
                
                clouds.append(cloud_dict)
        
        return json.dumps(clouds, ensure_ascii=False) if clouds else None
    
    def parse_taf(self, taf_text: str, station_data: Dict) -> Optional[Dict]:
        """解析TAF数据，只使用metar-taf-parser库"""
        if not taf_text or not taf_text.strip():
            return None
        
        # 线程安全的统计更新
        with self.stats_lock:
            self.parse_stats['taf_total'] += 1
            
        # 检查NIL报告 - 直接跳过
        # 兼容多种形式：
        # - "TAF XXXX DDHHMMZ NIL="
        # - "TAF XXXX DDHHMMZ NIL RMK ..."（如 CWSA 271940Z NIL RMK ...）
        upper_taf = re.sub(r'\s+', ' ', taf_text.upper()).strip()
        if ('NIL=' in upper_taf
            or upper_taf.endswith(' NIL')
            or ' NIL ' in upper_taf):
            with self.stats_lock:
                self.parse_stats['taf_success'] += 1
            return None
        
        if MIVEK_AVAILABLE:
            result = self._parse_taf_with_mivek(taf_text, station_data)
            if result:
                with self.stats_lock:
                    self.parse_stats['taf_success'] += 1
            return result
        else:
            # 库不可用，返回None
            return None
    
    def _parse_taf_with_mivek(self, taf_text: str, station_data: Dict) -> Optional[Dict]:
        """使用metar-taf-parser库解析TAF"""
        try:
            clean_text = taf_text.replace('=', '').strip()
            # 去掉 AMD/COR 修正标识，避免干扰库解析（是否为修正报由原始文本判断）
            clean_text = re.sub(r'\b(?:AMD|COR)\b\s*', '', clean_text, flags=re.IGNORECASE)
            if not clean_text.strip().upper().startswith('TAF'):
                clean_text = 'TAF ' + clean_text
            clean_text = re.sub(r'\s+', ' ', clean_text).strip()
            
            # 清理不完整的关键字片段（末尾缺少必要信息）
            # 1. 清理不完整的TEMPO/BECMG/FM/TL（缺少时间值）
            clean_text = re.sub(r'\s+(TEMPO|BECMG|FM|TL)\s*$', '', clean_text, flags=re.IGNORECASE)
            clean_text = re.sub(r'\s+(TEMPO|BECMG)\s+(FM|TL)\s*$', '', clean_text, flags=re.IGNORECASE)
            # 修复TEMPO/BECMG中的时间格式问题
            clean_text = re.sub(r'FM(\d)\s+(\d+)', r'FM\1\2', clean_text)
            clean_text = re.sub(r'TL(\d)\s+(\d+)', r'TL\1\2', clean_text)
            
            # 2. 清理不完整的跑道信息（RWY后面缺少完整信息）
            clean_text = re.sub(r'\s+R\d{0,2}[LCR]?/\s*$', '', clean_text)  # 末尾不完整的 R##/
            clean_text = re.sub(r'\s+R\d{0,2}[LCR]?\s*$', '', clean_text)  # 末尾不完整的 R##
            
            # 3. 清理其他常见不完整片段
            clean_text = re.sub(r'\s+(NOSIG|RMK|RE|PROB\d*)\s*$', '', clean_text, flags=re.IGNORECASE)
            
            # 4. 清理中间不完整的片段（关键字后面没有有效数字）
            clean_text = re.sub(r'\s+(TEMPO|BECMG)\s+(?=TEMPO|BECMG|PROB|TX|TN)', '', clean_text, flags=re.IGNORECASE)
            
            if len(clean_text) < 15:
                return None
                
            parser = TAFParser()
            taf = parser.parse(clean_text)
            
            station_code = taf.station if hasattr(taf, 'station') else station_data.get('s', '')
            country = get_station_country(station_code)
            # 优先从TAF原文解析发布时间，如果失败则使用API的d字段
            taf_time = self._extract_taf_time(taf_text, station_data)
            
            result = {
                'raw_taf': taf_text,
                'station_code': station_code,
                'timestamp_utc': taf_time,
                'is_corrected': bool(re.search(r'\b(?:AMD|COR)\b', taf_text.upper())),
                'latitude': station_data.get('lat'),
                'longitude': station_data.get('lon'),
                'country': country,
                'update_time': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # 解析有效期
            valid_from_set = False
            valid_to_set = False
            
            if hasattr(taf, 'validity') and taf.validity:
                try:
                    if hasattr(taf.validity, 'start_day') and hasattr(taf.validity, 'start_hour'):
                        start_day = taf.validity.start_day
                        start_hour = taf.validity.start_hour
                        now = datetime.now(timezone.utc)
                        valid_from_dt = datetime(now.year, now.month, start_day, start_hour, 0, 0)
                        result['valid_from'] = valid_from_dt.strftime('%Y-%m-%d %H:%M:%S')
                        valid_from_set = True
                    
                    if hasattr(taf.validity, 'end_day') and hasattr(taf.validity, 'end_hour'):
                        end_day = taf.validity.end_day
                        end_hour = taf.validity.end_hour
                        now = datetime.now(timezone.utc)
                        valid_to_dt = datetime(now.year, now.month, end_day, end_hour, 0, 0)
                        result['valid_to'] = valid_to_dt.strftime('%Y-%m-%d %H:%M:%S')
                        valid_to_set = True
                except:
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
                
                if time_match:
                    try:
                        now = datetime.now(timezone.utc)
                        
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
                            year = now.year
                            month = now.month
                            if has_issue_time and issue_day is not None:
                                # 如果有效开始日期小于发布时间日期，可能是下个月
                                if from_day < issue_day:
                                    month += 1
                                    if month > 12:
                                        month = 1
                                        year += 1
                            else:
                                # 没有发布时间，从当前时间推断
                                if from_day < now.day:
                                    month += 1
                                    if month > 12:
                                        month = 1
                                        year += 1
                            valid_from_dt = datetime(year, month, from_day, from_hour, 0, 0)
                            result['valid_from'] = valid_from_dt.strftime('%Y-%m-%d %H:%M:%S')
                        
                        # 构建有效结束时间
                        if not valid_to_set:
                            end_year = now.year
                            end_month = now.month
                            if has_issue_time and issue_day is not None:
                                if from_day < issue_day:
                                    end_month += 1
                                    if end_month > 12:
                                        end_month = 1
                                        end_year += 1
                            else:
                                if from_day < now.day:
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
            
            # 解析风信息
            if hasattr(taf, 'wind') and taf.wind:
                wind_deg = taf.wind.degrees
                if wind_deg == 'VRB':
                    result['wind_direction_deg'] = None
                else:
                    result['wind_direction_deg'] = int(wind_deg) if wind_deg else None
                
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
            
            # 解析云层
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
            now = datetime.now(timezone.utc)
            
            # 最高温度
            if hasattr(taf, 'max_temperature') and taf.max_temperature:
                temp_info = taf.max_temperature
                temp_value = temp_info.temperature if hasattr(temp_info, 'temperature') else None
                temp_time = ''
                if hasattr(temp_info, 'day') and hasattr(temp_info, 'hour'):
                    try:
                        temp_dt = datetime(now.year, now.month, temp_info.day, temp_info.hour, 0, 0)
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
                if hasattr(temp_info, 'day') and hasattr(temp_info, 'hour'):
                    try:
                        temp_dt = datetime(now.year, now.month, temp_info.day, temp_info.hour, 0, 0)
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
                for trend in taf.trends:
                    trend_type = str(trend.type) if hasattr(trend, 'type') else ''
                    trend_type = trend_type.replace('WeatherChangeType.', '')
                    
                    trend_info = {
                        'type': trend_type
                    }
                    
                    # 安全提取有效期（处理整数day/hour，包括24时特殊情况）
                    if hasattr(trend, 'validity') and trend.validity:
                        try:
                            if hasattr(trend.validity, 'start_day') and hasattr(trend.validity, 'start_hour'):
                                start_day = trend.validity.start_day
                                start_hour = trend.validity.start_hour
                                # 处理24时：转换为次日00时
                                if start_hour == 24:
                                    from_dt = datetime(now.year, now.month, start_day, 0, 0, 0) + timedelta(days=1)
                                else:
                                    from_dt = datetime(now.year, now.month, start_day, start_hour, 0, 0)
                                trend_info['from'] = from_dt.strftime('%Y-%m-%d %H:%M:%S')
                            else:
                                trend_info['from'] = ''
                            
                            if hasattr(trend.validity, 'end_day') and hasattr(trend.validity, 'end_hour'):
                                end_day = trend.validity.end_day
                                end_hour = trend.validity.end_hour
                                # 处理24时：转换为次日00时
                                if end_hour == 24:
                                    to_dt = datetime(now.year, now.month, end_day, 0, 0, 0) + timedelta(days=1)
                                else:
                                    # 处理跨天跨月
                                    end_month = now.month
                                    end_year = now.year
                                    if hasattr(trend.validity, 'start_day') and end_day < trend.validity.start_day:
                                        end_month = now.month + 1 if now.month < 12 else 1
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
            # 使用集合记录已显示的错误类型，避免重复显示相同错误（线程安全）
            error_msg = str(e)[:50]
            error_key = f"{type(e).__name__}:{error_msg}"
            
            # 线程安全的错误去重检查和添加
            should_print = False
            if "Unparsed groups" not in error_msg:
                with self._error_lock:
                    if error_key not in self._shown_taf_errors:
                        self._shown_taf_errors.add(error_key)
                        should_print = True
            
            if should_print:
                with self.log_lock:
                    print(f"    ⚠️  Mivek TAF解析失败: {e}")
                    print(f"        TAF: {taf_text[:100]}")
            return None
    
    def process_region(self, region_name, region_coords, region_index, total_regions, station_codes_set):
        """
        处理单个区域（线程安全版本）
        
        Args:
            region_name: 区域名称
            region_coords: (lat_min, lat_max, lon_min, lon_max)
            region_index: 区域索引
            total_regions: 总区域数
            station_codes_set: 全局站点代码集合（用于去重）
        
        Returns:
            (region_name, new_stations_count, metar_count, taf_count, success)
        """
        lat_min, lat_max, lon_min, lon_max = region_coords
        thread_name = threading.current_thread().name
        
        try:
            # 线程安全的日志输出
            with self.log_lock:
                print(f"\n[{region_index}/{total_regions}] {region_name} (线程: {thread_name})")
                print(f"  范围: 纬度 {lat_min}~{lat_max}, 经度 {lon_min}~{lon_max}")
            
            self.log(f"[{region_index}/{total_regions}] 处理区域: {region_name}")
            
            # 爬取区域数据（使用 Session 避免连接池溢出）
            weather_data = scrape_region(lat_min, lat_max, lon_min, lon_max, session=self.session)
            
            # 自适应细分：如果该区域返回的数据量过大，可能被API截断，进一步细分为4个子区域重爬
            if weather_data and len(weather_data) > 200:
                with self.log_lock:
                    print(f"  ⚠ 区域返回 {len(weather_data)} 条数据，尝试细分为4个子区域以获取更多站点")
                self.log(f"区域 {region_name}: 初始返回 {len(weather_data)} 条数据，进行自适应细分")
                
                mid_lat = (lat_min + lat_max) / 2
                mid_lon = (lon_min + lon_max) / 2
                sub_boxes = [
                    (lat_min, mid_lat, lon_min, mid_lon),  # 左下
                    (lat_min, mid_lat, mid_lon, lon_max),  # 右下
                    (mid_lat, lat_max, lon_min, mid_lon),  # 左上
                    (mid_lat, lat_max, mid_lon, lon_max),  # 右上
                ]
                
                combined_weather = []
                for sub_lat_min, sub_lat_max, sub_lon_min, sub_lon_max in sub_boxes:
                    # 注意：这里必须使用子区域的 (sub_lon_min, sub_lon_max)，不能写错
                    sub_data = scrape_region(sub_lat_min, sub_lat_max, sub_lon_min, sub_lon_max, session=self.session)
                    if sub_data:
                        combined_weather.extend(sub_data)
                
                # 如果细分后的结果比原始更多或相差不大，则使用细分结果
                if combined_weather and len(combined_weather) >= len(weather_data):
                    weather_data = combined_weather
                    with self.log_lock:
                        print(f"  ✓ 细分后共获取 {len(weather_data)} 条数据")
                    self.log(f"区域 {region_name}: 细分后共获取 {len(weather_data)} 条数据")
            
            if not weather_data:
                with self.log_lock:
                    print(f"  - 该区域无数据")
                self.log(f"区域 {region_name}: 无数据")
                return (region_name, 0, 0, 0, False)
            
            # 处理站点数据
            new_stations = []
            metar_list = []
            taf_list = []
            
            for station in weather_data:
                code = station.get('s')
                if not code:
                    continue
                
                # 线程安全的站点去重检查
                is_new = False
                with self.stats_lock:
                    if code not in station_codes_set:
                        station_codes_set.add(code)
                        is_new = True
                
                if is_new:
                    new_stations.append(code)
                    
                    # 解析METAR
                    metar_text = station.get('m')
                    if metar_text:
                        parsed_metar = self.parse_metar(metar_text, station)
                        if parsed_metar:
                            metar_list.append(parsed_metar)
                    
                    # 解析TAF
                    taf_text = station.get('t')
                    if taf_text:
                        parsed_taf = self.parse_taf(taf_text, station)
                        if parsed_taf:
                            taf_list.append(parsed_taf)
            
            # 批量入库（使用线程本地数据库连接）
            metar_inserted = 0
            taf_inserted = 0
            
            if metar_list or taf_list:
                db_inserter = self.get_db_inserter()
                
                if db_inserter:
                    # 优化：批量大小从50增加到100
                    if metar_list:
                        metar_inserted = db_inserter.batch_insert_metar(metar_list)
                    if taf_list:
                        taf_inserted = db_inserter.batch_insert_taf(taf_list)
                else:
                    # 没有数据库连接时使用JSON保存
                    if metar_list:
                        for parsed in metar_list:
                            code = parsed.get('station_code', 'UNKNOWN')
                            self.save_data([parsed], 'METAR', code)
                        metar_inserted = len(metar_list)
                    if taf_list:
                        for parsed in taf_list:
                            code = parsed.get('station_code', 'UNKNOWN')
                            self.save_data([parsed], 'TAF', code)
                        taf_inserted = len(taf_list)
            
            # 线程安全的统计更新和内存清理
            should_gc = False
            current_count = 0
            with self.stats_lock:
                self.processed_count += len(new_stations)
                # 定期清理内存
                if self.processed_count % self.memory_cleanup_interval == 0:
                    should_gc = True
                    current_count = self.processed_count
            
            # 在锁外执行GC和日志（避免长时间持锁）
            if should_gc:
                self.clean_memory()
                with self.log_lock:
                    print(f"  [内存清理] 已处理 {current_count} 个站点")
            
            # 线程安全的结果输出
            if new_stations:
                with self.log_lock:
                    print(f"  ✓ 获取 {len(weather_data)} 条数据，新增 {len(new_stations)} 个站点")
                    print(f"    解析保存: METAR {metar_inserted}条, TAF {taf_inserted}条")
                self.log(f"区域 {region_name}: 新增 {len(new_stations)} 个站点, METAR {metar_inserted}条, TAF {taf_inserted}条")
            else:
                with self.log_lock:
                    print(f"  ⚠ 获取 {len(weather_data)} 条数据，但都是重复站点")
                self.log(f"区域 {region_name}: 无新站点")
            
            return (region_name, len(new_stations), metar_inserted, taf_inserted, True)
            
        except Exception as e:
            with self.log_lock:
                print(f"  ✗ {region_name} 处理失败: {e}")
            self.log(f"区域 {region_name} 处理失败: {e}")
            return (region_name, 0, 0, 0, False)
        
        finally:
            # 清理局部大对象
            if 'weather_data' in locals():
                del weather_data
            if 'metar_list' in locals():
                del metar_list
            if 'taf_list' in locals():
                del taf_list
            if 'new_stations' in locals():
                del new_stations
            
            # 关闭当前线程的数据库连接（避免线程本地连接泄漏）
            if hasattr(self, 'thread_local') and hasattr(self.thread_local, 'db_inserter'):
                try:
                    if hasattr(self.thread_local.db_inserter, 'conn'):
                        self.thread_local.db_inserter.conn.close()
                        # 清除线程本地存储
                        delattr(self.thread_local, 'db_inserter')
                except Exception:
                    pass
            
            # 强制垃圾回收
            import gc
            gc.collect()
    
    def log(self, message: str):
        """写入日志文件"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_line = f"[{timestamp}] {message}"
        
        with self.log_lock:
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(log_line + '\n')
        # 如果日志写入出错，不影响主流程
        try:
            pass
        except:
            # 日志失败时避免影响主流程
            pass
    
    def save_data(self, data: List[Dict], data_type: str, station_code: str):
        """保存解析后的数据到JSON文件或数据库"""
        if not data:
            return
        
        # 如果启用数据库，使用批量插入（性能优化 + 内存优化）
        if self.use_database and self.db_inserter:
            try:
                if data_type == 'METAR':
                    # METAR使用批量插入（减小批次以节省内存）
                    success_count = self.db_inserter.batch_insert_metar(data)
                else:  # TAF - 使用批量插入
                    success_count = self.db_inserter.batch_insert_taf(data)
                
                if success_count > 0:
                    print(f"    {data_type}: +{success_count}条入库 -> {station_code}")
                    self.log(f"{station_code} {data_type}: {success_count}条入库")
            except Exception as e:
                print(f"    ✗ 批量入库失败: {e}")
                self.log(f"{station_code} {data_type} 批量入库失败: {e}")
            return
        
        # 直接保存到metar或taf文件夹
        if data_type == 'METAR':
            file_path = os.path.join(self.metar_path, f"{station_code}.json")
        else:  # TAF
            file_path = os.path.join(self.taf_path, f"{station_code}.json")
        
        # 读取现有数据
        existing_data: List[Dict] = []
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                    if not isinstance(existing_data, list):
                        existing_data = []
            except Exception:
                existing_data = []
        
        # 为现有数据构建索引，用于去重和修正报覆盖
        existing_keys = set()          # station_code|time
        existing_raw_map = {}          # key -> 原始报文
        existing_index_map = {}        # key -> 在 existing_data 中的索引
        
        for idx, item in enumerate(existing_data):
            if not isinstance(item, dict):
                continue
            if data_type == 'METAR':
                code = item.get('station_code', station_code)
                time_key = item.get('observation_time', '')
                raw_key = item.get('raw_metar', '')
            else:  # TAF
                code = item.get('station_code', station_code)
                time_key = item.get('timestamp_utc', '')
                raw_key = item.get('raw_taf', '')
            if code and time_key:
                key = f"{code}|{time_key}"
                existing_keys.add(key)
                existing_raw_map[key] = raw_key
                existing_index_map[key] = idx
        
        now_str = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        
        new_data: List[Dict] = []
        updated_count = 0
        skipped_count = 0
        
        for item in data:
            if not isinstance(item, dict):
                continue
            
            # 补充时间和计数字段
            if not item.get('crawl_time'):
                item['crawl_time'] = now_str
            if not item.get('update_time'):
                item['update_time'] = now_str
            if 'update_count' not in item or item.get('update_count') is None:
                item['update_count'] = 0
            
            # TAF 的 is_corrected 补充
            if data_type == 'TAF':
                raw_taf = item.get('raw_taf') or ''
                upper = raw_taf.upper()
                if 'is_corrected' not in item:
                    item['is_corrected'] = bool(re.search(r'\bAMD\b', upper)) if raw_taf else False
            
            if data_type == 'METAR':
                code = item.get('station_code', station_code)
                time_key = item.get('observation_time', '')
                raw_key = item.get('raw_metar', '')
                is_correction = bool(re.search(r'\b(?:AMD|COR)\b', (raw_key or '').upper()))
            else:  # TAF
                code = item.get('station_code', station_code)
                time_key = item.get('timestamp_utc', '')
                raw_key = item.get('raw_taf', '')
                is_correction = bool(re.search(r'\b(?:AMD|COR)\b', (raw_key or '').upper()))
            
            vis_value = item.get('visibility_m')
            if isinstance(vis_value, (int, float)) and int(vis_value) == 9999:
                item['visibility_m'] = 10000
            
            if code and time_key:
                key = f"{code}|{time_key}"
                
                if key in existing_keys:
                    # 已有记录，检查是否为修正报且原文不同
                    existing_raw = existing_raw_map.get(key, '')
                    if is_correction and existing_raw and raw_key != existing_raw:
                        existing_idx = existing_index_map.get(key)
                        if existing_idx is not None and 0 <= existing_idx < len(existing_data):
                            old_record = existing_data[existing_idx]
                            if isinstance(old_record, dict) and old_record.get('crawl_time'):
                                # 保留首次爬取时间
                                item['crawl_time'] = old_record.get('crawl_time')
                            # 更新次数在旧值基础上+1
                            item['update_count'] = old_record.get('update_count', 0) + 1

                            item['update_time'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                            existing_data[existing_idx] = item
                            updated_count += 1
                        else:
                            # 找不到旧索引时，当作新记录追加
                            new_data.append(item)
                    else:
                        # 完全相同或不是修正报，跳过
                        skipped_count += 1
                else:
                    # 全新记录
                    new_data.append(item)
                    existing_keys.add(key)
                    if raw_key:
                        existing_raw_map[key] = raw_key
                        # 索引仅针对已存在数据，新追加的记录会在下次调用时参与索引
            else:
                # 缺少关键字段时直接追加
                new_data.append(item)
        
        if new_data:
            existing_data.extend(new_data)
        
        # 有新增或更新时才写回文件
        if new_data or updated_count > 0:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(existing_data, f, ensure_ascii=False, indent=2)
            
            # 简要日志输出
            status_parts = []
            if new_data:
                status_parts.append(f"+{len(new_data)}")
            if updated_count > 0:
                status_parts.append(f"更新{updated_count}")
            if skipped_count > 0:
                status_parts.append(f"跳过{skipped_count}")
            summary = ', '.join(status_parts) if status_parts else '无变更'
            print(f"    {data_type}: {summary} -> {os.path.basename(file_path)}")
            self.log(f"{station_code} {data_type}: {summary} -> {os.path.basename(file_path)}")


def scrape_region(lat_min, lat_max, lon_min, lon_max, session=None):
    """爬取指定区域
    
    Args:
        session: requests.Session 对象（用于连接池复用）
    """
    center_lat = (lat_min + lat_max) / 2
    center_lon = (lon_min + lon_max) / 2
    
    # 动态计算res值（根据区域大小）
    # 区域越大，res应该越小（更详细），以获取更多站点
    lat_range = lat_max - lat_min
    lon_range = lon_max - lon_min
    area_size = lat_range * lon_range
    
    # 根据区域面积动态调整res（极限配置）
    # res值越小 = 缩放越近 = 站点越多
    # 参考：用户示例链接使用res=1222，我们用更激进的配置
    if area_size < 50:
        res_value = 300  # 极小区域，最高细节
    elif area_size < 150:
        res_value = 600  # 小区域，超高细节
    elif area_size < 300:
        res_value = 900  # 中等区域，高细节
    elif area_size < 500:
        res_value = 1100  # 较大区域，中等细节
    elif area_size < 800:
        res_value = 1400  # 大区域
    else:
        res_value = 1600  # 超大区域
    
    url = "https://skyvector.com/api/dLayer"
    params = {
        'll': f"{center_lat},{center_lon}",
        'll1': f"{lat_min},{lon_min}",
        'll2': f"{lat_max},{lon_max}",
        'pv': 0,
        'res': str(res_value),  # 动态res值
        'windMB': 300,
        'windZulu': int(datetime.now().timestamp()),
        'fuelcurr': 'USD',
        'layers': 'metar,taf,jeta',  # 添加taf图层！
        'rand': 37706,
        '_': int(datetime.now().timestamp() * 1000)  # 防缓存时间戳（毫秒）
    }
    
    # User-Agent轮换列表（模拟不同浏览器）
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    ]
    
    import random
    headers = {
        'User-Agent': random.choice(user_agents),
        'Referer': 'https://skyvector.com/'
    }
    
    # 使用提供的 session 或创建临时的 requests
    http_client = session if session else requests
    
    try:
        response = http_client.get(url, params=params, headers=headers, timeout=30)
        if response.status_code == 200:
            weather = response.json().get('weather', [])
            return [w for w in weather if w]
        return []
    except Exception as e:
        print(f"    请求错误: {str(e)[:80]}...")
        return []


def scrape_optimized_world(use_database=False, use_parallel=True, max_workers=6):
    """
    优化版全球覆盖 - 减少冗余区域，添加METAR/TAF解析
    
    Args:
        use_database: 是否使用数据库
        use_parallel: 是否使用并行处理
        max_workers: 最大并发数（建议3-5，避免触发限流）
    """
    
    # 记录开始时间
    start_time = datetime.now()
    
    # 创建解析器
    parser = SkyVectorParser(use_database=use_database, max_workers=max_workers)
    
    # 优化后的区域定义：合并小区域，删除无数据区域
    regions = {
        # ========== 北美洲 ==========
        "阿拉斯加": (55, 72, -180, -130),
        "加拿大西北部": (50, 75, -140, -95),
        "加拿大中南部（阿尔伯塔/萨斯喀彻温）": (42, 55, -120, -95),
        "加拿大东部": (42, 62, -95, -52),
        "格陵兰": (60, 84, -75, -10),
        "冰岛": (63, 67, -25, -13),
        
        # 美国细分（高密度地区）
        "美国-华盛顿/俄勒冈": (42, 49, -125, -116),
        "美国-加利福尼亚北部": (37, 42, -124, -119),
        "美国-加利福尼亚南部": (32, 37, -121, -114),
        "美国-内华达/犹他/爱达荷": (37, 45, -120, -109),
        "美国-亚利桑那/新墨西哥": (31, 37, -115, -103),
        "美国-蒙大拿/怀俄明": (41, 49, -116, -104),
        "美国-科罗拉多": (37, 41, -109, -102),
        "美国-德克萨斯北部": (32, 37, -107, -93),
        "美国-德克萨斯南部": (26, 32, -107, -93),
        "美国-大平原（北达科他/南达科他/内布拉斯加/堪萨斯）": (37, 49, -105, -94),
        "美国-五大湖区（明尼苏达/威斯康星/密歇根）": (42, 49, -97, -82),
        "美国-中西部（伊利诺伊/印第安纳/俄亥俄）": (37, 43, -92, -80),
        "美国-东北部（纽约/宾夕法尼亚/新英格兰）": (39, 48, -80, -66),
        "美国-东南部（弗吉尼亚/北卡/南卡/佐治亚）": (32, 39, -85, -75),
        "美国-佛罗里达": (24, 31, -88, -79),
        "美国-墨西哥湾沿岸（路易斯安那/密西西比/阿拉巴马）": (28, 33, -95, -85),
        
        "墨西哥": (14, 32, -117, -86),
        "中美洲": (7, 22, -92, -77),
        "巴拿马/哥伦比亚北部": (3, 11, -82, -66),
        "加勒比海": (10, 28, -85, -59),
        "夏威夷": (18, 23, -161, -154),
        
        # ========== 南美洲 ==========
        "哥伦比亚/委内瑞拉/圭亚那": (-1, 13, -80, -51),
        "厄瓜多尔/秘鲁": (-19, 2, -82, -68),
        "玻利维亚": (-23, -9, -70, -57),
        "巴西北部": (-10, 5, -75, -45),
        "巴西东南部": (-25, -5, -55, -35),
        "巴西南部": (-34, -20, -60, -44),
        "智利": (-56, -17, -76, -66),
        "阿根廷": (-55, -22, -74, -53),
        "乌拉圭/巴拉圭": (-36, -19, -63, -53),
        
        # ========== 欧洲（细分高密度）==========
        "北欧-挪威": (58, 72, 4, 32),
        "北欧-瑞典": (55, 70, 10, 25),
        "北欧-芬兰": (60, 70, 20, 32),
        "英国": (50, 61, -8, 2),
        "爱尔兰": (51, 56, -11, -5),
        "法国": (42, 52, -5, 8),
        "比荷卢": (49, 54, 2, 8),
        "德国": (47, 55, 6, 15),
        "波兰": (49, 55, 14, 25),
        "捷克/斯洛伐克/奥地利": (47, 51, 12, 19),
        "伊比利亚（西班牙/葡萄牙）": (36, 44, -10, 4),
        "意大利/瑞士/奥地利": (36, 49, 5, 19),
        "巴尔干半岛": (39, 48, 13, 30),
        "希腊/土耳其西部": (35, 42, 19, 30),
        
        "波罗的海国家/白俄罗斯": (44, 60, 20, 41),
        "乌克兰": (44, 53, 22, 41),
        
        "土耳其": (36, 42, 26, 45),
        
        # ========== 俄罗斯/中亚 ==========
        "俄罗斯西部": (48, 68, 30, 65),
        "俄罗斯西伯利亚": (50, 72, 65, 120),
        "俄罗斯远东": (43, 72, 120, 180),
        "俄罗斯北极群岛": (72, 82, 40, 180),
        
        "高加索地区": (38, 46, 38, 50),
        "中亚五国": (36, 54, 46, 88),
        
        # ========== 中东 ==========
        "黎凡特（以色列/黎巴嫩/叙利亚/约旦）": (29, 38, 32, 43),
        "伊拉克": (29, 38, 38, 49),
        "沙特阿拉伯": (16, 32, 34, 56),
        "也门/阿曼": (12, 27, 42, 60),
        "阿联酋/卡塔尔/巴林": (22, 27, 48, 57),
        "伊朗": (25, 40, 44, 64),
        
        # ========== 非洲 ==========
        "北非（埃及/利比亚/突尼斯/阿尔及利亚/摩洛哥）": (20, 38, -17, 37),
        "萨赫勒地区（毛里塔尼亚/马里/尼日尔/乍得）": (8, 24, -17, 24),
        "西非沿海": (4, 15, -18, 16),
        "东非（苏丹/埃塞俄比亚/索马里）": (0, 24, 21, 52),
        "中非": (-13, 8, 8, 42),
        "南部非洲（安哥拉/赞比亚/津巴布韦/莫桑比克）": (-27, -8, 11, 41),
        "南非/纳米比亚/博茨瓦纳": (-35, -17, 11, 33),
        
        "马达加斯加": (-26, -12, 43, 51),
        "印度洋岛屿（毛里求斯/留尼旺/塞舌尔）": (-22, -4, 43, 58),
        
        # ========== 南亚 ==========
        "巴基斯坦": (23, 37, 60, 78),
        "印度": (8, 36, 68, 93),
        "孟加拉国": (20, 27, 88, 93),
        "斯里兰卡": (5, 10, 79, 82),
        "马尔代夫": (-1, 8, 72, 74),
        "尼泊尔/不丹": (26, 31, 80, 93),
        "缅甸": (9, 29, 92, 102),
        
        # ========== 东南亚 ==========
        "泰国/老挝/柬埔寨": (5, 23, 97, 110),
        "越南": (8, 24, 102, 110),
        "马来西亚/新加坡/文莱": (1, 8, 99, 120),
        "印度尼西亚西部（苏门答腊/爪哇）": (-9, 6, 95, 116),
        "印度尼西亚东部（加里曼丹/苏拉威西）": (-10, 8, 108, 127),
        "印度尼西亚东部延伸（摩鹿加/西巴布亚）": (-10, 5, 127, 142),
        "菲律宾": (5, 21, 119, 127),
        
        # ========== 东亚 ==========
        "蒙古/中国内蒙古": (38, 53, 87, 127),
        "中国西部（新疆/西藏/青海/甘肃）": (27, 50, 75, 107),
        "中国东北": (38, 54, 110, 135),
        "中国-华北（京津冀/山西/内蒙古南部）": (35, 43, 105, 120),
        "中国-华东（江浙沪/安徽）": (28, 35, 115, 123),
        "中国-华中（豫鄂湘赣）": (26, 35, 108, 117),
        "中国-华南（粤闽）": (20, 28, 110, 120),
        "中国-西南（川渝）": (26, 34, 97, 110),
        "中国-西南（云贵）": (21, 30, 97, 110),
        "中国-南海诸岛": (3, 22, 110, 118),
        "中国-台湾": (21, 26, 119, 122),
        
        "朝鲜半岛（朝鲜/韩国）": (33, 43, 124, 131),
        "日本-九州/冲绳": (26, 34, 127, 132),
        "日本-本州西部（大阪/广岛）": (33, 36, 130, 137),
        "日本-本州中部（名古屋）": (34, 37, 136, 140),
        "日本-本州东部（东京）": (35, 37, 138, 142),
        "日本-本州北部（仙台）": (37, 41, 139, 142),
        "日本-北海道": (41, 46, 139, 146),
        
        # ========== 大洋洲 ==========
        "巴布亚新几内亚/所罗门群岛": (-12, 0, 140, 163),
        
        "澳大利亚-西澳（珀斯）": (-35, -25, 112, 120),
        "澳大利亚-西澳北部": (-25, -12, 112, 129),
        "澳大利亚-北领地": (-26, -10, 129, 138),
        "澳大利亚-昆士兰北部（凯恩斯）": (-20, -10, 138, 150),
        "澳大利亚-昆士兰南部（布里斯班）": (-30, -20, 145, 154),
        "澳大利亚-新南威尔士（悉尼）": (-38, -28, 140, 154),
        "澳大利亚-维多利亚（墨尔本）": (-39, -34, 140, 150),
        "澳大利亚-南澳（阿德莱德）": (-38, -26, 129, 142),
        "澳大利亚-塔斯马尼亚": (-44, -39, 144, 149),
        "新喀里多尼亚/瓦努阿图北部": (-23, -14, 163, 169),
        
        "新西兰": (-48, -34, 166, 179),
        
        # ========== 太平洋岛屿 ==========
        "密克罗尼西亚（关岛/帕劳/马绍尔群岛）": (4, 15, 130, 172),
        "美拉尼西亚（斐济/瓦努阿图）": (-22, -5, 155, -177),
        "波利尼西亚（萨摩亚/汤加/塔希提/库克群岛）": (-22, -8, -180, -134),
        
        # ========== 补充区域（岛屿覆盖优化）==========
        "大西洋中北部岛屿（加那利/亚速尔/百慕大）": (14, 40, -70, -10),
        "南大西洋岛屿（圣赫勒拿/福克兰）": (-60, -10, -65, 0),
        "印度洋中部岛屿（科科斯/查戈斯）": (-15, 5, 60, 105),
        "南太平洋东部（复活节岛/加拉帕戈斯）": (-30, 5, -135, -80),
        "亚北极岛屿（法罗/斯瓦尔巴/扬马延）": (60, 80, -30, 35),
        
        # ========== 极地 ==========
        # 南极和北冰洋通常没有民用气象站，可以删除
        # "南极": (-90, -60, -180, 180),
        # "北冰洋": (75, 90, -180, 180),
        "南极": (-90, -60, -180, 180),
        "北冰洋": (75, 90, -180, 180),
    }
    
    all_data = []
    station_codes = set()
    failed_regions = []
    no_new_data_regions = []
    
    # 重置处理计数（用于内存清理）
    parser.processed_count = 0
    
    total = len(regions)
    print(f"=== SkyVector 优化覆盖爬虫 ===\n")
    print(f"总共 {total} 个区域需要爬取")
    if use_parallel:
        print(f"并发模式: 启用 ({max_workers} 线程)")
        print(f"预计耗时: {total * 2 / 60 / max_workers:.1f} 分钟\n")
    else:
        print(f"并发模式: 禁用（单线程）")
        print(f"预计耗时: {total * 2 / 60:.1f} 分钟\n")
    print("="*60)
    parser.log(f"开始爬取 {total} 个区域的数据 (数据库={'启用' if use_database else '禁用'}, 并行={'启用' if use_parallel else '禁用'}, 线程数={max_workers if use_parallel else 1})")
    
    if use_parallel:
        # 并行模式：使用 ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="Region") as executor:
            # 提交所有区域任务
            future_to_region = {
                executor.submit(
                    parser.process_region,
                    name,
                    coords,
                    i,
                    total,
                    station_codes  # 共享的站点代码集合
                ): name
                for i, (name, coords) in enumerate(regions.items(), 1)
            }
            
            # 收集结果
            completed_count = 0
            total_new_stations = 0
            total_metar = 0
            total_taf = 0
            
            for future in as_completed(future_to_region):
                region_name = future_to_region[future]
                try:
                    result = future.result(timeout=120)  # 每个区域最多120秒
                    region_name, new_count, metar_count, taf_count, success = result
                    
                    completed_count += 1
                    total_new_stations += new_count
                    total_metar += metar_count
                    total_taf += taf_count
                    
                    if not success:
                        failed_regions.append(region_name)
                    elif new_count == 0:
                        no_new_data_regions.append(region_name)
                    
                    # 显示进度
                    if completed_count % 5 == 0:
                        with parser.log_lock:
                            print(f"\n进度: {completed_count}/{total} 区域完成，累计 {total_new_stations} 站点，METAR {total_metar}，TAF {total_taf}")
                    
                except Exception as e:
                    with parser.log_lock:
                        print(f"  ✗ 区域 {region_name} 异常: {e}")
                    failed_regions.append(region_name)
                    parser.log(f"区域 {region_name} 异常: {e}")
    else:
        # 单线程模式（原有逻辑）
        for i, (name, coords) in enumerate(regions.items(), 1):
            result = parser.process_region(name, coords, i, total, station_codes)
            region_name, new_count, metar_count, taf_count, success = result
            
            if success:
                if new_count > 0:
                    all_data.extend([{'s': code} for code in station_codes])  # 简化处理
                else:
                    no_new_data_regions.append(name)
            else:
                failed_regions.append(name)
            
            # 单线程时保留延迟
            if i < total:
                time.sleep(2)
    
    # 清理数据库连接和资源（总是执行）
    parser.close()
    
    # 全局内存清理（爬取结束后）
    print("\n执行最终内存清理...")
    parser.clean_memory(force=True)
    parser.log(f"最终内存清理完成，总处理 {parser.processed_count} 个站点")
    
    # 记录结束时间并计算耗时
    end_time = datetime.now()
    elapsed_time = (end_time - start_time).total_seconds()
    
    # 使用 parser.processed_count 作为准确的站点计数（并行模式下 all_data 可能为空）
    actual_station_count = parser.processed_count if parser.processed_count > 0 else len(all_data)
    parser.log(f"爬取完成: 总站点数 {actual_station_count}, 失败区域 {len(failed_regions)}, 无新数据区域 {len(no_new_data_regions)}, 耗时 {elapsed_time:.2f}秒")
    return all_data, failed_regions, no_new_data_regions, parser, elapsed_time


def save_results(all_data, failed_regions, no_new_data_regions, parser=None, elapsed_time=None):
    """保存结果"""
    
    print(f"\n{'='*60}")
    print("=== 爬取和解析完成 ===")
    print(f"{'='*60}\n")
    
    # 使用 parser.processed_count 作为准确的站点计数（并行模式下 all_data 可能为空）
    actual_count = parser.processed_count if parser and parser.processed_count > 0 else len(all_data)
    print(f"总站点数: {actual_count}")
    if elapsed_time is not None:
        print(f"耗时: {elapsed_time:.2f} 秒 ({elapsed_time/60:.2f} 分钟)")
    
    # 只有在 all_data 非空时才计算详细统计
    if all_data:
        with_metar = len([x for x in all_data if x.get('m')])
        with_taf = len([x for x in all_data if x.get('t')])
        with_coords = len([x for x in all_data if x.get('lat') and x.get('lon')])
        
        print(f"有 METAR: {with_metar} ({with_metar/len(all_data)*100:.1f}%)")
        print(f"有 TAF: {with_taf} ({with_taf/len(all_data)*100:.1f}%)")
        print(f"有经纬度: {with_coords} ({with_coords/len(all_data)*100:.1f}%)")
    
    # 显示解析统计
    if parser and hasattr(parser, 'parse_stats'):
        stats = parser.parse_stats
        print(f"\n解析统计:")
        if stats['metar_total'] > 0:
            metar_success_rate = (stats['metar_success'] / stats['metar_total']) * 100
            print(f"METAR解析: {stats['metar_success']}/{stats['metar_total']} ({metar_success_rate:.1f}%)")
            if stats['metar_simple'] > 0:
                print(f"  - 完整解析: {stats['metar_success']}")
                print(f"  - 简化解析: {stats['metar_simple']}")
        
        if stats['taf_total'] > 0:
            taf_success_rate = (stats['taf_success'] / stats['taf_total']) * 100
            print(f"TAF解析: {stats['taf_success']}/{stats['taf_total']} ({taf_success_rate:.1f}%)")
            if stats['taf_simple'] > 0:
                print(f"  - 完整解析: {stats['taf_success']}")
                print(f"  - 简化解析: {stats['taf_simple']}")
    
    # 统计解析后的文件
    metar_path = os.path.join("skyvector", "metar")
    taf_path = os.path.join("skyvector", "taf")
    
    metar_files = len([f for f in os.listdir(metar_path) if f.endswith('.json')]) if os.path.exists(metar_path) else 0
    taf_files = len([f for f in os.listdir(taf_path) if f.endswith('.json')]) if os.path.exists(taf_path) else 0
    
    print(f"\n解析结果:")
    print(f"METAR文件数: {metar_files}")
    print(f"TAF文件数: {taf_files}")
    print(f"保存路径: skyvector/metar/ 和 skyvector/taf/")
    
    print(f"\n站点代码前缀分布:")
    prefixes = {}
    for station in all_data:
        code = station.get('s', '')
        if code:
            prefix = code[0]
            prefixes[prefix] = prefixes.get(prefix, 0) + 1
    
    for prefix, count in sorted(prefixes.items(), key=lambda x: -x[1]):
        print(f"  {prefix}: {count} 个站点")
    

def run_scheduled_scrape(use_database=False, use_parallel=True, max_workers=6):
    """
    执行定时爬取任务
    
    Args:
        use_database: 是否使用数据库
        use_parallel: 是否使用并行处理（默认True）
        max_workers: 最大并发数（默认4）
    """
    start_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"\n[{start_time_str}] 开始定时爬取任务...")
    
    try:
        all_data, failed_regions, no_new_data_regions, parser, elapsed_time = scrape_optimized_world(
            use_database=use_database,
            use_parallel=use_parallel,
            max_workers=max_workers
        )
        
        # 判断任务是否成功
        task_succeeded = False
        
        # 1) 如果有 all_data（主要用于非并行/文件保存模式），按原逻辑处理
        if all_data:
            save_results(all_data, failed_regions, no_new_data_regions, parser, elapsed_time)
            task_succeeded = True
        
        # 2) 对于使用数据库的模式，即使 all_data 为空，只要有成功解析的报文，也视为成功
        if not task_succeeded and use_database and parser and hasattr(parser, 'parse_stats'):
            stats = parser.parse_stats
            metar_ok = stats.get('metar_success', 0) > 0
            taf_ok = stats.get('taf_success', 0) > 0
            if metar_ok or taf_ok:
                task_succeeded = True
        
        # 根据最终结果输出日志
        if task_succeeded:
            end_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"[{end_time_str}] 定时爬取任务完成 (耗时: {elapsed_time:.2f}秒)")
        else:
            print(f"[{start_time_str}] 定时爬取任务失败：未获取到任何数据")
            
    except Exception as e:
        error_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{error_time}] 定时爬取任务出错: {e}")


def run_single_scrape():
    """执行单次爬取"""
    print("=== SkyVector 优化覆盖爬虫 + METAR/TAF解析器 ===")
    print("功能:")
    print("1. 爬取全球气象站点的METAR和TAF原始数据")
    print("2. 使用python-metar和metar-taf-parser库解析数据")
    print("3. 保存解析后的数据（JSON文件或数据库）")
    print("4. 包含站点经纬度信息")
    print("已删除极地区域，合并了小区域以提高效率\n")
    
    # 询问保存方式
    print("\n请选择保存方式:")
    print("  1. 保存到本地文件（JSON格式）")
    print("  2. 实时入库到PostgreSQL数据库")
    save_choice = input("\n请输入选项 (1-2): ").strip()
    
    use_database = (save_choice == '2')
    
    # 询问是否使用并行处理
    print("\n请选择处理模式:")
    print("  1. 并行处理（推荐，速度快 3-5倍）")
    print("  2. 单线程处理（稳妥，避免限流）")
    parallel_choice = input("\n请输入选项 (1-2，默认1): ").strip() or '1'
    
    use_parallel = (parallel_choice == '1')
    max_workers = 4  # 默认4个线程
    
    if use_parallel:
        workers_input = input("\n并发线程数 (1-8，默认4): ").strip()
        if workers_input.isdigit():
            max_workers = max(1, min(8, int(workers_input)))
    
    all_data, failed_regions, no_new_data_regions, parser, elapsed_time = scrape_optimized_world(
        use_database=use_database,
        use_parallel=use_parallel,
        max_workers=max_workers
    )
    
    if all_data:
        save_results(all_data, failed_regions, no_new_data_regions, parser, elapsed_time)
    else:
        print("\n✗ 未能获取任何数据")


def main():
    """主菜单"""
    print("="*80)
    print("SkyVector METAR/TAF 数据爬虫 + 解析器")
    print("="*80)
    print("\n请选择运行模式：")
    print("  1. 单次爬取全球数据")
    print("  2. 定时爬取模式")
    print("  3. 退出")
    
    while True:
        choice = input("\n请输入选项 (1-3): ").strip()
        
        if choice == '1':
            # 单次爬取
            confirm = input("\n确认爬取全球数据？这可能需要较长时间 (y/n): ").strip().lower()
            if confirm in ['y', 'yes']:
                run_single_scrape()
            break
            
        elif choice == '2':
            # 定时爬取模式
            if not SCHEDULE_AVAILABLE:
                print("\n❌ 定时任务功能需要安装 schedule 库")
                print("安装命令: pip install schedule")
                print("安装后请重新运行程序")
                continue
            
            # 询问保存方式
            print("\n请选择保存方式:")
            print("  1. 保存到本地文件（JSON格式）")
            print("  2. 实时入库到PostgreSQL数据库")
            save_choice_schedule = input("\n请输入选项 (1-2): ").strip()
            use_database_schedule = (save_choice_schedule == '2')
            
            print("\n定时爬取模式")
            print("请选择定时任务配置：")
            print("  1. 每小时爬取一次")
            print("  2. 每6小时爬取一次")
            print("  3. 每天爬取一次（指定时间）")
            print("  4. 自定义分钟间隔")
            print("  5. 返回主菜单")
            
            schedule_choice = input("\n请输入选项 (1-5): ").strip()
            
            if schedule_choice == '1':
                # 每小时爬取
                print("\n已设置：每小时爬取一次")
                print("立即执行第一次爬取...")
                run_scheduled_scrape(use_database=use_database_schedule)
                schedule.every().hour.do(run_scheduled_scrape, use_database=use_database_schedule)
                print("\n定时任务已启动，按 Ctrl+C 停止")
                try:
                    while True:
                        schedule.run_pending()
                        time.sleep(60)  # 每分钟检查一次
                except KeyboardInterrupt:
                    print("\n\n定时任务已停止")
                    break
                    
            elif schedule_choice == '2':
                # 每6小时爬取
                print("\n已设置：每6小时爬取一次")
                print("立即执行第一次爬取...")
                run_scheduled_scrape(use_database=use_database_schedule)
                schedule.every(6).hours.do(run_scheduled_scrape, use_database=use_database_schedule)
                print("\n定时任务已启动，按 Ctrl+C 停止")
                try:
                    while True:
                        schedule.run_pending()
                        time.sleep(60)
                except KeyboardInterrupt:
                    print("\n\n定时任务已停止")
                    break
                    
            elif schedule_choice == '3':
                # 每天指定时间爬取
                print("\n已设置：每天指定时间爬取")
                time_str = input("请输入时间 (格式: HH:MM，例如 02:00): ").strip()
                try:
                    hour, minute = map(int, time_str.split(':'))
                    print("立即执行第一次爬取...")
                    run_scheduled_scrape(use_database=use_database_schedule)
                    schedule.every().day.at(f"{hour:02d}:{minute:02d}").do(run_scheduled_scrape, use_database=use_database_schedule)
                    print(f"\n定时任务已启动，每天 {time_str} 执行，按 Ctrl+C 停止")
                    try:
                        while True:
                            schedule.run_pending()
                            time.sleep(60)
                    except KeyboardInterrupt:
                        print("\n\n定时任务已停止")
                        break
                except ValueError:
                    print("❌ 时间格式错误，请使用 HH:MM 格式")
                    
            elif schedule_choice == '4':
                # 自定义分钟间隔
                print("\n自定义分钟间隔爬取")
                print("提示：建议间隔不少于30分钟，以避免频繁请求")
                try:
                    minutes = int(input("请输入间隔分钟数（例如：60）: ").strip())
                    if minutes < 1:
                        print("❌ 间隔必须大于0分钟")
                        continue
                    if minutes < 30:
                        confirm = input(f"⚠️  间隔 {minutes} 分钟可能过于频繁，确认继续？(y/n): ").strip().lower()
                        if confirm not in ['y', 'yes']:
                            continue
                    
                    print(f"\n立即执行第一次爬取...")
                    run_scheduled_scrape(use_database=use_database_schedule)
                    schedule.every(minutes).minutes.do(run_scheduled_scrape, use_database=use_database_schedule)
                    print(f"\n✓ 定时任务已启动，每 {minutes} 分钟执行一次")
                    print("按 Ctrl+C 停止")
                    
                    try:
                        while True:
                            schedule.run_pending()
                            time.sleep(30)  # 每30秒检查一次
                    except KeyboardInterrupt:
                        print("\n\n定时任务已停止")
                        break
                except ValueError:
                    print("❌ 请输入有效的数字")
                    
            elif schedule_choice == '5':
                continue
            else:
                print("❌ 无效选项")
                
        elif choice == '3':
            print("已退出")
            break
        else:
            print("❌ 无效选项，请输入 1-3")


if __name__ == "__main__":
    main()
