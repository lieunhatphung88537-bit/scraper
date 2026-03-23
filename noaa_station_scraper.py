# -*- coding: utf-8 -*-
"""
NOAA站点数据爬取器
====================
这个脚本用于爬取NOAA站点列表页面的所有站点数据。
数据来源：https://www.ndbc.noaa.gov/to_station.shtml

主要功能：
1. 自动获取所有站点ID
2. 访问每个站点的详情页面
3. 解析当前观察数据和历史观察数据
4. 支持多种数据类型（气象、海洋等）
5. 数据保存为CSV文件
6. 支持多线程并行爬取
7. 支持定时自动爬取（默认10分钟间隔）
8. 支持增量更新，避免重复数据

创建时间：2025年10月9日
最后更新：2025年10月16日
"""

import requests
import pandas as pd
from datetime import datetime
import os
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from collections import defaultdict
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
class NOAAStationScraper:
    """
    NOAA站点数据爬取器类
    """
    def __init__(self, unit_system='metric'):
        """
        初始化爬虫
        
        Args:
            unit_system (str): 单位制，'metric'(公制)或'english'(英制)，默认为'metric'
        """
        self.station_list_url = "https://www.ndbc.noaa.gov/to_station.shtml"
        self.station_page_base = "https://www.ndbc.noaa.gov/station_page.php?station="
        
        # 单位制配置
        self.unit_system = unit_system.lower()
        if self.unit_system not in ['metric', 'english']:
            print(f"⚠️  警告: 无效的单位制 '{unit_system}'，使用默认值 'metric'")
            self.unit_system = 'metric'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        }
        
        # 数据存储配置
        self.data_folder = "station_page_data"  # 站点详情页数据文件夹
        if not os.path.exists(self.data_folder):
            os.makedirs(self.data_folder)
        
        # 失败记录文件夹
        self.failed_folder = "failed_stations"  # 失败站点记录文件夹
        if not os.path.exists(self.failed_folder):
            os.makedirs(self.failed_folder)
        
        # 多线程配置
        self.max_workers = 10
        self.stats_lock = threading.Lock()
        
        # 网络请求配置
        self.request_timeout = 60
        self.max_retries = 3
        self.retry_delay = 5
        
        # 定时爬取配置
        self.auto_mode = False  # 是否自动定时模式
        self.update_interval = 10 * 60  # 默认10分钟（单位：秒）
        self.min_interval = 5 * 60  # 最小间隔5分钟（单位：秒）
        self.is_running = False  # 定时任务是否正在运行
        
        # 统计信息
        self.stats = {
            'total_stations': 0,
            'successful_stations': 0,
            'failed_stations': 0,
            'total_tables': 0,
            'errors': []
        }
    
    def fetch_station_list(self):
        """
        获取所有站点ID列表
        
        Returns:
            list: 站点ID列表
        """
        try:
            print("正在获取站点列表...")
            response = requests.get(self.station_list_url, headers=self.headers, timeout=self.request_timeout)
            response.raise_for_status() 
            soup = BeautifulSoup(response.text, 'html.parser')
            # 查找所有站点链接
            station_ids = set()
            # 方法1: 从页面链接中提取站点ID
            links = soup.find_all('a', href=True)
            for link in links:
                href = link.get('href')
                # 匹配 station_page.php?station=XXXX 格式
                match = re.search(r'station=([A-Z0-9]+)', href)
                if match:
                    station_id = match.group(1)
                    station_ids.add(station_id)
            # 方法2: 直接从页面文本中提取站点ID（页面中列出的站点ID）
            # 查找包含站点ID的文本内容
            page_text = soup.get_text()
            # 匹配站点ID模式（通常是5-6个字符的字母数字组合）
            id_pattern = r'\b[A-Z0-9]{5,6}\b'
            potential_ids = re.findall(id_pattern, page_text)
            # 过滤掉一些明显不是站点ID的内容
            exclude_words = {'NOAA', 'NDBC', 'LINKS', 'HOME', 'DART', 'IOOS', 'SHIP'}
            for potential_id in potential_ids:
                if potential_id not in exclude_words and not potential_id.startswith('HTTP'):
                    station_ids.add(potential_id)
            station_list = sorted(list(station_ids))
            print(f"找到 {len(station_list)} 个站点")
            
            # 显示前10个站点作为示例
            if station_list:
                print("\n示例站点ID（前10个）:")
                for sid in station_list[:10]:
                    print(f"  {sid}")
                if len(station_list) > 10:
                    print(f"  ... 还有 {len(station_list) - 10} 个站点")
            
            return station_list
            
        except Exception as e:
            print(f"获取站点列表失败: {e}")
            return []
    
    def fetch_station_data(self, station_id):
        """
        获取单个站点的数据
        
        Args:
            station_id (str): 站点ID
            
        Returns:
            dict: 包含所有数据表的字典
        """
        # 构建URL，添加单位制和时区参数
        station_url = f"{self.station_page_base}{station_id}"
        
        # 添加单位制和时区参数
        params = {
            'unit': 'M' if self.unit_system == 'metric' else 'E',  # M=公制, E=英制
            'tz': 'GMT'  # 使用GMT时区
        }
        
        # 重试机制
        for attempt in range(self.max_retries):
            try:
                response = requests.get(
                    station_url, 
                    params=params,
                    headers=self.headers, 
                    timeout=self.request_timeout
                )
                response.raise_for_status()
                
                # 【绝对条件】先检查页面是否包含"No Recent Reports"（最优先）
                page_text = response.text.lower()
                if 'no recent reports' in page_text:
                    return None  # 站点无最近报告
                
                # 解析页面
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # 检查是否有表格或textarea（水柱高度数据）
                tables = soup.find_all('table')
                textarea = soup.find('textarea', {'id': 'data', 'name': 'data'})
                
                if len(tables) == 0 and not textarea:
                    # 既没有表格也没有textarea，再检查其他无数据提示
                    if 'no data available' in page_text:
                        return None  # 站点无数据
                    elif 'not found' in page_text or 'does not exist' in page_text:
                        return None  # 站点不存在
                    else:
                        return None  # 页面无数据表格或textarea
                
                # 尝试提取所有数据表（包括表格和textarea）
                tables_data = self.parse_station_page(soup, station_id)
                
                # 检查是否成功提取到数据
                if not tables_data or len([k for k in tables_data.keys() if not k.startswith('_')]) == 0:
                    return None  # 未找到有效数据表
                
                return tables_data
                
            except requests.exceptions.Timeout:
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                    continue
                return None
                
            except requests.exceptions.ConnectionError:
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                    continue
                return None
                
            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                    continue
                return None
                
            except Exception as e:
                print(f"  站点 {station_id} 解析失败: {e}")
                return None
        
        return None
    
    def parse_station_page(self, soup, station_id):
        """
        解析站点页面，按区域严格划分提取所有数据表
        
        Args:
            soup (BeautifulSoup): 解析后的页面对象
            station_id (str): 站点ID
            
        Returns:
            dict: 包含所有数据表的字典
        """
        tables_data = {}
        
        # 提取页面时间信息（从标题中）
        page_time = self.extract_page_time(soup)
        if page_time:
            tables_data['_page_time'] = page_time  # 保存页面时间供后续使用
        
        # 查找页面中所有的表格
        tables = soup.find_all('table')
        
        # 按区域分组表格：基于caption内容和前面的标题识别区域
        region_tables = {}
        
        for i, table in enumerate(tables):
            region_name = None
            
            # 方法1: 检查表格的caption
            caption = table.find('caption')
            if caption:
                caption_text = caption.get_text(strip=True).lower()
                
                # 使用正则表达式匹配，处理空格变化
                import re
                
                # Conditions: "Conditions at" (排除Ocean和Solar)
                if re.search(r'conditions\s+at', caption_text) and not re.search(r'ocean\s+conditions', caption_text) and not re.search(r'solar\s+radiation', caption_text):
                    region_name = 'conditions'
                # Ocean Conditions: "Ocean Conditions at"
                elif re.search(r'ocean\s+conditions\s+at', caption_text):
                    region_name = 'ocean_conditions'
                # Solar Radiation Conditions: "Solar Radiation Conditions at"
                elif re.search(r'solar\s+radiation\s+conditions?\s+at', caption_text):
                    region_name = 'solar_radiation_conditions'
                # Wave Summary: "Wave Summary for" (处理"Wave Summaryfor"无空格情况)
                elif re.search(r'wave\s+summary\s*for', caption_text):
                    region_name = 'wave_summary'
                # Rainfall: "Rainfall"
                elif re.search(r'rainfall', caption_text):
                    region_name = 'rainfall'
                # Water Column Heights: "Water Column Height"
                elif re.search(r'water\s+column\s+height', caption_text):
                    region_name = 'water_column_heights'
                # Meteorological: "Meteorological"
                elif re.search(r'meteorological', caption_text):
                    region_name = 'meteorological'
                # Ocean Current Data: "Ocean Current"
                elif re.search(r'ocean\s+current', caption_text):
                    region_name = 'ocean_current_data'
            
            # 方法2: 检查表格的caption是否为"Previous observations"或"Previous N observations"
            if not region_name and caption:
                caption_text = caption.get_text(strip=True).lower()
                # 匹配"previous observations"或"previous 25 observations"等格式
                if re.search(r'previous\s+(\d+\s+)?observations', caption_text):
                    # 根据前面的区域标题确定类型
                    region_name = self._find_region_from_context(table, tables, i)
            
            # 方法3: 检查表格前面的标题（用于识别没有caption的"Previous observations"表格）
            if not region_name:
                # 查找表格前面的标题元素
                current = table.previous_sibling
                count = 0
                while current and count < 10:
                    if hasattr(current, 'get_text'):
                        text = current.get_text(strip=True).lower()
                        if text:
                            # 检查是否是"Previous observations"或"Previous N observations"
                            if re.search(r'previous\s+(\d+\s+)?observations', text):
                                # 根据前面的区域标题确定类型
                                # 向上查找更早的标题来确定区域类型
                                region_name = self._find_region_from_context(table, tables, i)
                                break
                    current = current.previous_sibling
                    count += 1
            
            # 方法4: 根据表格内容识别Ocean Current Data（没有caption的情况）
            if not region_name:
                # 检查表格结构是否像Ocean Current Data
                rows = table.find_all('tr')
                if len(rows) >= 2:
                    # 检查第二行是否包含Depth, Dir, Speed等关键词
                    second_row = rows[1]
                    cells = second_row.find_all(['th', 'td'])
                    if cells:
                        cell_texts = [cell.get_text(strip=True).lower() for cell in cells]
                        combined_text = ' '.join(cell_texts)
                        
                        # 检查是否包含Ocean Current的特征列名
                        has_depth = 'depth' in combined_text
                        has_dir = 'dir' in combined_text
                        has_speed = 'speed' in combined_text
                        
                        if has_depth and has_dir and has_speed:
                            region_name = 'ocean_current_data'
            
            if region_name:
                if region_name not in region_tables:
                    region_tables[region_name] = []
                region_tables[region_name].append(table)
        
        print(f"  找到区域: {list(region_tables.keys())}")
        
        # 按区域顺序处理表格
        region_order = [
            'conditions', 'ocean_conditions', 'solar_radiation_conditions', 
            'wave_summary', 'rainfall', 'water_column_heights', 
            'meteorological', 'ocean_current_data'
        ]
        
        # 文件命名映射表（内部key → 保存文件名）
        self.region_name_mapping = {
            'conditions': 'txt',
            'ocean_conditions': 'ocean', 
            'solar_radiation_conditions': 'srad',
            'wave_summary': 'wave',
            'rainfall': 'rain',
            'water_column_heights': 'dart',
            'meteorological': 'meteo',
            'ocean_current_data': 'ocean_current_data'
        }
        
        for region_name in region_order:
            if region_name not in region_tables:
                continue
                
            print(f"  处理 {region_name} 区域: {len(region_tables[region_name])} 个表格")
            
            region_table_count = 0
            for table in region_tables[region_name]:
                try:
                    # 根据区域类型选择解析方法
                    if region_name == 'ocean_current_data':
                        df = self._parse_ocean_current_table(table)
                    else:
                        df = self.parse_html_table(table)
                    
                    if df is not None and not df.empty:
                        region_table_count += 1
                        table_key = f"{region_name}_{region_table_count}"
                        tables_data[table_key] = df
                        
                except Exception as e:
                    continue
            
            # 处理该区域的实时数据
            if region_name == 'conditions':
                realtime_data = self._extract_realtime_conditions(soup)
                if realtime_data:
                    realtime_df = pd.DataFrame([realtime_data])
                    tables_data['conditions_0'] = realtime_df
                    
            elif region_name == 'ocean_conditions':
                realtime_data = self._extract_realtime_ocean_conditions(soup)
                if realtime_data:
                    # 查找该区域的实时快照表格
                    for table_name in list(tables_data.keys()):
                        if table_name.startswith('ocean_conditions_'):
                            df = tables_data[table_name]
                            cols_lower = [str(col).lower() for col in df.columns]
                            has_time = any('time' in col.lower() for col in df.columns)
                            
                            # 单行无TIME列的ocean表格，添加TIME列
                            if not has_time and len(df) == 1:
                                df['TIME'] = realtime_data
                                tables_data['ocean_conditions_0'] = df
                                if table_name != 'ocean_conditions_0':
                                    del tables_data[table_name]
                                break
                                
            elif region_name == 'solar_radiation_conditions':
                realtime_data = self._extract_realtime_solar_radiation(soup)
                if realtime_data:
                    realtime_df = pd.DataFrame([realtime_data])
                    tables_data['solar_radiation_conditions_0'] = realtime_df
                else:
                    # 查找该区域的实时快照表格
                    for table_name in list(tables_data.keys()):
                        if table_name.startswith('solar_radiation_conditions_'):
                            df = tables_data[table_name]
                            cols_lower = [str(col).lower() for col in df.columns]
                            has_time = any('time' in col.lower() for col in df.columns)
                            
                            # 单行无TIME列的solar表格，添加TIME列
                            if not has_time and len(df) == 1:
                                # 提取solar实时时间
                                page_text = soup.get_text()
                                solar_time_match = re.search(r'Solar\s+Radiation\s+Conditions?\s+at\s+\w+\s+as\s+of\s*(\d{4})\s*GMT\s*on\s*(\d{1,2}/\d{1,2}/\d{4})', 
                                                           page_text, re.DOTALL | re.IGNORECASE)
                                if solar_time_match:
                                    gmt_time_str = solar_time_match.group(1).strip()
                                    gmt_date_str = solar_time_match.group(2).strip()
                                    hour = gmt_time_str[:2]
                                    minute = gmt_time_str[2:]
                                    try:
                                        date_parts = gmt_date_str.split('/')
                                        if len(date_parts) == 3:
                                            month, day, year = date_parts
                                            formatted_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                                            solar_realtime_time = f"{formatted_date} {hour}:{minute}"
                                            df['TIME'] = solar_realtime_time
                                            tables_data['solar_radiation_conditions_0'] = df
                                            if table_name != 'solar_radiation_conditions_0':
                                                del tables_data[table_name]
                                            break
                                    except:
                                        pass
                                        
            elif region_name == 'wave_summary':
                realtime_data = self._extract_realtime_wave_summary(soup)
                if realtime_data:
                    realtime_df = pd.DataFrame([realtime_data])
                    tables_data['wave_summary_0'] = realtime_df
        
        # 提取水柱高度数据（来自textarea元素）
        water_column_data = self._extract_water_column_heights(soup)
        if water_column_data is not None:
            tables_data['water_column_heights'] = water_column_data
        
        return tables_data
    
    def _find_region_from_context(self, table, all_tables, table_index):
        """
        根据表格的上下文确定区域类型（用于识别"Previous observations"表格）
        
        Args:
            table: 当前表格
            all_tables: 所有表格列表
            table_index: 当前表格的索引
            
        Returns:
            str: 区域类型名称
        """
        try:
            # 向前查找最近的区域标题
            for i in range(table_index - 1, -1, -1):
                prev_table = all_tables[i]
                caption = prev_table.find('caption')
                if caption:
                    caption_text = caption.get_text(strip=True).lower()
                    
                    # 根据标题确定区域类型
                    if 'conditions at' in caption_text and 'ocean conditions' not in caption_text and 'solar radiation' not in caption_text:
                        return 'conditions'
                    elif 'ocean conditions at' in caption_text:
                        return 'ocean_conditions'
                    elif 'solar radiation conditions at' in caption_text:
                        return 'solar_radiation_conditions'
                    elif 'wave summary' in caption_text:
                        return 'wave_summary'
                    elif 'rainfall' in caption_text:
                        return 'rainfall'
                    elif 'water column height' in caption_text:
                        return 'water_column_heights'
                    elif 'meteorological' in caption_text:
                        return 'meteorological'
                    elif 'ocean current' in caption_text:
                        return 'ocean_current_data'
            
            # 如果没找到明确的区域标题，根据表格内容推断
            # 检查表格的列名来推断类型
            rows = table.find_all('tr')
            if len(rows) > 0:
                header_row = rows[0]
                cells = header_row.find_all(['th', 'td'])
                if cells:
                    header_text = ' '.join([cell.get_text(strip=True).lower() for cell in cells])
                    
                    # 根据列名推断区域类型
                    if 'wvht' in header_text and 'wspd' in header_text:
                        return 'conditions'
                    elif 'otmp' in header_text and 'depth' in header_text:
                        return 'ocean_conditions'
                    elif 'srad' in header_text or 'lrad' in header_text:
                        return 'solar_radiation_conditions'
                    elif 'swh' in header_text and 'wvht' in header_text:
                        return 'wave_summary'
            
            return 'data'  # 默认类型
            
        except Exception as e:
            return 'data'
    
    def extract_page_time(self, soup):
        """
        从页面标题中提取GMT时间信息
        
        Args:
            soup (BeautifulSoup): 解析后的页面对象
            
        Returns:
            str: 格式化的GMT时间字符串（如 "2025-10-14 07:00"）
        """
        try:
            # 查找包含 "GMT on" 的文本
            # GMT格式: "HHMM GMT on MM/DD/YYYY"
            
            time_text = None
            
            # 方法1: 查找所有可能包含时间的标签
            for heading in soup.find_all(['h1', 'h2', 'h3', 'h4', 'p', 'div', 'span', 'b', 'strong']):
                text = heading.get_text(separator=' ', strip=True)
                if 'GMT' in text:
                    time_text = text
                    break
            
            if not time_text:
                # 方法2: 查找包含 "GMT" 的文本
                for element in soup.find_all(string=True):
                    element_str = str(element).strip()
                    if 'GMT' in element_str:
                        time_text = element_str
                        break
            
            if not time_text:
                return None
            
            # 提取GMT时间：格式 "HHMM GMT on MM/DD/YYYY"
            # 示例: "0700 GMT on 10/09/2025" 或 "0730 GMT on 10/14/2025"
            gmt_match = re.search(r'(\d{4})\s*GMT\s*on\s*(\d{1,2}/\d{1,2}/\d{4})', time_text, re.IGNORECASE)
            
            if not gmt_match:
                return None
            
            # 提取GMT时间 (HHMM格式)
            gmt_time_str = gmt_match.group(1)  # e.g., "0730"
            date_str = gmt_match.group(2)  # e.g., "10/14/2025"
            
            # 解析时间
            hour = gmt_time_str[:2]  # "07"
            minute = gmt_time_str[2:]  # "30"
            
            # 解析日期
            date_match = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})', date_str)
            if date_match:
                month, day, year = date_match.groups()
            else:
                return None
            
            # 格式化为: "2025-10-14 07:30"
            formatted_time = f"{year}-{month.zfill(2)}-{day.zfill(2)} {hour}:{minute}"
            
            return formatted_time
            
        except Exception as e:
            return None
    
    def _parse_ocean_current_table(self, table):
        """
        专门解析Ocean Current Data表格（两行表头结构）
        
        Args:
            table (BeautifulSoup.Tag): Ocean Current表格元素
            
        Returns:
            pandas.DataFrame: 解析后的数据框
        """
        try:
            rows = table.find_all('tr')
            if len(rows) < 3:  # 至少需要2行表头+1行数据
                return None
            
            # 解析第1行：时间（带colspan）
            row1 = rows[0]
            time_headers = []
            for th in row1.find_all(['th', 'td']):
                text = th.get_text(strip=True).replace('\xa0', ' ')  # 替换&nbsp;
                colspan = int(th.get('colspan', 1))
                if text:  # 跳过空单元格
                    time_headers.append((text, colspan))
            
            # 解析第2行：Depth + Dir/Speed对
            row2 = rows[1]
            param_headers = []
            for th in row2.find_all(['th', 'td']):
                text = th.get_text(strip=True).replace('\xa0', ' ')
                param_headers.append(text)
            
            # 构建完整的列名
            final_headers = []
            if param_headers:
                final_headers.append(param_headers[0])  # Depth列
                
                param_idx = 1  # 从第二个参数开始（Dir）
                for time_text, colspan in time_headers:
                    for _ in range(colspan):
                        if param_idx < len(param_headers):
                            # 组合：如 "3:00 am AST_Dir"
                            final_headers.append(f"{time_text}_{param_headers[param_idx]}")
                            param_idx += 1
            
            # 提取数据行
            data_rows = []
            for row in rows[2:]:
                cells = row.find_all(['td', 'th'])
                if cells:
                    data_row = [cell.get_text(strip=True) for cell in cells]
                    if data_row and any(data_row):
                        data_rows.append(data_row)
            
            if not data_rows:
                return None
            
            # 创建DataFrame
            df = pd.DataFrame(data_rows, columns=final_headers[:len(data_rows[0])] if data_rows else final_headers)
            
            # 数据清理
            df = df.replace(['', '-', 'MM', 'N/A', 'NA', 'n/a', 'na'], 'NAN')
            
            return df
            
        except Exception as e:
            print(f"    解析Ocean Current表格失败: {e}")
            return None
    
    def parse_html_table(self, table):
        """
        解析HTML表格为DataFrame
        
        Args:
            table (BeautifulSoup.Tag): 表格元素
            
        Returns:
            pandas.DataFrame: 解析后的数据框
        """
        try:
            # 查找表头 - 改进版，处理复杂表头
            headers = []
            all_rows = table.find_all('tr')
            
            # 尝试从多种方式获取表头
            potential_headers = []  # 存储所有可能的表头行
            
            for i, tr in enumerate(all_rows[:5]):  # 检查前5行，找到真正的表头
                # 方法1：从th标签获取
                th_tags = tr.find_all('th')
                if th_tags:
                    temp_headers = []
                    for th in th_tags:
                        # 获取完整文本，包括子元素
                        text = th.get_text(separator=' ', strip=True)
                        temp_headers.append(text)
                    
                    if temp_headers and len(temp_headers) > 1:
                        potential_headers.append((i, temp_headers, 'th'))
                
                # 方法2：从td标签获取（有些表格用td作为表头）
                td_tags = tr.find_all('td')
                if td_tags:
                    temp_headers = []
                    for td in td_tags:
                        text = td.get_text(separator=' ', strip=True)
                        temp_headers.append(text)
                    
                    # 检查是否看起来像表头（不是纯数字）
                    if temp_headers and not all(self._is_numeric(h) for h in temp_headers if h):
                        potential_headers.append((i, temp_headers, 'td'))
            
            # 从候选表头中选择最合适的一行
            if potential_headers:
                # 优先选择包含 "TIME" 关键词的行（更可能是真正的列名）
                for idx, temp_headers, tag_type in potential_headers:
                    header_text = ' '.join(temp_headers).upper()
                    if 'TIME' in header_text and 'RATE' not in header_text:
                        # 包含TIME但不是"Rain Rate"这种标题
                        headers = temp_headers
                        data_start_idx = idx + 1
                        break
                
                # 如果没有TIME关键词，选择第一个合适的表头
                if not headers and potential_headers:
                    # 跳过明显是标题行的（如包含很多连字符、很长的单一单元格等）
                    for idx, temp_headers, tag_type in potential_headers:
                        # 检查是否是标题行（特征：包含大量连字符，或单元格很长）
                        is_title_row = any(('---------' in str(h) or len(str(h)) > 100) for h in temp_headers)
                        if not is_title_row:
                            headers = temp_headers
                            data_start_idx = idx + 1
                            break
            
            # 如果没找到表头，使用第一行
            if not headers:
                data_start_idx = 1
                first_row = all_rows[0] if all_rows else None
                if first_row:
                    cells = first_row.find_all(['th', 'td'])
                    headers = [cell.get_text(separator=' ', strip=True) for cell in cells]
            
            # 提取数据行
            rows = []
            for tr in all_rows[data_start_idx:]:
                cells = tr.find_all(['td', 'th'])
                # 使用separator=' '确保嵌套元素之间有空格（修复TIME列日期时间拼接问题）
                row = [cell.get_text(separator=' ', strip=True) for cell in cells]
                if row and any(row):  # 确保不是空行
                    rows.append(row)
            
            if not rows:
                return None
            
            # 创建DataFrame
            if headers and len(headers) == len(rows[0]):
                df = pd.DataFrame(rows, columns=headers)
            elif headers and len(headers) < len(rows[0]):
                # 表头列数少于数据列数，扩展表头
                headers.extend([f'Column_{i}' for i in range(len(headers), len(rows[0]))])
                df = pd.DataFrame(rows, columns=headers)
            elif headers and len(headers) > len(rows[0]):
                # 表头列数多于数据列数，截断表头
                df = pd.DataFrame(rows, columns=headers[:len(rows[0])])
            else:
                # 使用默认列名
                df = pd.DataFrame(rows)
            
            # 过滤掉包含超长配置文字的行（通常在第一行）
            if not df.empty and len(df.columns) >= 1:
                # 检查第一列的每个值，如果某行的值超长且包含配置关键词，删除该行
                rows_to_drop = []
                for idx, row in df.iterrows():
                    first_val = str(row.iloc[0]) if len(row) > 0 else ''
                    if len(first_val) > 200 and any(keyword in first_val.lower() for keyword in 
                                                     ['unit of measure', 'time zone', 'click on the graph']):
                        rows_to_drop.append(idx)
                if rows_to_drop:
                    df = df.drop(rows_to_drop).reset_index(drop=True)
            
            # 检查是否是键值对形式的表格（如Conditions表格）
            df = self.transform_key_value_table(df)
            
            # 清理空列和无意义的列名
            df = self._clean_empty_columns(df)
            
            # 清理无用列（包含plot、combined等关键词的列）
            df = self._filter_useless_columns(df)
            
            # 标准化列名：只保留括号内的参数名
            df = self._standardize_column_names(df)
            
            # 格式化GMT时间列
            df = self._format_gmt_time_column(df)
            
            # 数据清理：只替换空值为NAN，不删除列
            df = df.replace(['', '-', 'MM', 'N/A', 'NA', 'n/a', 'na'], 'NAN')
            
            return df
            
        except Exception as e:
            return None
    
    def _extract_realtime_wave_summary(self, soup):
        """
        从Wave Summary文本区域提取实时数据（使用GMT时间）
        
        Args:
            soup (BeautifulSoup): 页面对象
            
        Returns:
            dict: 包含实时波浪数据的字典，如果没有找到则返回None
        """
        try:
            # 查找包含"Wave Summary"的文本区域
            page_text = soup.get_text()
            
            # 查找Wave Summary部分，提取GMT时间
            # GMT格式: "Wave Summary for XX as of HHMM GMT on MM/DD/YYYY"
            wave_summary_match = re.search(r'Wave Summary.*?as of\s*(\d{4})\s*GMT\s*on\s*(\d{1,2}/\d{1,2}/\d{4})', page_text, re.DOTALL | re.IGNORECASE)
            
            if not wave_summary_match:
                return None
            
            # 提取GMT时间 (HHMM格式)
            gmt_time_str = wave_summary_match.group(1).strip()  # e.g., "0730"
            gmt_date_str = wave_summary_match.group(2).strip()  # e.g., "10/14/2025"
            
            # 解析GMT时间
            hour = gmt_time_str[:2]  # "07"
            minute = gmt_time_str[2:]  # "30"
            
            # 转换日期格式：10/14/2025 -> 2025-10-14
            try:
                date_parts = gmt_date_str.split('/')
                if len(date_parts) == 3:
                    month, day, year = date_parts
                    formatted_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                else:
                    formatted_date = gmt_date_str
            except:
                formatted_date = gmt_date_str
            
            # 构建完整GMT时间字符串: "2025-10-14 07:30"
            full_time = f"{formatted_date} {hour}:{minute}"
            
            # 查找实时数据值
            realtime_data = {'TIME': full_time}
            
            # 提取Significant Wave Height (WVHT)
            wvht_match = re.search(r'Significant Wave Height.*?\(WVHT\)\s*:\s*([\d.]+)\s*m', page_text, re.IGNORECASE)
            if wvht_match:
                realtime_data['WVHT'] = wvht_match.group(1)
            
            # 提取Swell Height (SwH)
            swh_match = re.search(r'Swell Height.*?\(SwH\)\s*:\s*([\d.]+)', page_text, re.IGNORECASE)
            if swh_match:
                realtime_data['SwH'] = swh_match.group(1)
            
            # 提取Swell Period (SwP)
            swp_match = re.search(r'Swell Period.*?\(SwP\)\s*:\s*([\d.]+)', page_text, re.IGNORECASE)
            if swp_match:
                realtime_data['SwP'] = swp_match.group(1)
            
            # 提取Swell Direction (SwD)
            swd_match = re.search(r'Swell Direction.*?\(SwD\)\s*:\s*([A-Z]+)', page_text, re.IGNORECASE)
            if swd_match:
                realtime_data['SwD'] = swd_match.group(1)
            
            # 提取Wind Wave Height (WWH)
            wwh_match = re.search(r'Wind Wave Height.*?\(WWH\)\s*:\s*([\d.]+)', page_text, re.IGNORECASE)
            if wwh_match:
                realtime_data['WWH'] = wwh_match.group(1)
            
            # 提取Wind Wave Period (WWP)
            wwp_match = re.search(r'Wind Wave Period.*?\(WWP\)\s*:\s*([\d.]+)', page_text, re.IGNORECASE)
            if wwp_match:
                realtime_data['WWP'] = wwp_match.group(1)
            
            # 提取Wind Wave Direction (WWD)
            wwd_match = re.search(r'Wind Wave Direction.*?\(WWD\)\s*:\s*([A-Z]+)', page_text, re.IGNORECASE)
            if wwd_match:
                realtime_data['WWD'] = wwd_match.group(1)
            
            # 提取Wave Steepness
            steepness_match = re.search(r'Wave Steepness.*?\(STEEPNESS\)\s*:\s*([A-Z_]+)', page_text, re.IGNORECASE)
            if steepness_match:
                realtime_data['STEEPNESS'] = steepness_match.group(1)
            
            # 提取Average Wave Period (APD)
            apd_match = re.search(r'Average.*?Period.*?\(APD\)\s*:\s*([\d.]+)', page_text, re.IGNORECASE)
            if apd_match:
                realtime_data['APD'] = apd_match.group(1)
            
            # 注意：不提取DPD，因为Wave Summary区域通常不显示DPD实时数据
            # DPD通常在Conditions表格中，会被自动合并
            
            # 只有当至少提取到WVHT或STEEPNESS时才返回数据
            if 'WVHT' in realtime_data or 'STEEPNESS' in realtime_data:
                return realtime_data
            
            return None
            
        except Exception as e:
            return None
    
    def _extract_realtime_ocean_conditions(self, soup):
        """
        从Ocean Conditions文本区域提取实时数据的GMT时间
        
        Args:
            soup (BeautifulSoup): 页面对象
            
        Returns:
            str: GMT时间字符串（如 "2025-10-14 08:00"），如果没有找到则返回None
        """
        try:
            # 查找包含Ocean Conditions的区域
            page_text = soup.get_text()
            
            # GMT格式: "Ocean Conditions at XX as of HHMM GMT on MM/DD/YYYY"
            ocean_time_match = re.search(r'Ocean\s+Conditions?\s+at\s+\w+\s+as\s+of\s*(\d{4})\s*GMT\s*on\s*(\d{1,2}/\d{1,2}/\d{4})', page_text, re.DOTALL | re.IGNORECASE)
            
            if ocean_time_match:
                # 提取GMT时间 (HHMM格式)
                gmt_time_str = ocean_time_match.group(1).strip()  # e.g., "0800"
                gmt_date_str = ocean_time_match.group(2).strip()  # e.g., "10/14/2025"
                
                # 解析GMT时间
                hour = gmt_time_str[:2]  # "08"
                minute = gmt_time_str[2:]  # "00"
                
                # 转换日期格式：10/14/2025 -> 2025-10-14
                try:
                    date_parts = gmt_date_str.split('/')
                    if len(date_parts) == 3:
                        month, day, year = date_parts
                        formatted_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                    else:
                        formatted_date = gmt_date_str
                except:
                    formatted_date = gmt_date_str
                
                # 构建完整GMT时间字符串: "2025-10-14 08:00"
                full_time = f"{formatted_date} {hour}:{minute}"
                return full_time
            
            return None
            
        except Exception as e:
            return None
    
    def _extract_realtime_conditions(self, soup):
        """
        从Conditions文本区域提取实时数据（非Ocean Conditions）
        
        Args:
            soup (BeautifulSoup): 页面对象
            
        Returns:
            dict: 包含实时数据和TIME的字典，如果没有找到则返回None
        """
        try:
            page_text = soup.get_text()
            
            # GMT格式: "Conditions at XX as of HHMM GMT on MM/DD/YYYY"
            # 注意要排除"Ocean Conditions"
            conditions_match = re.search(r'(?<!Ocean\s)Conditions\s+at\s+\w+\s+as\s+of\s*(\d{4})\s*GMT\s*on\s*(\d{1,2}/\d{1,2}/\d{4})', page_text, re.DOTALL | re.IGNORECASE)
            
            if not conditions_match:
                return None
            
            # 提取GMT时间 (HHMM格式)
            gmt_time_str = conditions_match.group(1).strip()  # e.g., "0730"
            gmt_date_str = conditions_match.group(2).strip()  # e.g., "10/14/2025"
            
            # 解析GMT时间
            hour = gmt_time_str[:2]  # "07"
            minute = gmt_time_str[2:]  # "30"
            
            # 转换日期格式：10/14/2025 -> 2025-10-14
            try:
                date_parts = gmt_date_str.split('/')
                if len(date_parts) == 3:
                    month, day, year = date_parts
                    formatted_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                else:
                    formatted_date = gmt_date_str
            except:
                formatted_date = gmt_date_str
            
            # 构建完整GMT时间字符串: "2025-10-14 07:30"
            full_time = f"{formatted_date} {hour}:{minute}"
            
            # 提取实时数据值 - 限制在Conditions区域内搜索
            # 找到Conditions区域的结束位置（通常是下一个主要标题）
            conditions_start = conditions_match.start()
            
            # 查找Conditions区域的结束位置
            # 可能的结束标记：Ocean Conditions, Wave Summary等
            conditions_end_match = re.search(r'(Ocean\s+Conditions|Wave\s+Summary)', 
                                            page_text[conditions_start:], re.IGNORECASE)
            if conditions_end_match:
                conditions_end = conditions_start + conditions_end_match.start()
            else:
                # 如果找不到结束标记，限制搜索范围为2000字符
                conditions_end = min(conditions_start + 2000, len(page_text))
            
            # 限定在Conditions区域的文本
            conditions_text = page_text[conditions_start:conditions_end]
            
            # 提取实时数据值
            realtime_data = {'TIME': full_time}
            
            # 提取Wave Height (WVHT)
            wvht_match = re.search(r'Wave\s+Height\s*\(WVHT\)\s*:\s*([\d.]+)', conditions_text, re.IGNORECASE)
            if wvht_match:
                realtime_data['WVHT'] = wvht_match.group(1)
            
            # 提取Dominant Wave Period (DPD)
            dpd_match = re.search(r'Dominant\s+Wave\s+Period\s*\(DPD\)\s*:\s*([\d.]+)', conditions_text, re.IGNORECASE)
            if dpd_match:
                realtime_data['DPD'] = dpd_match.group(1)
            
            # 提取Average Period (APD)
            apd_match = re.search(r'Average\s+(?:Wave\s+)?Period\s*\(APD\)\s*:\s*([\d.]+)', conditions_text, re.IGNORECASE)
            if apd_match:
                realtime_data['APD'] = apd_match.group(1)
            
            # 提取Mean Wave Direction (MWD)
            mwd_match = re.search(r'Mean\s+Wave\s+Direction\s*\(MWD\)\s*:\s*([A-Z]+)', conditions_text, re.IGNORECASE)
            if mwd_match:
                realtime_data['MWD'] = mwd_match.group(1)
            
            # 提取Water Temperature (WTMP)
            wtmp_match = re.search(r'Water\s+Temperature\s*\(WTMP\)\s*:\s*([\d.]+)', conditions_text, re.IGNORECASE)
            if wtmp_match:
                realtime_data['WTMP'] = wtmp_match.group(1)
            
            # 提取Air Temperature (ATMP)
            atmp_match = re.search(r'Air\s+Temperature\s*\(ATMP\)\s*:\s*([\d.]+)', conditions_text, re.IGNORECASE)
            if atmp_match:
                realtime_data['ATMP'] = atmp_match.group(1)
            
            # 提取Wind Direction (WDIR) - 可能包含详细格式如"SSE ( 150 deg true )"
            wdir_match = re.search(r'Wind\s+Direction\s*\(WDIR\)\s*:\s*([A-Z]+(?:\s*\([^)]+\))?)', conditions_text, re.IGNORECASE)
            if wdir_match:
                realtime_data['WDIR'] = wdir_match.group(1).strip()
            
            # 提取Wind Speed (WSPD)
            wspd_match = re.search(r'Wind\s+Speed\s*\(WSPD\)\s*:\s*([\d.]+)', conditions_text, re.IGNORECASE)
            if wspd_match:
                realtime_data['WSPD'] = wspd_match.group(1)
            
            # 提取Wind Gust (GST)
            gst_match = re.search(r'Wind\s+Gust\s*\(GST\)\s*:\s*([\d.]+)', conditions_text, re.IGNORECASE)
            if gst_match:
                realtime_data['GST'] = gst_match.group(1)
            
            # 提取Pressure (PRES)
            pres_match = re.search(r'(?:Atmospheric\s+)?Pressure\s*\(PRES\)\s*:\s*([\d.]+)', conditions_text, re.IGNORECASE)
            if pres_match:
                realtime_data['PRES'] = pres_match.group(1)
            
            # 提取Dewpoint (DEWP)
            dewp_match = re.search(r'Dew\s*[Pp]oint\s*\(DEWP\)\s*:\s*([\d.]+)', conditions_text, re.IGNORECASE)
            if dewp_match:
                realtime_data['DEWP'] = dewp_match.group(1)
            
            # 提取Salinity (SAL) - 只在Conditions区域内搜索
            sal_match = re.search(r'Salinity\s*\(SAL\)\s*:\s*([\d.]+)', conditions_text, re.IGNORECASE)
            if sal_match:
                realtime_data['SAL'] = sal_match.group(1)
            
            # 提取Wind Speed at 10 meters (WSPD10M)
            wspd10m_match = re.search(r'Wind\s+Speed\s+at\s+10\s+meters?\s*\(WSPD10M\)\s*:\s*([\d.]+)', conditions_text, re.IGNORECASE)
            if wspd10m_match:
                realtime_data['WSPD10M'] = wspd10m_match.group(1)
            
            # 提取Wind Speed at 20 meters (WSPD20M)
            wspd20m_match = re.search(r'Wind\s+Speed\s+at\s+20\s+meters?\s*\(WSPD20M\)\s*:\s*([\d.]+)', conditions_text, re.IGNORECASE)
            if wspd20m_match:
                realtime_data['WSPD20M'] = wspd20m_match.group(1)
            
            # 只有当至少提取到TIME和一个其他参数时才返回数据
            if len(realtime_data) > 1:
                return realtime_data
            
            return None
            
        except Exception as e:
            return None
    
    def _extract_realtime_solar_radiation(self, soup):
        """
        从Solar Radiation文本区域提取实时数据（使用GMT时间）
        
        Args:
            soup (BeautifulSoup): 页面对象
            
        Returns:
            dict: 包含实时太阳辐射数据的字典，如果没有找到则返回None
        """
        try:
            # 查找包含"Solar Radiation"的文本区域
            page_text = soup.get_text()
            
            # GMT格式: "Solar Radiation Conditions at XX as of HHMM GMT on MM/DD/YYYY"
            # 注意：可能没有空格，如 "as of0000 GMT"
            solar_time_match = re.search(r'Solar\s+Radiation\s+Conditions?\s+at\s+\w+\s+as\s+of\s*(\d{4})\s*GMT\s*on\s*(\d{1,2}/\d{1,2}/\d{4})', page_text, re.DOTALL | re.IGNORECASE)
            
            if not solar_time_match:
                return None
            
            # 提取GMT时间 (HHMM格式)
            gmt_time_str = solar_time_match.group(1).strip()  # e.g., "0530"
            gmt_date_str = solar_time_match.group(2).strip()  # e.g., "10/14/2025"
            
            # 解析GMT时间
            hour = gmt_time_str[:2]  # "05"
            minute = gmt_time_str[2:]  # "30"
            
            # 转换日期格式：10/14/2025 -> 2025-10-14
            try:
                date_parts = gmt_date_str.split('/')
                if len(date_parts) == 3:
                    month, day, year = date_parts
                    formatted_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                else:
                    formatted_date = gmt_date_str
            except:
                formatted_date = gmt_date_str
            
            # 构建完整GMT时间字符串: "2025-10-14 05:30"
            full_time = f"{formatted_date} {hour}:{minute}"
            
            # 限定在Solar Radiation区域内搜索实时数据
            solar_start = solar_time_match.start()
            # 查找区域结束位置（通常是下一个主要标题）
            solar_end_match = re.search(r'(Conditions\s+at|Ocean\s+Conditions)', 
                                       page_text[solar_start:], re.IGNORECASE)
            if solar_end_match:
                solar_end = solar_start + solar_end_match.start()
            else:
                solar_end = min(solar_start + 1500, len(page_text))
            
            # 限定在Solar Radiation区域的文本
            solar_text = page_text[solar_start:solar_end]
            
            # 查找实时数据值
            realtime_data = {'TIME': full_time}
            
            # 提取Short Wave Radiation 1 (SRAD1)
            srad1_match = re.search(r'Short\s+Wave\s+Radiation\s+1.*?\(SRAD1?\)\s*:\s*([\d.]+)', solar_text, re.IGNORECASE)
            if srad1_match:
                realtime_data['SRAD1'] = srad1_match.group(1)
            
            # 提取Short Wave Radiation 2 (SRAD2/SWRAD)
            srad2_match = re.search(r'Short\s+Wave\s+Radiation\s+2.*?\((SRAD2?|SWRAD)\)\s*:\s*([\d.]+)', solar_text, re.IGNORECASE)
            if srad2_match:
                realtime_data['SRAD2'] = srad2_match.group(2)
            
            # 提取Long Wave Radiation (LRAD/LWRAD)
            lrad_match = re.search(r'Long\s+Wave\s+Radiation.*?\((LRAD|LWRAD)\)\s*:\s*([\d.]+)', solar_text, re.IGNORECASE)
            if lrad_match:
                realtime_data['LRAD'] = lrad_match.group(2)
            
            # 只有当至少提取到一个辐射参数时才返回数据
            if 'SRAD1' in realtime_data or 'SRAD2' in realtime_data or 'LRAD' in realtime_data:
                return realtime_data
            
            return None
            
        except Exception as e:
            return None
    
    def _is_numeric(self, text):
        """
        检查文本是否是数字
        
        Args:
            text (str): 待检查的文本
            
        Returns:
            bool: 是否是数字
        """
        try:
            if not text or text == 'NAN':
                return False
            # 移除常见的单位和符号
            cleaned = text.replace('-', '').replace('.', '').replace('+', '').strip()
            return cleaned.isdigit()
        except:
            return False
    
    def _filter_useless_columns(self, df):
        """
        过滤掉无用的列（如包含plot、combined等关键词）
        
        Args:
            df (pandas.DataFrame): 数据框
            
        Returns:
            pandas.DataFrame: 过滤后的数据框
        """
        if df is None or df.empty:
            return df
        
        try:
            # 定义无用列的关键词
            useless_keywords = [
                'plot', 'combined', 'click on', 'graph icon', 
                'see a time series', 'chart', 'figure'
            ]
            
            # 筛选需要删除的列
            cols_to_drop = []
            for col in df.columns:
                col_lower = str(col).lower()
                # 检查是否包含无用关键词
                if any(keyword in col_lower for keyword in useless_keywords):
                    cols_to_drop.append(col)
            
            # 删除无用列
            if cols_to_drop:
                df = df.drop(columns=cols_to_drop)
            
            return df
            
        except Exception as e:
            return df
    
    def _standardize_column_names(self, df):
        """
        标准化列名：只提取括号内的参数名，其他保持原样
        
        Args:
            df (pandas.DataFrame): 数据框
            
        Returns:
            pandas.DataFrame: 标准化列名后的数据框
        """
        if df is None or df.empty:
            return df
        
        try:
            new_columns = []
            for col in df.columns:
                col_str = str(col).strip()
                
                # 特殊处理：TIME列统一为"TIME"（移除时区标记）
                if col_str.upper().startswith('TIME'):
                    new_columns.append('TIME')
                    continue
                
                # 尝试提取括号内的参数名（支持大小写混合）
                match = re.search(r'\(([A-Za-z0-9_]+)\)', col_str)
                if match:
                    # 找到括号内的参数名，使用它（保持原始大小写）
                    param_name = match.group(1)
                    new_columns.append(param_name)
                else:
                    # 没有括号，移除末尾的单位，但保持原来的大小写
                    cleaned_col = col_str
                    
                    # 定义常见单位列表
                    units = ['m/s', 'kts', '°C', '°F', 'mb', 'in', 'ft', 'm', 'km', 'nmi', 
                             'psu', 'sec', 'mS/cm', '%', 'ppm', 'ug/l', 'FTU', 'mV', 
                             'w/m 2', 'w/m2', 'watts/meter 2', 'mm/hour', 'cm/s', 'deg']
                    
                    # 从列名末尾移除单位
                    for unit in units:
                        if cleaned_col.endswith(' ' + unit):
                            cleaned_col = cleaned_col[:-len(' ' + unit)].strip()
                            break
                    
                    new_columns.append(cleaned_col)
            
            # 更新列名
            df.columns = new_columns
            
            return df
            
        except Exception as e:
            return df
    
    def _format_gmt_time_column(self, df):
        """
        格式化GMT时间列，将紧凑格式转换为标准格式
        
        Args:
            df (pandas.DataFrame): 数据框
            
        Returns:
            pandas.DataFrame: 格式化后的数据框
        """
        if df is None or df.empty:
            return df
        
        try:
            # 查找TIME列
            time_col = None
            for col in df.columns:
                if 'TIME' in str(col).upper():
                    time_col = col
                    break
            
            if not time_col:
                return df
            
            # 格式化TIME列中的每个值
            def format_time_value(value):
                value_str = str(value).strip()
                
                # 跳过NAN和空值
                if not value_str or value_str == 'NAN' or value_str == 'nan':
                    return value
                
                # 检查是否是GMT紧凑格式：YYYYMMDDHHMM (12位) 或 YYYY-MM-DDHHMM
                # 例如：2025-10-140700 (14位，没有空格）或 2025-10-14 0700 (15位，有空格）
                
                # 模式1: YYYY-MM-DDHHMM (格式：2025-10-140700，14位，没有空格)
                if len(value_str) == 14 and value_str[4] == '-' and value_str[7] == '-' and value_str[10].isdigit():
                    date_part = value_str[:10]  # "2025-10-14"
                    time_part = value_str[10:]  # "0700"
                    if len(time_part) == 4:
                        hour = time_part[:2]
                        minute = time_part[2:]
                        return f"{date_part} {hour}:{minute}"
                
                # 模式2: YYYY-MM-DD HHMM (格式：2025-10-14 0700，15位，有空格但没有冒号)
                elif len(value_str) == 15 and value_str[4] == '-' and value_str[7] == '-' and value_str[10] == ' ' and value_str[11].isdigit():
                    date_part = value_str[:10]  # "2025-10-14"
                    time_part = value_str[11:]  # "0700"
                    if len(time_part) == 4 and time_part.isdigit():
                        hour = time_part[:2]
                        minute = time_part[2:]
                        return f"{date_part} {hour}:{minute}"
                
                # 模式3: YYYYMMDDHHMM (格式：202510140700 - 12位)
                elif len(value_str) == 12 and value_str.isdigit():
                    year = value_str[:4]
                    month = value_str[4:6]
                    day = value_str[6:8]
                    hour = value_str[8:10]
                    minute = value_str[10:12]
                    return f"{year}-{month}-{day} {hour}:{minute}"
                
                # 模式4: 已经是正确格式 (YYYY-MM-DD HH:MM)
                elif ' ' in value_str and ':' in value_str:
                    return value
                
                # 其他格式保持不变
                return value
            
            # 应用格式化函数
            df[time_col] = df[time_col].apply(format_time_value)
            
            return df
            
        except Exception as e:
            # 格式化失败，返回原数据
            return df
    
    def _clean_empty_columns(self, df):
        """
        清理空列和无意义的列名
        
        Args:
            df (pandas.DataFrame): 数据框
            
        Returns:
            pandas.DataFrame: 清理后的数据框
        """
        if df is None or df.empty:
            return df
        
        try:
            # 1. 找出所有列名为空字符串或"Unnamed"的列
            cols_to_drop = []
            for col in df.columns:
                col_str = str(col).strip()
                # 空列名
                if not col_str or col_str == '':
                    cols_to_drop.append(col)
                # Unnamed列（但保留 Unnamed: 0 这种作为索引的）
                elif 'Unnamed:' in col_str and col_str != 'Unnamed: 0':
                    # 检查这列是否全是NAN
                    if df[col].replace(['', '-', 'NAN'], pd.NA).isna().all():
                        cols_to_drop.append(col)
            
            # 2. 找出数据全为空的列（但保留有意义列名的列，如数字列名）
            for col in df.columns:
                if col not in cols_to_drop:
                    col_str = str(col).strip()
                    # 检查是否是有意义的列名
                    # 如果列名以Column_开头或为空，且数据全为空，才删除
                    is_meaningless_name = col_str.startswith('Column_') or col_str == ''
                    
                    if is_meaningless_name:
                        # 检查列数据是否全为空值
                        col_values = df[col].astype(str).str.strip()
                        if col_values.isin(['', '-', 'NAN', 'nan']).all():
                            cols_to_drop.append(col)
            
            # 3. 删除这些列
            if cols_to_drop:
                df = df.drop(columns=cols_to_drop)
            
            return df
            
        except Exception as e:
            # 清理失败，返回原数据
            return df
    
    def transform_key_value_table(self, df):
        """
        将键值对形式的表格转换为标准表格格式
        
        Args:
            df (pandas.DataFrame): 原始数据框
            
        Returns:
            pandas.DataFrame: 转换后的数据框
        """
        try:
            # 检查是否是2列或3列的键值对表格
            if df.shape[1] == 2 or df.shape[1] == 3:
                # 检查第一列是否包含冒号（键值对的特征）
                first_col_values = df.iloc[:, 0].astype(str)
                has_colons = first_col_values.str.contains(':', na=False).sum()
                
                # 如果大部分行都有冒号，认为是键值对表格
                if has_colons >= len(df) * 0.7:  # 提高阈值到70%
                    # 提取键和值（只处理第一列包含冒号的行）
                    keys = []
                    values = []
                    
                    for idx, row in df.iterrows():
                        # 确保第一列包含冒号
                        if ':' in str(row.iloc[0]):
                            key = str(row.iloc[0]).strip()
                            value = str(row.iloc[1]).strip()  # 只取第2列（数值），忽略第3列（单位）
                            
                            # 移除键中的冒号
                            key = key.replace(':', '').strip()
                            
                            # 清理值中的单位
                            cleaned_value = self._clean_unit_from_value(value)
                            
                            keys.append(key)
                            values.append(cleaned_value)
                    
                    # 只有当提取到了数据且数量合理时才转换
                    if keys and values and len(keys) >= 1:  # 至少要有1个键值对（solar可能只有1个SRAD1）
                        # 转换为横向表格：键作为列名，值作为第一行
                        transformed_df = pd.DataFrame([values], columns=keys)
                        return transformed_df
            
            return df
            
        except Exception as e:
            # 转换失败，返回原始数据框
            return df
    
    def merge_similar_tables(self, tables_data):
        """
        分离实时数据和历史数据（不再合并）
        
        Args:
            tables_data (dict): 数据表字典
            
        Returns:
            dict: 分离后的数据表字典
        """
        try:
            separated_data = {}
            
            # 按类型分组
            type_groups = defaultdict(lambda: {'realtime': [], 'history': []})
            
            for table_name, df in tables_data.items():
                # 跳过特殊键
                if table_name.startswith('_'):
                    continue
                
                # 过滤无用的表格
                if self._is_useless_table(df):
                    continue
                
                # 特殊处理1：water_column_heights直接保存
                if table_name == 'water_column_heights':
                    separated_data[table_name] = df
                    continue
                
                # 特殊处理2：ocean_current_data直接保存（不分离实时/历史）
                if table_name.startswith('ocean_current_data'):
                    # 提取表格类型（去掉序号）
                    parts = table_name.split('_')
                    if len(parts) >= 2 and parts[-1].isdigit():
                        table_type = '_'.join(parts[:-1])
                        if table_type == 'ocean_current_data':
                            separated_data['ocean_current_data'] = df
                            continue
                
                # 提取表格类型和序号
                parts = table_name.split('_')
                if len(parts) >= 2 and parts[-1].isdigit():
                    table_type = '_'.join(parts[:-1])
                    table_index = int(parts[-1])
                else:
                    # 没有序号的表格，直接保存
                    separated_data[table_name] = df
                    continue
                
                # 特殊处理：检查 'table' 或 'data' 类型的表格
                if table_type in ['table', 'data']:
                    cols_lower = [str(col).lower() for col in df.columns]
                    if 'depth' in cols_lower and ('otmp' in cols_lower or 'sal' in cols_lower):
                        table_type = 'ocean_conditions'
                
                # 区分实时数据和历史数据
                # 判断规则：
                # 1. 序号为0 → 实时数据
                # 2. 单行且没有TIME列 → 实时数据（实时快照）
                # 3. 其他 → 历史数据
                has_time_col = any('time' in str(col).upper() for col in df.columns)
                is_single_row = len(df) == 1
                
                if table_index == 0 or (is_single_row and not has_time_col):
                    # 实时数据（包括实时快照）
                    type_groups[table_type]['realtime'].append(df)
                else:
                    # 历史数据
                    type_groups[table_type]['history'].append(df)
            
            # 处理每个类型的数据
            for table_type, data_dict in type_groups.items():
                has_realtime = len(data_dict['realtime']) > 0
                has_history = len(data_dict['history']) > 0
                
                # 如果只有历史数据没有实时数据，说明该类型不支持实时数据提取
                # 直接保存为原始名称（不加_history后缀）
                if has_history and not has_realtime:
                    if len(data_dict['history']) == 1:
                        df = data_dict['history'][0]
                        # 将TIME列移到第一列
                        df = self._move_time_column_to_first(df)
                        separated_data[table_type] = df
                    else:
                        # 多个历史表格，检查列结构
                        first_cols = set(data_dict['history'][0].columns)
                        same_structure = all(set(df.columns) == first_cols for df in data_dict['history'])
                        
                        if same_structure:
                            # 相同结构，纵向合并（追加行）
                            merged_history = pd.concat(data_dict['history'], axis=0, ignore_index=True)
                            # 将TIME列移到第一列
                            merged_history = self._move_time_column_to_first(merged_history)
                            separated_data[table_type] = merged_history
                        else:
                            # 不同结构，分别保存（带序号）
                            for i, df in enumerate(data_dict['history'], start=1):
                                # 将TIME列移到第一列
                                df = self._move_time_column_to_first(df)
                                separated_data[f"{table_type}_{i}"] = df
                    continue
                
                # 有实时数据的类型，分离实时和历史数据
                # 保存实时数据
                if has_realtime:
                    if len(data_dict['realtime']) == 1:
                        realtime_df = data_dict['realtime'][0]
                    else:
                        # 多个实时数据，合并列（横向合并）
                        realtime_df = self._merge_single_row_tables(
                            [(f"{table_type}_0_{i}", df) for i, df in enumerate(data_dict['realtime'])]
                        )
                    
                    # 将TIME列移到第一列
                    if realtime_df is not None:
                        realtime_df = self._move_time_column_to_first(realtime_df)
                        separated_data[f"{table_type}_realtime"] = realtime_df
                
                # 保存历史数据
                if has_history:
                    if len(data_dict['history']) == 1:
                        history_df = data_dict['history'][0]
                        # 将TIME列移到第一列
                        history_df = self._move_time_column_to_first(history_df)
                        separated_data[f"{table_type}_history"] = history_df
                    else:
                        # 多个历史表格，检查列结构
                        first_cols = set(data_dict['history'][0].columns)
                        same_structure = all(set(df.columns) == first_cols for df in data_dict['history'])
                        
                        if same_structure:
                            # 相同结构，纵向合并（追加行）
                            merged_history = pd.concat(data_dict['history'], axis=0, ignore_index=True)
                            # 将TIME列移到第一列
                            merged_history = self._move_time_column_to_first(merged_history)
                            separated_data[f"{table_type}_history"] = merged_history
                        else:
                            # 不同结构，分别保存（带序号）
                            for i, df in enumerate(data_dict['history'], start=1):
                                # 将TIME列移到第一列
                                df = self._move_time_column_to_first(df)
                                separated_data[f"{table_type}_history_{i}"] = df
            
            # 应用文件命名映射
            mapped_data = {}
            for table_name, df in separated_data.items():
                # 提取基础类型（去掉_realtime, _history等后缀）
                if '_realtime' in table_name:
                    base_type = table_name.replace('_realtime', '')
                    suffix = '_realtime'
                elif '_history' in table_name:
                    # 处理_history_1这种情况
                    if '_history_' in table_name:
                        parts = table_name.split('_history_')
                        base_type = parts[0]
                        suffix = f'_history_{parts[1]}'
                    else:
                        base_type = table_name.replace('_history', '')
                        suffix = '_history'
                else:
                    base_type = table_name
                    suffix = ''
                
                # 应用映射
                if base_type in self.region_name_mapping:
                    new_name = self.region_name_mapping[base_type] + suffix
                else:
                    new_name = table_name
                
                mapped_data[new_name] = df
            
            return mapped_data
            
        except Exception as e:
            print(f"  分离表格失败: {e}")
            # 失败时返回原始数据
            return {k: v for k, v in tables_data.items() if not k.startswith('_')}
    
    def _is_useless_table(self, df):
        """
        判断是否是无用的表格
        
        Args:
            df (pandas.DataFrame): 数据表
            
        Returns:
            bool: True表示无用，False表示有用
        """
        try:
            # 检查列名
            column_2_count = 0
            for col in df.columns:
                col_lower = str(col).lower()
                col_str = str(col)
                
                # 包含 "Unit of Measure" 等配置信息的表格
                if 'unit of measure' in col_lower and len(col_str) > 200:
                    return True
                
                # 列名过长（超过500字符）的异常表格
                if len(col_str) > 500:
                    return True
                
                # 统计Column_N这种无意义列名的数量
                if col_str.startswith('Column_'):
                    column_2_count += 1
            
            # 如果大部分列名都是 Column_N 格式，认为是无用表格
            # 但要排除 rainfall 类型的表格（它们可能有很多数据列）
            has_rain_keywords = any('rain' in str(col).lower() or 'accum' in str(col).lower() 
                                   for col in df.columns)
            if column_2_count > len(df.columns) * 0.5 and not has_rain_keywords:
                return True
            
            # 检查数据内容
            for _, row in df.iterrows():
                for value in row:
                    value_str = str(value).lower()
                    # 包含说明文字的表格
                    if '* indicates interpolated' in value_str:
                        return True
                    # 数据值过长（超过500字符）的异常表格
                    if len(value_str) > 500:
                        return True
            
            return False
            
        except Exception as e:
            return False
    
    def _merge_single_row_tables(self, tables_with_names):
        """
        智能合并多个单行表格，避免重复列名
        
        Args:
            tables_with_names (list): [(table_name, df), ...] 列表（都是单行表格）
            
        Returns:
            pandas.DataFrame: 合并后的数据框
        """
        try:
            if not tables_with_names:
                return None
            
            # 收集所有列名和对应的值
            all_columns = {}
            time_value = None
            
            for _, df in tables_with_names:
                for col in df.columns:
                    col_str = str(col).strip()
                    value = df[col].iloc[0] if len(df) > 0 else 'NAN'
                    
                    # 特殊处理TIME列
                    if 'TIME' in col_str.upper():
                        if time_value is None:
                            time_value = value
                        continue
                    
                    # 提取参数名作为标准列名
                    param_name = self._extract_param_name(col_str)
                    
                    # 如果参数名已存在，选择更完整的列名
                    if param_name in all_columns:
                        existing_col = all_columns[param_name]['column']
                        if len(col_str) > len(existing_col):
                            all_columns[param_name] = {'column': col_str, 'value': value}
                    else:
                        all_columns[param_name] = {'column': col_str, 'value': value}
            
            # 构建合并后的数据
            merged_data = {}
            
            # 添加TIME列（如果存在）
            if time_value:
                merged_data['TIME'] = time_value
            
            # 添加其他列
            for param_name, col_info in all_columns.items():
                merged_data[param_name] = col_info['value']
            
            # 创建DataFrame
            merged_df = pd.DataFrame([merged_data])
            
            return merged_df
            
        except Exception as e:
            # 合并失败，返回第一个表格
            return tables_with_names[0][1] if tables_with_names else None
    
    def _move_time_column_to_first(self, df):
        """
        将TIME列移动到DataFrame的第一列
        
        Args:
            df (pandas.DataFrame): 数据框
            
        Returns:
            pandas.DataFrame: TIME列在第一列的数据框
        """
        if df is None or df.empty:
            return df
        
        try:
            # 查找TIME列
            time_col = None
            for col in df.columns:
                if 'TIME' in str(col).upper():
                    time_col = col
                    break
            
            # 如果找到TIME列且不在第一列，则移动
            if time_col and df.columns[0] != time_col:
                # 获取所有列名
                cols = df.columns.tolist()
                # 移除TIME列
                cols.remove(time_col)
                # 将TIME列插入到第一位
                cols.insert(0, time_col)
                # 重新排列DataFrame的列
                df = df[cols]
            
            return df
            
        except Exception as e:
            # 移动失败，返回原数据
            return df
    
    def _clean_unit_from_value(self, value_str):
        """
        从数值中清理单位，只保留数字
        
        Args:
            value_str (str): 带单位的值（如 "29.5 °C", "15.88 psu"）
            
        Returns:
            str: 清理后的值（如 "29.5", "15.88"）
        """
        if not value_str or value_str == 'NAN':
            return 'NAN'
        
        try:
            # 移除常见单位（按长度排序，长的先匹配，避免误匹配）
            units = ['mS/cm', 'ug/l', '°C', '°F', 'psu', 'm/s', 'kts', 'mb', 'ppm', 'in', 
                     'ft', 'km', 'nmi', 'sec', 'FTU', 'mV', 'mm', '%', 'm']
            
            cleaned = value_str.strip()
            
            for unit in units:
                # 处理单位前后可能有空格的情况
                cleaned = cleaned.replace(' ' + unit, '').strip()
                cleaned = cleaned.replace(unit + ' ', '').strip()
                cleaned = cleaned.replace(unit, '').strip()
            
            # 验证是否为数字（包括负数和小数）
            try:
                float(cleaned)
                return cleaned
            except ValueError:
                return value_str  # 如果不是数字，返回原值
                
        except Exception as e:
            return value_str
    
    def _extract_param_name(self, column_name):
        """
        从列名中提取参数名（优先使用括号内容）
        
        Args:
            column_name (str): 列名（如 "Wind Speed at 10 meters (WSPD10M)"、"Swell Height (SwH)"）
            
        Returns:
            str: 参数名（如 "WSPD10M"、"SwH"）
        """
        # 特殊处理：TIME列（包括TIME (AST), TIME (UTC)等）统一返回TIME
        if 'TIME' in column_name.upper():
            return 'TIME'
        
        # 优先查找括号中的参数名（支持大小写混合，如 SwH, WSPD, WSPD10M 等）
        # 支持字母（大小写）、数字、下划线、百分号的组合
        match = re.search(r'\(([A-Za-z0-9_%]+)\)', column_name)
        if match:
            param = match.group(1)
        else:
            # 如果没有找到括号中的参数名，尝试从列名中提取
            param = column_name
            
            # 移除常见的单位
            units = ['m/s', 'kts', '°C', '°F', 'mb', 'in', 'ft', 'm', 'km', 'nmi', 'psu', 'sec', 
                     'mS/cm', '%', 'ppm', 'ug/l', 'FTU', 'mV', 'w/m²', 'w/m2', 'watts/meter²',
                     'mm/hour', 'cm/s', 'deg']
            for unit in units:
                param = param.replace(unit, '').strip()
            
            # 标准化参数名
            param = param.upper().strip()
        
        # 参数名标准化映射（处理同一参数的不同表示方式）
        param_mapping = {
            'O2PCT': 'O2%',      # 溶解氧百分比饱和度统一为O2%
            'O2PERCENT': 'O2%',  # 其他可能的变体
            'OXYGEN%': 'O2%',    # 其他可能的变体
        }
        
        # 应用标准化映射
        if param in param_mapping:
            param = param_mapping[param]
        
        return param
    
    def _incremental_update_water_column(self, new_df, filepath):
        """
        为water_column_heights进行增量更新
        
        Args:
            new_df (pd.DataFrame): 新抓取的数据
            filepath (str): CSV文件路径
            
        Returns:
            pd.DataFrame: 合并后的数据（新数据+旧数据，去重）
        """
        try:
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 为新数据添加元数据列
            if 'first_crawl_time' not in new_df.columns:
                new_df['first_crawl_time'] = current_time
            if 'last_update_time' not in new_df.columns:
                new_df['last_update_time'] = current_time
            if 'update_count' not in new_df.columns:
                new_df['update_count'] = '1'
            
            # 检查文件是否存在
            if not os.path.exists(filepath):
                # 文件不存在，直接返回新数据
                print(f"    首次保存water_column_heights数据")
                return new_df
            
            # 读取现有数据（保持字符串格式）
            existing_df = pd.read_csv(filepath, encoding='utf-8-sig', dtype=str)
            
            if existing_df.empty:
                return new_df
            
            # 为旧数据添加元数据列（如果缺失）
            if 'first_crawl_time' not in existing_df.columns:
                existing_df['first_crawl_time'] = ''  # 旧数据设为空
            if 'last_update_time' not in existing_df.columns:
                existing_df['last_update_time'] = ''  # 旧数据设为空
            if 'update_count' not in existing_df.columns:
                existing_df['update_count'] = ''  # 旧数据设为空
            
            # 确保时间列都是字符串类型，并补齐前导零
            time_cols = ['YY', 'MM', 'DD', 'hh', 'mm', 'ss']
            for col in time_cols:
                if col in new_df.columns:
                    new_df[col] = new_df[col].astype(str).str.strip()
                if col in existing_df.columns:
                    existing_df[col] = existing_df[col].astype(str).str.strip()
            
            # 创建时间戳字符串用于比较（格式：YYYYMMDDhhmmss）
            def create_timestamp(row):
                try:
                    # 使用字符串拼接，保持字符串格式
                    return f"{row['YY'].zfill(4)}{row['MM'].zfill(2)}{row['DD'].zfill(2)}" \
                           f"{row['hh'].zfill(2)}{row['mm'].zfill(2)}{row['ss'].zfill(2)}"
                except:
                    return "000000000000"
            
            new_df['_timestamp'] = new_df.apply(create_timestamp, axis=1)
            existing_df['_timestamp'] = existing_df.apply(create_timestamp, axis=1)
            
            # 找到现有数据中的最新时间戳（第一行，因为数据是倒序的）
            latest_existing_timestamp = existing_df['_timestamp'].iloc[0]
            
            # 过滤出比现有数据更新的记录
            new_records = new_df[new_df['_timestamp'] > latest_existing_timestamp].copy()
            
            if len(new_records) == 0:
                print(f"    无新数据需要更新")
                # 删除临时时间戳列
                existing_df = existing_df.drop(columns=['_timestamp'])
                return existing_df
            
            # 合并数据：新数据在前（保持倒序）
            combined_df = pd.concat([new_records, existing_df], ignore_index=True)
            
            # 按时间戳倒序排序
            combined_df = combined_df.sort_values('_timestamp', ascending=False).reset_index(drop=True)
            
            # 去重（基于时间戳）
            combined_df = combined_df.drop_duplicates(subset=['_timestamp'], keep='first')
            
            # 删除临时时间戳列
            combined_df = combined_df.drop(columns=['_timestamp'])
            
            added_count = len(new_records)
            print(f"    增量更新：添加 {added_count} 条新记录")
            
            return combined_df
            
        except Exception as e:
            print(f"    增量更新失败: {e}，使用新数据覆盖")
            return new_df
    
    def _incremental_update_realtime(self, new_df, filepath):
        """
        为实时数据进行增量更新（支持列变化，同时间批次智能合并）
        
        更新策略：
        1. 新时间批次：直接追加到文件顶部
        2. 同时间批次：智能合并
           - 列缺失（NAN）：保留旧值
           - 列有值：更新为新值
           - 新增列：添加到数据中
        
        Args:
            new_df (pd.DataFrame): 新抓取的实时数据
            filepath (str): CSV文件路径
            
        Returns:
            pd.DataFrame: 更新后的实时数据
        """
        try:
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 为新数据添加元数据列
            if 'first_crawl_time' not in new_df.columns:
                new_df['first_crawl_time'] = current_time
            if 'last_update_time' not in new_df.columns:
                new_df['last_update_time'] = current_time
            if 'update_count' not in new_df.columns:
                new_df['update_count'] = '1'
            
            # 检查文件是否存在
            if not os.path.exists(filepath):
                print(f"    首次保存实时数据")
                return new_df
            
            # 读取现有数据（保持字符串格式）
            existing_df = pd.read_csv(filepath, encoding='utf-8-sig', dtype=str)
            
            if existing_df.empty:
                return new_df
            
            # 为旧数据添加元数据列（如果缺失）
            if 'first_crawl_time' not in existing_df.columns:
                existing_df['first_crawl_time'] = ''  # 旧数据设为空
            if 'last_update_time' not in existing_df.columns:
                existing_df['last_update_time'] = ''  # 旧数据设为空
            if 'update_count' not in existing_df.columns:
                existing_df['update_count'] = ''  # 旧数据设为空
            
            # 查找TIME列
            time_col = None
            for col in new_df.columns:
                if 'TIME' in str(col).upper():
                    time_col = col
                    break
            
            # 获取新旧数据的列集合
            new_cols = set(new_df.columns)
            existing_cols = set(existing_df.columns)
            
            # 找出新增的列和删除的列
            added_cols = new_cols - existing_cols
            removed_cols = existing_cols - new_cols
            
            # 如果有列变化
            if added_cols or removed_cols:
                if added_cols:
                    print(f"    实时数据新增列: {', '.join(added_cols)}")
                    # 为现有数据添加新列（填充NAN）
                    for col in added_cols:
                        existing_df[col] = 'NAN'
                
                if removed_cols:
                    print(f"    实时数据删除列: {', '.join(removed_cols)}")
                    # 为新数据添加删除的列（填充NAN）
                    for col in removed_cols:
                        new_df[col] = 'NAN'
            
            # 统一列顺序
            all_cols = list(existing_df.columns)
            for col in new_df.columns:
                if col not in all_cols:
                    all_cols.append(col)
            
            # 确保两个DataFrame有相同的列
            existing_df = existing_df.reindex(columns=all_cols, fill_value='NAN')
            new_df = new_df.reindex(columns=all_cols, fill_value='NAN')
            
            # 检查是否有TIME列用于时间批次处理
            if time_col and time_col in existing_df.columns:
                # 检查新数据的时间是否已存在
                new_time = new_df[time_col].iloc[0]
                
                if new_time in existing_df[time_col].values:
                    # 同时间批次：智能合并更新
                    print(f"    实时数据时间 {new_time} 已存在，执行智能合并更新")
                    
                    # 找到同时间的旧数据行索引
                    old_row_idx = existing_df[existing_df[time_col] == new_time].index[0]
                    
                    # 获取旧数据行
                    old_row = existing_df.loc[old_row_idx].copy()
                    new_row = new_df.iloc[0].copy()
                    
                    # 合并策略：新值优先，但如果新值是NAN则保留旧值
                    merged_row = {}
                    updated_cols = []
                    kept_cols = []
                    has_data_changes = False  # 标记数据列是否有变化
                    
                    for col in all_cols:
                        # 跳过元数据列，后面单独处理
                        if col in ['first_crawl_time', 'last_update_time', 'update_count']:
                            continue
                        
                        new_val = str(new_row[col]).strip()
                        old_val = str(old_row[col]).strip() if col in old_row.index else 'NAN'
                        
                        # TIME列直接使用新值
                        if col == time_col:
                            merged_row[col] = new_val
                        # 新值不是NAN：使用新值
                        elif new_val not in ['NAN', 'nan', 'N/A', 'NA', '']:
                            merged_row[col] = new_val
                            # 记录更新的列（如果旧值不同）
                            if old_val != new_val and old_val not in ['NAN', 'nan', 'N/A', 'NA', '']:
                                updated_cols.append(f"{col}: {old_val}→{new_val}")
                                has_data_changes = True
                        # 新值是NAN：保留旧值
                        else:
                            merged_row[col] = old_val
                            if old_val not in ['NAN', 'nan', 'N/A', 'NA', '']:
                                kept_cols.append(col)
                    
                    # 处理元数据列
                    # first_crawl_time：保持旧值（如果存在），否则使用当前时间
                    if 'first_crawl_time' in old_row.index and old_row['first_crawl_time']:
                        merged_row['first_crawl_time'] = old_row['first_crawl_time']
                    else:
                        merged_row['first_crawl_time'] = current_time
                    
                    # last_update_time：如果有数据变化，更新为当前时间；否则保持旧值
                    if has_data_changes:
                        merged_row['last_update_time'] = current_time
                    else:
                        merged_row['last_update_time'] = old_row['last_update_time'] if 'last_update_time' in old_row.index else current_time
                    
                    # update_count：如果有数据变化，计数+1；否则保持不变
                    if has_data_changes:
                        if 'update_count' in old_row.index and old_row['update_count']:
                            try:
                                merged_row['update_count'] = str(int(old_row['update_count']) + 1)
                            except:
                                merged_row['update_count'] = '2'
                        else:
                            merged_row['update_count'] = '2'
                    else:
                        merged_row['update_count'] = old_row['update_count'] if 'update_count' in old_row.index else '1'
                    
                    # 更新现有数据中的这一行
                    for col, val in merged_row.items():
                        existing_df.at[old_row_idx, col] = val
                    
                    # 打印更新信息
                    if updated_cols:
                        print(f"      更新的列: {', '.join(updated_cols[:5])}" + 
                              (f"... 等{len(updated_cols)}个列" if len(updated_cols) > 5 else ""))
                    if kept_cols:
                        print(f"      保留的列: {', '.join(kept_cols[:5])}" + 
                              (f"... 等{len(kept_cols)}个列" if len(kept_cols) > 5 else ""))
                    if added_cols:
                        print(f"      新增的列: {', '.join(added_cols)}")
                    
                    combined_df = existing_df
                else:
                    # 新时间批次：追加新数据（新数据在前）
                    combined_df = pd.concat([new_df, existing_df], ignore_index=True)
                    
                    # 按时间倒序排序
                    combined_df = combined_df.sort_values(time_col, ascending=False).reset_index(drop=True)
                    
                    print(f"    新时间批次 {new_time} 已追加到顶部")
            else:
                # 没有TIME列，直接追加
                combined_df = pd.concat([new_df, existing_df], ignore_index=True)
                print(f"    实时数据已追加（无TIME列）")
            
            # 确保TIME列在第一列
            combined_df = self._move_time_column_to_first(combined_df)
            
            return combined_df
            
        except Exception as e:
            print(f"    实时数据更新失败: {e}，使用新数据覆盖")
            return new_df
    
    def _incremental_update_history(self, new_df, filepath):
        """
        为历史数据进行增量更新（直接追加新数据）
        
        Args:
            new_df (pd.DataFrame): 新抓取的历史数据
            filepath (str): CSV文件路径
            
        Returns:
            pd.DataFrame: 合并后的数据（新数据+旧数据，去重）
        """
        try:
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 为新数据添加元数据列
            if 'first_crawl_time' not in new_df.columns:
                new_df['first_crawl_time'] = current_time
            if 'last_update_time' not in new_df.columns:
                new_df['last_update_time'] = current_time
            if 'update_count' not in new_df.columns:
                new_df['update_count'] = '1'
            
            # 检查文件是否存在
            if not os.path.exists(filepath):
                print(f"    首次保存历史数据")
                return new_df
            
            # 读取现有数据（保持字符串格式）
            existing_df = pd.read_csv(filepath, encoding='utf-8-sig', dtype=str)
            
            if existing_df.empty:
                return new_df
            
            # 为旧数据添加元数据列（如果缺失）
            if 'first_crawl_time' not in existing_df.columns:
                existing_df['first_crawl_time'] = ''  # 旧数据设为空
            if 'last_update_time' not in existing_df.columns:
                existing_df['last_update_time'] = ''  # 旧数据设为空
            if 'update_count' not in existing_df.columns:
                existing_df['update_count'] = ''  # 旧数据设为空
            
            # 查找TIME列
            time_col = None
            for col in new_df.columns:
                if 'TIME' in str(col).upper():
                    time_col = col
                    break
            
            if not time_col:
                # 没有TIME列，直接返回新数据
                print(f"    警告：历史数据没有TIME列，直接覆盖")
                return new_df
            
            # 找到现有数据中的最新时间（第一行）
            if time_col not in existing_df.columns:
                print(f"    警告：现有数据没有TIME列，直接覆盖")
                return new_df
            
            latest_existing_time = existing_df[time_col].iloc[0]
            
            # 过滤出比现有数据更新的记录
            new_records = new_df[new_df[time_col] > latest_existing_time].copy()
            
            if len(new_records) == 0:
                print(f"    无新历史数据需要更新")
                return existing_df
            
            # 合并数据：新数据在前
            combined_df = pd.concat([new_records, existing_df], ignore_index=True)
            
            # 按时间倒序排序
            combined_df = combined_df.sort_values(time_col, ascending=False).reset_index(drop=True)
            
            # 去重（基于时间）
            combined_df = combined_df.drop_duplicates(subset=[time_col], keep='first')
            
            added_count = len(new_records)
            print(f"    增量更新：添加 {added_count} 条历史记录")
            
            return combined_df
            
        except Exception as e:
            print(f"    历史数据更新失败: {e}，使用新数据覆盖")
            return new_df
    
    def _save_ocean_current_with_hash(self, new_df, station_folder, station_id, page_time=None):
        """
        为ocean_current_data进行哈希对比保存
        如果数据与上次不同，则保存为新文件（使用网页时间命名）
        
        Args:
            new_df (pd.DataFrame): 新抓取的ocean current数据
            station_folder (str): 站点文件夹路径
            station_id (str): 站点ID
            page_time (str): 网页时间（如 "2025-10-16 01:30"）
            
        Returns:
            bool: 是否保存了新文件
        """
        try:
            import hashlib
            
            # 创建ocean_current_data子文件夹
            ocean_current_folder = os.path.join(station_folder, 'ocean_current_data')
            if not os.path.exists(ocean_current_folder):
                os.makedirs(ocean_current_folder)
            
            # 计算新数据的哈希值
            new_data_str = new_df.to_csv(index=False)
            new_hash = hashlib.md5(new_data_str.encode('utf-8')).hexdigest()
            
            # 查找最新的文件
            existing_files = sorted([f for f in os.listdir(ocean_current_folder) if f.endswith('.csv')])
            
            if existing_files:
                # 读取最新文件并计算哈希（保持字符串格式）
                latest_file = os.path.join(ocean_current_folder, existing_files[-1])
                existing_df = pd.read_csv(latest_file, encoding='utf-8-sig', dtype=str)
                existing_data_str = existing_df.to_csv(index=False)
                existing_hash = hashlib.md5(existing_data_str.encode('utf-8')).hexdigest()
                
                # 如果哈希相同，不保存新文件
                if new_hash == existing_hash:
                    print(f"  ✓ ocean_current_data数据未变化，跳过保存")
                    return False
            
            # 生成时间戳文件名（使用网页时间）
            if page_time:
                try:
                    # page_time格式: "2025-10-16 01:30"
                    # 转换为: 20251016_0130
                    timestamp = page_time.replace('-', '').replace(' ', '_').replace(':', '')
                except:
                    # 如果解析失败，使用当前时间
                    from datetime import datetime
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            else:
                # 如果没有page_time，使用当前时间
                from datetime import datetime
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            filename = f"{timestamp}.csv"
            filepath = os.path.join(ocean_current_folder, filename)
            
            # 保存新文件
            new_df.to_csv(filepath, index=False, encoding='utf-8-sig')
            print(f"  ✓ 保存: {station_id}/ocean_current_data/{filename} ({len(new_df)} 行 x {len(new_df.columns)} 列)")
            
            return True
            
        except Exception as e:
            print(f"  ocean_current_data保存失败: {e}")
            return False
    
    def save_station_data(self, station_id, tables_data):
        """
        保存站点数据到CSV文件（支持增量更新）
        
        Args:
            station_id (str): 站点ID
            tables_data (dict): 数据表字典
            
        Returns:
            bool: 保存是否成功
        """
        if not tables_data:
            return False
        
        try:
            # 提取页面时间（用于ocean_current_data文件命名）
            page_time = tables_data.get('_page_time', None)
            
            # 先尝试合并相似的表格
            tables_data = self.merge_similar_tables(tables_data)
            
            # 为该站点创建独立的文件夹
            station_folder = os.path.join(self.data_folder, station_id)
            if not os.path.exists(station_folder):
                os.makedirs(station_folder)
            
            saved_count = 0
            for table_name, df in tables_data.items():
                if df is not None and not df.empty:
                    # 跳过特殊键
                    if table_name.startswith('_'):
                        continue
                    
                    # 特殊处理1：dart (原water_column_heights) 进行增量更新
                    if table_name == 'dart':
                        filename = f"{table_name}.csv"
                        filepath = os.path.join(station_folder, filename)
                        df = self._incremental_update_water_column(df, filepath)
                        df.to_csv(filepath, index=False, encoding='utf-8-sig')
                        saved_count += 1
                        print(f"  ✓ 保存: {station_id}/{filename} ({len(df)} 行 x {len(df.columns)} 列)")
                        continue
                    
                    # 特殊处理2：ocean_current_data 使用哈希对比保存（使用网页时间命名）
                    if table_name == 'ocean_current_data':
                        if self._save_ocean_current_with_hash(df, station_folder, station_id, page_time):
                            saved_count += 1
                        continue
                    
                    # 判断表格类型并应用对应的增量更新策略
                    filename = f"{table_name}.csv"
                    filepath = os.path.join(station_folder, filename)
                    
                    # 实时数据：支持列变化的增量更新
                    if table_name.endswith('_realtime'):
                        df = self._incremental_update_realtime(df, filepath)
                    
                    # 历史数据：直接追加新记录
                    elif table_name.endswith('_history'):
                        df = self._incremental_update_history(df, filepath)
                    
                    # 其他只有历史数据的区域（rain, meteo）：增量更新
                    elif table_name in ['rain', 'meteo']:
                        df = self._incremental_update_history(df, filepath)
                    
                    # 保存为CSV
                    df.to_csv(filepath, index=False, encoding='utf-8-sig')
                    saved_count += 1
                    print(f"  ✓ 保存: {station_id}/{filename} ({len(df)} 行 x {len(df.columns)} 列)")
            
            if saved_count > 0:
                with self.stats_lock:
                    self.stats['total_tables'] += saved_count
                return True
            else:
                return False
                
        except Exception as e:
            print(f"  保存站点 {station_id} 数据失败: {e}")
            return False
    
    def process_single_station(self, station_id):
        """
        处理单个站点（线程安全）
        
        Args:
            station_id (str): 站点ID
            
        Returns:
            bool: 处理是否成功
        """
        try:
            # 获取站点数据
            tables_data = self.fetch_station_data(station_id)
            
            if tables_data:
                # 保存数据
                if self.save_station_data(station_id, tables_data):
                    with self.stats_lock:
                        self.stats['successful_stations'] += 1
                    return True
                else:
                    with self.stats_lock:
                        self.stats['failed_stations'] += 1
                        self.stats['errors'].append(f"{station_id}: 保存失败")
                    print(f"  ✗ {station_id}: 保存失败")
                    return False
            else:
                with self.stats_lock:
                    self.stats['failed_stations'] += 1
                    self.stats['errors'].append(f"{station_id}: 获取数据失败")
                print(f"  ✗ {station_id}: 无数据")
                return False
                
        except Exception as e:
            with self.stats_lock:
                self.stats['failed_stations'] += 1
                self.stats['errors'].append(f"{station_id}: {str(e)}")
            print(f"  ✗ {station_id}: 错误 - {e}")
            return False
    
    def scrape_all_stations(self, station_list=None, use_multithread=True):
        """
        爬取所有站点数据（兼容性方法，内部调用run_once）
        
        Args:
            station_list (list): 站点ID列表，None表示自动获取
            use_multithread (bool): 是否使用多线程
            
        Returns:
            dict: 处理结果统计
        """
        return self.run_once(station_list, use_multithread)
    
    def set_update_interval(self, minutes):
        """
        设置更新间隔时间
        
        Args:
            minutes (int): 更新间隔（单位：分钟），不能少于5分钟
        """
        if minutes < 5:
            print(f"⚠️  更新间隔不能少于5分钟，已自动设置为5分钟")
            self.update_interval = self.min_interval
        else:
            self.update_interval = minutes * 60
            print(f"✓ 更新间隔已设置为: {minutes} 分钟")
    
    def run_once(self, station_list=None, use_multithread=True):
        """
        运行一次完整的数据爬取流程
        
        Args:
            station_list (list): 站点ID列表，None表示自动获取
            use_multithread (bool): 是否使用多线程
            
        Returns:
            dict: 处理结果统计
        """
        print(f"\n{'='*70}")
        print(f"开始数据爬取 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*70}")
        
        # 获取站点列表
        if station_list is None:
            station_list = self.fetch_station_list()
        
        if not station_list:
            print("未找到站点")
            return None
        
        # 重置统计信息
        self.stats = {
            'total_stations': len(station_list),
            'successful_stations': 0,
            'failed_stations': 0,
            'total_tables': 0,
            'errors': []
        }
        
        print(f"总站点数: {len(station_list)}")
        print(f"多线程: {'是' if use_multithread else '否'} (线程数: {self.max_workers})")
        print(f"数据保存位置: {self.data_folder}")
        print(f"{'='*70}\n")
        
        start_time = time.time()
        
        if use_multithread:
            # 多线程处理
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(self.process_single_station, station_id): station_id 
                    for station_id in station_list
                }
                
                completed = 0
                for future in as_completed(futures):
                    completed += 1
                    if completed % 50 == 0:
                        print(f"\n进度: {completed}/{len(station_list)} 站点已处理")
        else:
            # 单线程处理
            for i, station_id in enumerate(station_list, 1):
                self.process_single_station(station_id)
                if i % 20 == 0:
                    print(f"\n进度: {i}/{len(station_list)} 站点已处理")
        
        elapsed_time = time.time() - start_time
        
        # 打印结果摘要
        self.print_summary(elapsed_time)
        
        # 保存失败站点信息到文件
        self.save_failed_stations()
        
        return self.stats
    
    def run_auto(self, station_list=None, use_multithread=True):
        """
        自动定时运行数据爬取
        
        Args:
            station_list (list): 站点ID列表，None表示自动获取
            use_multithread (bool): 是否使用多线程
        """
        self.auto_mode = True
        self.is_running = True
        interval_minutes = self.update_interval // 60
        
        print(f"\n{'='*70}")
        print(f" 自动定时爬取模式启动")
        print(f"{'='*70}")
        print(f"  更新间隔: {interval_minutes} 分钟")
        print(f"  线程数: {self.max_workers}")
        print(f"  数据保存位置: {self.data_folder}")
        print(f"  按 Ctrl+C 停止程序")
        print(f"{'='*70}\n")
        
        run_count = 0
        try:
            while self.auto_mode and self.is_running:
                run_count += 1
                
                print(f"\n{'#'*70}")
                print(f"  第 {run_count} 次运行")
                print(f"{'#'*70}")
                
                # 运行一次爬取
                stats = self.run_once(station_list, use_multithread)
                
                if stats:
                    print(f"\n下次更新时间: {datetime.fromtimestamp(time.time() + self.update_interval).strftime('%Y-%m-%d %H:%M:%S')}")
                    print(f"等待 {interval_minutes} 分钟...")
                    
                    # 等待指定的时间间隔
                    time.sleep(self.update_interval)
                else:
                    print(f"\n本次爬取失败，将在 {interval_minutes} 分钟后重试...")
                    time.sleep(self.update_interval)
                    
        except KeyboardInterrupt:
            print(f"\n\n{'='*70}")
            print(f"  用户手动停止程序")
            print(f"  总运行次数: {run_count}")
            print(f"{'='*70}")
            self.auto_mode = False
            self.is_running = False
        except Exception as e:
            print(f"\n\n自动模式出错: {e}")
            self.auto_mode = False
            self.is_running = False
    
    def stop_auto(self):
        """
        停止自动定时爬取
        """
        if not self.auto_mode:
            print("自动模式未在运行")
            return
        
        print("\n正在停止自动定时爬取...")
        self.auto_mode = False
        self.is_running = False
        print("自动定时爬取已停止")
    
    def get_auto_status(self):
        """
        获取自动模式状态
        
        Returns:
            dict: 自动模式状态信息
        """
        status = {
            'is_auto_mode': self.auto_mode,
            'is_running': self.is_running,
            'update_interval_minutes': self.update_interval // 60,
            'min_interval_minutes': self.min_interval // 60
        }
        return status
    
    def print_summary(self, elapsed_time):
        """
        打印处理结果摘要
        
        Args:
            elapsed_time (float): 处理耗时（秒）
        """
        print(f"\n{'='*70}")
        print(f"爬取完成 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*70}")
        print(f"总站点数: {self.stats['total_stations']}")
        print(f"成功: {self.stats['successful_stations']}")
        print(f"失败: {self.stats['failed_stations']}")
        print(f"总数据表数: {self.stats['total_tables']}")
        
        if self.stats['total_stations'] > 0:
            success_rate = self.stats['successful_stations'] / self.stats['total_stations'] * 100
            print(f"成功率: {success_rate:.1f}%")
        
        print(f"耗时: {elapsed_time:.1f} 秒")
        print(f"数据保存位置: {self.data_folder}")
        
        if self.stats['errors']:
            print(f"\n错误信息 (前10个):")
            for error in self.stats['errors'][:10]:
                print(f"  {error}")
            if len(self.stats['errors']) > 10:
                print(f"  ... 还有 {len(self.stats['errors']) - 10} 个错误")
        
        print(f"{'='*70}\n")
    
    def save_failed_stations(self):
        """
        保存失败站点信息到专门的失败记录文件夹
        """
        if not self.stats['errors']:
            return
        
        try:
            # 创建失败记录文件（保存在专门的失败记录文件夹中）
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            failed_file = os.path.join(self.failed_folder, f'failed_stations_{timestamp}.txt')
            
            with open(failed_file, 'w', encoding='utf-8') as f:
                f.write(f"失败站点记录\n")
                f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"{'='*70}\n\n")
                f.write(f"总站点数: {self.stats['total_stations']}\n")
                f.write(f"成功: {self.stats['successful_stations']}\n")
                f.write(f"失败: {self.stats['failed_stations']}\n")
                f.write(f"\n{'='*70}\n")
                f.write(f"失败站点详情:\n")
                f.write(f"{'='*70}\n\n")
                
                for error in self.stats['errors']:
                    f.write(f"{error}\n")
            
            print(f"✓ 失败站点信息已保存到: {self.failed_folder}/failed_stations_{timestamp}.txt")
            
        except Exception as e:
            print(f"✗ 保存失败站点信息失败: {e}")
    
    def scrape_single_station_demo(self, station_id):
        """
        演示：爬取单个站点数据（用于测试）
        
        Args:
            station_id (str): 站点ID
        """
        print(f"\n测试爬取站点: {station_id}")
        print("-" * 50)
        
        # 获取数据
        tables_data = self.fetch_station_data(station_id)
        
        if tables_data:
            # 过滤掉特殊键
            display_tables = {k: v for k, v in tables_data.items() if not k.startswith('_')}
            print(f"找到 {len(display_tables)} 个数据表:")
            for table_name, df in display_tables.items():
                print(f"\n表格: {table_name}")
                print(f"  行数: {len(df)}")
                print(f"  列数: {len(df.columns)}")
                print(f"  列名: {list(df.columns)}")
                print(f"\n  前5行数据:")
                print(df.head().to_string(index=False))
            
            # 保存数据
            if self.save_station_data(station_id, tables_data):
                print(f"\n✓ 数据已保存到: {self.data_folder}")
            else:
                print("\n✗ 数据保存失败")
        else:
            print("未获取到数据")

    def _extract_water_column_heights(self, soup):
        """
        从textarea元素中提取水柱高度数据（DART站点专用）
        
        Args:
            soup (BeautifulSoup): 解析后的页面对象
            
        Returns:
            pandas.DataFrame: 水柱高度数据的DataFrame，如果未找到则返回None
        """
        try:
            # 查找id="data"的textarea元素
            textarea = soup.find('textarea', {'id': 'data', 'name': 'data'})
            
            if not textarea:
                return None
            
            # 获取textarea中的文本内容
            text_content = textarea.get_text()
            
            if not text_content or not text_content.strip():
                return None
            
            # 解析文本数据
            lines = text_content.strip().split('\n')
            
            # 跳过注释行（以#开头）
            data_lines = [line for line in lines if line.strip() and not line.strip().startswith('#')]
            
            if not data_lines:
                return None
            
            # 解析数据
            # 数据格式: YY MM DD hh mm ss T HEIGHT
            # 示例: 2025 10 15 00 00 00 1 5779.821
            data_rows = []
            for line in data_lines:
                parts = line.split()
                if len(parts) >= 8:  # 确保有足够的列
                    try:
                        # 保持字符串格式，不进行类型转换
                        year = parts[0]
                        month = parts[1]
                        day = parts[2]
                        hour = parts[3]
                        minute = parts[4]
                        second = parts[5]
                        measurement_type = parts[6]
                        height = parts[7]
                        
                        data_rows.append({
                            'YY': year,
                            'MM': month,
                            'DD': day,
                            'hh': hour,
                            'mm': minute,
                            'ss': second,
                            'T': measurement_type,
                            'HEIGHT m': height
                        })
                    except (ValueError, IndexError):
                        # 如果解析失败，跳过该行
                        continue
            
            if not data_rows:
                return None
            
            # 创建DataFrame
            df = pd.DataFrame(data_rows)
            
            return df
            
        except Exception as e:
            print(f"  提取水柱高度数据失败: {e}")
            return None


def main():
    print("=" * 70)
    print("  NOAA 站点数据爬取程序")
    print("=" * 70)
    print("\n请选择单位制：")
    print("  1. 公制 (Metric) - m/s, mb, °C")
    print("  2. 英制 (English) - kts, in, °F")
    print("-" * 70)
    unit_choice = input("请选择 (1-2，默认1): ").strip() or "1"
    unit_system = 'metric' if unit_choice == '1' else 'english'
    scraper = NOAAStationScraper(unit_system=unit_system)
    print("\n" + "=" * 70)
    print(f"  配置:")
    print(f"  - 单位制: {'公制 (Metric)' if unit_system == 'metric' else '英制 (English)'}")
    print(f"  - 线程数: {scraper.max_workers}")
    print(f"  - 数据文件夹: {scraper.data_folder}")
    print("=" * 70)
    print("\n请选择运行模式：")
    print("  1. 测试单个站点（示例：CNBF1）")
    print("  2. 爬取所有站点")
    print("  3. 爬取指定站点列表")
    print("  4. 自动定时爬取（默认10分钟间隔）")
    print("  5. 自定义间隔自动爬取")
    print("  0. 退出")
    print("-" * 70)
    try:
        choice = input("请输入选项 (0-5): ").strip()
        if choice == '1':
            station_id = input("请输入站点ID（默认：CNBF1）: ").strip() or "CNBF1"
            scraper.scrape_single_station_demo(station_id)
        elif choice == '2':
            print("\n开始爬取所有站点...")
            confirm = input("这可能需要较长时间，确认继续？(y/n): ").strip().lower()
            if confirm == 'y':
                scraper.scrape_all_stations(use_multithread=True)
            else:
                print("已取消")
        elif choice == '3':
            # 爬取指定站点
            print("\n请输入站点ID列表（用逗号或空格分隔）:")
            station_input = input("站点ID: ").strip()
            # 支持逗号或空格分隔
            station_list = re.split(r'[,\s]+', station_input)
            station_list = [s.strip().upper() for s in station_list if s.strip()]
            if station_list:
                print(f"\n将爬取 {len(station_list)} 个站点: {', '.join(station_list)}")
                scraper.scrape_all_stations(station_list=station_list, use_multithread=True)
            else:
                print("未输入有效的站点ID")
        elif choice == '4':
            # 自动定时爬取（默认10分钟间隔）
            print("\n✓ 选择: 自动定时爬取（10分钟间隔）")
            scraper.run_auto(use_multithread=True)
        elif choice == '5':
            # 自定义间隔自动爬取
            print("\n✓ 选择: 自定义间隔自动爬取")
            try:
                interval = int(input("请输入更新间隔（分钟，最少5分钟）: ").strip())
                scraper.set_update_interval(interval)
                scraper.run_auto(use_multithread=True)
            except ValueError:
                print("输入无效，请输入数字")
        elif choice == '0':
            print("\n再见！")
            return
        else:
            print("\n无效选项")
    except KeyboardInterrupt:
        print("\n\n用户中断程序")
    except Exception as e:
        print(f"\n程序运行出错: {e}")
        import traceback
        traceback.print_exc()
if __name__ == "__main__":
    main()

