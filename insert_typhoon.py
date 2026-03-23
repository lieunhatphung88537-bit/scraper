import os
import json
import logging
from datetime import datetime, timezone, timedelta
import psycopg2
from psycopg2 import OperationalError
import hashlib
import uuid
from pathlib import Path
import random
import json
import os
import zipfile
import time
import schedule
import geopandas as gpd
from tempfile import TemporaryDirectory

import requests
import re
import pytz
from lxml import etree,html
import math
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from shapely.geometry import Point, Polygon
from shapely.ops import transform
import io
import tempfile
import shutil
import pyproj

from distutils.core import gen_usage
from deep_translator import GoogleTranslator
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/typhoon_insert.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
headers_list = [
        {
            'user-agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1'
        }, {
            'user-agent': 'Mozilla/5.0 (Linux; Android 8.0.0; SM-G955U Build/R16NW) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Mobile Safari/537.36'
        }, {
            'user-agent': 'Mozilla/5.0 (Linux; Android 10; SM-G981B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.162 Mobile Safari/537.36'
        }, {
            'user-agent': 'Mozilla/5.0 (iPad; CPU OS 13_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/87.0.4280.77 Mobile/15E148 Safari/604.1'
        }, {
            'user-agent': 'Mozilla/5.0 (Linux; Android 8.0; Pixel 2 Build/OPD3.170816.012) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Mobile Safari/537.36'
        }, {
            'user-agent': 'Mozilla/5.0 (Linux; Android) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.109 Safari/537.36 CrKey/1.54.248666'
        }, {
            'user-agent': 'Mozilla/5.0 (X11; Linux aarch64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.188 Safari/537.36 CrKey/1.54.250320'
        }, {
            'user-agent': 'Mozilla/5.0 (BB10; Touch) AppleWebKit/537.10+ (KHTML, like Gecko) Version/10.0.9.2372 Mobile Safari/537.10+'
        }, {
            'user-agent': 'Mozilla/5.0 (PlayBook; U; RIM Tablet OS 2.1.0; en-US) AppleWebKit/536.2+ (KHTML like Gecko) Version/7.2.1.0 Safari/536.2+'
        }, {
            'user-agent': 'Mozilla/5.0 (Linux; U; Android 4.3; en-us; SM-N900T Build/JSS15J) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30'
        }, {
            'user-agent': 'Mozilla/5.0 (Linux; U; Android 4.1; en-us; GT-N7100 Build/JRO03C) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30'
        }, {
            'user-agent': 'Mozilla/5.0 (Linux; U; Android 4.0; en-us; GT-I9300 Build/IMM76D) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30'
        }, {
            'user-agent': 'Mozilla/5.0 (Linux; Android 7.0; SM-G950U Build/NRD90M) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.84 Mobile Safari/537.36'
        }, {
            'user-agent': 'Mozilla/5.0 (Linux; Android 8.0.0; SM-G965U Build/R16NW) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.111 Mobile Safari/537.36'
        }, {
            'user-agent': 'Mozilla/5.0 (Linux; Android 8.1.0; SM-T837A) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.80 Safari/537.36'
        }, {
            'user-agent': 'Mozilla/5.0 (Linux; U; en-us; KFAPWI Build/JDQ39) AppleWebKit/535.19 (KHTML, like Gecko) Silk/3.13 Safari/535.19 Silk-Accelerated=true'
        }, {
            'user-agent': 'Mozilla/5.0 (Linux; U; Android 4.4.2; en-us; LGMS323 Build/KOT49I.MS32310c) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/102.0.0.0 Mobile Safari/537.36'
        }, {
            'user-agent': 'Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 550) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2486.0 Mobile Safari/537.36 Edge/14.14263'
        }, {
            'user-agent': 'Mozilla/5.0 (Linux; Android 6.0.1; Moto G (4)) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Mobile Safari/537.36'
        }, {
            'user-agent': 'Mozilla/5.0 (Linux; Android 6.0.1; Nexus 10 Build/MOB31T) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'
        }, {
            'user-agent': 'Mozilla/5.0 (Linux; Android 4.4.2; Nexus 4 Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Mobile Safari/537.36'
        }, {
            'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Mobile Safari/537.36'
        }, {
            'user-agent': 'Mozilla/5.0 (Linux; Android 8.0.0; Nexus 5X Build/OPR4.170623.006) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Mobile Safari/537.36'
        }, {
            'user-agent': 'Mozilla/5.0 (Linux; Android 7.1.1; Nexus 6 Build/N6F26U) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Mobile Safari/537.36'
        }, {
            'user-agent': 'Mozilla/5.0 (Linux; Android 8.0.0; Nexus 6P Build/OPP3.170518.006) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Mobile Safari/537.36'
        }, {
            'user-agent': 'Mozilla/5.0 (Linux; Android 6.0.1; Nexus 7 Build/MOB30X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'
        }, {
            'user-agent': 'Mozilla/5.0 (compatible; MSIE 10.0; Windows Phone 8.0; Trident/6.0; IEMobile/10.0; ARM; Touch; NOKIA; Lumia 520)'
        }, {
            'user-agent': 'Mozilla/5.0 (MeeGo; NokiaN9) AppleWebKit/534.13 (KHTML, like Gecko) NokiaBrowser/8.5.0 Mobile Safari/534.13'
        }, {
            'user-agent': 'Mozilla/5.0 (Linux; Android 9; Pixel 3 Build/PQ1A.181105.017.A1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.158 Mobile Safari/537.36'
        }, {
            'user-agent': 'Mozilla/5.0 (Linux; Android 10; Pixel 4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Mobile Safari/537.36'
        }, {
            'user-agent': 'Mozilla/5.0 (Linux; Android 11; Pixel 3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.181 Mobile Safari/537.36'
        }, {
            'user-agent': 'Mozilla/5.0 (Linux; Android 5.0; SM-G900P Build/LRX21T) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Mobile Safari/537.36'
        }, {
            'user-agent': 'Mozilla/5.0 (Linux; Android 8.0; Pixel 2 Build/OPD3.170816.012) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Mobile Safari/537.36'
        }, {
            'user-agent': 'Mozilla/5.0 (Linux; Android 8.0.0; Pixel 2 XL Build/OPD1.170816.004) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Mobile Safari/537.36'
        }, {
            'user-agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 10_3_1 like Mac OS X) AppleWebKit/603.1.30 (KHTML, like Gecko) Version/10.0 Mobile/14E304 Safari/602.1'
        }, {
            'user-agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1'
        }, {
            'user-agent': 'Mozilla/5.0 (iPad; CPU OS 11_0 like Mac OS X) AppleWebKit/604.1.34 (KHTML, like Gecko) Version/11.0 Mobile/15A5341f Safari/604.1'
        }
    ]

headers = random.choice(headers_list)

class TimeFormatConverter:
    @staticmethod
    def convert_to_utc(time_str: str) -> str:
        if 'AST' in time_str:
            tz = pytz.timezone('America/Puerto_Rico')
            time_str = time_str.replace('AST', '').strip()
        elif 'MST' in time_str:
            tz = pytz.timezone('US/Mountain')
            time_str = time_str.replace('MST', '').strip()
        elif 'CST' in time_str:
            tz = pytz.timezone('US/Central')
            time_str = time_str.replace('CST', '').strip()
        elif 'EST' in time_str:
            tz = pytz.timezone('US/Eastern')
            time_str = time_str.replace('EST', '').strip()
        elif 'JST' in time_str:
            tz = pytz.timezone('Asia/Tokyo')
            time_str = time_str.replace('JST', '').strip()
        elif 'KST' in time_str:
            tz = pytz.timezone('Asia/Seoul')
            time_str = time_str.replace('KST', '').strip()
        elif 'CST_CHINA' in time_str:
            tz = pytz.timezone('Asia/Shanghai')
            time_str = time_str.replace('CST_CHINA', '').strip()
        elif 'IST' in time_str:
            tz = pytz.timezone('Asia/Kolkata')
            time_str = time_str.replace('IST', '').strip()
        elif 'GMT' in time_str:
            tz = pytz.timezone('GMT')
            time_str = time_str.replace('GMT', '').strip()
        elif 'PDT' in time_str:
            tz = pytz.timezone('US/Pacific')
            time_str = time_str.replace('PDT', '').strip()
        elif 'EDT' in time_str:
            tz = pytz.timezone('America/New_York')  # EDT/EST are handled automatically by this
            time_str = time_str.replace('EDT', '').strip()
        elif 'PST' in time_str:
            tz = pytz.timezone('America/Los_Angeles')
            time_str = time_str.replace('PST', '').strip()
        else:
            raise ValueError("不支持的时区")

        time_str = re.sub(r'\s+', ' ', time_str.strip())

        try:
            dt = datetime.strptime(time_str, '%Y-%m-%d %I:%M %p %a')
        except ValueError:
            match = re.search(r'^(\d{1,4})\s*(AM|PM)\s+(.+)', time_str, re.IGNORECASE)
            if not match:
                raise ValueError(f"无法解析时间格式: {time_str}")

            time_num = match.group(1)
            am_pm = match.group(2).upper()
            rest = match.group(3)
            if len(time_num) == 3:
                hour = int(time_num[0])
                minute = int(time_num[1:])
            elif len(time_num) == 4:
                hour = int(time_num[:2])
                minute = int(time_num[2:])
            elif len(time_num) <= 2:
                hour = int(time_num)
                minute = 0
            else:

                raise ValueError("不支持的时间格式")

            formatted_time = f"{hour:02d}:{minute:02d} {am_pm}"
            final_str = f"{formatted_time} {rest}"

            dt = datetime.strptime(final_str, '%I:%M %p %a %b %d %Y')
        local_dt = tz.localize(dt)
        utc_dt = local_dt.astimezone(pytz.utc)

        return utc_dt.strftime('%Y-%m-%d %H:%M:%S')
class NHCDataCollector:
    def __init__(self):
        self.data_history = []
        self.data_pre = []
        self.history_filename = []
        self.pre_filename = []
    def extract_links_by_pattern(self, url):
        response = requests.get(url,headers=headers)
        response.raise_for_status()
        tree = html.fromstring(response.content)

        xpath_pattern = '//table//tr//td[4]/a'

        links = []
        for a in tree.xpath(xpath_pattern):
            href = a.get('href')
            text = a.text_content().strip()
            if href:
                full_url = urljoin(url, href)
                links.append({
                    'text': str(text),
                    'href': href,
                    'full_url': full_url
                })

        return links

    def find_all_similar_links(self, base_url):
        response = requests.get(base_url,headers=headers)
        response.raise_for_status()
        tree = html.fromstring(response.content)
        xpath_expr = '//div[5]/div/table//tr[3]/td/table//tr[2]/td/table//tr[2]/td[3]/a'
        a_elements = tree.xpath(xpath_expr)
        results = []
        for a in a_elements:
            href = a.get('href')
            text = a.text_content().strip()
            if href:
                full_url = urljoin(base_url, href)
                results.append({
                    'text': str(text),
                    'full_url': full_url
                })

        return results

    def get_last_zip_from_gis_page(self, main_page_url):
        resp = requests.get(main_page_url,headers=headers)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')

        a1 = soup.find('a', string="Download GIS Data")
        if a1 is None:
            logging.error("未在主页面找到 Download GIS Data 链接")
        href1 = a1.get('href')
        gis_page_url = urljoin(main_page_url, href1)
        resp2 = requests.get(gis_page_url)
        resp2.raise_for_status()
        soup2 = BeautifulSoup(resp2.text, 'html.parser')
        zip_links = []
        for a in soup2.find_all('a', href=True):
            href = a['href']
            if href.lower().endswith('.zip'):
                full_url = urljoin(gis_page_url, href)
                zip_links.append(full_url)

        if not zip_links:
            raise RuntimeError("在 GIS 页面未找到任何 .zip 链接")
        last_zip = zip_links[-1]
        return last_zip

    def scrape_typhoon_data(sef, base_url):
        """主抓取函数，遍历直到页面不存在"""
        #base_url = 'https://www.nhc.noaa.gov/text/refresh/MIATCMAT2+shtml/182051.shtml'
        try:
            url = base_url
            response = requests.get(url,headers=headers, timeout=30)
            response.encoding = response.apparent_encoding
            if response.status_code == 404:
                logging.error(f"页面不存在: {url}，停止抓取")
            if response.status_code != 200:
                logging.error(f"HTTP错误: {response.status_code}，跳过")

            label = etree.HTML(response.text)
            context_elements = label.xpath('/html/body/div[5]/div/div[2]/div/pre/text()')

            context = context_elements[0].split(' \n')
            return context

        except requests.exceptions.RequestException as e:
            logging.error(f"网络错误 : {e}")
        except Exception as e:
            logging.error(f"处理错误 : {e}")

    def save_data(self, only_name, context, png_data, pts_data, output_dir="NHC"):
        def max_inscribed_circle_radius(lon_lat_polygon, center_lon_lat):
            """
            计算多边形内，以指定中心点为圆心的最大内切圆半径（单位：公里）
            :param lon_lat_polygon: 多边形边界点列表 [(lon1, lat1), (lon2, lat2), ...]
            :param center_lon_lat: 圆心坐标 (lon, lat)
            :return: 最大内切圆半径，单位为海里（nm）
            """
            polygon = Polygon(lon_lat_polygon)
            center = Point(center_lon_lat)

            if lon_lat_polygon[0] != lon_lat_polygon[-1]:
                polygon = Polygon(lon_lat_polygon + [lon_lat_polygon[0]])
            proj_local = pyproj.Proj(proj='aeqd', lat_0=center.y, lon_0=center.x, datum='WGS84')
            project = lambda x, y: proj_local(x, y)

            polygon_proj = transform(project, polygon)
            center_proj = transform(project, center)

            distance_meters = polygon_proj.exterior.distance(center_proj)
            distance_km = distance_meters / 1000.0
            distance_nm = distance_km / 1.852
            return round(distance_nm, 1)
        adv_data = context
        def save_history_data():
            def find_direction(text):
                """确定播报台风移动方向和移动速度"""
                direction_desc = 'null'
                move_speed = '-999'
                direction_mapping = {
                    "NORTH": "北",
                    "NORTH-NORTHEAST": "北东北",
                    "NORTHEAST": "东北",
                    "EAST-NORTHEAST": "东东北",
                    "EAST": "东",
                    "EAST-SOUTHEAST": "东东南",
                    "SOUTHEAST": "东南",
                    "SOUTH-SOUTHEAST": "南东南",
                    "SOUTH": "南",
                    "SOUTH-SOUTHWEST": "南西南",
                    "SOUTHWEST": "西南",
                    "WEST-SOUTHWEST": "西西南",
                    "WEST": "西",
                    "WEST-NORTHWEST": "西西北",
                    "NORTHWEST": "西北",
                    "NORTH-NORTHWEST": "北西北",
                    "VAR": "多变",
                    "STNR": "静止",
                    "": "",
                    "NULL": ""
                }
                pattern = r'.*TOWARD\s*THE\s*([A-Z\-]+)\s*OR\s*(\d{1,3})\s*DEGREES\s*AT\s*(\d{1,2}) KT'
                match1 = re.search(pattern, text[3])
                match2 = re.search(pattern, text[4])
                if match1:
                    direction_desc = match1.group(1)
                    move_speed = match1.group(3)
                    direction_desc = direction_mapping.get(direction_desc, direction_desc)
                elif match2:
                    direction_desc = match2.group(1)
                    move_speed = match2.group(3)
                    direction_desc = direction_mapping.get(direction_desc, direction_desc)
                return direction_desc, move_speed

            def find_rad(text):
                """解析给出的风圈半径"""
                def com_rad(ne, se, sw, nw):
                    try:
                        total_area = (math.pi / 4) * (int(ne) ** 2 + int(se) ** 2 + int(sw) ** 2 + int(nw) ** 2)
                        equivalent_radius_area = math.sqrt(total_area / math.pi)
                        return f"{equivalent_radius_area:.2f}"
                    except:
                        return '-999'
                rad_data = {
                    'rad7': '-999', 'rad8(34kt)': '-999','rad10(50kt)': '-999', 'rad12(64kt)': '-999',
                    'r8Ne': '-999', 'r8Se': '-999', 'r8Sw': '-999', 'r8Nw': '-999',
                    'r7Ne': '-999', 'r7Se': '-999', 'r7Sw': '-999', 'r7Nw': '-999',
                    'r10Ne': '-999', 'r10Se': '-999', 'r10Sw': '-999', 'r10Nw': '-999',
                    'r12Ne': '-999', 'r12Se': '-999', 'r12Sw': '-999', 'r12Nw': '-999'
                }

                if 'ESTIMATED' in text[4]:
                    data = text[4].split('\n')
                else:
                    data = text[5].split('\n')
                for line in data:

                    pattern3 = r'(\d{1,3})\s*KT\.*\s*(\d{1,3})NE\s*(\d{1,3})SE\s*(\d{1,3})SW\s*(\d{1,3})NW'
                    match3 = re.search(pattern3, line)
                    if match3:
                        rad = match3.group(1)
                        if rad == '64':
                            rad_data.update({
                                'r12Ne': match3.group(2), 'r12Se': match3.group(3),
                                'r12Sw': match3.group(4), 'r12Nw': match3.group(5),
                                'rad12(64kt)': com_rad(match3.group(2), match3.group(3), match3.group(4), match3.group(5))
                            })
                        elif rad == '50':
                            rad_data.update({
                                'r10Ne': match3.group(2), 'r10Se': match3.group(3),
                                'r10Sw': match3.group(4), 'r10Nw': match3.group(5),
                                'rad10(50kt)': com_rad(match3.group(2), match3.group(3), match3.group(4), match3.group(5))
                            })
                        elif rad == '34':
                            rad_data.update({
                                'r8Ne': match3.group(2), 'r8Se': match3.group(3),
                                'r8Sw': match3.group(4), 'r8Nw': match3.group(5),
                                'rad8(34kt)': com_rad(match3.group(2), match3.group(3), match3.group(4), match3.group(5))
                            })
                return rad_data

            rad_data = find_rad(adv_data)
            pts_item = pts_data['features'][0]['properties']
            dir, movespeed = find_direction(adv_data)
            name = pts_item['STORMNAME']
            if name != ' ' and name != '':
                name = name.split(' ')[-1]
            pre_result = [{
                    "name":name,
                    "intensity":pts_item['STORMTYPE'],
                    "lat": str(pts_item['LAT']),
                    "lng": str(pts_item['LON']),
                    "radius7": rad_data['rad7'],
                    "radius8(34kt)": rad_data['rad8(34kt)'],
                    "radius10(50kt)": rad_data['rad10(50kt)'],
                    "radius12(64kt)": rad_data['rad12(64kt)'],
                    "r7Ne": rad_data['r7Ne'],
                    "r7Se": rad_data['r7Se'],
                    "r7Sw": rad_data['r7Sw'],
                    "r7Nw": rad_data['r7Nw'],
                    "r8Ne": rad_data['r8Ne'],
                    "r8Se": rad_data['r8Se'],
                    "r8Sw": rad_data['r8Sw'],
                    "r8Nw": rad_data['r8Nw'],
                    "r10Ne": rad_data['r10Ne'],
                    "r10Se": rad_data['r10Se'],
                    "r10Sw": rad_data['r10Sw'],
                    "r10Nw": rad_data['r10Nw'],
                    "r12Ne": rad_data['r12Ne'],
                    "r12Se": rad_data['r12Se'],
                    "r12Sw": rad_data['r12Sw'],
                    "r12Nw": rad_data['r12Nw'],
                    "centerSpeed": str(int(pts_item['MAXWIND'])),
                    "bizDate": TimeFormatConverter.convert_to_utc(pts_item['FLDATELBL']),
                    "centerPressure": str(int(pts_item['MSLP'])),
                    "moveSpeed": movespeed,
                    "moveDirection": dir,
                    "forecastSource": "NHC",
                    "forecastOrg": "USA",
                    "gust": str(pts_item['GUST']),
                    "update_Org_time": TimeFormatConverter.convert_to_utc(pts_item['ADVDATE']),
                    "code": str(pts_item['STORMNUM']),
                    "probabilityCircle":  str(max_inscribed_circle_radius(png_data['features'][0]['geometry']['coordinates'][0],[pts_item['LON'],pts_item['LAT']])),
                }]

            pre_time =  TimeFormatConverter.convert_to_utc(pts_item['ADVDATE'])
            safe_time = pre_time.replace(":", "").replace(" ", "_").replace("-", "_")
            output_filename = f'NHC/{pts_data["features"][0]["properties"]["BASIN"]}/{only_name}/NHC_history_{safe_time}.json'


            return output_filename, pre_result

        def save_forecast_data():
            def find_rad(text):
                """解析给出的风圈半径"""
                def com_rad(ne, se, sw, nw):
                    try:
                        total_area = (math.pi / 4) * (int(ne) ** 2 + int(se) ** 2 + int(sw) ** 2 + int(nw) ** 2)
                        equivalent_radius_area = math.sqrt(total_area / math.pi)
                        return f"{equivalent_radius_area:.2f}"
                    except:
                        return '-999'
                rad_data = {
                    'rad7': '-999', 'rad8(34kt)': '-999','rad10(50kt)': '-999', 'rad12(64kt)': '-999',
                    'r8Ne': '-999', 'r8Se': '-999', 'r8Sw': '-999', 'r8Nw': '-999',
                    'r7Ne': '-999', 'r7Se': '-999', 'r7Sw': '-999', 'r7Nw': '-999',
                    'r10Ne': '-999', 'r10Se': '-999', 'r10Sw': '-999', 'r10Nw': '-999',
                    'r12Ne': '-999', 'r12Se': '-999', 'r12Sw': '-999', 'r12Nw': '-999'
                }
                pattern3 = r'(\d{1,3})\s*KT\.*\s*(\d{1,3})NE\s*(\d{1,3})SE\s*(\d{1,3})SW\s*(\d{1,3})NW'
                match3 = re.search(pattern3, text)
                if match3:
                    rad = match3.group(1)
                    if rad == '64':
                        rad_data.update({
                            'r12Ne': match3.group(2), 'r12Se': match3.group(3),
                            'r12Sw': match3.group(4), 'r12Nw': match3.group(5),
                            'rad12(64kt)': com_rad(match3.group(2), match3.group(3), match3.group(4), match3.group(5))
                        })
                    elif rad == '50':
                        rad_data.update({
                            'r10Ne': match3.group(2), 'r10Se': match3.group(3),
                            'r10Sw': match3.group(4), 'r10Nw': match3.group(5),
                            'rad10(50kt)': com_rad(match3.group(2), match3.group(3), match3.group(4), match3.group(5))
                        })
                    elif rad == '34':
                        rad_data.update({
                            'r8Ne': match3.group(2), 'r8Se': match3.group(3),
                            'r8Sw': match3.group(4), 'r8Nw': match3.group(5),
                            'rad8(34kt)': com_rad(match3.group(2), match3.group(3), match3.group(4), match3.group(5))
                        })
                return rad_data
            result = []
            i = 0
            for pts_item in pts_data['features'][1:]:
                line = adv_data[5+i]
                rad7, rad10 = '-999', '-999'
                r8Ne,r8Se,r8Sw,r8Nw = '-999', '-999', '-999','-999'
                r10Ne, r10Se, r10Sw, r10Nw = '-999', '-999', '-999', '-999'
                while ('FORECAST VALID' not in line) and ('OUTLOOK VALID' not in line):
                    i += 1
                    line = adv_data[5+i]

                data = line.split('\n')
                rad_data = {
                    'rad7': '-999', 'rad8(34kt)': '-999', 'rad10(50kt)': '-999', 'rad12(64kt)': '-999',
                    'r8Ne': '-999', 'r8Se': '-999', 'r8Sw': '-999', 'r8Nw': '-999',
                    'r7Ne': '-999', 'r7Se': '-999', 'r7Sw': '-999', 'r7Nw': '-999',
                    'r10Ne': '-999', 'r10Se': '-999', 'r10Sw': '-999', 'r10Nw': '-999',
                    'r12Ne': '-999', 'r12Se': '-999', 'r12Sw': '-999', 'r12Nw': '-999'
                }
                if len(data) >= 3:
                    for l in data[2:-1]:
                        rad_data = find_rad(l)
                name = pts_item['properties']['STORMNAME']
                if name != ' ' and name != '':
                    name = name.split(' ')[-1]
                forecast_data = {
                    "name": name,
                    "intensity": pts_item['properties']['STORMTYPE'],
                    "lat": str(pts_item['properties']['LAT']),
                    "lng": str(pts_item['properties']['LON']),
                    'bizDate': TimeFormatConverter.convert_to_utc(pts_item['properties']['FLDATELBL']),
                    "update_Org_time": TimeFormatConverter.convert_to_utc(pts_item['properties']['ADVDATE']),
                    "code": str(pts_item['properties']['STORMNUM']),
                    "forecastSource": "NHC",
                    "forecastOrg": "USA",
                    "moveSpeed": "-999",
                    "gust": str(pts_item['properties']['GUST']),
                    "centerPressure": str(int(pts_item['properties']['MSLP'])),
                    "centerSpeed": str(int(pts_item['properties']['MAXWIND'])),
                    "radius7": rad_data['rad7'],
                    "radius8(34kt)": rad_data['rad8(34kt)'],
                    "radius10(50kt)": rad_data['rad10(50kt)'],
                    "radius12(64kt)": rad_data['rad12(64kt)'],
                    "r7Ne": rad_data['r7Ne'],
                    "r7Se": rad_data['r7Se'],
                    "r7Sw": rad_data['r7Sw'],
                    "r7Nw": rad_data['r7Nw'],
                    "r8Ne": rad_data['r8Ne'],
                    "r8Se": rad_data['r8Se'],
                    "r8Sw": rad_data['r8Sw'],
                    "r8Nw": rad_data['r8Nw'],
                    "r10Ne": rad_data['r10Ne'],
                    "r10Se": rad_data['r10Se'],
                    "r10Sw": rad_data['r10Sw'],
                    "r10Nw": rad_data['r10Nw'],
                    "r12Ne": rad_data['r12Ne'],
                    "r12Se": rad_data['r12Se'],
                    "r12Sw": rad_data['r12Sw'],
                    "r12Nw": rad_data['r12Nw'],
                    "probabilityCircle": str(
                        max_inscribed_circle_radius(png_data['features'][0]['geometry']['coordinates'][0],
                                                    [pts_item['properties']['LON'], pts_item['properties']['LAT']]))
                }
                result.append(forecast_data)

                i += 1
            pre_time = TimeFormatConverter.convert_to_utc(pts_data['features'][0]['properties']['ADVDATE'])
            safe_time = pre_time.replace(":", "").replace(" ", "_").replace("-", "_")
            output_dir = f'NHC/{pts_data["features"][0]["properties"]["BASIN"]}/{only_name}/NHC_pre_{safe_time}.json'



            return output_dir, result

        his_path, his_data = save_history_data()
        pre_path, pre_data = save_forecast_data()
        return his_path, his_data, pre_path, pre_data

    def download_zip_in_memory(self, url):
        """下载 ZIP 文件到内存（BytesIO）"""
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return io.BytesIO(response.content)
        except requests.exceptions.RequestException as e:
            logging.error(f"下载文件时出错: {e}")
            return None

    def parse_gis_zip_in_tmp(self, zip_bytes):
        """
        解析文件名包含 'pts' 或 'png'/'pgn' 的 shapefile
        在临时目录中解压、解析，然后自动删除
        不进行几何过滤，保留所有几何
        """
        geojson_results = []
        temp_dir = tempfile.mkdtemp(prefix="gis_zip_")

        try:
            # 解压到临时目录
            with zipfile.ZipFile(zip_bytes) as zf:
                zf.extractall(temp_dir)

            # 找出 .shp 文件
            shp_files = list(Path(temp_dir).rglob("*.shp"))
            if not shp_files:
                logging.error(" ZIP 中未找到 .shp 文件")
                return []

            # 只保留文件名包含 'pts' 或 'png'/'pgn' 的 shapefile
            shp_files = [f for f in shp_files if any(k in f.stem.lower() for k in ["pts", "png", "pgn"])]

            for shp_path in shp_files:
                try:
                    gdf = gpd.read_file(shp_path)

                    if gdf.crs is None or gdf.crs.to_epsg() is None:
                        gdf = gdf.set_crs("EPSG:4326", allow_override=True)

                    for col in gdf.columns:
                        if pd.api.types.is_datetime64_any_dtype(gdf[col]):
                            gdf[col] = gdf[col].apply(
                                lambda x: x.strftime('%Y-%m-%d %I:%M %p %a GMT') if pd.notnull(x) else None
                            )

                    geojson_data = json.loads(gdf.to_json())
                    geojson_data["file_name"] = Path(shp_path).stem
                    geojson_results.append(geojson_data)

                except Exception as e:
                    logging.error(f"读取 shapefile 失败: {shp_path}\n原因: {e}")

        except Exception as e:
            logging.error(f"解析 ZIP 文件出错: {e}")

        finally:
            # 删除临时目录
            try:
                shutil.rmtree(temp_dir)
            except Exception as e:
                logging.error(f"删除临时目录失败: {e}")

        return geojson_results

    def main_task(self):
        try:
            base_url = 'https://www.nhc.noaa.gov/?epac'
            forecast_links = self.find_all_similar_links(base_url)
            gis_links = self.extract_links_by_pattern(base_url)

            forecast_lists = [
                link['full_url'] for link in forecast_links if "Forecast" in link['text']
            ]
            gis_lists = [
                item['full_url'] for item in gis_links if 'Warnings/ConeInteractive Map' in item['text']
            ]

            # 遍历每个台风
            for i in range(len(forecast_lists)):
                try:
                    context = self.scrape_typhoon_data(forecast_lists[i])
                    main_url = gis_lists[i]

                    # 从 GIS 页面获取 shapefile ZIP 链接
                    zip_url = self.get_last_zip_from_gis_page(main_url)
                    if not zip_url:
                        logging.error(f"无法找到 GIS ZIP 链接: {main_url}")
                        continue

                    zip_bytes = self.download_zip_in_memory(zip_url)  # 内存下载
                    if not zip_bytes:
                        logging.error(f"下载 ZIP 失败: {zip_url}")
                        continue

                    geojson_list = self.parse_gis_zip_in_tmp(zip_bytes)  # 临时目录解析 shapefile


                    png_data, pts_data = None, None
                    for g in geojson_list:
                        fname = g.get("file_name", "").lower()
                        if any(k in fname for k in ["png", "pgn"]):
                            png_data = g
                        elif "pts" in fname:
                            pts_data = g
                    if png_data is None or pts_data is None:
                        logging.error(f"未找到 PNG/PTS 数据: {zip_url}")
                    else:
                        only_name = png_data['file_name'].split('-')[0]
                        his_path, his_data, pre_path, pre_data = self.save_data(only_name, context, png_data, pts_data)
                        self.history_filename.append(his_path)
                        self.data_history.append(his_data)
                        self.pre_filename.append(pre_path)
                        self.data_pre.append(pre_data)
                except Exception as e:
                    logging.error(f"台风任务出错 [{forecast_lists[i]}]: {e}")

        except Exception as e:
            logging.error(f"任务执行出错: {e}")

class WMODataCollector:
    def __init__(self):
        self.DEFAULT_MISSING_VALUE = "-999"
        self.data_history = []
        self.data_pre = []
        self.history_filename = []
        self.pre_filename = []
    def get_save_history_data(self, sys_id,history_url,storm_id,storm_name):
        def translate_direction(direction):
            return direction_mapping.get(direction)

        def parse_wind_radii_field(field):
            if not field:
                return []
            result = []
            wind_levels = field.split("##")
            for level in wind_levels:
                parts = level.split(";;")
                if not parts:
                    continue
                wind_speed = parts[0].strip(' kt')
                radii_info = parts[1:]
                radii_list = []
                for info in radii_info:
                    sub_parts = info.split(";;") if ';;' in info else [info]
                    for sub in sub_parts:
                        if "|" in sub:
                            radius, direction = sub.split("|")
                            radii_list.append({
                                'radius': radius.strip('nm'),
                                'direction': direction.strip()
                            })
                        else:
                            radii_list.append({
                                'radius': sub.strip('nm'),
                                'direction': None
                            })
                result.append({
                    'wind_speed': wind_speed,
                    'radii': radii_list
                })
            return result
        def process_data_history(data_id,data, new_history_fields):
            def process_item(item):
                # 删除字段
                item.pop('center_id', None)
                item['name'] = item.get('tc_name', '')
                item.pop('tc_name', None)
                if 'gust' in item and item['gust'] is not None:
                    item['gust'] = item.pop('gust', '-999')
                else:
                    item['gust'] = '-999'
                radius_fields = ['radius7', 'radius10', 'radius12', 'r7N', 'r7S', 'r7E', 'r7W', 'r12N', 'r12S', 'r12E', 'r12W',
                                 'r10N', 'r10S', 'r10E', 'r10W', 'r7Ne', 'r10Ne', 'r12Ne',
                                 'r7Se', 'r10Se', 'r12Se', 'r7Sw', 'r10Sw', 'r12Sw',
                                 'r7Nw', 'r10Nw', 'r12Nw']
                for field in radius_fields:
                    item[field] = self.DEFAULT_MISSING_VALUE
                windrad_value = item.get('wind_radii')
                if windrad_value in [None, 'null', 'None', '']:
                    item.pop('wind_radii', None)
                else:
                    parsed = parse_wind_radii_field(windrad_value)
                    for wind_item in parsed:
                        if wind_item['wind_speed'] == '50':
                            for data in wind_item['radii']:
                                direction = data.get('direction')
                                radius = data.get('radius', self.DEFAULT_MISSING_VALUE)
                                if direction is None:
                                    item['radius10'] = radius
                                elif direction == 'NORTH':
                                    item['r10N'] = radius
                                elif direction == 'SOUTH':
                                    item['r10S'] = radius
                                elif direction == 'EAST':
                                    item['r10E'] = radius
                                elif direction == 'WEST':
                                    item['r10W'] = radius
                                elif direction == 'NORTHEAST':
                                    item['r10Ne'] = radius
                                elif direction == 'SOUTHEAST':
                                    item['r10Se'] = radius
                                elif direction == 'SOUTHWEST':
                                    item['r10Sw'] = radius
                                elif direction == 'NORTHWEST':
                                    item['r10Nw'] = radius
                        elif wind_item['wind_speed'] == '30':
                            for data in wind_item['radii']:
                                direction = data.get('direction')
                                radius = data.get('radius', self.DEFAULT_MISSING_VALUE)
                                if direction is None:
                                    item['radius7'] = radius
                                elif direction == 'NORTH':
                                    item['r7N'] = radius
                                elif direction == 'SOUTH':
                                    item['r7S'] = radius
                                elif direction == 'EAST':
                                    item['r7E'] = radius
                                elif direction == 'WEST':
                                    item['r7W'] = radius
                                elif direction == 'NORTHEAST':
                                    item['r7Ne'] = radius
                                elif direction == 'SOUTHEAST':
                                    item['r7Se'] = radius
                                elif direction == 'SOUTHWEST':
                                    item['r7Sw'] = radius
                                elif direction == 'NORTHWEST':
                                    item['r7Nw'] = radius
                        elif wind_item['wind_speed'] == '64':
                            for data in wind_item['radii']:
                                direction = data.get('direction')
                                radius = data.get('radius', self.DEFAULT_MISSING_VALUE)
                                if direction is None:
                                    item['radius12'] = radius
                                elif direction == 'NORTH':
                                    item['r12N'] = radius
                                elif direction == 'SOUTH':
                                    item['r12S'] = radius
                                elif direction == 'EAST':
                                    item['r12E'] = radius
                                elif direction == 'WEST':
                                    item['r12W'] = radius
                                elif direction == 'NORTHEAST':
                                    item['r12Ne'] = radius
                                elif direction == 'SOUTHEAST':
                                    item['r12Se'] = radius
                                elif direction == 'SOUTHWEST':
                                    item['r12Sw'] = radius
                                elif direction == 'NORTHWEST':
                                    item['r12Nw'] = radius
                    item.pop('wind_radii', None)
                # max_wind_speed -> centerSpeed
                max_ws = item.get('max_wind_speed')
                if not max_ws or not max_ws.isdigit():
                    item['centerSpeed'] = self.DEFAULT_MISSING_VALUE
                    item.pop('max_wind_speed', None)
                else:
                    item['centerSpeed'] = item.pop('max_wind_speed')

                # intensity 字段
                intensity_value = item.get('intensity')
                if intensity_value in [None, 'null', 'None', '']:
                    item['intensity'] = None
                else:
                    item['intensity'] = intensity_value

                # analysis_time -> bizDate
                if 'analysis_time' in item:
                    item['bizDate'] = item.pop('analysis_time')
                else:
                    item['bizDate'] = None

                # pressure -> centerPressure
                pressure = item.get('pressure')
                if not pressure or not (str(pressure).isdigit()):
                    item['centerPressure'] = self.DEFAULT_MISSING_VALUE
                    item.pop('pressure', None)
                else:
                    item['centerPressure'] = item.pop('pressure')

                # speed_of_movement -> moveSpeed
                speed = item.get('speed_of_movement')
                if not speed or not (str(speed).isdigit()):
                    item['moveSpeed'] = self.DEFAULT_MISSING_VALUE
                    item.pop('speed_of_movement', None)
                else:
                    item['moveSpeed'] = item.pop('speed_of_movement')

                # movement_direction -> moveDirection, 并翻译方向
                if 'movement_direction' in item:
                    item['moveDirection'] = translate_direction(item.pop('movement_direction'))
                else:
                    item['moveDirection'] = None

                # tc_id -> code
                tc_id = item.get('tc_id')
                if not tc_id:
                    item['code'] = ""
                    item.pop('tc_id', None)
                else:
                    item['code'] = item.pop('tc_id')

                item['sys_id'] = data_id
                # 添加新字段
                item.update(new_history_fields)
                try:
                    item['lat'] = f"{float(item.get('lat', self.DEFAULT_MISSING_VALUE)):.6f}"
                except (TypeError, ValueError):
                    item['lat'] = self.DEFAULT_MISSING_VALUE

                try:
                    item['lng'] = f"{float(item.get('lng', self.DEFAULT_MISSING_VALUE)):.6f}"
                except (TypeError, ValueError):
                    item['lng'] = self.DEFAULT_MISSING_VALUE
                return item

            data = [process_item(item) for item in data]
            if data:
                last_item = data[-1]
                biz_date = last_item.get('bizDate')
                update_Org_time = last_item.get('update_Org_time')
                try:
                    fmt = "%Y-%m-%d %H:%M:%S"
                    if biz_date and update_Org_time:
                        biz_dt = datetime.strptime(biz_date, fmt)
                        upd_dt = datetime.strptime(update_Org_time, fmt)
                        if biz_dt > upd_dt:
                            data.pop()
                except Exception:
                    pass
            for item in data:
                item['update_Org_time'] = data[-1].get('bizDate')

            return data

        direction_mapping = {
            "N": "北",
            "NNE": "北东北",
            "NE": "东北",
            "ENE": "东东北",
            "E": "东",
            "ESE": "东东南",
            "SE": "东南",
            "SSE": "南东南",
            "S": "南",
            "SSW": "南西南",
            "SW": "西南",
            "WSW": "西西南",
            "W": "西",
            "WNW": "西西北",
            "NW": "西北",
            "NNW": "北西北",
            "VAR": "多变",
            "STNR": "静止",
            "": "",
            "null": ""
        }
        try:
            headers = random.choice(headers_list)
            wmo_response = requests.get(history_url, headers=headers, timeout=5)
            wmo_response.raise_for_status()
            wmo_data = wmo_response.json()
        except requests.RequestException as e:
            logging.error(f"请求历史数据失败: {e}")
            return False
        except json.JSONDecodeError as e:
            logging.error(f"解析历史数据失败: {e}")
            return False
        data_id = wmo_data.get('sys_id',None)
        data_history = wmo_data.get('track', [])

        new_history_fields = {
            "update_Org_time": None,
            "sys_id": str(sys_id),
            "forcastSource":"WMO"
        }

        all_data = process_data_history(str(data_id), data_history, new_history_fields)

        try:
            dirs = f"wmo_data/{sys_id}"
            history_filename = f"{dirs}/onlyhistory.json"

            return history_filename, all_data

        except Exception as e:
            logging.error(f"数据保存失败: {e}")
            return None, None

    def get_data(self, sys_id, wmo_url,storm_id,storm_name):
        def translate_direction(direction):
            return direction_mapping.get(direction)

        def parse_wind_radii_field(field):
            if not field:
                return []
            KM_TO_NM = 0.539957
            result = []
            wind_levels = field.split("##")

            for level in wind_levels:
                parts = level.split(";;")
                if len(parts) < 2:
                    continue
                wind_speed_part = parts[0].strip()
                if "kt" in wind_speed_part.lower():
                    try:
                        wind_speed = wind_speed_part.lower().replace("kt", "").strip()
                    except ValueError:
                        continue
                else:
                    continue

                radii_list = []

                for info in parts[1:]:
                    info = info.strip()
                    if not info:
                        continue
                    if "|" in info:
                        radius_str, direction = info.split("|")
                        direction = direction.strip().upper()
                    else:
                        radius_str = info
                        direction = None

                    radius_str = radius_str.strip().upper()

                    try:
                        if radius_str.endswith("KM"):
                            radius_val = float(radius_str.replace("KM", "").strip()) * KM_TO_NM
                        elif radius_str.endswith("NM"):
                            radius_val = float(radius_str.replace("NM", "").strip())
                        else:
                            radius_val = float(radius_str)
                    except ValueError:
                        continue

                    radii_list.append({
                        'radius': str(round(radius_val, 2)),
                        'direction': direction
                    })

                result.append({
                    'wind_speed': wind_speed,
                    'radii': radii_list
                })

            return result

        def process_data_history(data, new_history_fields):
            def process_item(item):
                # 删除字段
                item.pop('center_id', None)
                item['name'] = item.get('tc_name', '')
                item.pop('tc_name', None)
                if 'gust' in item and item['gust'] is not None:
                    item['gust'] = item.pop('gust', '-999')
                else:
                    item['gust'] = '-999'
                radius_fields = ['radius7', 'radius10', 'radius12', 'r7N', 'r7S', 'r7E', 'r7W', 'r12N', 'r12S', 'r12E', 'r12W',
                                 'r10N', 'r10S', 'r10E', 'r10W', 'r7Ne', 'r10Ne', 'r12Ne',
                                 'r7Se', 'r10Se', 'r12Se', 'r7Sw', 'r10Sw', 'r12Sw',
                                 'r7Nw', 'r10Nw', 'r12Nw']
                for field in radius_fields:
                    item[field] = self.DEFAULT_MISSING_VALUE
                windrad_value = item.get('wind_radii')
                if windrad_value in [None, 'null', 'None', '']:
                    item.pop('wind_radii', None)
                else:
                    parsed = parse_wind_radii_field(windrad_value)
                    for wind_item in parsed:
                        if wind_item['wind_speed'] == '50':
                            for data in wind_item['radii']:
                                direction = data.get('direction')
                                radius = data.get('radius', self.DEFAULT_MISSING_VALUE)
                                if direction is None:
                                    item['radius10'] = radius
                                elif direction == 'NORTH':
                                    item['r10N'] = radius
                                elif direction == 'SOUTH':
                                    item['r10S'] = radius
                                elif direction == 'EAST':
                                    item['r10E'] = radius
                                elif direction == 'WEST':
                                    item['r10W'] = radius
                                elif direction == 'NORTHEAST':
                                    item['r10Ne'] = radius
                                elif direction == 'SOUTHEAST':
                                    item['r10Se'] = radius
                                elif direction == 'SOUTHWEST':
                                    item['r10Sw'] = radius
                                elif direction == 'NORTHWEST':
                                    item['r10Nw'] = radius
                        elif wind_item['wind_speed'] == '30':
                            for data in wind_item['radii']:
                                direction = data.get('direction')
                                radius = data.get('radius', self.DEFAULT_MISSING_VALUE)
                                if direction is None:
                                    item['radius7'] = radius
                                elif direction == 'NORTH':
                                    item['r7N'] = radius
                                elif direction == 'SOUTH':
                                    item['r7S'] = radius
                                elif direction == 'EAST':
                                    item['r7E'] = radius
                                elif direction == 'WEST':
                                    item['r7W'] = radius
                                elif direction == 'NORTHEAST':
                                    item['r7Ne'] = radius
                                elif direction == 'SOUTHEAST':
                                    item['r7Se'] = radius
                                elif direction == 'SOUTHWEST':
                                    item['r7Sw'] = radius
                                elif direction == 'NORTHWEST':
                                    item['r7Nw'] = radius
                        elif wind_item['wind_speed'] == '64':
                            for data in wind_item['radii']:
                                direction = data.get('direction')
                                radius = data.get('radius', self.DEFAULT_MISSING_VALUE)
                                if direction is None:
                                    item['radius12'] = radius
                                elif direction == 'NORTH':
                                    item['r12N'] = radius
                                elif direction == 'SOUTH':
                                    item['r12S'] = radius
                                elif direction == 'EAST':
                                    item['r12E'] = radius
                                elif direction == 'WEST':
                                    item['r12W'] = radius
                                elif direction == 'NORTHEAST':
                                    item['r12Ne'] = radius
                                elif direction == 'SOUTHEAST':
                                    item['r12Se'] = radius
                                elif direction == 'SOUTHWEST':
                                    item['r12Sw'] = radius
                                elif direction == 'NORTHWEST':
                                    item['r12Nw'] = radius
                    item.pop('wind_radii', None)
                # max_wind_speed -> centerSpeed
                max_ws = item.get('max_wind_speed')
                if not max_ws or not max_ws.isdigit():
                    item['centerSpeed'] = self.DEFAULT_MISSING_VALUE
                    item.pop('max_wind_speed', None)
                else:
                    item['centerSpeed'] = item.pop('max_wind_speed')

                # intensity 字段
                intensity_value = item.get('intensity')
                if intensity_value in [None, 'null', 'None', '']:
                    item['intensity'] = None
                else:
                    item['intensity'] = intensity_value

                # analysis_time -> bizDate
                if 'analysis_time' in item:
                    item['bizDate'] = item.pop('analysis_time')
                else:
                    item['bizDate'] = None

                # pressure -> centerPressure
                pressure = item.get('pressure')
                if not pressure or not (str(pressure).isdigit()):
                    item['centerPressure'] = self.DEFAULT_MISSING_VALUE
                    item.pop('pressure', None)
                else:
                    item['centerPressure'] = item.pop('pressure')

                # speed_of_movement -> moveSpeed
                speed = item.get('speed_of_movement')
                if not speed or not (str(speed).isdigit()):
                    item['moveSpeed'] = self.DEFAULT_MISSING_VALUE
                    item.pop('speed_of_movement', None)
                else:
                    item['moveSpeed'] = item.pop('speed_of_movement')

                # movement_direction -> moveDirection
                if 'movement_direction' in item:
                    item['moveDirection'] = translate_direction(item.pop('movement_direction'))
                else:
                    item['moveDirection'] = None

                # tc_id -> code
                tc_id = item.get('tc_id')
                if not tc_id:
                    item['code'] = ""
                    item.pop('tc_id', None)
                else:
                    item['code'] = item.pop('tc_id')
                # 添加新字段
                item.update(new_history_fields)
                try:
                    item['lat'] = f"{float(item.get('lat', self.DEFAULT_MISSING_VALUE)):.6f}"
                except (TypeError, ValueError):
                    item['lat'] = self.DEFAULT_MISSING_VALUE

                try:
                    item['lng'] = f"{float(item.get('lng', self.DEFAULT_MISSING_VALUE)):.6f}"
                except (TypeError, ValueError):
                    item['lng'] = self.DEFAULT_MISSING_VALUE
                return item

            data = [process_item(item) for item in data]
            # 比较最后一个 item 的 bizDate 与 update_Org_time
            if data:
                last_item = data[-1]
                biz_date = last_item.get('bizDate')
                update_Org_time = last_item.get('update_Org_time')
                try:
                    fmt = "%Y-%m-%d %H:%M:%S"
                    if biz_date and update_Org_time:
                        biz_dt = datetime.strptime(biz_date, fmt)
                        upd_dt = datetime.strptime(update_Org_time, fmt)
                        if biz_dt > upd_dt:
                            data.pop()
                except Exception:
                    pass

            return data
        def process_data_pre(data_history, data, new_pre_fields):
            def processs_forecast_items(item):
                item.update(new_pre_fields)
                # 原始记录中没有的数值型数据记为-999
                if 'speed_of_movement' not in item:
                    item['moveSpeed'] = "-999"
                elif (not item['speed_of_movement']) or (not item['speed_of_movement'].isdigit()):
                    item['moveSpeed'] = "-999"
                    item.pop('speed_of_movement')
                else:
                    item['moveSpeed'] = item.pop('speed_of_movement')
                if 'pressure' not in item:
                    item['centerPressure'] = "-999"
                elif (not item['pressure']) or (not item['pressure'].isdigit()):
                    item['centerPressure'] = "-999"
                    item.pop('pressure')
                else:
                    item['centerPressure'] = item.pop('pressure')
                if 'max_wind_speed' not in item:
                    item['centerSpeed'] = "-999"
                elif (not item['max_wind_speed']) or (not item['max_wind_speed'].isdigit()):
                    item['centerSpeed'] = "-999"
                    item.pop('max_wind_speed')
                else:
                    item['centerSpeed'] = item.pop('max_wind_speed')

                radius_fields = ['radius7', 'radius10', 'radius12', 'r7N', 'r7S', 'r7E', 'r7W', 'r12N', 'r12S', 'r12E',
                                 'r12W',
                                 'r10N', 'r10S', 'r10E', 'r10W', 'r7Ne', 'r10Ne', 'r12Ne',
                                 'r7Se', 'r10Se', 'r12Se', 'r7Sw', 'r10Sw', 'r12Sw',
                                 'r7Nw', 'r10Nw', 'r12Nw']
                for field in radius_fields:
                    item[field] = self.DEFAULT_MISSING_VALUE
                windrad_value = item.get('wind_radii')
                if windrad_value in [None, 'null', 'None', '']:
                    item.pop('wind_radii', None)
                else:
                    parsed = parse_wind_radii_field(windrad_value)
                    for wind_item in parsed:
                        if wind_item['wind_speed'] == '50':
                            for data in wind_item['radii']:
                                direction = data.get('direction')
                                radius = data.get('radius', self.DEFAULT_MISSING_VALUE)
                                if direction is None:
                                    item['radius10'] = radius
                                elif direction == 'NORTH':
                                    item['r10N'] = radius
                                elif direction == 'SOUTH':
                                    item['r10S'] = radius
                                elif direction == 'EAST':
                                    item['r10E'] = radius
                                elif direction == 'WEST':
                                    item['r10W'] = radius
                                elif direction == 'NORTHEAST':
                                    item['r10Ne'] = radius
                                elif direction == 'SOUTHEAST':
                                    item['r10Se'] = radius
                                elif direction == 'SOUTHWEST':
                                    item['r10Sw'] = radius
                                elif direction == 'NORTHWEST':
                                    item['r10Nw'] = radius
                        elif wind_item['wind_speed'] == '30':
                            for data in wind_item['radii']:
                                direction = data.get('direction')
                                radius = data.get('radius', self.DEFAULT_MISSING_VALUE)
                                if direction is None:
                                    item['radius7'] = radius
                                elif direction == 'NORTH':
                                    item['r7N'] = radius
                                elif direction == 'SOUTH':
                                    item['r7S'] = radius
                                elif direction == 'EAST':
                                    item['r7E'] = radius
                                elif direction == 'WEST':
                                    item['r7W'] = radius
                                elif direction == 'NORTHEAST':
                                    item['r7Ne'] = radius
                                elif direction == 'SOUTHEAST':
                                    item['r7Se'] = radius
                                elif direction == 'SOUTHWEST':
                                    item['r7Sw'] = radius
                                elif direction == 'NORTHWEST':
                                    item['r7Nw'] = radius
                        elif wind_item['wind_speed'] == '64':
                            for data in wind_item['radii']:
                                direction = data.get('direction')
                                radius = data.get('radius', self.DEFAULT_MISSING_VALUE)
                                if direction is None:
                                    item['radius12'] = radius
                                elif direction == 'NORTH':
                                    item['r12N'] = radius
                                elif direction == 'SOUTH':
                                    item['r12S'] = radius
                                elif direction == 'EAST':
                                    item['r12E'] = radius
                                elif direction == 'WEST':
                                    item['r12W'] = radius
                                elif direction == 'NORTHEAST':
                                    item['r12Ne'] = radius
                                elif direction == 'SOUTHEAST':
                                    item['r12Se'] = radius
                                elif direction == 'SOUTHWEST':
                                    item['r12Sw'] = radius
                                elif direction == 'NORTHWEST':
                                    item['r12Nw'] = radius
                    item.pop('wind_radii', None)

                if 'forecast_time' in item:
                    item.pop('forecast_time', None)
                intensity_value = item.get('intensity')
                if intensity_value in [None, 'null', 'None', '']:
                    item['intensity'] = None
                else:
                    item['intensity'] = intensity_value

                if 'movement_direction' in item:
                    item['movementDirection'] = translate_direction(item['movement_direction'])
                else:
                    item['moveDirection'] = None
                item.pop('movement_direction')
                if 'gust' in item and item['gust'] is not None:
                    item['gust'] = item.pop('gust', '-999')
                else:
                    item['gust'] = '-999'
                if 'forecast_time' in item:
                    item.pop('forecast_time', None)
                lat_value = float(item['lat'])
                item['lat'] = f"{lat_value:.6f}"
                lng_value = float(item['lng'])
                item['lng'] = f"{lng_value:.6f}"
                item['name'] = storm_name
                return item

            start_time = data_history[-1]['bizDate']
            if start_time is not None:
                start_datetime = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
            else:
                start_datetime = datetime.min
            hour = 0
            processed_data = []
            for i,item in enumerate(data):
                if item['time_interval'].isdigit():
                    hour = int(item['time_interval']) / 100
                    current_time = start_datetime + timedelta(hours=hour)
                    item['bizDate'] = current_time.strftime("%Y-%m-%d %H:%M:%S")
                    item = processs_forecast_items(item)
                    item.pop('time_interval', None)
                    processed_data.append(item)
            return processed_data
        direction_mapping = {
            "N": "北",
            "NNE": "北东北",
            "NE": "东北",
            "ENE": "东东北",
            "E": "东",
            "ESE": "东东南",
            "SE": "东南",
            "SSE": "南东南",
            "S": "南",
            "SSW": "南西南",
            "SW": "西南",
            "WSW": "西西南",
            "W": "西",
            "WNW": "西西北",
            "NW": "西北",
            "NNW": "北西北",
            "VAR": "多变",
            "STNR": "静止",
            "": "",
            "null": ""
        }

        try:
            headers = random.choice(headers_list)
            wmo_response = requests.get(wmo_url, headers=headers, timeout=5)
            wmo_soup = BeautifulSoup(wmo_response.content, 'lxml')
            wmo_data = wmo_response.json()
        except requests.exceptions.Timeout:
            logging.warning(f"请求超时：{wmo_url}")
            return None, None, None, None
        except requests.exceptions.RequestException as e:
            logging.error(f"请求失败：{wmo_url}，错误信息：{e}")
            return None, None, None, None

        get_location = {
            '1':'HongKong',
            '2':'Beijing',
            '3':'TC RSMC Honolulu',
            '4':'TC RSMC Miami',
            '5':'TC RSMC Tokyo',
            '6':'TC RSMC New Delhi',
            '7':'TC RSMC La Reunion',
            '8':'TCWC Melbourne',
            '9':'TCWC Melbourne',
            '10':'TCWC Melbourne',
            '11':'TC RSMC Nadi',
            '12':'TCWC Wellington',
            '13':'JTWC',
            '14':'Manila',
            '15':'Macao',
        }
        data_history = wmo_data['track']
        data_pre = wmo_data['forecast']
        location_name_id = wmo_data['track'][0]['center_id']
        location_name = get_location[location_name_id]

        new_history_fields = {
            "update_Org_time": None,
            "code": str(storm_id),
            "sys_id": str(wmo_data['sys_id']),
            "forecastOrg": location_name,
            "forcastSource":"WMO"
        }
        data_history = process_data_history(data_history, new_history_fields)
        uptime = data_history[-1]['bizDate']
        for item in data_history:
            item['update_Org_time'] = uptime
        new_pre_fields = {
            "update_Org_time": uptime,
            "code": str(storm_id),
            "sys_id":str(wmo_data['sys_id']),
            "forecastOrg": location_name,
            "forcastSource": "WMO",
        }


        data_pre = process_data_pre(data_history, data_pre, new_pre_fields)
        updatetime = uptime.replace(":", "").replace(" ", "_").replace("-", "_")

        return data_history,data_pre,location_name,updatetime

    def save_data(self, sys_id, label, data_history, data_pre, safe_updatetime):
        if not data_history or not data_pre:
            return None, None, None, None
        try:
            history_filename = f"wmo_data/{sys_id}/{label}/{label}_history_{safe_updatetime}.json"
            pre_filename = f"wmo_data/{sys_id}/{label}/{label}_pre_{safe_updatetime}.json"

            return data_history, data_pre, history_filename, pre_filename

        except Exception as e:
            logging.error(f"数据保存失败: {e}")
            return False

    def time_job(self):
        url = f"https://severeweather.wmo.int/json/tc_inforce.json?_={int(time.time() * 1000)}"
        headers = random.choice(headers_list)
        response = requests.get(url, headers=headers, timeout=20)
        soup = BeautifulSoup(response.text, "lxml")
        tc_inforce_data = response.json()
        tropical_datas = []
        for line in tc_inforce_data['inforce']:
            tropical_data = {
                "sysids": [],
                "name": None,
                "start_time": None,
                "last_time": None,
                "id": None,
                "sysid": None,
            }
            tropical_data['sysids'].append(line[0])
            tropical_data['sysid'] = line[0]
            if line[6]:
                items = line[6].split(',')
                for item in items:
                    tropical_data['sysids'].append(item)

            tropical_data['name'] = line[1]
            tropical_data['start_time'] = line[4]
            tropical_data['last_time'] = line[5]
            tropical_data['id'] = line[2]
            tropical_datas.append(tropical_data)
        wmo_url_list = {}
        id_list = {}
        wmo_url_history_list = {}
        sys_id_list = {}
        # 构建URL字典
        for tmp in tropical_datas:
            key = tmp['name']
            urls = [f"https://severeweather.wmo.int/json/tc_{sysid}.json?_={int(time.time() * 1000)}" for sysid in
                    tmp['sysids']]
            wmo_url_list[key] = urls if len(urls) > 1 else {urls[0]}
            id_list[key] = tmp['id']
            wmo_url_history_list[
                key] = f"https://severeweather.wmo.int/json/tc_{tmp['sysid']}.json?_={int(time.time() * 1000)}"
            sys_id_list[key] = tmp['sysid']
        logging.info("=== 开始执行WMO定时任务 ===")
        for storm_name,urls in wmo_url_list.items():
            storm_id = id_list[storm_name]
            history_url = wmo_url_history_list[storm_name]
            sys_id = sys_id_list[storm_name]
            history_filename1, data_history1 = self.get_save_history_data(sys_id, history_url, storm_id ,storm_name)
            self.data_history.append(data_history1)
            self.history_filename.append(history_filename1)
            for url in urls:
                data_history, data_pre, location_name,safe_updatetime = self.get_data(sys_id, url,storm_id,storm_name)
                data_history, data_pre1, history_filename, pre_filename1 = self.save_data(sys_id, location_name, data_history, data_pre, safe_updatetime)
                self.data_pre.append(data_pre1)
                self.pre_filename.append(pre_filename1)
        logging.info("=== WMO定时任务完成 ===")

class JapanDataCollector:
    def __init__(self):
        self.DEFAULT_MISSING_VALUE = "-999"
        self.data_history = []
        self.data_pre = []
        self.history_filename = []
        self.pre_filename = []

    def save_data(self, context,sys_id):
        def nested_get(dic, keys, default=None):
            for key in keys:
                try:
                    if isinstance(dic, dict):
                        dic = dic.get(key, default)
                    elif isinstance(dic, list) and isinstance(key, int) and 0 <= key < len(dic):
                        dic = dic[key]
                    else:
                        return default
                    if dic is None:
                        return default
                except Exception:
                    return default
            return dic

        # 日本方向词（16方位）与中文对照
        direction_mapping = {
            "北": "北",
            "北北東": "北北东",
            "北東": "北东",
            "東北東": "东北东",
            "東": "东",
            "東南東": "东南东",
            "南東": "南东",
            "南南東": "南南东",
            "南": "南",
            "南南西": "南南西",
            "南西": "南西",
            "西南西": "西南西",
            "西": "西",
            "西北西": "西北西",
            "北西": "北西",
            "北北西": "北北西",
        }

        def save_time_series(iso_time_str):
            if not iso_time_str:
                return ''
            try:
                dt = datetime.strptime(iso_time_str, "%Y-%m-%dT%H:%M:%SZ")
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                return iso_time_str

        def translate_jp_ch(japanese_direction):
            if not japanese_direction:
                return ''
            if japanese_direction in direction_mapping:
                return direction_mapping[japanese_direction]
            try:
                return GoogleTranslator(source='ja', target='zh-CN').translate(japanese_direction)
            except Exception:
                return japanese_direction

        def find_r10(datar10):
            r10 = '-999'
            r10N = r10S = r10W = r10E = r10Ne = r10Se = r10Nw = r10Sw = '-999'

            storm_warning = nested_get(datar10, ['stormWarning'], [])
            if storm_warning and isinstance(storm_warning, list):
                r10 = str(nested_get(storm_warning[0], ['range', 'nm'], '-999'))

            datar10 = datar10
            if 'galeWarning' in datar10:
                gale_data = datar10['galeWarning']
                for item in gale_data:
                    area_en = nested_get(item, ['area', 'en'],'')
                    nm_range = str(nested_get(item, ['range', 'nm'], '-999'))

                    if area_en == 'All':
                        if r10 == '-999':
                            r10 = nm_range
                        continue

                    area_cn = translate_jp_ch(nested_get(item, ['area'], ''))

                    if area_cn == '北东':
                        r10Ne = nm_range
                    elif area_cn == '北西':
                        r10Nw = nm_range
                    elif area_cn == '南东':
                        r10Se = nm_range
                    elif area_cn == '南西':
                        r10Sw = nm_range
                    elif area_cn == '东':
                        r10E = nm_range
                    elif area_cn == '南':
                        r10S = nm_range
                    elif area_cn == '西':
                        r10W = nm_range
                    elif area_cn == '北':
                        r10N = nm_range

            return r10, r10N, r10S, r10W, r10E, r10Ne, r10Se, r10Nw, r10Sw

        def find_r7(datar7):
            r7, r7N, r7S, r7W, r7E, r7Ne, r7Se, r7Nw, r7Sw = '-999', '-999', '-999', '-999', '-999', '-999', '-999', '-999', '-999'

            if 'galeWarning' in datar7:
                data = datar7['galeWarning']
                for item in data:
                    area_en = nested_get(item, ['area', 'en'])
                    nm_range = str(nested_get(item, ['range', 'nm'], '-999'))

                    if area_en == 'All':
                        r7 = nm_range
                        continue

                    area_cn = translate_jp_ch(nested_get(item, ['area'], ''))

                    if area_cn == '北东':
                        r7Ne = nm_range
                    elif area_cn == '北西':
                        r7Nw = nm_range
                    elif area_cn == '南东':
                        r7Se = nm_range
                    elif area_cn == '南西':
                        r7Sw = nm_range
                    elif area_cn == '东':
                        r7E = nm_range
                    elif area_cn == '南':
                        r7S = nm_range
                    elif area_cn == '西':
                        r7W = nm_range
                    elif area_cn == '北':
                        r7N = nm_range

            return r7, r7N, r7S, r7W, r7E, r7Ne, r7Se, r7Nw, r7Sw

        def find_speed(data):
            if not isinstance(data, dict):
                return '-999'
            return str(data.get('kt', '-999'))

        def find_centerspeed():
            return str(nested_get(context, [1, 'maximumWind', 'sustained', 'kt'], '-999'))

        def save_history_data(context):
            r7, r7N, r7S, r7W, r7E, r7Ne, r7Se, r7Nw, r7Sw = find_r7(context[1])
            r10, r10N, r10S, r10W, r10E, r10Ne, r10Se, r10Nw, r10Sw = find_r10(context[1])
            lat = str(nested_get(context, [1, 'position', 'deg', 0], '-999'))
            lng = str(nested_get(context, [1, 'position', 'deg', 1], '-999'))
            center_pressure = str(nested_get(context, [1, 'pressure'], '-999'))
            move_speed = find_speed(nested_get(context, [1, 'speed'], {}))
            move_direction_jp = nested_get(context, [1, 'course'], '')
            move_direction = translate_jp_ch(move_direction_jp)
            biz_date = save_time_series(nested_get(context, [1, 'validtime', 'UTC'], ''))
            updatetime = save_time_series(nested_get(context, [0, 'issue', 'UTC'], ''))
            code = nested_get(context, [0, 'typhoonNumber'], '')
            name = nested_get(context, [0, 'name','en'], '')
            gust = nested_get(context, [1, 'maximumWind','gust','kt'], '-999')
            result = [{
                "name":name,
                "lat": lat,
                "lng": lng,
                "radius7": r7,
                "radius10": r10,
                "radius12": '-999',
                "r7Ne": r7Ne,
                "r7N": r7N,
                "r7S": r7S,
                "r7W": r7W,
                "r7E": r7E,
                "r7Se": r7Se,
                "r7Sw": r7Sw,
                "r7Nw": r7Nw,
                "r10Ne": r10Ne,
                "r10N": r10N,
                "r10S": r10S,
                "r10W": r10W,
                "r10E": r10E,
                "r10Se": r10Se,
                "r10Sw": r10Sw,
                "r10Nw": r10Nw,
                "r12Ne": '-999',
                "r12Se": '-999',
                "r12Sw": '-999',
                "r12Nw": '-999',
                "centerSpeed": find_centerspeed(),
                "bizDate": biz_date,
                "centerPressure": center_pressure,
                "moveSpeed": move_speed,
                "gust" : gust,
                "moveDirection": move_direction,
                "update_Org_time": updatetime,
                "code": code,
                "intensity": nested_get(context, [0, 'category', 'en'], None),
                "forecastOrg":"Japan",
                "forcastSource":"JMA"
            }]
            output_dir = f'Japan/{sys_id}'
            os.makedirs(output_dir, exist_ok=True)
            safe_time = updatetime.replace(":", "").replace(" ", "_").replace("-", "_")
            output_filename = os.path.join(output_dir, f"NHC_history_{safe_time}.json").replace('\\','/')
            return output_filename, result

        def save_pre_data(context):
            results = []
            code = nested_get(context, [0, 'typhoonNumber'], '')
            update_time = save_time_series(nested_get(context, [0, 'issue', 'UTC'], ''))
            for data in context[2:]:
                r7, r7N, r7S, r7W, r7E, r7Ne, r7Se, r7Nw, r7Sw = find_r7(data)
                r10, r10N, r10S, r10W, r10E, r10Ne, r10Se, r10Nw, r10Sw = find_r10(data)
                lat = nested_get(data, ['position', 'deg', 0], '-999')
                lng = nested_get(data, ['position', 'deg', 1], '-999')
                biz_date = save_time_series(nested_get(data, ['validtime', 'UTC'], ''))
                move_speed = find_speed(nested_get(data, ['speed'], {}))
                center_pressure = str(nested_get(data, ['pressure'], '-999'))
                move_direction_jp = nested_get(data, ['course'], '')
                direction = translate_jp_ch(move_direction_jp)
                center_speed = str(nested_get(data, ['maximumWind', 'sustained', 'kt'], '-999'))
                probability_circle = str(nested_get(data, ['probabilityCircleRadius', 'nm'], '-999'))
                intensity = nested_get(data, ['category', 'en'], None)
                gust = nested_get(data, ['maximumWind', 'gust', 'kt'], '-999')
                result = {
                    "name":nested_get(context, [0, 'name','en'], ''),
                    "lat": f"{float(lat):.1f}" if lat != '-999' else '-999',
                    "lng": f"{float(lng):.1f}" if lng != '-999' else '-999',
                    "bizDate": biz_date,
                    "update_Org_time": update_time,
                    "code": code,
                    "forecastOrg": "Japan",
                    "forecastSource": "JMA",
                    "moveDirection": direction,
                    "moveSpeed": move_speed,
                    "gust": gust,
                    "centerPressure": center_pressure,
                    "centerSpeed": center_speed,
                    "radius7": r7,
                    "radius10": r10,
                    "r7Ne": r7Ne,
                    "r7N": r7N,
                    "r7S": r7S,
                    "r7W": r7W,
                    "r7E": r7E,
                    "r7Se": r7Se,
                    "r7Sw": r7Sw,
                    "r7Nw": r7Nw,
                    "r10Ne": r10Ne,
                    "r10N": r10N,
                    "r10S": r10S,
                    "r10W": r10W,
                    "r10E": r10E,
                    "r10Se": r10Se,
                    "r10Sw": r10Sw,
                    "r10Nw": r10Nw,
                    "probabilityCircle": probability_circle,
                    "intensity": intensity,
                }
                results.append(result)
            output_dir = f'Japan/{sys_id}'
            os.makedirs(output_dir, exist_ok=True)
            safe_time = update_time.replace(":", "").replace(" ", "_").replace("-", "_")
            output_filename = os.path.join(output_dir, f"NHC_pre_{safe_time}.json").replace('\\','/')
            return output_filename,results

        his_path,his_data = save_history_data(context)
        pre_path,pre_data = save_pre_data(context)
        return pre_path, pre_data, his_path, his_data
    def main(self):
        logging.info(f"Japan任务开始执行: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        try:
            url = 'https://www.jma.go.jp/bosai/typhoon/data/targetTc.json'
            response = requests.get(url)
            soup = BeautifulSoup(response.text, 'lxml')
            context = response.json()
            for item in context:
                typhoon_url = f'https://www.jma.go.jp/bosai/typhoon/data/{item["tropicalCyclone"]}/specifications.json'
                response = requests.get(typhoon_url)
                soup = BeautifulSoup(response.text, 'lxml')
                context = response.json()
                pre_path, pre_data, his_path, his_data = self.save_data(context,item["tropicalCyclone"])
                self.data_history.append(his_data)
                self.data_pre.append(pre_data)
                self.pre_filename.append(pre_path)
                self.history_filename.append(his_path)
        except Exception as e:
            logging.error(f"任务执行出错: {e}")

        logging.info(f"本次任务结束: {time.strftime('%Y-%m-%d %H:%M:%S')}")

class TyphoonDBHandler:
    PROCESSED_PRE_FILES = {}
    TIME_INTERVAL = 2
    
    def __init__(self, db_config=None):
        # 使用统一数据库配置
        if db_config is None:
            try:
                from db_config import get_db_config
                db_config = get_db_config('typhoon')
            except ImportError:
                import os
                db_config = {
                    'host': os.getenv('DB_HOST', 'database-1.cxik82g8267p.us-east-2.rds.amazonaws.com'),
                    'port': int(os.getenv('DB_PORT', '5432')),
                    'database': 'typhoon_data',
                    'user': os.getenv('DB_USER', 'postgres'),
                    'password': os.getenv('DB_PASSWORD', 'oceantest1')
                }
        self.conn_params = db_config
        self.connection = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(**self.conn_params)
            logging.info("成功连接到台风数据库 (typhoon_data)")
        except OperationalError as e:
            logging.error(f"数据库连接失败：{e}")
            raise

    def close(self):
        if self.connection:
            self.connection.close()
            logging.info("数据库连接已关闭")

    # ------------------- 建表 -------------------
    def create_typhoon_tables(self):
        """创建台风信息、历史数据和预测表"""

        # 1. typhoon_info 表
        create_info_table = """
        CREATE TABLE IF NOT EXISTS typhoon_info (
            tc_id VARCHAR(50) PRIMARY KEY,
            name VARCHAR(30) DEFAULT '',
            forecastSource VARCHAR(20) DEFAULT '',
            forecastOrg VARCHAR(20) DEFAULT '',
            intensity VARCHAR(40) DEFAULT NULL,
            code VARCHAR(10) DEFAULT '',
            sys_id VARCHAR(20) DEFAULT '',
            status VARCHAR(2) DEFAULT '1',
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

        # 2. 历史数据表
        create_history_table = """
        CREATE TABLE IF NOT EXISTS typhoon_history (
            id SERIAL PRIMARY KEY,
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            tc_id VARCHAR(50) REFERENCES typhoon_info(tc_id),
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_deleted VARCHAR(2) DEFAULT '0',
            sys_id VARCHAR(20) DEFAULT '',
            name VARCHAR(30) DEFAULT '',
            code VARCHAR(20) DEFAULT '',
            intensity VARCHAR(40) DEFAULT NULL,
            lat NUMERIC(9,6) DEFAULT -999,
            lng NUMERIC(9,6) DEFAULT -999,
            forecastSource VARCHAR(20) DEFAULT '',
            radius7 NUMERIC(9,2) DEFAULT -999,
            radius8 NUMERIC(9,2) DEFAULT -999,
            radius10 NUMERIC(9,2) DEFAULT -999,
            radius12 NUMERIC(9,2) DEFAULT -999,
            r7N NUMERIC(9,2) DEFAULT -999,
            r7S NUMERIC(9,2) DEFAULT -999,
            r7W NUMERIC(9,2) DEFAULT -999,
            r7E NUMERIC(9,2) DEFAULT -999,
            r7Ne NUMERIC(9,2) DEFAULT -999,
            r7Se NUMERIC(9,2) DEFAULT -999,
            r7Sw NUMERIC(9,2) DEFAULT -999,
            r7Nw NUMERIC(9,2) DEFAULT -999,
            r8N NUMERIC(9,2) DEFAULT -999,
            r8S NUMERIC(9,2) DEFAULT -999,
            r8W NUMERIC(9,2) DEFAULT -999,
            r8E NUMERIC(9,2) DEFAULT -999,
            r8Ne NUMERIC(9,2) DEFAULT -999,
            r8Se NUMERIC(9,2) DEFAULT -999,
            r8Sw NUMERIC(9,2) DEFAULT -999,
            r8Nw NUMERIC(9,2) DEFAULT -999,
            r10N NUMERIC(9,2) DEFAULT -999,
            r10S NUMERIC(9,2) DEFAULT -999,
            r10W NUMERIC(9,2) DEFAULT -999,
            r10E NUMERIC(9,2) DEFAULT -999,
            r10Ne NUMERIC(9,2) DEFAULT -999,
            r10Se NUMERIC(9,2) DEFAULT -999,
            r10Sw NUMERIC(9,2) DEFAULT -999,
            r10Nw NUMERIC(9,2) DEFAULT -999,
            r12N NUMERIC(9,2) DEFAULT -999,
            r12S NUMERIC(9,2) DEFAULT -999,
            r12W NUMERIC(9,2) DEFAULT -999,
            r12E NUMERIC(9,2) DEFAULT -999,
            r12Ne NUMERIC(9,2) DEFAULT -999,
            r12Se NUMERIC(9,2) DEFAULT -999,
            r12Sw NUMERIC(9,2) DEFAULT -999,
            r12Nw NUMERIC(9,2) DEFAULT -999,
            centerSpeed NUMERIC(9,2) DEFAULT -999,
            bizDate VARCHAR(20)  DEFAULT NULL,
            centerPressure NUMERIC(9,2) DEFAULT -999,
            moveSpeed NUMERIC(9,2) DEFAULT -999,
            gust NUMERIC(9,2) DEFAULT -999,
            moveDirection VARCHAR(8)  DEFAULT NULL,
            update_Org_time VARCHAR(20)  DEFAULT NULL,
            probabilityCircle NUMERIC(9,2) DEFAULT -999
        );
        """

        # 3. 预测表
        create_forecast_table = """
        CREATE TABLE IF NOT EXISTS typhoon_forecast (
            id SERIAL PRIMARY KEY,
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            tc_id VARCHAR(50) REFERENCES typhoon_info(tc_id),
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_deleted VARCHAR(2) DEFAULT '0',
            code VARCHAR(20) DEFAULT '',
            name VARCHAR(30) DEFAULT '',
            intensity VARCHAR(40) DEFAULT NULL,
            lat NUMERIC(9,6) DEFAULT -999,
            lng NUMERIC(9,6) DEFAULT -999,
            sys_id VARCHAR(10) DEFAULT '',
            forecastSource VARCHAR(10) DEFAULT '',
            forecastOrg VARCHAR(20) DEFAULT '',
            radius7 NUMERIC(9,2) DEFAULT -999,
            radius8 NUMERIC(9,2) DEFAULT -999,
            radius10 NUMERIC(9,2) DEFAULT -999,
            radius12 NUMERIC(9,2) DEFAULT -999,
            r7N NUMERIC(9,2) DEFAULT -999,
            r7S NUMERIC(9,2) DEFAULT -999,
            r7W NUMERIC(9,2) DEFAULT -999,
            r7E NUMERIC(9,2) DEFAULT -999,
            r7Ne NUMERIC(9,2) DEFAULT -999,
            r7Se NUMERIC(9,2) DEFAULT -999,
            r7Sw NUMERIC(9,2) DEFAULT -999,
            r7Nw NUMERIC(9,2) DEFAULT -999,
            r8N NUMERIC(9,2) DEFAULT -999,
            r8S NUMERIC(9,2) DEFAULT -999,
            r8W NUMERIC(9,2) DEFAULT -999,
            r8E NUMERIC(9,2) DEFAULT -999,
            r8Ne NUMERIC(9,2) DEFAULT -999,
            r8Se NUMERIC(9,2) DEFAULT -999,
            r8Sw NUMERIC(9,2) DEFAULT -999,
            r8Nw NUMERIC(9,2) DEFAULT -999,
            r10N NUMERIC(9,2) DEFAULT -999,
            r10S NUMERIC(9,2) DEFAULT -999,
            r10W NUMERIC(9,2) DEFAULT -999,
            r10E NUMERIC(9,2) DEFAULT -999,
            r10Ne NUMERIC(9,2) DEFAULT -999,
            r10Se NUMERIC(9,2) DEFAULT -999,
            r10Sw NUMERIC(9,2) DEFAULT -999,
            r10Nw NUMERIC(9,2) DEFAULT -999,
            r12N NUMERIC(9,2) DEFAULT -999,
            r12S NUMERIC(9,2) DEFAULT -999,
            r12W NUMERIC(9,2) DEFAULT -999,
            r12E NUMERIC(9,2) DEFAULT -999,
            r12Ne NUMERIC(9,2) DEFAULT -999,
            r12Se NUMERIC(9,2) DEFAULT -999,
            r12Sw NUMERIC(9,2) DEFAULT -999,
            r12Nw NUMERIC(9,2) DEFAULT -999,
            centerSpeed NUMERIC(9,2) DEFAULT -999,
            bizDate VARCHAR(20)  DEFAULT NULL,
            centerPressure NUMERIC(9,2) DEFAULT -999,
            moveSpeed NUMERIC(9,2) DEFAULT -999,
            gust NUMERIC(9,2) DEFAULT -999,
            moveDirection VARCHAR(10)  DEFAULT NULL,
            update_Org_time VARCHAR(20)  DEFAULT NULL,
            probabilityCircle NUMERIC(9,2) DEFAULT -999
        );
        """

        with self.connection.cursor() as cursor:
            cursor.execute(create_info_table)
            cursor.execute(create_history_table)
            cursor.execute(create_forecast_table)
            self.connection.commit()
            logging.info("表 typhoon_info, typhoon_history, typhoon_forecast 创建完成（或已存在）")

    def drop_table(self, table_name):
        query = f"DROP TABLE IF EXISTS {table_name};"
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            self.connection.commit()
        logging.info(f"表 {table_name} 已删除")
    # ------------------- 插入数据 -------------------
    def get_max_bizdate_by_source_sysid(self, tc_id):
        query = """
        SELECT MAX(bizDate) FROM typhoon_history 
        WHERE tc_id = %s;
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query, (tc_id,))
            result = cursor.fetchone()[0]
        return result
    def get_max_bizdate_by_source_forecast(self, tc_id):
        query = """
        SELECT MAX(bizDate) FROM typhoon_forecast 
        WHERE tc_id = %s;
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query, (tc_id,))
            result = cursor.fetchone()[0]
        return result
    def generate_tc_id_from_filepath(self, file_path):
        """生成基于文件路径的唯一 tc_id"""
        hash_value = hashlib.sha256(file_path.encode('utf-8')).hexdigest()
        return str(uuid.UUID(hash_value[:32]))  # 使用哈希值的前32个字符作为UUID

    def convert_to_float(self, value, default_value="-999"):
        """将字符串转换为 float，如果无法转换则返回 default_value"""
        try:
            return float(value)
        except (ValueError, TypeError):
            return float(default_value)

    def convert_to_datetime(self, value):
        """将字符串转换为 datetime 对象，如果无法转换则返回 None"""
        try:
            return datetime.fromisoformat(value)
        except (ValueError, TypeError):
            return None

    def convert_to_str(self, value, default_value=""):
        """确保值转换为字符串"""
        return str(value) if value is not None else default_value

    def find_first_duplicate_in_info_recent_days(self, name, days=5):
        try:
            with self.connection.cursor() as cursor:
                name_upper = name.upper()

                query = """
                SELECT tc_code, name
                FROM typhoon_info 
                WHERE UPPER(name) = %s 
                AND create_time >= CURRENT_TIMESTAMP - INTERVAL '%s days'
                ORDER BY create_time DESC
                LIMIT 1;
                """

                cursor.execute(query, (name_upper, days))
                result = cursor.fetchone()

                if result:
                    return result[0]
                else:
                    return None

        except Exception as e:
            logging.error(f"查找最近{days}天重复名称失败: {e}")
            return None

    def get_typhoon_info_count(self):
        try:
            with self.connection.cursor() as cursor:
                query = "SELECT COUNT(*) FROM typhoon_info;"
                cursor.execute(query)
                count = cursor.fetchone()[0]
                return count
        except Exception as e:
            logging.error(f"获取记录数量失败: {e}")
            return random.randint(1, 10000000)

    def find_similar_recent_record(self, lat, lng, days=5, error_range=2):
        """
        检索最近N天存入的info表中对应历史表中的最后一条记录
        检查经纬度是否在误差范围内

        Args:
            lat: 当前记录的纬度
            lng: 当前记录的经度
            days: 检查的天数，默认5天
            error_range: 经纬度误差范围，默认2度

        Returns:
            str: 匹配的tc_code，如果没有则返回None
        """
        try:
            # 确保经纬度是数值类型
            try:
                lat_float = float(lat)
                lng_float = float(lng)
            except (ValueError, TypeError) as e:
                logging.warning(f"经纬度转换失败: lat={lat}, lng={lng}, 错误: {e}")
                return None

            with self.connection.cursor() as cursor:
                query = """
                SELECT ti.tc_code, th.lat, th.lng
                FROM typhoon_info ti
                INNER JOIN typhoon_history th ON ti.tc_id = th.tc_id
                WHERE ti.create_time >= CURRENT_TIMESTAMP - INTERVAL '%s days'
                AND th.lat IS NOT NULL 
                AND th.lng IS NOT NULL
                AND th.lat::numeric >= %s - %s
                AND th.lat::numeric <= %s + %s
                AND th.lng::numeric >= %s - %s
                AND th.lng::numeric <= %s + %s
                ORDER BY th.create_time DESC
                LIMIT 1;
                """

                cursor.execute(query, (
                    days,
                    lat_float, error_range,
                    lat_float, error_range,
                    lng_float, error_range,
                    lng_float, error_range
                ))
                result = cursor.fetchone()

                if result:
                    tc_code = result[0]
                    logging.info(f"找到相似记录: tc_code={tc_code}, 经纬度=({result[1]}, {result[2]})")
                    return tc_code
                else:
                    logging.info(
                        f"未找到相似记录: 经纬度=({lat_float}, {lng_float}), 范围={error_range}度, 天数={days}天")
                    return None

        except Exception as e:
            logging.error(f"查找相似记录失败: {e}")
            return None

    def insert_or_update_typhoon_info(self, file_path, data, batch_time):
        """根据文件路径生成 tc_id，并更新 typhoon_info 表中的数据"""
        tc_id = self.generate_tc_id_from_filepath(file_path)

        # 检查 typhoon_info 表中是否存在该 tc_id
        select_query = """
        SELECT tc_id FROM typhoon_info WHERE tc_id = %s;
        """
        with self.connection.cursor() as cursor:
            cursor.execute(select_query, (tc_id,))
            existing_tc_id = cursor.fetchone()

            if existing_tc_id:
                # 如果该 tc_id 存在，执行 UPDATE
                update_query = """
                UPDATE typhoon_info SET 
                    name = %s, 
                    forecastSource = %s, 
                    forecastOrg = %s,
                    intensity = %s, 
                    code = %s,
                    sys_id = %s,
                    status = %s,
                    update_time = %s
                WHERE tc_id = %s;
                """

                source = data.get("forecastSource") or data.get("forcastSource")
                org = data.get("forecastOrg") or data.get("forcastOrg")

                cursor.execute(update_query, (
                    self.convert_to_str(data.get("name")).upper(),
                    self.convert_to_str(source),
                    self.convert_to_str(org),
                    self.convert_to_str(data.get("intensity")),
                    self.convert_to_str(data.get("code")),
                    self.convert_to_str(data.get("sys_id")),
                    "1",  # assuming status is active (1)
                    batch_time,
                    tc_id
                ))
                logging.info(f"Updated typhoon_info for tc_id: {tc_id}")
            else:

                current_lat = data.get("lat")
                current_lng = data.get("lng")

                if current_lat is not None and current_lng is not None:
                    similar_tc_id = self.find_similar_recent_record(current_lat, current_lng, 3, 2)
                    if similar_tc_id:
                        tc_code = similar_tc_id
                    else:
                        count = self.get_typhoon_info_count()
                        tc_code = self.generate_tc_id_from_filepath(str(count))
                else:
                    count = self.get_typhoon_info_count()
                    tc_code = self.generate_tc_id_from_filepath(str(count))
                # 如果该 tc_id 不存在，执行 INSERT
                insert_query = f"""
                INSERT INTO typhoon_info (
                    tc_id, tc_code, name, forecastSource, forecastOrg, intensity, code, sys_id, status, create_time, update_time
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s);
                """
                source = data.get("forecastSource") or data.get("forcastSource")
                org = data.get("forecastOrg") or data.get("forcastOrg")
                cursor.execute(insert_query, (
                    tc_id,
                    tc_code,
                    self.convert_to_str(data.get("name")).upper(),
                    source,
                    org,
                    self.convert_to_str(data.get("intensity")),
                    self.convert_to_str(data.get("code")),
                    self.convert_to_str(data.get("sys_id")),
                    "1",  # assuming status is active (1),
                    batch_time,
                    batch_time
                ))

                logging.info(f"Inserted new typhoon_info for tc_id: {tc_id}")

        self.connection.commit()
        return tc_id

    def insert_history_data(self, data, batch_time, file_path):
        """根据 forecastSource 和 sys_id 过滤插入比最大 bizDate 更新的数据"""
        default_value = "-999"
        fields = [
            "tc_id","create_time","update_time", "sys_id", "name", "lat", "lng", "radius7", "radius8", "radius10", "radius12",
            "r7N", "r7S", "r7W", "r7E",
            "r7Ne", "r7Se", "r7Sw", "r7Nw",
            "r8N", "r8S", "r8W", "r8E",
            "r8Ne", "r8Se", "r8Sw", "r8Nw",
            "r10N", "r10S", "r10W", "r10E",
            "r10Ne", "r10Se", "r10Sw", "r10Nw",
            "r12N", "r12S", "r12W", "r12E",
            "r12Ne", "r12Se", "r12Sw", "r12Nw",
            "centerSpeed", "bizDate", "centerPressure", "moveSpeed",
            "moveDirection", "update_Org_time", "forecastSource", "gust","code", "intensity", "probabilityCircle"
        ]

        if isinstance(data, dict):
            data = [data]

        from collections import defaultdict
        source_sys_map = defaultdict(list)
        for record in data:
            source = record.get("forecastSource") or record.get("forecastsource") or record.get(
                "forcastsource") or record.get("forcastSource") or ""
            sys_id = record.get('sys_id') or record.get('code', '')
            source_sys_map[(source, sys_id)].append(record)

        insert_query = f"""
        INSERT INTO typhoon_history ({", ".join(fields)})
        VALUES ({", ".join(["%s"] * len(fields))});
        """
        with self.connection.cursor() as cursor:
            total_inserted = 0
            for (source, sys_id), records in source_sys_map.items():
                # Here, insert_or_update_typhoon_info should return the tc_id of the inserted/updated record
                tc_id = self.insert_or_update_typhoon_info(file_path, records[-1], batch_time)
                max_bizdate = self.get_max_bizdate_by_source_sysid(tc_id)
                # Filter records based on max_bizdate
                filtered_records = [r for r in records if
                                    r.get("bizDate") and (max_bizdate is None or r.get("bizDate") > max_bizdate)]

                if not filtered_records:
                    continue
                for record in filtered_records:

                    # 转换字段类型
                    system_id = self.convert_to_str(record.get('sys_id') or record.get('code'))
                    r7 = self.convert_to_float(record.get('radius7') or record.get('radius7(30kt)', default_value))
                    r8 = self.convert_to_float(record.get('radius8') or record.get('radius8(34kt)', default_value))
                    r10 = self.convert_to_float(record.get('radius10') or record.get('radius10(50kt)', default_value))
                    r12 = self.convert_to_float(record.get('radius12') or record.get('radius12(64kt)', default_value))
                    source = record.get("forecastSource") or record.get("forecastsource") or record.get("forcastsource")or record.get("forcastSource")  or ""
                    values = [
                        tc_id,
                        batch_time,
                        batch_time,
                        system_id,
                        self.convert_to_str(record.get("name")),
                        self.convert_to_float(record.get("lat", default_value)),
                        self.convert_to_float(record.get("lng", default_value)),
                        r7, r8, r10, r12,
                        self.convert_to_float(record.get("r7N", default_value)),
                        self.convert_to_float(record.get("r7S", default_value)),
                        self.convert_to_float(record.get("r7W", default_value)),
                        self.convert_to_float(record.get("r7E", default_value)),
                        self.convert_to_float(record.get("r7Ne", default_value)),
                        self.convert_to_float(record.get("r7Se", default_value)),
                        self.convert_to_float(record.get("r7Sw", default_value)),
                        self.convert_to_float(record.get("r7Nw", default_value)),
                        self.convert_to_float(record.get("r8N", default_value)),
                        self.convert_to_float(record.get("r8S", default_value)),
                        self.convert_to_float(record.get("r8W", default_value)),
                        self.convert_to_float(record.get("r8E", default_value)),
                        self.convert_to_float(record.get("r8Ne", default_value)),
                        self.convert_to_float(record.get("r8Se", default_value)),
                        self.convert_to_float(record.get("r8Sw", default_value)),
                        self.convert_to_float(record.get("r8Nw", default_value)),
                        self.convert_to_float(record.get("r10N", default_value)),
                        self.convert_to_float(record.get("r10S", default_value)),
                        self.convert_to_float(record.get("r10W", default_value)),
                        self.convert_to_float(record.get("r10E", default_value)),
                        self.convert_to_float(record.get("r10Ne", default_value)),
                        self.convert_to_float(record.get("r10Se", default_value)),
                        self.convert_to_float(record.get("r10Sw", default_value)),
                        self.convert_to_float(record.get("r10Nw", default_value)),
                        self.convert_to_float(record.get("r12N", default_value)),
                        self.convert_to_float(record.get("r12S", default_value)),
                        self.convert_to_float(record.get("r12W", default_value)),
                        self.convert_to_float(record.get("r12E", default_value)),
                        self.convert_to_float(record.get("r12Ne", default_value)),
                        self.convert_to_float(record.get("r12Se", default_value)),
                        self.convert_to_float(record.get("r12Sw", default_value)),
                        self.convert_to_float(record.get("r12Nw", default_value)),
                        self.convert_to_float(record.get("centerSpeed", default_value)),
                        self.convert_to_datetime(record.get("bizDate")),
                        self.convert_to_float(record.get("centerPressure", default_value)),
                        self.convert_to_float(record.get("moveSpeed", default_value)),
                        self.convert_to_str(record.get("moveDirection")),
                        self.convert_to_datetime(record.get("update_Org_time")),
                        source,
                        self.convert_to_float(record.get("gust", default_value)),
                        self.convert_to_str(record.get("code")),
                        self.convert_to_str(record.get("intensity")),
                        self.convert_to_float(record.get("probabilityCircle"))
                    ]

                    cursor.execute(insert_query, values)
                    total_inserted += 1

            self.connection.commit()
            logging.info(f"共插入: {total_inserted}条数据")

    def update_typhoon_info_status(self):
        """检查typhoon_info表格中status为1的行，更新相应的status为0

        逻辑：
        1. 如果预测表中有该tc_id的数据，使用预测表的最大bizDate
        2. 如果预测表中没有数据，使用历史表的最大bizDate加上5天
        3. 如果时间条件满足，将status设为0，并将相关表的is_deleted设为1
        """
        # 获取当前时间
        current_time = datetime.now(timezone.utc)

        try:
            # 获取所有 status = 1 的记录
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT tc_id FROM typhoon_info WHERE status = '1'")
                active_tc_ids = [row[0] for row in cursor.fetchall()]

                if not active_tc_ids:
                    logging.info("没有status为1的活跃台风记录")
                    return

                updated_count = 0

                for tc_id in active_tc_ids:
                    should_update = False
                    target_time = None

                    # 1. 首先检查预测表中是否有该tc_id的数据
                    max_forecast_bizdate = self.get_max_bizdate_by_source_forecast(tc_id)

                    if max_forecast_bizdate:
                        # 预测表中有数据，使用预测表的逻辑
                        if isinstance(max_forecast_bizdate, str):
                            try:
                                max_forecast_bizdate = datetime.strptime(max_forecast_bizdate, "%Y-%m-%d %H:%M:%S")
                                # 转换为 UTC aware datetime
                                max_forecast_bizdate = max_forecast_bizdate.replace(tzinfo=timezone.utc)
                            except ValueError:
                                logging.warning(f"预测表时间格式错误，tc_id: {tc_id}, bizDate: {max_forecast_bizdate}")
                                continue

                        # 如果最大bizDate小于当前时间，需要更新状态
                        if max_forecast_bizdate < current_time:
                            target_time = max_forecast_bizdate
                            logging.info(f"台风 {tc_id} 预测数据已过期，最大bizDate: {max_forecast_bizdate}")
                            # 更新typhoon_info表的status为0
                            cursor.execute("""
                                            UPDATE typhoon_info
                                            SET status = '0'
                                            WHERE tc_id = %s
                                        """, (tc_id,))

                            # 更新历史表的is_deleted为1
                            cursor.execute("""
                                            UPDATE typhoon_history
                                            SET is_deleted = '1'
                                            WHERE tc_id = %s
                                        """, (tc_id,))

                            # 更新预测表的is_deleted为1
                            cursor.execute("""
                                            UPDATE typhoon_forecast
                                            SET is_deleted = '1'
                                            WHERE tc_id = %s
                                        """, (tc_id,))

                            updated_count += 1
                    else:
                        # 2. 预测表中没有数据，检查历史表
                        max_history_bizdate = self.get_max_bizdate_by_source_history(tc_id)

                        if max_history_bizdate:
                            if isinstance(max_history_bizdate, str):
                                try:
                                    max_history_bizdate = datetime.strptime(max_history_bizdate, "%Y-%m-%d %H:%M:%S")
                                    # 转换为 UTC aware datetime
                                    max_history_bizdate = max_history_bizdate.replace(tzinfo=timezone.utc)
                                except ValueError:
                                    logging.warning(
                                        f"历史表时间格式错误，tc_id: {tc_id}, bizDate: {max_history_bizdate}")
                                    continue

                            # 计算历史表最大bizDate加上可控天数
                            history_plus_5_days = max_history_bizdate + timedelta(days=TyphoonDBHandler.TIME_INTERVAL)

                            # 如果加上5天后小于当前时间，需要更新状态
                            if history_plus_5_days < current_time:
                                target_time = max_history_bizdate
                                logging.info(
                                    f"台风 {tc_id} 历史数据已过期（超过5天），最大bizDate: {max_history_bizdate}, 过期时间: {history_plus_5_days}")
                                # 更新typhoon_info表的status为0
                                cursor.execute("""
                                                UPDATE typhoon_info
                                                SET status = '0'
                                                WHERE tc_id = %s
                                            """, (tc_id,))

                                # 更新历史表的is_deleted为1
                                cursor.execute("""
                                    UPDATE typhoon_history
                                    SET is_deleted = '1',
                                        set_passdays = %s
                                    WHERE tc_id = %s
                                """, (TyphoonDBHandler.TIME_INTERVAL, tc_id,))
                                updated_count += 1
                        else:
                            # 历史表中也没有数据，直接标记为过期
                            logging.info(f"台风 {tc_id} 在预测表和历史表中均无数据，标记为过期")
                            # 更新typhoon_info表的status为0
                            cursor.execute("""
                                            UPDATE typhoon_info
                                            SET status = '0'
                                            WHERE tc_id = %s
                                        """, (tc_id,))
                            updated_count += 1
                        logging.info(f"已更新台风 {tc_id} 状态为过期，目标时间: {target_time}")

                # 提交事务
                self.connection.commit()
                logging.info(f"状态更新完成，共更新了 {updated_count} 个台风的状态")

        except Exception as e:
            logging.error(f"更新typhoon_info表格的status失败: {e}")
            self.connection.rollback()

    # ------------------- 批量处理 ------------
    def get_max_bizdate_by_source_history(self, tc_id):
        """获取历史表中指定tc_id的最大bizDate"""
        query = """
        SELECT MAX(bizDate) FROM typhoon_history 
        WHERE tc_id = %s AND is_deleted = '0';
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, (tc_id,))
                result = cursor.fetchone()[0]
            return result
        except Exception as e:
            logging.error(f"获取历史表最大bizDate失败，tc_id: {tc_id}, 错误: {e}")
            return None

    # ------------------- 批量处理 -------------------
    def clean_inactive_typhoons(self):
        """
        检查 typhoon_info 表中 status = 0 的台风，
        并将其在 typhoon_forecast 和 typhoon_info_history 表中的数据标记为 is_deleted = 1。
        """
        try:
            with self.connection.cursor() as cursor:
                # 1️⃣ 找出所有 status = 0 的 tc_id
                cursor.execute("""
                    SELECT tc_id FROM typhoon_info WHERE status = '0'
                """)
                inactive_tc_ids = [row[0] for row in cursor.fetchall()]

                if not inactive_tc_ids:
                    logging.info("没有 status = 0 的台风，不需要更新。")
                    return

                logging.info(f"检测到 {len(inactive_tc_ids)} 个失效台风：{inactive_tc_ids}")

                # 2️⃣ 更新 typhoon_forecast
                cursor.execute(f"""
                    UPDATE typhoon_forecast
                    SET is_deleted = 1
                    WHERE tc_id IN ({",".join(["%s"] * len(inactive_tc_ids))})
                """, inactive_tc_ids)
                logging.info(f"已将 typhoon_forecast 中 {cursor.rowcount} 条记录标记为 is_deleted = 1。")

                # 3️⃣ 更新 typhoon_info_history
                cursor.execute(f"""
                    UPDATE typhoon_history
                    SET is_deleted = 1
                    WHERE tc_id IN ({",".join(["%s"] * len(inactive_tc_ids))})
                """, inactive_tc_ids)
                logging.info(f"已将 typhoon_history 中 {cursor.rowcount} 条记录标记为 is_deleted = 1。")

                # 4️⃣ 提交事务
                self.connection.commit()
                logging.info("已成功同步删除状态的台风关联数据。")

        except Exception as e:
            logging.error(f"清理失效台风数据时发生错误: {e}")
            self.connection.rollback()

    def batch_insert_JTWC_historydata(self, data, file_path):
        """
        遍历 base_dir 文件夹，递归读取所有 JSON 文件，只处理文件名中包含 "history" 的文件，批量插入数据库。
        特殊逻辑：
        - 若当前路径以 "WMO" 开头，则只处理 onlyhistory.json 文件，不处理其他 *_history_*.json 文件。
        - 其他目录保持原逻辑。
        """
        try:
            # 如果是单条记录，转成列表
            if isinstance(data, dict):
                data = [data]

            # 用 bizDate 去重 + 过滤 update_Org_time < bizDate
            bizdate_map = {}
            for record in data:
                update_org = record.get("update_Org_time")
                biz_date = record.get("bizDate")
                if not update_org or not biz_date:
                    bizdate_map[biz_date] = record
                    continue

                try:
                    update_dt = datetime.fromisoformat(update_org)
                    biz_dt = datetime.fromisoformat(biz_date)
                    if update_dt >= biz_dt:
                        bizdate_map[biz_date] = record
                except Exception:
                    bizdate_map[biz_date] = record

            filtered_data = list(bizdate_map.values())

            # 若 base_dir 包含 "JTWC"，设置默认 forecastsource
            if 'JTWC' in file_path:
                for record in filtered_data:
                    if "forecastsource" not in record and "forecastSource" not in record:
                        record["forecastsource"] = "JTWC"

            batch_time = (datetime.now() - timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
            logging.info(f"正在处理文件: {file_path}，有效记录数: {len(filtered_data)}")
            path_parts = file_path.split("/")

            # 如果是以 "WMO" 开头，返回前两个部分
            if path_parts[0] == "wmo_data":
                file_path = "/".join(path_parts[:2])
            else:
                # 否则返回路径的最后一部分（除去文件名部分）
                file_path = "/".join(path_parts[:-1])
            self.insert_history_data(filtered_data, batch_time,file_path)

        except Exception as e:
            logging.error(f"读取或插入文件 {file_path} 出错: {e}")

    def insert_JTWC_forecast_data(self, data, batch_time, file_path):
        """插入台风数据（所有记录共用相同的当前时间）"""
        tc_id = self.generate_tc_id_from_filepath(file_path)
        default_value = "-999"

        fields = [
            "tc_id","create_time","update_time", "sys_id", "name", "lat", "lng", "radius7", "radius8", "radius10", "radius12",
            "r7N", "r7S", "r7W", "r7E",
            "r7Ne", "r7Se", "r7Sw", "r7Nw",
            "r8N", "r8S", "r8W", "r8E",
            "r8Ne", "r8Se", "r8Sw", "r8Nw",
            "r10N", "r10S", "r10W", "r10E",
            "r10Ne", "r10Se", "r10Sw", "r10Nw",
            "r12N", "r12S", "r12W", "r12E",
            "r12Ne", "r12Se", "r12Sw", "r12Nw",
            "centerSpeed","gust", "bizDate", "centerPressure", "moveSpeed",
            "moveDirection", "update_Org_time", "forecastSource", "forecastOrg", "code", "intensity",
            "probabilityCircle"
        ]

        if isinstance(data, dict):
            data = [data]

        insert_query = f"""
        INSERT INTO typhoon_forecast
        ({", ".join(fields)})
        VALUES ({", ".join(["%s"] * len(fields))});
        """

        with self.connection.cursor() as cursor:
            if data and len(data) > 0:
                forecast_org = data[0].get("forecastOrg")
                if forecast_org:
                    cursor.execute("""
                                UPDATE typhoon_forecast
                                SET is_deleted = 1
                                WHERE tc_id = %s AND forecastOrg = %s
                            """, (tc_id, forecast_org))
                    logging.info(
                        f"Set is_deleted = 1 for existing forecast data of tc_id: {tc_id} and forecastOrg: {forecast_org}")

            for record in data:
                if 'sys_id' in record:
                    system_id = record.get('sys_id')
                else:
                    system_id = record.get("code")
                if 'radius7' in record:
                    r7 = record.get('radius7')
                else:
                    r7 = record.get('radius7(30kt)', default_value)
                if 'radius8' in record:
                    r8 = record.get('radius8')
                else:
                    r8 = record.get('radius8(34kt)', default_value)
                if 'radius10' in record:
                    r10 = record.get('radius10')
                else:
                    r10 = record.get('radius10(50kt)', default_value)
                if 'radius12' in record:
                    r12 = record.get('radius12')
                else:
                    r12 = record.get('radius12(64kt)', default_value)
                if 'forcastsource' in record:
                    forsource = record.get('forcastsource')
                elif 'forecastsource' in record:
                    forsource = record.get('forecastsource')
                elif 'forcastSource' in record:
                    forsource = record.get('forcastSource')
                elif 'forecastSource' in record:
                    forsource = record.get('forecastSource')
                else:
                    forsource = record.get('forecastSource')
                values = [
                    tc_id,
                    batch_time,
                    batch_time,
                    system_id,
                    record.get("name", ""),
                    record.get("lat", default_value),
                    record.get("lng", default_value),
                    r7,
                    r8,
                    r10,
                    r12,
                    record.get("r7N", default_value),
                    record.get("r7S", default_value),
                    record.get("r7W", default_value),
                    record.get("r7E", default_value),
                    record.get("r7Ne", default_value),
                    record.get("r7Se", default_value),
                    record.get("r7Sw", default_value),
                    record.get("r7Nw", default_value),
                    record.get("r8N", default_value),
                    record.get("r8S", default_value),
                    record.get("r8W", default_value),
                    record.get("r8E", default_value),
                    record.get("r8Ne", default_value),
                    record.get("r8Se", default_value),
                    record.get("r8Sw", default_value),
                    record.get("r8Nw", default_value),
                    record.get("r10N", default_value),
                    record.get("r10S", default_value),
                    record.get("r10W", default_value),
                    record.get("r10E", default_value),
                    record.get("r10Ne", default_value),
                    record.get("r10Se", default_value),
                    record.get("r10Sw", default_value),
                    record.get("r10Nw", default_value),
                    record.get("r12N", default_value),
                    record.get("r12S", default_value),
                    record.get("r12W", default_value),
                    record.get("r12E", default_value),
                    record.get("r12Ne", default_value),
                    record.get("r12Se", default_value),
                    record.get("r12Sw", default_value),
                    record.get("r12Nw", default_value),
                    record.get("centerSpeed", default_value),
                    record.get("gust", default_value),
                    record.get("bizDate"),
                    record.get("centerPressure", default_value),
                    record.get("moveSpeed", default_value),
                    record.get("moveDirection"),
                    record.get("update_Org_time"),
                    forsource,
                    record.get("forecastOrg"),
                    record.get("code"),
                    record.get("intensity"),
                    record.get("probabilityCircle", default_value)
                ]
                cursor.execute(insert_query, values)
            self.connection.commit()

    def batch_insert_JTWC_forecastdata(self, data, file_path):
        """
        遍历 base_dir 文件夹，递归读取所有 JSON 文件，只处理文件名中包含 "pre" 的文件。
        对每个目录在内存中记录最近 3 个处理过的文件名；
        如果当前文件名已经在记录中，则跳过处理。
        """
        dir_key = os.path.dirname(file_path)
        if dir_key not in TyphoonDBHandler.PROCESSED_PRE_FILES:
            TyphoonDBHandler.PROCESSED_PRE_FILES[dir_key] = []

        processed_files = TyphoonDBHandler.PROCESSED_PRE_FILES[dir_key]


        if file_path in processed_files:
            logging.info(f"[{dir_key}] 文件 {file_path} 已处理过，跳过。")
            return

        else:
            try:
                if isinstance(data, dict):
                    data = [data]

                batch_time = (datetime.now() - timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
                logging.info(f"[{dir_key}] 正在处理文件: {file_path}，有效记录数: {len(data)}")
                path_parts = file_path.split("/")

                # 如果是以 "WMO" 开头，返回前两个部分
                if path_parts[0] == "wmo_data":
                    file_path1 = "/".join(path_parts[:2])
                else:
                    # 否则返回路径的最后一部分（除去文件名部分）
                    file_path1 = "/".join(path_parts[:-1])
                self.insert_JTWC_forecast_data(data, batch_time, file_path1)

                # 更新内存列表，只保留最近 3 个
                processed_files.append(file_path)
                if len(processed_files) > 3:
                    processed_files.pop(0)
                self.update_typhoon_info_status()
            except Exception as e:
                logging.error(f"[{dir_key}] 读取或插入文件 {file_path} 出错: {e}")

    # ------------------- 工具函数 -------------------
    def export_table_to_csv(self, table_name, file_name):
        query = f"COPY {table_name} TO STDOUT WITH CSV HEADER"
        with self.connection.cursor() as cursor, open(file_name, 'w', encoding='utf-8') as f:
            cursor.copy_expert(query, f)
        logging.info(f"表 {table_name} 导出到 {file_name} 成功")

def job():
    handler = TyphoonDBHandler()
    handler.connect()
    logging.info("数据库连接成功。")
    runners = [NHCDataCollector(), WMODataCollector(), JapanDataCollector()]

    for runner in runners:
        logging.info(f"{runner.__class__.__name__} 开始抓取数据...")
        try:
            # 分别执行各自抓取任务
            if isinstance(runner, NHCDataCollector):
                runner.main_task()
                logging.info(f"{runner.__class__.__name__} 数据抓取完成。")
            elif isinstance(runner, WMODataCollector):
                runner.time_job()
                logging.info(f"{runner.__class__.__name__} 数据抓取完成。")
            else:
                runner.main()
                logging.info(f"{runner.__class__.__name__} 数据抓取完成。")
        except Exception as e:
            logging.error(f"{runner.__class__.__name__} 抓取数据失败: {e}")

        history_files = getattr(runner, "history_filename", [])
        if history_files:  # 列表不为空才执行
            try:
                for i in range(len(history_files)):
                    # 检查文件路径是否为 None
                    if runner.history_filename[i] is None:
                        logging.warning(f"{runner.__class__.__name__} 历史数据文件路径为 None，跳过第 {i} 个文件")
                        continue

                    handler.batch_insert_JTWC_historydata(
                        runner.data_history[i], runner.history_filename[i]
                    )
                    logging.info(f"正在插入历史数据文件: {runner.history_filename[i]}")
            except Exception as e:
                logging.error(f"{runner.__class__.__name__} 历史数据入库失败: {e}")
        else:
            logging.info(f"{runner.__class__.__name__} 没有历史数据可插入，跳过。")

        pre_files = getattr(runner, "pre_filename", [])
        if pre_files:
            try:
                for i in range(len(pre_files)):
                    # 检查文件路径是否为 None
                    if runner.pre_filename[i] is None:
                        logging.warning(f"{runner.__class__.__name__} 预测数据文件路径为 None，跳过第 {i} 个文件")
                        continue
                    handler.batch_insert_JTWC_forecastdata(
                        runner.data_pre[i], runner.pre_filename[i]
                    )
                    logging.info(f"正在插入预测数据文件: {runner.pre_filename[i]}")
            except Exception as e:
                logging.error(f"{runner.__class__.__name__} 预测数据入库失败: {e}")
        else:
            logging.info(f"{runner.__class__.__name__} 没有预测数据可插入，跳过。")

    # handler.export_table_to_csv("typhoon_info", "typhoon_data.csv")
    logging.info("数据抓取完成，等待下次运行...")


schedule.every(2).hours.do(job)
logging.info("调度器已启动，每2小时执行一次任务。")
job()
while True:
    schedule.run_pending()
    time.sleep(60)