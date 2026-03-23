# -*- coding: utf-8 -*-
"""
在本地开发环境下，若遇到 net::ERR_NAME_NOT_RESOLVED 错误，请在 Docker 运行命令中添加 --dns 8.8.8.8 以绕过 DNS 污染。
2026-03-20 Created  - 重构 JTWC 模块，弃用 requests 改用 Selenium 绕过 403 屏蔽
2026-03-20 Updated  - 修复 XML 解析 mismatched tag 问题，引入 BeautifulSoup 预处理
"""
import os
import psycopg2
from psycopg2 import OperationalError
from psycopg2.extras import execute_values
import json
import tempfile
import logging
import time
import hashlib
import uuid
import random
import zipfile
import schedule
import geopandas as gpd
import pandas as pd
import requests
import re
import pytz
import shutil
import io
import base64
import csv
import xml.etree.ElementTree as ET
from lxml import etree,html
import math
from datetime import datetime, timezone, timedelta
from tempfile import TemporaryDirectory
from pathlib import Path
from urllib.parse import urljoin
from shapely.geometry import Point, Polygon
from shapely.ops import transform
from distutils.core import gen_usage
from deep_translator import GoogleTranslator
from psycopg2 import OperationalError
from psycopg2.extras import RealDictCursor, execute_values, Json
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from bs4 import BeautifulSoup
import pyproj
import warnings


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/typhoon_raodong_insert.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
# Healthchecks.io 监控配置（单爬虫级别）
# 为每个爬虫在 https://healthchecks.io 创建独立的监控项，然后填入对应的UUID
HEALTHCHECKS_UUIDS = {
    'WMO': '98f574cc-177f-4b7e-9c8e-276e6b7a5045',
    'raodong': 'b6938adb-137d-4b05-a23a-e4604d9a733c',
    'JTWC': '9577cfd5-22ff-4ab0-8605-7a01f8f3e035'
}
HEALTHCHECKS_BASE_URL = 'https://hc-ping.com'
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


class NHCProcessorDB:
    RECENT_FILES_PATH = "nhc_recent_files.json"  # 文件存储路径

    def __init__(self, output_dir="NHC_raodong", db_params=None):
        self.output_dir = output_dir
        Path(self.output_dir).mkdir(exist_ok=True)

        warnings.filterwarnings(
            "ignore",
            message="GeoDataFrame's CRS is not representable in URN OGC format"
        )

        if db_params is None:
            try:
                from db_config import get_db_config
                db_params = get_db_config('typhoon')
            except ImportError:
                import os
                db_params = {
                    'host': os.getenv('DB_HOST', 'database-1.cxik82g8267p.us-east-2.rds.amazonaws.com'),
                    'port': int(os.getenv('DB_PORT', '5432')),
                    'database': 'typhoon_data',
                    'user': os.getenv('DB_USER', 'postgres'),
                    'password': os.getenv('DB_PASSWORD', '123456')
                }
        self.conn_params = db_params

        # 初始化时从文件加载最近处理的文件记录
        self.recent_files = self._load_recent_files()

    def _load_recent_files(self):
        """从JSON文件加载最近处理的文件记录"""
        try:
            if os.path.exists(self.RECENT_FILES_PATH):
                with open(self.RECENT_FILES_PATH, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                return []
        except (json.JSONDecodeError, FileNotFoundError):
            return []

    def _save_recent_files(self):
        """保存最近处理的文件记录到JSON文件"""
        try:
            with open(self.RECENT_FILES_PATH, 'w', encoding='utf-8') as f:
                json.dump(self.recent_files, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logging.error(f"保存最近处理文件记录失败: {e}")

    def _create_table_if_not_exists(self, year):
        """为指定年份创建数据库表（如果不存在）"""
        table_name = f"raodong_{year}"
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            raodong_id SERIAL PRIMARY KEY,
            id TEXT,
            updateTime TIMESTAMP,
            BASIN TEXT,
            PROB2DAY TEXT,
            PROB7DAY TEXT,
            coordinates JSONB,
            create_time TIMESTAMP,
            update_time TIMESTAMP
        );
        """
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor()
        try:
            cursor.execute(create_sql)
            conn.commit()
            logging.info(f"已创建或确认表 {table_name} 存在")
        except Exception as e:
            logging.error(f"创建表 {table_name} 失败: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()

    def _is_valid_data(self, item):
        """检查数据是否有效，空数据返回False"""
        if not item:
            return False

        # 检查coordinates是否为空
        coordinates = item.get('coordinates', [])
        if not coordinates or (isinstance(coordinates, list) and len(coordinates) == 0):
            return False

        # 检查必需字段是否为空
        required_fields = ['id', 'updateTime']
        for field in required_fields:
            if not item.get(field):
                return False

        return True

    def drop_table(self, year=None):
        """删除表（谨慎操作，会清空所有数据）"""
        if year:
            table_name = f"raodong_{year}"
            confirm_msg = f"你确定要删除表 {table_name} 吗？此操作不可恢复！(yes/no): "
            drop_sql = f"DROP TABLE IF EXISTS {table_name};"
        else:
            confirm_msg = "你确定要删除所有 raodong_年份 表吗？此操作不可恢复！(yes/no): "
            drop_sql = "DROP TABLE IF EXISTS raodong;"

        confirm = input(confirm_msg)
        if confirm.lower() != 'yes':
            logging.info("操作已取消。")
            return

        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor()
        cursor.execute(drop_sql)
        conn.commit()
        cursor.close()
        conn.close()

        if year:
            logging.info(f"表 {table_name} 已被删除。")
        else:
            logging.info("表 raodong 已被删除。")

    def save_json_file_to_db(self, json_file_path):
        """将本地 JSON 文件写入数据库表，按年份分表"""
        if not os.path.exists(json_file_path):
            logging.error(f"文件不存在: {json_file_path}")
            return

        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        if not data or not isinstance(data, list):
            logging.warning("JSON 数据为空或格式不正确")
            return

        # 按年份分组数据
        year_groups = {}
        utc_now = datetime.utcnow()
        utc_now_str = utc_now.strftime('%Y-%m-%d %H:%M:%S')
        valid_count = 0

        for item in data:
            # 检查数据是否有效
            if not self._is_valid_data(item):
                logging.debug(f"跳过无效数据项: {item}")
                continue

            # 设置默认值
            item.setdefault('id', '-999')
            item.setdefault('updateTime', utc_now_str)
            item.setdefault('BASIN', None)
            item.setdefault('PROB2DAY', None)
            item.setdefault('PROB7DAY', None)
            item.setdefault('coordinates', [])

            # 解析年份
            try:
                update_time = datetime.strptime(item['updateTime'], '%Y-%m-%d %H:%M:%S')
                year = update_time.year
            except Exception as e:
                logging.error(f"解析时间失败: {item['updateTime']}, 错误: {e}")
                continue

            # 确保表存在
            self._create_table_if_not_exists(year)

            # 添加到对应的年份组
            if year not in year_groups:
                year_groups[year] = []

            year_groups[year].append((
                item['id'],
                item['updateTime'],
                item['BASIN'],
                item['PROB2DAY'],
                item['PROB7DAY'],
                json.dumps(item['coordinates']),
                utc_now_str,
                utc_now_str
            ))
            valid_count += 1

        if valid_count == 0:
            logging.warning("没有有效数据可以插入")
            return

        # 按年份表插入数据
        total_inserted = 0
        for year, values in year_groups.items():
            table_name = f"raodong_{year}"
            insert_sql = f"""
            INSERT INTO {table_name} (id, updateTime, BASIN, PROB2DAY, PROB7DAY, coordinates, create_time, update_time)
            VALUES %s
            """

            conn = psycopg2.connect(**self.conn_params)
            cursor = conn.cursor()
            try:
                execute_values(cursor, insert_sql, values)
                conn.commit()
                count = len(values)
                logging.info(f"已将 {count} 条记录写入表 {table_name}")
                total_inserted += count
            except Exception as e:
                conn.rollback()
                logging.error(f"写入表 {table_name} 失败: {e}")
            finally:
                cursor.close()
                conn.close()

        logging.info(f"从 {json_file_path} 处理了 {valid_count} 条有效数据，成功插入 {total_inserted} 条记录")

    def save_data(self, json_data):
        """转换 JSON 数据，只返回数据列表"""
        results = []
        time_str = json_data['file_name'].split('_')[-1]

        try:
            dt = datetime.strptime(time_str, '%Y%m%d%H%M')
        except ValueError:
            logging.error(f"时间格式错误: {time_str}")
            return []

        for item in json_data['features']:
            # 提取坐标数据
            coordinates = item.get('geometry', {}).get('coordinates', [])

            # 检查数据是否有效
            if not coordinates or (isinstance(coordinates, list) and len(coordinates) == 0):
                logging.debug(f"跳过无坐标数据的项: {item.get('id', 'unknown')}")
                continue

            result = {
                'id': item.get('id', '-999'),
                'updateTime': dt.strftime('%Y-%m-%d %H:%M:%S'),
                'BASIN': item.get('properties', {}).get('BASIN'),
                'PROB2DAY': item.get('properties', {}).get('PROB2DAY'),
                'PROB7DAY': item.get('properties', {}).get('PROB7DAY'),
                'coordinates': coordinates,
            }
            results.append(result)

        if not results:
            logging.warning("没有有效数据可以保存")
            return []

        return results  # 只返回数据，不保存文件

    def read_geojson(self, file_path):
        """读取 GeoJSON 文件"""
        try:
            gdf = gpd.read_file(file_path)
            json_data = json.loads(gdf.to_json())
            json_data['file_name'] = Path(file_path).stem
            return json_data
        except Exception as e:
            logging.error(f"读取 GeoJSON 失败: {e}")
            return None

    def download_zip(self, url):
        """下载 ZIP 文件到临时目录"""
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            tmp_zip = TemporaryDirectory()
            zip_path = os.path.join(tmp_zip.name, "temp.zip")
            with open(zip_path, 'wb') as f:
                f.write(response.content)
            return zip_path, tmp_zip
        except requests.exceptions.RequestException as e:
            logging.error(f"下载文件时出错: {e}")
            return None, None

    def parse_and_get_geojson(self, zip_file_path):
        """解析 GIS zip 文件，返回第一个符合条件的 GeoJSON"""
        with TemporaryDirectory() as temp_dir:
            try:
                with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)

                for root, _, files in os.walk(temp_dir):
                    shp_files = [f for f in files if f.endswith('.shp') and 'areas' in f]
                    if shp_files:
                        shp_path = os.path.join(root, shp_files[0])
                        gdf = gpd.read_file(shp_path)
                        json_data = json.loads(gdf.to_json())
                        json_data['file_name'] = Path(shp_files[0]).stem
                        return json_data
            except Exception as e:
                logging.error(f"解析 ZIP 文件出错: {e}")
        return None

    def save_to_db(self, data):
        """保存 JSON 数据到 PostgreSQL，按年份分表"""
        if not data:
            logging.warning("没有数据需要保存")
            return

        # 按年份分组数据
        year_groups = {}
        utc_now = datetime.utcnow()
        utc_now_str = utc_now.strftime('%Y-%m-%d %H:%M:%S')
        valid_count = 0

        for item in data:
            # 检查数据是否有效
            if not self._is_valid_data(item):
                logging.debug(f"跳过无效数据项: {item}")
                continue

            # 解析年份
            try:
                update_time = datetime.strptime(item['updateTime'], '%Y-%m-%d %H:%M:%S')
                year = update_time.year
            except Exception as e:
                logging.error(f"解析时间失败: {item['updateTime']}, 错误: {e}")
                continue

            # 确保表存在
            self._create_table_if_not_exists(year)

            # 添加到对应的年份组
            if year not in year_groups:
                year_groups[year] = []

            year_groups[year].append((
                item['id'],
                item['updateTime'],
                item['BASIN'],
                item['PROB2DAY'],
                item['PROB7DAY'],
                json.dumps(item['coordinates']),
                utc_now_str,
                utc_now_str
            ))
            valid_count += 1

        if valid_count == 0:
            logging.warning("没有有效数据可以插入")
            return

        # 按年份表插入数据
        total_inserted = 0
        for year, values in year_groups.items():
            table_name = f"raodong_{year}"
            insert_sql = f"""
            INSERT INTO {table_name} (id, updateTime, BASIN, PROB2DAY, PROB7DAY, coordinates, create_time, update_time)
            VALUES %s
            """

            conn = psycopg2.connect(**self.conn_params)
            cursor = conn.cursor()
            try:
                execute_values(cursor, insert_sql, values)
                conn.commit()
                count = len(values)
                logging.info(f"已将 {count} 条记录写入表 {table_name}")
                total_inserted += count
            except Exception as e:
                conn.rollback()
                logging.error(f"写入表 {table_name} 失败: {e}")
            finally:
                cursor.close()
                conn.close()

        logging.info(f"处理了 {valid_count} 条有效数据，成功插入 {total_inserted} 条记录到数据库")

    def export_to_csv(self, year=None, csv_path=None):
        """将数据库表的数据导出到本地 CSV"""
        if year:
            table_name = f"raodong_{year}"
            csv_path = csv_path or os.path.join(self.output_dir, f"raodong_{year}_export.csv")
        else:
            table_name = "raodong"
            csv_path = csv_path or os.path.join(self.output_dir, "raodong_export.csv")

        Path(self.output_dir).mkdir(exist_ok=True)

        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        try:
            cursor.execute(f"SELECT * FROM {table_name} ORDER BY raodong_id;")
            rows = cursor.fetchall()

            if not rows:
                logging.warning(f"表 {table_name} 中没有数据")
                return csv_path

            # 对 JSONB 字段进行字符串化
            for row in rows:
                if isinstance(row.get("coordinates"), (dict, list)):
                    row["coordinates"] = json.dumps(row["coordinates"])

            columns = rows[0].keys()

            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=columns)
                writer.writeheader()
                writer.writerows(rows)

            logging.info(f"已将表 {table_name} 的 {len(rows)} 条记录导出到 CSV: {csv_path}")
            return csv_path
        except Exception as e:
            logging.error(f"导出表 {table_name} 数据失败: {e}")
            return None
        finally:
            cursor.close()
            conn.close()

    def main_task(self):
        logging.info(f"任务开始执行: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        url = "https://www.nhc.noaa.gov/xgtwo/gtwo_shapefiles.zip"
        zip_path, tmp_dir_obj = self.download_zip(url)

        if zip_path:
            json_data = self.parse_and_get_geojson(zip_path)
            if json_data:
                file_name = json_data['file_name']

                # 检查是否在最近处理列表中
                if file_name in self.recent_files:
                    logging.info(f"文件 {file_name} 已处理过，跳过。")
                else:
                    raodong_data = self.save_data(json_data)
                    if raodong_data:  # 只有有有效数据时才保存
                        self.save_to_db(raodong_data)

                    # 更新最近处理列表
                    self.recent_files.append(file_name)
                    if len(self.recent_files) > 3:
                        self.recent_files.pop(0)

                    # 保存到文件
                    self._save_recent_files()

                    logging.info(f"更新最近处理文件列表: {self.recent_files}")

            if tmp_dir_obj:
                tmp_dir_obj.cleanup()

        logging.info(f"本次任务结束: {time.strftime('%Y-%m-%d %H:%M:%S')}")

class JTWCDataCollector:
    def __init__(self,
                 chrome_path="/usr/bin/chromium",
                 chromedriver_path="/usr/bin/chromedriver",
                 user_data_dir="/tmp/chrome-profile",
                 url="https://mirror.mesovortices.com/jtwc/jtwc.html",
                 headless=True,
                 debug_port=9222):
        self.CHROME_PATH = chrome_path
        self.CHROMEDRIVER_PATH = chromedriver_path
        self.DEBUG_PORT = debug_port
        self.DEBUG_ADDR = f"127.0.0.1:{self.DEBUG_PORT}"
        self.USER_DATA_DIR = user_data_dir
        self.URL = url
        self.headless = headless
        self.his_data = []
        self.his_time = []
        self.pre_data = []
        self.pre_time = []

    def setup_chrome_options(self):
        """设置Chrome选项以更好地通过人机验证"""
        options = Options()
        options.binary_location = self.CHROME_PATH

        # 基础设置
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument(f'--user-data-dir={self.USER_DATA_DIR}')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-plugins')
        options.add_argument('--disable-default-apps')

        # 窗口和显示设置
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--start-maximized')

        # 网络和性能优化
        options.add_argument('--disable-background-timer-throttling')
        options.add_argument('--disable-backgrounding-occluded-windows')
        options.add_argument('--disable-renderer-backgrounding')
        options.add_argument('--disable-features=VizDisplayCompositor')

        # 音频/视频相关
        options.add_argument('--mute-audio')
        options.add_argument('--disable-component-update')

        # 反检测核心设置
        options.add_experimental_option("excludeSwitches", [
            "enable-automation",
            "enable-logging",
            "disable-background-timer-throttling"
        ])
        options.add_experimental_option('useAutomationExtension', False)

        # 隐藏自动化特征
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-features=IsolateOrigins,site-per-process')

        # 语言和区域设置
        options.add_argument('--lang=zh-CN')
        options.add_argument('--accept-lang=zh-CN,zh;q=0.9,en;q=0.8')

        # 用户代理设置 - 使用真实的User Agent
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        options.add_argument(f'--user-agent={user_agent}')

        # Headless模式特殊设置
        if self.headless:
            options.add_argument('--headless=new')  # 使用新的headless模式
            options.add_argument('--disable-gpu')
            options.add_argument('--no-zygote')
            options.add_argument('--single-process')
            # Headless模式下需要模拟更多真实浏览器行为
            options.add_argument('--remote-debugging-port=0')
            options.add_argument('--hide-scrollbars')
            options.add_argument('--disable-background-networking')
        else:
            # 非headless模式可以启用GPU加速
            options.add_argument('--disable-gpu-sandbox')
            options.add_argument('--enable-gpu-rasterization')

        # 额外的反检测设置
        options.set_capability("goog:loggingPrefs", {'performance': 'ALL'})

        return options

    def verify_resolution(self, driver):
        """验证分辨率设置是否正确"""
        try:
            # 获取窗口大小
            window_size = driver.get_window_size()
            logging.info(f"当前窗口大小: {window_size}")

            # 获取GjRM0位置信息
            gjrm0 = driver.find_element(By.XPATH, "//*[@id='GjRM0']")
            location = gjrm0.location
            size = gjrm0.size

            logging.info(f"GjRM0位置: {location}")
            logging.info(f"GjRM0大小: {size}")

            # 计算相对位置百分比
            rel_x = (location['x'] / window_size['width']) * 100
            rel_y = (location['y'] / window_size['height']) * 100

            logging.info(f"GjRM0相对位置: 水平{rel_x:.1f}%, 垂直{rel_y:.1f}%")

            # 截图验证
            driver.save_screenshot("log_typhoon/resolution_verification.png")
            logging.info("分辨率验证截图已保存")

            return True
        except Exception as e:
            logging.error(f"验证分辨率失败: {e}")
            return False

    def click_using_known_coordinates(self, driver):
        """基于已知的有效坐标进行点击"""
        try:
            gjrm0 = driver.find_element(By.XPATH, "//*[@id='GjRM0']")
            location = gjrm0.location
            size = gjrm0.size

            logging.info(f"GjRM0位置: {location}, 大小: {size}")

            # 计算点击位置
            docker_x = location['x'] + size['width'] * 0.03
            docker_y = location['y'] + size['height'] * 0.43

            # 使用ActionChains移动并点击
            actions = ActionChains(driver)

            # 移动到指定坐标（模仿pyautogui.moveTo）
            actions.move_by_offset(docker_x, docker_y)

            # 点击（模仿pyautogui.click）
            actions.click()

            # 执行动作
            actions.perform()
            logging.info("ActionChains点击成功")
            return True

        except Exception as e:
            logging.error(f"ActionChains点击失败: {e}")
            return False

    def mark_click_position(self, driver, x, y):
        """标记点击位置"""
        mark_script = """
        var marker = document.createElement('div');
        marker.style.position = 'fixed';
        marker.style.left = arguments[0] + 'px';
        marker.style.top = arguments[1] + 'px';
        marker.style.width = '10px';
        marker.style.height = '10px';
        marker.style.backgroundColor = 'red';
        marker.style.borderRadius = '50%';
        marker.style.zIndex = '999999';
        document.body.appendChild(marker);
        """
        driver.execute_script(mark_script, x, y)
        time.sleep(1)
        driver.save_screenshot("log_typhoon/click_position.png")
        logging.info("点击位置截图已保存")
        driver.execute_script("document.body.removeChild(document.body.lastChild)")
    # 移除所有PyAutoGUI相关的代码
    def fallback_pyautogui_click(self, driver):
        """在Docker中不使用PyAutoGUI"""
        logging.warning("PyAutoGUI在Docker中不可用，跳过备用点击方案")
        return False

    def debug_page_structure(self, driver):
        """调试页面结构"""
        try:
            logging.info("=== 页面结构调试 ===")

            # 获取完整页面源码
            page_source = driver.page_source
            logging.info(f"页面源码长度: {len(page_source)}")

            # 保存页面源码
            with open("log_typhoon/challenge_page.html", "w", encoding="utf-8") as f:
                f.write(page_source)
            logging.info("页面源码已保存到 log_typhoon/challenge_page.html")

            # 截图
            driver.save_screenshot("log_typhoon/challenge_page.png")
            logging.info("页面截图已保存到 log_typhoon/challenge_page.png")

            # 检查所有元素
            elements_info = [
                ("GjRM0 div", "//div[@id='GjRM0']"),
                ("所有input", "//input"),
                ("所有checkbox", "//input[@type='checkbox']"),
                ("所有label", "//label"),
                ("包含'确认'文本的元素", "//*[contains(text(), '确认')]")
            ]

            for desc, selector in elements_info:
                try:
                    elements = driver.find_elements(By.XPATH, selector)
                    logging.info(f"{desc}: 找到 {len(elements)} 个")
                    for i, elem in enumerate(elements[:3]):  # 只显示前3个
                        try:
                            displayed = elem.is_displayed()
                            enabled = elem.is_enabled()
                            tag = elem.tag_name
                            logging.info(f"  {desc}[{i}]: {tag}, displayed={displayed}, enabled={enabled}")
                        except:
                            pass
                except Exception as e:
                    logging.info(f"{desc}: 查找失败 - {e}")

        except Exception as e:
            logging.error(f"调试页面结构时出错: {e}")

    def debug_real_structure(self, driver):
        """调试真实的页面结构"""
        try:
            logging.info("=== 真实结构调试 ===")

            # 检查GjRM0内部的所有元素
            gjrm0 = driver.find_element(By.ID, "GjRM0")
            all_elements = gjrm0.find_elements(By.XPATH, ".//*")
            logging.info(f"GjRM0内部元素总数: {len(all_elements)}")

            for i, elem in enumerate(all_elements):
                try:
                    tag = elem.tag_name
                    classes = elem.get_attribute("class")
                    text = elem.text.strip()
                    displayed = elem.is_displayed()

                    if text or classes:
                        logging.info(f"元素[{i}]: <{tag} class='{classes}'>{text}</{tag}>, displayed={displayed}")
                except:
                    pass

            # 特别检查所有可点击元素
            clickable_selectors = [
                ".//*[@onclick]",
                ".//*[@role='button']",
                ".//*[@role='checkbox']",
                ".//label",
                ".//span",
                ".//div"
            ]

            for selector in clickable_selectors:
                try:
                    elements = gjrm0.find_elements(By.XPATH, selector)
                    for elem in elements:
                        if elem.is_displayed() and elem.is_enabled():
                            tag = elem.tag_name
                            text = elem.text.strip()
                            logging.info(f"可点击元素: <{tag}>{text}</{tag}>")
                except:
                    pass

        except Exception as e:
            logging.error(f"调试真实结构时出错: {e}")

    def extract_sys_id(self, filename: str) -> str:
        """
        从文件名中提取系统ID（数字部分）。
        """
        match = re.search(r'(\d+)', os.path.basename(filename))
        sys_id = match.group(1) if match else None
        logging.debug(f"Extracted sys_id '{sys_id}' from filename '{filename}'")
        return sys_id

    def save_history_data(self, txt_data, json_data, sys_id):
        def is_valid_format(s):
            pattern = r"^\d{8}Z$"
            return bool(re.match(pattern, s))

        def timeformat(time_str):
            year = 2000 + int(time_str[0:2])
            month = int(time_str[2:4])
            day = int(time_str[4:6])
            hour = int(time_str[6:8])
            dt = datetime(year, month, day, hour, 0, 0)
            formatted_time = dt.strftime("%Y-%m-%d %H:%M:%S")
            return formatted_time

        def find_name_code_instensity(text):
            match_line = re.search(r"^1\.\s*(.+)$", text, re.MULTILINE)
            if match_line:
                line = match_line.group(1)
                match = re.search(
                    r"(POST-TROPICAL CYCLONE|EXTRATROPICAL CYCLONE|POTENTIAL TROPICAL CYCLONE|SUBTROPICAL STORM|SUBTROPICAL DEPRESSION|REMNANT LOW|DISTURBANCE|LOW PRESSURE AREA|INVEST|SUPER TYPHOON|TYPHOON|TROPICAL CYCLONE|TROPICAL STORM|TROPICAL DEPRESSION)\s+(\S+)\s+\(([^)]+)\)",
                    line
                )
                if match:
                    storm_type = match.group(1)
                    storm_number = match.group(2)
                    storm_name = match.group(3)

                    return storm_type, storm_number, storm_name
            return None, None, None

        def find_updatetime(text):
            now = datetime.now()
            current_year = now.year
            current_month = now.month
            match = re.search(r"\b(\d{6})\b", text)
            if not match:
                return None

            code = match.group(1)
            day = int(code[:2])
            hour = int(code[2:4])
            minute = int(code[4:6])

            try:
                dt = datetime(current_year, current_month, day, hour, minute)
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None

        def find_tcfa_biadate(text):
            now = datetime.now()
            current_year = now.year
            current_month = now.month
            match = re.search(r'IMAGERY AT (\d{6})Z', text)
            if not match:
                return None

            code = match.group(1)
            day = int(code[:2])
            hour = int(code[2:4])
            minute = int(code[4:6])

            try:
                dt = datetime(current_year, current_month, day, hour, minute)
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None

        def find_bizdate(text):
            match = re.search(r'\b(\d{6})Z\b', text)
            if not match:
                return None

            time_code = match.group(1)  # '091800'
            day = int(time_code[:2])
            hour = int(time_code[2:4])
            minute = int(time_code[4:6])

            now = datetime.utcnow()
            current_year = now.year
            current_month = now.month

            try:
                dt = datetime(current_year, current_month, day, hour, minute)
            except ValueError:
                if current_month == 1:
                    current_year -= 1
                    current_month = 12
                else:
                    current_month -= 1
                dt = datetime(current_year, current_month, day, hour, minute)

            return dt.strftime('%Y-%m-%d %H:%M:%S')

        def find_movespeed(text):
            movement_match = re.search(r'MOVEMENT PAST SIX HOURS\s*-\s*(\d{3}) DEGREES AT (\d{1,3}) KTS', text)
            if movement_match:
                direction = movement_match.group(1)
                speed = movement_match.group(2)
                directions = [
                    "北", "北东北", "北东", "北东东",
                    "东", "南东东", "南东", "南东南",
                    "南", "南西南", "南西", "南西西",
                    "西", "北西西", "北西", "北西北"
                ]
                index = int(((int(direction) + 11.25) % 360) // 22.5)
                return directions[index], str(int(speed))
            return None, '-999'

        def find_maxwind(text):
            wind_match = re.search(r'MAX SUSTAINED WINDS\s*-\s*(\d{3}) KT, GUSTS (\d{3}) KT', text)
            if wind_match:
                sustained = wind_match.group(1)
                gusts = wind_match.group(2)
                return str(int(sustained)), str(int(gusts))
            return '-999', '-999'

        def find_radius(text):
            def com_rad(ne, se, sw, nw):
                try:
                    total_area = (math.pi / 4) * (int(ne) ** 2 + int(se) ** 2 + int(sw) ** 2 + int(nw) ** 2)
                    equivalent_radius_area = math.sqrt(total_area / math.pi)
                    return f"{equivalent_radius_area:.2f}"
                except:
                    return '-999'

            wind_radii = re.findall(r'RADIUS OF (\d{3}) KT WINDS', text)
            radius8, radius10, radius12, r8Ne, r8Se, r8Sw, r8Nw, r10Ne, r10Se, r10Sw, r10Nw, r12Ne, r12Se, r12Sw, r12Nw = "-999", "-999", "-999", "-999", "-999", "-999", "-999", "-999", "-999", "-999", "-999", "-999", "-999", "-999", "-999"

            radius_matches = re.findall(r'(\d{3}) NM (NORTHEAST|SOUTHEAST|SOUTHWEST|NORTHWEST) QUADRANT', text)
            i = 0
            flag12 = flag10 = flag8 = 0
            for radius, quadrant in radius_matches:
                wind_radius = wind_radii[i // 4]
                i += 1
                if int(wind_radius) == 64:
                    flag12 += 1
                    if quadrant == 'NORTHEAST':
                        r12Ne = str(int(radius))
                    elif quadrant == 'SOUTHEAST':
                        r12Se = str(int(radius))
                    elif quadrant == 'SOUTHWEST':
                        r12Sw = str(int(radius))
                    elif quadrant == 'NORTHWEST':
                        r12Nw = str(int(radius))
                    if flag12 == 4:
                        radius12 = com_rad(r12Ne, r12Se, r12Sw, r12Nw)
                elif int(wind_radius) == 50:
                    flag10 += 1
                    if quadrant == 'NORTHEAST':
                        r10Ne = str(int(radius))
                    elif quadrant == 'SOUTHEAST':
                        r10Se = str(int(radius))
                    elif quadrant == 'SOUTHWEST':
                        r10Sw = str(int(radius))
                    elif quadrant == 'NORTHWEST':
                        r10Nw = str(int(radius))
                    if flag10 == 4:
                        radius10 = com_rad(r10Ne, r10Se, r10Sw, r10Nw)
                elif int(wind_radius) == 34:
                    flag8 += 1
                    if quadrant == 'NORTHEAST':
                        r8Ne = str(int(radius))
                    elif quadrant == 'SOUTHEAST':
                        r8Se = str(int(radius))
                    elif quadrant == 'SOUTHWEST':
                        r8Sw = str(int(radius))
                    elif quadrant == 'NORTHWEST':
                        r8Nw = str(int(radius))
                    if flag8 == 4:
                        radius8 = com_rad(r8Ne, r8Se, r8Sw, r8Nw)
            return radius8, radius10, radius12, r8Ne, r8Se, r8Sw, r8Nw, r10Ne, r10Se, r10Sw, r10Nw, r12Ne, r12Se, r12Sw, r12Nw

        def find_pressure(text):
            cleaned_text = text.replace('\n', '')
            match = re.search(r"MINIMUM CENTRAL PRESSURE AT \d+Z IS (\d+)\s*MB", cleaned_text, re.IGNORECASE)
            if match:
                return str(int(match.group(1)))
            else:
                return '-999'

        def find_radius_ord(data):
            r34_ord = []
            flag34 = 0
            r50_ord = []
            flag50 = 0
            r64_ord = []
            flag64 = 0
            for text in data:
                if text['name'] == 'RADIUS OF 34 KT WINDS' and flag34 == 0:
                    flag34 = 1
                    cord_list = text['coordinates'].split('0 ')
                    for cord in cord_list:
                        cord_item = [0, 0]
                        cord_item[0] = cord.split(',')[0]
                        cord_item[1] = cord.split(',')[1]
                        r34_ord.append(cord_item)
                elif text['name'] == 'RADIUS OF 50 KT WINDS' and flag50 == 0:
                    flag50 = 1
                    cord_list = text['coordinates'].split('0 ')
                    for cord in cord_list:
                        cord_item = [0, 0]
                        cord_item[0] = cord.split(',')[0]
                        cord_item[1] = cord.split(',')[1]
                        r50_ord.append(cord_item)
                elif text['name'] == 'RADIUS OF 64 KT WINDS' and flag64 == 0:
                    flag64 = 1
                    cord_list = text['coordinates'].split('0 ')
                    for cord in cord_list:
                        cord_item = [0, 0]
                        cord_item[0] = cord.split(',')[0]
                        cord_item[1] = cord.split(',')[1]
                        r64_ord.append(cord_item)
            return r34_ord, r50_ord, r64_ord

        def find_danger_ord(data):
            danger_ord = []
            flag_danger = 0

            for text in data:
                if text['name'] == '34 knot Danger Swath' and flag_danger == 0:
                    flag_danger = 1
                    cord_list = text['coordinates'].split('0 ')
                    for cord in cord_list:
                        cord_item = [0, 0]
                        cord_item[0] = cord.split(',')[0]
                        cord_item[1] = cord.split(',')[1]
                        danger_ord.append(cord_item)

            return danger_ord

        def find_tcfa(text):
            lat_match = re.search(r'NEAR (\d+(?:\.\d+)?)(N|S) (\d+(?:\.\d+)?)(E|W)', text)
            if lat_match:
                lat = float(lat_match.group(1))
                lat_dir = lat_match.group(2)
                lon = float(lat_match.group(3))
                lon_dir = lat_match.group(4)
                if lat_dir == 'S':
                    lat = -lat
                if lon_dir == 'W':
                    lon = -lon
            else:
                lat = -999
                lon = -999
            lon = str(lon)
            lat = str(lat)

            wind_match = re.search(r'(\d{2})\s*KNOTS', text)

            dir_match = re.search(r'MOVING ([A-Z\-]+)WARD', text)
            speed_match = re.search(r'AT (\d{2})\s*KNOTS', text)
            pressure_match = re.search(r'NEAR (\d{4}) MB', text)
            max_wind = wind_match.group(1) if wind_match else '-999'
            bizdate = find_tcfa_biadate(text)
            DIRECTION_MAP = {
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
                "WEST-NORTHWEST": "西北西",
                "NORTHWEST": "西北",
                "NORTH-NORTHWEST": "北西北"
            }

            direc = dir_match.group(1).upper().strip() if dir_match else None
            if direc in DIRECTION_MAP:
                direc = DIRECTION_MAP[direc]

            move_speed = speed_match.group(1) if speed_match else None
            pressure = pressure_match.group(1) if pressure_match else None
            return lat, lon, max_wind, bizdate, direc, move_speed, pressure

        def extract_position_from_txt(txt_data):

            try:
                # 使用正则表达式匹配经纬度模式
                import re

                # 匹配模式：数字+小数点+数字+N/S 和 数字+小数点+数字+E/W
                pattern = r'NEAR\s+([\d.]+)([NS])\s+([\d.]+)([EW])'
                match = re.search(pattern, txt_data)

                if match:
                    lat_value = float(match.group(1))
                    lat_dir = match.group(2)
                    lng_value = float(match.group(3))
                    lng_dir = match.group(4)

                    # 转换经纬度格式
                    lat = lat_value if lat_dir == 'N' else -lat_value
                    lng = lng_value if lng_dir == 'E' else -lng_value

                    return str(lat), str(lng)
                else:
                    # 如果正则匹配失败，尝试其他模式或返回默认值
                    logging.warning("无法从txt数据中提取经纬度，使用默认值")
                    return "0.0", "0.0"

            except Exception as e:
                logging.exception(f"从txt数据提取经纬度时出错: {e}")
                return "0.0", "0.0"

        txt_split = txt_data.split('---\n')
        history_data = []
        storm_type, storm_number, storm_name = find_name_code_instensity(txt_split[0])
        updatetime = find_updatetime(txt_split[0])
        for json_item in json_data:
            if is_valid_format(json_item['name']):
                history_item = {
                    "name": "",
                    "lat": str(float(json_item['coordinates'].split(',')[1])),
                    "lng": str(float(json_item['coordinates'].split(',')[0])),
                    "radius8": "-999",
                    "radius10": "-999",
                    "radius12": "-999",
                    "r8Ne": "-999",
                    "r8Se": "-999",
                    "r8Sw": "-999",
                    "r8Nw": "-999",
                    "r10Ne": '-999',
                    "r10Se": '-999',
                    "r10Sw": '-999',
                    "r10Nw": '-999',
                    "r12Ne": '-999',
                    "r12Se": '-999',
                    "r12Sw": '-999',
                    "r12Nw": '-999',
                    "centerSpeed": "-999",
                    "gust": "-999",
                    "bizDate": timeformat(json_item['name']),
                    "centerPressure": "-999",
                    "moveSpeed": "-999",
                    "moveDirection": None,
                    "update_Org_time": updatetime,
                    "code": storm_number,
                    "intensity": None,
                    "danger_ord" : None
                }
                history_data.append(history_item)
        if len(txt_split) > 1:
            if json_data and len(json_data) > 0:
                # 如果 json_data 不是空列表，按照原来的方式获取经纬度
                lat = str(float(json_data[0]['coordinates'].split(',')[1]))
                lng = str(float(json_data[0]['coordinates'].split(',')[0]))
            else:
                lat, lng = extract_position_from_txt(txt_split[1])
            radius8, radius10, radius12, r8Ne, r8Se, r8Sw, r8Nw, r10Ne, r10Se, r10Sw, r10Nw, r12Ne, r12Se, r12Sw, r12Nw = find_radius(
                txt_split[1])

            history_item_latest = {
                "name": storm_name,
                "lat": lat,
                "lng": lng,
                "radius8": radius8,
                "radius10": radius10,
                "radius12": radius12,
                "r8Ne": r8Ne,
                "r8Se": r8Se,
                "r8Sw": r8Sw,
                "r8Nw": r8Nw,
                "r10Ne": r10Ne,
                "r10Se": r10Se,
                "r10Sw": r10Sw,
                "r10Nw": r10Nw,
                "r12Ne": r12Ne,
                "r12Se": r12Se,
                "r12Sw": r12Sw,
                "r12Nw": r12Nw,
                "centerSpeed": find_maxwind(txt_split[1])[0],
                "gust": find_maxwind(txt_split[1])[1],
                "bizDate": find_bizdate(txt_split[1]),
                "centerPressure": find_pressure(txt_split[-1]),
                "moveSpeed": find_movespeed(txt_split[1])[1],
                "moveDirection": find_movespeed(txt_split[1])[0],
                "update_Org_time": updatetime,
                "forecastOrg": "United States Navy",
                "forcastSource": "JTWC",
                "code": storm_number,
                "intensity": storm_type,
                "danger_ord" : find_danger_ord(json_data)
            }
        else:
            lat, lng, max_wind, bizdate, direc, move_speed, pressure = find_tcfa(txt_data)
            history_item_latest = {
                "name": storm_name,
                "lat": lat,
                "lng": lng,
                "radius8": '-999',
                "radius10": "-999",
                "radius12": "-999",
                "r8Ne": "-999",
                "r8Se": "-999",
                "r8Sw": "-999",
                "r8Nw": "-999",
                "r10Ne": "-999",
                "r10Se": "-999",
                "r10Sw": "-999",
                "r10Nw": "-999",
                "r12Ne": "-999",
                "r12Se": "-999",
                "r12Sw": "-999",
                "r12Nw": "-999",
                "centerSpeed": max_wind,
                "gust": "-999",
                "bizDate": bizdate,
                "centerPressure": pressure,
                "moveSpeed": move_speed,
                "moveDirection": direc,
                "update_Org_time": updatetime,
                "forecastOrg": "United States Navy",
                "forcastSource": "JTWC",
                "code": storm_number,
                "intensity": storm_type,
                "danger_ord" : []
            }
        history_data.append(history_item_latest)

        safe_updatetime = updatetime.replace(":", "").replace(" ", "_").replace("-", "_")
        safe_name = f"JTWC_data/{sys_id}/history_{safe_updatetime}.json"
        return history_data, safe_name

    def save_pre_data(self, txt_data, sys_id):
        def find_updatetime(text):
            now = datetime.now()
            current_year = now.year
            current_month = now.month
            match = re.search(r"\b(\d{6})\b", text)
            if not match:
                return None
            code = match.group(1)
            day = int(code[:2])
            hour = int(code[2:4])
            minute = int(code[4:6])

            try:
                dt = datetime(current_year, current_month, day, hour, minute)
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None

        def find_time(time_data):
            time_match = re.search(r'(\d{6})Z', time_data)
            time = find_updatetime(time_match.group(1)) if time_match else None
            return time

        def find_name_code_instensity(text):
            match_line = re.search(r"^1\.\s*(.+)$", text, re.MULTILINE)
            if match_line:
                line = match_line.group(1)
                match = re.search(
                    r"(POST-TROPICAL CYCLONE|EXTRATROPICAL CYCLONE|POTENTIAL TROPICAL CYCLONE|SUBTROPICAL STORM|SUBTROPICAL DEPRESSION|REMNANT LOW|DISTURBANCE|LOW PRESSURE AREA|INVEST|SUPER TYPHOON|TYPHOON|TROPICAL CYCLONE|TROPICAL STORM|TROPICAL DEPRESSION)\s+(\S+)\s+\(([^)]+)\)",
                    line
                )
                if match:
                    storm_type = match.group(1)
                    storm_number = match.group(2)
                    storm_name = match.group(3)

                    return storm_type, storm_number, storm_name
            return None, None, None

        def com_rad(ne, se, sw, nw):
            try:
                total_area = (math.pi / 4) * (int(ne) ** 2 + int(se) ** 2 + int(sw) ** 2 + int(nw) ** 2)
                equivalent_radius_area = math.sqrt(total_area / math.pi)
                return f"{equivalent_radius_area:.2f}"
            except:
                return '-999'

        def find_lat_lon(forecast_text):
            lat_match = re.search(r'(\d+\.\d+)(N|S)', forecast_text)
            if lat_match:
                lat_value = lat_match.group(1)
                lat_dir = lat_match.group(2)
            else:
                lat_value = '-999'
                lat_dir = ''
            if lat_dir == 'S':
                lat_value = str((float(lat_value)) * (-1))

            lon_match = re.search(r'(\d+\.\d+)(E|W)', forecast_text)
            if lon_match:
                lon_value = lon_match.group(1)
                lon_dir = lon_match.group(2)
            else:
                lon_value = '-999'
                lon_dir = ''
            if lon_dir == 'W':
                lon_value = str((float(lon_value)) * (-1))
            return lat_value, lon_value

        def find_max_wind(forecast_text):
            max_wind_match = re.search(r'MAX SUSTAINED WINDS - (\d{3}) KT', forecast_text)
            gust_match = re.search(r'MAX SUSTAINED WINDS - (\d{3}) KT, GUSTS (\d{3}) KT', forecast_text)
            max_wind = str(int(max_wind_match.group(1))) if max_wind_match else '-999'
            gust = str(int(gust_match.group(2))) if gust_match else '-999'
            return max_wind, gust

        def find_radii(forecast_text):
            lines = forecast_text.strip().split('\n')
            wind_data = {}
            all_quadrants = ['NORTHEAST', 'SOUTHEAST', 'SOUTHWEST', 'NORTHWEST']
            current_speed = None
            for line in lines:
                line = line.strip()

                speed_match = re.match(r'RADIUS OF (\d{3}) KT WINDS - (\d{3}) NM ([A-Z]+) QUADRANT', line)
                if speed_match:
                    speed = speed_match.group(1).lstrip('0')  # 去前导0
                    radius = str(int(speed_match.group(2)))
                    direction = speed_match.group(3)

                    current_speed = speed
                    wind_data[current_speed] = {q: '-999' for q in all_quadrants}
                    wind_data[current_speed][direction] = radius
                else:
                    direction_match = re.match(r'(\d{3}) NM ([A-Z]+) QUADRANT', line)
                    if direction_match and current_speed:
                        radius = str(int(direction_match.group(1)))
                        direction = direction_match.group(2)
                        wind_data[current_speed][direction] = radius
            r8_flag = r10_flag = r12_flag = 0
            r8 = r10 = r12 = '-999'
            r8Ne = r8Se = r8Sw = r8Nw = '-999'
            r10Ne = r10Se = r10Sw = r10Nw = '-999'
            r12Ne = r12Se = r12Sw = r12Nw = '-999'
            for speed in sorted(wind_data.keys()):
                if speed == '34':
                    for direction in all_quadrants:
                        r8_flag += 1
                        if direction == 'NORTHEAST':
                            r8Ne = wind_data[speed][direction]
                        elif direction == 'SOUTHEAST':
                            r8Se = wind_data[speed][direction]
                        elif direction == 'SOUTHWEST':
                            r8Sw = wind_data[speed][direction]
                        elif direction == 'NORTHWEST':
                            r8Nw = wind_data[speed][direction]
                        if r8_flag == 4:
                            r8 = com_rad(r8Ne, r8Se, r8Sw, r8Nw)
                elif speed == '50':
                    for direction in all_quadrants:
                        r10_flag += 1
                        if direction == 'NORTHEAST':
                            r10Ne = wind_data[speed][direction]
                        elif direction == 'SOUTHEAST':
                            r10Se = wind_data[speed][direction]
                        elif direction == 'SOUTHWEST':
                            r10Sw = wind_data[speed][direction]
                        elif direction == 'NORTHWEST':
                            r10Nw = wind_data[speed][direction]
                        if r10_flag == 4:
                            r10 = com_rad(r10Ne, r10Se, r10Sw, r10Nw)
                elif speed == '64':
                    for direction in all_quadrants:
                        r12_flag += 1
                        if direction == 'NORTHEAST':
                            r12Ne = wind_data[speed][direction]
                        elif direction == 'SOUTHEAST':
                            r12Se = wind_data[speed][direction]
                        elif direction == 'SOUTHWEST':
                            r12Sw = wind_data[speed][direction]
                        elif direction == 'NORTHWEST':
                            r12Nw = wind_data[speed][direction]
                        if r12_flag == 4:
                            r12 = com_rad(r12Ne, r12Se, r12Sw, r12Nw)
            return r8, r10, r12, r8Ne, r8Se, r8Sw, r8Nw, r10Ne, r10Se, r10Sw, r10Nw, r12Ne, r12Se, r12Sw, r12Nw

        def find_move_speed_dir(forecast_text):
            vector_match = re.search(r'VECTOR TO (\d{2,3}) HR POSIT: (\d{2,3}) DEG/ (\d{2,3}) KTS', forecast_text)
            vector_deg = str(int(vector_match.group(2))) if vector_match else None
            vector_speed = str(int(vector_match.group(3))) if vector_match else '-999'
            directions = [
                "北", "北东北", "北东", "北东东",
                "东", "南东东", "南东", "南东南",
                "南", "南西南", "南西", "南西西",
                "西", "北西西", "北西", "北西北"
            ]
            if vector_deg is not None:
                index = int(((int(vector_deg) + 11.25) % 360) // 22.5)
                dir = directions[index]
            else:
                dir = None
            return dir, vector_speed

        txt_split = txt_data.split('---\n')
        pre_data = []
        storm_type, storm_number, storm_name = find_name_code_instensity(txt_split[0])
        updatetime = find_updatetime(txt_split[0])
        if len(txt_split) > 3:
            flag_outlook = False
            for i, txt in enumerate(txt_split[2:]):
                if 'HRS' not in txt:
                    flag_outlook = True
                    count = 0
                    continue
                dir_pre, speed_pre = None, '-999'
                if i > 0:
                    if not flag_outlook:
                        dir_pre, speed_pre = find_move_speed_dir(txt_split[1 + i])
                    else:
                        count += 1
                        if count == 1:
                            dir_pre, speed_pre = find_move_speed_dir(txt_split[i])
                        else:
                            dir_pre, speed_pre = find_move_speed_dir(txt_split[i + 1])
                r8, r10, r12, r8Ne, r8Se, r8Sw, r8Nw, r10Ne, r10Se, r10Sw, r10Nw, r12Ne, r12Se, r12Sw, r12Nw = find_radii(
                    txt)
                pre_item = {
                    "name": storm_name,
                    "lat": find_lat_lon(txt)[0],
                    "lng": find_lat_lon(txt)[1],
                    "radius8": r8,
                    "radius10": r10,
                    "radius12": r12,
                    "r8Ne": r8Ne,
                    "r8Se": r8Se,
                    "r8Sw": r8Sw,
                    "r8Nw": r8Nw,
                    "r10Ne": r10Ne,
                    "r10Se": r10Se,
                    "r10Sw": r10Sw,
                    "r10Nw": r10Nw,
                    "r12Ne": r12Ne,
                    "r12Se": r12Se,
                    "r12Sw": r12Sw,
                    "r12Nw": r12Nw,
                    "centerSpeed": find_max_wind(txt)[0],
                    "gust": find_max_wind(txt)[1],
                    "bizDate": find_time(txt),
                    "centerPressure": '-999',
                    "moveSpeed": speed_pre,
                    "moveDirection": dir_pre,
                    "update_Org_time": updatetime,
                    "forecastOrg": "United States Navy",
                    "forcastSource": "JTWC",
                    "code": storm_number,
                    "intensity": None,
                }
                pre_data.append(pre_item)

        safe_updatetime = updatetime.replace(":", "").replace(" ", "_").replace("-", "_")
        save_name = f"JTWC_data/{sys_id}/pre_{safe_updatetime}.json"
        return pre_data, save_name

    def page_contains_tc_warning(self, driver):
        try:
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(2)
            page_text = driver.find_element(By.TAG_NAME, "body").text

            return "TC Warning Text" in page_text or "TCFA Text" in page_text
        except Exception as e:
            logging.error("页面加载失败：", e)
            return False

    def extract_urls_from_html(self, base_url, html_content=None, url=None):
        """
        从 HTML 中提取特定链接

        Args:
            base_url: 基础URL用于解析相对链接
            html_content: 直接提供HTML内容
            url: 或者提供URL自动获取
        """
        if url and not html_content:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Referer': 'https://www.metoc.navy.mil/jtwc/jtwc.html'
            }
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            html_content = response.text

        soup = BeautifulSoup(html_content, "html.parser")
        tc_urls, earth_urls = [], []

        # 更精确的选择器
        # 方法1：直接查找所有链接，根据文本过滤
        for a_tag in soup.find_all("a", href=True):
            text = a_tag.get_text(strip=True)
            href = a_tag["href"]

            # 转换为绝对URL
            full_url = urljoin(base_url, href)

            # 分类
            if any(keyword in text for keyword in ["TC Warning Text", "TCFA Text"]):
                tc_urls.append(full_url)
            elif "Google Earth Overlay" in text:
                earth_urls.append(full_url)

        # 方法2：使用CSS选择器更精确查找
        # 假设链接在特定结构中
        for item in soup.select("ul li, ol li, div.link-item"):
            a_tag = item.find("a", href=True)
            if a_tag:
                text = a_tag.get_text(strip=True)
                href = urljoin(base_url, a_tag["href"])

                if "TC Warning Text" in text or "TCFA Text" in text:
                    tc_urls.append(href)
                elif "Google Earth Overlay" in text:
                    earth_urls.append(href)

        return tc_urls, earth_urls

    def fetch_txt_via_browser(self, driver, url):
        try:
            driver.get(url)
            time.sleep(2)
            body_text = driver.find_element(By.TAG_NAME, "pre").text
            logging.info(f"获取 TXT 成功：{url}")
            return body_text
        except Exception as e:
            logging.error(f"获取 TXT 失败：{url}, 原因：{e}")
            return ''

    def fetch_via_browser_universal(self, driver, url):
        """
        万能浏览器下载器：替代原有的 requests.get
        直接利用已经创建好的 driver 访问网页并获取源码
        """
        try:
            logging.info(f"正在通过浏览器获取: {url}")
            driver.get(url)
            time.sleep(3)  # 稍微等一下，躲避反爬并确保加载完成
            return driver.page_source
        except Exception as e:
            logging.error(f"浏览器获取失败: {url}, 原因: {e}")
            return ""

    def fetch_and_parse_kmz(self, driver, url):
        """
        通过浏览器获取 KMZ 文件（二进制模式），自动携带 cookies/referer，不保存到本地。
        自动在内存中解析出 KML → JSON。
        """
        try:
            js = f"""
            return fetch("{url}", {{
                method: 'GET',
                credentials: 'include'
            }}).then(r => r.arrayBuffer()).then(buf => {{
                let bytes = new Uint8Array(buf);
                let chunkSize = 0x8000; // 每次处理 32KB，避免栈溢出
                let binary = '';
                for (let i = 0; i < bytes.length; i += chunkSize) {{
                    let chunk = bytes.subarray(i, i + chunkSize);
                    binary += String.fromCharCode.apply(null, chunk);
                }}
                return btoa(binary);
            }});
            """
            b64_data = driver.execute_script(js)
            kmz_bytes = io.BytesIO(base64.b64decode(b64_data))

            ns = {'kml': 'http://www.opengis.net/kml/2.2'}
            placemarks = []
            with zipfile.ZipFile(kmz_bytes, 'r') as kmz:
                for file in kmz.namelist():
                    if file.endswith('.kml'):
                        root = ET.fromstring(kmz.read(file))
                        for pm in root.findall('.//kml:Placemark', ns):
                            name = pm.find('kml:name', ns)
                            coords = pm.find('.//kml:coordinates', ns)
                            if name is not None and coords is not None:
                                placemarks.append({
                                    "name": name.text.strip(),
                                    "coordinates": coords.text.strip()
                                })

            logging.info(f" 已成功从浏览器中解析 KMZ：{url}（{len(placemarks)} 条记录）")
            return placemarks

        except Exception as e:
            logging.error(f"KMZ 解析失败（浏览器模式）：{url}，原因：{e}")
            return []

    def get_default_download_dir(self):
        return os.path.join(os.environ["USERPROFILE"], "Downloads")

    def parse_kml_to_json(self, kml_path, output_json_path):
        tree = ET.parse(kml_path)
        root = tree.getroot()
        ns = {'kml': 'http://www.opengis.net/kml/2.2'}
        placemarks_data = []

        for placemark in root.findall('.//kml:Placemark', ns):
            name = placemark.find('kml:name', ns)
            coords = placemark.find('.//kml:coordinates', ns)
            if name is not None and coords is not None:
                placemarks_data.append({
                    "name": name.text.strip(),
                    "coordinates": coords.text.strip()
                })
        with open(output_json_path, 'w', encoding='utf-8') as f:
            json.dump(placemarks_data, f, ensure_ascii=False, indent=2)

    def attach_driver_with_retries(self, retries=10, delay=1.0):
        """手动指定Chromium路径"""
        options = self.setup_chrome_options()
        last_exc = None

        for attempt in range(1, retries + 1):
            try:
                # 手动创建Service
                service = ChromeService(executable_path=self.CHROMEDRIVER_PATH)
                driver = webdriver.Chrome(service=service, options=options)

                driver.set_page_load_timeout(60)
                driver.implicitly_wait(10)
                logging.info(f"成功创建Chromium WebDriver (第{attempt}次尝试)")
                return driver

            except Exception as e:
                last_exc = e
                logging.warning(f"创建WebDriver第{attempt}次尝试失败: {e}")
                time.sleep(delay)

        logging.error(f"多次尝试创建WebDriver都失败: {last_exc}")
        raise last_exc

    def safe_quit_driver(self, driver):
        try:
            if driver:
                driver.quit()
                logging.info("Driver 已正常退出。")
        except Exception as e:
            logging.warning(f"退出 driver 时发生异常：{e}")

    def extract_prefix(self, filename: str):
        """提取完整台风代码前缀（例如 ep9225.json → ep9225）"""
        match = re.search(r'([a-zA-Z]{2}\d{4})', os.path.basename(filename))
        return match.group(1) if match else None

    def test_with_requests(self):
        """使用requests测试访问"""
        import requests

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }

        try:
            response = requests.get('https://mirror.mesovortices.com/jtwc/jtwc.html',
                                    headers=headers, timeout=30)
            logging.info(f"Requests访问结果: 状态码 {response.status_code}")

            if response.status_code == 200:
                logging.info("Requests访问成功")
                # 保存内容用于分析
                with open("log_typhoon/requests_response.html", "w", encoding="utf-8") as f:
                    f.write(response.text)
                return True
            else:
                logging.error(f"Requests访问失败: {response.status_code}")
                with open("log_typhoon/requests_response.html", "w", encoding="utf-8") as f:
                    f.write(response.text)
                return False

        except Exception as e:
            logging.error(f"Requests访问异常: {e}")
            return False

    def run(self):
        max_retries = 5
        retry_count = 0
        driver = None

        try:
            while retry_count < max_retries:
                try:
                    if driver is None:
                        driver = self.attach_driver_with_retries(retries=5, delay=2.0)
                        if driver is None:
                            logging.error("无法创建WebDriver，等待后重试...")
                            retry_count += 1
                            time.sleep(10)
                            continue
                    rss_url = 'https://www.metoc.navy.mil/jtwc/rss/jtwc.rss'
                    logging.info(f"访问URL: {rss_url}")
                    driver.get(rss_url)

                    time.sleep(5)

                    if self.page_contains_tc_warning(driver):
                        logging.info("成功通过验证并检测到TC警告信息，开始处理数据...")
                        headers = {
                            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                            "Referer": "https://www.metoc.navy.mil/jtwc/jtwc.html"
                        }

                        url = "https://www.metoc.navy.mil/jtwc/rss/jtwc.rss"

                        # response = requests.get(url, headers=headers, timeout=30)
                        # # 直接解析response.text
                        # root = ET.fromstring(response.text)
                        # 修改后的代码
                        driver.get(url)  # 让已经打开的浏览器去访问这个网址
                        time.sleep(3)  # 等 3 秒，让网页加载完

                        # 1. 拿到浏览器里的内容（包含 HTML 标签）
                        html_content = driver.page_source

                        # 2. 提取出真正的 XML 部分（过滤掉 pre 标签）
                        soup = BeautifulSoup(html_content, 'html.parser')
                        rss_text = soup.find('pre').text if soup.find('pre') else html_content
                        # 去掉可能存在的 XML 声明前的杂质
                        rss_text = rss_text.strip()

                        try:
                            # 3. 重新定义 root，解决 NameError
                            root = ET.fromstring(rss_text)
                            logging.info("成功解析纯净 RSS XML 内容")
                        except Exception as e:
                            logging.error(f"XML 解析依然失败: {e}")
                            # 如果还是失败，打印前100位看看是不是有乱码
                            logging.error(f"失败内容前缀: {rss_text[:100]}")
                            continue

                        tc_urls = []
                        earth_urls = []

                        # 4. 现在 root 已经定义了，可以正常遍历了
                        for item in root.findall('.//item'):
                            description = item.find('description')
                            if description is not None and description.text:
                                # 解析 description 里的 HTML 片段
                                desc_soup = BeautifulSoup(description.text, 'html.parser')
                                for a_tag in desc_soup.find_all('a', href=True):
                                    href = a_tag['href']
                                    text = a_tag.get_text(strip=True)
                                    full_url = urljoin("https://www.metoc.navy.mil", href)

                                    if "TC Warning Text" in text or "TCFA Text" in text:
                                        tc_urls.append(full_url)
                                    elif "Google Earth Overlay" in text:
                                        earth_urls.append(full_url)
                        logging.info(f"找到{len(tc_urls)}个TXT链接和{len(earth_urls)}个KMZ链接")

                        txt_data_map = {}
                        json_data_map = {}

                        # 处理TXT数据
                        for url in tc_urls:
                            txt_content = self.fetch_txt_via_browser(driver, url)
                            sid = self.extract_sys_id(url)
                            prefix = self.extract_prefix(url)
                            if txt_content and sid and prefix:
                                txt_data_map[sid] = (prefix, txt_content)

                        # 处理KMZ数据
                        for url in earth_urls:
                            json_data = self.fetch_and_parse_kmz(driver, url)
                            sid = self.extract_sys_id(url)
                            prefix = self.extract_prefix(url)
                            if json_data and sid and prefix:
                                json_data_map[sid] = (prefix, json_data)

                        # 获取匹配的ID
                        matched_ids = set(txt_data_map.keys()) & set(json_data_map.keys())
                        txt_only_ids = set(txt_data_map.keys()) - matched_ids

                        # 为只有txt数据的记录创建空的json_data
                        for sid in txt_only_ids:
                            prefix, _ = txt_data_map[sid]
                            json_data_map[sid] = (prefix, [])

                        # 处理所有有txt数据的记录
                        all_txt_ids = matched_ids | txt_only_ids
                        processed_count = 0

                        for sys_id in all_txt_ids:
                            prefix, txt_data = txt_data_map[sys_id]
                            _, json_data = json_data_map[sys_id]
                            try:
                                JTWC_history_data, his_updatetime = self.save_history_data(txt_data, json_data, prefix)
                                self.his_data.append(JTWC_history_data)
                                self.his_time.append(his_updatetime)

                                JTWC_pre_data, pre_updatetime = self.save_pre_data(txt_data, prefix)
                                self.pre_data.append(JTWC_pre_data)
                                self.pre_time.append(pre_updatetime)

                                processed_count += 1
                                logging.info(f"成功处理系统 {prefix}")

                            except Exception as e:
                                logging.exception(f"处理系统 {prefix} 时出错: {e}")

                        logging.info(f"数据处理完成，共处理 {processed_count} 个系统")
                        break

                    else:
                        logging.warning("未检测到TC警告信息，刷新页面重试...")
                        driver.refresh()
                        time.sleep(20)
                        retry_count += 1
                        rss_url = 'https://www.metoc.navy.mil/jtwc/rss/jtwc.rss'
                        logging.info(f"访问URL: {rss_url}")
                        driver.get(rss_url)


                except Exception as e:
                    logging.exception(f"处理过程中出现异常: {e}")
                    self.safe_quit_driver(driver)
                    driver = None
                    retry_count += 1
                    time.sleep(10)

        except KeyboardInterrupt:
            logging.info("程序被用户中断")
        finally:
            self.safe_quit_driver(driver)
            logging.info("程序退出")

class TyphoonDBHandler:
    PROCESSED_PRE_FILES_PATH = "processed_files.json"
    TIME_INTERVAL = 2

    def __init__(self, db_config=None):
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
        self.tables_created = False

        # 初始化时加载已处理文件记录
        self._load_processed_files()

    def _load_processed_files(self):
        """从JSON文件加载已处理文件记录"""
        try:
            if os.path.exists(self.PROCESSED_PRE_FILES_PATH):
                with open(self.PROCESSED_PRE_FILES_PATH, 'r', encoding='utf-8') as f:
                    self.PROCESSED_PRE_FILES = json.load(f)
            else:
                self.PROCESSED_PRE_FILES = {}
        except (json.JSONDecodeError, FileNotFoundError):
            self.PROCESSED_PRE_FILES = {}

    def _save_processed_files(self):
        """保存已处理文件记录到JSON文件"""
        try:
            with open(self.PROCESSED_PRE_FILES_PATH, 'w', encoding='utf-8') as f:
                json.dump(self.PROCESSED_PRE_FILES, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logging.error(f"保存已处理文件记录失败: {e}")


    def connect(self):
        try:
            self.connection = psycopg2.connect(**self.conn_params)
            logging.info("成功连接到 PostgreSQL 数据库")
            self.ensure_tables_exist()
        except OperationalError as e:
            logging.error(f"数据库连接失败：{e}")
            raise

    def close(self):
        if self.connection:
            self.connection.close()
            logging.info("数据库连接已关闭")

    # ------------------- 建表 -------------------
    def ensure_tables_exist(self):
        """确保所有必要的表都存在"""
        if not self.tables_created:
            self.create_typhoon_tables()
            self.tables_created = True

    def create_typhoon_tables(self):
        """创建台风信息主表"""

        # 1. typhoon_info 主表（不分年）- 添加 tc_code 字段
        create_info_table = """
        CREATE TABLE IF NOT EXISTS typhoon_info (
            tc_id VARCHAR(50) PRIMARY KEY,
            tc_code VARCHAR(50),
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

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(create_info_table)
                self.connection.commit()
                logging.info("主表 typhoon_info 创建完成（或已存在）")
        except Exception as e:
            logging.error(f"创建表失败: {e}")
            self.connection.rollback()
            raise

    def create_year_table(self, table_type, year):
        """创建指定年份的历史或预测表"""

        table_name = f"typhoon_{table_type}_{year}"

        if table_type == "history":
            # 历史表结构
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                tc_id VARCHAR(50),
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
                bizDate VARCHAR(20) DEFAULT NULL,
                centerPressure NUMERIC(9,2) DEFAULT -999,
                moveSpeed NUMERIC(9,2) DEFAULT -999,
                gust NUMERIC(9,2) DEFAULT -999,
                moveDirection VARCHAR(8) DEFAULT NULL,
                update_Org_time VARCHAR(20) DEFAULT NULL,
                probabilityCircle NUMERIC(9,2) DEFAULT -999,
                danger_ord JSONB DEFAULT '[]'::jsonb,
                FOREIGN KEY (tc_id) REFERENCES typhoon_info(tc_id) ON DELETE CASCADE
            );
            """
        elif table_type == "forecast":
            # 预测表结构
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                tc_id VARCHAR(50),
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
                bizDate VARCHAR(20) DEFAULT NULL,
                centerPressure NUMERIC(9,2) DEFAULT -999,
                moveSpeed NUMERIC(9,2) DEFAULT -999,
                gust NUMERIC(9,2) DEFAULT -999,
                moveDirection VARCHAR(10) DEFAULT NULL,
                update_Org_time VARCHAR(20) DEFAULT NULL,
                probabilityCircle NUMERIC(9,2) DEFAULT -999,
                FOREIGN KEY (tc_id) REFERENCES typhoon_info(tc_id) ON DELETE CASCADE
            );
            """
        else:
            raise ValueError(f"未知的表类型: {table_type}")

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(create_table_sql)
                self.connection.commit()
                logging.info(f"分表 {table_name} 创建完成（或已存在）")
        except Exception as e:
            logging.error(f"创建分表 {table_name} 失败: {e}")
            self.connection.rollback()
            raise

    def drop_table(self, table_name):
        query = f"DROP TABLE IF EXISTS {table_name};"
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            self.connection.commit()
        logging.info(f"表 {table_name} 已删除")

    # ------------------- 辅助函数 -------------------
    def get_typhoon_year_from_info(self, tc_id):
        """从typhoon_info表获取台风创建年份"""
        self.ensure_tables_exist()

        query = """
        SELECT EXTRACT(YEAR FROM create_time) as year 
        FROM typhoon_info 
        WHERE tc_id = %s;
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, (tc_id,))
                result = cursor.fetchone()
                if result and result[0]:
                    return int(result[0])
                else:
                    return datetime.now().year
        except Exception as e:
            logging.error(f"获取台风年份失败: {e}")
            return datetime.now().year

    def get_year_table_name(self, table_type, tc_id):
        """获取指定台风对应的年份分表名"""
        self.ensure_tables_exist()

        year = self.get_typhoon_year_from_info(tc_id)
        table_name = f"typhoon_{table_type}_{year}"

        try:
            self.create_year_table(table_type, year)
        except:
            pass

        return table_name

    # ------------------- 插入数据 -------------------
    def get_max_bizdate_by_source_sysid(self, tc_id):
        """获取历史表中指定tc_id的最大bizDate"""
        try:
            table_name = self.get_year_table_name("history", tc_id)
            query = f"""
            SELECT MAX(bizDate) FROM {table_name} 
            WHERE tc_id = %s;
            """
            with self.connection.cursor() as cursor:
                cursor.execute(query, (tc_id,))
                result = cursor.fetchone()
                if result and result[0]:
                    return result[0]
                return None
        except Exception as e:
            logging.error(f"获取最大bizdate失败: {e}")
            return None

    def get_max_bizdate_by_source_forecast(self, tc_id):
        """获取预测表中指定tc_id的最大bizDate"""
        try:
            table_name = self.get_year_table_name("forecast", tc_id)
            query = f"""
            SELECT MAX(bizDate) FROM {table_name} 
            WHERE tc_id = %s;
            """
            with self.connection.cursor() as cursor:
                cursor.execute(query, (tc_id,))
                result = cursor.fetchone()
                if result and result[0]:
                    return result[0]
                return None
        except Exception as e:
            logging.error(f"获取最大bizdate失败: {e}")
            return None

    def generate_tc_id_from_filepath(self, file_path):
        """生成基于文件路径的唯一 tc_id"""
        hash_value = hashlib.sha256(file_path.encode('utf-8')).hexdigest()
        return str(uuid.UUID(hash_value[:32]))


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
            self.ensure_tables_exist()
            with self.connection.cursor() as cursor:
                name_upper = name.upper()

                query = """
                SELECT tc_code
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
            self.ensure_tables_exist()
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
        """
        try:
            try:
                lat_float = float(lat)
                lng_float = float(lng)
            except (ValueError, TypeError) as e:
                logging.warning(f"经纬度转换失败: lat={lat}, lng={lng}, 错误: {e}")
                return None

            self.ensure_tables_exist()
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name LIKE 'typhoon_history_%'
                """)
                history_tables = [row[0] for row in cursor.fetchall()]

                for table_name in history_tables:
                    query = f"""
                    SELECT ti.tc_code, th.lat, th.lng
                    FROM typhoon_info ti
                    INNER JOIN {table_name} th ON ti.tc_id = th.tc_id
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

                logging.info(f"未找到相似记录: 经纬度=({lat_float}, {lng_float}), 范围={error_range}度, 天数={days}天")
                return None

        except Exception as e:
            logging.error(f"查找相似记录失败: {e}")
            return None

    def find_similar_recent_record_tc_id(self, lat, lng, forecast_org, days=5, error_range=2):
        """
        检索最近N天存入的info表中对应历史表中的最后一条记录
        检查经纬度是否在误差范围内，并且forecastOrg相同
        """
        try:
            try:
                lat_float = float(lat)
                lng_float = float(lng)
            except (ValueError, TypeError) as e:
                logging.warning(f"经纬度转换失败: lat={lat}, lng={lng}, 错误: {e}")
                return None

            self.ensure_tables_exist()
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name LIKE 'typhoon_history_%'
                """)
                history_tables = [row[0] for row in cursor.fetchall()]

                for table_name in history_tables:
                    query = f"""
                    SELECT ti.tc_id, ti.tc_code, th.lat, th.lng, ti.forecastorg
                    FROM typhoon_info ti
                    INNER JOIN {table_name} th ON ti.tc_id = th.tc_id
                    WHERE ti.create_time >= CURRENT_TIMESTAMP - INTERVAL '%s days'
                    AND th.lat IS NOT NULL 
                    AND th.lng IS NOT NULL
                    AND ti.forecastorg = %s
                    AND th.lat::numeric >= %s - %s
                    AND th.lat::numeric <= %s + %s
                    AND th.lng::numeric >= %s - %s
                    AND th.lng::numeric <= %s + %s
                    ORDER BY th.create_time DESC
                    LIMIT 1;
                    """

                    cursor.execute(query, (
                        days,
                        forecast_org,
                        lat_float, error_range,
                        lat_float, error_range,
                        lng_float, error_range,
                        lng_float, error_range
                    ))
                    result = cursor.fetchone()

                    if result:
                        tc_id = result[0]
                        tc_code = result[1]
                        logging.info(
                            f"找到相似记录: tc_id={tc_id}, tc_code={tc_code}, 经纬度=({result[2]}, {result[3]}), forecastorg={result[4]}")
                        return {"tc_id": tc_id, "tc_code": tc_code}

                logging.info(
                    f"未找到相似记录: 经纬度=({lat_float}, {lng_float}), forecastorg={forecast_org}, 范围={error_range}度, 天数={days}天")
                return None

        except Exception as e:
            logging.error(f"查找相似记录失败: {e}")
            return None

    def insert_or_update_typhoon_info(self, file_path, data, batch_time):
        """根据文件路径生成 tc_id，并更新 typhoon_info 表中的数据"""
        self.ensure_tables_exist()

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
                    "1",
                    batch_time,
                    tc_id
                ))
                logging.info(f"Updated typhoon_info for tc_id: {tc_id}")

            else:
                current_lat = data.get("lat")
                current_lng = data.get("lng")

                source = data.get("forecastSource") or data.get("forcastSource")
                # 生成 tc_code
                if current_lat is not None and current_lng is not None:
                    similar_tc_id_recode = self.find_similar_recent_record_tc_id(current_lat, current_lng, source, 2, 2)
                    if similar_tc_id_recode:
                        tc_id = similar_tc_id_recode['tc_id']
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

                        org = data.get("forecastOrg") or data.get("forcastOrg")

                        cursor.execute(update_query, (
                            self.convert_to_str(data.get("name")).upper(),
                            self.convert_to_str(source),
                            self.convert_to_str(org),
                            self.convert_to_str(data.get("intensity")),
                            self.convert_to_str(data.get("code")),
                            self.convert_to_str(data.get("sys_id")),
                            "1",
                            batch_time,
                            tc_id
                        ))
                        logging.info(f"Updated typhoon_info for tc_id: {tc_id}")


                    else:
                        similar_tc_code = self.find_similar_recent_record(current_lat, current_lng, 3, 2)
                        if similar_tc_code:
                            tc_code = similar_tc_code
                        else:
                            count = self.get_typhoon_info_count()
                            tc_code = self.generate_tc_id_from_filepath(str(count))
                else:
                    count = self.get_typhoon_info_count()
                    tc_code = self.generate_tc_id_from_filepath(str(count))
                # 如果该 tc_id 不存在，执行 INSERT（包含 tc_code）
                insert_query = """
                INSERT INTO typhoon_info (
                    tc_id, tc_code, name, forecastSource, forecastOrg, intensity, code, sys_id, status, create_time, update_time
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
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
                    "1",
                    batch_time,
                    batch_time
                ))

                logging.info(f"Inserted new typhoon_info for tc_id: {tc_id}, tc_code: {tc_code}")

        self.connection.commit()
        return tc_id

    def insert_history_data(self, data, batch_time, file_path):
        """插入历史数据到对应年份的分表"""
        self.ensure_tables_exist()

        default_value = "-999"

        fields = [
            "tc_id", "create_time", "update_time", "sys_id", "name", "lat", "lng", "radius7", "radius8", "radius10",
            "radius12",
            "r7N", "r7S", "r7W", "r7E",
            "r7Ne", "r7Se", "r7Sw", "r7Nw",
            "r8N", "r8S", "r8W", "r8E",
            "r8Ne", "r8Se", "r8Sw", "r8Nw",
            "r10N", "r10S", "r10W", "r10E",
            "r10Ne", "r10Se", "r10Sw", "r10Nw",
            "r12N", "r12S", "r12W", "r12E",
            "r12Ne", "r12Se", "r12Sw", "r12Nw",
            "centerSpeed", "bizDate", "centerPressure", "moveSpeed",
            "moveDirection", "update_Org_time", "forecastSource", "gust", "code", "intensity", "probabilityCircle","danger_ord"
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

        with self.connection.cursor() as cursor:
            total_inserted = 0
            for (source, sys_id), records in source_sys_map.items():
                try:
                    tc_id = self.insert_or_update_typhoon_info(file_path, records[-1], batch_time)
                except Exception as e:
                    logging.error(f"插入或更新typhoon_info失败: {e}")
                    continue

                try:
                    table_name = self.get_year_table_name("history", tc_id)
                except Exception as e:
                    logging.error(f"获取年份分表失败: {e}")
                    continue

                insert_query = f"""
                INSERT INTO {table_name} ({", ".join(fields)})
                VALUES ({", ".join(["%s"] * len(fields))});
                """

                try:
                    max_bizdate = self.get_max_bizdate_by_source_sysid(tc_id)
                except:
                    max_bizdate = None

                filtered_records = [r for r in records if
                                    r.get("bizDate") and (max_bizdate is None or r.get("bizDate") > max_bizdate)]

                if not filtered_records:
                    continue

                for record in filtered_records:
                    try:
                        system_id = self.convert_to_str(record.get('sys_id') or record.get('code'))
                        r7 = self.convert_to_float(record.get('radius7') or record.get('radius7(30kt)', default_value))
                        r8 = self.convert_to_float(record.get('radius8') or record.get('radius8(34kt)', default_value))
                        r10 = self.convert_to_float(
                            record.get('radius10') or record.get('radius10(50kt)', default_value))
                        r12 = self.convert_to_float(
                            record.get('radius12') or record.get('radius12(64kt)', default_value))
                        source = record.get("forecastSource") or record.get("forecastsource") or record.get(
                            "forcastsource") or record.get("forcastSource") or ""
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
                            self.convert_to_float(record.get("probabilityCircle")),
                            Json(record.get("danger_ord", []))
                        ]

                        cursor.execute(insert_query, values)
                        total_inserted += 1
                    except Exception as e:
                        logging.error(f"插入单条历史记录失败: {e}")
                        continue

            self.connection.commit()
            logging.info(f"共插入历史数据: {total_inserted}条数据")

    def insert_JTWC_forecast_data(self, data, batch_time, file_path):
        """插入台风预测数据到对应年份的分表"""
        self.ensure_tables_exist()

        tc_id = self.generate_tc_id_from_filepath(file_path)
        default_value = "-999"

        fields = [
            "tc_id", "create_time", "update_time", "sys_id", "name", "lat", "lng", "radius7", "radius8", "radius10",
            "radius12",
            "r7N", "r7S", "r7W", "r7E",
            "r7Ne", "r7Se", "r7Sw", "r7Nw",
            "r8N", "r8S", "r8W", "r8E",
            "r8Ne", "r8Se", "r8Sw", "r8Nw",
            "r10N", "r10S", "r10W", "r10E",
            "r10Ne", "r10Se", "r10Sw", "r10Nw",
            "r12N", "r12S", "r12W", "r12E",
            "r12Ne", "r12Se", "r12Sw", "r12Nw",
            "centerSpeed", "gust", "bizDate", "centerPressure", "moveSpeed",
            "moveDirection", "update_Org_time", "forecastSource", "forecastOrg", "code", "intensity",
            "probabilityCircle"
        ]

        if isinstance(data, dict):
            data = [data]

        try:
            table_name = self.get_year_table_name("forecast", tc_id)
        except Exception as e:
            logging.error(f"获取年份分表失败: {e}")
            return

        insert_query = f"""
        INSERT INTO {table_name}
        ({", ".join(fields)})
        VALUES ({", ".join(["%s"] * len(fields))});
        """

        with self.connection.cursor() as cursor:
            if data and len(data) > 0:
                forecast_org = data[0].get("forecastOrg")
                if forecast_org:
                    try:
                        cursor.execute(f"""
                                    UPDATE {table_name}
                                    SET is_deleted = 1
                                    WHERE tc_id = %s AND forecastOrg = %s
                                """, (tc_id, forecast_org))
                        logging.info(
                            f"Set is_deleted = 1 for existing forecast data of tc_id: {tc_id} and forecastOrg: {forecast_org}")
                    except Exception as e:
                        logging.error(f"更新预测表失败: {e}")

            for record in data:
                try:
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
                except Exception as e:
                    logging.error(f"插入单条预测记录失败: {e}")
                    continue
            self.connection.commit()

    def update_typhoon_info_status(self):
        """检查typhoon_info表格中status为1的行，更新相应的status为0"""
        self.ensure_tables_exist()

        current_time = datetime.now(timezone.utc)

        try:
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

                    max_forecast_bizdate = self.get_max_bizdate_by_source_forecast(tc_id)

                    if max_forecast_bizdate:
                        if isinstance(max_forecast_bizdate, str):
                            try:
                                max_forecast_bizdate = datetime.strptime(max_forecast_bizdate, "%Y-%m-%d %H:%M:%S")
                                max_forecast_bizdate = max_forecast_bizdate.replace(tzinfo=timezone.utc)
                            except ValueError:
                                logging.warning(f"预测表时间格式错误，tc_id: {tc_id}, bizDate: {max_forecast_bizdate}")
                                continue

                        if max_forecast_bizdate < current_time:
                            target_time = max_forecast_bizdate
                            logging.info(f"台风 {tc_id} 预测数据已过期，最大bizDate: {max_forecast_bizdate}")

                            cursor.execute("""
                                            UPDATE typhoon_info
                                            SET status = '0'
                                            WHERE tc_id = %s
                                        """, (tc_id,))

                            year = self.get_typhoon_year_from_info(tc_id)
                            history_table = f"typhoon_history_{year}"
                            forecast_table = f"typhoon_forecast_{year}"

                            cursor.execute(f"""
                                UPDATE {history_table}
                                SET is_deleted = '1'
                                WHERE tc_id = %s
                            """, (tc_id,))

                            cursor.execute(f"""
                                UPDATE {forecast_table}
                                SET is_deleted = '1'
                                WHERE tc_id = %s
                            """, (tc_id,))

                            updated_count += 1
                    else:
                        max_history_bizdate = self.get_max_bizdate_by_source_sysid(tc_id)

                        if max_history_bizdate:
                            if isinstance(max_history_bizdate, str):
                                try:
                                    max_history_bizdate = datetime.strptime(max_history_bizdate, "%Y-%m-%d %H:%M:%S")
                                    max_history_bizdate = max_history_bizdate.replace(tzinfo=timezone.utc)
                                except ValueError:
                                    logging.warning(
                                        f"历史表时间格式错误，tc_id: {tc_id}, bizDate: {max_history_bizdate}")
                                    continue

                            history_plus_5_days = max_history_bizdate + timedelta(days=TyphoonDBHandler.TIME_INTERVAL)

                            if history_plus_5_days < current_time:
                                target_time = max_history_bizdate
                                logging.info(
                                    f"台风 {tc_id} 历史数据已过期（超过5天），最大bizDate: {max_history_bizdate}, 过期时间: {history_plus_5_days}")

                                cursor.execute("""
                                                UPDATE typhoon_info
                                                SET status = '0'
                                                WHERE tc_id = %s
                                            """, (tc_id,))

                                year = self.get_typhoon_year_from_info(tc_id)
                                history_table = f"typhoon_history_{year}"

                                cursor.execute(f"""
                                    UPDATE {history_table}
                                    SET is_deleted = '1'
                                    WHERE tc_id = %s
                                """, (tc_id,))
                                updated_count += 1
                        else:
                            logging.info(f"台风 {tc_id} 在预测表和历史表中均无数据，标记为过期")
                            cursor.execute("""
                                            UPDATE typhoon_info
                                            SET status = '0'
                                            WHERE tc_id = %s
                                        """, (tc_id,))
                            updated_count += 1
                        logging.info(f"已更新台风 {tc_id} 状态为过期，目标时间: {target_time}")

                self.connection.commit()
                logging.info(f"状态更新完成，共更新了 {updated_count} 个台风的状态")

        except Exception as e:
            logging.error(f"更新typhoon_info表格的status失败: {e}")
            self.connection.rollback()

    def get_max_bizdate_by_source_history(self, tc_id):
        """获取历史表中指定tc_id的最大bizDate"""
        try:
            table_name = self.get_year_table_name("history", tc_id)
            query = f"""
            SELECT MAX(bizDate) FROM {table_name} 
            WHERE tc_id = %s AND is_deleted = '0';
            """
            with self.connection.cursor() as cursor:
                cursor.execute(query, (tc_id,))
                result = cursor.fetchone()
                if result and result[0]:
                    return result[0]
                return None
        except Exception as e:
            logging.error(f"获取历史表最大bizDate失败，tc_id: {tc_id}, 错误: {e}")
            return None

    def clean_inactive_typhoons(self):
        """
        检查 typhoon_info 表中 status = 0 的台风，
        并将其在 typhoon_forecast 和 typhoon_history 表中的数据标记为 is_deleted = 1。
        """
        self.ensure_tables_exist()

        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT tc_id FROM typhoon_info WHERE status = '0'")
                inactive_tc_ids = [row[0] for row in cursor.fetchall()]

                if not inactive_tc_ids:
                    logging.info("没有 status = 0 的台风，不需要更新。")
                    return

                logging.info(f"检测到 {len(inactive_tc_ids)} 个失效台风：{inactive_tc_ids}")

                for tc_id in inactive_tc_ids:
                    year = self.get_typhoon_year_from_info(tc_id)

                    forecast_table = f"typhoon_forecast_{year}"
                    try:
                        cursor.execute(f"""
                            UPDATE {forecast_table}
                            SET is_deleted = 1
                            WHERE tc_id = %s
                        """, (tc_id,))
                    except Exception as e:
                        logging.error(f"更新预测表 {forecast_table} 失败: {e}")

                    history_table = f"typhoon_history_{year}"
                    try:
                        cursor.execute(f"""
                            UPDATE {history_table}
                            SET is_deleted = 1
                            WHERE tc_id = %s
                        """, (tc_id,))
                    except Exception as e:
                        logging.error(f"更新历史表 {history_table} 失败: {e}")

                self.connection.commit()
                logging.info("已成功同步删除状态的台风关联数据。")

        except Exception as e:
            logging.error(f"清理失效台风数据时发生错误: {e}")
            self.connection.rollback()

    def batch_insert_JTWC_historydata(self, data, file_path):
        """批量插入历史数据"""
        try:
            if not self.connection:
                self.connect()
            else:
                self.ensure_tables_exist()

            if isinstance(data, dict):
                data = [data]

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

            if 'JTWC' in file_path:
                for record in filtered_data:
                    if "forecastsource" not in record and "forecastSource" not in record:
                        record["forecastsource"] = "JTWC"

            batch_time = (datetime.now() - timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
            logging.info(f"正在处理文件: {file_path}，有效记录数: {len(filtered_data)}")
            path_parts = file_path.split("/")

            if path_parts[0] == "wmo_data":
                file_path = "/".join(path_parts[:2])
            else:
                file_path = "/".join(path_parts[:-1])
            self.insert_history_data(filtered_data, batch_time, file_path)

        except Exception as e:
            logging.error(f"读取或插入文件 {file_path} 出错: {e}")

    def batch_insert_JTWC_forecastdata(self, data, file_path):
        """批量插入预测数据"""
        dir_key = os.path.dirname(file_path)
        if dir_key not in self.PROCESSED_PRE_FILES:
            self.PROCESSED_PRE_FILES[dir_key] = []

        processed_files = self.PROCESSED_PRE_FILES[dir_key]

        if file_path in processed_files:
            logging.info(f"[{dir_key}] 文件 {file_path} 已处理过，跳过。")
            return

        else:
            try:
                if not self.connection:
                    self.connect()
                else:
                    self.ensure_tables_exist()

                if isinstance(data, dict):
                    data = [data]

                batch_time = (datetime.now() - timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
                logging.info(f"[{dir_key}] 正在处理文件: {file_path}，有效记录数: {len(data)}")
                path_parts = file_path.split("/")

                if path_parts[0] == "wmo_data":
                    file_path1 = "/".join(path_parts[:2])
                else:
                    file_path1 = "/".join(path_parts[:-1])
                self.insert_JTWC_forecast_data(data, batch_time, file_path1)

                processed_files.append(file_path)
                if len(processed_files) > 3:
                    processed_files.pop(0)

                # 保存到文件
                self._save_processed_files()

                self.update_typhoon_info_status()
            except Exception as e:
                logging.error(f"[{dir_key}] 读取或插入文件 {file_path} 出错: {e}")

    def export_table_to_csv(self, table_name, file_name):
        """导出表数据到CSV文件"""
        try:
            self.ensure_tables_exist()

            query = f"COPY {table_name} TO STDOUT WITH CSV HEADER"
            with self.connection.cursor() as cursor, open(file_name, 'w', encoding='utf-8') as f:
                cursor.copy_expert(query, f)
            logging.info(f"表 {table_name} 导出到 {file_name} 成功")
        except Exception as e:
            logging.error(f"导出表 {table_name} 失败: {e}")



def job_2hours():
    uuid = HEALTHCHECKS_UUIDS.get('WMO')
    if uuid:
        try:
            requests.get(f"{HEALTHCHECKS_BASE_URL}/{uuid}/start", timeout=5)
        except Exception as e:
            logging.warning(f"[WMO] Healthchecks ping(start) 失败: {e}")
    try:
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

            # 插入历史数据
            history_files = getattr(runner, "history_filename", [])
            if history_files:  # 列表不为空才执行
                try:
                    for i in range(len(history_files)):
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

            # 插入预测数据
            pre_files = getattr(runner, "pre_filename", [])
            if pre_files:
                try:
                    for i in range(len(pre_files)):
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

        logging.info("数据抓取完成，等待下次运行...")
        # Healthchecks.io 监控 - 任务成功
        if uuid:
            try:
                requests.get(f"{HEALTHCHECKS_BASE_URL}/{uuid}", timeout=5)
            except Exception as e:
                logging.warning(f"[WMO] Healthchecks ping(success) 失败: {e}")
    except Exception as e:
        # Healthchecks.io 监控 - 任务失败
        if uuid:
            try:
                requests.get(f"{HEALTHCHECKS_BASE_URL}/{uuid}/fail", timeout=5)
            except:
                pass


def job_30minutes():
    uuid = HEALTHCHECKS_UUIDS.get('raodong')
    if uuid:
        try:
            requests.get(f"{HEALTHCHECKS_BASE_URL}/{uuid}/start", timeout=5)
        except Exception as e:
            logging.warning(f"[raodong] Healthchecks ping(start) 失败: {e}")
    try:
        processor = NHCProcessorDB()

        logging.info("30分钟任务执行中...")

        processor.main_task()

        logging.info("30分钟任务执行完成。")
        # Healthchecks.io 监控 - 任务成功
        if uuid:
            try:
                requests.get(f"{HEALTHCHECKS_BASE_URL}/{uuid}", timeout=5)
            except Exception as e:
                logging.warning(f"[raodong] Healthchecks ping(success) 失败: {e}")
    except Exception as e:
        # Healthchecks.io 监控 - 任务失败
        if uuid:
            try:
                requests.get(f"{HEALTHCHECKS_BASE_URL}/{uuid}/fail", timeout=5)
            except:
                pass


def job_6hours():
    # Healthchecks.io 监控 - 任务开始
    uuid = HEALTHCHECKS_UUIDS.get('JTWC')
    if uuid:
        try:
            requests.get(f"{HEALTHCHECKS_BASE_URL}/{uuid}/start", timeout=5)
        except Exception as e:
            logging.warning(f"[JTWC] Healthchecks ping(start) 失败: {e}")
    try:
        # 初始化数据库连接
        handler = TyphoonDBHandler()
        handler.connect()

        # 需要执行的抓取器列表
        runners = [
            JTWCDataCollector()
        ]

        for runner in runners:
            logging.info(f"{runner.__class__.__name__} 开始抓取数据...")
            try:
                # 根据不同类型调用对应抓取方法
                if isinstance(runner, JTWCDataCollector):
                    runner.run()
                logging.info(f"{runner.__class__.__name__} 数据抓取完成。")
            except Exception as e:
                logging.error(f"{runner.__class__.__name__} 抓取数据失败: {e}")

            # 插入历史数据
            history_files = getattr(runner, "his_time", [])
            data_history = getattr(runner, "his_data", [])
            if history_files and data_history:
                try:
                    for i in range(len(history_files)):
                        handler.batch_insert_JTWC_historydata(data_history[i], history_files[i])
                        logging.info(f"正在插入历史数据文件: {history_files[i]}")
                except Exception as e:
                    logging.error(f"{runner.__class__.__name__} 历史数据入库失败: {e}")
            else:
                logging.info(f"{runner.__class__.__name__} 没有历史数据可插入，跳过。")

            # 插入预测数据
            pre_files = getattr(runner, "pre_time", [])
            data_pre = getattr(runner, "pre_data", [])
            if pre_files and data_pre:
                try:
                    for i in range(len(pre_files)):
                        handler.batch_insert_JTWC_forecastdata(data_pre[i], pre_files[i])
                        logging.info(f"正在插入预测数据文件: {pre_files[i]}")
                except Exception as e:
                    logging.error(f"{runner.__class__.__name__} 预测数据入库失败: {e}")
            else:
                logging.info(f"{runner.__class__.__name__} 没有预测数据可插入，跳过。")

        logging.info("所有抓取器任务完成，等待下次运行...")
        # Healthchecks.io 监控 - 任务成功
        if uuid:
            try:
                requests.get(f"{HEALTHCHECKS_BASE_URL}/{uuid}", timeout=5)
            except Exception as e:
                logging.warning(f"[JTWC] Healthchecks ping(success) 失败: {e}")
    except Exception as e:
        # Healthchecks.io 监控 - 任务失败
        if uuid:
            try:
                requests.get(f"{HEALTHCHECKS_BASE_URL}/{uuid}/fail", timeout=5)
            except:
                pass



if __name__ == "__main__":

    schedule.every(30).minutes.do(job_30minutes)
    schedule.every(2).hours.do(job_2hours)
    schedule.every(6).hours.do(job_6hours)


    logging.info("调度器已启动，每30分钟执行Raodong任务，每2小时执行Typhoon任务，每6小时执行JTWC任务")
    job_30minutes()
    job_2hours()
    job_6hours()

    while True:
        schedule.run_pending()
        time.sleep(60)
