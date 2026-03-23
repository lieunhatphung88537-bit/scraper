# 先在本地运行Start-Process -FilePath "C:\Program Files\Google\Chrome\Application\chrome.exe" -ArgumentList '--remote-debugging-port=9222','--user-data-dir=C:\ChromeDebugProfile'
# 下载KML到本地时可能会出现网页需要确认可以同时下载多个文件，需要手动确认
# 浏览器需要设置成可以默认最大化打开
# 需要挂梯子，人工验证已经显示下方点击方法可验证成功(国内源也可能访问成功，代码中自动设置了重复验证)
import time
import base64
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
import re
import math
import random
import logging
import io, zipfile
import os
import json
from datetime import datetime, timezone, timedelta
import psycopg2
from psycopg2 import OperationalError
import schedule
import hashlib
import uuid

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/jtwc_data_collector.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)


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

    def extract_urls_from_page_source(self, driver):
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        tc_urls, earth_urls = [], []
        for li in soup.find_all("li"):
            a_tag = li.find("a")
            if a_tag and "href" in a_tag.attrs:
                text = a_tag.get_text(strip=True)
                href = a_tag["href"]
                if ("TC Warning Text" in text) or ("TCFA Text" in text):
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

                    logging.info(f"访问URL: {self.URL}")
                    driver.get(self.URL)

                    time.sleep(60)

                    if self.page_contains_tc_warning(driver):
                        logging.info("成功通过验证并检测到TC警告信息，开始处理数据...")

                        # 提取URLs
                        tc_urls, earth_urls = self.extract_urls_from_page_source(driver)
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
                        time.sleep(60)
                        retry_count += 1
                        self.click_using_known_coordinates(driver)

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
    TIME_INTERVAL = 2
    PROCESSED_PRE_FILES = {}

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

    def adjust_time_minus_8_hours(self):
        """
        将三张表中的 create_time 和 update_time 字段都减去8小时
        格式从 '2025-10-30 14:39:42' 改为 '2025-10-30 06:39:42'
        """
        try:
            with self.connection.cursor() as cursor:
                # 要处理的表列表
                tables = ['typhoon_info', 'typhoon_history', 'typhoon_forecast']

                total_updated = 0

                for table in tables:
                    # 检查表是否存在需要更新的时间字段
                    if table == 'typhoon_info':
                        # typhoon_info 表有 create_time 和 update_time
                        update_query = f"""
                        UPDATE {table} 
                        SET create_time = create_time - INTERVAL '8 hours',
                            update_time = update_time - INTERVAL '8 hours'
                        WHERE create_time IS NOT NULL OR update_time IS NOT NULL;
                        """
                    else:
                        # typhoon_history 和 typhoon_forecast 表有 create_time 和 update_time
                        update_query = f"""
                        UPDATE {table} 
                        SET create_time = create_time - INTERVAL '8 hours',
                            update_time = update_time - INTERVAL '8 hours'
                        WHERE create_time IS NOT NULL OR update_time IS NOT NULL;
                        """

                    cursor.execute(update_query)
                    rows_updated = cursor.rowcount
                    total_updated += rows_updated

                    logging.info(f"表 {table}: 更新了 {rows_updated} 条记录的时间字段（-8小时）")

                self.connection.commit()
                logging.info(f"时间调整完成！总计更新了 {total_updated} 条记录")

        except Exception as e:
            logging.error(f"调整时间字段失败: {e}")
            self.connection.rollback()
            raise

    def update_forecastOrg_american_to_usa(self):
        """
        将三个表中的所有 forecastOrg 字段值从 'American' 更新为 'USA'
        """
        try:
            with self.connection.cursor() as cursor:
                # 更新 typhoon_info 表
                update_info_query = """
                   UPDATE typhoon_info 
                   SET forecastorg = 'USA'
                   WHERE forecastorg = 'American';
                   """
                cursor.execute(update_info_query)
                info_updated = cursor.rowcount

                # 更新 typhoon_forecast 表
                update_forecast_query = """
                   UPDATE typhoon_forecast 
                   SET forecastorg = 'USA'
                   WHERE forecastorg = 'American';
                   """
                cursor.execute(update_forecast_query)
                forecast_updated = cursor.rowcount

                self.connection.commit()

                logging.info(f"forecastOrg字段更新完成:")
                logging.info(f"  - typhoon_info表: {info_updated} 条记录已更新")
                logging.info(f"  - typhoon_forecast表: {forecast_updated} 条记录已更新")
                logging.info(f"总计更新了 {info_updated + forecast_updated} 条记录")

        except Exception as e:
            logging.error(f"更新forecastOrg字段失败: {e}")
            self.connection.rollback()
            raise

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
        try:
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
                    # 初始化 tc_code
                    tc_code = None

                    current_lat = data.get("lat")
                    current_lng = data.get("lng")

                    # 验证经纬度是否为有效数值
                    try:
                        lat_valid = current_lat is not None and self.is_convertible_to_float(current_lat)
                        lng_valid = current_lng is not None and self.is_convertible_to_float(current_lng)

                        if lat_valid and lng_valid:
                            similar_tc_id = self.find_similar_recent_record(current_lat, current_lng, 3, 2)
                            if similar_tc_id:
                                tc_code = similar_tc_id
                            else:
                                count = self.get_typhoon_info_count()
                                tc_code = self.generate_tc_id_from_filepath(str(count))
                        else:
                            count = self.get_typhoon_info_count()
                            tc_code = self.generate_tc_id_from_filepath(str(count))
                    except (ValueError, TypeError) as e:
                        logging.warning(f"经纬度转换失败: {e}，使用计数生成tc_code")
                        count = self.get_typhoon_info_count()
                        tc_code = self.generate_tc_id_from_filepath(str(count))

                    # 确保 tc_code 有值
                    if tc_code is None:
                        count = self.get_typhoon_info_count()
                        tc_code = self.generate_tc_id_from_filepath(str(count))

                    # 执行 INSERT
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
                        "1",  # assuming status is active (1)
                        batch_time,
                        batch_time
                    ))

                    logging.info(f"Inserted new typhoon_info for tc_id: {tc_id}")

            self.connection.commit()
            return tc_id

        except Exception as e:
            self.connection.rollback()
            logging.error(f"插入或更新台风信息失败: {e}")
            logging.error(f"失败数据 - 文件路径: {file_path}, 数据: {data}")
            raise

    def is_convertible_to_float(self, value):
        """检查值是否可以转换为float"""
        try:
            float(value)
            return True
        except (ValueError, TypeError):
            return False

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

    def insert_history_data(self, data, batch_time, file_path):
        """根据 forecastSource 和 sys_id 过滤插入比最大 bizDate 更新的数据"""
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
            "moveDirection", "update_Org_time", "forecastSource", "gust", "code", "intensity", "probabilityCircle"
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

    def sync_update_time_with_create_time(self, tc_ids):
        """
        将指定tc_id的update_time设置为与create_time相同

        Args:
            tc_ids: 字符串或字符串列表，要处理的tc_id
        """
        if isinstance(tc_ids, str):
            tc_ids = [tc_ids]

        if not tc_ids:
            logging.warning("未提供tc_id，跳过处理")
            return

        try:
            with self.connection.cursor() as cursor:
                total_updated = 0

                # 要处理的表列表
                tables = ['typhoon_info', 'typhoon_history']

                for table in tables:
                    # 构建SQL查询
                    update_query = f"""
                    UPDATE {table} 
                    SET update_time = create_time
                    WHERE tc_id IN ({','.join(['%s'] * len(tc_ids))})
                    AND create_time IS NOT NULL;
                    """

                    cursor.execute(update_query, tc_ids)
                    rows_updated = cursor.rowcount
                    total_updated += rows_updated

                    logging.info(f"表 {table}: 更新了 {rows_updated} 条记录")

                self.connection.commit()
                logging.info(f"同步完成！总计更新了 {total_updated} 条记录的update_time")

        except Exception as e:
            logging.error(f"同步update_time失败: {e}")
            self.connection.rollback()
            raise

    def sync_specific_tc_ids(self):
        """
        专门处理指定的两个tc_id
        """
        target_tc_ids = [
            "7980c5d1-3de8-ef51-21ed-1e636abdd828",
            "1b00eb89-ecad-3d23-ea1a-cbda704e6958"
        ]

        logging.info(f"开始处理指定的tc_id: {target_tc_ids}")
        self.sync_update_time_with_create_time(target_tc_ids)

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
            self.insert_history_data(filtered_data, batch_time, file_path)

        except Exception as e:
            logging.error(f"读取或插入文件 {file_path} 出错: {e}")

    def insert_JTWC_forecast_data(self, data, batch_time, file_path):
        """插入台风数据（所有记录共用相同的当前时间）"""
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

        insert_query = f"""
        INSERT INTO typhoon_forecast
        ({", ".join(fields)})
        VALUES ({", ".join(["%s"] * len(fields))});
        """

        with self.connection.cursor() as cursor:
            cursor.execute("""
                            UPDATE typhoon_forecast
                            SET is_deleted = 1
                            WHERE tc_id = %s
                        """, (tc_id,))
            logging.info(f"Set is_deleted = 1 for existing forecast data of tc_id: {tc_id}")
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


# 调度任务
schedule.every(6).hours.do(job)
logging.info("调度器已启动，每6小时执行一次任务。")
job()
# 持续运行调度器
while True:
    schedule.run_pending()
    time.sleep(60 * 60)  # 每小时检查一次任务

