# -*- coding: utf-8 -*-
"""
NOAA 统一爬虫入口 v1.0
======================================================================================
在一个进程中并行运行三个NOAA数据爬虫，每个爬虫有独立的定时任务。

包含的爬虫：
1. TXT 数据爬虫 - 标准气象数据 (.txt) → txt_data 表
2. OCEAN 数据爬虫 - 海洋数据 (.ocean) → ocean_data 表  
3. 多类型数据爬虫 - DART/DRIFT/SPEC/SUPL → 对应表

运行方式：
- Docker: python noaa_combined_scraper.py
- 本地: python noaa_combined_scraper.py

环境变量配置（可选）：
- TXT_INTERVAL: TXT爬虫间隔（分钟），默认 10
- OCEAN_INTERVAL: OCEAN爬虫间隔（分钟），默认 30
- MULTI_INTERVAL: 多类型爬虫间隔（分钟），默认 30
- ENABLE_TXT: 是否启用TXT爬虫，默认 true
- ENABLE_OCEAN: 是否启用OCEAN爬虫，默认 true
- ENABLE_MULTI: 是否启用多类型爬虫，默认 true

创建时间：2025-12-03
"""

import os
import sys
import time
import threading
import logging
from datetime import datetime, timedelta
import signal
import schedule
import requests

# Healthchecks.io 监控配置（单爬虫级别）
# 为每个爬虫在 https://healthchecks.io 创建独立的监控项，然后填入对应的UUID
HEALTHCHECKS_UUIDS = {
    'TXT': '89a066e8-2aaf-446b-8440-6a6c64b5e791',      # NOAA TXT数据爬虫（填入UUID）
    'OCEAN': 'e341c108-57af-44a8-804a-731e2d4d5a17',    # NOAA OCEAN数据爬虫（填入UUID）
    'MULTI': '94bbbe83-2cb3-4125-9a00-1fe96e3b0eb7',    # NOAA 多类型爬虫（填入UUID）
}
HEALTHCHECKS_BASE_URL = 'https://hc-ping.com'
HEARTBEAT_INTERVAL = 1440  # 心跳间隔（秒），任务运行期间每60秒发送一次

# 配置日志
def setup_main_logging():
    """设置主程序日志（只管理统一入口自己的日志）"""
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
    log_file = os.path.join(log_dir, 'noaa_combined.log')

    logger = logging.getLogger('noaa_combined')
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        # 文件 handler（每条日志立即刷新）
        class FlushFileHandler(logging.FileHandler):
            def emit(self, record):
                super().emit(record)
                self.flush()
        
        file_handler = FlushFileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(logging.Formatter('%(asctime)s - [%(name)s] - %(levelname)s - %(message)s'))
        logger.addHandler(file_handler)
        
        # 控制台 handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter('%(asctime)s - [%(name)s] - %(levelname)s - %(message)s'))
        logger.addHandler(console_handler)
    
    return logger


class ScraperRunner:
    """爬虫运行器 - 多线程独立运行"""
    
    def __init__(self, name, scraper_class, interval_minutes, **kwargs):
        self.name = name
        self.scraper_class = scraper_class
        self.interval_minutes = interval_minutes
        self.kwargs = kwargs
        self.run_count = 0
        self.consecutive_errors = 0
        self.max_consecutive_errors = 5
        self.logger = logging.getLogger('noaa_combined')
        self.running = True
        self.thread = None
    
    def run_task(self):
        """执行一次爬虫任务"""
        self.run_count += 1
        self.logger.info(f"\n{'#'*50}")
        self.logger.info(f"[{self.name}] 第 {self.run_count} 次运行")
        self.logger.info(f"{'#'*50}")
        
        # Healthchecks.io 监控 - 任务开始
        uuid = HEALTHCHECKS_UUIDS.get(self.name)
        if uuid:
            try:
                requests.get(f"{HEALTHCHECKS_BASE_URL}/{uuid}/start", timeout=5)
            except Exception as e:
                self.logger.warning(f"[{self.name}] Healthchecks ping(start) 失败: {e}")
        
        # 启动心跳线程（长时间任务期间定期发送心跳）
        heartbeat_stop = threading.Event()
        def heartbeat_worker():
            # 首次立即发送心跳（不等待）
            if uuid:
                try:
                    requests.get(f"{HEALTHCHECKS_BASE_URL}/{uuid}", timeout=5)
                except:
                    pass
            # 之后每隔HEARTBEAT_INTERVAL秒发送
            while not heartbeat_stop.is_set():
                heartbeat_stop.wait(HEARTBEAT_INTERVAL)
                if not heartbeat_stop.is_set() and uuid:
                    try:
                        requests.get(f"{HEALTHCHECKS_BASE_URL}/{uuid}", timeout=5)
                    except:
                        pass
        
        heartbeat_thread = None
        if uuid:
            heartbeat_thread = threading.Thread(target=heartbeat_worker, daemon=True)
            heartbeat_thread.start()
        
        try:
            import gc
            root = logging.getLogger()
            old_handlers = [h for h in root.handlers[2:]]
            for h in old_handlers:
                h.close()
                root.removeHandler(h)
            
            scraper = self.scraper_class(**self.kwargs)
            scraper.run_once()
            del scraper
            self.consecutive_errors = 0
            
            # 先停止心跳线程，再发送success
            heartbeat_stop.set()
            if heartbeat_thread:
                heartbeat_thread.join(timeout=2)
            
            # Healthchecks.io 监控 - 任务成功
            if uuid:
                try:
                    requests.get(f"{HEALTHCHECKS_BASE_URL}/{uuid}", timeout=5)
                except Exception as e:
                    self.logger.warning(f"[{self.name}] Healthchecks ping(success) 失败: {e}")
            
        except MemoryError as e:
            self.logger.error(f"[{self.name}] 内存不足: {e}")
            import gc
            gc.collect()
            self.consecutive_errors += 1
            # 先停止心跳线程，再发送fail
            heartbeat_stop.set()
            if heartbeat_thread:
                heartbeat_thread.join(timeout=2)
            # Healthchecks.io 监控 - 任务失败
            if uuid:
                try:
                    requests.get(f"{HEALTHCHECKS_BASE_URL}/{uuid}/fail", timeout=5)
                except:
                    pass
            
        except Exception as e:
            self.consecutive_errors += 1
            self.logger.error(f"[{self.name}] 运行出错 ({self.consecutive_errors}/{self.max_consecutive_errors}): {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            # 先停止心跳线程，再发送fail
            heartbeat_stop.set()
            if heartbeat_thread:
                heartbeat_thread.join(timeout=2)
            # Healthchecks.io 监控 - 任务失败
            if uuid:
                try:
                    requests.get(f"{HEALTHCHECKS_BASE_URL}/{uuid}/fail", timeout=5)
                except:
                    pass
        
        finally:
            # 停止心跳线程
            heartbeat_stop.set()
            if heartbeat_thread:
                heartbeat_thread.join(timeout=2)
            
            # 增强内存清理（参考 IEM_insert_db.py 策略）
            import gc
            import sys
            
            # 1. 清理 pandas 内部缓存（如果存在）
            try:
                if 'pandas' in sys.modules:
                    import pandas as pd
                    
                    # 清理 pandas.core.algorithms 缓存
                    if hasattr(pd.core, 'algorithms'):
                        for attr in ['_unique', '_mode', '_rank']:
                            if hasattr(pd.core.algorithms, attr):
                                try:
                                    setattr(pd.core.algorithms, attr, None)
                                except:
                                    pass
                    
                    # 清理 pandas IO 缓存
                    try:
                        import functools
                        if hasattr(pd.io.common, '_get_filepath_or_buffer'):
                            if hasattr(pd.io.common._get_filepath_or_buffer, 'cache_clear'):
                                pd.io.common._get_filepath_or_buffer.cache_clear()
                    except:
                        pass
            except:
                pass
            
            # 2. 清理 BeautifulSoup 缓存
            try:
                if 'bs4' in sys.modules:
                    from bs4 import BeautifulSoup
                    if hasattr(BeautifulSoup, 'reset'):
                        BeautifulSoup.reset()
            except:
                pass
            
            # 3. 多次垃圾回收确保彻底（参考 IEM_insert_db._force_gc）
            collected_total = 0
            for i in range(3):
                collected = gc.collect()
                if collected == 0:
                    break
                collected_total += collected
            
            # 4. 深度垃圾回收
            collected_total += gc.collect(2)
            
            # 5. 每5次运行记录一次内存清理日志
            if self.run_count % 5 == 0:
                self.logger.info(f"[{self.name}] 内存清理完成：回收 {collected_total} 个对象 (第 {self.run_count} 次运行)")
    
    def run_loop(self):
        """线程主循环 - 独立定时（带崩溃保护）"""
        self.logger.info(f"[{self.name}] 启动，间隔 {self.interval_minutes} 分钟")
        
        while self.running:
            self.run_task()
            
            if not self.running:
                break
            
            # 连续错误过多，增加等待时间（指数退避）
            if self.consecutive_errors >= self.max_consecutive_errors:
                wait_minutes = min(self.consecutive_errors * 2, 30)
                self.logger.warning(f"[{self.name}] 连续错误 {self.consecutive_errors} 次，等待 {wait_minutes} 分钟")
                for _ in range(wait_minutes * 60):
                    if not self.running:
                        break
                    time.sleep(1)
                continue
            
            next_run = datetime.now() + timedelta(minutes=self.interval_minutes)
            self.logger.info(f"[{self.name}] 下次运行: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # 分段睡眠，便于响应停止信号
            for _ in range(self.interval_minutes * 60):
                if not self.running:
                    break
                time.sleep(1)
        
        self.logger.info(f"[{self.name}] 已停止，共运行 {self.run_count} 次")
    
    def start(self):
        """启动线程"""
        self.thread = threading.Thread(target=self.run_loop, name=f"Scraper-{self.name}", daemon=True)
        self.thread.start()
        return self.thread
    
    def stop(self):
        """停止"""
        self.running = False


class NOAACombinedManager:
    
    def __init__(self):
        self.logger = setup_main_logging()
        self.runners = []
        self.running = True
        
        # 设置信号处理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """信号处理器"""
        self.logger.info(f"\n收到停止信号 ({signum})，正在关闭...")
        self.stop_all()
    
    def _get_env_bool(self, key, default=True):
        """获取环境变量布尔值"""
        value = os.environ.get(key, str(default)).lower()
        return value in ('true', '1', 'yes', 'on')
    
    def _get_env_int(self, key, default):
        """获取环境变量整数值"""
        try:
            return int(os.environ.get(key, default))
        except ValueError:
            return default
    
    def setup_scrapers(self):
        """设置所有爬虫"""
        
        # 从环境变量读取配置
        enable_txt = self._get_env_bool('ENABLE_TXT', True)
        enable_ocean = self._get_env_bool('ENABLE_OCEAN', True)
        enable_multi = self._get_env_bool('ENABLE_MULTI', True)
        
        txt_interval = self._get_env_int('TXT_INTERVAL', 2)
        ocean_interval = self._get_env_int('OCEAN_INTERVAL', 2)
        multi_interval = self._get_env_int('MULTI_INTERVAL', 2)
        
        # 最小间隔限制
        txt_interval = max(1440, txt_interval)      #一天爬一次
        ocean_interval = max(1440, ocean_interval)
        multi_interval = max(1440, multi_interval)
        
        self.logger.info("=" * 70)
        self.logger.info("  NOAA 统一爬虫管理器 v1.0")
        self.logger.info("=" * 70)
        self.logger.info("配置:")
        self.logger.info(f"  - TXT 爬虫:    {'启用' if enable_txt else '禁用'} (间隔 {txt_interval} 分钟)")
        self.logger.info(f"  - OCEAN 爬虫:  {'启用' if enable_ocean else '禁用'} (间隔 {ocean_interval} 分钟)")
        self.logger.info(f"  - 多类型爬虫: {'启用' if enable_multi else '禁用'} (间隔 {multi_interval} 分钟)")
        self.logger.info("=" * 70)
        
        # 1. TXT 数据爬虫（标准气象数据）
        if enable_txt:
            try:
                from noaa_realtime2_scraper_to_db import NOAARealtimeToDatabase as TxtScraper
                runner = ScraperRunner('TXT', TxtScraper, txt_interval, enable_partitioning=True)
                self.runners.append(runner)
                self.logger.info(f"✓ TXT 爬虫已注册 (间隔 {txt_interval} 分钟)")
            except ImportError as e:
                self.logger.error(f"✗ 无法导入 TXT 爬虫: {e}")
        
        # 2. OCEAN 数据爬虫（海洋数据）
        if enable_ocean:
            try:
                from noaa_realtime2_scraper_to_db_multi import NOAARealtimeToDatabase as OceanScraper
                runner = ScraperRunner('OCEAN', OceanScraper, ocean_interval, enable_partitioning=True)
                self.runners.append(runner)
                self.logger.info(f"✓ OCEAN 爬虫已注册 (间隔 {ocean_interval} 分钟)")
            except ImportError as e:
                self.logger.error(f"✗ 无法导入 OCEAN 爬虫: {e}")
        
        # 3. 多类型数据爬虫（DART/DRIFT/SPEC/SUPL）
        if enable_multi:
            try:
                from noaa_realtime_multi_types_scraper import NOAARealtimeMultiTypesScraper as MultiScraper
                runner = ScraperRunner('MULTI', MultiScraper, multi_interval, 
                                       data_types=['dart', 'drift', 'spec', 'supl'],
                                       enable_partitioning=True)
                self.runners.append(runner)
                self.logger.info(f"✓ 多类型爬虫已注册 (间隔 {multi_interval} 分钟)")
            except ImportError as e:
                self.logger.error(f"✗ 无法导入多类型爬虫: {e}")
        
        if not self.runners:
            self.logger.error("没有可用的爬虫，程序退出")
            sys.exit(1)
        
        self.logger.info(f"\n共注册 {len(self.runners)} 个爬虫")
    
    def run(self):
        """运行所有爬虫"""
        self.setup_scrapers()
        
        self.logger.info("\n" + "=" * 70)
        self.logger.info("启动所有爬虫 (多线程并行模式)...")
        self.logger.info("=" * 70)
        
        # 启动所有爬虫线程（并行运行）
        threads = []
        for i, runner in enumerate(self.runners):
            if i > 0:
                time.sleep(10)  # 错开60秒启动，避免同时连接数据库
            thread = runner.start()
            threads.append(thread)
            self.logger.info(f"✓ {runner.name} 线程已启动 (每 {runner.interval_minutes} 分钟)")
        
        self.logger.info("\n所有爬虫已启动，按 Ctrl+C 停止")
        self.logger.info("=" * 70 + "\n")
        
        # 主循环：监控线程状态
        try:
            while self.running:
                alive = sum(1 for t in threads if t.is_alive())
                if alive == 0:
                    self.logger.warning("所有线程已退出")
                    break
                time.sleep(5)
        except KeyboardInterrupt:
            self.logger.info("\n收到中断信号...")
        
        self.stop_all()
    
    def stop_all(self):
        """停止所有爬虫"""
        self.running = False
        self.logger.info("正在停止所有爬虫...")
        
        for runner in self.runners:
            runner.stop()
        
        for runner in self.runners:
            if runner.thread and runner.thread.is_alive():
                runner.thread.join(timeout=5)
        
        self.logger.info("\n" + "=" * 70)
        self.logger.info("✓ 所有爬虫已停止")
        self.logger.info("运行统计:")
        for runner in self.runners:
            self.logger.info(f"  - {runner.name}: 运行了 {runner.run_count} 次")
        self.logger.info("=" * 70)


def main():
    """主入口"""
    print("=" * 70)
    print("  NOAA 统一爬虫入口 v1.0")
    print("  在一个进程中并行运行三个数据爬虫")
    print("=" * 70)
    print("\n包含的爬虫：")
    print("  1. TXT 爬虫   - 标准气象数据 (.txt)")
    print("  2. OCEAN 爬虫 - 海洋数据 (.ocean)")
    print("  3. MULTI 爬虫 - DART/DRIFT/SPEC/SUPL")
    
    manager = NOAACombinedManager()
    manager.run()


if __name__ == "__main__":
    main()
