# -*- coding: utf-8 -*-
"""
气象报文数据统一爬虫入口 v2.0
======================================================================================
在一个进程中并行运行六个气象报文爬虫，每个爬虫有独立的定时任务。

包含的爬虫：
1. OGIMET METAR/TAF - 机场气象报文
2. OGIMET SYNOP - 地面观测报文
3. SkyVector - 全球METAR/TAF报文
4. IEM METAR - Iowa Environmental Mesonet 报文
5. AWC METAR - Aviation Weather Center 报文
6. NWS METAR - National Weather Service 报文

运行方式：
- Docker: python baowen_combined_scraper.py
- 本地: python baowen_combined_scraper.py

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
    'OGIMET': 'c07ef118-953a-4cf7-b9e4-35549f7ea62b',     # OGIMET METAR/TAF爬虫（填入UUID）
    'SYNOP': '6d7b28b2-be34-46b4-af99-6ad05b40c05c',      # SYNOP爬虫（填入UUID）
    'SKYVECTOR': '287235d2-954b-4d06-8aec-3d6e1daf1bc8',  # SkyVector爬虫（填入UUID）
    'IEM': '23d84d74-8bea-4aeb-aa06-1572b9a8063d',        # IEM爬虫（填入UUID）
    'AWC': 'e77b3f72-95b1-4f2d-a06c-1ec9eeabf118',        # AWC爬虫（填入UUID）
    'NWS': '43567a37-cb75-40ff-82d9-e807434fa9c9',        # NWS爬虫（填入UUID）
}
HEALTHCHECKS_BASE_URL = 'https://hc-ping.com'
HEARTBEAT_INTERVAL = 120  # 心跳间隔（秒），任务运行期间每60秒发送一次


def setup_main_logging():
    """设置主程序日志"""
    import sys
    script_dir = os.path.dirname(os.path.abspath(__file__))
    local_log_dir = os.path.join(script_dir, 'logs')
    if sys.platform == 'win32':
        log_dir = local_log_dir
    else:
        log_dir = '/logs' if os.path.exists('/logs') and os.path.isdir('/logs') else local_log_dir
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, 'baowen_combined.log')
    logger = logging.getLogger('baowen_combined')
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        class FlushFileHandler(logging.FileHandler):
            def emit(self, record):
                super().emit(record)
                self.flush()
        file_handler = FlushFileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(logging.Formatter('%(asctime)s - [%(name)s] - %(levelname)s - %(message)s'))
        logger.addHandler(file_handler)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter('%(asctime)s - [%(name)s] - %(levelname)s - %(message)s'))
        logger.addHandler(console_handler)
    
    return logger


class ScraperRunner:
    """爬虫运行器 - 多线程独立运行"""
    
    def __init__(self, name, run_func, interval_minutes, **kwargs):
        self.name = name
        self.run_func = run_func
        self.interval_minutes = interval_minutes
        self.kwargs = kwargs
        self.run_count = 0
        self.consecutive_errors = 0
        self.max_consecutive_errors = 5
        self.logger = logging.getLogger('baowen_combined')
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
        
        # 添加开始时间标记
        start_time = time.time()
        try:
            self.run_func(**self.kwargs)
            elapsed = time.time() - start_time
            self.logger.info(f"[{self.name}] 执行完成，耗时 {elapsed:.1f} 秒")
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
            # 停止心跳线程（确保关闭）
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



def run_ogimet_metar_taf():
    """运行 OGIMET METAR/TAF 爬虫"""
    import logging
    import gc
    from ogimet_scraper import OgimetScraper
    scraper = OgimetScraper(delay=0.5, save_format='db', max_workers=8, use_database=True)
    try:
        scraper.scrape_all(use_multithreading=True)
    finally:
        scraper.close()
        del scraper
        gc.collect()


# def run_ogimet_synop():
#     """运行 OGIMET SYNOP 爬虫"""
#     import logging
#     import gc
    
#     from ogimet_synop_scraper import SynopScraper
#     scraper = SynopScraper(delay=0.2, max_workers=5, use_database=True)
#     try:
#         scraper.scrape_all(use_multithreading=True)
#     finally:
#         scraper.close()
#         del scraper
#         gc.collect()


def run_skyvector():
    import logging
    import gc
    
    from skyvector_scraper import run_scheduled_scrape
    try:
        run_scheduled_scrape(use_database=True, use_parallel=True, max_workers=4)
    finally:
        gc.collect()


def run_iem():
    """运行 IEM METAR 爬虫"""
    import logging
    import gc
    
    from IEM_insert_db import IEM_METAR
    logger = logging.getLogger('baowen_combined')
    try:
        iem = IEM_METAR()
        if not iem.connect():
            logger.error("[IEM] 数据库连接失败")
            return
        try:
            iem.process_stations()
        finally:
            iem.close()
            del iem
            gc.collect()
    except Exception as e:
        logger.error(f"[IEM] 爬虫运行异常: {e}", exc_info=True)


def run_awc():
    """运行 AWC METAR 爬虫"""
    import logging
    import gc
    
    from AWC_insert_db import AWC_METAR
    logger = logging.getLogger('baowen_combined')
    try:
        awc = AWC_METAR()
        if not awc.connect():
            logger.error("[AWC] 数据库连接失败")
            return
        try:
            awc.download_and_save_data()
        finally:
            awc.close()
            del awc
            gc.collect()
    except Exception as e:
        logger.error(f"[AWC] 爬虫运行异常: {e}", exc_info=True)


def run_nws():
    """运行 NWS METAR 爬虫"""
    import logging
    import gc
    
    from NWS_insert_db import NWS_METAR
    logger = logging.getLogger('baowen_combined')
    try:
        nws = NWS_METAR()
        if not nws.connect():
            logger.error("[NWS] 数据库连接失败")
            return
        try:
            nws.process_all_stations()
        finally:
            nws.close()
            del nws
            gc.collect()
    except Exception as e:
        logger.error(f"[NWS] 爬虫运行异常: {e}", exc_info=True)


class OgimetCombinedManager:
    """OGIMET 爬虫统一管理器"""
    
    def __init__(self):
        self.logger = setup_main_logging()
        self.runners = []
        self.running = True
        
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        self.logger.info(f"\n收到停止信号 ({signum})，正在关闭...")
        self.stop_all()
    
    def _get_env_bool(self, key, default=True):
        value = os.environ.get(key, str(default)).lower()
        return value in ('true', '1', 'yes', 'on')
    
    def _get_env_int(self, key, default):
        try:
            return int(os.environ.get(key, default))
        except ValueError:
            return default
    
    def setup_scrapers(self):
        """设置所有爬虫"""
        
        # 原有爬虫开关
        enable_ogimet = self._get_env_bool('ENABLE_OGIMET', True)
        #enable_synop = self._get_env_bool('ENABLE_SYNOP', True)
        enable_synop = False
        enable_skyvector = self._get_env_bool('ENABLE_SKYVECTOR', True)
        # 新增爬虫开关
        enable_iem = self._get_env_bool('ENABLE_IEM', True)
        enable_awc = self._get_env_bool('ENABLE_AWC', True)
        enable_nws = self._get_env_bool('ENABLE_NWS', True)
        
        # 原有爬虫间隔
        ogimet_interval = self._get_env_int('OGIMET_INTERVAL', 10)
        synop_interval = self._get_env_int('SYNOP_INTERVAL', 10)
        skyvector_interval = self._get_env_int('SKYVECTOR_INTERVAL', 3)
        # 新增爬虫间隔
        iem_interval = self._get_env_int('IEM_INTERVAL', 20)
        awc_interval = self._get_env_int('AWC_INTERVAL', 1)  # AWC 默认1分钟
        nws_interval = self._get_env_int('NWS_INTERVAL', 10)
        
        # 最小间隔限制
        ogimet_interval = max(10, ogimet_interval)
        synop_interval = max(10, synop_interval)
        skyvector_interval = max(10, skyvector_interval)
        iem_interval = max(5, iem_interval)
        awc_interval = max(1, awc_interval)
        nws_interval = max(5, nws_interval)
        
        self.logger.info("=" * 70)
        self.logger.info("  气象数据统一爬虫管理器 v2.0")
        self.logger.info("=" * 70)
        self.logger.info("配置:")
        self.logger.info(f"  - OGIMET METAR/TAF: {'启用' if enable_ogimet else '禁用'} (间隔 {ogimet_interval} 分钟)")
        self.logger.info(f"  - OGIMET SYNOP:     {'启用' if enable_synop else '禁用'} (间隔 {synop_interval} 分钟)")
        self.logger.info(f"  - SkyVector:        {'启用' if enable_skyvector else '禁用'} (间隔 {skyvector_interval} 分钟)")
        self.logger.info(f"  - IEM METAR:        {'启用' if enable_iem else '禁用'} (间隔 {iem_interval} 分钟)")
        self.logger.info(f"  - AWC METAR:        {'启用' if enable_awc else '禁用'} (间隔 {awc_interval} 分钟)")
        self.logger.info(f"  - NWS METAR:        {'启用' if enable_nws else '禁用'} (间隔 {nws_interval} 分钟)")
        self.logger.info("=" * 70)
        
        # 1. OGIMET METAR/TAF 爬虫
        if enable_ogimet:
            try:
                from ogimet_scraper import OgimetScraper
                runner = ScraperRunner('OGIMET', run_ogimet_metar_taf, ogimet_interval)
                self.runners.append(runner)
                self.logger.info(f"✓ OGIMET METAR/TAF 爬虫已注册 (间隔 {ogimet_interval} 分钟)")
            except ImportError as e:
                self.logger.error(f"✗ 无法导入 OGIMET 爬虫: {e}")
        
        # # 2. OGIMET SYNOP 爬虫
        # if enable_synop:
        #     try:
        #         from ogimet_synop_scraper import SynopScraper
        #         runner = ScraperRunner('SYNOP', run_ogimet_synop, synop_interval)
        #         self.runners.append(runner)
        #         self.logger.info(f"✓ OGIMET SYNOP 爬虫已注册 (间隔 {synop_interval} 分钟)")
        #     except ImportError as e:
        #         self.logger.error(f"✗ 无法导入 SYNOP 爬虫: {e}")
        
        # 3. SkyVector 爬虫
        if enable_skyvector:
            try:
                from skyvector_scraper import run_scheduled_scrape
                runner = ScraperRunner('SKYVECTOR', run_skyvector, skyvector_interval)
                self.runners.append(runner)
                self.logger.info(f"✓ SkyVector 爬虫已注册 (间隔 {skyvector_interval} 分钟)")
            except ImportError as e:
                self.logger.error(f"✗ 无法导入 SkyVector 爬虫: {e}")
        
        # 4. IEM METAR 爬虫
        if enable_iem:
            try:
                from IEM_insert_db import IEM_METAR
                runner = ScraperRunner('IEM', run_iem, iem_interval)
                self.runners.append(runner)
                self.logger.info(f"✓ IEM METAR 爬虫已注册 (间隔 {iem_interval} 分钟)")
            except ImportError as e:
                self.logger.error(f"✗ 无法导入 IEM 爬虫: {e}")
        
        # 5. AWC METAR 爬虫
        if enable_awc:
            try:
                from AWC_insert_db import AWC_METAR
                runner = ScraperRunner('AWC', run_awc, awc_interval)
                self.runners.append(runner)
                self.logger.info(f"✓ AWC METAR 爬虫已注册 (间隔 {awc_interval} 分钟)")
            except ImportError as e:
                self.logger.error(f"✗ 无法导入 AWC 爬虫: {e}")
        
        # 6. NWS METAR 爬虫
        if enable_nws:
            try:
                from NWS_insert_db import NWS_METAR
                runner = ScraperRunner('NWS', run_nws, nws_interval)
                self.runners.append(runner)
                self.logger.info(f"✓ NWS METAR 爬虫已注册 (间隔 {nws_interval} 分钟)")
            except ImportError as e:
                self.logger.error(f"✗ 无法导入 NWS 爬虫: {e}")
        
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
                time.sleep(3)  # 错开启动
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
    print("  气象报文数据统一爬虫 v2.0")
    print("  在一个进程中并行运行六个报文爬虫")
    print("=" * 70)
    print("\n包含的爬虫：")
    print("  1. OGIMET 爬虫   - 机场METAR/TAF报文")
    print("  2. SYNOP 爬虫    - 地面观测报文")
    print("  3. SkyVector 爬虫 - 全球METAR/TAF报文")
    print("  4. IEM 爬虫      - IEM METAR报文")
    print("  5. AWC 爬虫      - AWC METAR报文")
    print("  6. NWS 爬虫      - NWS METAR报文")
    print("=" * 70 + "\n")
    
    manager = OgimetCombinedManager()
    manager.run()


if __name__ == "__main__":
    main()
