# -*- coding: utf-8 -*-
"""
数据库数据清理脚本
功能：
1. 清理数据库中超过指定时间的旧数据
2. 清理前先将数据导出为CSV保存到服务器
3. 从服务器拉取CSV到本地
"""
import os
import sys
import argparse
import logging
from logging.handlers import RotatingFileHandler
import time
from datetime import datetime, timedelta
from pathlib import Path
import psycopg2
import paramiko
from scp import SCPClient
import schedule
from ftplib import FTP
log_handler = RotatingFileHandler(
    'data_cleanup.log',
    maxBytes=10*1024*1024,  
    backupCount=5,           
    encoding='utf-8'
)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        log_handler,
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
# ====== 配置区 ======
# 运行模式: 'local' = 本地测试（连接本地数据库，上传到服务器）
#          'server' = 服务器部署（连接本地数据库，推送到客户端）
RUN_MODE = 'server'

# 定时任务配置
SCHEDULE_INTERVAL = 60 # 定时间隔（小时或天数）
SCHEDULE_UNIT = 'days'  # 'hours' = 小时, 'days' = 天
SCHEDULE_TIME = '02:00'  # 执行时间（仅当SCHEDULE_UNIT='days'时有效）
SCHEDULE_CLEANUP_DAYS = 60 # 清理多少天前的数据，保留最近2个月（60天）
#数据库配置
DB_CONFIG = {
    'host': 'database-1.cxik82g8267p.us-east-2.rds.amazonaws.com',
    'port': 5432,
    'database': 'noaa_data',
    'user': 'postgres',
    'password': 'oceantest1'
}

# ===== 传输方式配置 =====
# 'smb' = Windows共享文件夹, 'ssh' = SSH/SCP传输, 'ftp' = FTP传输
TRANSFER_MODE = 'ssh'

# SSH配置（连接EC2服务器）
SSH_CONFIG = {
    'host': '3.145.18.61',  # EC2公网IP
    'port': 22,
    'username': 'ec2-user',
    'key_file': 'C:/Users/13352/.ssh/ec2_key'  # SSH私钥文件路径
}

# 服务器上CSV保存路径（SSH模式用）
SERVER_CSV_DIR = '/home/ec2-user/backup_csv'

# SMB共享路径配置（如果使用SMB方式）
SMB_SERVER_SHARE = r'\\192.168.10.26\noaa_backup'
SMB_CLIENT_SHARE = r'\\192.168.10.140\backup_csv'
SMB_SHARE_PATH = SMB_SERVER_SHARE if RUN_MODE == 'local' else SMB_CLIENT_SHARE

# 本地CSV保存路径（脚本所在目录下）
LOCAL_CSV_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backup_csv')

# 需要清理的表及其时间字段（非分区表）
TABLES_CONFIG = {
    #'typhoon_history': 'update_time',  # 测试用
    # 'txt_data_copy': 'observation_time',
    # 'ocean_data_copy': 'observation_time',
    # 'dart_data_copy': 'observation_time',
    # 'drift_data_copy': 'observation_time',
    # 'spec_data_copy': 'observation_time',
     #'supl_data': 'observation_time'
}

# ====== 多数据库配置 ======
# 亚马逊RDS数据库配置
# 数据库1: noaa_data（默认）
DB_CONFIG_NOAA = {
    'host': 'database-1.cxik82g8267p.us-east-2.rds.amazonaws.com',
    'port': 5432,
    'database': 'noaa_data',
    'user': 'postgres',
    'password': 'oceantest1'
}

# 数据库3: metar_taf_synop_data（ogimet/skyvector数据）
DB_CONFIG_METAR_TAF = {
    'host': 'database-1.cxik82g8267p.us-east-2.rds.amazonaws.com',
    'port': 5432,
    'database': 'metar_taf_synop_data',
    'user': 'postgres',
    'password': 'oceantest1'
}

# 数据库4: typhoon_data（台风数据）
DB_CONFIG_TYPHOON = {
    'host': 'database-1.cxik82g8267p.us-east-2.rds.amazonaws.com',
    'port': 5432,
    'database': 'typhoon_data',
    'user': 'postgres',
    'password': 'oceantest1'
}

# ====== 分区表配置 ======
# 分区类型: 'ten_days' = 10天一分区, 'monthly' = 1月一分区, 'yearly' = 1年一分区
# db_config: 指定使用哪个数据库配置

PARTITION_TABLES_CONFIG = {
    # ===== 十天分区表（metar_taf_synop_data 库）=====
     'awc_metar_data': {'partition_type': 'ten_days', 'time_column': 'observation_time', 'db_config': 'metar_taf'},
     'iem_metar_data': {'partition_type': 'ten_days', 'time_column': 'observation_time', 'db_config': 'metar_taf'},
     'nws_metar_data': {'partition_type': 'ten_days', 'time_column': 'observation_time', 'db_config': 'metar_taf'},
    
    # ===== 十天分区表（metar_taf_synop_data 库）=====
     'ogimet_metar_data': {'partition_type': 'ten_days', 'time_column': 'observation_time', 'db_config': 'metar_taf'},
     'ogimet_synop_data': {'partition_type': 'ten_days', 'time_column': 'observation_time', 'db_config': 'metar_taf'},
     'skyvector_metar_data': {'partition_type': 'ten_days', 'time_column': 'observation_time', 'db_config': 'metar_taf'},
     'skyvector_taf_data': {'partition_type': 'ten_days', 'time_column': 'observation_time', 'db_config': 'metar_taf'},
    
    # ===== 十天分区表（noaa_data 库）=====
     'txt_data': {'partition_type': 'ten_days', 'time_column': 'observation_time', 'db_config': 'noaa'},
    
    # ===== 月分区表（metar_taf_synop_data 库）=====
     'ogimet_taf_data': {'partition_type': 'monthly', 'time_column': 'observation_time', 'db_config': 'metar_taf'},
    
    # ===== 月分区表（noaa_data 库）=====
     'ocean_data': {'partition_type': 'monthly', 'time_column': 'observation_time', 'db_config': 'noaa'},
     'spec_data': {'partition_type': 'monthly', 'time_column': 'observation_time', 'db_config': 'noaa'},
     'supl_data': {'partition_type': 'monthly', 'time_column': 'observation_time', 'db_config': 'noaa'},
     'dart_data': {'partition_type': 'monthly', 'time_column': 'observation_time', 'db_config': 'noaa'},
    
    # ===== 年分区表（typhoon_data 库，格式：table_2025）=====
    # 'typhoon_forecast': {'partition_type': 'yearly', 'time_column': 'update_time', 'db_config': 'typhoon'},
    # 'typhoon_history': {'partition_type': 'yearly', 'time_column': 'update_time', 'db_config': 'typhoon'},
    # 'typhoon_info': {'partition_type': 'yearly', 'time_column': 'update_time', 'db_config': 'typhoon'},
    # 'raodong': {'partition_type': 'yearly', 'time_column': 'observation_time', 'db_config': 'typhoon'},
    
    # ===== 年分区表（noaa_data 库）=====
    # 'drift_data': {'partition_type': 'yearly', 'time_column': 'observation_time', 'db_config': 'noaa'},
}

# 数据库配置映射
DB_CONFIG_MAP = {
    'noaa': DB_CONFIG_NOAA,
    'metar_taf': DB_CONFIG_METAR_TAF,
    'typhoon': DB_CONFIG_TYPHOON,
}

# 分区起始日期（用于计算10天分区）
PARTITION_START_DATE = datetime(2025, 1, 1)

def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info(f"数据库连接成功: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
        return conn
    except Exception as e:
        logger.error(f"数据库连接失败: {e}")
        raise


def get_partitions_to_cleanup(conn, base_table, partition_type, keep_count):
    """
    获取需要清理的分区列表（基于分区数量）
    
    Args:
        conn: 数据库连接
        base_table: 基础表名（如 ogimet_metar_data_copy）
        partition_type: 分区类型 ('ten_days', 'monthly', 'yearly')
        keep_count: 保留最近多少个分区
    
    Returns:
        list: 需要清理的分区表名列表
    """
    cursor = conn.cursor()
    
    # 查询所有匹配的分区表
    cursor.execute("""
        SELECT tablename FROM pg_tables 
        WHERE schemaname = 'public' AND tablename LIKE %s
        ORDER BY tablename
    """, (f"{base_table}_%",))
    
    all_partitions = [row[0] for row in cursor.fetchall()]
    cursor.close()
    
    # 过滤有效的分区表名
    valid_partitions = []
    for partition_name in all_partitions:
        suffix = partition_name.replace(f"{base_table}_", "")
        
        try:
            if partition_type == 'ten_days':
                # 格式: 20251225_20260103（17位）
                if '_' in suffix and len(suffix) == 17:
                    valid_partitions.append(partition_name)
            
            elif partition_type == 'monthly':
                # 格式: 202512（6位）
                if len(suffix) == 6 and suffix.isdigit():
                    valid_partitions.append(partition_name)
            
            elif partition_type == 'yearly':
                # 格式: 2025（4位）
                if len(suffix) == 4 and suffix.isdigit():
                    valid_partitions.append(partition_name)
                    
        except (ValueError, IndexError) as e:
            logger.warning(f"无法解析分区名 {partition_name}: {e}")
            continue
    
    # 按分区名排序（从旧到新）
    valid_partitions.sort()
    
    # 计算需要删除的分区（保留最近keep_count个）
    if len(valid_partitions) <= keep_count:
        return []
    
    partitions_to_drop = valid_partitions[:-keep_count]
    
    return partitions_to_drop


def export_partition_to_csv(conn, partition_name, local_csv_path):
    """
    将整个分区表导出为CSV
    """
    import csv
    
    cursor = conn.cursor()
    cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = %s ORDER BY ordinal_position", (partition_name,))
    columns = [row[0] for row in cursor.fetchall()]
    
    if not columns:
        logger.warning(f"分区表 {partition_name} 没有列信息，跳过导出")
        cursor.close()
        return False
    
    cursor.execute(f'SELECT * FROM {partition_name}')
    
    with open(local_csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        batch_size = 10000
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            writer.writerows(rows)
    
    cursor.close()
    logger.info(f"分区已导出: {partition_name} -> {local_csv_path}")
    return True


def drop_partition(conn, partition_name, dry_run=False):
    """
    删除（DROP）分区表
    
    Args:
        conn: 数据库连接
        partition_name: 分区表名
        dry_run: 是否仅预览
    
    Returns:
        bool: 是否成功
    """
    if dry_run:
        logger.info(f"[DRY RUN] 将删除分区: {partition_name}")
        return True
    
    cursor = conn.cursor()
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {partition_name}")
        conn.commit()
        logger.info(f"✓ 已删除分区: {partition_name}")
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"✗ 删除分区失败 {partition_name}: {e}")
        return False
    finally:
        cursor.close()


def cleanup_partition_table(conn, base_table, config, keep_count, dry_run=False):
    """
    清理分区表 - 导出并删除旧分区（保留最近N个分区）
    
    Args:
        conn: 数据库连接
        base_table: 基础表名
        config: 分区配置 {'partition_type': 'ten_days'/'monthly'/'yearly', 'time_column': 'xxx'}
        keep_count: 保留最近多少个分区
        dry_run: 是否仅预览
    
    Returns:
        dict: 清理结果
    """
    result = {
        'table': base_table,
        'partitions_found': 0,
        'partitions_exported': 0,
        'partitions_dropped': 0,
        'csv_paths': []
    }
    
    partition_type = config.get('partition_type', 'ten_days')
    
    # 获取需要清理的分区（保留最近keep_count个）
    partitions = get_partitions_to_cleanup(conn, base_table, partition_type, keep_count)
    result['partitions_found'] = len(partitions)
    
    if not partitions:
        logger.info(f"{base_table}: 没有需要清理的分区")
        return result
    
    logger.info(f"{base_table}: 发现 {len(partitions)} 个待清理分区")
    for p in partitions:
        logger.info(f"  - {p}")
    
    if dry_run:
        logger.info(f"[DRY RUN] {base_table}: 将清理 {len(partitions)} 个分区")
        return result
    
    # 确保本地目录存在
    Path(LOCAL_CSV_DIR).mkdir(parents=True, exist_ok=True)
    
    for partition_name in partitions:
        # 1. 导出分区（使用原分区表名作为CSV文件名）
        csv_filename = f"{partition_name}.csv"
        local_csv_path = os.path.join(LOCAL_CSV_DIR, csv_filename)
        
        exported = export_partition_to_csv(conn, partition_name, local_csv_path)
        if exported:
            result['partitions_exported'] += 1
            result['csv_paths'].append(local_csv_path)
            
            # 2. 删除分区
            dropped = drop_partition(conn, partition_name, dry_run)
            if dropped:
                result['partitions_dropped'] += 1
        else:
            logger.warning(f"跳过删除 {partition_name}（导出失败）")
    
    logger.info(f"{base_table}: 清理完成 - 导出 {result['partitions_exported']}, 删除 {result['partitions_dropped']}")
    return result


def ensure_remote_dir(ssh, remote_dir):
    stdin, stdout, stderr = ssh.exec_command(f'mkdir -p {remote_dir}')
    stdout.read()  
    logger.info(f"确保远程目录存在: {remote_dir}")


def count_old_data(conn, table_name, time_column, cutoff_time):
    cursor = conn.cursor()
    query = f'SELECT COUNT(*) FROM {table_name} WHERE "{time_column}" < %s'
    cursor.execute(query, (cutoff_time,))
    count = cursor.fetchone()[0]
    cursor.close()
    return count


def get_earliest_time(conn, table_name, time_column):
    cursor = conn.cursor()
    query = f'SELECT MIN("{time_column}") FROM {table_name}'
    cursor.execute(query)
    result = cursor.fetchone()[0]
    cursor.close()
    return result


def export_to_csv_on_server(conn, table_name, time_column, cutoff_time, server_csv_path):
    """
    将旧数据导出为CSV（在服务器上执行COPY命令）
    注意：COPY TO 需要superuser权限或pg_write_server_files角色
    """
    cursor = conn.cursor()
    
    # 使用 COPY 命令导出数据到服务器文件
    # 方案1：直接COPY TO（需要服务器写权限）
    query = f"""
    COPY (
        SELECT * FROM {table_name} 
        WHERE "{time_column}" < %s
    ) TO %s WITH CSV HEADER;
    """
    
    try:
        cursor.execute(query, (cutoff_time, server_csv_path))
        conn.commit()
        logger.info(f"数据已导出到服务器: {server_csv_path}")
        return True
    except psycopg2.Error as e:
        conn.rollback()
        logger.warning(f"COPY TO 失败（可能权限不足）: {e}")
        return export_to_csv_via_python(conn, table_name, time_column, cutoff_time, server_csv_path)
    finally:
        cursor.close()


def export_to_csv_via_python(conn, table_name, time_column, cutoff_time, local_csv_path):
    """
    方案2：通过Python读取数据并写入本地CSV
    如果COPY TO权限不足，使用此方法
    """
    import csv
    
    cursor = conn.cursor()
    cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = %s ORDER BY ordinal_position", (table_name,))
    columns = [row[0] for row in cursor.fetchall()]
    query = f'SELECT * FROM {table_name} WHERE "{time_column}" < %s'
    cursor.execute(query, (cutoff_time,))
    with open(local_csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(columns)  
        batch_size = 10000
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            writer.writerows(rows)
    cursor.close()
    logger.info(f"数据已导出到本地: {local_csv_path}")
    return True


def delete_old_data(conn, table_name, time_column, cutoff_time, dry_run=False):
    cursor = conn.cursor()
    if dry_run:
        logger.info(f"[DRY RUN] 将删除 {table_name} 中 {time_column} < {cutoff_time} 的数据")
        cursor.close()
        return 0
    query = f'DELETE FROM {table_name} WHERE "{time_column}" < %s'
    cursor.execute(query, (cutoff_time,))
    deleted_count = cursor.rowcount
    conn.commit()
    cursor.close()
    logger.info(f"已删除 {table_name} 中 {deleted_count} 条数据")
    return deleted_count


def download_csv_from_server(ssh, remote_path, local_path):
    try:
        with SCPClient(ssh.get_transport()) as scp:
            scp.get(remote_path, local_path)
        logger.info(f"已下载: {remote_path} -> {local_path}")
        return True
    except Exception as e:
        logger.error(f"下载失败: {e}")
        return False


def cleanup_table(conn, ssh, table_name, time_column, cutoff_time, dry_run=False):
    """
    清理单个表的数据
    
    Args:
        conn: 数据库连接
        ssh: SSH连接（如果需要从服务器拉取）
        table_name: 表名
        time_column: 时间字段名
        cutoff_time: 截止时间
        dry_run: 是否仅预览
    
    Returns:
        dict: 清理结果统计
    """
    result = {
        'table': table_name,
        'count': 0,
        'exported': False,
        'deleted': False,
        'csv_path': None }
    count = count_old_data(conn, table_name, time_column, cutoff_time)
    result['count'] = count
    
    if count == 0:
        logger.info(f"{table_name}: 没有需要清理的数据")
        return result
    
    logger.info(f"{table_name}: 发现 {count} 条待清理数据 (时间 < {cutoff_time})")
    
    if dry_run:
        logger.info(f"[DRY RUN] {table_name}: 将清理 {count} 条数据")
        return result
    # 使用原表名作为CSV文件名
    csv_filename = f"{table_name}.csv"
    # 3. 导出数据到CSV
    # 优先尝试服务器端COPY，失败则使用本地导出
    Path(LOCAL_CSV_DIR).mkdir(parents=True, exist_ok=True)
    local_csv_path = os.path.join(LOCAL_CSV_DIR, csv_filename)
    # 直接使用Python导出到本地（更可靠）
    exported = export_to_csv_via_python(conn, table_name, time_column, cutoff_time, local_csv_path)
    if exported:
        result['exported'] = True
        result['csv_path'] = local_csv_path
        logger.info(f"{table_name}: CSV导出成功 -> {local_csv_path}")
    else:
        logger.error(f"{table_name}: CSV导出失败，跳过删除操作")
        return result
    deleted_count = delete_old_data(conn, table_name, time_column, cutoff_time, dry_run)
    result['deleted'] = deleted_count > 0
    return result


def cleanup_all_tables(days_ago=0, hours_ago=0, tables=None, dry_run=False):
    """
    清理所有配置的表 - 保留最近N天/N小时的数据，删除更早的数据
    支持普通表（DELETE）和分区表（DROP PARTITION）
    
    Args:
        days_ago: 保留最近多少天的数据（删除更早的）
        hours_ago: 保留最近多少小时的数据（删除更早的）
        tables: 指定要清理的表（None表示全部）
        dry_run: 是否仅预览不执行
    """
    time_desc = f"{days_ago}天{hours_ago}小时" if days_ago else f"{hours_ago}小时"
    logger.info(f"=" * 60)
    logger.info(f"开始数据清理任务")
    logger.info(f"清理规则: 保留最近 {time_desc} 的数据，删除更早的数据")
    logger.info(f"模式: {'预览模式 (DRY RUN)' if dry_run else '执行模式'}")
    logger.info(f"=" * 60)
    results = []
    partition_results = []
    conn = None
    ssh = None
    try:
        conn = get_db_connection()
        
        # ===== 1. 清理普通表（使用 DELETE）=====
        tables_to_clean = tables or list(TABLES_CONFIG.keys())
        for table_name in tables_to_clean:
            if table_name not in TABLES_CONFIG:
                logger.warning(f"未知的表: {table_name}，跳过")
                continue
            time_column = TABLES_CONFIG[table_name]
            # 获取表中最早的时间（用于日志显示）
            earliest_time = get_earliest_time(conn, table_name, time_column)
            if earliest_time is None:
                logger.info(f"{table_name}: 表中没有数据，跳过")
                continue   
            # 计算截止时间 = 当前时间 - N天/N小时（保留最近N天）
            cutoff_time = datetime.now() - timedelta(days=days_ago, hours=hours_ago)
            logger.info(f"{table_name}: 最早时间={earliest_time}, 截止时间={cutoff_time} (保留{days_ago}天)")
            
            result = cleanup_table(conn, ssh, table_name, time_column, cutoff_time, dry_run)
            results.append(result)
        
        # ===== 2. 清理分区表（使用 DROP TABLE）=====
        if PARTITION_TABLES_CONFIG:
            logger.info(f"\n{'='*60}")
            logger.info("开始清理分区表（基于分区数量）...")
            logger.info(f"{'='*60}")
            
            # 按数据库分组处理分区表
            db_connections = {}  # 缓存数据库连接
            try:
                for base_table, config in PARTITION_TABLES_CONFIG.items():
                    if tables and base_table not in tables:
                        continue
                    
                    # 获取或创建对应数据库的连接
                    db_key = config.get('db_config', 'noaa')
                    if db_key not in db_connections:
                        db_cfg = DB_CONFIG_MAP.get(db_key, DB_CONFIG)
                        try:
                            db_connections[db_key] = psycopg2.connect(**db_cfg)
                            logger.info(f"连接数据库: {db_cfg['database']}")
                        except Exception as e:
                            logger.error(f"连接数据库 {db_cfg['database']} 失败: {e}")
                            continue
                    
                    # 根据分区类型确定保留分区数量
                    partition_type = config.get('partition_type', 'ten_days')
                    if partition_type == 'ten_days':
                        keep_count = 6  # 保留最近6个10天分区（60天）
                        keep_desc = "6个分区（60天）"
                    elif partition_type == 'monthly':
                        keep_count = 2  # 保留最近2个月分区（2个月）
                        keep_desc = "2个分区（2个月）"
                    elif partition_type == 'yearly':
                        keep_count = 2  # 保留最近2年分区
                        keep_desc = "2个分区（2年）"
                    else:
                        keep_count = 2  # 默认保留2个分区
                        keep_desc = "2个分区"
                    
                    partition_conn = db_connections[db_key]
                    logger.info(f"\n处理分区表: {base_table} (类型: {partition_type}, 保留: {keep_desc})")
                    logger.info(f"  数据库: {DB_CONFIG_MAP.get(db_key, DB_CONFIG)['database']}")
                    result = cleanup_partition_table(partition_conn, base_table, config, keep_count, dry_run)
                    partition_results.append(result)
            finally:
                # 关闭所有分区表数据库连接
                for db_key, db_conn in db_connections.items():
                    try:
                        db_conn.close()
                        logger.info(f"关闭数据库连接: {db_key}")
                    except:
                        pass
        logger.info(f"\n" + "=" * 60)
        logger.info("清理结果汇总:")
        logger.info("-" * 60)
        total_count = 0
        csv_files = []  # 收集本次生成的CSV文件
        
        # 普通表结果
        if results:
            logger.info("【普通表】")
            for r in results:
                status = "✓ 已清理" if r['deleted'] else ("⊘ 无数据" if r['count'] == 0 else "✗ 未执行")
                logger.info(f"  {r['table']:25} | {r['count']:>10} 条 | {status}")
                total_count += r['count']
                if r.get('csv_path'):
                    csv_files.append(r['csv_path'])
        
        # 分区表结果
        if partition_results:
            logger.info("【分区表】")
            total_partitions = 0
            for r in partition_results:
                status = f"✓ 删除 {r['partitions_dropped']} 个分区" if r['partitions_dropped'] > 0 else ("⊘ 无分区" if r['partitions_found'] == 0 else "✗ 未执行")
                logger.info(f"  {r['table']:25} | {r['partitions_found']:>5} 个分区 | {status}")
                total_partitions += r['partitions_dropped']
                csv_files.extend(r.get('csv_paths', []))
            logger.info(f"  分区表总计: 删除 {total_partitions} 个分区")
        
        logger.info("-" * 60)
        logger.info(f"普通表总计: {total_count} 条数据")
        logger.info(f"CSV备份目录: {os.path.abspath(LOCAL_CSV_DIR)}")
        logger.info("=" * 60)
        if csv_files and not dry_run:
            logger.info(f"\nCSV文件已保存到本地: {LOCAL_CSV_DIR}")
            logger.info(f"共 {len(csv_files)} 个文件")
            for f in csv_files:
                logger.info(f"  - {os.path.basename(f)}")
        return results + partition_results       
    finally:
        if conn:
            conn.close()
            logger.info("数据库连接已关闭")
        if ssh:
            ssh.close()
            logger.info("SSH连接已关闭")

def download_csv_from_remote(remote_pattern='*.csv', delete_after=False):
    """
    从服务器下载CSV文件到本地，可选择下载后删除服务器文件
    支持SMB、SSH和FTP三种方式
    
    Args:
        remote_pattern: 远程文件匹配模式
        delete_after: 下载后是否删除服务器上的文件
    """
    logger.info(f"传输方式: {TRANSFER_MODE.upper()}")
    
    if TRANSFER_MODE == 'smb':
        download_via_smb(remote_pattern, delete_after)
    elif TRANSFER_MODE == 'ssh':
        download_via_ssh(remote_pattern, delete_after)
    elif TRANSFER_MODE == 'ftp':
        download_via_ftp(remote_pattern, delete_after)


def download_via_smb(remote_pattern='*.csv'):
    """通过SMB共享文件夹下载"""
    import shutil
    import glob
    
    try:
        if not os.path.exists(SMB_SHARE_PATH):
            logger.error(f"无法访问共享路径: {SMB_SHARE_PATH}")
            logger.info("请先运行: net use \\\\192.168.10.26\\noaa_backup /user:Administrator Hthj150328")
            return
        remote_files = list(Path(SMB_SHARE_PATH).glob(remote_pattern))
        if not remote_files:
            logger.info(f"服务器上没有找到CSV文件: {SMB_SHARE_PATH}\\{remote_pattern}")
            return
        logger.info(f"发现 {len(remote_files)} 个CSV文件")
        Path(LOCAL_CSV_DIR).mkdir(parents=True, exist_ok=True)
        success_count = 0
        for remote_file in remote_files:
            local_file = os.path.join(LOCAL_CSV_DIR, remote_file.name)
            logger.info(f"下载: {remote_file.name}")
            shutil.copy2(str(remote_file), local_file)
            logger.info(f"  -> {local_file}")
            success_count += 1
        logger.info(f"下载完成，共 {success_count} 个文件")
        logger.info(f"本地目录: {LOCAL_CSV_DIR}")
    except Exception as e:
        logger.error(f"SMB下载失败: {e}")


def upload_specific_files(file_paths):
    """
    上传指定的CSV文件到服务器（用于cleanup后自动上传）
    
    Args:
        file_paths: CSV文件路径列表
    """
    import shutil
    
    if not file_paths:
        logger.info("没有需要上传的文件")
        return
    
    logger.info(f"传输方式: {TRANSFER_MODE.upper()}")
    files = [Path(f) for f in file_paths]
    
    if TRANSFER_MODE == 'smb':
        upload_via_smb(files)
    elif TRANSFER_MODE == 'ssh':
        upload_via_ssh(files)
    elif TRANSFER_MODE == 'ftp':
        upload_via_ftp(files)


def upload_csv_to_remote():
    """
    上传本地CSV文件到远程服务器（上传目录下所有CSV）
    支持SMB（Windows共享）和SSH两种方式
    """
    import shutil
    if not os.path.exists(LOCAL_CSV_DIR):
        logger.error(f"本地目录不存在: {LOCAL_CSV_DIR}")
        return
    
    local_files = list(Path(LOCAL_CSV_DIR).glob('*.csv'))
    if not local_files:
        logger.error(f"本地目录没有CSV文件: {LOCAL_CSV_DIR}")
        return
    
    logger.info(f"发现 {len(local_files)} 个本地CSV文件")
    logger.info(f"传输方式: {TRANSFER_MODE.upper()}")
    
    if TRANSFER_MODE == 'smb':
        upload_via_smb(local_files)
    elif TRANSFER_MODE == 'ssh':
        upload_via_ssh(local_files)


def upload_via_smb(local_files):
    """通过SMB共享文件夹上传"""
    import shutil
    try:
        if not os.path.exists(SMB_SHARE_PATH):
            logger.error(f"无法访问共享路径: {SMB_SHARE_PATH}")
            logger.info("请确保:")
            logger.info("  1. 服务器已共享文件夹")
            logger.info("  2. 共享名正确")
            logger.info("  3. 有访问权限")
            logger.info(f"  当前配置: SMB_SHARE_PATH = '{SMB_SHARE_PATH}'")
            return
        
        logger.info(f"共享路径可访问: {SMB_SHARE_PATH}")
        success_count = 0
        for local_file in local_files:
            remote_file = os.path.join(SMB_SHARE_PATH, local_file.name)
            logger.info(f"复制: {local_file.name}")
            shutil.copy2(str(local_file), remote_file)
            logger.info(f"  -> {remote_file}")
            success_count += 1
        logger.info(f"上传完成，共 {success_count} 个文件")
        logger.info(f"远程目录: {SMB_SHARE_PATH}")
        
    except PermissionError as e:
        logger.error(f"权限不足: {e}")
        logger.info("请检查共享文件夹的访问权限")
    except Exception as e:
        logger.error(f"SMB上传失败: {e}")


def upload_via_ssh(local_files):
    """通过SSH/SCP上传文件到EC2服务器"""
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        logger.info(f"连接到EC2服务器: {SSH_CONFIG['host']}")
        ssh.connect(
            hostname=SSH_CONFIG['host'],
            port=SSH_CONFIG['port'],
            username=SSH_CONFIG['username'],
            key_filename=SSH_CONFIG['key_file']
        )
        logger.info("SSH连接成功")
        
        # 确保服务器目录存在
        stdin, stdout, stderr = ssh.exec_command(f"mkdir -p {SERVER_CSV_DIR}")
        stdout.channel.recv_exit_status()
        
        # 使用SCP上传文件
        with SCPClient(ssh.get_transport()) as scp:
            success_count = 0
            for local_file in local_files:
                remote_path = f"{SERVER_CSV_DIR}/{local_file.name}"
                logger.info(f"上传: {local_file.name}")
                scp.put(str(local_file), remote_path)
                logger.info(f"  -> {remote_path}")
                success_count += 1
        
        logger.info(f"上传完成，共 {success_count} 个文件")
        logger.info(f"服务器目录: {SERVER_CSV_DIR}")
        ssh.close()
        
    except Exception as e:
        logger.error(f"SSH上传失败: {e}")
        if 'ssh' in locals():
            ssh.close()


def download_via_ssh(remote_pattern='*.csv', delete_after=False):
    """通过SSH/SCP从EC2服务器下载文件"""
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        logger.info(f"连接到EC2服务器: {SSH_CONFIG['host']}")
        ssh.connect(
            hostname=SSH_CONFIG['host'],
            port=SSH_CONFIG['port'],
            username=SSH_CONFIG['username'],
            key_filename=SSH_CONFIG['key_file']
        )
        logger.info("SSH连接成功")
        
        # 列出服务器上的CSV文件
        cmd = f"ls {SERVER_CSV_DIR}/{remote_pattern} 2>/dev/null || echo 'NO_FILES'"
        stdin, stdout, stderr = ssh.exec_command(cmd)
        output = stdout.read().decode().strip()
        
        if output == 'NO_FILES' or not output:
            logger.info(f"服务器上没有找到CSV文件: {SERVER_CSV_DIR}/{remote_pattern}")
            ssh.close()
            return
        
        remote_files = output.split('\n')
        logger.info(f"发现 {len(remote_files)} 个CSV文件")
        
        # 创建本地目录
        Path(LOCAL_CSV_DIR).mkdir(parents=True, exist_ok=True)
        
        # 使用SCP下载文件
        with SCPClient(ssh.get_transport()) as scp:
            success_count = 0
            for remote_file in remote_files:
                filename = os.path.basename(remote_file)
                local_path = os.path.join(LOCAL_CSV_DIR, filename)
                logger.info(f"下载: {filename}")
                scp.get(remote_file, local_path)
                logger.info(f"  -> {local_path}")
                success_count += 1
                
                # 如果指定删除，则删除服务器文件
                if delete_after:
                    stdin, stdout, stderr = ssh.exec_command(f"rm -f {remote_file}")
                    stdout.channel.recv_exit_status()
                    logger.info(f"  已删除服务器文件: {remote_file}")
        
        logger.info(f"下载完成，共 {success_count} 个文件")
        logger.info(f"本地目录: {LOCAL_CSV_DIR}")
        
        if delete_after:
            logger.info(f"服务器文件已全部删除")
        
        ssh.close()
        
    except Exception as e:
        logger.error(f"SSH下载失败: {e}")
        if 'ssh' in locals():
            ssh.close()


def download_via_ftp(remote_pattern='*.csv', delete_after=False):
    """通过FTP下载CSV文件"""
    try:
        logger.info(f"连接到FTP服务器: {FTP_CONFIG['host']}:{FTP_CONFIG['port']}")
        ftp = FTP()
        ftp.connect(FTP_CONFIG['host'], FTP_CONFIG['port'])
        ftp.login(FTP_CONFIG['username'], FTP_CONFIG['password'])
        ftp.cwd(FTP_CONFIG['remote_dir'])
        logger.info("FTP连接成功")
        
        # 列出远程文件
        remote_files = []
        ftp.dir(lambda x: remote_files.append(x.split()[-1]))
        
        # 过滤CSV文件
        import fnmatch
        csv_files = [f for f in remote_files if fnmatch.fnmatch(f, remote_pattern)]
        
        if not csv_files:
            logger.info(f"FTP服务器上没有找到CSV文件: {FTP_CONFIG['remote_dir']}/{remote_pattern}")
            ftp.quit()
            return
        
        logger.info(f"发现 {len(csv_files)} 个CSV文件")
        
        # 创建本地目录
        Path(LOCAL_CSV_DIR).mkdir(parents=True, exist_ok=True)
        
        success_count = 0
        for filename in csv_files:
            local_path = os.path.join(LOCAL_CSV_DIR, filename)
            logger.info(f"下载: {filename}")
            
            with open(local_path, 'wb') as f:
                ftp.retrbinary(f'RETR {filename}', f.write)
            
            file_size = os.path.getsize(local_path)
            size_mb = file_size / (1024 * 1024)
            logger.info(f"  -> {local_path} ({size_mb:.2f} MB)")
            success_count += 1
            
            # 如果指定删除，则删除服务器文件
            if delete_after:
                ftp.delete(filename)
                logger.info(f"  已删除FTP服务器文件: {filename}")
        
        logger.info(f"下载完成，共 {success_count} 个文件")
        logger.info(f"本地目录: {LOCAL_CSV_DIR}")
        
        if delete_after:
            logger.info(f"FTP服务器文件已全部删除")
        
        ftp.quit()
        
    except Exception as e:
        logger.error(f"FTP下载失败: {e}")
        if 'ftp' in locals():
            try:
                ftp.quit()
            except:
                pass


def upload_via_ftp(local_files):
    """通过FTP上传CSV文件"""
    try:
        logger.info(f"连接到FTP服务器: {FTP_CONFIG['host']}:{FTP_CONFIG['port']}")
        ftp = FTP()
        ftp.connect(FTP_CONFIG['host'], FTP_CONFIG['port'])
        ftp.login(FTP_CONFIG['username'], FTP_CONFIG['password'])
        
        # 切换到远程目录（如果不存在则创建）
        try:
            ftp.cwd(FTP_CONFIG['remote_dir'])
        except:
            logger.info(f"远程目录不存在，尝试创建: {FTP_CONFIG['remote_dir']}")
            # 逐级创建目录
            parts = FTP_CONFIG['remote_dir'].strip('/').split('/')
            for part in parts:
                try:
                    ftp.cwd(part)
                except:
                    ftp.mkd(part)
                    ftp.cwd(part)
        
        logger.info("FTP连接成功")
        
        success_count = 0
        for local_file in local_files:
            logger.info(f"上传: {local_file.name}")
            
            with open(local_file, 'rb') as f:
                ftp.storbinary(f'STOR {local_file.name}', f)
            
            file_size = os.path.getsize(local_file)
            size_mb = file_size / (1024 * 1024)
            logger.info(f"  -> {FTP_CONFIG['remote_dir']}/{local_file.name} ({size_mb:.2f} MB)")
            success_count += 1
        
        logger.info(f"上传完成，共 {success_count} 个文件")
        logger.info(f"FTP服务器: {FTP_CONFIG['host']}:{FTP_CONFIG['port']}/{FTP_CONFIG['remote_dir']}")
        
        ftp.quit()
        
    except Exception as e:
        logger.error(f"FTP上传失败: {e}")
        if 'ftp' in locals():
            try:
                ftp.quit()
            except:
                pass


def cleanup_remote_logs(days_to_keep=7, log_dir='/home/ec2-user/scraper-logs'):
    """
    清理EC2服务器上的旧日志文件
    
    Args:
        days_to_keep: 保留最近多少天的日志
        log_dir: 服务器日志目录路径
    """
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        logger.info(f"连接到EC2服务器: {SSH_CONFIG['host']}")
        ssh.connect(
            hostname=SSH_CONFIG['host'],
            port=SSH_CONFIG['port'],
            username=SSH_CONFIG['username'],
            key_filename=SSH_CONFIG['key_file']
        )
        logger.info("SSH连接成功")
        
        # 查看日志文件
        logger.info(f"检查日志目录: {log_dir}")
        stdin, stdout, stderr = ssh.exec_command(f"ls -lh {log_dir} 2>/dev/null || echo 'NO_DIR'")
        output = stdout.read().decode().strip()
        
        if output == 'NO_DIR':
            logger.warning(f"日志目录不存在: {log_dir}")
            ssh.close()
            return
        
        logger.info(f"当前日志文件:\n{output}")
        
        # 删除N天前的日志文件
        cmd = f"find {log_dir} -name '*.log' -type f -mtime +{days_to_keep} -delete"
        logger.info(f"\n执行清理命令: {cmd}")
        stdin, stdout, stderr = ssh.exec_command(cmd)
        stdout.channel.recv_exit_status()
        
        # 显示清理后的状态
        stdin, stdout, stderr = ssh.exec_command(f"du -sh {log_dir}")
        size = stdout.read().decode().strip()
        logger.info(f"清理完成，当前目录大小: {size}")
        
        # 显示保留的文件
        stdin, stdout, stderr = ssh.exec_command(f"ls -lh {log_dir}/*.log 2>/dev/null | wc -l")
        count = stdout.read().decode().strip()
        logger.info(f"保留的日志文件数量: {count}")
        
        ssh.close()
        logger.info(f"已删除 {days_to_keep} 天前的日志文件")
        
    except Exception as e:
        logger.error(f"清理远程日志失败: {e}")
        if 'ssh' in locals():
            ssh.close()


def scheduled_cleanup():
    logger.info("\n" + "="*60)
    logger.info("定时任务触发：开始执行数据库清理")
    logger.info(f"普通表清理规则：保留最近 {SCHEDULE_CLEANUP_DAYS} 天的数据（约{SCHEDULE_CLEANUP_DAYS//30}个月）")
    logger.info("分区表清理规则：十天分区保留6个（60天），月分区保留2个（2个月）")
    logger.info("="*60 + "\n")
    try:
        cleanup_all_tables(days_ago=SCHEDULE_CLEANUP_DAYS, dry_run=False)
        logger.info("\n定时任务执行完成")
    except Exception as e:
        logger.error(f"定时任务执行失败: {e}", exc_info=True)


def run_scheduler():
    """
    启动定时调度器（持续运行）
    """
    logger.info("="*60)
    logger.info("数据库清理定时服务启动")
    
    if SCHEDULE_UNIT == 'hours':
        logger.info(f"配置：每 {SCHEDULE_INTERVAL} 小时执行一次")
        schedule.every(SCHEDULE_INTERVAL).hours.do(scheduled_cleanup)
    else:
        logger.info(f"配置：每 {SCHEDULE_INTERVAL} 天执行一次，时间：{SCHEDULE_TIME}")
        schedule.every(SCHEDULE_INTERVAL).days.at(SCHEDULE_TIME).do(scheduled_cleanup)
    
    logger.info(f"普通表清理规则：保留最近 {SCHEDULE_CLEANUP_DAYS} 天的数据（约{SCHEDULE_CLEANUP_DAYS//30}个月）")
    logger.info("分区表清理规则：十天分区保留6个（60天），月分区保留2个（2个月）")
    logger.info("按 Ctrl+C 停止服务")
    logger.info("="*60)
    next_run = schedule.next_run()
    logger.info(f"\n下次执行时间: {next_run}\n")
    wait_time = f"{SCHEDULE_INTERVAL}{'小时' if SCHEDULE_UNIT == 'hours' else '天'}"
    logger.info(f"是否立即执行一次清理？(跳过等待{wait_time})")
    logger.info("输入 'y' 立即执行，其他任意键跳过（10秒后自动跳过）")
    import threading
    user_input = [None]
    def get_input():
        try:
            user_input[0] = input("> ").strip().lower()
        except:
            pass
    
    input_thread = threading.Thread(target=get_input, daemon=True)
    input_thread.start()
    input_thread.join(timeout=10)
    
    if user_input[0] == 'y':
        logger.info("\n立即执行清理任务...\n")
        scheduled_cleanup()
    else:
        logger.info("\n跳过立即执行，等待定时触发\n")
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # 每分钟检查一次
    except KeyboardInterrupt:
        logger.info("\n定时服务已停止")


def main():
    parser = argparse.ArgumentParser(description='数据库数据清理工具')
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    cleanup_parser = subparsers.add_parser('cleanup', help='清理数据库中的旧数据')
    cleanup_parser.add_argument('--days', type=int, default=0, 
                               help='清理多少天前的数据')
    cleanup_parser.add_argument('--hours', type=int, default=0, 
                               help='清理多少小时前的数据')
    cleanup_parser.add_argument('--tables', nargs='+', 
                               help='指定要清理的表 (默认: 全部)')
    cleanup_parser.add_argument('--dry-run', action='store_true',
                               help='仅预览，不实际执行删除')

    download_parser = subparsers.add_parser('download', help='从服务器下载CSV备份')
    download_parser.add_argument('--pattern', default='*.csv',
                                help='文件匹配模式 (默认: *.csv)')
    download_parser.add_argument('--delete', action='store_true',
                                help='下载后删除服务器上的CSV文件')
    
    upload_parser = subparsers.add_parser('upload', help='上传本地CSV到服务器')
    
    test_ssh_parser = subparsers.add_parser('test-ssh', help='测试SSH连接')
    
    stats_parser = subparsers.add_parser('stats', help='查看各表数据统计')
    
    cleanup_logs_parser = subparsers.add_parser('cleanup-logs', help='清理EC2服务器上的旧日志文件')
    cleanup_logs_parser.add_argument('--days', type=int, default=7,
                                    help='保留最近多少天的日志 (默认: 7天)')
    cleanup_logs_parser.add_argument('--log-dir', default='/home/ec2-user/scraper-logs',
                                    help='服务器日志目录路径')
    
    scheduler_parser = subparsers.add_parser('scheduler', help='启动定时调度服务（按配置自动清理）')
    scheduler_parser.add_argument('--now', action='store_true',
                                 help='立即执行一次清理，然后开始定时')

    args = parser.parse_args()
    
    if args.command == 'cleanup':
        if args.days == 0 and args.hours == 0:
            logger.error("请指定 --days 或 --hours 参数")
            return
        cleanup_all_tables(
            days_ago=args.days,
            hours_ago=args.hours,
            tables=args.tables,
            dry_run=args.dry_run )
    
    elif args.command == 'download':
        download_csv_from_remote(args.pattern, delete_after=args.delete)
    
    elif args.command == 'upload':
        upload_csv_to_remote()
    
    elif args.command == 'cleanup-logs':
        cleanup_remote_logs(days_to_keep=args.days, log_dir=args.log_dir)
    
    elif args.command == 'stats':
        show_table_stats()
    
    elif args.command == 'scheduler':
        if args.now:
            logger.info("立即执行一次清理...\n")
            scheduled_cleanup()
        run_scheduler()
    
    else:
        parser.print_help()


def show_table_stats():
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        logger.info("\n" + "=" * 80)
        logger.info("数据表统计")
        logger.info("=" * 80)
        
        for table_name, time_column in TABLES_CONFIG.items():
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total = cursor.fetchone()[0]
            cursor.execute(f'SELECT MIN("{time_column}"), MAX("{time_column}") FROM {table_name}')
            min_time, max_time = cursor.fetchone()
            now = datetime.now()
            stats = {}
            for days in [7, 30, 90, 180, 365]:
                cutoff = now - timedelta(days=days)
                cursor.execute(f'SELECT COUNT(*) FROM {table_name} WHERE "{time_column}" < %s', (cutoff,))
                stats[days] = cursor.fetchone()[0]
            logger.info(f"\n{table_name}:")
            logger.info(f"  总数据量: {total:,} 条")
            logger.info(f"  时间范围: {min_time} ~ {max_time}")
            logger.info(f"  可清理数据:")
            for days, count in stats.items():
                pct = (count / total * 100) if total > 0 else 0
                logger.info(f"    > {days:3} 天前: {count:>12,} 条 ({pct:.1f}%)")
        cursor.close()
        logger.info("\n" + "=" * 80)
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    # 示例用法：
    # 1. 查看统计: python data_cleanup_server.py stats
    # 2. 预览清理: python data_cleanup_server.py cleanup --days 60 --dry-run
    # 3. 执行清理: python data_cleanup_server.py cleanup --days 60
    # 4. 清理指定表: python data_cleanup_server.py cleanup --days 60 --tables txt_data_copy ocean_data_copy
    # 5. 启动定时服务: python data_cleanup_server.py scheduler
    # 6. 启动定时服务并立即执行一次: python data_cleanup_server.py scheduler --now
    # 
    # 定时配置（脚本顶部修改）：
    #   SCHEDULE_INTERVAL = 30       # 间隔数量
    #   SCHEDULE_UNIT = 'days'       # 'hours'=小时测试, 'days'=天数正式
    #   SCHEDULE_CLEANUP_DAYS = 60   # 保留最近多少天的数据（60天=2个月）
    #
    # 清理规则说明：
    #   普通表：基于时间清理，保留最近 N 天的数据（删除早于截止时间的数据）
    #   分区表：基于分区数量清理
    #     - 十天分区（ten_days）：保留最近 6 个分区（约60天）
    #     - 月分区（monthly）：保留最近 2 个分区（2个月）
    #     - 年分区（yearly）：保留最近 2 个分区（2年）
    main()
