# -*- coding: utf-8 -*-
"""
从EC2下载CSV文件到本地（下载后删除EC2上的文件）
用法:
    python download_csv_from_ec2.py              # 下载并删除
    python download_csv_from_ec2.py --keep       # 仅下载不删除
    python download_csv_from_ec2.py --schedule   # 启动定时下载服务
"""
import os
import sys
import argparse
import logging
from pathlib import Path
import paramiko
from scp import SCPClient
import schedule
import time
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ====== 配置 ======
EC2_CONFIG = {
    'host': '3.145.18.61',
    'port': 22,
    'username': 'ec2-user',
    'key_file': 'C:/Users/13352/.ssh/ec2_key'
}

REMOTE_CSV_DIR = '/home/ec2-user/backup_csv/backup_csv'
LOCAL_CSV_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backup_csv')

# 定时配置
SCHEDULE_INTERVAL = 60  # 每60天执行一次
SCHEDULE_TIME = '03:00'  # 执行时间


def get_ssh_client():
    """创建SSH连接"""
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    logger.info(f"连接EC2: {EC2_CONFIG['host']}")
    ssh.connect(
        hostname=EC2_CONFIG['host'],
        port=EC2_CONFIG['port'],
        username=EC2_CONFIG['username'],
        key_filename=EC2_CONFIG['key_file']
    )
    logger.info("SSH连接成功")
    return ssh


def list_remote_csv(ssh):
    """列出EC2上的CSV文件"""
    stdin, stdout, stderr = ssh.exec_command(f"ls {REMOTE_CSV_DIR}/*.csv 2>/dev/null")
    output = stdout.read().decode().strip()
    if not output:
        return []
    return output.split('\n')


def download_csv_files(keep_remote=False):
    """
    下载EC2上的CSV文件到本地
    
    Args:
        keep_remote: 是否保留EC2上的文件（默认删除）
    """
    logger.info("=" * 50)
    logger.info("开始从EC2下载CSV文件")
    logger.info(f"远程目录: {REMOTE_CSV_DIR}")
    logger.info(f"本地目录: {LOCAL_CSV_DIR}")
    logger.info(f"下载后删除远程文件: {not keep_remote}")
    logger.info("=" * 50)
    
    ssh = None
    try:
        ssh = get_ssh_client()
        
        # 列出远程CSV文件
        remote_files = list_remote_csv(ssh)
        if not remote_files:
            logger.info("EC2上没有CSV文件需要下载")
            return
        
        logger.info(f"发现 {len(remote_files)} 个CSV文件:")
        for f in remote_files:
            logger.info(f"  - {os.path.basename(f)}")
        
        # 确保本地目录存在
        Path(LOCAL_CSV_DIR).mkdir(parents=True, exist_ok=True)
        
        # 下载文件
        logger.info("\n开始下载...")
        with SCPClient(ssh.get_transport()) as scp:
            for remote_path in remote_files:
                filename = os.path.basename(remote_path)
                local_path = os.path.join(LOCAL_CSV_DIR, filename)
                
                logger.info(f"下载: {filename}")
                scp.get(remote_path, local_path)
                
                # 显示文件大小
                size_mb = os.path.getsize(local_path) / (1024 * 1024)
                logger.info(f"  -> {local_path} ({size_mb:.2f} MB)")
        
        logger.info(f"\n下载完成! 共 {len(remote_files)} 个文件")
        
        # 删除远程文件
        if not keep_remote:
            logger.info("\n删除EC2上的CSV文件...")
            stdin, stdout, stderr = ssh.exec_command(f"rm -f {REMOTE_CSV_DIR}/*.csv")
            stdout.channel.recv_exit_status()
            logger.info("EC2上的CSV文件已删除")
        
        # 显示本地文件列表
        logger.info("\n本地文件:")
        for f in Path(LOCAL_CSV_DIR).glob('*.csv'):
            size_mb = f.stat().st_size / (1024 * 1024)
            logger.info(f"  {f.name} ({size_mb:.2f} MB)")
        
    except Exception as e:
        logger.error(f"下载失败: {e}")
    finally:
        if ssh:
            ssh.close()
            logger.info("SSH连接已关闭")


def scheduled_download():
    """定时任务触发的下载"""
    logger.info(f"\n{'='*50}")
    logger.info(f"定时任务触发: {datetime.now()}")
    logger.info(f"{'='*50}\n")
    download_csv_files(keep_remote=False)


def run_scheduler():
    """启动定时下载服务"""
    logger.info("=" * 50)
    logger.info("CSV定时下载服务启动")
    logger.info(f"配置: 每 {SCHEDULE_INTERVAL} 天执行一次，时间: {SCHEDULE_TIME}")
    logger.info("按 Ctrl+C 停止服务")
    logger.info("=" * 50)
    
    schedule.every(SCHEDULE_INTERVAL).days.at(SCHEDULE_TIME).do(scheduled_download)
    
    next_run = schedule.next_run()
    logger.info(f"下次执行时间: {next_run}")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("\n定时服务已停止")


def main():
    parser = argparse.ArgumentParser(description='从EC2下载CSV备份文件')
    parser.add_argument('--keep', action='store_true', 
                       help='下载后保留EC2上的文件（默认删除）')
    parser.add_argument('--schedule', action='store_true',
                       help='启动定时下载服务')
    
    args = parser.parse_args()
    
    if args.schedule:
        run_scheduler()
    else:
        download_csv_files(keep_remote=args.keep)


if __name__ == '__main__':
    main()
