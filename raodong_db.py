import os
import json
import time
import zipfile
import warnings
import requests
from pathlib import Path
from tempfile import TemporaryDirectory
from psycopg2.extras import RealDictCursor
from datetime import datetime
import geopandas as gpd
import psycopg2
import csv
import schedule
from psycopg2.extras import execute_values
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/data_insert_raodong.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class NHCProcessorDB:
    recent_files = []
    def __init__(self, output_dir="NHC_raodong", db_config=None):
        self.output_dir = output_dir
        Path(self.output_dir).mkdir(exist_ok=True)

        warnings.filterwarnings(
            "ignore",
            message="GeoDataFrame's CRS is not representable in URN OGC format"
        )

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

        self._create_table_if_not_exists()

    def _create_table_if_not_exists(self):
        """创建数据库表（如果不存在）"""
        create_sql = """
        CREATE TABLE IF NOT EXISTS raodong (
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
        cursor.execute(create_sql)
        conn.commit()
        cursor.close()
        conn.close()

    def drop_table(self):
        """删除 raodong 表（谨慎操作，会清空所有数据）"""
        confirm = input("你确定要删除 raodong 表吗？此操作不可恢复！(yes/no): ")
        if confirm.lower() != 'yes':
            logging.info("操作已取消。")
            return

        drop_sql = "DROP TABLE IF EXISTS raodong;"
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor()
        cursor.execute(drop_sql)
        conn.commit()
        cursor.close()
        conn.close()
        logging.info("表 raodong 已被删除。")

    def save_json_file_to_db(self, json_file_path):
        """将本地 JSON 文件写入 raodong 数据库表"""
        if not os.path.exists(json_file_path):
            logging.error(f"文件不存在: {json_file_path}")
            return

        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # 每条记录增加 create_time 和 update_time
        utc_now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        for item in data:
            item.setdefault('id', '-999')
            item.setdefault('updateTime', utc_now)
            item.setdefault('BASIN', None)
            item.setdefault('PROB2DAY', None)
            item.setdefault('PROB7DAY', None)
            item.setdefault('coordinates', [])

        # 写入数据库
        insert_sql = """
        INSERT INTO raodong (id, updateTime, BASIN, PROB2DAY, PROB7DAY, coordinates, create_time, update_time)
        VALUES %s
        """
        values = [
            (
                item['id'],
                item['updateTime'],
                item['BASIN'],
                item['PROB2DAY'],
                item['PROB7DAY'],
                json.dumps(item['coordinates']),
                utc_now,
                utc_now
            )
            for item in data
        ]

        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor()
        execute_values(cursor, insert_sql, values)
        conn.commit()
        cursor.close()
        conn.close()
        logging.info(f"已将 {len(data)} 条记录从 {json_file_path} 写入数据库 raodong")

    def save_data(self, json_data):
        """转换 JSON 数据，返回本地保存路径和数据列表"""
        results = []
        time_str = json_data['file_name'].split('_')[-1]
        for item in json_data['features']:
            dt = datetime.strptime(time_str, '%Y%m%d%H%M')
            result = {
                'id': item.get('id', '-999'),
                'updateTime': dt.strftime('%Y-%m-%d %H:%M:%S'),
                'BASIN': item['properties'].get('BASIN'),
                'PROB2DAY': item['properties'].get('PROB2DAY'),
                'PROB7DAY': item['properties'].get('PROB7DAY'),
                'coordinates': item['geometry'].get('coordinates', []),
            }
            results.append(result)

        output_filename = f"{json_data['file_name']}.json"
        output_path = os.path.join(self.output_dir, output_filename)



        return output_path, results

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
        """保存 JSON 数据到 PostgreSQL"""
        insert_sql = """
        INSERT INTO raodong (id, updateTime, BASIN, PROB2DAY, PROB7DAY, coordinates, create_time, update_time)
        VALUES %s
        """
        utc_now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        values = [
            (
                item['id'],
                item['updateTime'],
                item['BASIN'],
                item['PROB2DAY'],
                item['PROB7DAY'],
                json.dumps(item['coordinates']),
                utc_now,
                utc_now
            )
            for item in data
        ]

        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor()
        execute_values(cursor, insert_sql, values)
        conn.commit()
        cursor.close()
        conn.close()
        logging.info(f"已保存 {len(data)} 条记录到数据库 raodong")

    def export_to_csv(self, csv_path=None):
        """将数据库 raodong 表的数据导出到本地 CSV"""
        csv_path = csv_path or os.path.join(self.output_dir, "raodong_export.csv")
        Path(self.output_dir).mkdir(exist_ok=True)

        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SELECT * FROM raodong ORDER BY raodong_id;")
        rows = cursor.fetchall()

        if not rows:
            logging.error("数据库中没有数据")
            cursor.close()
            conn.close()
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

        cursor.close()
        conn.close()
        logging.info(f"已将 {len(rows)} 条记录导出到 CSV: {csv_path}")
        return csv_path

    def main_task(self):
        logging.info(f"任务开始执行: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        url = "https://www.nhc.noaa.gov/xgtwo/gtwo_shapefiles.zip"
        zip_path, tmp_dir_obj = self.download_zip(url)
        if zip_path:
            json_data = self.parse_and_get_geojson(zip_path)
            if json_data:
                file_name = json_data['file_name']

                # 检查是否在最近处理列表中
                if file_name in NHCProcessorDB.recent_files:
                    logging.info(f"文件 {file_name} 已处理过，跳过。")
                else:
                    output_path, raodong_data = self.save_data(json_data)
                    self.save_to_db(raodong_data)

                    # 更新最近处理列表
                    NHCProcessorDB.recent_files.append(file_name)
                    if len(NHCProcessorDB.recent_files) > 3:
                        NHCProcessorDB.recent_files.pop(0)
                    logging.info(f"更新最近处理文件列表: {NHCProcessorDB.recent_files}")

            tmp_dir_obj.cleanup()
        logging.info(f"本次任务结束: {time.strftime('%Y-%m-%d %H:%M:%S')}")

def run_task():
    try:
        logging.info("开始执行NHC数据处理任务...")
        processor.main_task()
        logging.info("NHC数据处理任务执行完成")
    except Exception as e:
        logging.error(f"执行任务时发生错误: {e}")

if __name__ == "__main__":
    processor = NHCProcessorDB()
    """directory = Path("NHC_raodong")

    for json_file in directory.glob("*.json"):
        processor.save_json_file_to_db(str(json_file))"""

    # 每30分钟执行一次
    schedule.every(30).minutes.do(run_task)

    # 立即执行一次
    run_task()

    logging.info("调度器已启动，每30分钟执行一次任务")
    while True:
        schedule.run_pending()
        time.sleep(1)
        #processor.export_to_csv()
