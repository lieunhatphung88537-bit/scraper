FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=Asia/Shanghai
#国内镜像网站，本地测试可用
RUN sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list.d/debian.sources 2>/dev/null || sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list 2>/dev/null || true
# 一次性安装所有依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    tzdata \
    wget \
    curl \
    xvfb \
    chromium \
    chromium-driver \
    gdal-bin \
    libgdal-dev \
    libspatialindex-dev \
    && rm -rf /var/lib/apt/lists/*


WORKDIR /app

# 安装 Python 依赖
COPY requirements.txt .
#RUN pip install --no-cache-dir -r requirements.txt
# 加上清华源，防止下载超时
#RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
# 拷贝全部代码
#COPY . .

# 显式复制IEM站点配置文件（确保存在）
#COPY all_IEM.txt .

# 创建必要的目录
RUN mkdir -p logs data/airport skyvector/metar skyvector/taf synop_test

# 使用多数据库配置系统
# db_config.py 会自动导入 db_config_multi.py
# 各爬虫通过 DATABASES 字典访问对应数据库

# 默认入口是 python，运行时指定具体脚本
# 用法示例:
# docker run scrapers noaa_combined_scraper.py      # NOAA海洋数据
# docker run scrapers baowen_combined_scraper.py    # 气象报文数据
#ENTRYPOINT ["python"]
#CMD ["--help"]
