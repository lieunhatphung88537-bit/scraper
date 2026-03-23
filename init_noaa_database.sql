-- NOAA数据库初始化脚本
-- 使用方法: psql -U postgres -f init_database.sql

-- 创建数据库（如果需要）
-- CREATE DATABASE noaa_data;

-- 连接到数据库
\c noaa_data;

-- 创建站点详情表
CREATE TABLE IF NOT EXISTS stations (
    station_id BIGINT PRIMARY KEY,          -- 递增主键
    name VARCHAR(10) UNIQUE NOT NULL,       -- 站点名称（如13002, 41001等）
    latitude DECIMAL(10, 3),                -- 纬度
    longitude DECIMAL(10, 3)                -- 经度
);

-- 创建站点名称的唯一索引
CREATE UNIQUE INDEX IF NOT EXISTS idx_stations_name ON stations(name);

-- 添加表注释
COMMENT ON TABLE stations IS '气象站点详情表';
COMMENT ON COLUMN stations.station_id IS '站点ID（11位随机数字主键）';
COMMENT ON COLUMN stations.name IS '站点名称（如13002, 41001等）';
COMMENT ON COLUMN stations.latitude IS '纬度（度，精确到3位小数）';
COMMENT ON COLUMN stations.longitude IS '经度（度，精确到3位小数）';
COMMENT ON COLUMN stations.location_description IS '位置描述信息';

-- 创建观测数据表（使用NOAA原始列名）
CREATE TABLE IF NOT EXISTS txt_data (
    id BIGSERIAL PRIMARY KEY,
    station_name VARCHAR(10) NOT NULL,
    observation_time TIMESTAMP,
    "WDIR" REAL,                        -- 风向 (度) Wind Direction（统一使用REAL）
    "WSPD" REAL,                        -- 风速 (m/s) Wind Speed
    "GST" REAL,                         -- 阵风速度 (m/s) Gust
    "WVHT" REAL,                        -- 有效波高 (m) Wave Height
    "DPD" REAL,                         -- 主导波周期 (秒) Dominant Wave Period
    "APD" REAL,                         -- 平均波周期 (秒) Average Wave Period
    "MWD" REAL,                         -- 平均波向 (度) Mean Wave Direction（统一使用REAL）
    "PRES" REAL,                        -- 气压 (hPa) Pressure
    "ATMP" REAL,                        -- 气温 (°C) Air Temperature
    "WTMP" REAL,                        -- 水温 (°C) Water Temperature
    "DEWP" REAL,                        -- 露点温度 (°C) Dewpoint Temperature
    "VIS" REAL,                         -- 能见度 (海里) Visibility
    "PTDY" REAL,                        -- 气压趋势 (hPa) Pressure Tendency
    "TIDE" REAL,                        -- 潮位 (ft) Tide
    "WSPD10M" REAL,                     -- 10米高度风速 (m/s) Wind Speed at 10 meters
    "WSPD20M" REAL,                     -- 20米高度风速 (m/s) Wind Speed at 20 meters
    first_crawl_time TIMESTAMP NOT NULL,         -- 首次抓取时间（UTC，由Python提供）
    update_time TIMESTAMP NOT NULL,         -- 最后更新时间（UTC，由Python提供）
    update_count INTEGER DEFAULT 1,     -- 更新次数
    create_time TIMESTAMP NOT NULL,  -- 入库时间（UTC，由Python提供）
    FOREIGN KEY (station_name) REFERENCES stations(name) ON DELETE CASCADE ON UPDATE CASCADE,
    UNIQUE (station_name, observation_time)
);


-- 创建索引
CREATE INDEX IF NOT EXISTS idx_txt_station_name 
    ON txt_data(station_name);
CREATE INDEX IF NOT EXISTS idx_txt_observation_time 
    ON txt_data(observation_time);
    
-- 创建海洋观测数据表
CREATE TABLE IF NOT EXISTS ocean_data (
    id BIGSERIAL PRIMARY KEY,
    station_name VARCHAR(10) NOT NULL,
    observation_time TIMESTAMP,
    "DEPTH" REAL,                       -- 深度 (m) Depth
    "OTMP" REAL,                        -- 海洋温度 (°C) Ocean Temperature
    "COND" REAL,                        -- 电导率 (S/m) Conductivity
    "SAL" REAL,                         -- 盐度 (PSU) Salinity
    "O2%" REAL,                         -- 溶解氧百分比 (%) Dissolved Oxygen Percent
    "O2PPM" REAL,                       -- 溶解氧浓度 (ppm) Dissolved Oxygen PPM
    "CLCON" REAL,                       -- 氯离子浓度 Chloride Concentration
    "TURB" REAL,                        -- 浊度 (NTU) Turbidity
    "PH" REAL,                          -- pH值 pH
    "EH" REAL,                          -- 氧化还原电位 (mV) Redox Potential
    first_crawl_time TIMESTAMP NOT NULL,         -- 首次抓取时间（UTC，由Python提供）
    update_time TIMESTAMP NOT NULL,              -- 最后更新时间（UTC，由Python提供）
    update_count INTEGER DEFAULT 1,     -- 更新次数
    create_time TIMESTAMP NOT NULL,  -- 入库时间（UTC，由Python提供）
    FOREIGN KEY (station_name) REFERENCES stations(name) ON DELETE CASCADE ON UPDATE CASCADE,
    UNIQUE (station_name, observation_time, "DEPTH")  -- 唯一约束：站点+时间+深度
);

-- 添加表注释
COMMENT ON TABLE ocean_data IS '海洋观测数据表（NOAA ocean格式）';
COMMENT ON COLUMN ocean_data.station_name IS '站点名称（外键关联stations.name）';
COMMENT ON COLUMN ocean_data.observation_time IS '观测时间（由YY/MM/DD/hh/mm合成）';
COMMENT ON COLUMN ocean_data."DEPTH" IS '深度(m) Depth';
COMMENT ON COLUMN ocean_data."OTMP" IS '海洋温度(°C) Ocean Temperature';
COMMENT ON COLUMN ocean_data."COND" IS '电导率(S/m) Conductivity';
COMMENT ON COLUMN ocean_data."SAL" IS '盐度(PSU) Salinity';
COMMENT ON COLUMN ocean_data."O2%" IS '溶解氧百分比(%) Dissolved Oxygen Percent';
COMMENT ON COLUMN ocean_data."O2PPM" IS '溶解氧浓度(ppm) Dissolved Oxygen PPM';
COMMENT ON COLUMN ocean_data."CLCON" IS '氯离子浓度 Chloride Concentration';
COMMENT ON COLUMN ocean_data."TURB" IS '浊度(NTU) Turbidity';
COMMENT ON COLUMN ocean_data."PH" IS 'pH值 pH';
COMMENT ON COLUMN ocean_data."EH" IS '氧化还原电位(mV) Redox Potential';
COMMENT ON COLUMN ocean_data.first_crawl_time IS '首次抓取时间';
COMMENT ON COLUMN ocean_data.update_time IS '最后更新时间';
COMMENT ON COLUMN ocean_data.update_count IS '更新次数';
COMMENT ON COLUMN ocean_data.create_time IS '数据入库时间';

-- 创建索引（提升查询性能）
CREATE INDEX IF NOT EXISTS idx_ocean_station_name 
    ON ocean_data(station_name);
CREATE INDEX IF NOT EXISTS idx_ocean_observation_time 
    ON ocean_data(observation_time);

-- 创建DART®海啸监测数据表
CREATE TABLE IF NOT EXISTS dart_data (
    id BIGSERIAL PRIMARY KEY,
    station_name VARCHAR(10) NOT NULL,
    observation_time TIMESTAMP,
    "T" INTEGER,                        -- 测量类型 (1=15分钟, 2=1分钟, 3=15秒) Measurement Type
    "HEIGHT" REAL,                      -- 水柱高度 (m) Height of water column
    first_crawl_time TIMESTAMP NOT NULL,         -- 首次抓取时间（UTC，由Python提供）
    update_time TIMESTAMP NOT NULL,              -- 最后更新时间（UTC，由Python提供）
    update_count INTEGER DEFAULT 1,     -- 更新次数
    create_time TIMESTAMP NOT NULL,  -- 入库时间（UTC，由Python提供）
    FOREIGN KEY (station_name) REFERENCES stations(name) ON DELETE CASCADE ON UPDATE CASCADE,
    UNIQUE (station_name, observation_time, "T")  -- 唯一约束：站点+时间（含秒）+测量类型
);

-- 添加表注释
COMMENT ON TABLE dart_data IS 'DART®海啸监测数据表';
COMMENT ON COLUMN dart_data.station_name IS '站点名称（外键关联stations.name）';
COMMENT ON COLUMN dart_data.observation_time IS '观测时间（由YY/MM/DD/hh/mm/ss合成，精确到秒）';
COMMENT ON COLUMN dart_data."T" IS '测量类型: 1=15分钟, 2=1分钟, 3=15秒';
COMMENT ON COLUMN dart_data."HEIGHT" IS '水柱高度(m) Height of water column';
COMMENT ON COLUMN dart_data.first_crawl_time IS '首次抓取时间';
COMMENT ON COLUMN dart_data.update_time IS '最后更新时间';
COMMENT ON COLUMN dart_data.update_count IS '更新次数';
COMMENT ON COLUMN dart_data.create_time IS '数据入库时间';

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_dart_station_name 
    ON dart_data(station_name);
CREATE INDEX IF NOT EXISTS idx_dart_observation_time 
    ON dart_data(observation_time);

-- 创建漂流浮标数据表
CREATE TABLE IF NOT EXISTS drift_data (
    id BIGSERIAL PRIMARY KEY,
    station_name VARCHAR(10) NOT NULL,
    observation_time TIMESTAMP,
    "LAT" REAL,                         -- 纬度 (度) Latitude
    "LON" REAL,                         -- 经度 (度) Longitude
    "WDIR" REAL,                        -- 风向 (度) Wind Direction
    "WSPD" REAL,                        -- 风速 (m/s) Wind Speed
    "GST" REAL,                         -- 阵风速度 (m/s) Gust
    "PRES" REAL,                        -- 气压 (hPa) Pressure
    "PTDY" REAL,                        -- 气压趋势 (hPa) Pressure Tendency
    "ATMP" REAL,                        -- 气温 (°C) Air Temperature
    "WTMP" REAL,                        -- 水温 (°C) Water Temperature
    "DEWP" REAL,                        -- 露点温度 (°C) Dewpoint Temperature
    "WVHT" REAL,                        -- 有效波高 (m) Wave Height
    "DPD" REAL,                         -- 主导波周期 (秒) Dominant Wave Period
    first_crawl_time TIMESTAMP NOT NULL,         -- 首次抓取时间（UTC，由Python提供）
    update_time TIMESTAMP NOT NULL,              -- 最后更新时间（UTC，由Python提供）
    update_count INTEGER DEFAULT 1,     -- 更新次数
    create_time TIMESTAMP NOT NULL,  -- 入库时间（UTC，由Python提供）
    FOREIGN KEY (station_name) REFERENCES stations(name) ON DELETE CASCADE ON UPDATE CASCADE,
    UNIQUE (station_name, observation_time)  -- 唯一约束：站点+时间
);

-- 添加表注释
COMMENT ON TABLE drift_data IS '漂流浮标数据表（含GPS位置信息）';
COMMENT ON COLUMN drift_data.station_name IS '站点名称（外键关联stations.name）';
COMMENT ON COLUMN drift_data.observation_time IS '观测时间（由YY/MM/DD/hhmm合成）';
COMMENT ON COLUMN drift_data."LAT" IS '纬度(度) Latitude';
COMMENT ON COLUMN drift_data."LON" IS '经度(度) Longitude';
COMMENT ON COLUMN drift_data."WDIR" IS '风向(度) Wind Direction';
COMMENT ON COLUMN drift_data."WSPD" IS '风速(m/s) Wind Speed';
COMMENT ON COLUMN drift_data."GST" IS '阵风速度(m/s) Gust';
COMMENT ON COLUMN drift_data."PRES" IS '气压(hPa) Pressure';
COMMENT ON COLUMN drift_data."PTDY" IS '气压趋势(hPa) Pressure Tendency';
COMMENT ON COLUMN drift_data."ATMP" IS '气温(°C) Air Temperature';
COMMENT ON COLUMN drift_data."WTMP" IS '水温(°C) Water Temperature';
COMMENT ON COLUMN drift_data."DEWP" IS '露点温度(°C) Dewpoint Temperature';
COMMENT ON COLUMN drift_data."WVHT" IS '有效波高(m) Wave Height';
COMMENT ON COLUMN drift_data."DPD" IS '主导波周期(秒) Dominant Wave Period';
COMMENT ON COLUMN drift_data.first_crawl_time IS '首次抓取时间';
COMMENT ON COLUMN drift_data.update_time IS '最后更新时间';
COMMENT ON COLUMN drift_data.update_count IS '更新次数';
COMMENT ON COLUMN drift_data.create_time IS '数据入库时间';

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_drift_station_name 
    ON drift_data(station_name);
CREATE INDEX IF NOT EXISTS idx_drift_observation_time 
    ON drift_data(observation_time);

-- 创建波谱分析数据表
CREATE TABLE IF NOT EXISTS spec_data (
    id BIGSERIAL PRIMARY KEY,
    station_name VARCHAR(10) NOT NULL,
    observation_time TIMESTAMP,
    "WVHT" REAL,                        -- 有效波高 (m) Significant Wave Height
    "SwH" REAL,                         -- 涌浪高度 (m) Swell Height
    "SwP" REAL,                         -- 涌浪周期 (秒) Swell Period
    "WWH" REAL,                         -- 风浪高度 (m) Wind Wave Height
    "WWP" REAL,                         -- 风浪周期 (秒) Wind Wave Period
    "SwD" VARCHAR(10),                  -- 涌浪方向 Swell Direction
    "WWD" VARCHAR(10),                  -- 风浪方向 Wind Wave Direction
    "STEEPNESS" VARCHAR(10),            -- 波浪陡度 Wave Steepness
    "APD" REAL,                         -- 平均波周期 (秒) Average Wave Period
    "MWD" REAL,                         -- 平均波向 (度) Mean Wave Direction
    first_crawl_time TIMESTAMP NOT NULL,         -- 首次抓取时间（UTC，由Python提供）
    update_time TIMESTAMP NOT NULL,              -- 最后更新时间（UTC，由Python提供）
    update_count INTEGER DEFAULT 1,     -- 更新次数
    create_time TIMESTAMP NOT NULL,  -- 入库时间（UTC，由Python提供）
    FOREIGN KEY (station_name) REFERENCES stations(name) ON DELETE CASCADE ON UPDATE CASCADE,
    UNIQUE (station_name, observation_time)  -- 唯一约束：站点+时间
);

-- 添加表注释
COMMENT ON TABLE spec_data IS '波谱分析数据表（波浪详细特征）';
COMMENT ON COLUMN spec_data.station_name IS '站点名称（外键关联stations.name）';
COMMENT ON COLUMN spec_data.observation_time IS '观测时间（由YY/MM/DD/hh/mm合成）';
COMMENT ON COLUMN spec_data."WVHT" IS '有效波高(m) Significant Wave Height';
COMMENT ON COLUMN spec_data."SwH" IS '涌浪高度(m) Swell Height';
COMMENT ON COLUMN spec_data."SwP" IS '涌浪周期(秒) Swell Period';
COMMENT ON COLUMN spec_data."WWH" IS '风浪高度(m) Wind Wave Height';
COMMENT ON COLUMN spec_data."WWP" IS '风浪周期(秒) Wind Wave Period';
COMMENT ON COLUMN spec_data."SwD" IS '涌浪方向 Swell Direction';
COMMENT ON COLUMN spec_data."WWD" IS '风浪方向 Wind Wave Direction';
COMMENT ON COLUMN spec_data."STEEPNESS" IS '波浪陡度 (STEEP/AVERAGE/SWELL) Wave Steepness';
COMMENT ON COLUMN spec_data."APD" IS '平均波周期(秒) Average Wave Period';
COMMENT ON COLUMN spec_data."MWD" IS '平均波向(度) Mean Wave Direction';
COMMENT ON COLUMN spec_data.first_crawl_time IS '首次抓取时间';
COMMENT ON COLUMN spec_data.update_time IS '最后更新时间';
COMMENT ON COLUMN spec_data.update_count IS '更新次数';
COMMENT ON COLUMN spec_data.create_time IS '数据入库时间';

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_spec_station_name 
    ON spec_data(station_name);
CREATE INDEX IF NOT EXISTS idx_spec_observation_time 
    ON spec_data(observation_time);

-- 创建补充气象数据表
CREATE TABLE IF NOT EXISTS supl_data (
    id BIGSERIAL PRIMARY KEY,
    station_name VARCHAR(10) NOT NULL,
    observation_time TIMESTAMP,
    "PRES" REAL,                        -- 气压 (hPa) Pressure
    "PTIME" VARCHAR(4),                 -- 气压观测时间 (hhmm格式，如0950) Pressure Time
    "WSPD" REAL,                        -- 风速 (m/s) Wind Speed
    "WDIR" REAL,                        -- 风向 (度) Wind Direction
    "WTIME" VARCHAR(4),                 -- 风观测时间 (hhmm格式，如0950) Wind Time
    first_crawl_time TIMESTAMP NOT NULL,         -- 首次抓取时间（UTC，由Python提供）
    update_time TIMESTAMP NOT NULL,              -- 最后更新时间（UTC，由Python提供）
    update_count INTEGER DEFAULT 1,     -- 更新次数
    create_time TIMESTAMP NOT NULL,  -- 入库时间（UTC，由Python提供）
    FOREIGN KEY (station_name) REFERENCES stations(name) ON DELETE CASCADE ON UPDATE CASCADE,
    UNIQUE (station_name, observation_time)  -- 唯一约束：站点+时间
);

-- 添加表注释
COMMENT ON TABLE supl_data IS '补充气象数据表（高频观测数据）';
COMMENT ON COLUMN supl_data.station_name IS '站点名称（外键关联stations.name）';
COMMENT ON COLUMN supl_data.observation_time IS '观测时间（由YY/MM/DD/hh/mm合成）';
COMMENT ON COLUMN supl_data."PRES" IS '气压(hPa) Pressure';
COMMENT ON COLUMN supl_data."PTIME" IS '气压观测时间(hhmm格式，如0950) Pressure Time';
COMMENT ON COLUMN supl_data."WSPD" IS '风速(m/s) Wind Speed';
COMMENT ON COLUMN supl_data."WDIR" IS '风向(度) Wind Direction';
COMMENT ON COLUMN supl_data."WTIME" IS '风观测时间(hhmm格式，如0950) Wind Time';
COMMENT ON COLUMN supl_data.first_crawl_time IS '首次抓取时间';
COMMENT ON COLUMN supl_data.update_time IS '最后更新时间';
COMMENT ON COLUMN supl_data.update_count IS '更新次数';
COMMENT ON COLUMN supl_data.create_time IS '数据入库时间';

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_supl_station_name 
    ON supl_data(station_name);
CREATE INDEX IF NOT EXISTS idx_supl_observation_time 
    ON supl_data(observation_time);



