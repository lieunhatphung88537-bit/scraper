-- 创建数据库（如果不存在）
-- CREATE DATABASE metar_taf_synop_data ENCODING 'UTF8';

-- 连接到数据库后执行以下内容
-- \c metar_taf_synop_data

-- ==========================================
-- 1. OGIMET METAR 数据表
-- ==========================================
DROP TABLE IF EXISTS ogimet_metar_data CASCADE;

CREATE TABLE ogimet_metar_data
(
    id BIGSERIAL PRIMARY KEY,                               -- 主键ID，自增序列
    raw_text TEXT NOT NULL,                                 -- 原始METAR报文文本
    corrected BOOLEAN DEFAULT FALSE,                        -- 是否为订正报文（COR）
    auto BOOLEAN DEFAULT FALSE,                             -- 是否为自动观测站报文（AUTO）
    station_id CHARACTER VARYING(10) NOT NULL,              -- 机场ICAO四字码（如ZBAA=北京首都）
    observation_time TIMESTAMP WITHOUT TIME ZONE,           -- 观测时间（UTC）
    latitude NUMERIC(9, 6),                                 -- 机场纬度（度）
    longitude NUMERIC(9, 6),                                -- 机场经度（度）
    temp NUMERIC(9, 2),                                     -- 气温（摄氏度）
    dewpoint NUMERIC(9, 2),                                 -- 露点温度（摄氏度）
    weather TEXT,                                           -- 天气现象（RA=雨,SN=雪,FG=雾,TS=雷暴）
    sky TEXT,                                               -- 天空状况/云层信息
    wind_direction_variation TEXT,                          -- 风向变化范围（如VRB,350V040）
    wind_dir_degrees NUMERIC(9, 1),                         -- 风向（度，0-360）
    wind_speed NUMERIC(9, 2),                               -- 风速（米/秒）
    wind_gust NUMERIC(9, 2),                                -- 阵风风速（米/秒）
    peak_wind_drct NUMERIC(9, 1),                           -- 峰值风向（度）
    peak_wind_gust NUMERIC(9, 2),                           -- 峰值阵风风速（米/秒）
    visibility_statute NUMERIC(15, 2),                      -- 能见度（米）
    altimeter_hpa NUMERIC(9, 2),                            -- 高度计修正值/场压（百帕）
    sea_level_pressure NUMERIC(9, 2),                       -- 海平面气压（百帕）
    pcp1h NUMERIC(9, 2),                                    -- 1小时降水量（毫米）
    pcp3h NUMERIC(9, 2),                                    -- 3小时降水量（毫米）
    pcp6h NUMERIC(9, 2),                                    -- 6小时降水量（毫米）
    pcp24h NUMERIC(9, 2),                                   -- 24小时降水量（毫米）
    maxT6h NUMERIC(9, 2),                                   -- 6小时最高气温（摄氏度）
    minT6h NUMERIC(9, 2),                                   -- 6小时最低气温（摄氏度）
    maxT24h NUMERIC(9, 2),                                  -- 24小时最高气温（摄氏度）
    minT24h NUMERIC(9, 2),                                  -- 24小时最低气温（摄氏度）
    metar_type CHARACTER VARYING(10),                       -- METAR类型（METAR=例行报,SPECI=特殊报）
    remarks TEXT,                                           -- 备注信息（RMK段内容）
    rvr TEXT,                                               -- 跑道视程RVR信息
    country CHARACTER VARYING(100),                         -- 国家/地区名称
    crawl_time TIMESTAMP WITHOUT TIME ZONE,                 -- 爬虫抓取时间
    updatetime TIMESTAMP WITHOUT TIME ZONE NOT NULL,        -- 数据最后更新时间
    update_count INTEGER DEFAULT 0,                         -- 数据更新次数
    create_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,       -- 记录创建时间
    UNIQUE (station_id, observation_time)
);

CREATE INDEX idx_ogimet_metar_station_id ON ogimet_metar_data(station_id);
CREATE INDEX idx_ogimet_metar_observation_time ON ogimet_metar_data(observation_time);

-- 表注释
COMMENT ON TABLE ogimet_metar_data IS 'OGIMET METAR航空气象观测数据表';

-- 字段注释
COMMENT ON COLUMN ogimet_metar_data.id IS '主键ID，自增序列';
COMMENT ON COLUMN ogimet_metar_data.raw_text IS '原始METAR报文文本';
COMMENT ON COLUMN ogimet_metar_data.corrected IS '是否为订正报文（COR）';
COMMENT ON COLUMN ogimet_metar_data.auto IS '是否为自动观测站报文（AUTO）';
COMMENT ON COLUMN ogimet_metar_data.station_id IS '机场ICAO四字码（如ZBAA=北京首都）';
COMMENT ON COLUMN ogimet_metar_data.observation_time IS '观测时间（UTC）';
COMMENT ON COLUMN ogimet_metar_data.latitude IS '机场纬度（度）';
COMMENT ON COLUMN ogimet_metar_data.longitude IS '机场经度（度）';
COMMENT ON COLUMN ogimet_metar_data.temp IS '气温（摄氏度）';
COMMENT ON COLUMN ogimet_metar_data.dewpoint IS '露点温度（摄氏度）';
COMMENT ON COLUMN ogimet_metar_data.weather IS '天气现象（如RA=雨, SN=雪, FG=雾, TS=雷暴）';
COMMENT ON COLUMN ogimet_metar_data.sky IS '天空状况/云层信息（如FEW020=少云2000ft, SCT040=疏云4000ft）';
COMMENT ON COLUMN ogimet_metar_data.wind_direction_variation IS '风向变化范围（如VRB=变化不定, 350V040=350°至040°变化）';
COMMENT ON COLUMN ogimet_metar_data.wind_dir_degrees IS '风向（度，0-360）';
COMMENT ON COLUMN ogimet_metar_data.wind_speed IS '风速（米/秒）';
COMMENT ON COLUMN ogimet_metar_data.wind_gust IS '阵风风速（米/秒）';
COMMENT ON COLUMN ogimet_metar_data.peak_wind_drct IS '峰值风向（度）';
COMMENT ON COLUMN ogimet_metar_data.peak_wind_gust IS '峰值阵风风速（米/秒）';
COMMENT ON COLUMN ogimet_metar_data.visibility_statute IS '能见度（米）';
COMMENT ON COLUMN ogimet_metar_data.altimeter_hpa IS '高度计修正值/场压（百帕）';
COMMENT ON COLUMN ogimet_metar_data.sea_level_pressure IS '海平面气压（百帕）';
COMMENT ON COLUMN ogimet_metar_data.pcp1h IS '1小时降水量（毫米）';
COMMENT ON COLUMN ogimet_metar_data.pcp3h IS '3小时降水量（毫米）';
COMMENT ON COLUMN ogimet_metar_data.pcp6h IS '6小时降水量（毫米）';
COMMENT ON COLUMN ogimet_metar_data.pcp24h IS '24小时降水量（毫米）';
COMMENT ON COLUMN ogimet_metar_data.maxT6h IS '6小时最高气温（摄氏度）';
COMMENT ON COLUMN ogimet_metar_data.minT6h IS '6小时最低气温（摄氏度）';
COMMENT ON COLUMN ogimet_metar_data.maxT24h IS '24小时最高气温（摄氏度）';
COMMENT ON COLUMN ogimet_metar_data.minT24h IS '24小时最低气温（摄氏度）';
COMMENT ON COLUMN ogimet_metar_data.metar_type IS 'METAR类型（METAR=例行报, SPECI=特殊报）';
COMMENT ON COLUMN ogimet_metar_data.remarks IS '备注信息（RMK段内容）';
COMMENT ON COLUMN ogimet_metar_data.rvr IS '跑道视程RVR信息';
COMMENT ON COLUMN ogimet_metar_data.country IS '国家/地区名称';
COMMENT ON COLUMN ogimet_metar_data.crawl_time IS '爬虫抓取时间';
COMMENT ON COLUMN ogimet_metar_data.updatetime IS '数据最后更新时间';
COMMENT ON COLUMN ogimet_metar_data.update_count IS '数据更新次数';
COMMENT ON COLUMN ogimet_metar_data.create_time IS '记录创建时间';

-- ==========================================
-- 2. OGIMET TAF 数据表
-- ==========================================
DROP TABLE IF EXISTS ogimet_taf_data CASCADE;

CREATE TABLE ogimet_taf_data (
    id BIGSERIAL PRIMARY KEY,                               -- 主键ID，自增序列
    raw_taf TEXT NOT NULL,                                  -- 原始TAF报文文本
    observation_time TIMESTAMP WITHOUT TIME ZONE,           -- 发报时间（UTC）
    station_id CHARACTER VARYING(10) NOT NULL,              -- 机场ICAO四字码
    latitude NUMERIC(9, 6),                                 -- 机场纬度（度）
    longitude NUMERIC(9, 6),                                -- 机场经度（度）
    country CHARACTER VARYING(100),                         -- 国家/地区名称
    is_corrected BOOLEAN DEFAULT FALSE,                     -- 是否为订正预报（AMD/COR）
    valid_from TIMESTAMP WITHOUT TIME ZONE,                 -- 预报有效期开始时间（UTC）
    valid_to TIMESTAMP WITHOUT TIME ZONE,                   -- 预报有效期结束时间（UTC）
    wind_direction_deg NUMERIC(9, 1),                       -- 预报风向（度）
    wind_speed_mps NUMERIC(9, 2),                           -- 预报风速（米/秒）
    visibility_m NUMERIC(12, 2),                            -- 预报能见度（米）
    weather_phenomena TEXT,                                 -- 预报天气现象
    cloud_cover CHARACTER VARYING(20),                      -- 预报云量/云况
    max_temp_c NUMERIC(5, 2),                               -- 预报最高气温（摄氏度）
    min_temp_c NUMERIC(5, 2),                               -- 预报最低气温（摄氏度）
    trends_descriptions TEXT,                               -- 趋势预报描述（TEMPO/BECMG/PROB等）
    crawl_time TIMESTAMP WITHOUT TIME ZONE,                 -- 爬虫抓取时间
    update_time TIMESTAMP WITHOUT TIME ZONE,                -- 数据最后更新时间
    update_count INTEGER DEFAULT 0,                         -- 数据更新次数
    create_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,       -- 记录创建时间
    UNIQUE (station_id, observation_time)
);

CREATE INDEX idx_ogimet_taf_copy_station_id ON ogimet_taf_data(station_id);
CREATE INDEX idx_ogimet_taf_copy_observation_time ON ogimet_taf_data(observation_time);

-- 表注释
COMMENT ON TABLE ogimet_taf_data IS 'OGIMET TAF航空气象预报数据表';

-- 字段注释
COMMENT ON COLUMN ogimet_taf_data.id IS '主键ID，自增序列';
COMMENT ON COLUMN ogimet_taf_data.raw_taf IS '原始TAF报文文本';
COMMENT ON COLUMN ogimet_taf_data.observation_time IS '发报时间（UTC）';
COMMENT ON COLUMN ogimet_taf_data.station_id IS '机场ICAO四字码（如ZBAA=北京首都）';
COMMENT ON COLUMN ogimet_taf_data.latitude IS '机场纬度（度）';
COMMENT ON COLUMN ogimet_taf_data.longitude IS '机场经度（度）';
COMMENT ON COLUMN ogimet_taf_data.country IS '国家/地区名称';
COMMENT ON COLUMN ogimet_taf_data.is_corrected IS '是否为订正预报（AMD/COR）';
COMMENT ON COLUMN ogimet_taf_data.valid_from IS '预报有效期开始时间（UTC）';
COMMENT ON COLUMN ogimet_taf_data.valid_to IS '预报有效期结束时间（UTC）';
COMMENT ON COLUMN ogimet_taf_data.wind_direction_deg IS '预报风向（度）';
COMMENT ON COLUMN ogimet_taf_data.wind_speed_mps IS '预报风速（米/秒）';
COMMENT ON COLUMN ogimet_taf_data.visibility_m IS '预报能见度（米）';
COMMENT ON COLUMN ogimet_taf_data.weather_phenomena IS '预报天气现象';
COMMENT ON COLUMN ogimet_taf_data.cloud_cover IS '预报云量/云况';
COMMENT ON COLUMN ogimet_taf_data.max_temp_c IS '预报最高气温（摄氏度）';
COMMENT ON COLUMN ogimet_taf_data.min_temp_c IS '预报最低气温（摄氏度）';
COMMENT ON COLUMN ogimet_taf_data.trends_descriptions IS '趋势预报描述（TEMPO/BECMG/PROB等）';
COMMENT ON COLUMN ogimet_taf_data.crawl_time IS '爬虫抓取时间';
COMMENT ON COLUMN ogimet_taf_data.update_time IS '数据最后更新时间';
COMMENT ON COLUMN ogimet_taf_data.update_count IS '数据更新次数';
COMMENT ON COLUMN ogimet_taf_data.create_time IS '记录创建时间';

-- ==========================================
-- 3. SKYVECTOR METAR 数据表
-- ==========================================
DROP TABLE IF EXISTS skyvector_metar_data CASCADE;

CREATE TABLE skyvector_metar_data (
    id BIGSERIAL PRIMARY KEY,                               -- 主键ID，自增序列
    raw_text TEXT NOT NULL,                                 -- 原始METAR报文文本
    corrected BOOLEAN DEFAULT FALSE,                        -- 是否为订正报文（COR）
    auto BOOLEAN DEFAULT FALSE,                             -- 是否为自动观测站报文（AUTO）
    station_id CHARACTER VARYING(10) NOT NULL,              -- 机场ICAO四字码
    observation_time TIMESTAMP WITHOUT TIME ZONE,           -- 观测时间（UTC）
    latitude NUMERIC(9, 6),                                 -- 机场纬度（度）
    longitude NUMERIC(9, 6),                                -- 机场经度（度）
    temp NUMERIC(9, 2),                                     -- 气温（摄氏度）
    dewpoint NUMERIC(9, 2),                                 -- 露点温度（摄氏度）
    weather TEXT,                                           -- 天气现象（RA=雨,SN=雪,FG=雾,TS=雷暴）
    sky TEXT,                                               -- 天空状况/云层信息
    wind_direction_variation TEXT,                          -- 风向变化范围
    wind_dir_degrees NUMERIC(9, 1),                         -- 风向（度，0-360）
    wind_speed NUMERIC(9, 2),                               -- 风速（米/秒）
    wind_gust NUMERIC(9, 2),                                -- 阵风风速（米/秒）
    peak_wind_drct NUMERIC(9, 1),                           -- 峰值风向（度）
    peak_wind_gust NUMERIC(9, 2),                           -- 峰值阵风风速（米/秒）
    visibility_statute NUMERIC(15, 2),                      -- 能见度（米）
    altimeter_hpa NUMERIC(9, 2),                            -- 高度计修正值/场压（百帕）
    sea_level_pressure NUMERIC(9, 2),                       -- 海平面气压（百帕）
    pcp1h NUMERIC(9, 2),                                    -- 1小时降水量（毫米）
    pcp3h NUMERIC(9, 2),                                    -- 3小时降水量（毫米）
    pcp6h NUMERIC(9, 2),                                    -- 6小时降水量（毫米）
    pcp24h NUMERIC(9, 2),                                   -- 24小时降水量（毫米）
    maxT6h NUMERIC(9, 2),                                   -- 6小时最高气温（摄氏度）
    minT6h NUMERIC(9, 2),                                   -- 6小时最低气温（摄氏度）
    maxT24h NUMERIC(9, 2),                                  -- 24小时最高气温（摄氏度）
    minT24h NUMERIC(9, 2),                                  -- 24小时最低气温（摄氏度）
    metar_type CHARACTER VARYING(10),                       -- METAR类型（METAR=例行报,SPECI=特殊报）
    remarks TEXT,                                           -- 备注信息（RMK段内容）
    rvr TEXT,                                               -- 跑道视程RVR信息
    country CHARACTER VARYING(100),                         -- 国家/地区名称
    crawl_time TIMESTAMP WITHOUT TIME ZONE,                 -- 爬虫抓取时间
    updatetime TIMESTAMP WITHOUT TIME ZONE NOT NULL,        -- 数据最后更新时间
    update_count INTEGER DEFAULT 0,                         -- 数据更新次数
    create_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,       -- 记录创建时间
    UNIQUE (station_id, observation_time)
);

CREATE INDEX idx_skyvector_metar_copy_station_id ON skyvector_metar_data(station_id);
CREATE INDEX idx_skyvector_metar_copy_observation_time ON skyvector_metar_data(observation_time);

-- 表注释
COMMENT ON TABLE skyvector_metar_data IS 'SkyVector METAR航空气象观测数据表';

-- 字段注释
COMMENT ON COLUMN skyvector_metar_data.id IS '主键ID，自增序列';
COMMENT ON COLUMN skyvector_metar_data.raw_text IS '原始METAR报文文本';
COMMENT ON COLUMN skyvector_metar_data.corrected IS '是否为订正报文（COR）';
COMMENT ON COLUMN skyvector_metar_data.auto IS '是否为自动观测站报文（AUTO）';
COMMENT ON COLUMN skyvector_metar_data.station_id IS '机场ICAO四字码（如ZBAA=北京首都）';
COMMENT ON COLUMN skyvector_metar_data.observation_time IS '观测时间（UTC）';
COMMENT ON COLUMN skyvector_metar_data.latitude IS '机场纬度（度）';
COMMENT ON COLUMN skyvector_metar_data.longitude IS '机场经度（度）';
COMMENT ON COLUMN skyvector_metar_data.temp IS '气温（摄氏度）';
COMMENT ON COLUMN skyvector_metar_data.dewpoint IS '露点温度（摄氏度）';
COMMENT ON COLUMN skyvector_metar_data.weather IS '天气现象（如RA=雨, SN=雪, FG=雾, TS=雷暴）';
COMMENT ON COLUMN skyvector_metar_data.sky IS '天空状况/云层信息';
COMMENT ON COLUMN skyvector_metar_data.wind_direction_variation IS '风向变化范围';
COMMENT ON COLUMN skyvector_metar_data.wind_dir_degrees IS '风向（度，0-360）';
COMMENT ON COLUMN skyvector_metar_data.wind_speed IS '风速（米/秒）';
COMMENT ON COLUMN skyvector_metar_data.wind_gust IS '阵风风速（米/秒）';
COMMENT ON COLUMN skyvector_metar_data.peak_wind_drct IS '峰值风向（度）';
COMMENT ON COLUMN skyvector_metar_data.peak_wind_gust IS '峰值阵风风速（米/秒）';
COMMENT ON COLUMN skyvector_metar_data.visibility_statute IS '能见度（米）';
COMMENT ON COLUMN skyvector_metar_data.altimeter_hpa IS '高度计修正值/场压（百帕）';
COMMENT ON COLUMN skyvector_metar_data.sea_level_pressure IS '海平面气压（百帕）';
COMMENT ON COLUMN skyvector_metar_data.pcp1h IS '1小时降水量（毫米）';
COMMENT ON COLUMN skyvector_metar_data.pcp3h IS '3小时降水量（毫米）';
COMMENT ON COLUMN skyvector_metar_data.pcp6h IS '6小时降水量（毫米）';
COMMENT ON COLUMN skyvector_metar_data.pcp24h IS '24小时降水量（毫米）';
COMMENT ON COLUMN skyvector_metar_data.maxT6h IS '6小时最高气温（摄氏度）';
COMMENT ON COLUMN skyvector_metar_data.minT6h IS '6小时最低气温（摄氏度）';
COMMENT ON COLUMN skyvector_metar_data.maxT24h IS '24小时最高气温（摄氏度）';
COMMENT ON COLUMN skyvector_metar_data.minT24h IS '24小时最低气温（摄氏度）';
COMMENT ON COLUMN skyvector_metar_data.metar_type IS 'METAR类型（METAR=例行报, SPECI=特殊报）';
COMMENT ON COLUMN skyvector_metar_data.remarks IS '备注信息（RMK段内容）';
COMMENT ON COLUMN skyvector_metar_data.rvr IS '跑道视程RVR信息';
COMMENT ON COLUMN skyvector_metar_data.country IS '国家/地区名称';
COMMENT ON COLUMN skyvector_metar_data.crawl_time IS '爬虫抓取时间';
COMMENT ON COLUMN skyvector_metar_data.updatetime IS '数据最后更新时间';
COMMENT ON COLUMN skyvector_metar_data.update_count IS '数据更新次数';
COMMENT ON COLUMN skyvector_metar_data.create_time IS '记录创建时间';

-- ==========================================
-- 4. SKYVECTOR TAF 数据表
-- ==========================================
DROP TABLE IF EXISTS skyvector_taf_data CASCADE;

CREATE TABLE skyvector_taf_data (
    id BIGSERIAL PRIMARY KEY,                               -- 主键ID，自增序列
    raw_taf TEXT NOT NULL,                                  -- 原始TAF报文文本
    observation_time TIMESTAMP WITHOUT TIME ZONE,           -- 发报时间（UTC）
    station_id CHARACTER VARYING(10) NOT NULL,              -- 机场ICAO四字码
    latitude NUMERIC(9, 6),                                 -- 机场纬度（度）
    longitude NUMERIC(9, 6),                                -- 机场经度（度）
    country CHARACTER VARYING(100),                         -- 国家/地区名称
    is_corrected BOOLEAN DEFAULT FALSE,                     -- 是否为订正预报（AMD/COR）
    valid_from TIMESTAMP WITHOUT TIME ZONE,                 -- 预报有效期开始时间（UTC）
    valid_to TIMESTAMP WITHOUT TIME ZONE,                   -- 预报有效期结束时间（UTC）
    wind_direction_deg NUMERIC(9, 1),                       -- 预报风向（度）
    wind_speed_mps NUMERIC(9, 2),                           -- 预报风速（米/秒）
    visibility_m NUMERIC(12, 2),                            -- 预报能见度（米）
    weather_phenomena TEXT,                                 -- 预报天气现象
    cloud_cover CHARACTER VARYING(20),                      -- 预报云量/云况
    max_temp_c NUMERIC(5, 2),                               -- 预报最高气温（摄氏度）
    min_temp_c NUMERIC(5, 2),                               -- 预报最低气温（摄氏度）
    trends_descriptions TEXT,                               -- 趋势预报描述（TEMPO/BECMG/PROB等）
    crawl_time TIMESTAMP WITHOUT TIME ZONE,                 -- 爬虫抓取时间
    update_time TIMESTAMP WITHOUT TIME ZONE,                -- 数据最后更新时间
    update_count INTEGER DEFAULT 0,                         -- 数据更新次数
    create_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,       -- 记录创建时间
    UNIQUE (station_id, observation_time)
);

CREATE INDEX idx_skyvector_taf_station_id ON skyvector_taf_data(station_id);
CREATE INDEX idx_skyvector_taf_observation_time ON skyvector_taf_data(observation_time);

-- 表注释
COMMENT ON TABLE skyvector_taf_data IS 'SkyVector TAF航空气象预报数据表';

-- 字段注释
COMMENT ON COLUMN skyvector_taf_data.id IS '主键ID，自增序列';
COMMENT ON COLUMN skyvector_taf_data.raw_taf IS '原始TAF报文文本';
COMMENT ON COLUMN skyvector_taf_data.observation_time IS '发报时间（UTC）';
COMMENT ON COLUMN skyvector_taf_data.station_id IS '机场ICAO四字码（如ZBAA=北京首都）';
COMMENT ON COLUMN skyvector_taf_data.latitude IS '机场纬度（度）';
COMMENT ON COLUMN skyvector_taf_data.longitude IS '机场经度（度）';
COMMENT ON COLUMN skyvector_taf_data.country IS '国家/地区名称';
COMMENT ON COLUMN skyvector_taf_data.is_corrected IS '是否为订正预报（AMD/COR）';
COMMENT ON COLUMN skyvector_taf_data.valid_from IS '预报有效期开始时间（UTC）';
COMMENT ON COLUMN skyvector_taf_data.valid_to IS '预报有效期结束时间（UTC）';
COMMENT ON COLUMN skyvector_taf_data.wind_direction_deg IS '预报风向（度）';
COMMENT ON COLUMN skyvector_taf_data.wind_speed_mps IS '预报风速（米/秒）';
COMMENT ON COLUMN skyvector_taf_data.visibility_m IS '预报能见度（米）';
COMMENT ON COLUMN skyvector_taf_data.weather_phenomena IS '预报天气现象';
COMMENT ON COLUMN skyvector_taf_data.cloud_cover IS '预报云量/云况';
COMMENT ON COLUMN skyvector_taf_data.max_temp_c IS '预报最高气温（摄氏度）';
COMMENT ON COLUMN skyvector_taf_data.min_temp_c IS '预报最低气温（摄氏度）';
COMMENT ON COLUMN skyvector_taf_data.trends_descriptions IS '趋势预报描述（TEMPO/BECMG/PROB等）';
COMMENT ON COLUMN skyvector_taf_data.crawl_time IS '爬虫抓取时间';
COMMENT ON COLUMN skyvector_taf_data.update_time IS '数据最后更新时间';
COMMENT ON COLUMN skyvector_taf_data.update_count IS '数据更新次数';
COMMENT ON COLUMN skyvector_taf_data.create_time IS '记录创建时间';

-- ==========================================
-- 5. OGIMET SYNOP 数据表
-- ==========================================
DROP TABLE IF EXISTS ogimet_synop_data CASCADE;

CREATE TABLE ogimet_synop_data (
    id                          BIGSERIAL PRIMARY KEY,                          -- 主键ID，自增序列
    raw_text                    TEXT NOT NULL,                                  -- 原始SYNOP报文文本
    station_id                  VARCHAR(10) NOT NULL,                           -- 气象站代码（WMO 5位站号）
    observation_time            TIMESTAMP WITHOUT TIME ZONE,                    -- 观测时间（UTC）
    latitude                    NUMERIC(9,6),                                   -- 站点纬度（度，正北负南）
    longitude                   NUMERIC(9,6),                                   -- 站点经度（度，正东负西）
    country                     VARCHAR(100),                                   -- 国家/地区名称
    report_type                 VARCHAR(10),                                    -- 报文类型（AAXX=陆地站, BBXX=船舶站）
    station_name                VARCHAR(200),                                   -- 站点名称
    altitude_m                  INTEGER,                                        -- 站点海拔高度（米）
    temp                        NUMERIC(9,2),                                   -- 气温（摄氏度）
    dewpoint                    NUMERIC(9,2),                                   -- 露点温度（摄氏度）
    wind_indicator              VARCHAR(5),                                     -- 风速单位指示符（0=m/s估计,1=m/s测量,3=kt估计,4=kt测量）
    wind_dir_degrees            NUMERIC(9,1),                                   -- 风向（度，0-360）
    wind_speed                  NUMERIC(9,2),                                   -- 风速（米/秒）
    sea_level_pressure          NUMERIC(9,2),                                   -- 海平面气压（百帕）
    station_pressure            NUMERIC(9,2),                                   -- 站点气压（百帕）
    pressure_tendency           NUMERIC(9,2),                                   -- 3小时气压变化（百帕，正升负降）
    visibility_statute          NUMERIC(15,2),                                  -- 能见度（米）
    precipitation_mm            NUMERIC(9,2),                                   -- 降水量（毫米）
    precipitation_hours         INTEGER,                                        -- 降水时长（小时）
    present_weather_desc        TEXT,                                           -- 当前天气现象描述
    total_cloud_amount_oktas    INTEGER,                                        -- 总云量（0-8 oktas，9=天空不可见）
    low_cloud_type              VARCHAR(10),                                    -- 低云类型代码（CL，0-9）
    mid_cloud_type              VARCHAR(10),                                    -- 中云类型代码（CM，0-9）
    high_cloud_type             VARCHAR(10),                                    -- 高云类型代码（CH，0-9）
    crawl_time                  TIMESTAMP WITHOUT TIME ZONE,                    -- 爬虫抓取时间
    updatetime                  TIMESTAMP WITHOUT TIME ZONE NOT NULL,           -- 数据最后更新时间
    update_count                INTEGER DEFAULT 0,                              -- 数据更新次数
    create_time                 TIMESTAMP WITHOUT TIME ZONE NOT NULL,           -- 记录创建时间

    CONSTRAINT ogimet_synop_data_station_id_observation_time_key 
        UNIQUE (station_id, observation_time)
);

CREATE INDEX idx_ogimet_synop_station_id ON ogimet_synop_data(station_id);
CREATE INDEX idx_ogimet_synop_observation_time ON ogimet_synop_data(observation_time);
CREATE INDEX idx_ogimet_synop_country ON ogimet_synop_data(country);
CREATE INDEX idx_ogimet_synop_station_obs_time ON ogimet_synop_data(station_id, observation_time DESC);
-- 表注释
COMMENT ON TABLE ogimet_synop_data IS 'OGIMET SYNOP 地面气象观测数据表';

-- 字段注释
COMMENT ON COLUMN ogimet_synop_data.id IS '主键ID，自增序列';
COMMENT ON COLUMN ogimet_synop_data.raw_text IS '原始 SYNOP 报文文本';
COMMENT ON COLUMN ogimet_synop_data.station_id IS '气象站代码（WMO 5位站号）';
COMMENT ON COLUMN ogimet_synop_data.observation_time IS '观测时间（UTC）';
COMMENT ON COLUMN ogimet_synop_data.latitude IS '站点纬度（度，正数为北纬，负数为南纬）';
COMMENT ON COLUMN ogimet_synop_data.longitude IS '站点经度（度，正数为东经，负数为西经）';
COMMENT ON COLUMN ogimet_synop_data.country IS '国家/地区名称';
COMMENT ON COLUMN ogimet_synop_data.report_type IS 'SYNOP 报文类型（AAXX=陆地站, BBXX=船舶站）';
COMMENT ON COLUMN ogimet_synop_data.station_name IS '站点名称';
COMMENT ON COLUMN ogimet_synop_data.altitude_m IS '站点海拔高度（米）';
COMMENT ON COLUMN ogimet_synop_data.temp IS '气温（摄氏度）';
COMMENT ON COLUMN ogimet_synop_data.dewpoint IS '露点温度（摄氏度）';
COMMENT ON COLUMN ogimet_synop_data.wind_indicator IS '风速单位指示符（0=m/s估计, 1=m/s测量, 3=kt估计, 4=kt测量）';
COMMENT ON COLUMN ogimet_synop_data.wind_dir_degrees IS '风向（度，0-360，0或360表示北风）';
COMMENT ON COLUMN ogimet_synop_data.wind_speed IS '风速（米/秒）';
COMMENT ON COLUMN ogimet_synop_data.sea_level_pressure IS '海平面气压（百帕）- SYNOP报文中的4PPPP组，为修正到海平面的气压值';
COMMENT ON COLUMN ogimet_synop_data.station_pressure IS '站点气压（百帕）- SYNOP报文中的3PPPP组，为站点实测气压值';
COMMENT ON COLUMN ogimet_synop_data.pressure_tendency IS '3小时气压变化（百帕）- 正值表示上升，负值表示下降';
COMMENT ON COLUMN ogimet_synop_data.visibility_statute IS '能见度（米）';
COMMENT ON COLUMN ogimet_synop_data.precipitation_mm IS '降水量（毫米）';
COMMENT ON COLUMN ogimet_synop_data.precipitation_hours IS '降水时长（小时，如6/12/24小时降水）';
COMMENT ON COLUMN ogimet_synop_data.present_weather_desc IS '当前天气现象描述（如雨、雪、雾等）';
COMMENT ON COLUMN ogimet_synop_data.total_cloud_amount_oktas IS '总云量（0-8 oktas，9=天空不可见）';
COMMENT ON COLUMN ogimet_synop_data.low_cloud_type IS '低云类型代码（CL，0-9）';
COMMENT ON COLUMN ogimet_synop_data.mid_cloud_type IS '中云类型代码（CM，0-9）';
COMMENT ON COLUMN ogimet_synop_data.high_cloud_type IS '高云类型代码（CH，0-9）';
COMMENT ON COLUMN ogimet_synop_data.crawl_time IS '爬虫抓取时间';
COMMENT ON COLUMN ogimet_synop_data.updatetime IS '数据最后更新时间';
COMMENT ON COLUMN ogimet_synop_data.update_count IS '数据更新次数（用于UPSERT计数）';
COMMENT ON COLUMN ogimet_synop_data.create_time IS '记录创建时间';

