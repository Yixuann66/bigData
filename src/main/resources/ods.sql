-- ODS --
-- Operate Data Source
-- 数据格式和压缩格式尽可能不变（中间件用gzip来压缩）
-- 存储从mysql业务数据库和日志服务器的日志文件采集的数据
-- 日志数据
				-- JSON
			-- 业务数据
				-- 历史数据
				-- 格式
					-- 全量 DATAX tsv  	用tab分割数据  csv -》 逗号分割数据
					-- 增量 maxwell  JSON
		-- 汇总数据
			-- 希望用最少的资源存储最多的数据 列的压缩效率高 单一数据结构
				-- 压缩
					-- gzip hadoop默认支持  压缩率极高 压缩效率低时间长
					-- snappy hadoop不支持 压缩率极高 压缩效率低时间长
					-- lzo hadoop不支持 压缩效率一般 压缩效率相对好
				-- ODS 选择 压缩率高的，因为不涉及计算，用最小资源存最多的数据


--	ODS层的设计要点如下：
-- （1）ODS层的表结构设计依托于从业务系统同步过来的数据结构。
-- （2）ODS层要保存全部历史数据，故其压缩格式应选择压缩比较高的，此处选择gzip。
-- （3）ODS层表名的命名规范为：ods_表名_单分区增量全量标识（inc/full）。




DROP TABLE IF EXISTS ods_log_inc;
--  ods层日志表 外部表 只管理元数据信息，删除表后不影响原数据
CREATE EXTERNAL TABLE ods_log_inc
(
    `common` STRUCT<
        ar :STRING,
        ba :STRING,
        ch :STRING,
        is_new :STRING,
        md :STRING,
        mid :STRING,
        os :STRING,
        sid :STRING,
        uid :STRING,
        vc :STRING
       > COMMENT '公共信息',
    `page` STRUCT<
        during_time :STRING,
        item :STRING,
        item_type :STRING,
        last_page_id :STRING,
        page_id :STRING,
        from_pos_id :STRING,
        from_pos_seq :STRING,
        refer_id :STRING> COMMENT '页面信息',
    `actions` ARRAY<STRUCT<
       action_id:STRING,
        item:STRING,
        item_type:STRING,
        ts:BIGINT
        >>
        COMMENT '动作信息',
    `displays` ARRAY<STRUCT<display_type :STRING,
        item :STRING,
        item_type :STRING,
        `pos_seq` :STRING,
        pos_id :STRING>> COMMENT '曝光信息',
    `start` STRUCT<entry :STRING,
        first_open :BIGINT,
        loading_time :BIGINT,
        open_ad_id :BIGINT,
        open_ad_ms :BIGINT,
        open_ad_skip_ms :BIGINT> COMMENT '启动信息',
    `err` STRUCT<error_code:BIGINT,
            msg:STRING> COMMENT '错误信息',
    `ts` BIGINT  COMMENT '时间戳'
) COMMENT '活动信息表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
LOCATION '/warehouse/gmall/ods/ods_log_inc/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');

-- 装载数据 load
LOAD DATA INPATH '/origin_data/gmall/log/topic_log/dt=2020-05-26' OVERWRITE INTO TABLE ods_log_inc PARTITION (dt='2020-05-26');
SELECT * FROM ods_log_inc;
SELECT count(*) FROM ods_log_inc;

-- SELECT
--   ids,
--   ids[1],
--   ids[0],
--   array_contains(ids, '1'),
--   sort_array(ids) AS sorted_ids -- 使用 sort_array 替代 sort_array_by
-- FROM (
--   SELECT array(1, '2', 3, 4) AS ids
-- ) e;


