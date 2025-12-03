-- ============================================
-- MySQL 千万级数据优化配置
-- 适用于 8GB+ 内存服务器
-- ============================================

-- 1. InnoDB 缓冲池优化（建议设置为物理内存的 50-70%）
SET GLOBAL innodb_buffer_pool_size = 4294967296;  -- 4GB

-- 2. 查询缓存（MySQL 5.7及以下）
SET GLOBAL query_cache_size = 268435456;  -- 256MB
SET GLOBAL query_cache_type = 1;

-- 3. 连接数优化
SET GLOBAL max_connections = 500;
SET GLOBAL max_connect_errors = 1000;

-- 4. InnoDB 日志优化
SET GLOBAL innodb_log_file_size = 536870912;  -- 512MB
SET GLOBAL innodb_log_buffer_size = 67108864;  -- 64MB
SET GLOBAL innodb_flush_log_at_trx_commit = 2;  -- 性能优先

-- 5. 并发优化
SET GLOBAL innodb_thread_concurrency = 0;  -- 自动
SET GLOBAL innodb_read_io_threads = 8;
SET GLOBAL innodb_write_io_threads = 8;

-- 6. 表缓存
SET GLOBAL table_open_cache = 4096;
SET GLOBAL table_definition_cache = 2048;

-- 7. 临时表优化
SET GLOBAL tmp_table_size = 134217728;  -- 128MB
SET GLOBAL max_heap_table_size = 134217728;  -- 128MB

-- 8. 排序和连接优化
SET GLOBAL sort_buffer_size = 4194304;  -- 4MB
SET GLOBAL join_buffer_size = 4194304;  -- 4MB
SET GLOBAL read_buffer_size = 2097152;  -- 2MB
SET GLOBAL read_rnd_buffer_size = 8388608;  -- 8MB

-- 9. 批量插入优化
SET GLOBAL bulk_insert_buffer_size = 67108864;  -- 64MB
SET GLOBAL innodb_autoinc_lock_mode = 2;  -- 交错锁模式

-- 10. 启用本地文件加载（用于 LOAD DATA INFILE）
SET GLOBAL local_infile = 1;

-- 11. 分区表优化
SET GLOBAL innodb_file_per_table = 1;  -- 每个表独立表空间

-- 12. 慢查询日志
SET GLOBAL slow_query_log = 1;
SET GLOBAL long_query_time = 2;  -- 2秒以上记录

-- 13. 二进制日志（生产环境建议开启）
-- SET GLOBAL binlog_format = 'ROW';
-- SET GLOBAL expire_logs_days = 7;

-- ============================================
-- 会话级优化（在ETL脚本中使用）
-- ============================================

-- 禁用约束检查（加载数据时）
SET SESSION foreign_key_checks = 0;
SET SESSION unique_checks = 0;

-- 禁用自动提交
SET SESSION autocommit = 0;

-- 增大批量插入缓冲
SET SESSION bulk_insert_buffer_size = 134217728;  -- 128MB

-- ============================================
-- 索引优化建议
-- ============================================

-- 查看索引使用情况
-- SELECT * FROM sys.schema_unused_indexes;

-- 查看重复索引
-- SELECT * FROM sys.schema_redundant_indexes;

-- 分析表统计信息
-- ANALYZE TABLE ods_orders;
-- ANALYZE TABLE ods_order_details;
-- ANALYZE TABLE dwd_fact_order;
-- ANALYZE TABLE dwd_fact_order_detail;

-- ============================================
-- 分区维护脚本
-- ============================================

-- 添加新分区（每月执行）
-- ALTER TABLE ods_orders ADD PARTITION (
--     PARTITION p202602 VALUES LESS THAN (202603)
-- );

-- 删除旧分区（归档历史数据）
-- ALTER TABLE ods_orders DROP PARTITION p202401;

-- 查看分区信息
-- SELECT 
--     TABLE_NAME,
--     PARTITION_NAME,
--     PARTITION_ORDINAL_POSITION,
--     TABLE_ROWS,
--     DATA_LENGTH / 1024 / 1024 AS data_mb
-- FROM information_schema.PARTITIONS
-- WHERE TABLE_SCHEMA = 'datas'
--   AND TABLE_NAME IN ('ods_orders', 'ods_order_details')
-- ORDER BY TABLE_NAME, PARTITION_ORDINAL_POSITION;
