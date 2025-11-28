"""
DWD层到DWS层数据转换
通过SQL在数据库中直接转换
支持多线程并行创建汇总表
"""
import pymysql
import sys
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import time


def get_db_connection(db_config):
    """获取数据库连接（优化版）"""
    try:
        conn = pymysql.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            charset='utf8mb4',
            connect_timeout=30,
            read_timeout=3600,  # 1小时读超时
            write_timeout=3600  # 1小时写超时
        )
        return conn
    except Exception as e:
        print(f"数据库连接失败: {e}")
        return None


def execute_sql(conn, sql, description):
    """执行SQL语句（极速模式）"""
    try:
        print(f"  正在执行: {description}...")
        sys.stdout.flush()
        
        cursor = conn.cursor()
        
        # 极速优化
        cursor.execute("SET unique_checks=0")
        cursor.execute("SET foreign_key_checks=0")
        cursor.execute("SET autocommit=0")
        
        # 尝试设置需要特殊权限的参数
        try:
            cursor.execute("SET sql_log_bin=0")  # 需要SUPER权限
        except:
            pass
        
        try:
            cursor.execute("SET innodb_flush_log_at_trx_commit=2")  # 需要SUPER权限
        except:
            pass
        
        cursor.execute(sql)
        
        # 恢复设置
        cursor.execute("SET unique_checks=1")
        cursor.execute("SET foreign_key_checks=1")
        
        try:
            cursor.execute("SET sql_log_bin=1")
        except:
            pass
        
        try:
            cursor.execute("SET innodb_flush_log_at_trx_commit=1")
        except:
            pass
        
        conn.commit()
        affected_rows = cursor.rowcount
        cursor.close()
        
        print(f"  ✓ {description} - 影响 {affected_rows:,} 行")
        sys.stdout.flush()
        return True
    except Exception as e:
        print(f"  ✗ {description} 失败: {e}")
        sys.stdout.flush()
        import traceback
        traceback.print_exc()
        return False


def transform_dws(mode='full', db_config=None):
    """
    转换DWS层数据
    mode: 'full' 全量模式（删除重建）, 'incremental' 增量模式（追加）
    """
    print("="*60)
    print("DWS层数据转换")
    print("="*60)
    
    conn = get_db_connection(db_config)
    if not conn:
        return False
    
    try:
        # 优化临时表空间和内存使用（避免硬盘被写爆）
        print("\n优化临时表空间配置...")
        sys.stdout.flush()
        cursor = conn.cursor()
        
        try:
            # 增加临时表大小限制（避免使用磁盘临时表）- 使用字节数
            cursor.execute("SET SESSION tmp_table_size = 536870912")  # 512MB
            cursor.execute("SET SESSION max_heap_table_size = 536870912")  # 512MB
            
            # 优化排序缓冲区
            cursor.execute("SET SESSION sort_buffer_size = 67108864")  # 64MB
            cursor.execute("SET SESSION read_rnd_buffer_size = 33554432")  # 32MB
            
            # 优化 JOIN 缓冲区
            cursor.execute("SET SESSION join_buffer_size = 67108864")  # 64MB
            
            # 禁用查询缓存（MySQL 8.0 已移除，兼容处理）
            try:
                cursor.execute("SET SESSION query_cache_type = OFF")
            except:
                pass
            
            print("  ✓ 临时表空间配置已优化")
            print("  - 临时表内存: 512MB")
            print("  - 排序缓冲区: 64MB")
            print("  - JOIN缓冲区: 64MB")
            sys.stdout.flush()
        except Exception as e:
            print(f"  警告: 部分优化设置失败 - {e}")
            sys.stdout.flush()
        
        # 创建索引以加速查询
        print("\n创建索引...")
        sys.stdout.flush()
        
        # 为 dwd_order_fact 创建索引（忽略已存在的索引）
        try:
            print("  正在为 dwd_order_fact 创建索引...")
            sys.stdout.flush()
            try:
                cursor.execute("CREATE INDEX idx_order_date ON dwd_order_fact(订单日期(20))")
            except:
                pass
            try:
                cursor.execute("CREATE INDEX idx_order_status ON dwd_order_fact(订单状态(20))")
            except:
                pass
            try:
                cursor.execute("CREATE INDEX idx_order_store ON dwd_order_fact(店铺ID)")
            except:
                pass
            print("  ✓ dwd_order_fact 索引创建完成")
            sys.stdout.flush()
        except Exception as e:
            print(f"  索引创建警告: {e}")
            sys.stdout.flush()
        
        # 为 dwd_order_detail_fact 创建索引
        try:
            print("  正在为 dwd_order_detail_fact 创建索引...")
            sys.stdout.flush()
            try:
                cursor.execute("CREATE INDEX idx_detail_order ON dwd_order_detail_fact(订单ID)")
            except:
                pass
            try:
                cursor.execute("CREATE INDEX idx_detail_product ON dwd_order_detail_fact(商品ID)")
            except:
                pass
            print("  ✓ dwd_order_detail_fact 索引创建完成")
            sys.stdout.flush()
        except Exception as e:
            print(f"  索引创建警告: {e}")
            sys.stdout.flush()
        
        conn.commit()
        cursor.close()
        
        # 使用多线程并行创建4个汇总表
        print("\n使用多线程并行创建汇总表...")
        sys.stdout.flush()
        
        # 保存 db_config 到局部变量
        _db_config = db_config
        _mode = mode
        
        # 定义4个汇总表的创建函数
        def create_sales_summary():
            """创建销售汇总表"""
            conn_local = get_db_connection(_db_config)
            if not conn_local:
                return False, "销售汇总表", "数据库连接失败"
            
            try:
                return create_sales_summary_impl(conn_local, _mode)
            finally:
                conn_local.close()
        
        def create_traffic_summary():
            """创建流量汇总表"""
            conn_local = get_db_connection(_db_config)
            if not conn_local:
                return False, "流量汇总表", "数据库连接失败"
            
            try:
                return create_traffic_summary_impl(conn_local, _mode)
            finally:
                conn_local.close()
        
        def create_inventory_summary():
            """创建库存汇总表"""
            conn_local = get_db_connection(_db_config)
            if not conn_local:
                return False, "库存汇总表", "数据库连接失败"
            
            try:
                return create_inventory_summary_impl(conn_local, _mode)
            finally:
                conn_local.close()
        
        def create_promotion_summary():
            """创建推广汇总表"""
            conn_local = get_db_connection(_db_config)
            if not conn_local:
                return False, "推广汇总表", "数据库连接失败"
            
            try:
                return create_promotion_summary_impl(conn_local, _mode)
            finally:
                conn_local.close()
        
        # 使用4线程并行创建（任何一个失败就停止）
        start_time = time.time()
        success_count = 0
        failed_table = None
        error_message = None
        
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {
                executor.submit(create_sales_summary): "销售汇总表",
                executor.submit(create_traffic_summary): "流量汇总表",
                executor.submit(create_inventory_summary): "库存汇总表",
                executor.submit(create_promotion_summary): "推广汇总表"
            }
            
            try:
                for future in as_completed(futures):
                    table_name = futures[future]
                    try:
                        success, name, message = future.result()
                        if success:
                            print(f"  ✓ {name} 创建完成")
                            sys.stdout.flush()
                            success_count += 1
                        else:
                            # 失败：记录错误并取消其他任务
                            failed_table = name
                            error_message = message
                            print(f"  ✗ {name} 创建失败: {message}")
                            sys.stdout.flush()
                            
                            # 取消所有未完成的任务
                            for f in futures:
                                f.cancel()
                            
                            raise Exception(f"{name} 创建失败: {message}")
                            
                    except Exception as e:
                        # 异常：记录错误并取消其他任务
                        if not failed_table:
                            failed_table = table_name
                            error_message = str(e)
                        
                        print(f"  ✗ {table_name} 创建异常: {e}")
                        sys.stdout.flush()
                        
                        # 取消所有未完成的任务
                        for f in futures:
                            f.cancel()
                        
                        raise
                        
            except Exception as e:
                elapsed = time.time() - start_time
                print(f"\n多线程创建失败: {failed_table or '未知表'} 出错，已停止所有任务")
                print(f"错误信息: {error_message or str(e)}")
                print(f"耗时: {elapsed:.1f}秒")
                sys.stdout.flush()
                return False
        
        elapsed = time.time() - start_time
        print(f"\n多线程创建完成: {success_count}/4 个表成功，耗时 {elapsed:.1f}秒")
        sys.stdout.flush()
        
        if success_count < 4:
            print(f"警告: 只有 {success_count} 个表创建成功")
            sys.stdout.flush()
            return False
        
        print("\n" + "="*60)
        print("✓ DWS层转换完成！")
        print("="*60)
        sys.stdout.flush()
        
        return True
        
    except Exception as e:
        print(f"✗ 转换失败: {e}")
        sys.stdout.flush()
        return False
    finally:
        conn.close()


def create_sales_summary_impl(conn, mode):
    """实现：创建销售汇总表（按商品分批聚合 - 避免内存溢出）"""
    try:
        print("销售汇总表：开始创建（按商品分批聚合）...")
        sys.stdout.flush()
        
        cursor = conn.cursor()
        
        if mode == 'full':
            cursor.execute("DROP TABLE IF EXISTS dws_sales_summary")
            conn.commit()
        
        # 第一步：创建空表结构
        print("销售汇总表：创建表结构...")
        sys.stdout.flush()
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS dws_sales_summary (
            日期 VARCHAR(20),
            商品ID VARCHAR(50),
            商品名称 VARCHAR(200),
            平台 VARCHAR(50),
            店铺ID VARCHAR(50),
            店铺名称 VARCHAR(200),
            一级类目 VARCHAR(100),
            二级类目 VARCHAR(100),
            订单数 INT DEFAULT 0,
            销售件数 INT DEFAULT 0,
            销售额 DECIMAL(15,2) DEFAULT 0,
            成本 DECIMAL(15,2) DEFAULT 0,
            毛利 DECIMAL(15,2) DEFAULT 0,
            毛利率 DECIMAL(10,2) DEFAULT 0,
            客单价 DECIMAL(10,2) DEFAULT 0,
            PRIMARY KEY (日期, 商品ID, 店铺ID)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        conn.commit()
        
        # 第二步：按店铺分批聚合（避免一次性处理所有数据）
        print("销售汇总表：开始分批聚合（按店铺维度）...")
        sys.stdout.flush()
        
        # 设置会话级优化参数
        cursor.execute("SET SESSION tmp_table_size = 2147483648")  # 2GB
        cursor.execute("SET SESSION max_heap_table_size = 2147483648")  # 2GB
        cursor.execute("SET SESSION sort_buffer_size = 268435456")  # 256MB
        cursor.execute("SET SESSION join_buffer_size = 268435456")  # 256MB
        cursor.execute("SET unique_checks=0")
        cursor.execute("SET foreign_key_checks=0")
        cursor.execute("SET autocommit=0")
        
        try:
            cursor.execute("SET sql_log_bin=0")
        except:
            pass
        
        try:
            cursor.execute("SET innodb_flush_log_at_trx_commit=0")
        except:
            pass
        
        # 获取所有店铺列表
        cursor.execute("SELECT DISTINCT 店铺ID, 店铺名称, 平台 FROM dwd_order_fact WHERE 订单状态 = '已完成'")
        stores = cursor.fetchall()
        total_stores = len(stores)
        
        print(f"销售汇总表：共 {total_stores} 个店铺需要处理")
        sys.stdout.flush()
        
        # 按店铺分批处理
        for idx, (store_id, store_name, platform) in enumerate(stores, 1):
            # 每个店铺单独聚合（按日期维度）
            cursor.execute("""
            INSERT INTO dws_sales_summary (
                日期, 商品ID, 商品名称, 平台, 店铺ID, 店铺名称, 一级类目, 二级类目, 
                订单数, 销售件数, 销售额, 成本, 毛利, 毛利率, 客单价
            )
            SELECT 
                o.订单日期 AS 日期,
                od.商品ID,
                od.商品名称,
                %s AS 平台,
                %s AS 店铺ID,
                %s AS 店铺名称,
                od.一级类目,
                od.二级类目,
                COUNT(DISTINCT od.订单ID) AS 订单数,
                SUM(od.数量) AS 销售件数,
                SUM(od.金额) AS 销售额,
                SUM(od.成本金额) AS 成本,
                SUM(od.毛利) AS 毛利,
                ROUND(AVG(od.毛利率), 2) AS 毛利率,
                ROUND(SUM(od.金额) / COUNT(DISTINCT od.订单ID), 2) AS 客单价
            FROM dwd_order_detail_fact od
            INNER JOIN dwd_order_fact o ON od.订单ID = o.订单ID
            WHERE o.订单状态 = '已完成' AND o.店铺ID = %s
            GROUP BY o.订单日期, od.商品ID, od.商品名称, od.一级类目, od.二级类目
            """, (platform, store_id, store_name, store_id))
            
            # 每处理5个店铺提交一次
            if idx % 5 == 0:
                conn.commit()
                progress = int((idx / total_stores) * 100)
                print(f"销售汇总表：进度 {progress}% ({idx}/{total_stores})")
                sys.stdout.flush()
        
        # 最后提交
        conn.commit()
        
        # 恢复设置
        cursor.execute("SET unique_checks=1")
        cursor.execute("SET foreign_key_checks=1")
        try:
            cursor.execute("SET sql_log_bin=1")
        except:
            pass
        try:
            cursor.execute("SET innodb_flush_log_at_trx_commit=1")
        except:
            pass
        
        # 获取最终行数
        cursor.execute("SELECT COUNT(*) FROM dws_sales_summary")
        row_count = cursor.fetchone()[0]
        print(f"销售汇总表：数据聚合完成（{row_count:,} 个商品），正在创建索引...")
        sys.stdout.flush()
        
        # 创建索引
        try:
            cursor.execute("ALTER TABLE dws_sales_summary ADD INDEX idx_date (日期)")
            cursor.execute("ALTER TABLE dws_sales_summary ADD INDEX idx_platform (平台)")
            cursor.execute("ALTER TABLE dws_sales_summary ADD INDEX idx_store (店铺ID)")
            cursor.execute("ALTER TABLE dws_sales_summary ADD INDEX idx_category (一级类目)")
            conn.commit()
            print("销售汇总表：索引创建完成")
            sys.stdout.flush()
        except Exception as e:
            print(f"销售汇总表：索引创建警告 - {e}")
            sys.stdout.flush()
        
        cursor.close()
        return True, "销售汇总表", "成功"
        
    except Exception as e:
        error_msg = str(e)
        print(f"销售汇总表：创建失败 - {error_msg}")
        sys.stdout.flush()
        import traceback
        traceback.print_exc()
        return False, "销售汇总表", error_msg


def create_traffic_summary_impl(conn, mode):
    """实现：创建流量汇总表（两步优化版）"""
    try:
        print("流量汇总表：开始创建...")
        sys.stdout.flush()
        
        cursor = conn.cursor()
        
        if mode == 'full':
            cursor.execute("DROP TABLE IF EXISTS dws_traffic_summary")
            conn.commit()
        
        # 优化参数
        cursor.execute("SET unique_checks=0")
        cursor.execute("SET foreign_key_checks=0")
        cursor.execute("SET autocommit=0")
        
        try:
            cursor.execute("SET sql_log_bin=0")
        except:
            pass
        
        try:
            cursor.execute("SET innodb_flush_log_at_trx_commit=0")
        except:
            pass
        
        # 第一步：创建临时订单汇总表（预聚合，避免重复计算）
        print("流量汇总表：预聚合订单数据...")
        sys.stdout.flush()
        
        cursor.execute("""
        CREATE TEMPORARY TABLE tmp_order_summary (
            店铺ID VARCHAR(50),
            订单日期 VARCHAR(20),
            成交订单数 INT,
            成交金额 DECIMAL(15,2),
            成交客户数 INT,
            INDEX idx_store_date (店铺ID, 订单日期)
        ) ENGINE=InnoDB
        """)
        
        cursor.execute("""
        INSERT INTO tmp_order_summary
        SELECT 
            店铺ID,
            订单日期,
            COUNT(DISTINCT 订单ID) AS 成交订单数,
            SUM(实付金额) AS 成交金额,
            COUNT(DISTINCT 用户ID) AS 成交客户数
        FROM dwd_order_fact
        WHERE 订单状态 = '已完成'
        GROUP BY 店铺ID, 订单日期
        """)
        conn.commit()
        
        print("流量汇总表：订单数据预聚合完成，开始关联流量数据...")
        sys.stdout.flush()
        
        # 第二步：关联流量数据（快速）
        cursor.execute("""
        CREATE TABLE dws_traffic_summary
        ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        AS
        SELECT 
            t.date AS 日期,
            t.store_id AS 店铺ID,
            t.platform AS 平台,
            COALESCE(s.店铺名称, '') AS 店铺名称,
            t.visitors AS 访客数,
            t.page_views AS 浏览量,
            t.search_traffic AS 搜索流量,
            t.recommend_traffic AS 推荐流量,
            t.direct_traffic AS 直接访问,
            t.other_traffic AS 其他流量,
            t.avg_stay_time AS 平均停留时长,
            t.bounce_rate AS 跳失率,
            COALESCE(agg.成交订单数, 0) AS 成交订单数,
            COALESCE(agg.成交金额, 0) AS 成交金额,
            COALESCE(agg.成交客户数, 0) AS 成交客户数,
            CASE 
                WHEN t.visitors > 0 THEN ROUND(COALESCE(agg.成交客户数, 0) / t.visitors * 100, 2)
                ELSE 0
            END AS 访问转化率,
            CASE 
                WHEN t.visitors > 0 THEN ROUND(COALESCE(agg.成交订单数, 0) / t.visitors * 100, 2)
                ELSE 0
            END AS 下单转化率,
            CASE 
                WHEN COALESCE(agg.成交订单数, 0) > 0 THEN ROUND(COALESCE(agg.成交金额, 0) / agg.成交订单数, 2)
                ELSE 0
            END AS 客单价,
            CASE 
                WHEN t.visitors > 0 THEN ROUND(t.page_views / t.visitors, 2)
                ELSE 0
            END AS 人均浏览页数,
            CASE 
                WHEN t.visitors > 0 THEN ROUND(t.search_traffic / t.visitors * 100, 2)
                ELSE 0
            END AS 搜索流量占比,
            CASE 
                WHEN t.visitors > 0 THEN ROUND(t.recommend_traffic / t.visitors * 100, 2)
                ELSE 0
            END AS 推荐流量占比,
            CASE 
                WHEN t.visitors > 0 THEN ROUND(t.direct_traffic / t.visitors * 100, 2)
                ELSE 0
            END AS 直接访问占比
        FROM ods_traffic t
        LEFT JOIN dim_store s ON t.store_id = s.店铺ID
        LEFT JOIN tmp_order_summary agg ON t.store_id = agg.店铺ID AND t.date = agg.订单日期
        """)
        
        # 删除临时表
        cursor.execute("DROP TEMPORARY TABLE IF EXISTS tmp_order_summary")
        
        # 恢复设置
        cursor.execute("SET unique_checks=1")
        cursor.execute("SET foreign_key_checks=1")
        try:
            cursor.execute("SET sql_log_bin=1")
        except:
            pass
        try:
            cursor.execute("SET innodb_flush_log_at_trx_commit=1")
        except:
            pass
        
        conn.commit()
        
        # 获取创建的行数
        cursor.execute("SELECT COUNT(*) FROM dws_traffic_summary")
        row_count = cursor.fetchone()[0]
        print(f"流量汇总表：数据创建完成（{row_count:,} 行），正在创建索引...")
        sys.stdout.flush()
        
        # 创建索引（TEXT 字段需要指定长度）
        try:
            cursor.execute("ALTER TABLE dws_traffic_summary ADD INDEX idx_date (日期(20))")
            cursor.execute("ALTER TABLE dws_traffic_summary ADD INDEX idx_store (店铺ID)")
            conn.commit()
            print("流量汇总表：索引创建完成")
            sys.stdout.flush()
        except Exception as e:
            print(f"流量汇总表：索引创建警告 - {e}")
            sys.stdout.flush()
        cursor.close()
        
        return True, "流量汇总表", "成功"
        
    except Exception as e:
        error_msg = str(e)
        print(f"流量汇总表：创建失败 - {error_msg}")
        sys.stdout.flush()
        import traceback
        traceback.print_exc()
        return False, "流量汇总表", error_msg


def create_inventory_summary_impl(conn, mode):
    """实现：创建库存汇总表（极致优化版）"""
    try:
        print("库存汇总表：开始创建...")
        sys.stdout.flush()
        
        if mode == 'full':
            cursor = conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS dws_inventory_summary")
            conn.commit()
            cursor.close()
        
        # 优化：从商品表复制，添加统计日期
        sql_inventory_summary = """
        CREATE TABLE dws_inventory_summary
        ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        AS
        SELECT 
            CURDATE() AS 统计日期,
            商品ID,
            商品名称,
            店铺ID,
            店铺名称,
            平台,
            一级类目,
            二级类目,
            售价,
            成本,
            库存 AS 当前库存,
            (成本 * 库存) AS 库存金额_成本,
            (售价 * 库存) AS 库存金额_售价
        FROM dim_product
        """
        
        cursor = conn.cursor()
        cursor.execute("SET unique_checks=0")
        cursor.execute("SET foreign_key_checks=0")
        cursor.execute("SET autocommit=0")
        
        try:
            cursor.execute("SET sql_log_bin=0")
        except:
            pass
        
        try:
            cursor.execute("SET innodb_flush_log_at_trx_commit=0")
        except:
            pass
        
        print("库存汇总表：开始执行 SQL（快速）...")
        sys.stdout.flush()
        
        cursor.execute(sql_inventory_summary)
        
        # 恢复设置
        cursor.execute("SET unique_checks=1")
        cursor.execute("SET foreign_key_checks=1")
        try:
            cursor.execute("SET sql_log_bin=1")
        except:
            pass
        try:
            cursor.execute("SET innodb_flush_log_at_trx_commit=1")
        except:
            pass
        
        conn.commit()
        
        # 获取创建的行数
        cursor.execute("SELECT COUNT(*) FROM dws_inventory_summary")
        row_count = cursor.fetchone()[0]
        print(f"库存汇总表：数据创建完成（{row_count:,} 行）")
        sys.stdout.flush()
        
        cursor.close()
        
        return True, "库存汇总表", "成功"
        
    except Exception as e:
        error_msg = str(e)
        print(f"库存汇总表：创建失败 - {error_msg}")
        sys.stdout.flush()
        import traceback
        traceback.print_exc()
        return False, "库存汇总表", error_msg


def create_promotion_summary_impl(conn, mode):
    """实现：创建推广汇总表（极致优化版）"""
    try:
        print("推广汇总表：开始创建...")
        sys.stdout.flush()
        
        if mode == 'full':
            cursor = conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS dws_promotion_summary")
            conn.commit()
            cursor.close()
        
        # 优化：简化聚合，日期字段已存在
        sql_promotion_summary = """
        CREATE TABLE dws_promotion_summary
        ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        AS
        SELECT 
            pm.date AS 日期,
            pm.platform AS 平台,
            pm.store_id AS 店铺ID,
            pm.product_id AS 商品ID,
            pm.channel AS 推广渠道,
            SUM(pm.cost) AS 推广花费,
            SUM(pm.impressions) AS 曝光量,
            SUM(pm.clicks) AS 点击量,
            CASE 
                WHEN SUM(pm.impressions) > 0 THEN ROUND(SUM(pm.clicks) / SUM(pm.impressions) * 100, 2)
                ELSE 0
            END AS 点击率,
            CASE 
                WHEN SUM(pm.clicks) > 0 THEN ROUND(SUM(pm.cost) / SUM(pm.clicks), 2)
                ELSE 0
            END AS 平均点击成本
        FROM ods_promotion pm
        GROUP BY pm.date, pm.platform, pm.store_id, pm.product_id, pm.channel
        """
        
        cursor = conn.cursor()
        cursor.execute("SET unique_checks=0")
        cursor.execute("SET foreign_key_checks=0")
        cursor.execute("SET autocommit=0")
        
        try:
            cursor.execute("SET sql_log_bin=0")
        except:
            pass
        
        try:
            cursor.execute("SET innodb_flush_log_at_trx_commit=0")
        except:
            pass
        
        cursor.execute(sql_promotion_summary)
        
        # 恢复设置
        cursor.execute("SET unique_checks=1")
        cursor.execute("SET foreign_key_checks=1")
        try:
            cursor.execute("SET sql_log_bin=1")
        except:
            pass
        try:
            cursor.execute("SET innodb_flush_log_at_trx_commit=1")
        except:
            pass
        
        conn.commit()
        
        # 获取创建的行数
        cursor.execute("SELECT COUNT(*) FROM dws_promotion_summary")
        row_count = cursor.fetchone()[0]
        print(f"推广汇总表：数据创建完成（{row_count:,} 行），正在创建索引...")
        sys.stdout.flush()
        
        # 创建索引（TEXT 字段需要指定长度）
        try:
            cursor.execute("ALTER TABLE dws_promotion_summary ADD INDEX idx_date (日期(20))")
            cursor.execute("ALTER TABLE dws_promotion_summary ADD INDEX idx_product (商品ID(50))")
            conn.commit()
            print("推广汇总表：索引创建完成")
            sys.stdout.flush()
        except Exception as e:
            print(f"推广汇总表：索引创建警告 - {e}")
            sys.stdout.flush()
        cursor.close()
        
        return True, "推广汇总表", "成功"
        
    except Exception as e:
        error_msg = str(e)
        print(f"推广汇总表：创建失败 - {error_msg}")
        sys.stdout.flush()
        import traceback
        traceback.print_exc()
        return False, "推广汇总表", error_msg


def main():
    """主函数"""
    # 读取配置参数
    config = {}
    if len(sys.argv) > 1:
        try:
            config = json.loads(sys.argv[1])
        except:
            pass
    
    # 获取数据库配置
    db_config = config.get('dbConfig', {
        'host': 'localhost',
        'port': 3306,
        'database': 'datas',
        'user': 'root',
        'password': ''
    })
    
    mode = config.get('mode', 'full')
    
    print(f"模式: {'全量（删除重建）' if mode == 'full' else '增量（追加数据）'}")
    
    success = transform_dws(mode, db_config)
    
    if not success:
        sys.exit(1)


if __name__ == '__main__':
    main()
