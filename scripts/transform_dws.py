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
        
        # 创建订单业务处理宽表
        print("\n1. 创建订单业务处理宽表...")
        
        if mode == 'full':
            execute_sql(conn, "DROP TABLE IF EXISTS dws_order_business", "删除旧表 dws_order_business")
        
        sql_order_business = """
        CREATE TABLE IF NOT EXISTS dws_order_business
        ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        AS
        SELECT 
            订单明细ID,
            订单ID,
            下单时间,
            订单日期,
            年, 月, 日, 星期, 小时, 年月,
            CASE 
                WHEN 订单状态 = '退款' THEN '售后完成'
                ELSE 订单状态
            END AS 订单状态,
            用户ID, 用户名, 用户性别, 用户年龄, 用户年龄段, 用户城市,
            店铺ID, 店铺名称, 平台,
            商品ID, 商品名称, 一级类目, 二级类目,
            数量, 单价,
            CASE 
                WHEN 订单状态 IN ('已完成', '已发货') THEN 实收金额
                ELSE 0
            END AS 实收金额,
            CASE 
                WHEN 订单状态 IN ('已完成', '已发货') THEN 0
                ELSE 实收金额
            END AS 退款金额,
            商品单位成本,
            商品成本金额,
            分摊运费成本,
            CASE 
                WHEN 订单状态 IN ('已完成', '已发货') THEN 实收金额
                ELSE 0
            END - 商品成本金额 - 分摊运费成本 AS 毛利,
            CASE 
                WHEN 订单状态 IN ('已完成', '已发货') AND 实收金额 > 0 
                THEN ROUND((实收金额 - 商品成本金额 - 分摊运费成本) / 实收金额 * 100, 2)
                ELSE 0
            END AS 毛利率,
            CASE 
                WHEN 订单状态 IN ('已完成', '已发货') THEN ROUND(实收金额 * 0.05, 2)
                ELSE 0
            END AS 平台费,
            CASE 
                WHEN 订单状态 IN ('已完成', '已发货') THEN ROUND(实收金额 * 0.02, 2)
                ELSE 0
            END AS 售后费,
            ROUND(实收金额 * 0.10, 2) AS 管理费,
            推广费,
            是否推广成交,
            投产比,
            CASE 
                WHEN 订单状态 IN ('已完成', '已发货') THEN 实收金额
                ELSE 0
            END - CASE 
                WHEN 订单状态 IN ('已完成', '已发货') THEN ROUND(实收金额 * 0.05, 2)
                ELSE 0
            END - CASE 
                WHEN 订单状态 IN ('已完成', '已发货') THEN ROUND(实收金额 * 0.02, 2)
                ELSE 0
            END - ROUND(实收金额 * 0.10, 2) - 商品成本金额 - 分摊运费成本 - 推广费 AS 净利润,
            CASE 
                WHEN 订单状态 IN ('已完成', '已发货') AND 实收金额 > 0 
                THEN ROUND((实收金额 - ROUND(实收金额 * 0.05, 2) - ROUND(实收金额 * 0.02, 2) - ROUND(实收金额 * 0.10, 2) - 商品成本金额 - 分摊运费成本 - 推广费) / 实收金额 * 100, 2)
                ELSE 0
            END AS 净利率,
            支付方式,
            推广渠道
        FROM dwd_order_detail_wide
        """
        
        if not execute_sql(conn, sql_order_business, "创建订单业务处理宽表"):
            return False
        
        # 创建退货率汇总表
        print("\n2. 创建退货率汇总表...")
        
        if mode == 'full':
            execute_sql(conn, "DROP TABLE IF EXISTS dws_return_rate", "删除旧表 dws_return_rate")
        
        sql_return_rate = """
        CREATE TABLE IF NOT EXISTS dws_return_rate
        ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        AS
        SELECT 
            '全部' AS 维度类型,
            '全部' AS 维度值,
            COUNT(DISTINCT CASE WHEN 订单状态 = '售后完成' THEN 订单ID END) AS 售后完成订单数,
            COUNT(DISTINCT CASE WHEN 订单状态 IN ('已完成', '已发货') THEN 订单ID END) AS 正常订单数,
            COUNT(DISTINCT CASE WHEN 订单状态 IN ('已完成', '已发货', '售后完成') THEN 订单ID END) AS 总有效订单数,
            CASE 
                WHEN COUNT(DISTINCT CASE WHEN 订单状态 IN ('已完成', '已发货') THEN 订单ID END) > 0 
                THEN ROUND(COUNT(DISTINCT CASE WHEN 订单状态 = '售后完成' THEN 订单ID END) * 100.0 / COUNT(DISTINCT CASE WHEN 订单状态 IN ('已完成', '已发货') THEN 订单ID END), 2)
                ELSE 0
            END AS 退货率
        FROM dws_order_business
        
        UNION ALL
        
        SELECT 
            '平台' AS 维度类型,
            平台 AS 维度值,
            COUNT(DISTINCT CASE WHEN 订单状态 = '售后完成' THEN 订单ID END) AS 售后完成订单数,
            COUNT(DISTINCT CASE WHEN 订单状态 IN ('已完成', '已发货') THEN 订单ID END) AS 正常订单数,
            COUNT(DISTINCT CASE WHEN 订单状态 IN ('已完成', '已发货', '售后完成') THEN 订单ID END) AS 总有效订单数,
            CASE 
                WHEN COUNT(DISTINCT CASE WHEN 订单状态 IN ('已完成', '已发货') THEN 订单ID END) > 0 
                THEN ROUND(COUNT(DISTINCT CASE WHEN 订单状态 = '售后完成' THEN 订单ID END) * 100.0 / COUNT(DISTINCT CASE WHEN 订单状态 IN ('已完成', '已发货') THEN 订单ID END), 2)
                ELSE 0
            END AS 退货率
        FROM dws_order_business
        GROUP BY 平台
        
        UNION ALL
        
        SELECT 
            '店铺' AS 维度类型,
            店铺名称 AS 维度值,
            COUNT(DISTINCT CASE WHEN 订单状态 = '售后完成' THEN 订单ID END) AS 售后完成订单数,
            COUNT(DISTINCT CASE WHEN 订单状态 IN ('已完成', '已发货') THEN 订单ID END) AS 正常订单数,
            COUNT(DISTINCT CASE WHEN 订单状态 IN ('已完成', '已发货', '售后完成') THEN 订单ID END) AS 总有效订单数,
            CASE 
                WHEN COUNT(DISTINCT CASE WHEN 订单状态 IN ('已完成', '已发货') THEN 订单ID END) > 0 
                THEN ROUND(COUNT(DISTINCT CASE WHEN 订单状态 = '售后完成' THEN 订单ID END) * 100.0 / COUNT(DISTINCT CASE WHEN 订单状态 IN ('已完成', '已发货') THEN 订单ID END), 2)
                ELSE 0
            END AS 退货率
        FROM dws_order_business
        GROUP BY 店铺名称
        
        UNION ALL
        
        SELECT 
            '一级类目' AS 维度类型,
            一级类目 AS 维度值,
            COUNT(DISTINCT CASE WHEN 订单状态 = '售后完成' THEN 订单ID END) AS 售后完成订单数,
            COUNT(DISTINCT CASE WHEN 订单状态 IN ('已完成', '已发货') THEN 订单ID END) AS 正常订单数,
            COUNT(DISTINCT CASE WHEN 订单状态 IN ('已完成', '已发货', '售后完成') THEN 订单ID END) AS 总有效订单数,
            CASE 
                WHEN COUNT(DISTINCT CASE WHEN 订单状态 IN ('已完成', '已发货') THEN 订单ID END) > 0 
                THEN ROUND(COUNT(DISTINCT CASE WHEN 订单状态 = '售后完成' THEN 订单ID END) * 100.0 / COUNT(DISTINCT CASE WHEN 订单状态 IN ('已完成', '已发货') THEN 订单ID END), 2)
                ELSE 0
            END AS 退货率
        FROM dws_order_business
        GROUP BY 一级类目
        """
        
        if not execute_sql(conn, sql_return_rate, "创建退货率汇总表"):
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


def create_traffic_summary_impl_disabled(conn, mode):
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


def create_promotion_summary_impl_disabled(conn, mode):
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
