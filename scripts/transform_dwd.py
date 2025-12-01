"""
ODS层到DWD层数据转换
通过SQL在数据库中直接转换，而不是读取CSV
"""
import pymysql
import sys
import json


def get_db_connection(db_config):
    """获取数据库连接"""
    try:
        conn = pymysql.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            charset='utf8mb4'
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
        return False


def transform_dwd(mode='full', db_config=None):
    """
    转换DWD层数据
    mode: 'full' 全量模式（删除重建）, 'incremental' 增量模式（追加）
    db_config: 数据库配置
    """
    print("="*60)
    print("DWD层数据转换")
    print("="*60)
    
    conn = get_db_connection(db_config)
    if not conn:
        return False
    
    try:
        # 优化数据库参数，防止崩溃
        print("\n优化数据库参数...")
        cursor = conn.cursor()
        try:
            # 增加临时表大小
            cursor.execute("SET SESSION tmp_table_size = 1073741824")  # 1GB
            cursor.execute("SET SESSION max_heap_table_size = 1073741824")  # 1GB
            # 增加排序缓冲区
            cursor.execute("SET SESSION sort_buffer_size = 134217728")  # 128MB
            cursor.execute("SET SESSION read_rnd_buffer_size = 67108864")  # 64MB
            # 增加JOIN缓冲区
            cursor.execute("SET SESSION join_buffer_size = 134217728")  # 128MB
            # 禁用查询缓存
            try:
                cursor.execute("SET SESSION query_cache_type = OFF")
            except:
                pass
            print("  ✓ 数据库参数优化完成")
        except Exception as e:
            print(f"  警告: 部分参数设置失败 - {e}")
        cursor.close()
        
        # 1. 构建订单事实表
        print("\n1. 构建订单事实表...")
        
        if mode == 'full':
            execute_sql(conn, "DROP TABLE IF EXISTS dwd_order_fact", "删除旧表 dwd_order_fact")
        
        # 创建索引以加速JOIN（忽略已存在的索引）
        print("  创建临时索引...")
        cursor = conn.cursor()
        indexes = [
            ("idx_user_id", "ods_orders", "user_id"),
            ("idx_store_id", "ods_orders", "store_id"),
            ("idx_order_time", "ods_orders", "order_time"),
            ("idx_product_id", "ods_order_details", "product_id"),
            ("idx_promo_product", "ods_promotion", "product_id, date")
        ]
        for idx_name, table_name, columns in indexes:
            try:
                cursor.execute(f"CREATE INDEX {idx_name} ON {table_name}({columns})")
            except:
                pass  # 索引已存在，忽略
        conn.commit()
        cursor.close()
        print("  ✓ 索引创建完成")
        
        # 先创建推广费临时表，避免复杂子查询
        print("  创建推广费临时表...")
        cursor = conn.cursor()
        cursor.execute("DROP TEMPORARY TABLE IF EXISTS tmp_order_promo")
        cursor.execute("""
            CREATE TEMPORARY TABLE tmp_order_promo (
                order_id VARCHAR(50),
                推广费 DECIMAL(15,2),
                PRIMARY KEY (order_id)
            ) ENGINE=InnoDB
        """)
        cursor.execute("""
            INSERT INTO tmp_order_promo
            SELECT 
                od.order_id,
                SUM(COALESCE(pm.cost, 0)) AS 推广费
            FROM ods_order_details od
            INNER JOIN ods_orders o2 ON od.order_id = o2.order_id
            LEFT JOIN ods_promotion pm ON 
                pm.product_id = od.product_id 
                AND pm.date = DATE(o2.order_time)
                AND pm.store_id = o2.store_id
                AND pm.platform = o2.platform
            GROUP BY od.order_id
        """)
        conn.commit()
        cursor.close()
        print("  ✓ 推广费临时表创建完成")
        
        sql_order_fact = """
        CREATE TABLE IF NOT EXISTS dwd_order_fact
        ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        AS
        SELECT 
            o.order_id AS 订单ID,
            o.user_id AS 用户ID,
            o.store_id AS 店铺ID,
            o.platform AS 平台,
            o.order_time AS 下单时间,
            o.order_status AS 订单状态,
            COALESCE(o.total_amount, 0) AS 商品总额,
            o.final_amount AS 实收金额,
            COALESCE(o.total_cost, 0) AS 商品成本,
            COALESCE(o.shipping_fee, 0) AS 运费成本,
            (o.final_amount - COALESCE(o.total_cost, 0) - COALESCE(o.shipping_fee, 0)) AS 毛利,
            CASE 
                WHEN o.final_amount > 0 THEN ROUND((o.final_amount - COALESCE(o.total_cost, 0) - COALESCE(o.shipping_fee, 0)) / o.final_amount * 100, 2)
                ELSE 0
            END AS 毛利率,
            ROUND(o.final_amount * 0.05, 2) AS 平台费,
            ROUND(o.final_amount * 0.02, 2) AS 售后费,
            ROUND(o.final_amount * 0.10, 2) AS 管理费,
            COALESCE(promo.推广费, 0) AS 推广费,
            CASE WHEN COALESCE(promo.推广费, 0) > 0 THEN 1 ELSE 0 END AS 是否推广成交,
            CASE 
                WHEN COALESCE(promo.推广费, 0) > 0 THEN ROUND(o.final_amount / promo.推广费, 2)
                ELSE 0
            END AS 投产比,
            (o.final_amount - ROUND(o.final_amount * 0.05, 2) - ROUND(o.final_amount * 0.02, 2) - ROUND(o.final_amount * 0.10, 2) - COALESCE(o.total_cost, 0) - COALESCE(o.shipping_fee, 0) - COALESCE(promo.推广费, 0)) AS 净利润,
            CASE 
                WHEN o.final_amount > 0 THEN ROUND((o.final_amount - ROUND(o.final_amount * 0.05, 2) - ROUND(o.final_amount * 0.02, 2) - ROUND(o.final_amount * 0.10, 2) - COALESCE(o.total_cost, 0) - COALESCE(o.shipping_fee, 0) - COALESCE(promo.推广费, 0)) / o.final_amount * 100, 2)
                ELSE 0
            END AS 净利率,
            COALESCE(o.payment_method, '') AS 支付方式,
            u.user_name AS 用户名,
            u.gender AS 用户性别,
            u.age AS 用户年龄,
            CASE 
                WHEN u.age <= 25 THEN '18-25岁'
                WHEN u.age <= 35 THEN '26-35岁'
                WHEN u.age <= 45 THEN '36-45岁'
                WHEN u.age <= 55 THEN '46-55岁'
                ELSE '55岁以上'
            END AS 用户年龄段,
            u.city AS 用户城市,
            s.store_name AS 店铺名称,
            DATE(o.order_time) AS 订单日期,
            YEAR(o.order_time) AS 年,
            MONTH(o.order_time) AS 月,
            DAY(o.order_time) AS 日,
            DAYOFWEEK(o.order_time) AS 星期,
            HOUR(o.order_time) AS 小时,
            DATE_FORMAT(o.order_time, '%Y-%m') AS 年月
        FROM ods_orders o
        LEFT JOIN ods_users u ON o.user_id = u.user_id
        LEFT JOIN ods_stores s ON o.store_id = s.store_id
        LEFT JOIN tmp_order_promo promo ON o.order_id = promo.order_id
        WHERE 
            o.order_id IS NOT NULL
            AND o.user_id IS NOT NULL
            AND o.store_id IS NOT NULL
            AND o.final_amount >= 0
            AND o.total_amount >= 0
        """
        
        if not execute_sql(conn, sql_order_fact, "创建订单事实表"):
            return False
        
        # 清理临时表
        cursor = conn.cursor()
        cursor.execute("DROP TEMPORARY TABLE IF EXISTS tmp_order_promo")
        cursor.close()
        print("  ✓ 临时表已清理")
        
        # 2. 构建订单明细大宽表（包含所有关键字段）
        print("\n2. 构建订单明细大宽表...")
        
        if mode == 'full':
            execute_sql(conn, "DROP TABLE IF EXISTS dwd_order_detail_wide", "删除旧表 dwd_order_detail_wide")
        
        sql_order_detail_wide = """
        CREATE TABLE IF NOT EXISTS dwd_order_detail_wide
        ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        AS
        SELECT 
            od.order_detail_id AS 订单明细ID,
            od.order_id AS 订单ID,
            o.order_time AS 下单时间,
            DATE(o.order_time) AS 订单日期,
            YEAR(o.order_time) AS 年,
            MONTH(o.order_time) AS 月,
            DAY(o.order_time) AS 日,
            DAYOFWEEK(o.order_time) AS 星期,
            HOUR(o.order_time) AS 小时,
            DATE_FORMAT(o.order_time, '%Y-%m') AS 年月,
            o.order_status AS 订单状态,
            o.user_id AS 用户ID,
            u.user_name AS 用户名,
            u.gender AS 用户性别,
            u.age AS 用户年龄,
            CASE 
                WHEN u.age <= 25 THEN '18-25岁'
                WHEN u.age <= 35 THEN '26-35岁'
                WHEN u.age <= 45 THEN '36-45岁'
                WHEN u.age <= 55 THEN '46-55岁'
                ELSE '55岁以上'
            END AS 用户年龄段,
            u.city AS 用户城市,
            o.store_id AS 店铺ID,
            s.store_name AS 店铺名称,
            o.platform AS 平台,
            od.product_id AS 商品ID,
            p.product_name AS 商品名称,
            p.category_l1 AS 一级类目,
            p.category_l2 AS 二级类目,
            od.quantity AS 数量,
            od.price AS 单价,
            od.amount AS 实收金额,
            COALESCE(p.cost, 0) AS 商品单位成本,
            (COALESCE(p.cost, 0) * od.quantity) AS 商品成本金额,
            ROUND(COALESCE(o.shipping_fee, 0) / order_item_count.item_count, 2) AS 分摊运费成本,
            (od.amount - (COALESCE(p.cost, 0) * od.quantity) - ROUND(COALESCE(o.shipping_fee, 0) / order_item_count.item_count, 2)) AS 毛利,
            CASE 
                WHEN od.amount > 0 THEN ROUND((od.amount - (COALESCE(p.cost, 0) * od.quantity) - ROUND(COALESCE(o.shipping_fee, 0) / order_item_count.item_count, 2)) / od.amount * 100, 2)
                ELSE 0
            END AS 毛利率,
            ROUND(od.amount * 0.05, 2) AS 平台费,
            ROUND(od.amount * 0.02, 2) AS 售后费,
            ROUND(od.amount * 0.10, 2) AS 管理费,
            COALESCE(pm.cost, 0) AS 推广费,
            CASE WHEN COALESCE(pm.cost, 0) > 0 THEN 1 ELSE 0 END AS 是否推广成交,
            CASE 
                WHEN COALESCE(pm.cost, 0) > 0 THEN ROUND(od.amount / pm.cost, 2)
                ELSE 0
            END AS 投产比,
            (od.amount - ROUND(od.amount * 0.05, 2) - ROUND(od.amount * 0.02, 2) - ROUND(od.amount * 0.10, 2) - (COALESCE(p.cost, 0) * od.quantity) - ROUND(COALESCE(o.shipping_fee, 0) / order_item_count.item_count, 2) - COALESCE(pm.cost, 0)) AS 净利润,
            CASE 
                WHEN od.amount > 0 THEN ROUND((od.amount - ROUND(od.amount * 0.05, 2) - ROUND(od.amount * 0.02, 2) - ROUND(od.amount * 0.10, 2) - (COALESCE(p.cost, 0) * od.quantity) - ROUND(COALESCE(o.shipping_fee, 0) / order_item_count.item_count, 2) - COALESCE(pm.cost, 0)) / od.amount * 100, 2)
                ELSE 0
            END AS 净利率,
            COALESCE(o.payment_method, '') AS 支付方式,
            COALESCE(pm.channel, '') AS 推广渠道
        FROM ods_order_details od
        LEFT JOIN ods_orders o ON od.order_id = o.order_id
        LEFT JOIN ods_products p ON od.product_id = p.product_id
        LEFT JOIN ods_users u ON o.user_id = u.user_id
        LEFT JOIN ods_stores s ON o.store_id = s.store_id
        LEFT JOIN (
            SELECT order_id, COUNT(*) AS item_count
            FROM ods_order_details
            GROUP BY order_id
        ) order_item_count ON od.order_id = order_item_count.order_id
        LEFT JOIN ods_promotion pm ON 
            pm.product_id = od.product_id 
            AND pm.date = DATE(o.order_time)
            AND pm.store_id = o.store_id
            AND pm.platform = o.platform
        WHERE 
            od.order_id IS NOT NULL
            AND od.product_id IS NOT NULL
            AND od.quantity > 0
            AND od.price > 0
            AND od.amount > 0
            AND o.order_id IS NOT NULL
            AND o.final_amount >= 0
        """
        
        if not execute_sql(conn, sql_order_detail_wide, "创建订单明细大宽表"):
            return False
        
        # 3. 构建推广明细表
        print("\n3. 构建推广明细表...")
        
        if mode == 'full':
            execute_sql(conn, "DROP TABLE IF EXISTS dwd_promotion_detail", "删除旧表 dwd_promotion_detail")
        
        sql_promotion_detail = """
        CREATE TABLE IF NOT EXISTS dwd_promotion_detail
        ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        AS
        SELECT 
            pm.promotion_id AS 推广ID,
            pm.date AS 推广日期,
            pm.store_id AS 店铺ID,
            s.store_name AS 店铺名称,
            pm.platform AS 平台,
            pm.product_id AS 商品ID,
            p.product_name AS 商品名称,
            p.category_l1 AS 一级类目,
            p.category_l2 AS 二级类目,
            pm.channel AS 推广渠道,
            pm.cost AS 推广花费,
            pm.impressions AS 曝光量,
            pm.clicks AS 点击量,
            CASE 
                WHEN pm.impressions > 0 THEN ROUND(pm.clicks / pm.impressions * 100, 2)
                ELSE 0
            END AS 点击率,
            CASE 
                WHEN pm.clicks > 0 THEN ROUND(pm.cost / pm.clicks, 2)
                ELSE 0
            END AS 平均点击成本,
            YEAR(pm.date) AS 年,
            MONTH(pm.date) AS 月,
            DAY(pm.date) AS 日,
            DATE_FORMAT(pm.date, '%Y-%m') AS 年月
        FROM ods_promotion pm
        LEFT JOIN ods_stores s ON pm.store_id = s.store_id
        LEFT JOIN ods_products p ON pm.product_id = p.product_id
        WHERE 
            pm.promotion_id IS NOT NULL
            AND pm.product_id IS NOT NULL
            AND pm.store_id IS NOT NULL
            AND pm.cost >= 0
            AND pm.impressions >= 0
            AND pm.clicks >= 0
            AND pm.clicks <= pm.impressions
        """
        
        if not execute_sql(conn, sql_promotion_detail, "创建推广明细表"):
            return False
        
        print("\n" + "="*60)
        print("✓ DWD层转换完成！")
        print("="*60)
        sys.stdout.flush()
        
        return True
        
    except Exception as e:
        print(f"✗ 转换失败: {e}")
        sys.stdout.flush()
        return False
    finally:
        conn.close()


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
    
    success = transform_dwd(mode, db_config)
    
    if not success:
        sys.exit(1)


if __name__ == '__main__':
    main()
