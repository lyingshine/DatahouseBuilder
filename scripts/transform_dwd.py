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
    """执行SQL语句"""
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        affected_rows = cursor.rowcount
        cursor.close()
        print(f"✓ {description} - 影响 {affected_rows} 行")
        return True
    except Exception as e:
        print(f"✗ {description} 失败: {e}")
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
        # 1. 构建订单事实表
        print("\n1. 构建订单事实表...")
        
        if mode == 'full':
            execute_sql(conn, "DROP TABLE IF EXISTS dwd_order_fact", "删除旧表 dwd_order_fact")
        
        sql_order_fact = """
        CREATE TABLE IF NOT EXISTS dwd_order_fact AS
        SELECT 
            o.order_id AS 订单ID,
            o.user_id AS 用户ID,
            o.store_id AS 店铺ID,
            o.platform AS 平台,
            o.order_time AS 下单时间,
            o.order_status AS 订单状态,
            o.final_amount AS 实付金额,
            COALESCE(o.total_cost, 0) AS 成本总额,
            (o.final_amount - COALESCE(o.total_cost, 0)) AS 毛利,
            CASE 
                WHEN o.final_amount > 0 THEN ROUND((o.final_amount - COALESCE(o.total_cost, 0)) / o.final_amount * 100, 2)
                ELSE 0
            END AS 毛利率,
            u.gender AS 性别,
            u.age AS 年龄,
            CASE 
                WHEN u.age <= 25 THEN '18-25岁'
                WHEN u.age <= 35 THEN '26-35岁'
                WHEN u.age <= 45 THEN '36-45岁'
                WHEN u.age <= 55 THEN '46-55岁'
                ELSE '55岁以上'
            END AS 年龄段,
            u.city AS 城市,
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
        """
        
        execute_sql(conn, sql_order_fact, "创建订单事实表")
        
        # 2. 构建订单明细事实表
        print("\n2. 构建订单明细事实表...")
        
        if mode == 'full':
            execute_sql(conn, "DROP TABLE IF EXISTS dwd_order_detail_fact", "删除旧表 dwd_order_detail_fact")
        
        sql_order_detail_fact = """
        CREATE TABLE IF NOT EXISTS dwd_order_detail_fact AS
        SELECT 
            od.order_detail_id AS 订单明细ID,
            od.order_id AS 订单ID,
            od.product_id AS 商品ID,
            p.product_name AS 商品名称,
            p.category_l1 AS 一级类目,
            p.category_l2 AS 二级类目,
            od.quantity AS 数量,
            od.price AS 单价,
            od.amount AS 金额,
            COALESCE(p.cost, 0) AS 成本,
            (COALESCE(p.cost, 0) * od.quantity) AS 成本金额,
            (od.amount - COALESCE(p.cost, 0) * od.quantity) AS 毛利,
            CASE 
                WHEN od.amount > 0 THEN ROUND((od.amount - COALESCE(p.cost, 0) * od.quantity) / od.amount * 100, 2)
                ELSE 0
            END AS 毛利率
        FROM ods_order_details od
        LEFT JOIN ods_products p ON od.product_id = p.product_id
        """
        
        execute_sql(conn, sql_order_detail_fact, "创建订单明细事实表")
        
        # 3. 构建商品维度表
        print("\n3. 构建商品维度表...")
        
        if mode == 'full':
            execute_sql(conn, "DROP TABLE IF EXISTS dim_product", "删除旧表 dim_product")
        
        sql_dim_product = """
        CREATE TABLE IF NOT EXISTS dim_product AS
        SELECT 
            p.product_id AS 商品ID,
            p.store_id AS 店铺ID,
            s.store_name AS 店铺名称,
            p.platform AS 平台,
            p.product_name AS 商品名称,
            p.category_l1 AS 一级类目,
            p.category_l2 AS 二级类目,
            p.price AS 售价,
            COALESCE(p.cost, 0) AS 成本,
            COALESCE(p.stock, 0) AS 库存,
            CASE 
                WHEN p.price > 0 THEN ROUND((p.price - COALESCE(p.cost, 0)) / p.price * 100, 2)
                ELSE 0
            END AS 利润率,
            p.create_time AS 创建时间
        FROM ods_products p
        LEFT JOIN ods_stores s ON p.store_id = s.store_id
        """
        
        execute_sql(conn, sql_dim_product, "创建商品维度表")
        
        # 4. 构建店铺维度表
        print("\n4. 构建店铺维度表...")
        
        if mode == 'full':
            execute_sql(conn, "DROP TABLE IF EXISTS dim_store", "删除旧表 dim_store")
        
        sql_dim_store = """
        CREATE TABLE IF NOT EXISTS dim_store AS
        SELECT 
            store_id AS 店铺ID,
            store_name AS 店铺名称,
            platform AS 平台,
            open_date AS 开店日期
        FROM ods_stores
        """
        
        execute_sql(conn, sql_dim_store, "创建店铺维度表")
        
        # 5. 构建用户维度表
        print("\n5. 构建用户维度表...")
        
        if mode == 'full':
            execute_sql(conn, "DROP TABLE IF EXISTS dim_user", "删除旧表 dim_user")
        
        sql_dim_user = """
        CREATE TABLE IF NOT EXISTS dim_user AS
        SELECT 
            user_id AS 用户ID,
            user_name AS 用户名,
            gender AS 性别,
            COALESCE(age, 0) AS 年龄,
            CASE 
                WHEN COALESCE(age, 0) <= 25 THEN '18-25岁'
                WHEN COALESCE(age, 0) <= 35 THEN '26-35岁'
                WHEN COALESCE(age, 0) <= 45 THEN '36-45岁'
                WHEN COALESCE(age, 0) <= 55 THEN '46-55岁'
                ELSE '55岁以上'
            END AS 年龄段,
            city AS 城市,
            register_date AS 注册日期
        FROM ods_users
        """
        
        execute_sql(conn, sql_dim_user, "创建用户维度表")
        
        print("\n" + "="*60)
        print("DWD层转换完成！")
        print("="*60)
        
        return True
        
    except Exception as e:
        print(f"转换失败: {e}")
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
