-- ============================================
-- 自行车电商数据仓库建表SQL
-- 适用于MySQL/PostgreSQL（根据需要调整数据类型）
-- ============================================

-- ============================================
-- ODS层（原始数据层）
-- ============================================

-- 店铺表
CREATE TABLE ods_stores (
    store_id VARCHAR(20) PRIMARY KEY,
    store_name VARCHAR(100),
    platform VARCHAR(20),
    open_date DATE
);

-- 商品表
CREATE TABLE ods_products (
    product_id VARCHAR(20) PRIMARY KEY,
    store_id VARCHAR(20),
    platform VARCHAR(20),
    product_name VARCHAR(200),
    category_l1 VARCHAR(50),
    category_l2 VARCHAR(50),
    price DECIMAL(10,2),
    cost DECIMAL(10,2),
    stock INT,
    create_time DATETIME
);

-- 用户表
CREATE TABLE ods_users (
    user_id VARCHAR(20) PRIMARY KEY,
    user_name VARCHAR(100),
    gender VARCHAR(10),
    age INT,
    city VARCHAR(50),
    register_date DATE
);

-- 订单主表
CREATE TABLE ods_orders (
    order_id VARCHAR(20) PRIMARY KEY,
    user_id VARCHAR(20),
    store_id VARCHAR(20),
    platform VARCHAR(20),
    order_time DATETIME,
    order_status VARCHAR(20),
    total_amount DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    shipping_fee DECIMAL(10,2),
    final_amount DECIMAL(10,2),
    total_cost DECIMAL(10,2),
    payment_method VARCHAR(20),
    create_time DATETIME,
    update_time DATETIME
);

-- 订单明细表
CREATE TABLE ods_order_details (
    order_detail_id VARCHAR(20) PRIMARY KEY,
    order_id VARCHAR(20),
    product_id VARCHAR(20),
    quantity INT,
    price DECIMAL(10,2),
    amount DECIMAL(10,2)
);

-- ============================================
-- DWD层（明细数据层）
-- ============================================

-- 订单明细宽表
CREATE TABLE dwd_orders (
    order_id VARCHAR(20) PRIMARY KEY,
    user_id VARCHAR(20),
    store_id VARCHAR(20),
    store_name VARCHAR(100),
    platform VARCHAR(20),
    order_time DATETIME,
    order_status VARCHAR(20),
    total_amount DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    shipping_fee DECIMAL(10,2),
    final_amount DECIMAL(10,2),
    total_cost DECIMAL(10,2),
    profit_amount DECIMAL(10,2),
    payment_method VARCHAR(20),
    gender VARCHAR(10),
    age INT,
    city VARCHAR(50),
    order_date DATE,
    order_year INT,
    order_month INT,
    order_day INT,
    order_weekday INT,
    order_hour INT,
    create_time DATETIME,
    update_time DATETIME
);

-- 订单商品明细宽表
CREATE TABLE dwd_order_details (
    order_detail_id VARCHAR(20) PRIMARY KEY,
    order_id VARCHAR(20),
    product_id VARCHAR(20),
    product_name VARCHAR(200),
    category_l1 VARCHAR(50),
    category_l2 VARCHAR(50),
    quantity INT,
    price DECIMAL(10,2),
    amount DECIMAL(10,2),
    cost DECIMAL(10,2),
    cost_amount DECIMAL(10,2),
    profit_amount DECIMAL(10,2)
);

-- 商品明细宽表
CREATE TABLE dwd_products (
    product_id VARCHAR(20) PRIMARY KEY,
    store_id VARCHAR(20),
    store_name VARCHAR(100),
    platform VARCHAR(20),
    product_name VARCHAR(200),
    category_l1 VARCHAR(50),
    category_l2 VARCHAR(50),
    price DECIMAL(10,2),
    cost DECIMAL(10,2),
    profit_rate DECIMAL(5,2),
    stock INT,
    create_time DATETIME
);

-- 用户明细宽表
CREATE TABLE dwd_users (
    user_id VARCHAR(20) PRIMARY KEY,
    user_name VARCHAR(100),
    gender VARCHAR(10),
    age INT,
    age_group VARCHAR(20),
    city VARCHAR(50),
    register_date DATE
);

-- ============================================
-- DWS层（汇总数据层）
-- ============================================

-- 每日销售汇总
CREATE TABLE dws_daily_sales (
    date DATE,
    platform VARCHAR(20),
    order_count INT,
    user_count INT,
    sales_amount DECIMAL(12,2),
    cost_amount DECIMAL(12,2),
    profit_amount DECIMAL(12,2),
    discount_amount DECIMAL(12,2),
    avg_order_amount DECIMAL(10,2),
    profit_rate DECIMAL(5,2),
    PRIMARY KEY (date, platform)
);

-- 店铺销售汇总
CREATE TABLE dws_store_sales (
    store_id VARCHAR(20) PRIMARY KEY,
    store_name VARCHAR(100),
    platform VARCHAR(20),
    order_count INT,
    user_count INT,
    sales_amount DECIMAL(12,2),
    cost_amount DECIMAL(12,2),
    profit_amount DECIMAL(12,2),
    avg_order_amount DECIMAL(10,2),
    profit_rate DECIMAL(5,2)
);

-- 商品销售汇总
CREATE TABLE dws_product_sales (
    product_id VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(200),
    category_l1 VARCHAR(50),
    category_l2 VARCHAR(50),
    order_count INT,
    sales_quantity INT,
    sales_amount DECIMAL(12,2),
    cost_amount DECIMAL(12,2),
    profit_amount DECIMAL(12,2),
    profit_rate DECIMAL(5,2)
);

-- 类目销售汇总
CREATE TABLE dws_category_sales (
    platform VARCHAR(20),
    category_l1 VARCHAR(50),
    category_l2 VARCHAR(50),
    order_count INT,
    sales_quantity INT,
    sales_amount DECIMAL(12,2),
    profit_amount DECIMAL(12,2),
    profit_rate DECIMAL(5,2),
    PRIMARY KEY (platform, category_l1, category_l2)
);

-- 用户行为汇总
CREATE TABLE dws_user_behavior (
    user_id VARCHAR(20) PRIMARY KEY,
    gender VARCHAR(10),
    age INT,
    age_group VARCHAR(20),
    city VARCHAR(50),
    order_count INT,
    total_amount DECIMAL(12,2),
    avg_order_amount DECIMAL(10,2),
    first_order_date DATE,
    last_order_date DATE,
    user_level VARCHAR(20)
);

-- ============================================
-- ADS层（应用数据层）
-- ============================================

-- 平台对比分析
CREATE TABLE ads_platform_report (
    platform VARCHAR(20) PRIMARY KEY,
    store_count INT,
    order_count INT,
    user_count INT,
    sales_amount DECIMAL(12,2),
    profit_amount DECIMAL(12,2),
    avg_store_sales DECIMAL(12,2),
    profit_rate DECIMAL(5,2),
    avg_order_amount DECIMAL(10,2),
    sales_ratio DECIMAL(5,2)
);

-- TOP商品排行
CREATE TABLE ads_top_products (
    product_id VARCHAR(20),
    product_name VARCHAR(200),
    category_l1 VARCHAR(50),
    category_l2 VARCHAR(50),
    rank_type VARCHAR(20),
    rank INT,
    order_count INT,
    sales_quantity INT,
    sales_amount DECIMAL(12,2),
    profit_amount DECIMAL(12,2),
    profit_rate DECIMAL(5,2),
    PRIMARY KEY (product_id, rank_type)
);

-- 类目分析
CREATE TABLE ads_category_analysis (
    category_l1 VARCHAR(50) PRIMARY KEY,
    order_count INT,
    sales_quantity INT,
    sales_amount DECIMAL(12,2),
    profit_amount DECIMAL(12,2),
    sales_ratio DECIMAL(5,2),
    profit_rate DECIMAL(5,2)
);

-- 用户等级分析
CREATE TABLE ads_user_level_analysis (
    user_level VARCHAR(20) PRIMARY KEY,
    user_count INT,
    total_amount DECIMAL(12,2),
    order_count INT,
    avg_amount_per_user DECIMAL(10,2),
    user_ratio DECIMAL(5,2),
    sales_ratio DECIMAL(5,2)
);

-- 月度趋势分析
CREATE TABLE ads_monthly_trend (
    year_month VARCHAR(10),
    platform VARCHAR(20),
    order_count INT,
    user_count INT,
    sales_amount DECIMAL(12,2),
    profit_amount DECIMAL(12,2),
    profit_rate DECIMAL(5,2),
    avg_order_amount DECIMAL(10,2),
    PRIMARY KEY (year_month, platform)
);

-- 店铺排行榜
CREATE TABLE ads_store_ranking (
    store_id VARCHAR(20) PRIMARY KEY,
    store_name VARCHAR(100),
    platform VARCHAR(20),
    order_count INT,
    user_count INT,
    sales_amount DECIMAL(12,2),
    profit_amount DECIMAL(12,2),
    avg_order_amount DECIMAL(10,2),
    profit_rate DECIMAL(5,2),
    overall_rank INT,
    platform_rank INT
);
