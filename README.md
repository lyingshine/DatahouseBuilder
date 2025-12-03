# 电商数仓配置器

现代化的电商数据仓库配置工具，支持 ODS/DWD/DWS/ADS 四层数据仓库架构。

## ✨ 特点

- 🏗️ **四层数据仓库架构** - ODS/DWD/DWS/ADS 完整分层
- 🎯 **企业体量驱动** - 从企业规模自动计算流量和订单
- 📊 **真实业务模型** - 商品分层、流量转化、推广费用真实模拟
- 🚀 **高性能** - 多进程并行，支持千万级数据
- 🎨 **现代化界面** - Electron 桌面应用，深色主题
- 🔧 **模块化架构** - 低耦合、高内聚、易扩展

## 🚀 快速开始

### 系统要求
- Python 3.8+
- MySQL 5.7+
- Node.js 14+ (开发模式)

### 安装依赖

```bash
# Python 依赖
pip install pandas numpy pymysql faker

# Node.js 依赖 (开发模式)
npm install
```

### 启动应用

```bash
# 开发模式
npm start

# 打包
npm run build
```

## 📊 数据架构

### ODS层（原始数据层）- 8张表
- `ods_stores` - 店铺表
- `ods_products` - 商品表（SKU维度）
- `ods_users` - 用户表
- `ods_orders` - 订单表
- `ods_order_details` - 订单明细表
- `ods_promotion` - 推广表
- `ods_traffic` - 流量表
- `ods_inventory` - 库存表

### DWD层（明细数据层）- 星型模型
**维度表**:
- `dim_date` - 日期维度
- `dim_user` - 用户维度
- `dim_product` - 商品维度
- `dim_store` - 店铺维度

**事实表**:
- `dwd_fact_order` - 订单事实表
- `dwd_fact_order_detail` - 订单明细事实表
- `dwd_fact_promotion` - 推广事实表
- `dwd_fact_traffic` - 流量事实表
- `dwd_fact_inventory` - 库存事实表

### DWS层（汇总数据层）
- `dws_trade_order_1d` - 订单日汇总
- `dws_trade_product_1d` - 商品日汇总
- `dws_store_daily` - 店铺日汇总
- `dws_store_total` - 店铺总汇总
- `dws_product_total` - 商品总汇总
- `dws_category_total` - 类目总汇总
- `dws_user_total` - 用户总汇总
- `dws_promotion_daily` - 推广日汇总
- `dws_traffic_daily` - 流量日汇总

### ADS层（应用数据层）
- `ads_daily_report` - 日报宽表
- `ads_platform_summary` - 平台汇总
- `ads_store_ranking` - 店铺排行榜
- `ads_traffic_report` - 流量报表

## 🏗️ 项目结构

```
├── main.js                      # Electron 主进程
├── renderer/                    # 前端界面
│   ├── index.html
│   ├── renderer.js
│   └── styles.css
├── scripts/                     # Python 脚本
│   ├── db_manager.py           # 数据库管理器
│   ├── config/                 # 配置模块
│   │   ├── business_config.py  # 商品分层配置
│   │   ├── platform_config.py  # 平台配置
│   │   └── category_config.py  # 类目配置
│   ├── generators/             # 数据生成器
│   │   ├── base_generator.py   # 基础生成器
│   │   ├── store_generator.py  # 店铺生成器
│   │   ├── user_generator.py   # 用户生成器
│   │   ├── product_generator.py # 商品生成器
│   │   └── order_generator.py  # 订单生成器
│   ├── generate_ods_data.py    # ODS 数据生成入口
│   ├── transform_dwd.py        # DWD 层转换
│   ├── transform_dws.py        # DWS 层转换
│   ├── transform_ads.py        # ADS 层转换
│   ├── business_scale.py       # 企业体量配置
│   ├── traffic_distribution.py # 流量分发器
│   ├── conversion_engine.py    # 转化引擎
│   └── ...                     # 其他工具脚本
└── docs/                       # 文档
    ├── 数据指标文档.md
    └── 流量分发模型说明.md
```

## 🎯 使用流程

1. **配置企业体量** - 选择企业规模（微型/小型/中型/大型/超大型）
2. **配置平台店铺** - 系统自动配置或手动调整
3. **生成 ODS 数据** - 自动生成真实业务数据
4. **转换 DWD 层** - 构建星型模型（维度表+事实表）
5. **聚合 DWS 层** - 多维度汇总
6. **构建 ADS 层** - 业务宽表和报表
7. **数据分析** - 使用 BI 工具或 SQL 查询

## 🔧 核心功能

### 企业体量驱动模型
- 从企业规模出发，自动计算流量和订单
- 流量 → 曝光/点击 → 转化 → 订单生成
- 真实的业务逻辑和数据关系

### 商品分层策略
- **畅销品**（30%）- 高销量，中等利润
- **利润品**（20%）- 高利润，低销量
- **主推新品**（15%）- 中等利润，推广加持
- **滞销品**（20%）- 低销量，利润不稳定
- **引流品**（15%）- 高销量，低利润

### 性能优化
- 多进程并行生成（20核CPU）
- 分批处理（每批10万行）
- MySQL 极限优化配置
- 支持千万级数据

## 📈 数据特点

- ✅ 真实的电商运营规律
- ✅ 合理的利润率分布（毛利率 30-37%，净利率 7-12%）
- ✅ 真实的推广费用占比（5-8%）
- ✅ 完整的流量转化链路
- ✅ 数据一致性保证

## 🛠️ 技术栈

- **前端**: Electron + HTML/CSS/JavaScript
- **后端**: Python + Pandas + NumPy
- **数据库**: MySQL
- **打包**: electron-builder

## 📝 许可证

MIT License

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

## 📮 联系方式

- GitHub: [lyingshine/DatahouseBuilder](https://github.com/lyingshine/DatahouseBuilder)
