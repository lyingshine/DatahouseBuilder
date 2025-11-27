# 电商数仓配置器

现代化的电商数据仓库配置工具，支持ODS/DWD/DWS三层数据仓库架构。

## 功能特点

- 🎯 **三层数据仓库架构**
  - ODS层：原始数据层，支持生成模拟数据或导入CSV
  - DWD层：明细数据层，在数据库中通过SQL转换
  - DWS层：汇总数据层，在数据库中通过SQL聚合

- 🏪 **多类目支持**
  - 自行车电商、服装电商、数码电商
  - 食品电商、美妆电商、家居电商

- 🔧 **强大功能**
  - 自定义平台和店铺配置
  - 自动生成真实业务数据
  - 一键加载到MySQL数据库
  - SQL转换生成DWD/DWS层
  - 数据预览和SQL查看
  - 一键清空数据

## 系统要求

- Windows 10/11 (64位)
- Python 3.8+
- MySQL 5.7+

## 安装使用

### 1. 安装Python依赖

```bash
pip install pandas pymysql sqlalchemy faker Pillow
```

### 2. 配置数据库

修改 `数据库信息` 文件：
```
数据库类型：mysql
数据库地址：localhost
端口：3306
数据库名：datas
用户名：root
密码：your_password
```

### 3. 启动应用

- 便携版：双击 `电商数仓配置器.exe`
- 开发模式：`npm start`

## 使用流程

1. **选择主营类目** - 选择业务类型（自行车、服装等）
2. **配置平台店铺** - 选择电商平台，配置店铺数量和名称
3. **生成ODS数据** - 自动生成并加载到数据库
4. **转换DWD层** - 在数据库中执行SQL转换
5. **聚合DWS层** - 在数据库中执行SQL聚合
6. **查看结果** - 预览数据或查看SQL语句

## 数据表结构

### ODS层（8张表）
- ods_stores - 店铺表
- ods_products - 商品表
- ods_users - 用户表
- ods_orders - 订单表
- ods_order_details - 订单明细表
- ods_promotion - 推广表
- ods_traffic - 流量表
- ods_inventory - 库存表

### DWD层（5张表）
- dwd_order_fact - 订单事实表
- dwd_order_detail_fact - 订单明细事实表
- dim_product - 商品维度表
- dim_store - 店铺维度表
- dim_user - 用户维度表

### DWS层（4张表）
- dws_sales_summary - 销售汇总表
- dws_traffic_summary - 流量汇总表
- dws_inventory_summary - 库存汇总表
- dws_promotion_summary - 推广汇总表

## 开发

### 项目结构
```
├── main.js                 # Electron主进程
├── renderer/              # 前端界面
│   ├── index.html
│   ├── renderer.js
│   └── styles.css
├── scripts/               # Python脚本
│   ├── generate_data.py   # 生成ODS数据
│   ├── transform_dwd.py   # DWD层转换
│   ├── transform_dws.py   # DWS层转换
│   ├── load_to_database.py # 数据库加载
│   ├── clear_data.py      # 清空数据
│   └── db_config.py       # 数据库配置
├── build/                 # 构建资源
│   └── icon.ico          # 应用图标
└── 数据库信息             # 数据库配置文件
```

### 开发命令
```bash
npm install          # 安装依赖
npm start           # 启动应用
npm run dev         # 开发模式
npm run build       # 打包应用
```

### 打包
```bash
npm run build       # 生成安装程序
npm run build:dir   # 仅打包目录
```

或使用便携版创建脚本：
```bash
.\创建便携版.bat
```

## 技术栈

- **前端**: Electron + HTML/CSS/JavaScript
- **后端**: Python + Pandas
- **数据库**: MySQL
- **打包**: electron-builder

## 许可证

MIT License

## 快速开始

### 下载使用

1. 从 [Releases](https://github.com/lyingshine/DatahouseBuilder/releases) 下载最新版本
2. 解压后运行 `电商数仓配置器.exe`
3. 配置数据库连接信息
4. 开始使用

## 更新日志

### v1.0.0 (2025-11-27)
- ✨ 初始版本发布
- 🎨 现代化深色主题界面
- 📊 支持三层数据仓库架构
- 🏪 支持6种主营类目
- 🔧 完整的数据生成和转换功能
- 🎯 自定义数仓主题图标
