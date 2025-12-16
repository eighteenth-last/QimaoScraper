# 七猫小说数据仓库项目

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.8+-blue.svg" alt="Python Version">
  <img src="https://img.shields.io/badge/PySpark-3.0+-orange.svg" alt="PySpark Version">
  <img src="https://img.shields.io/badge/MySQL-8.0+-green.svg" alt="MySQL Version">
  <img src="https://img.shields.io/badge/Hive-3.0+-yellow.svg" alt="Hive Version">
  <img src="https://img.shields.io/badge/License-MIT-red.svg" alt="License">
</p>

## 📖 项目简介

本项目是一个完整的**网络小说数据仓库解决方案**，专注于七猫小说平台的数据采集、处理、分析和价值挖掘。采用经典的三层数据仓库架构（ODS → DWD → ADS），从**平台运营、作者创作、用户阅读、资本投资**四大维度进行深度数据分析，为网文行业提供数据驱动的决策支持。

### 🎯 核心特性

- ✅ **完整的ETL流程**：从数据采集到可视化的全链路数据处理
- ✅ **四维度分析体系**：平台侧、作者侧、用户侧、资本侧全方位洞察
- ✅ **8大特征表**：涵盖热度分析、榜单趋势、IP价值评估等核心业务场景
- ✅ **自动化执行**：支持一键运行完整数据仓库处理流程
- ✅ **可扩展架构**：模块化设计，易于扩展新的分析维度

### 🏗️ 技术架构

```
┌─────────────────────────────────────────────────────────────────┐
│                        数据采集层                                 │
│                  Scrapy 分布式爬虫框架                            │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                        数据存储层                                 │
│        MySQL (源数据) → Hive (ODS/DWD/ADS) → MySQL (应用)        │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                        数据处理层                                 │
│              PySpark 分布式计算 + 特征工程                        │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                        数据应用层                                 │
│              BI可视化 / API服务 / 机器学习                        │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📚 技术栈

| 技术领域 | 使用技术 | 说明 |
|---------|---------|------|
| **数据采集** | Scrapy 2.x | 分布式爬虫框架，支持断点续传 |
| **数据存储** | MySQL 8.0+, Apache Hive 3.x | 关系型数据库 + 分布式数据仓库 |
| **数据处理** | PySpark 3.0+ | 分布式数据处理与特征工程 |
| **数据导出** | PySpark JDBC | 高性能数据导出 |
| **开发语言** | Python 3.8+ | 统一开发语言 |
| **运行环境** | Linux / Windows | 支持跨平台部署 |

---

## 📁 项目结构

```
QimaoScraper/
│
├── Spider_New/                          # 🕷️ 数据采集模块
│   ├── qimao_scrapy/                   # Scrapy爬虫项目
│   │   ├── spiders/                    # 爬虫脚本
│   │   │   └── qimao_spider.py        # 七猫榜单与详情爬虫
│   │   ├── items.py                   # 数据项定义
│   │   ├── pipelines.py               # 数据管道（入库）
│   │   ├── middlewares.py             # 中间件（代理/UA轮换）
│   │   ├── settings.py                # 爬虫配置
│   │   └── db_utils.py                # MySQL连接工具
│   ├── run_scrapy.py                  # 爬虫启动脚本
│   ├── Spider_data.py                 # 数据采集工具
│   ├── scrapy.cfg                     # Scrapy配置文件
│   └── qimao_books.json               # 书籍数据缓存
│
├── database/                            # 📊 数据库建表脚本
│   ├── QimaoScraper.sql               # 源数据库表结构（Seven_Cats_Novel_Data）
│   └── QimaoScraper_Feature_Data.sql  # 特征数据库表结构（8个ADS表）
│
├── Data_visualisation/                  # 🔧 数据处理模块
│   ├── config.py                      # ⚙️ 配置文件（MySQL/Hive/Spark配置）
│   ├── main.py                        # 🚀 主执行脚本（一键运行ETL）
│   ├── Spark_MySQL_to_Hive.py         # 步骤1: MySQL → Hive ODS
│   ├── Spark_Data_processing.py       # 步骤2: ODS → DWD 数据清洗
│   ├── Spark_dwd_to_ads.py            # 步骤3: DWD → ADS 特征工程（8表）
│   ├── Sqoop_hive_to_MySQL.py         # 步骤4: Hive ADS → MySQL 导出
│   └── 项目执行/                       # 📄 项目文档
│       ├── 数仓建模方案.md             # 数据仓库建模设计文档
│       ├── 执行方案.md                 # 四维度分析执行方案
│       └── 开发提示词.md               # 开发说明与提示
│
├── img/                                 # 📷 项目截图
│   └── *.png                          # 执行结果截图
│
└── README.md                            # 📖 项目说明文档（本文件）
```

---

## 🏛️ 数据仓库架构

### 数据流转全景

```
【数据源】
  七猫小说网站API
       ↓
【采集层】Scrapy爬虫
       ↓
【源数据库】MySQL.QimaoScraper.Seven_Cats_Novel_Data
       ↓ (Spark JDBC 全量/增量导入)
【ODS层】Hive.hive_QimaoScraper.ods_novel_data
       ↓ (数据清洗: 空值处理、异常值处理、数值化转换)
【DWD层】Hive.hive_QimaoScraper.dwd_novel_data
       ↓ (特征工程: 8个维度表生成)
【ADS层】Hive.hive_QimaoScraper.ads_* (8张特征表)
       ↓ (Spark JDBC 导出到应用数据库)
【应用层】MySQL.QimaoScraper_Feature_Data.ads_* (8张表)
       ↓
【可视化/API/ML】BI报表、推荐系统、价值评估
```

### 📊 分层设计详解

#### 1️⃣ ODS层（Operational Data Store - 原始数据层）

- **数据库**: `hive_QimaoScraper`
- **表名**: `ods_novel_data`
- **数据来源**: 从 MySQL 的 `Seven_Cats_Novel_Data` 表全量同步
- **处理原则**: **不做任何清洗**，保持原始数据状态，支持数据回溯
- **更新方式**: 支持全量覆盖或增量追加

#### 2️⃣ DWD层（Data Warehouse Detail - 明细数据层）

- **数据库**: `hive_QimaoScraper`
- **表名**: `dwd_novel_data`
- **处理逻辑**:
  1. **空值处理**: 删除全空列、全空行
  2. **缺失值填充**: 数值字段用平均值填充
  3. **异常值处理**: 使用3倍IQR法则，用中位数替换异常值
  4. **数值化转换**: 将带单位的字符串转为数值（如 "123.5万" → 1235000）
     - `popularity` → `numeric_popularity`
     - `read_count` → `numeric_read_count`
     - `score` → `numeric_score`
     - `words_num` → `numeric_words`

#### 3️⃣ ADS层（Application Data Store - 应用数据层）

- **数据库**: `hive_QimaoScraper` (Hive) / `QimaoScraper_Feature_Data` (MySQL)
- **表数量**: **8张特征表**，分为四大业务维度
- **处理逻辑**: 基于DWD层数据进行特征工程，生成面向业务场景的分析表

---

## 🎯 四大分析维度与8张特征表

### 🏢 一、平台侧分析（Platform Analytics）

**业务目标**: 优化流量分发策略，提升平台GMV

#### 📈 表1: `ads_platform_heat` - 热度分析表

**用途**: 监控作品热度变化趋势，识别爆款作品

| 核心指标 | 说明 | 业务价值 |
|---------|------|---------|
| `popularity_diff` | 一阶导数（热度日增量） | 识别快速增长的作品 |
| `popularity_acceleration` | 二阶导数（增长加速度） | 预测热度爆发拐点 |
| `is_cold_start_quality` | 冷启动质量标识 | 挖掘新书潜力股 |
| `heat_surge_flag` | 热度飙升标记 | 自动推荐位分配 |

**SQL查询示例**:
```sql
-- 查询今日热度飙升的TOP10作品
SELECT book_id, title, popularity_diff, popularity_acceleration
FROM ads_platform_heat
WHERE heat_surge_flag = 1 AND rank_date = CURRENT_DATE
ORDER BY popularity_acceleration DESC
LIMIT 10;
```

#### 📊 表2: `ads_platform_ranking_trend` - 榜单趋势分析表

**用途**: 分析榜单转化效率，识别"德不配位"作品

| 核心指标 | 说明 | 业务价值 |
|---------|------|---------|
| `ranking_conversion_rate` | 榜单转化率（上榜→阅读增长） | 评估榜单推荐效果 |
| `is_unworthy` | 德不配位标识（高榜位低转化） | 降权低效推荐位 |
| `recommend_weight` | 推荐权重系数 | 动态调整推荐策略 |
| `ranking_stability` | 榜单稳定性 | 识别长尾价值作品 |

**应用场景**: 
- 自动调整推荐位权重
- 识别刷榜行为
- 优化广告竞价策略

---

### ✍️ 二、作者侧分析（Author Analytics）

**业务目标**: 为作者提供创作指导，提升平台作品质量

#### 🔍 表3: `ads_author_reason` - 热度原因分析表

**用途**: 分析热度背后的关键因素

| 核心指标 | 说明 | 业务价值 |
|---------|------|---------|
| `has_hot_keywords` | 是否包含热门关键词 | 题材选择指导 |
| `is_golden_range` | 是否处于黄金字数区间 | 连载节奏建议 |
| `heat_index` | 综合热度指数 | 作品质量评分 |
| `category_avg_heat` | 分类平均热度 | 相对竞争力分析 |

**业务洞察输出**:
- *"都市分类中，包含'系统'、'逆袭'关键词的作品，平均初始热度高出30%"*
- *"50-80万字且连载中的作品，热度增长最快"*

#### 📉 表4: `ads_author_attenuation_effect` - 热度衰减效应表

**用途**: 分析完结后的热度衰减规律

| 核心指标 | 说明 | 业务价值 |
|---------|------|---------|
| `attenuation_rate` | 衰减率（完结后热度下降比例） | 预测作品生命周期 |
| `attenuation_type` | 衰减类型（快速/平稳/缓慢） | 分类管理策略 |
| `recommended_new_book_days` | 建议新书发布时间（天） | 最佳续作时机 |
| `days_since_completion` | 完结后天数 | 衰减曲线监控 |

**策略建议**: 
- 计算最佳"完结时长"
- 建议作者在热度衰减至20%前推出新书
- 利用老书余热导流

---

### 👥 三、用户侧分析（User Analytics）

**业务目标**: 优化个性化推荐，提升用户满意度

#### 🎯 表5: `ads_user_layered_recommendation` - 分层推荐表

**用途**: 挖掘小众爆款，避免热度噪音

| 核心指标 | 说明 | 业务价值 |
|---------|------|---------|
| `relative_heat` | 分类内相对热度 | 圈层爆款识别 |
| `is_niche_hit` | 小众爆款标识 | 长尾推荐优化 |
| `niche_recommendation_score` | 小众推荐分数 | 个性化推荐排序 |
| `category_rank` | 分类内排名 | 相对竞争力 |

**推荐策略**:
- 不看全局热度，看**分类内相对热度**
- 在"男频-科幻"小圈子里，推荐评分>9.0的TOP5
- 避免"热门但不合口味"的问题

#### ⚠️ 表6: `ads_user_avoid_pitfalls` - 避坑指南表

**用途**: 识别高热低分作品，防刷榜机制

| 核心指标 | 说明 | 业务价值 |
|---------|------|---------|
| `is_suspicious_boost` | 疑似刷榜标识 | 风控预警 |
| `risk_level` | 风险等级（正常/可疑/高危） | 降权决策 |
| `avoidance_advice` | 避坑建议 | 用户提示信息 |
| `heat_score_ratio` | 热度评分比（异常识别） | 刷榜检测 |

**风控规则**:
- 热度>300万 且 评分<6.0 → 高风险
- 热度>100万 且 评分<7.0 → 可疑
- 在推荐算法中降权

---

### 💰 四、资本侧分析（Capital Analytics）

**业务目标**: 评估IP长期价值，支持投资决策

#### 💎 表7: `ads_capital_ltv` - IP长尾价值计算表（LTV）

**用途**: 计算IP的长期变现潜力

| 核心指标 | 说明 | 业务价值 |
|---------|------|---------|
| `total_heat_integral` | 热度积分（时间轴累积面积） | 长期价值评估 |
| `ltv_score` | LTV分数 | 投资决策依据 |
| `ip_value_level` | IP价值等级（S/A/B/C） | 快速筛选标的 |
| `adaptation_suggestion` | 改编建议（短剧/长剧/动漫/游戏） | 商业化路径 |

**决策支持**:
- **短线IP**: 爆发快，衰减快（适合短剧改编）
- **长线IP**: 热度平稳上升，持续时间长（适合出版、动漫）

**计算公式**:
```
LTV分数 = 热度积分 × (1 + log10(生命周期天数)) × 改编适配度系数
```

#### 💸 表8: `ads_capital_future_purchasing_power` - 粉丝粘性与购买力验证表

**用途**: 验证粉丝的付费能力（ARPU分析）

| 核心指标 | 说明 | 业务价值 |
|---------|------|---------|
| `avg_arpu` | 平均ARPU值（热度/阅读量） | 付费能力评估 |
| `fan_value_score` | 粉丝价值分数 | 核心粉丝识别 |
| `investment_value_level` | 投资价值等级（优质/良好/一般/较差） | 投资优先级 |
| `investment_recommendation` | 投资建议 | 决策参考 |

**假设验证**:
- 如果 `read_count` 低，但 `rank_name`（打赏榜/月票榜）高
- 说明核心粉丝付费能力极强（ARPU高）
- 是优质资本标的

---

## 🚀 快速开始

### 环境要求

| 组件 | 版本要求 | 说明 |
|------|---------|------|
| Python | 3.8+ | 推荐使用Anaconda环境 |
| PySpark | 3.0+ | 需要配置JAVA_HOME |
| MySQL | 8.0+ | 支持UTF8MB4字符集 |
| Apache Hive | 3.0+ | 需启动Metastore服务 |
| 操作系统 | Linux/Windows | 推荐Linux环境 |

### 安装依赖

```bash
# 创建虚拟环境（推荐）
conda create -n qimao python=3.8
conda activate qimao

# 安装Python依赖
pip install pyspark==3.1.2
pip install pymysql
pip install scrapy
pip install pandas
```

### 配置修改

编辑 [Data_visualisation/config.py](Data_visualisation/config.py) 文件：

```python
# MySQL配置
MYSQL_CONFIG = {
    "host": "your_mysql_host",          # 修改为你的MySQL地址
    "port": 3306,
    "database": "QimaoScraper",
    "target_database": "QimaoScraper_Feature_Data",
    "user": "root",
    "password": "your_password",        # 修改为你的MySQL密码
    "charset": "utf8mb4"
}

# Hive配置
HIVE_CONFIG = {
    "database": "hive_QimaoScraper",
    "metastore_uri": "thrift://localhost:9083",  # 修改为你的Hive Metastore地址
    "warehouse_dir": "/user/hive/warehouse"
}
```

### 初始化数据库

```bash
# 1. 创建源数据库
mysql -u root -p < database/QimaoScraper.sql

# 2. 创建特征数据库
mysql -u root -p < database/QimaoScraper_Feature_Data.sql

# 3. 启动Hive Metastore（Linux环境）
nohup hive --service metastore &
```

---

## 📋 执行流程

### 方式一：一键运行完整流程

```bash
cd Data_visualisation

# 运行完整ETL流程（步骤1-4）
python main.py --all

# 或分步执行
python main.py --step 1  # MySQL → Hive ODS
python main.py --step 2  # ODS → DWD 数据清洗
python main.py --step 3  # DWD → ADS 特征工程
python main.py --step 4  # ADS → MySQL 导出
```

### 方式二：手动分步执行

#### 步骤0: 数据采集（可选）

```bash
cd Spider_New

# 方式1: 使用Scrapy命令
scrapy crawl qimao

# 方式2: 使用启动脚本
python run_scrapy.py
```

**采集范围**:
- 时间跨度: 2023年10月 - 2025年10月（24个月）
- 榜单类型: 大热榜、新书榜、完结榜、收藏榜、更新榜
- 分类覆盖: 男频/女频 × 20+细分分类
- 数据量级: 预计10万+条榜单记录

#### 步骤1: MySQL → Hive ODS层

```bash
python Spark_MySQL_to_Hive.py
```

**执行内容**:
- 从MySQL读取 `Seven_Cats_Novel_Data` 表
- 全量写入Hive `ods_novel_data` 表
- 支持覆盖模式（overwrite）和追加模式（append）

**执行时长**: 约1-5分钟（取决于数据量）

#### 步骤2: ODS → DWD层数据清洗

```bash
python Spark_Data_processing.py
```

**执行内容**:
1. 读取ODS层数据
2. 删除全空列和全空行
3. 缺失值填充（数值字段用均值）
4. 异常值处理（3倍IQR法则）
5. 数值化转换（popularity、read_count、score、words_num）
6. 写入DWD层 `dwd_novel_data` 表

**执行时长**: 约2-10分钟

#### 步骤3: DWD → ADS层特征工程

```bash
python Spark_dwd_to_ads.py
```

**执行内容**:
- 生成8张ADS特征表（见上文"四大分析维度"）
- 每张表独立计算，互不干扰
- 支持单表调试和全量生成

**执行时长**: 约5-20分钟

**输出表列表**:
```
✓ ads_platform_heat
✓ ads_platform_ranking_trend
✓ ads_author_reason
✓ ads_author_attenuation_effect
✓ ads_user_layered_recommendation
✓ ads_user_avoid_pitfalls
✓ ads_capital_ltv
✓ ads_capital_future_purchasing_power
```

#### 步骤4: Hive ADS → MySQL导出

```bash
python Sqoop_hive_to_MySQL.py
```

**执行内容**:
- 将8张ADS表从Hive导出到MySQL
- 使用Spark JDBC高性能写入
- 支持覆盖模式，确保数据最新

**执行时长**: 约2-10分钟

---

## 📊 数据查询示例

### 1. 查询今日热度飙升的作品

```sql
SELECT 
    book_id,
    title,
    category1_name,
    popularity_diff AS 日增量,
    popularity_acceleration AS 加速度,
    rank_date AS 日期
FROM QimaoScraper_Feature_Data.ads_platform_heat
WHERE heat_surge_flag = 1 
  AND rank_date = CURRENT_DATE
ORDER BY popularity_acceleration DESC
LIMIT 10;
```

### 2. 识别"德不配位"的推荐位

```sql
SELECT 
    book_id,
    title,
    rank_name AS 榜单名称,
    ranking_conversion_rate AS 转化率,
    recommend_weight AS 推荐权重
FROM QimaoScraper_Feature_Data.ads_platform_ranking_trend
WHERE is_unworthy = 1  -- 德不配位
  AND ranking_conversion_rate < 0.3  -- 转化率低于30%
ORDER BY recommend_weight ASC
LIMIT 20;
```

### 3. 挖掘小众爆款作品

```sql
SELECT 
    book_id,
    title,
    category1_name AS 分类,
    gender_type AS 性别向,
    relative_heat AS 相对热度,
    numeric_score AS 评分,
    niche_recommendation_score AS 推荐分数
FROM QimaoScraper_Feature_Data.ads_user_layered_recommendation
WHERE is_niche_hit = 1  -- 小众爆款标识
  AND gender_type = 'male'  -- 男频
  AND category1_name = '科幻'
ORDER BY niche_recommendation_score DESC
LIMIT 10;
```

### 4. IP投资价值评估

```sql
SELECT 
    book_id,
    title,
    author,
    ltv_score AS LTV分数,
    ip_value_level AS IP等级,
    adaptation_suggestion AS 改编建议,
    total_heat_integral AS 热度积分,
    lifecycle_days AS 生命周期天数
FROM QimaoScraper_Feature_Data.ads_capital_ltv
WHERE ip_value_level IN ('S', 'A')  -- 高价值IP
  AND adaptation_suggestion LIKE '%长剧%'  -- 适合长剧改编
ORDER BY ltv_score DESC
LIMIT 20;
```

### 5. 识别高ARPU核心粉丝作品

```sql
SELECT 
    book_id,
    title,
    avg_arpu AS 平均ARPU,
    fan_value_score AS 粉丝价值分数,
    investment_value_level AS 投资价值等级,
    investment_recommendation AS 投资建议
FROM QimaoScraper_Feature_Data.ads_capital_future_purchasing_power
WHERE investment_value_level = '优质'
  AND avg_arpu > 50  -- ARPU > 50
ORDER BY fan_value_score DESC
LIMIT 15;
```

---

## 🔧 高级功能

### 自定义特征开发

如需新增特征表，修改 [Spark_dwd_to_ads.py](Data_visualisation/Spark_dwd_to_ads.py)：

```python
def create_custom_feature(self, df):
    """
    自定义特征表生成
    :param df: DWD层DataFrame
    :return: 特征DataFrame
    """
    from pyspark.sql.functions import col, when, avg
    
    # 示例：计算作品的ROI指标
    custom_df = df.select(
        "book_id", "title", "author",
        col("numeric_popularity").alias("popularity"),
        col("numeric_read_count").alias("read_count"),
        col("numeric_score").alias("score")
    ).withColumn(
        "roi_score",
        when(col("read_count") > 0, 
             col("popularity") / col("read_count")
        ).otherwise(0)
    )
    
    return custom_df

# 在run()方法中调用
def run(self):
    # ... 现有代码 ...
    
    # 新增自定义特征表
    custom_feature = self.create_custom_feature(dwd_df)
    self.save_to_hive(custom_feature, "ads_custom_roi")
```

### 增量更新支持

修改 [config.py](Data_visualisation/config.py) 中的更新模式：

```python
# 全量覆盖模式（默认）
WAREHOUSE_UPDATE_MODE = "overwrite"

# 增量追加模式
WAREHOUSE_UPDATE_MODE = "append"
```

### 性能调优

```python
# 调整Spark分区数（config.py）
SPARK_CONFIG = {
    "shuffle_partitions": 20,  # 根据数据量调整（默认10）
    "driver_memory": "4g",     # 增加驱动内存
    "executor_memory": "4g"    # 增加执行器内存
}
```

---

## 📈 监控与日志

### 日志查看

所有脚本都输出详细日志：

```bash
# 查看实时日志
tail -f nohup.out

# 搜索错误日志
grep "ERROR" nohup.out
```

### 日志级别

```python
# 修改日志级别（在各脚本顶部）
logging.basicConfig(
    level=logging.INFO,  # 可改为DEBUG查看更多细节
    format='%(asctime)s - %(levelname)s - %(message)s'
)
```

### 执行监控

```bash
# 监控Spark任务
# 访问 http://localhost:4040 查看Spark Web UI

# 监控Hive任务
beeline -u jdbc:hive2://localhost:10000
> SHOW TABLES;
> SELECT COUNT(*) FROM ads_platform_heat;
```

---

## ❓ 常见问题

### Q1: Hive连接失败？

**现象**: `Could not connect to meta store using thrift`

**解决方案**:
```bash
# 检查Metastore是否启动
jps | grep RunJar

# 启动Metastore
nohup hive --service metastore &

# 验证连接
beeline -u jdbc:hive2://localhost:10000
```

### Q2: Spark内存不足？

**现象**: `java.lang.OutOfMemoryError`

**解决方案**:
```python
# 修改Spark配置（在各脚本中）
spark = SparkSession.builder \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "20") \
    .getOrCreate()
```

### Q3: MySQL连接超时？

**现象**: `pymysql.err.OperationalError: (2003, "Can't connect to MySQL server")`

**解决方案**:
```python
# 增加连接超时时间（config.py）
MYSQL_CONFIG = {
    "host": "your_host",
    "connect_timeout": 60,  # 增加超时时间
    # ...其他配置
}
```

### Q4: 数值转换出错？

**现象**: `ValueError: could not convert string to float`

**解决方案**:
```python
# 检查Spark_Data_processing.py中的数值化函数
# 确保正则表达式能匹配所有格式
def convert_to_numeric(value_str):
    if value_str is None or value_str == "":
        return 0.0
    # 添加更多格式支持
    if "亿" in value_str:
        return float(value_str.replace("亿", "")) * 100000000
    # ...
```

### Q5: ADS表数据为0？

**现象**: `ads_user_avoid_pitfalls 表创建完成，共 0 条数据`

**排查步骤**:
```sql
-- 1. 检查DWD层是否有数据
SELECT COUNT(*) FROM hive_QimaoScraper.dwd_novel_data;

-- 2. 检查字段值分布
SELECT 
    COUNT(*) AS 总数,
    AVG(numeric_popularity) AS 平均热度,
    AVG(numeric_score) AS 平均评分
FROM hive_QimaoScraper.dwd_novel_data;

-- 3. 检查过滤条件是否过严
-- 修改Spark_dwd_to_ads.py中的过滤逻辑
```

---

## 🤝 贡献指南

欢迎贡献代码和建议！

### 贡献方式

1. Fork本项目
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 提交Pull Request

### 开发规范

- 代码风格: 遵循PEP 8
- 注释要求: 关键函数必须有文档字符串
- 测试要求: 新功能需包含单元测试

---

## 📄 许可证

本项目仅供**学习和研究使用**，请勿用于商业用途。

---

## 📮 联系方式

- **项目维护者**: 程序员Eighteen
- **问题反馈**: 请通过Issues提交
- **邮箱**: 3273495516@qq.com & eighteenthstuai@gmail.com

---

## 🙏 致谢

感谢以下开源项目：
- [Apache Spark](https://spark.apache.org/)
- [Apache Hive](https://hive.apache.org/)
- [Scrapy](https://scrapy.org/)
- [MySQL](https://www.mysql.com/)

---

<p align="center">
  <b>⭐ 如果这个项目对你有帮助，请给个Star支持一下！⭐</b>
</p>

- 作者: 程序员Eighteen
- 创建日期: 2025-12-16
