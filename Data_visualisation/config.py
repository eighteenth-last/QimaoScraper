"""
数据仓库配置文件
请根据实际环境修改配置参数
"""

# ============================================
# MySQL 配置
# ============================================
MYSQL_CONFIG = {
    # MySQL服务器地址
    "host": "172.31.142.67",
    
    # MySQL服务器端口
    "port": 3306,
    
    # 源数据库名称（爬虫数据存储）
    "database": "QimaoScraper",
    
    # 目标数据库名称（特征数据存储）
    "target_database": "QimaoScraper_Feature_Data",
    
    # MySQL用户名
    "user": "root",
    
    # MySQL密码（请修改为实际密码）
    "password": "qwer4321",
    
    # 字符集
    "charset": "utf8mb4"
}


# ============================================
# Hive 配置
# ============================================
HIVE_CONFIG = {
    # Hive数据库名称
    "database": "hive_QimaoScraper",
    
    # Hive Metastore URI
    "metastore_uri": "thrift://172.31.142.67:9083",
    
    # Hive数据仓库目录
    "warehouse_dir": "/user/hive/warehouse"
}


# ============================================
# Spark 配置
# ============================================
SPARK_CONFIG = {
    # Spark应用名称前缀
    "app_name_prefix": "QimaoScraper_DataWarehouse",
    
    # Spark驱动内存
    "driver_memory": "2g",
    
    # Spark执行器内存
    "executor_memory": "2g",
    
    # Shuffle分区数（根据数据量调整）
    "shuffle_partitions": 10,
    
    # 是否启用Hive支持
    "enable_hive_support": True
}


# ============================================
# 数据仓库表配置
# ============================================
WAREHOUSE_TABLES = {
    # ODS层表
    "ods": {
        "ods_novel_data": "原始小说数据表"
    },
    
    # DWD层表
    "dwd": {
        "dwd_novel_data": "清洗后的小说数据表"
    },


    # ADS层表
    "ads": {
        "ads_platform_heat": "平台侧 - 热度分析（一阶导数、二阶导数、冷启动监测）",
        "ads_platform_ranking_trend": "平台侧 - 榜单趋势分析（德不配位识别、转化率）",
        "ads_author_reason": "作者侧 - 热度原因分析（关键词归因、字数区间）",
        "ads_author_attenuation_effect": "作者侧 - 热度衰减效应（完结后衰减曲线）",
        "ads_user_layered_recommendation": "用户侧 - 分层推荐（圈层爆款、相对热度）",
        "ads_user_avoid_pitfalls": "用户侧 - 避坑指南（刷榜识别、高热低分预警）",
        "ads_capital_ltv": "资本侧 - IP长尾价值计算（热度积分累积、改编适配度）",
        "ads_capital_future_purchasing_power": "资本侧 - 粉丝粘性与购买力验证（ARPU分析）"
    }
}


# ============================================
# 业务规则配置
# ============================================
BUSINESS_RULES = {
    # 热度飙升阈值（增长率%）
    "heat_surge_threshold": 50,
    
    # 高分作品阈值
    "high_score_threshold": 8.0,
    
    # 低分作品阈值
    "low_score_threshold": 6.0,
    
    # 高热作品阈值（热度值）
    "high_heat_threshold": 1000000,
    
    # 异常值检测倍数（IQR倍数）
    "outlier_iqr_multiplier": 3,
    
    # 作者分级标准
    "author_levels": {
        "新人作家": {"min_books": 0, "max_books": 1},
        "新锐作家": {"min_books": 2, "max_books": 4},
        "资深作家": {"min_books": 5, "max_books": 9},
        "高产作家": {"min_books": 10, "max_books": float('inf')}
    },
    
    # IP潜力等级标准（基于ip_longtail_value）
    "ip_potential_levels": {
        "C级": {"min": 0, "max": 5},
        "B级": {"min": 5, "max": 10},
        "A级": {"min": 10, "max": 20},
        "S级": {"min": 20, "max": 50},
        "SS级": {"min": 50, "max": 100},
        "SSS级": {"min": 100, "max": float('inf')}
    },
    
    # 热度等级标准（基于numeric_popularity）
    "heat_levels": {
        "冷门": {"min": 0, "max": 100000},
        "中等": {"min": 100000, "max": 1000000},
        "热门": {"min": 1000000, "max": 10000000},
        "爆款": {"min": 10000000, "max": float('inf')}
    },
    
    # 分类热度等级标准（基于avg_popularity）
    "category_popularity_levels": {
        "小众": {"min": 0, "max": 500000},
        "中等": {"min": 500000, "max": 1000000},
        "热门": {"min": 1000000, "max": 5000000},
        "超热门": {"min": 5000000, "max": float('inf')}
    }
}


# ============================================
# 数据处理配置
# ============================================
DATA_PROCESSING = {
    # 是否删除完全空的列
    "drop_empty_columns": True,
    
    # 是否删除完全空的行
    "drop_empty_rows": True,
    
    # 缺失值填充策略
    "fill_strategy": {
        "numeric": "mean",      # mean, median, mode
        "categorical": "unknown"  # 分类字段默认填充值
    },
    
    # 是否进行异常值处理
    "handle_outliers": True,
    
    # 数据质量评分权重
    "quality_weights": {
        "popularity": 0.4,
        "read_count": 0.3,
        "score": 0.3
    }
}


# ============================================
# 导出配置
# ============================================
EXPORT_CONFIG = {
    # 导出方式：spark 或 sqoop
    "export_method": "spark",  # spark, sqoop
    
    # Sqoop配置
    "sqoop": {
        "num_mappers": 4,
        "input_null_string": "\\\\N",
        "input_null_non_string": "\\\\N"
    },
    
    # JDBC配置
    "jdbc": {
        "driver": "com.mysql.cj.jdbc.Driver",
        "fetch_size": 1000,
        "batch_size": 1000
    }
}


# ============================================
# 日志配置
# ============================================
LOG_CONFIG = {
    # 日志级别：DEBUG, INFO, WARNING, ERROR, CRITICAL
    "level": "INFO",
    
    # 日志格式
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    
    # 日志文件路径（None表示只输出到控制台）
    "file_path": None
}


# ============================================
# 性能优化配置
# ============================================
PERFORMANCE_CONFIG = {
    # 是否启用数据采样（用于开发测试）
    "enable_sampling": False,
    
    # 采样比例（0-1之间）
    "sample_ratio": 0.1,
    
    # 是否启用缓存
    "enable_cache": True,
    
    # 并行度
    "parallelism": 4
}


# ============================================
# 环境配置
# ============================================
ENVIRONMENT = {
    # 运行环境：development, production
    "mode": "development",
    
    # 是否启用调试模式
    "debug": True,
    
    # 是否显示详细日志
    "verbose": True
}


def get_config(config_name):
    """
    获取指定配置
    :param config_name: 配置名称
    :return: 配置字典
    """
    config_map = {
        "mysql": MYSQL_CONFIG,
        "hive": HIVE_CONFIG,
        "spark": SPARK_CONFIG,
        "tables": WAREHOUSE_TABLES,
        "business": BUSINESS_RULES,
        "processing": DATA_PROCESSING,
        "export": EXPORT_CONFIG,
        "log": LOG_CONFIG,
        "performance": PERFORMANCE_CONFIG,
        "environment": ENVIRONMENT
    }
    return config_map.get(config_name.lower(), {})


def validate_config():
    """
    验证配置的完整性和正确性
    :return: (bool, str) - (是否有效, 错误信息)
    """
    errors = []
    
    # 验证MySQL配置
    if MYSQL_CONFIG["password"] == "qwer4321":
        errors.append("请修改MySQL密码配置")
    
    # 验证Spark内存配置
    if not SPARK_CONFIG["driver_memory"].endswith(('g', 'G', 'm', 'M')):
        errors.append("Spark内存配置格式错误")
    
    # 验证业务规则
    if BUSINESS_RULES["heat_surge_threshold"] <= 0:
        errors.append("热度飙升阈值必须大于0")
    
    if errors:
        return False, "; ".join(errors)
    return True, "配置验证通过"


if __name__ == "__main__":
    # 验证配置
    is_valid, message = validate_config()
    if is_valid:
        print(f"✓ {message}")
    else:
        print(f"✗ 配置验证失败: {message}")
    
    # 打印配置摘要
    print("\n配置摘要:")
    print(f"MySQL主机: {MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}")
    print(f"Hive数据库: {HIVE_CONFIG['database']}")
    print(f"Spark应用前缀: {SPARK_CONFIG['app_name_prefix']}")
    print(f"ADS层表数量: {len(WAREHOUSE_TABLES['ads'])}")
    print(f"运行环境: {ENVIRONMENT['mode']}")
