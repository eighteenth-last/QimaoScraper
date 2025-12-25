"""
*@BelongsProject: Python
*@BelongsPackage:
*@Author: 程序员Eighteen
*@CreateTime: 2025-12-16  11:11
*@Description: 用Spark SQL将dwd层的数据处理成ads层的数据表结构 - 严格按照数仓建模方案
*@Version: 2.0 - 8个ADS表版本
"""
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lit, when, lag, avg, max, min, sum as spark_sum, count, countDistinct, stddev, datediff, length, dense_rank, first,
    desc, least, collect_list, log10, row_number
)
from pyspark.sql.types import *
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DWDToADSProcessor:
    """DWD层到ADS层的特征工程处理 - 严格按照数仓建模方案创建8个ADS表"""
    
    def __init__(self, hive_database="hive_QimaoScraper"):
        """
        初始化配置
        :param hive_database: Hive数据库名称
        """
        self.hive_database = hive_database
        self.spark = None
        
    def init_spark(self):
        """初始化Spark会话"""
        try:
            self.spark = SparkSession.builder \
                .appName("DWD_to_ADS_Feature_Engineering") \
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
                .config("hive.metastore.uris", "thrift://localhost:9083") \
                .config("spark.sql.shuffle.partitions", "10") \
                .enableHiveSupport() \
                .getOrCreate()
            
            logger.info("Spark会话初始化成功")
            return self.spark
        except Exception as e:
            logger.error(f"Spark会话初始化失败: {str(e)}")
            raise
    
    def load_dwd_data(self, dwd_table="dwd_novel_data"):
        """
        从DWD层加载数据
        :param dwd_table: DWD层表名
        :return: DataFrame
        """
        try:
            df = self.spark.table(f"{self.hive_database}.{dwd_table}")
            logger.info(f"从DWD层加载数据成功，共 {df.count()} 条数据")
            return df
        except Exception as e:
            logger.error(f"加载DWD数据失败: {str(e)}")
            raise
    
    # ============================================
    # 平台侧 - 2个表
    # ============================================
    
    def create_ads_platform_heat(self, df):
        """
        平台侧1: ads_platform_heat - 热度分析
        分析逻辑：计算同一 book_id 在不同 rank_date 的热度差值（一阶导数）和差值的变化率（二阶导数）
        涉及字段：book_id, numeric_popularity, rank_date, category1_name
        场景：
        - 环比增速：查询昨日与今日热度差值超过阈值的作品
        - 冷启动监测：筛选 created_at 在7天内，且 numeric_popularity 增长斜率高于同类平均值的作品
        """
        try:
            logger.info("开始创建 ads_platform_heat 表...")
            
            # 创建窗口函数：按book_id分区，按rank_date排序
            window_by_book = Window.partitionBy("book_id").orderBy("rank_date")
            window_by_category = Window.partitionBy("category1_name")
            
            # 计算热度变化
            ads_platform_heat = df.select(
                col("book_id"),
                col("title"),
                col("author"),
                col("category1_name"),
                col("category2_name"),
                col("rank_name"),
                col("rank_date"),
                col("created_at"),
                col("numeric_popularity"),
                col("numeric_read_count"),
                col("numeric_score"),
                col("status"),
                col("gender_type")
            ).withColumn(
                # 前一天的热度
                "prev_day_popularity",
                lag("numeric_popularity", 1).over(window_by_book)
            ).withColumn(
                # 热度差值（一阶导数）
                "popularity_diff",
                when(col("prev_day_popularity").isNotNull(),
                     col("numeric_popularity") - col("prev_day_popularity"))
                .otherwise(lit(0))
            ).withColumn(
                # 热度变化率（环比增速）
                "popularity_growth_rate",
                when((col("prev_day_popularity").isNotNull()) & (col("prev_day_popularity") > 0),
                     ((col("numeric_popularity") - col("prev_day_popularity")) / col("prev_day_popularity")) * 100)
                .otherwise(lit(0))
            ).withColumn(
                # 前一天的热度差值
                "prev_popularity_diff",
                lag("popularity_diff", 1).over(window_by_book)
            ).withColumn(
                # 热度加速度（二阶导数）
                "popularity_acceleration",
                when(col("prev_popularity_diff").isNotNull(),
                     col("popularity_diff") - col("prev_popularity_diff"))
                .otherwise(lit(0))
            ).withColumn(
                # 同类平均增长率
                "category_avg_growth_rate",
                avg("popularity_growth_rate").over(window_by_category)
            ).withColumn(
                # 是否新书（7天内）
                "is_new_book",
                when(datediff(col("rank_date"), col("created_at")) <= 7, lit(1)).otherwise(lit(0))
            ).withColumn(
                # 是否高增长（超过同类平均）
                "is_high_growth",
                when(col("popularity_growth_rate") > col("category_avg_growth_rate"), lit(1)).otherwise(lit(0))
            ).withColumn(
                # 是否冷启动优质作品（新书且高增长）
                "is_cold_start_quality",
                when((col("is_new_book") == 1) & (col("is_high_growth") == 1), lit(1)).otherwise(lit(0))
            ).withColumn(
                # 热度等级
                "heat_level",
                when(col("numeric_popularity") >= 10000000, lit("爆款"))
                .when(col("numeric_popularity") >= 1000000, lit("热门"))
                .when(col("numeric_popularity") >= 100000, lit("中等"))
                .otherwise(lit("冷门"))
            )
            
            # 只保留最新日期的记录（快照）
            window_latest = Window.partitionBy("book_id").orderBy(desc("rank_date"))
            ads_platform_heat_latest = ads_platform_heat.withColumn("rn", row_number().over(window_latest)) \
                .filter(col("rn") == 1) \
                .drop("rn")
            
            logger.info(f"ads_platform_heat 表创建完成，共 {ads_platform_heat_latest.count()} 条数据")
            return ads_platform_heat_latest
            
        except Exception as e:
            logger.error(f"创建 ads_platform_heat 失败: {str(e)}")
            raise
    
    def create_ads_platform_ranking_trend(self, df):
        """
        平台侧2: ads_platform_ranking_trend - 榜单趋势分析
        分析逻辑：分析 rank_name（如"推荐榜"）与 read_count 增长的相关性
        如果某本书上了榜但阅读量未随之增长，说明"德不配位"，应降权
        优化动作：动态调整推荐位权重，将流量向高转化率（高评分 score + 高留存）作品倾斜
        """
        try:
            logger.info("开始创建 ads_platform_ranking_trend 表...")
            
            # 创建窗口函数
            # 修改：计算增长率时应该按 book_id 分区，而不是 (book_id, rank_name)
            # 这样可以捕捉到书在不同榜单间流转时的总体阅读量增长
            window_by_book = Window.partitionBy("book_id").orderBy("rank_date")
            
            # 计算榜单效应
            ads_platform_ranking_trend = df.select(
                col("book_id"),
                col("title"),
                col("author"),
                col("rank_name"),
                col("category1_name"),
                col("rank_date"),
                col("numeric_popularity"),
                col("numeric_read_count"),
                col("numeric_score"),
                col("status"),
                col("gender_type")
            ).withColumn(
                # 前一期阅读量
                "prev_read_count",
                lag("numeric_read_count", 1).over(window_by_book)
            ).withColumn(
                # 阅读量增长
                "read_count_growth",
                when(col("prev_read_count").isNotNull(),
                     col("numeric_read_count") - col("prev_read_count"))
                .otherwise(lit(0))
            ).withColumn(
                # 阅读量增长率
                "read_count_growth_rate",
                when((col("prev_read_count").isNotNull()) & (col("prev_read_count") > 0),
                     ((col("numeric_read_count") - col("prev_read_count")) / col("prev_read_count")) * 100)
                .otherwise(lit(0))
            ).withColumn(
                # 榜单转化率（阅读量增长是否跟得上）
                "ranking_conversion_rate",
                when(col("numeric_popularity") > 0,
                     col("read_count_growth") / col("numeric_popularity") * 100)
                .otherwise(lit(0))
            ).withColumn(
                # 是否德不配位（上榜但阅读量增长低）
                "is_unworthy",
                when((col("numeric_popularity") > 1000000) & (col("read_count_growth_rate") < 5), lit(1))
                .otherwise(lit(0))
            ).withColumn(
                # 高转化率标记（高评分+高阅读量增长）
                "is_high_conversion",
                when((col("numeric_score") >= 8.0) & (col("read_count_growth_rate") > 20), lit(1))
                .otherwise(lit(0))
            ).withColumn(
                # 推荐权重（综合评分和转化率）
                "recommend_weight",
                (col("numeric_score") / 10) * 0.5 + 
                (when(col("ranking_conversion_rate") > 0, 
                      least(col("ranking_conversion_rate") / 100, lit(1))).otherwise(0)) * 0.5
            )
            
            # 只保留最新日期的记录（快照）
            # 修改：按 (book_id, rank_name) 分区，保留每本书在每个榜单上的最新状态
            window_latest = Window.partitionBy("book_id", "rank_name").orderBy(desc("rank_date"))
            ads_platform_ranking_trend_latest = ads_platform_ranking_trend.withColumn("rn", row_number().over(window_latest)) \
                .filter(col("rn") == 1) \
                .drop("rn")
            
            logger.info(f"ads_platform_ranking_trend 表创建完成，共 {ads_platform_ranking_trend_latest.count()} 条数据")
            return ads_platform_ranking_trend_latest
            
        except Exception as e:
            logger.error(f"创建 ads_platform_ranking_trend 失败: {str(e)}")
            raise
    
    # ============================================
    # 作者侧 - 2个表
    # ============================================
    
    def create_ads_author_reason(self, df):
        """
        作者侧1: ads_author_reason - 分析热度原因
        分析逻辑：将 intro（简介）和 title 进行分词处理，与 numeric_popularity 做回归分析
        涉及字段：category2_name（细分题材）, intro, words_num, status
        洞察输出：
        - 在'都市'分类下，简介包含'系统'、'逆袭'关键词的作品，平均初始热度高出30%
        - 字数在 50-80万字 区间且状态为 连载中 的作品，热度爬升最快
        """
        try:
            logger.info("开始创建 ads_author_reason 表...")
            
            # 只选取最新数据进行统计分析，避免历史数据重复计算权重
            window_latest = Window.partitionBy("book_id").orderBy(desc("rank_date"))
            df_latest = df.withColumn("rn", row_number().over(window_latest)).filter(col("rn") == 1).drop("rn")
            
            # 分析作品特征与热度的关系
            # 修改：移除 intro 字段依赖，只分析 title
            ads_author_reason = df_latest.select(
                col("book_id"),
                col("title"),
                col("author"),
                col("category1_name"),
                col("category2_name"),
                # col("intro"), # 已移除 intro 字段
                col("status"),
                col("numeric_words"),
                col("numeric_popularity"),
                col("numeric_read_count"),
                col("numeric_score"),
                col("gender_type"),
                col("rank_date")
            ).withColumn(
                # 字数区间分类
                "words_range",
                when(col("numeric_words") < 200000, lit("短篇(<20万)"))
                .when(col("numeric_words") < 500000, lit("中篇(20-50万)"))
                .when(col("numeric_words") < 800000, lit("长篇(50-80万)"))
                .when(col("numeric_words") < 1500000, lit("超长篇(80-150万)"))
                .otherwise(lit("巨著(>150万)"))
            ).withColumn(
                # 简介长度 - intro 字段已移除，设为 0
                "intro_length",
                lit(0)
            ).withColumn(
                # 标题长度
                "title_length",
                when(col("title").isNotNull(), length(col("title"))).otherwise(0)
            ).withColumn(
                # 是否包含热门关键词（仅分析 title）
                "has_hot_keywords",
                when(
                    col("title").rlike("系统|逆袭|重生|穿越|霸总|修仙|玄幻|都市|言情"),
                    lit(1)
                ).otherwise(lit(0))
            ).withColumn(
                # 是否黄金字数区间（50-80万且连载中）
                "is_golden_range",
                when((col("numeric_words").between(500000, 800000)) & (col("status") == "连载中"), lit(1))
                .otherwise(lit(0))
            ).withColumn(
                # 热度等级
                "popularity_level",
                when(col("numeric_popularity") >= 10000000, lit(5))
                .when(col("numeric_popularity") >= 1000000, lit(4))
                .when(col("numeric_popularity") >= 500000, lit(3))
                .when(col("numeric_popularity") >= 100000, lit(2))
                .otherwise(lit(1))
            )
            
            # 按分类和特征聚合统计
            ads_author_reason_agg = ads_author_reason.groupBy(
                "category1_name", "category2_name", "words_range", 
                "status", "has_hot_keywords", "is_golden_range"
            ).agg(
                count("book_id").alias("book_count"),
                avg("numeric_popularity").alias("avg_popularity"),
                avg("numeric_score").alias("avg_score"),
                avg("numeric_words").alias("avg_words"),
                avg("intro_length").alias("avg_intro_length"),
                avg("title_length").alias("avg_title_length")
            ).withColumn(
                # 热度指数（相对平均热度）
                "heat_index",
                col("avg_popularity") / 1000000
            )
            
            logger.info(f"ads_author_reason 表创建完成，共 {ads_author_reason_agg.count()} 条数据")
            return ads_author_reason_agg
            
        except Exception as e:
            logger.error(f"创建 ads_author_reason 失败: {str(e)}")
            raise
    
    def create_ads_author_attenuation_effect(self, df):
        """
        作者侧2: ads_author_attenuation_effect - 分析热度衰减效应
        断更/完结热度衰减模型
        分析逻辑：监控 status 变为"已完结"后，numeric_popularity 的衰减曲线
        涉及字段：status, updated_at, rank_date
        策略建议：计算最佳"完结时长"，建议作者在热度衰减至 20% 前推出新书
        """
        try:
            logger.info("开始创建 ads_author_attenuation_effect 表...")
            
            # 筛选有完结和连载状态的作品
            window_by_book = Window.partitionBy("book_id").orderBy("rank_date")
            
            # 找出每本书的峰值热度和完结后热度变化
            ads_attenuation = df.select(
                col("book_id"),
                col("title"),
                col("author"),
                col("category1_name"),
                col("status"),
                col("rank_date"),
                col("updated_at"),
                col("numeric_popularity"),
                col("numeric_score"),
                col("gender_type")
            ).withColumn(
                # 该书的历史最高热度
                "peak_popularity",
                max("numeric_popularity").over(Window.partitionBy("book_id"))
            ).withColumn(
                # 相对峰值的热度衰减率
                "attenuation_rate",
                when(col("peak_popularity") > 0,
                     ((col("peak_popularity") - col("numeric_popularity")) / col("peak_popularity")) * 100)
                .otherwise(lit(0))
            ).withColumn(
                # 前一次的热度
                "prev_popularity",
                lag("numeric_popularity", 1).over(window_by_book)
            ).withColumn(
                # 热度衰减速度
                "attenuation_speed",
                when((col("prev_popularity").isNotNull()) & (col("prev_popularity") > 0),
                     ((col("prev_popularity") - col("numeric_popularity")) / col("prev_popularity")) * 100)
                .otherwise(lit(0))
            ).withColumn(
                # 距离峰值的天数
                "days_from_peak",
                datediff(col("rank_date"), 
                        first("rank_date").over(Window.partitionBy("book_id").orderBy(desc("numeric_popularity"))))
            ).withColumn(
                # 是否已严重衰减（低于峰值80%）
                "is_severe_attenuation",
                when(col("attenuation_rate") >= 80, lit(1)).otherwise(lit(0))
            ).withColumn(
                # 是否中度衰减（低于峰值50-80%）
                "is_moderate_attenuation",
                when(col("attenuation_rate").between(50, 80), lit(1)).otherwise(lit(0))
            )
            
            # 按作品聚合，计算整体衰减特征
            ads_author_attenuation_effect = ads_attenuation.groupBy(
                "book_id", "title", "author", "category1_name", "status", "gender_type"
            ).agg(
                max("peak_popularity").alias("peak_popularity"),
                min("numeric_popularity").alias("lowest_popularity"),
                avg("attenuation_rate").alias("avg_attenuation_rate"),
                avg("attenuation_speed").alias("avg_attenuation_speed"),
                max("days_from_peak").alias("lifecycle_days"),
                avg("numeric_score").alias("avg_score"),
                max("is_severe_attenuation").alias("has_severe_attenuation")
            ).withColumn(
                # 总衰减幅度
                "total_attenuation",
                when(col("peak_popularity") > 0,
                     ((col("peak_popularity") - col("lowest_popularity")) / col("peak_popularity")) * 100)
                .otherwise(lit(0))
            ).withColumn(
                # 衰减类型
                "attenuation_type",
                when(col("total_attenuation") < 20, lit("稳定型"))
                .when(col("total_attenuation") < 50, lit("缓慢衰减"))
                .when(col("total_attenuation") < 80, lit("快速衰减"))
                .otherwise(lit("断崖式衰减"))
            ).withColumn(
                # 建议推出新书时间（天数）
                "recommended_new_book_days",
                when(col("avg_attenuation_speed") > 0,
                     (20 / col("avg_attenuation_speed")) * 1)  # 衰减至20%所需天数
                .otherwise(lit(90))  # 默认90天
            )
            
            logger.info(f"ads_author_attenuation_effect 表创建完成，共 {ads_author_attenuation_effect.count()} 条数据")
            return ads_author_attenuation_effect
            
        except Exception as e:
            logger.error(f"创建 ads_author_attenuation_effect 失败: {str(e)}")
            raise
    
    # ============================================
    # 用户侧 - 2个表
    # ============================================
    
    def create_ads_user_layered_recommendation(self, df):
        """
        用户侧1: ads_user_layered_recommendation - 分层推荐
        小众热度挖掘（圈层爆款）
        分析逻辑：不看全局热度，看分类内相对热度
        涉及字段：category1_name, gender_type, score
        SQL场景：在 gender_type = 'male' 且 category1_name = '科幻' 的小圈子里，
                寻找 score > 9.0 且 numeric_popularity 排名 Top 5 的书
        """
        try:
            logger.info("开始创建 ads_user_layered_recommendation 表...")
            
            # 只选取最新数据进行推荐计算
            window_latest = Window.partitionBy("book_id").orderBy(desc("rank_date"))
            df_latest = df.withColumn("rn", row_number().over(window_latest)).filter(col("rn") == 1).drop("rn")
            
            # 创建窗口函数：按分类和性别分区
            window_by_category = Window.partitionBy("category1_name", "gender_type")
            window_by_subcategory = Window.partitionBy("category1_name", "category2_name", "gender_type")
            
            ads_user_layered_recommendation = df_latest.select(
                col("book_id"),
                col("title"),
                col("author"),
                col("category1_name"),
                col("category2_name"),
                col("gender_type"),
                col("numeric_popularity"),
                col("numeric_score"),
                col("numeric_read_count"),
                col("status")
            ).withColumn(
                # 分类内热度排名
                "category_heat_rank",
                dense_rank().over(window_by_category.orderBy(desc("numeric_popularity")))
            ).withColumn(
                # 细分类内热度排名
                "subcategory_heat_rank",
                dense_rank().over(window_by_subcategory.orderBy(desc("numeric_popularity")))
            ).withColumn(
                # 分类内平均热度
                "category_avg_popularity",
                avg("numeric_popularity").over(window_by_category)
            ).withColumn(
                # 相对热度（相对于分类平均）
                "relative_heat",
                when(col("category_avg_popularity") > 0,
                     col("numeric_popularity") / col("category_avg_popularity"))
                .otherwise(lit(0))
            ).withColumn(
                # 是否圈层爆款（分类内前10且高分）
                "is_niche_hit",
                when((col("category_heat_rank") <= 10) & (col("numeric_score") >= 8.5), lit(1))
                .otherwise(lit(0))
            ).withColumn(
                # 是否高分小众（评分高但全局热度不高）
                "is_high_score_niche",
                when((col("numeric_score") >= 9.0) & 
                     (col("numeric_popularity") < 5000000) & 
                     (col("category_heat_rank") <= 5), lit(1))
                .otherwise(lit(0))
            ).withColumn(
                # 推荐分（圈层内）
                "niche_recommendation_score",
                (col("numeric_score") / 10) * 0.7 + 
                (col("relative_heat") / 5) * 0.3  # 相对热度最高为5倍
            ).withColumn(
                # 推荐等级
                "recommendation_level",
                when(col("niche_recommendation_score") >= 0.9, lit("强烈推荐"))
                .when(col("niche_recommendation_score") >= 0.8, lit("推荐"))
                .when(col("niche_recommendation_score") >= 0.7, lit("可看"))
                .otherwise(lit("一般"))
            )
            
            # 筛选值得推荐的作品（分类内前20或高分作品）
            ads_user_layered_recommendation = ads_user_layered_recommendation.filter(
                (col("category_heat_rank") <= 20) | (col("numeric_score") >= 8.5)
            )
            
            logger.info(f"ads_user_layered_recommendation 表创建完成，共 {ads_user_layered_recommendation.count()} 条数据")
            return ads_user_layered_recommendation
            
        except Exception as e:
            logger.error(f"创建 ads_user_layered_recommendation 失败: {str(e)}")
            raise
    
    def create_ads_user_avoid_pitfalls(self, df):
        """
        用户侧2: ads_user_avoid_pitfalls - 避坑指南
        避坑指南（高热低分预警）
        分析逻辑：识别"刷榜"或"黑红"特征
        指标计算：计算 numeric_popularity 与 score 的比率
        如果热度极高但评分极低（<6.0），则在推荐算法中降权
        """
        try:
            logger.info("开始创建 ads_user_avoid_pitfalls 表...")
            
            # 只选取最新数据进行避坑分析
            window_latest = Window.partitionBy("book_id").orderBy(desc("rank_date"))
            df_latest = df.withColumn("rn", row_number().over(window_latest)).filter(col("rn") == 1).drop("rn")
            
            ads_user_avoid_pitfalls = df_latest.select(
                col("book_id"),
                col("title"),
                col("author"),
                col("category1_name"),
                col("category2_name"),
                col("rank_name"),
                col("gender_type"),
                col("numeric_popularity"),
                col("numeric_score"),
                col("numeric_read_count"),
                col("rank_date")
            ).withColumn(
                # 热度评分比（热度/评分，越高越可疑）
                "heat_score_ratio",
                when(col("numeric_score") > 0,
                     col("numeric_popularity") / col("numeric_score") / 100000)  # 标准化
                .otherwise(lit(0))
            ).withColumn(
                # 是否高热低分（热度高但评分低）
                "is_high_heat_low_score",
                when((col("numeric_popularity") >= 5000000) & (col("numeric_score") < 6.0), lit(1))
                .otherwise(lit(0))
            ).withColumn(
                # 是否可疑刷榜（热度极高但评分极低）
                "is_suspicious_boost",
                when((col("numeric_popularity") >= 10000000) & (col("numeric_score") < 5.5), lit(1))
                .otherwise(lit(0))
            ).withColumn(
                # 阅读转化率（阅读量/热度）
                "read_conversion",
                when(col("numeric_popularity") > 0,
                     col("numeric_read_count") / col("numeric_popularity") * 100)
                .otherwise(lit(0))
            ).withColumn(
                # 是否低转化（热度高但阅读量低，可能刷热度）
                "is_low_conversion",
                when((col("numeric_popularity") >= 5000000) & (col("read_conversion") < 10), lit(1))
                .otherwise(lit(0))
            ).withColumn(
                # 风险等级
                "risk_level",
                when(col("is_suspicious_boost") == 1, lit("高风险"))
                .when(col("is_high_heat_low_score") == 1, lit("中风险"))
                .when(col("is_low_conversion") == 1, lit("低风险"))
                .otherwise(lit("正常"))
            ).withColumn(
                # 推荐权重（降权处理）
                "recommendation_weight",
                when(col("is_suspicious_boost") == 1, lit(0.3))  # 可疑刷榜降权70%
                .when(col("is_high_heat_low_score") == 1, lit(0.5))  # 高热低分降权50%
                .when(col("is_low_conversion") == 1, lit(0.7))  # 低转化降权30%
                .otherwise(lit(1.0))  # 正常权重
            ).withColumn(
                # 避坑建议
                "avoidance_advice",
                when(col("is_suspicious_boost") == 1, lit("强烈建议避开，疑似刷榜"))
                .when(col("is_high_heat_low_score") == 1, lit("谨慎阅读，评分较低"))
                .when(col("is_low_conversion") == 1, lit("注意，热度与实际阅读不符"))
                .otherwise(lit("可正常阅读"))
            )
            
            # 保留所有数据（不过滤），便于完整分析
            # 注释：如果数据量过大，可以取消下面的注释来过滤
            # ads_user_avoid_pitfalls = ads_user_avoid_pitfalls.filter(
            #     (col("risk_level") != "正常") | (col("numeric_popularity") >= 100000)
            # )
            
            logger.info(f"ads_user_avoid_pitfalls 表创建完成，共 {ads_user_avoid_pitfalls.count()} 条数据")
            return ads_user_avoid_pitfalls
            
        except Exception as e:
            logger.error(f"创建 ads_user_avoid_pitfalls 失败: {str(e)}")
            raise
    
    # ============================================
    # 资本侧 - 2个表
    # ============================================
    
    def create_ads_capital_ltv(self, df):
        """
        资本侧1: ads_capital_ltv - IP 长尾价值计算（LTV）
        分析逻辑：热度积分（Heat Integral）。计算一本书在时间轴上 numeric_popularity 的累积面积
        涉及字段：rank_date, numeric_popularity, date_month
        决策支持：
        - 短线IP：爆发快，衰减快（适合短剧改编）
        - 长线IP：热度平稳上升，持续时间长，累积积分高（适合出版、动漫实体化）
        """
        try:
            logger.info("开始创建 ads_capital_ltv 表...")
            
            # 按书籍聚合，计算生命周期价值
            ads_capital_ltv = df.groupBy(
                "book_id", "title", "author", "category1_name", "category2_name", 
                "status", "gender_type"
            ).agg(
                # 热度积分（累积热度）
                spark_sum("numeric_popularity").alias("total_heat_integral"),
                # 平均热度
                avg("numeric_popularity").alias("avg_popularity"),
                # 峰值热度
                max("numeric_popularity").alias("peak_popularity"),
                # 最低热度
                min("numeric_popularity").alias("lowest_popularity"),
                # 上榜次数
                count("*").alias("appearance_count"),
                # 首次上榜日期
                min("rank_date").alias("first_rank_date"),
                # 最近上榜日期
                max("rank_date").alias("latest_rank_date"),
                # 平均评分
                avg("numeric_score").alias("avg_score"),
                # 平均阅读量
                avg("numeric_read_count").alias("avg_read_count"),
                # 总字数
                avg("numeric_words").alias("total_words")
            ).withColumn(
                # 生命周期天数
                "lifecycle_days",
                datediff(col("latest_rank_date"), col("first_rank_date")) + 1
            ).withColumn(
                # 日均热度
                "daily_avg_heat",
                when(col("lifecycle_days") > 0,
                     col("total_heat_integral") / col("lifecycle_days"))
                .otherwise(col("avg_popularity"))
            ).withColumn(
                # 热度波动率（峰值与平均的比值）
                "heat_volatility",
                when(col("avg_popularity") > 0,
                     col("peak_popularity") / col("avg_popularity"))
                .otherwise(lit(0))
            ).withColumn(
                # 热度稳定性（最低热度与平均热度的比值）
                "heat_stability",
                when(col("avg_popularity") > 0,
                     col("lowest_popularity") / col("avg_popularity"))
                .otherwise(lit(0))
            ).withColumn(
                # IP类型（基于波动率和生命周期）
                "ip_type",
                when((col("heat_volatility") > 3) & (col("lifecycle_days") < 30), lit("短线爆款"))
                .when((col("heat_volatility") <= 2) & (col("lifecycle_days") >= 90), lit("长线稳定"))
                .when(col("heat_volatility") > 2, lit("波动型"))
                .otherwise(lit("成长型"))
            ).withColumn(
                # LTV分数（长尾价值综合评分）
                "ltv_score",
                (col("total_heat_integral") / 10000000) * 0.4 +  # 累积热度权重40%
                (col("lifecycle_days") / 365) * 0.3 +  # 生命周期权重30%
                (col("avg_score") / 10) * 0.2 +  # 评分权重20%
                (col("heat_stability")) * 0.1  # 稳定性权重10%
            ).withColumn(
                # IP价值等级
                "ip_value_level",
                when(col("ltv_score") >= 10, lit("SSS级"))
                .when(col("ltv_score") >= 5, lit("SS级"))
                .when(col("ltv_score") >= 2, lit("S级"))
                .when(col("ltv_score") >= 1, lit("A级"))
                .when(col("ltv_score") >= 0.5, lit("B级"))
                .otherwise(lit("C级"))
            ).withColumn(
                # 改编建议
                "adaptation_suggestion",
                when(col("ip_type") == "短线爆款", lit("适合短剧、网络电影"))
                .when(col("ip_type") == "长线稳定", lit("适合影视剧、动漫、游戏"))
                .when(col("ip_type") == "波动型", lit("适合短期营销炒作"))
                .otherwise(lit("需持续观察"))
            )
            
            logger.info(f"ads_capital_ltv 表创建完成，共 {ads_capital_ltv.count()} 条数据")
            return ads_capital_ltv
            
        except Exception as e:
            logger.error(f"创建 ads_capital_ltv 失败: {str(e)}")
            raise
    
    def create_ads_capital_future_purchasing_power(self, df):
        """
        资本侧2: ads_capital_future_purchasing_power - 粉丝粘性与购买力验证
        分析逻辑：利用 number（榜单排名数值）和 read_count 的比对
        涉及字段：unit（单位）, read_count
        假设：如果 read_count（阅读人数）相对较低，但 rank_name（如打赏榜/月票榜）很高，
             说明核心粉丝付费能力极强（ARPU高），是优质资本标的
        """
        try:
            logger.info("开始创建 ads_capital_future_purchasing_power 表...")
            
            # 分析粉丝粘性和购买力
            ads_capital_purchasing = df.select(
                col("book_id"),
                col("title"),
                col("author"),
                col("category1_name"),
                col("rank_name"),
                col("number"),
                col("unit"),
                col("numeric_popularity"),
                col("numeric_read_count"),
                col("numeric_score"),
                col("gender_type"),
                col("rank_date")
            ).withColumn(
                # ARPU指标（热度/阅读量，越高说明付费能力越强）
                "arpu_indicator",
                when(col("numeric_read_count") > 0,
                     col("numeric_popularity") / col("numeric_read_count"))
                .otherwise(lit(0))
            ).withColumn(
                # 是否付费榜（打赏榜、月票榜等）
                "is_payment_rank",
                when(col("rank_name").rlike("打赏|月票|畅销|VIP"), lit(1))
                .otherwise(lit(0))
            ).withColumn(
                # 榜单排名数值（转换为数值）
                "rank_number",
                when(col("number").isNotNull(), col("number").cast("double"))
                .otherwise(lit(999))
            ).withColumn(
                # 粉丝质量指数（付费榜排名高+阅读量适中=高质量粉丝）
                "fan_quality_index",
                when((col("is_payment_rank") == 1) & (col("rank_number") <= 50),
                     col("arpu_indicator") * 1.5)  # 付费榜加权
                .otherwise(col("arpu_indicator"))
            ).withColumn(
                # 是否高ARPU作品
                "is_high_arpu",
                when((col("arpu_indicator") > 50) & (col("is_payment_rank") == 1), lit(1))
                .otherwise(lit(0))
            ).withColumn(
                # 粉丝粘性等级
                "fan_stickiness_level",
                when(col("fan_quality_index") >= 100, lit("极强"))
                .when(col("fan_quality_index") >= 50, lit("强"))
                .when(col("fan_quality_index") >= 20, lit("中等"))
                .otherwise(lit("弱"))
            ).withColumn(
                # 购买力评级
                "purchasing_power_rating",
                when((col("is_high_arpu") == 1) & (col("numeric_score") >= 8.0), lit("优质资本标的"))
                .when(col("is_high_arpu") == 1, lit("高付费潜力"))
                .when(col("arpu_indicator") > 20, lit("中等付费潜力"))
                .otherwise(lit("一般"))
            )
            
            # 按书籍聚合，计算整体粉丝特征
            ads_capital_future_purchasing_power = ads_capital_purchasing.groupBy(
                "book_id", "title", "author", "category1_name", "gender_type"
            ).agg(
                avg("arpu_indicator").alias("avg_arpu"),
                max("arpu_indicator").alias("max_arpu"),
                avg("fan_quality_index").alias("avg_fan_quality"),
                max("is_payment_rank").alias("has_payment_rank"),
                max("is_high_arpu").alias("is_high_arpu_book"),
                avg("numeric_score").alias("avg_score"),
                avg("numeric_read_count").alias("avg_read_count"),
                avg("numeric_popularity").alias("avg_popularity"),
                count("*").alias("appearance_count")
            ).withColumn(
                # 综合粉丝价值评分
                "fan_value_score",
                (col("avg_arpu") / 100) * 0.5 +  # ARPU权重50%
                (col("avg_fan_quality") / 100) * 0.3 +  # 粉丝质量权重30%
                (col("avg_score") / 10) * 0.2  # 作品质量权重20%
            ).withColumn(
                # 投资价值等级
                "investment_value_level",
                when(col("fan_value_score") >= 2, lit("S级"))
                .when(col("fan_value_score") >= 1, lit("A级"))
                .when(col("fan_value_score") >= 0.5, lit("B级"))
                .otherwise(lit("C级"))
            ).withColumn(
                # 是否推荐投资
                "investment_recommendation",
                when((col("is_high_arpu_book") == 1) & (col("avg_score") >= 8.0), 
                     lit("强烈推荐-高付费+高质量"))
                .when(col("is_high_arpu_book") == 1, lit("推荐-高付费潜力"))
                .when(col("avg_fan_quality") >= 50, lit("可考虑-粉丝粘性强"))
                .otherwise(lit("观望"))
            )
            
            logger.info(f"ads_capital_future_purchasing_power 表创建完成，共 {ads_capital_future_purchasing_power.count()} 条数据")
            return ads_capital_future_purchasing_power
            
        except Exception as e:
            logger.error(f"创建 ads_capital_future_purchasing_power 失败: {str(e)}")
            raise
    
    def save_to_ads(self, df, ads_table_name):
        """
        保存数据到ADS层
        :param df: DataFrame
        :param ads_table_name: ADS层表名
        """
        try:
            # 删除已存在的表
            self.spark.sql(f"DROP TABLE IF EXISTS {self.hive_database}.{ads_table_name}")
            
            # 保存为Hive表
            df.write \
                .mode("overwrite") \
                .format("hive") \
                .saveAsTable(f"{self.hive_database}.{ads_table_name}")
            
            logger.info(f"数据成功保存到ADS层表 {self.hive_database}.{ads_table_name}")
            
            # 验证数据
            count = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {self.hive_database}.{ads_table_name}").collect()[0]['cnt']
            logger.info(f"验证: ADS层表 {ads_table_name} 共有 {count} 条数据")
            
            # 显示数据样例
            logger.info(f"{ads_table_name} 数据样例:")
            self.spark.sql(f"SELECT * FROM {self.hive_database}.{ads_table_name} LIMIT 3").show(truncate=False)
            
        except Exception as e:
            logger.error(f"保存数据到ADS层失败: {str(e)}")
            raise
    
    def run(self):
        """执行完整的DWD到ADS特征工程流程 - 创建8个ADS表"""
        try:
            logger.info("=" * 60)
            logger.info("开始执行 DWD -> ADS 特征工程处理（8个ADS表）")
            logger.info("=" * 60)
            
            # 1. 初始化Spark
            self.init_spark()
            
            # 2. 从DWD层加载数据
            dwd_df = self.load_dwd_data()
            
            # 缓存DWD数据，提高性能
            dwd_df.cache()
            
            # ============================================
            # 3. 平台侧 - 2个表
            # ============================================
            logger.info("\n" + "=" * 60)
            logger.info("【平台侧】创建ADS表")
            logger.info("=" * 60)
            
            logger.info("\n[1/8] 处理 ads_platform_heat - 热度分析...")
            ads_platform_heat = self.create_ads_platform_heat(dwd_df)
            self.save_to_ads(ads_platform_heat, "ads_platform_heat")
            
            logger.info("\n[2/8] 处理 ads_platform_ranking_trend - 榜单趋势分析...")
            ads_platform_ranking_trend = self.create_ads_platform_ranking_trend(dwd_df)
            self.save_to_ads(ads_platform_ranking_trend, "ads_platform_ranking_trend")
            
            # ============================================
            # 4. 作者侧 - 2个表
            # ============================================
            logger.info("\n" + "=" * 60)
            logger.info("【作者侧】创建ADS表")
            logger.info("=" * 60)
            
            logger.info("\n[3/8] 处理 ads_author_reason - 分析热度原因...")
            ads_author_reason = self.create_ads_author_reason(dwd_df)
            self.save_to_ads(ads_author_reason, "ads_author_reason")
            
            logger.info("\n[4/8] 处理 ads_author_attenuation_effect - 分析热度衰减效应...")
            ads_author_attenuation_effect = self.create_ads_author_attenuation_effect(dwd_df)
            self.save_to_ads(ads_author_attenuation_effect, "ads_author_attenuation_effect")
            
            # ============================================
            # 5. 用户侧 - 2个表
            # ============================================
            logger.info("\n" + "=" * 60)
            logger.info("【用户侧】创建ADS表")
            logger.info("=" * 60)
            
            logger.info("\n[5/8] 处理 ads_user_layered_recommendation - 分层推荐...")
            ads_user_layered_recommendation = self.create_ads_user_layered_recommendation(dwd_df)
            self.save_to_ads(ads_user_layered_recommendation, "ads_user_layered_recommendation")
            
            logger.info("\n[6/8] 处理 ads_user_avoid_pitfalls - 避坑指南...")
            ads_user_avoid_pitfalls = self.create_ads_user_avoid_pitfalls(dwd_df)
            self.save_to_ads(ads_user_avoid_pitfalls, "ads_user_avoid_pitfalls")
            
            # ============================================
            # 6. 资本侧 - 2个表
            # ============================================
            logger.info("\n" + "=" * 60)
            logger.info("【资本侧】创建ADS表")
            logger.info("=" * 60)
            
            logger.info("\n[7/8] 处理 ads_capital_ltv - IP长尾价值计算...")
            ads_capital_ltv = self.create_ads_capital_ltv(dwd_df)
            self.save_to_ads(ads_capital_ltv, "ads_capital_ltv")
            
            logger.info("\n[8/8] 处理 ads_capital_future_purchasing_power - 粉丝粘性与购买力验证...")
            ads_capital_future_purchasing_power = self.create_ads_capital_future_purchasing_power(dwd_df)
            self.save_to_ads(ads_capital_future_purchasing_power, "ads_capital_future_purchasing_power")
            
            # 释放缓存
            dwd_df.unpersist()
            
            logger.info("\n" + "=" * 60)
            logger.info("DWD -> ADS 特征工程处理完成！")
            logger.info("=" * 60)
            logger.info("已创建8个ADS层特征表：")
            logger.info("【平台侧】")
            logger.info("  1. ads_platform_heat - 热度分析")
            logger.info("  2. ads_platform_ranking_trend - 榜单趋势分析")
            logger.info("【作者侧】")
            logger.info("  3. ads_author_reason - 分析热度原因")
            logger.info("  4. ads_author_attenuation_effect - 分析热度衰减效应")
            logger.info("【用户侧】")
            logger.info("  5. ads_user_layered_recommendation - 分层推荐")
            logger.info("  6. ads_user_avoid_pitfalls - 避坑指南")
            logger.info("【资本侧】")
            logger.info("  7. ads_capital_ltv - IP长尾价值计算(LTV)")
            logger.info("  8. ads_capital_future_purchasing_power - 粉丝粘性与购买力验证")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"特征工程处理流程失败: {str(e)}")
            raise
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("Spark会话已关闭")


if __name__ == "__main__":
    # 创建处理器实例并执行
    processor = DWDToADSProcessor()
    processor.run()
