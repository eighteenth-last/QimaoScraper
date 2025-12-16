"""
*@BelongsProject: Python
*@BelongsPackage: 
*@Author: 程序员Eighteen
*@CreateTime: 2025-12-16  11:09
*@Description: 使用spark SQL 对数据进行处理 - ODS层到DWD层的数据清理
*@Version: 1.0
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, regexp_replace, avg, current_date
)
from pyspark.sql.types import *
import logging
from functools import reduce

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ODSToDWDProcessor:
    """ODS层到DWD层的数据清理处理"""
    
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
                .appName("ODS_to_DWD_Processing") \
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
                .config("hive.metastore.uris", "thrift://localhost:9083") \
                .enableHiveSupport() \
                .getOrCreate()

            logger.info("Spark会话初始化成功")
            return self.spark
        except Exception as e:
            logger.error(f"Spark会话初始化失败: {str(e)}")
            raise
    
    def load_ods_data(self, ods_table="ods_novel_data"):
        """
        从ODS层加载数据
        :param ods_table: ODS层表名
        :return: DataFrame
        """
        try:
            df = self.spark.table(f"{self.hive_database}.{ods_table}")
            logger.info(f"从ODS层加载数据成功，共 {df.count()} 条数据")
            return df
        except Exception as e:
            logger.error(f"加载ODS数据失败: {str(e)}")
            raise
    
    def convert_to_numeric(self, value_col):
        """
        将带有"万"、"亿"单位的字符串转换为数值
        :param value_col: 列名
        :return: 转换后的数值列
        """
        return when(col(value_col).contains("亿"), 
                   regexp_replace(col(value_col), "亿", "").cast("double") * 100000000) \
              .when(col(value_col).contains("万"), 
                   regexp_replace(col(value_col), "万", "").cast("double") * 10000) \
              .otherwise(col(value_col).cast("double"))
    
    def clean_data(self, df):
        """
        数据清理处理
        :param df: 原始DataFrame
        :return: 清理后的DataFrame
        """
        try:
            logger.info("开始数据清理处理...")
            
            # 1. 删除完全空的列（如果整列都是NULL）
            # 先检查每列的非空数量
            total_count = df.count()
            columns_to_keep = []
            
            for col_name in df.columns:
                non_null_count = df.filter(col(col_name).isNotNull()).count()
                if non_null_count > 0:  # 保留有数据的列
                    columns_to_keep.append(col_name)
                else:
                    logger.info(f"删除空列: {col_name}")
            
            df = df.select(columns_to_keep)
            
            # 2. 删除完全空的行（所有重要字段都为空的行）
            # 定义关键字段
            key_fields = ['book_id', 'title', 'author']
            df = df.filter(
                reduce(lambda x, y: x | y, [col(c).isNotNull() for c in key_fields])
            )
            logger.info(f"删除空行后，剩余 {df.count()} 条数据")
            
            # 3. 数值化处理：popularity 和 read_count
            df = df.withColumn("numeric_popularity", self.convert_to_numeric("popularity"))
            df = df.withColumn("numeric_read_count", self.convert_to_numeric("read_count"))
            logger.info("已添加数值化字段: numeric_popularity, numeric_read_count")
            
            # 4. 数值化 score 字段
            df = df.withColumn("numeric_score", 
                             when(col("score").isNotNull(), col("score").cast("double"))
                             .otherwise(lit(None)))
            logger.info("已添加数值化字段: numeric_score")
            
            # 5. 数值化 words_num 字段（去除"万字"等单位）
            df = df.withColumn("numeric_words", 
                             when(col("words_num").contains("万字"), 
                                 regexp_replace(col("words_num"), "万字", "").cast("double") * 10000)
                             .when(col("words_num").contains("字"), 
                                 regexp_replace(col("words_num"), "字", "").cast("double"))
                             .otherwise(col("words_num").cast("double")))
            logger.info("已添加数值化字段: numeric_words")
            
            # 6. 处理缺失值 - 用平均值填充数值型字段
            # 计算平均值
            avg_values = df.agg(
                avg("numeric_popularity").alias("avg_pop"),
                avg("numeric_read_count").alias("avg_read"),
                avg("numeric_score").alias("avg_score"),
                avg("numeric_words").alias("avg_words")
            ).collect()[0]
            
            # 填充缺失值
            df = df.fillna({
                "numeric_popularity": avg_values["avg_pop"] if avg_values["avg_pop"] else 0,
                "numeric_read_count": avg_values["avg_read"] if avg_values["avg_read"] else 0,
                "numeric_score": avg_values["avg_score"] if avg_values["avg_score"] else 0,
                "numeric_words": avg_values["avg_words"] if avg_values["avg_words"] else 0
            })
            logger.info("已用平均值填充数值字段的缺失值")
            
            # 7. 处理异常值 - 使用中位数填充极端异常值
            # 计算四分位数和IQR（四分位距）
            quantiles = df.approxQuantile(
                ["numeric_popularity", "numeric_read_count", "numeric_score"], 
                [0.25, 0.5, 0.75], 
                0.01
            )
            
            # 处理 popularity 异常值
            if len(quantiles[0]) >= 3:
                q1_pop, median_pop, q3_pop = quantiles[0]
                iqr_pop = q3_pop - q1_pop
                lower_pop = q1_pop - 3 * iqr_pop
                upper_pop = q3_pop + 3 * iqr_pop
                
                df = df.withColumn("numeric_popularity",
                                 when((col("numeric_popularity") < lower_pop) | 
                                      (col("numeric_popularity") > upper_pop), 
                                      lit(median_pop))
                                 .otherwise(col("numeric_popularity")))
                logger.info(f"已处理 popularity 异常值，阈值: [{lower_pop}, {upper_pop}]")
            
            # 8. 字符串字段缺失值填充
            df = df.fillna({
                "category1_name": "未知",
                "category2_name": "未知",
                "status": "未知",
                "rank_name": "未知",
                "gender_type": "unknown"
            })
            logger.info("已填充字符串字段的缺失值")
            
            # 9. 添加数据质量标记
            df = df.withColumn("data_quality_score",
                             when(col("numeric_popularity").isNotNull() & 
                                  col("numeric_read_count").isNotNull() &
                                  col("numeric_score").isNotNull(), lit(1.0))
                             .otherwise(lit(0.5)))
            
            logger.info(f"数据清理完成，最终数据量: {df.count()} 条")
            return df
            
        except Exception as e:
            logger.error(f"数据清理失败: {str(e)}")
            raise
    
    def save_to_dwd(self, df, dwd_table="dwd_novel_data"):
        """
        保存数据到DWD层
        :param df: 清理后的DataFrame
        :param dwd_table: DWD层表名
        """
        try:
            # 删除已存在的表
            self.spark.sql(f"DROP TABLE IF EXISTS {self.hive_database}.{dwd_table}")
            
            # 保存为Hive表
            df.write \
                .mode("overwrite") \
                .format("hive") \
                .saveAsTable(f"{self.hive_database}.{dwd_table}")
            
            logger.info(f"数据成功保存到DWD层表 {self.hive_database}.{dwd_table}")
            
            # 验证数据
            count = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {self.hive_database}.{dwd_table}").collect()[0]['cnt']
            logger.info(f"验证: DWD层表 {dwd_table} 共有 {count} 条数据")
            
            # 显示数据样例
            logger.info("数据样例:")
            self.spark.sql(f"SELECT * FROM {self.hive_database}.{dwd_table} LIMIT 5").show()
            
        except Exception as e:
            logger.error(f"保存数据到DWD层失败: {str(e)}")
            raise
    
    def run(self):
        """执行完整的ODS到DWD数据处理流程"""
        try:
            logger.info("=" * 50)
            logger.info("开始执行 ODS -> DWD 数据清理处理")
            logger.info("=" * 50)
            
            # 1. 初始化Spark
            self.init_spark()
            
            # 2. 从ODS层加载数据
            ods_df = self.load_ods_data()
            
            # 3. 数据清理
            dwd_df = self.clean_data(ods_df)
            
            # 4. 保存到DWD层
            self.save_to_dwd(dwd_df)
            
            logger.info("=" * 50)
            logger.info("ODS -> DWD 数据清理处理完成")
            logger.info("=" * 50)
            
        except Exception as e:
            logger.error(f"数据处理流程失败: {str(e)}")
            raise
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("Spark会话已关闭")


if __name__ == "__main__":
    # 创建处理器实例并执行
    processor = ODSToDWDProcessor()
    processor.run()
