"""
*@BelongsProject: Python
*@BelongsPackage: 
*@Author: 程序员Eighteen
*@CreateTime: 2025-12-16  11:09
*@Description: 通过Spark将MySQL数据导入Hive
*@Version: 1.0
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MySQLToHiveLoader:
    """MySQL数据导入Hive的ODS层"""
    
    def __init__(self, mysql_config, hive_database="hive_QimaoScraper"):
        """
        初始化配置
        :param mysql_config: MySQL连接配置字典
        :param hive_database: Hive数据库名称
        """
        self.mysql_config = mysql_config
        self.hive_database = hive_database
        self.spark = None
        
    def init_spark(self):
        """初始化Spark会话"""
        try:
            self.spark = SparkSession.builder \
                .appName("MySQL_to_Hive_ODS") \
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
                .config("hive.metastore.uris", "thrift://localhost:9083") \
                .enableHiveSupport() \
                .getOrCreate()
            
            logger.info("Spark会话初始化成功")
            return self.spark
        except Exception as e:
            logger.error(f"Spark会话初始化失败: {str(e)}")
            raise
    
    def create_hive_database(self):
        """创建Hive数据库"""
        try:
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.hive_database}")
            self.spark.sql(f"USE {self.hive_database}")
            logger.info(f"Hive数据库 {self.hive_database} 创建/使用成功")
        except Exception as e:
            logger.error(f"创建Hive数据库失败: {str(e)}")
            raise
    
    def load_from_mysql(self, table_name="Seven_Cats_Novel_Data"):
        """
        从MySQL读取数据
        :param table_name: MySQL表名
        :return: DataFrame
        """
        try:
            jdbc_url = f"jdbc:mysql://{self.mysql_config['host']}:{self.mysql_config['port']}/{self.mysql_config['database']}"
            
            df = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", table_name) \
                .option("user", self.mysql_config['user']) \
                .option("password", self.mysql_config['password']) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .load()
            
            logger.info(f"从MySQL读取表 {table_name} 成功，共 {df.count()} 条数据")
            return df
        except Exception as e:
            logger.error(f"从MySQL读取数据失败: {str(e)}")
            raise
    
    def save_to_hive_ods(self, df, ods_table_name="ods_novel_data"):
        """
        保存数据到Hive ODS层
        :param df: DataFrame
        :param ods_table_name: ODS层表名
        """
        try:
            # 删除已存在的表（可选，根据需求调整）
            self.spark.sql(f"DROP TABLE IF EXISTS {self.hive_database}.{ods_table_name}")
            
            # 保存为Hive表
            df.write \
                .mode("overwrite") \
                .format("hive") \
                .saveAsTable(f"{self.hive_database}.{ods_table_name}")
            
            logger.info(f"数据成功保存到Hive表 {self.hive_database}.{ods_table_name}")
            
            # 验证数据
            count = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {self.hive_database}.{ods_table_name}").collect()[0]['cnt']
            logger.info(f"验证: Hive ODS层表 {ods_table_name} 共有 {count} 条数据")
            
        except Exception as e:
            logger.error(f"保存数据到Hive失败: {str(e)}")
            raise
    
    def run(self):
        """执行完整的数据导入流程"""
        try:
            logger.info("=" * 50)
            logger.info("开始执行 MySQL -> Hive ODS 数据导入")
            logger.info("=" * 50)
            
            # 1. 初始化Spark
            self.init_spark()
            
            # 2. 创建Hive数据库
            self.create_hive_database()
            
            # 3. 从MySQL读取数据
            df = self.load_from_mysql()
            
            # 4. 保存到Hive ODS层
            self.save_to_hive_ods(df)
            
            logger.info("=" * 50)
            logger.info("MySQL -> Hive ODS 数据导入完成")
            logger.info("=" * 50)
            
        except Exception as e:
            logger.error(f"数据导入流程失败: {str(e)}")
            raise
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("Spark会话已关闭")


if __name__ == "__main__":
    # MySQL配置（请根据实际情况修改）
    mysql_config = {
        "host": "localhost",
        "port": 3306,
        "database": "QimaoScraper",
        "user": "root",
        "password": "qwer4321"  # 请修改为实际密码
    }
    
    # 创建加载器实例并执行
    loader = MySQLToHiveLoader(mysql_config)
    loader.run()
