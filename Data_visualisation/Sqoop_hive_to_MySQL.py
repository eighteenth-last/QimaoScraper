"""
*@BelongsProject: Python
*@BelongsPackage: 
*@Author: 程序员Eighteen
*@CreateTime: 2025-12-16  11:12
*@Description: 使用Sqoop将Hive的ADS层数据导出到MySQL数据库
*@Version: 1.0
"""

import subprocess
import logging
import pymysql
from typing import List, Dict

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class HiveToMySQLExporter:
    """使用Sqoop将Hive ADS层数据导出到MySQL"""
    
    def __init__(self, mysql_config, hive_database="hive_QimaoScraper", 
                 target_mysql_database="QimaoScraper_Feature_Data"):
        """
        初始化配置
        :param mysql_config: MySQL连接配置字典
        :param hive_database: Hive数据库名称
        :param target_mysql_database: 目标MySQL数据库名称
        """
        self.mysql_config = mysql_config
        self.hive_database = hive_database
        self.target_mysql_database = target_mysql_database
        self.ads_tables = [
            "ads_platform_heat",
            "ads_platform_ranking_trend",
            "ads_author_reason",
            "ads_author_attenuation_effect",
            "ads_user_layered_recommendation",
            "ads_user_avoid_pitfalls",
            "ads_capital_ltv",
            "ads_capital_future_purchasing_power"
        ]
    
    def create_mysql_database(self):
        """创建MySQL目标数据库"""
        try:
            # 连接到MySQL（不指定数据库）
            conn = pymysql.connect(
                host=self.mysql_config['host'],
                port=self.mysql_config['port'],
                user=self.mysql_config['user'],
                password=self.mysql_config['password'],
                charset='utf8mb4'
            )
            cursor = conn.cursor()
            
            # 创建数据库
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.target_mysql_database} "
                          f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            logger.info(f"MySQL数据库 {self.target_mysql_database} 创建成功")
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"创建MySQL数据库失败: {str(e)}")
            raise
    
    def export_table_with_sqoop(self, hive_table, mysql_table):
        """
        使用Sqoop导出单个表
        :param hive_table: Hive表名
        :param mysql_table: MySQL表名
        """
        try:
            logger.info(f"开始导出表: {hive_table} -> {mysql_table}")
            
            # 构建Sqoop命令
            jdbc_url = f"jdbc:mysql://{self.mysql_config['host']}:{self.mysql_config['port']}/{self.target_mysql_database}"
            
            sqoop_cmd = [
                "sqoop", "export",
                "--connect", jdbc_url,
                "--username", self.mysql_config['user'],
                "--password", self.mysql_config['password'],
                "--table", mysql_table,
                "--export-dir", f"/user/hive/warehouse/{self.hive_database}.db/{hive_table}",
                "--input-fields-terminated-by", "\\001",  # Hive默认分隔符
                "--input-null-string", "\\\\N",
                "--input-null-non-string", "\\\\N",
                "--num-mappers", "4"
            ]
            
            # 执行Sqoop命令
            logger.info(f"执行Sqoop命令: {' '.join(sqoop_cmd)}")
            result = subprocess.run(sqoop_cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.info(f"表 {hive_table} 导出成功")
            else:
                logger.error(f"表 {hive_table} 导出失败")
                logger.error(f"错误信息: {result.stderr}")
                raise Exception(f"Sqoop导出失败: {result.stderr}")
            
        except FileNotFoundError:
            logger.warning("Sqoop命令未找到，将使用Spark进行数据导出")
            self.export_table_with_spark(hive_table, mysql_table)
        except Exception as e:
            logger.error(f"导出表 {hive_table} 失败: {str(e)}")
            raise
    
    def export_table_with_spark(self, hive_table, mysql_table):
        """
        使用Spark作为替代方案导出表（当Sqoop不可用时）
        :param hive_table: Hive表名
        :param mysql_table: MySQL表名
        """
        try:
            from pyspark.sql import SparkSession
            
            logger.info(f"使用Spark导出表: {hive_table} -> {mysql_table}")
            
            # 初始化Spark
            spark = SparkSession.builder \
                .appName(f"Export_{hive_table}") \
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
                .config("hive.metastore.uris", "thrift://localhost:9083") \
                .enableHiveSupport() \
                .getOrCreate()
            
            # 读取Hive表
            df = spark.table(f"{self.hive_database}.{hive_table}")
            logger.info(f"从Hive读取表 {hive_table}，共 {df.count()} 条数据")
            
            # 构建JDBC URL
            jdbc_url = f"jdbc:mysql://{self.mysql_config['host']}:{self.mysql_config['port']}/{self.target_mysql_database}"
            
            # 写入MySQL
            df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", mysql_table) \
                .option("user", self.mysql_config['user']) \
                .option("password", self.mysql_config['password']) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .mode("overwrite") \
                .save()
            
            logger.info(f"表 {hive_table} 通过Spark导出成功")
            
            spark.stop()
            
        except Exception as e:
            logger.error(f"使用Spark导出表 {hive_table} 失败: {str(e)}")
            raise
    
    def verify_export(self, mysql_table):
        """
        验证MySQL表的数据
        :param mysql_table: MySQL表名
        """
        try:
            conn = pymysql.connect(
                host=self.mysql_config['host'],
                port=self.mysql_config['port'],
                user=self.mysql_config['user'],
                password=self.mysql_config['password'],
                database=self.target_mysql_database,
                charset='utf8mb4'
            )
            cursor = conn.cursor()
            
            # 查询表的记录数
            cursor.execute(f"SELECT COUNT(*) FROM {mysql_table}")
            count = cursor.fetchone()[0]
            logger.info(f"验证: MySQL表 {mysql_table} 共有 {count} 条数据")
            
            # 显示表结构
            cursor.execute(f"DESCRIBE {mysql_table}")
            columns = cursor.fetchall()
            logger.info(f"表 {mysql_table} 的结构:")
            for col in columns[:5]:  # 只显示前5列
                logger.info(f"  {col[0]} - {col[1]}")
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.warning(f"验证表 {mysql_table} 失败: {str(e)}")
    
    def run(self):
        """执行完整的Hive到MySQL导出流程"""
        try:
            logger.info("=" * 60)
            logger.info("开始执行 Hive ADS -> MySQL 数据导出")
            logger.info("=" * 60)
            
            # 1. 创建MySQL目标数据库
            self.create_mysql_database()
            
            # 2. 遍历导出所有ADS表
            for i, hive_table in enumerate(self.ads_tables, 1):
                logger.info(f"\n[{i}/{len(self.ads_tables)}] 处理表: {hive_table}")
                mysql_table = hive_table  # MySQL表名与Hive表名相同
                
                try:
                    # 尝试使用Sqoop导出
                    self.export_table_with_sqoop(hive_table, mysql_table)
                except:
                    # 如果Sqoop失败，使用Spark导出
                    logger.info("Sqoop导出失败，切换到Spark导出方式")
                    self.export_table_with_spark(hive_table, mysql_table)
                
                # 验证导出结果
                self.verify_export(mysql_table)
            
            logger.info("=" * 60)
            logger.info("Hive ADS -> MySQL 数据导出完成！")
            logger.info(f"已导出 {len(self.ads_tables)} 个表到MySQL数据库: {self.target_mysql_database}")
            logger.info("导出的表:")
            for table in self.ads_tables:
                logger.info(f"  - {table}")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"数据导出流程失败: {str(e)}")
            raise


class SparkHiveToMySQL:
    """纯Spark实现的Hive到MySQL数据导出（推荐使用）"""
    
    def __init__(self, mysql_config, hive_database="hive_QimaoScraper",
                 target_mysql_database="QimaoScraper_Feature_Data"):
        """
        初始化配置
        :param mysql_config: MySQL连接配置字典
        :param hive_database: Hive数据库名称
        :param target_mysql_database: 目标MySQL数据库名称
        """
        self.mysql_config = mysql_config
        self.hive_database = hive_database
        self.target_mysql_database = target_mysql_database
        self.ads_tables = [
            "ads_platform_heat",
            "ads_platform_ranking_trend",
            "ads_author_reason",
            "ads_author_attenuation_effect",
            "ads_user_layered_recommendation",
            "ads_user_avoid_pitfalls",
            "ads_capital_ltv",
            "ads_capital_future_purchasing_power"
        ]
        self.spark = None
    
    def init_spark(self):
        """初始化Spark会话"""
        try:
            from pyspark.sql import SparkSession
            
            self.spark = SparkSession.builder \
                .appName("Hive_to_MySQL_Export") \
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
                .config("hive.metastore.uris", "thrift://localhost:9083") \
                .enableHiveSupport() \
                .getOrCreate()
            
            logger.info("Spark会话初始化成功")
            return self.spark
        except Exception as e:
            logger.error(f"Spark会话初始化失败: {str(e)}")
            raise
    
    def create_mysql_database(self):
        """创建MySQL目标数据库"""
        try:
            import pymysql
            
            conn = pymysql.connect(
                host=self.mysql_config['host'],
                port=self.mysql_config['port'],
                user=self.mysql_config['user'],
                password=self.mysql_config['password'],
                charset='utf8mb4'
            )
            cursor = conn.cursor()
            
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.target_mysql_database} "
                          f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            logger.info(f"MySQL数据库 {self.target_mysql_database} 创建成功")
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"创建MySQL数据库失败: {str(e)}")
            raise
    
    def export_table(self, hive_table, mysql_table):
        """
        使用Spark导出单个表
        :param hive_table: Hive表名
        :param mysql_table: MySQL表名
        """
        try:
            logger.info(f"开始导出表: {hive_table} -> {mysql_table}")
            
            # 读取Hive表
            df = self.spark.table(f"{self.hive_database}.{hive_table}")
            record_count = df.count()
            logger.info(f"从Hive读取表 {hive_table}，共 {record_count} 条数据")
            
            # 构建JDBC URL
            jdbc_url = f"jdbc:mysql://{self.mysql_config['host']}:{self.mysql_config['port']}/{self.target_mysql_database}?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=UTC"
            
            # 写入MySQL
            df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", mysql_table) \
                .option("user", self.mysql_config['user']) \
                .option("password", self.mysql_config['password']) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .mode("overwrite") \
                .save()
            
            logger.info(f"表 {hive_table} 导出到MySQL成功")
            
        except Exception as e:
            logger.error(f"导出表 {hive_table} 失败: {str(e)}")
            raise
    
    def run(self):
        """执行完整的导出流程"""
        try:
            logger.info("=" * 60)
            logger.info("开始执行 Hive ADS -> MySQL 数据导出（Spark方式）")
            logger.info("=" * 60)
            
            # 1. 初始化Spark
            self.init_spark()
            
            # 2. 创建MySQL数据库
            self.create_mysql_database()
            
            # 3. 导出所有ADS表
            for i, hive_table in enumerate(self.ads_tables, 1):
                logger.info(f"\n[{i}/{len(self.ads_tables)}] 处理表: {hive_table}")
                mysql_table = hive_table
                self.export_table(hive_table, mysql_table)
            
            logger.info("=" * 60)
            logger.info("Hive ADS -> MySQL 数据导出完成！")
            logger.info(f"已导出 {len(self.ads_tables)} 个表到MySQL数据库: {self.target_mysql_database}")
            for table in self.ads_tables:
                logger.info(f"  ✓ {table}")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"数据导出流程失败: {str(e)}")
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
        "user": "root",
        "password": "qwer4321"  # 请修改为实际密码
    }

    # 方式一：使用纯Spark方式（推荐）
    logger.info("使用Spark方式导出数据...")
    exporter = SparkHiveToMySQL(mysql_config)
    exporter.run()
    
    # 方式二：使用Sqoop方式（如果环境支持Sqoop）
    # logger.info("使用Sqoop方式导出数据...")
    # exporter = HiveToMySQLExporter(mysql_config)
    # exporter.run()
