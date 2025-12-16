"""
*@BelongsProject: Python
*@BelongsPackage: 
*@Author: 程序员Eighteen
*@CreateTime: 2025-12-16
*@Description: 数据仓库主执行脚本 - 一键运行完整ETL流程
*@Version: 1.0
"""

import sys
import logging
import argparse
from datetime import datetime

# 导入各个处理模块
try:
    from Spark_MySQL_to_Hive import MySQLToHiveLoader
    from Spark_Data_processing import ODSToDWDProcessor
    from Spark_dwd_to_ads import DWDToADSProcessor
    from Sqoop_hive_to_MySQL import SparkHiveToMySQL
    from config import MYSQL_CONFIG, HIVE_CONFIG, ENVIRONMENT
except ImportError as e:
    print(f"导入模块失败: {e}")
    print("请确保所有依赖模块都在同一目录下")
    sys.exit(1)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataWarehouseETL:
    """数据仓库ETL主控制器"""
    
    def __init__(self, mysql_config=None):
        """
        初始化ETL控制器
        :param mysql_config: MySQL配置，如果为None则使用config.py中的配置
        """
        self.mysql_config = mysql_config or MYSQL_CONFIG
        self.start_time = None
        self.end_time = None
        
    def run_step_1_ods(self):
        """步骤1: MySQL -> Hive ODS层数据导入"""
        try:
            logger.info("=" * 70)
            logger.info("步骤1: 开始执行 MySQL -> Hive ODS 层数据导入")
            logger.info("=" * 70)
            
            loader = MySQLToHiveLoader(self.mysql_config)
            loader.run()
            
            logger.info("✓ 步骤1完成: ODS层数据导入成功\n")
            return True
            
        except Exception as e:
            logger.error(f"✗ 步骤1失败: {str(e)}")
            return False
    
    def run_step_2_dwd(self):
        """步骤2: ODS -> DWD层数据清洗"""
        try:
            logger.info("=" * 70)
            logger.info("步骤2: 开始执行 ODS -> DWD 层数据清洗")
            logger.info("=" * 70)
            
            processor = ODSToDWDProcessor()
            processor.run()
            
            logger.info("✓ 步骤2完成: DWD层数据清洗成功\n")
            return True

        except Exception as e:
            logger.error(f"✗ 步骤2失败: {str(e)}")
            return False
    
    def run_step_3_ads(self):
        """步骤3: DWD -> ADS层特征工程"""
        try:
            logger.info("=" * 70)
            logger.info("步骤3: 开始执行 DWD -> ADS 层特征工程")
            logger.info("=" * 70)
            
            processor = DWDToADSProcessor()
            processor.run()
            
            logger.info("✓ 步骤3完成: ADS层特征工程成功\n")
            return True
            
        except Exception as e:
            logger.error(f"✗ 步骤3失败: {str(e)}")
            return False
    
    def run_step_4_export(self):
        """步骤4: Hive ADS -> MySQL导出"""
        try:
            logger.info("=" * 70)
            logger.info("步骤4: 开始执行 Hive ADS -> MySQL 数据导出")
            logger.info("=" * 70)
            
            exporter = SparkHiveToMySQL(self.mysql_config)
            exporter.run()
            
            logger.info("✓ 步骤4完成: 数据导出到MySQL成功\n")
            return True
            
        except Exception as e:
            logger.error(f"✗ 步骤4失败: {str(e)}")
            return False
    
    def run_all(self):
        """运行完整的ETL流程"""
        self.start_time = datetime.now()
        
        logger.info("\n" + "=" * 70)
        logger.info("七猫小说数据仓库 - 完整ETL流程启动")
        logger.info(f"开始时间: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 70 + "\n")
        
        # 执行各个步骤
        steps = [
            ("ODS层导入", self.run_step_1_ods),
            ("DWD层清洗", self.run_step_2_dwd),
            ("ADS层特征工程", self.run_step_3_ads),
            ("MySQL导出", self.run_step_4_export)
        ]
        
        success_count = 0
        failed_steps = []
        
        for step_name, step_func in steps:
            logger.info(f"\n{'*' * 70}")
            logger.info(f"执行步骤: {step_name}")
            logger.info('*' * 70)
            
            if step_func():
                success_count += 1
            else:
                failed_steps.append(step_name)
                logger.warning(f"步骤 [{step_name}] 执行失败，继续执行后续步骤...")
        
        # 总结
        self.end_time = datetime.now()
        duration = (self.end_time - self.start_time).total_seconds()
        
        logger.info("\n" + "=" * 70)
        logger.info("ETL流程执行完毕")
        logger.info("=" * 70)
        logger.info(f"开始时间: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"结束时间: {self.end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"总耗时: {duration:.2f} 秒 ({duration/60:.2f} 分钟)")
        logger.info(f"成功步骤: {success_count}/{len(steps)}")
        
        if failed_steps:
            logger.warning(f"失败步骤: {', '.join(failed_steps)}")
            logger.info("=" * 70 + "\n")
            return False
        else:
            logger.info("✓ 所有步骤执行成功！")
            logger.info("=" * 70 + "\n")
            return True
    
    def run_single_step(self, step):
        """
        运行单个步骤
        :param step: 步骤编号或名称 (1-4 或 ods/dwd/ads/export)
        """
        step_map = {
            "1": ("ODS层导入", self.run_step_1_ods),
            "2": ("DWD层清洗", self.run_step_2_dwd),
            "3": ("ADS层特征工程", self.run_step_3_ads),
            "4": ("MySQL导出", self.run_step_4_export),
            "ods": ("ODS层导入", self.run_step_1_ods),
            "dwd": ("DWD层清洗", self.run_step_2_dwd),
            "ads": ("ADS层特征工程", self.run_step_3_ads),
            "export": ("MySQL导出", self.run_step_4_export)
        }

        step_key = str(step).lower()
        if step_key not in step_map:
            logger.error(f"无效的步骤: {step}")
            logger.info("有效的步骤: 1/ods, 2/dwd, 3/ads, 4/export")
            return False
        
        step_name, step_func = step_map[step_key]
        
        self.start_time = datetime.now()
        logger.info(f"\n执行单个步骤: {step_name}")
        
        success = step_func()
        
        self.end_time = datetime.now()
        duration = (self.end_time - self.start_time).total_seconds()
        
        if success:
            logger.info(f"✓ 步骤 [{step_name}] 执行成功，耗时: {duration:.2f} 秒")
        else:
            logger.error(f"✗ 步骤 [{step_name}] 执行失败")
        
        return success


def main():
    """主函数 - 解析命令行参数并执行"""
    parser = argparse.ArgumentParser(
        description='七猫小说数据仓库ETL主执行脚本',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 运行完整ETL流程
  python main.py --all
  
  # 运行单个步骤
  python main.py --step 1     # ODS层导入
  python main.py --step 2     # DWD层清洗
  python main.py --step 3     # ADS层特征工程
  python main.py --step 4     # MySQL导出
  
  # 或使用步骤名称
  python main.py --step ods
  python main.py --step dwd
  python main.py --step ads
  python main.py --step export
  
  # 指定MySQL密码
  python main.py --all --password qwer4321
        """
    )
    
    parser.add_argument(
        '--all',
        action='store_true',
        help='运行完整的ETL流程（所有步骤）'
    )
    
    parser.add_argument(
        '--step',
        type=str,
        help='运行单个步骤: 1/ods, 2/dwd, 3/ads, 4/export'
    )
    
    parser.add_argument(
        '--password',
        type=str,
        help='MySQL密码（覆盖config.py中的配置）'
    )
    
    parser.add_argument(
        '--host',
        type=str,
        help='MySQL主机地址（覆盖config.py中的配置）'
    )
    
    args = parser.parse_args()
    
    # 如果没有指定任何参数，显示帮助信息
    if not args.all and not args.step:
        parser.print_help()
        return
    
    # 准备MySQL配置
    mysql_config = MYSQL_CONFIG.copy()
    if args.password:
        mysql_config['password'] = args.password
    if args.host:
        mysql_config['host'] = args.host
    
    # 创建ETL控制器
    etl = DataWarehouseETL(mysql_config)
    
    # 执行任务
    if args.all:
        success = etl.run_all()
    elif args.step:
        success = etl.run_single_step(args.step)
    else:
        logger.error("请指定 --all 或 --step 参数")
        return
    
    # 返回执行状态
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\n用户中断执行")
        sys.exit(130)
    except Exception as e:
        logger.error(f"执行过程中发生错误: {str(e)}", exc_info=True)
        sys.exit(1)
