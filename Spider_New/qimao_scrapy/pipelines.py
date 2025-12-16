# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import json
import os
from datetime import datetime
from qimao_scrapy.db_utils import get_db_session, MaleOrientedNovels, WomensFiction, create_tables


class DatabasePipeline:
    """数据库存储管道 - 男频/女频分表存储"""
    
    def open_spider(self, spider):
        """爬虫开始时初始化"""
        try:
            create_tables()
            spider.logger.info("✅ 数据库连接成功，表初始化完成")
        except Exception as e:
            spider.logger.error(f"❌ 数据库连接失败: {e}")
            raise e
        
        self.success_count = 0
        self.fail_count = 0
        self.male_count = 0
        self.female_count = 0
    
    def close_spider(self, spider):
        """爬虫结束时输出统计"""
        spider.logger.info("\n" + "="*60)
        spider.logger.info("数据库存储统计：")
        spider.logger.info(f"  男频小说: {self.male_count} 条")
        spider.logger.info(f"  女频小说: {self.female_count} 条")
        spider.logger.info(f"  成功: {self.success_count} 条 | 失败: {self.fail_count} 条")
        spider.logger.info("="*60 + "\n")
    
    def process_item(self, item, spider):
        """处理每个Item - 实时存入数据库"""
        item_dict = dict(item)
        try:
            gender = item_dict.get('gender', '')
            
            # 根据性别选择表
            if gender == '男生':
                model_class = MaleOrientedNovels
                self.male_count += 1
            elif gender == '女生':
                model_class = WomensFiction
                self.female_count += 1
            else:
                spider.logger.warning(f"⚠️ 未知性别: {gender}, 跳过存储")
                return item
            
            # 存入数据库
            with get_db_session() as session:
                # 检查是否已存在（book_id + rank_name + date_month组合唯一）
                existing = session.query(model_class).filter_by(
                    book_id=item_dict['book_id'],
                    rank_name=item_dict['rank_name'],
                    date_month=item_dict['date_month']
                ).first()
                
                if existing:
                    # 更新现有记录
                    for key, value in item_dict.items():
                        if key not in ['gender']:  # gender不需要更新
                            setattr(existing, key, value)
                    spider.logger.debug(f"🔄 更新: 《{item_dict['title']}》")
                else:
                    # 创建新记录
                    new_record = model_class(
                        book_id=item_dict['book_id'],
                        title=item_dict['title'],
                        author=item_dict['author'],
                        category1_name=item_dict['category1_name'],
                        category2_name=item_dict['category2_name'],
                        words_num=item_dict['words_num'],
                        intro=item_dict['intro'],
                        image_link=item_dict['image_link'],
                        status=item_dict['status'],
                        number=item_dict['number'],
                        unit=item_dict['unit'],
                        rank_name=item_dict['rank_name'],
                        date_type=item_dict['date_type'],
                        date_month=item_dict['date_month'],
                        score=item_dict['score'],
                        read_count=item_dict['read_count'],
                        popularity=item_dict['popularity'],
                        error=str(item_dict['error']).lower(),
                        error_msg=item_dict.get('error_msg', '')
                    )
                    session.add(new_record)
                    spider.logger.debug(f"➕ 新增: 《{item_dict['title']}》")
                
                session.commit()
                self.success_count += 1
                
        except Exception as e:
            self.fail_count += 1
            spider.logger.error(f"❌ 数据库存储失败: {e}")
            spider.logger.error(f"   书籍: 《{item_dict.get('title', 'Unknown')}》")
        
        return item


class QimaoScrapyPipeline:
    """数据处理管道"""
    
    def open_spider(self, spider):
        """爬虫开始时初始化"""
        self.file = open('qimao_books.json', 'w', encoding='utf-8')
        self.items = []
        self.success_count = 0
        self.fail_count = 0
        spider.logger.info("\n" + "="*60)
        spider.logger.info("开始爬取七猫小说榜单数据...")
        spider.logger.info("="*60 + "\n")
    
    def close_spider(self, spider):
        """爬虫结束时保存数据"""
        # 写入JSON文件
        json.dump(self.items, self.file, ensure_ascii=False, indent=2)
        self.file.close()
        
        # 输出统计信息
        spider.logger.info("\n" + "="*60)
        spider.logger.info(f"全部完成！共处理 {len(self.items)} 本书")
        spider.logger.info(f"成功: {self.success_count} 本 | 失败: {self.fail_count} 本")
        spider.logger.info(f"数据已保存到: qimao_books.json")
        spider.logger.info("="*60 + "\n")
    
    def process_item(self, item, spider):
        """处理每个Item"""
        # 转换为dict
        item_dict = dict(item)
        self.items.append(item_dict)
        
        # 统计
        if item_dict.get('error'):
            self.fail_count += 1
        else:
            self.success_count += 1
        
        # 打印数据
        status = '完结' if item_dict.get('status') == '1' else '连载中'
        gender_rank = f"{item_dict.get('gender', '')}-{item_dict.get('rank_name', '')}榜"
        date_info = f"({item_dict.get('date_type', '')})" if item_dict.get('date_type') else ""
        month_info = f" [{item_dict.get('date_month', '')}]" if item_dict.get('date_month') else ""
        
        if item_dict.get('error'):
            spider.logger.info(
                f"[{len(self.items)}] ID:{item_dict['book_id']} | 《{item_dict['title']}》 | "
                f"{item_dict['author']} | {item_dict['category1_name']}-{item_dict['category2_name']} | "
                f"{item_dict['words_num']} | {status} | {item_dict['number']}{item_dict.get('unit', '')} | "
                f"{gender_rank}{date_info}{month_info} | ❗️失败({item_dict.get('error_msg', '')})"
            )
        else:
            spider.logger.info(
                f"[{len(self.items)}] ID:{item_dict['book_id']} | 《{item_dict['title']}》 | "
                f"{item_dict['author']} | {item_dict['category1_name']}-{item_dict['category2_name']} | "
                f"{item_dict['words_num']} | {status} | {item_dict['number']}{item_dict.get('unit', '')} | "
                f"{gender_rank}{date_info}{month_info} | ⭐{item_dict['score']}分 | "
                f"📖{item_dict['read_count']}万 | 🔥{item_dict['popularity']}万"
            )
        
        return item


class JsonWriterPipeline:
    """简单JSON写入管道"""
    
    def open_spider(self, spider):
        self.file = open('items.jsonl', 'w', encoding='utf-8')
    
    def close_spider(self, spider):
        self.file.close()
    
    def process_item(self, item, spider):
        line = json.dumps(dict(item), ensure_ascii=False) + "\n"
        self.file.write(line)
        return item
