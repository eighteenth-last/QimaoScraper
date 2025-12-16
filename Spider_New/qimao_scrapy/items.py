# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class QimaoBookItem(scrapy.Item):
    """七猫小说书籍数据"""
    # 榜单数据
    book_id = scrapy.Field()
    title = scrapy.Field()
    author = scrapy.Field()
    category1_name = scrapy.Field()
    category2_name = scrapy.Field()
    words_num = scrapy.Field()
    intro = scrapy.Field()
    image_link = scrapy.Field()
    status = scrapy.Field()  # 0:连载中 1:完结
    number = scrapy.Field()
    unit = scrapy.Field()
    
    # 榜单信息
    gender = scrapy.Field()  # 男生/女生
    rank_name = scrapy.Field()  # 大热/新书/完结/收藏/更新
    date_type = scrapy.Field()  # 日榜/月榜
    date_month = scrapy.Field()  # 202310-202510
    
    # 详情数据
    score = scrapy.Field()  # 评分
    read_count = scrapy.Field()  # 阅读量
    popularity = scrapy.Field()  # 人气值
    
    # 错误信息
    error = scrapy.Field()
    error_msg = scrapy.Field()
