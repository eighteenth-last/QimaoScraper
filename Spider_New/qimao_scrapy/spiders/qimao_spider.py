"""
*@BelongsProject: QimaoScraper
*@Author: 程序员Eighteen
*@Description: 七猫小说Scrapy爬虫 - 爬取榜单和详情数据
*@Version: 3.0 (Scrapy重构版)
"""
import scrapy
from scrapy import Request
from qimao_scrapy.items import QimaoBookItem


class QimaoSpider(scrapy.Spider):
    name = 'qimao'
    allowed_domains = ['qimao.com']
    
    # 自定义配置
    custom_settings = {
        'CONCURRENT_REQUESTS': 16,  # 并发请求数
        'DOWNLOAD_DELAY': 0.5,      # 下载延迟
        'COOKIES_ENABLED': True,
        'DEFAULT_REQUEST_HEADERS': {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
    }
    
    def __init__(self, *args, **kwargs):
        super(QimaoSpider, self).__init__(*args, **kwargs)
        self.list_url = "https://www.qimao.com/qimaoapi/api/rank/book-list"
        self.detail_url_template = "https://www.qimao.com/shuku/{}"
        
        # 生成24个月份（202310-202510）
        self.date_list = self._generate_months()
        self.logger.info(f"📅 将爬取 {len(self.date_list)} 个月份: {self.date_list[0]} 至 {self.date_list[-1]}")
    
    def _generate_months(self):
        """生成202310-202510的24个月份"""
        months = []
        for year in [2023, 2024, 2025]:
            start_month = 10 if year == 2023 else 1
            end_month = 10 if year == 2025 else 12
            for month in range(start_month, end_month + 1):
                months.append(f"{year}{month:02d}")
        return months
    
    def start_requests(self):
        """生成所有榜单请求"""
        rank_names = {"1": "大热", "2": "新书", "3": "完结", "4": "收藏", "6": "更新"}
        
        # 只爬取月榜
        self.logger.info(f"\n{'='*60}\n开始爬取月榜数据...\n{'='*60}")
        
        for is_girl in ["0", "1"]:
            gender = "男生" if is_girl == "0" else "女生"
            
            for rank_type in ["1", "2", "3", "4", "6"]:
                rank_name = rank_names[rank_type]
                
                # 大热、新书、完结榜：遍历所有月份
                if rank_type in ["1", "2", "3"]:
                    month_list = self.date_list
                else:
                    # 收藏榜、更新榜：只爬取一次，不需要月份
                    month_list = [None]
                
                for date_value in month_list:
                    for page in range(1, 6):
                        # 构建参数
                        if rank_type in ["4", "6"]:
                            # 收藏榜、更新榜：不需要date_type和date
                            params = {
                                "is_girl": is_girl,
                                "rank_type": rank_type,
                                "date_type": "1",  # 固定为月榜
                                "date": "",
                                "page": str(page)
                            }
                        else:
                            # 大热、新书、完结榜：需要date_type和date
                            params = {
                                "is_girl": is_girl,
                                "rank_type": rank_type,
                                "date_type": "2",  # 固定为月榜
                                "date": date_value,
                                "page": str(page)
                            }
                        
                        # 元数据
                        meta = {
                            'gender': gender,
                            'rank_name': rank_name,
                            'date_type': "" if rank_type in ["4", "6"] else "月榜",
                            'date_month': "" if rank_type in ["4", "6"] else date_value,
                            'rank_type': rank_type,
                            'page': page
                        }
                        
                        # 构建完整URL（带参数）
                        from urllib.parse import urlencode
                        full_url = f"{self.list_url}?{urlencode(params)}"
                        
                        yield Request(
                            url=full_url,
                            callback=self.parse_list,
                            meta=meta,
                            dont_filter=True,
                            errback=self.errback_httpbin
                        )
    
    def parse_list(self, response):
        """解析榜单列表"""
        try:
            data = response.json()
            
            if 'data' in data and 'table_data' in data['data']:
                book_list = data['data']['table_data']
                self.logger.info(f"✓ 成功获取 {len(book_list)} 本书 - {response.meta['gender']}频道 - "
                               f"{response.meta['rank_name']}榜 - 第{response.meta['page']}页")
                
                for book in book_list:
                    # 创建Item
                    item = QimaoBookItem()
                    
                    # 榜单数据
                    item['book_id'] = book.get('book_id', '')
                    item['title'] = book.get('title', '')
                    item['author'] = book.get('author', '')
                    item['category1_name'] = book.get('category1_name', '')
                    item['category2_name'] = book.get('category2_name', '')
                    item['words_num'] = book.get('words_num', '')
                    item['intro'] = book.get('intro', '')
                    item['image_link'] = book.get('image_link', '')
                    item['status'] = book.get('is_over', '')
                    item['number'] = book.get('number', '')
                    item['unit'] = book.get('unit', '')
                    
                    # 榜单信息
                    item['gender'] = response.meta['gender']
                    item['rank_name'] = response.meta['rank_name']
                    item['date_type'] = response.meta['date_type']
                    item['date_month'] = response.meta['date_month']
                    
                    # 请求详情页
                    detail_url = self.detail_url_template.format(book.get('book_id', ''))
                    yield Request(
                        url=detail_url,
                        callback=self.parse_detail,
                        meta={'item': item},
                        dont_filter=True,
                        errback=self.errback_detail
                    )
            else:
                self.logger.warning(f"✗ 数据格式异常: {data.get('msg', '未知错误')}")
                
        except Exception as e:
            self.logger.error(f"✗ 解析榜单失败: {e}")
    
    def parse_detail(self, response):
        """解析详情页"""
        item = response.meta['item']
        
        try:
            # 使用XPath提取详情数据
            score = response.xpath('//*[@id="__layout"]/div/div[3]/div/div/div/div[1]/div/div[1]/div[2]/div[1]/span[2]/text()').get()
            read_count = response.xpath('//*[@id="__layout"]/div/div[3]/div/div/div/div[1]/div/div[1]/div[2]/div[4]/span[2]/em/text()').get()
            popularity = response.xpath('//*[@id="__layout"]/div/div[3]/div/div/div/div[1]/div/div[1]/div[2]/div[4]/span[3]/em/text()').get()
            
            item['score'] = score.strip() if score else '未找到'
            item['read_count'] = read_count.strip() if read_count else '未找到'
            item['popularity'] = popularity.strip() if popularity else '未找到'
            item['error'] = False
            item['error_msg'] = ''
            
            self.logger.info(f"📚 《{item['title']}》 详情获取成功")
            
        except Exception as e:
            item['score'] = '获取失败'
            item['read_count'] = '获取失败'
            item['popularity'] = '获取失败'
            item['error'] = True
            item['error_msg'] = str(e)
            self.logger.error(f"✗ 详情解析失败: {e}")
        
        yield item
    
    def errback_httpbin(self, failure):
        """榜单请求错误回调"""
        self.logger.error(f"✗ 榜单请求失败: {failure.request.url}")
        self.logger.error(f"  错误: {failure.value}")
    
    def errback_detail(self, failure):
        """详情请求错误回调"""
        item = failure.request.meta['item']
        item['score'] = '请求失败'
        item['read_count'] = '请求失败'
        item['popularity'] = '请求失败'
        item['error'] = True
        item['error_msg'] = str(failure.value)
        
        self.logger.error(f"✗ 详情请求失败: {failure.request.url}")
        yield item
