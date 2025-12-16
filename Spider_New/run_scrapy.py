"""
运行Scrapy爬虫的脚本
使用方法: python run_scrapy.py
"""
from scrapy import cmdline

# 运行爬虫
cmdline.execute("scrapy crawl qimao".split())
