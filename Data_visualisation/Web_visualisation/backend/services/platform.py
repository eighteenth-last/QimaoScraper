from sqlmodel import Session, select
from backend.models.models import AdsPlatformHeat
from datetime import datetime, timedelta

class PlatformService:
    def __init__(self, session: Session):
        self.session = session

    def get_heat_growth_ranking(self, limit: int = 10):
        """
        计算每部作品的热度增长率
        基于 ads_platform_heat 表
        """
        # 查询有增长率数据的记录
        statement = select(AdsPlatformHeat).where(AdsPlatformHeat.popularity_growth_rate != None)
        # 按增长率降序
        statement = statement.order_by(AdsPlatformHeat.popularity_growth_rate.desc()).limit(limit)
        
        books = self.session.exec(statement).all()
        
        result = []
        for book in books:
            result.append({
                "book_id": book.book_id,
                "title": book.title,
                "current_heat": book.numeric_popularity,
                "previous_heat": book.prev_day_popularity,
                "growth_rate": round(book.popularity_growth_rate, 4) if book.popularity_growth_rate else 0
            })
        return result

    def get_new_book_trend(self):
        """
        新作品热度变化趋势
        基于 ads_platform_heat 表，筛选 is_new_book=1
        """
        statement = select(AdsPlatformHeat).where(AdsPlatformHeat.is_new_book == 1)
        books = self.session.exec(statement).all()
        
        # 按日期聚合平均热度
        data_map = {}
        for book in books:
            if not book.rank_date:
                continue
            date_str = book.rank_date.strftime("%Y-%m-%d")
            if date_str not in data_map:
                data_map[date_str] = []
            if book.numeric_popularity:
                data_map[date_str].append(book.numeric_popularity)
            
        trend = []
        for date_str, heats in data_map.items():
            if heats:
                trend.append({
                    "date": date_str,
                    "avg_heat": round(sum(heats) / len(heats), 2)
                })
        
        # 按日期升序
        trend.sort(key=lambda x: x["date"])
        return trend
