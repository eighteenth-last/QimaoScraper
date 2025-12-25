from sqlmodel import Session, select
from backend.models.models import AdsCapitalLtv, AdsCapitalFuturePurchasingPower

class CapitalService:
    def __init__(self, session: Session):
        self.session = session

    def get_ip_value_ranking(self, limit: int = 10):
        """
        作品热度积分（IP 价值）
        基于 ads_capital_ltv 表
        """
        statement = select(AdsCapitalLtv).order_by(AdsCapitalLtv.ltv_score.desc()).limit(limit)
        books = self.session.exec(statement).all()
        
        result = []
        for book in books:
            result.append({
                "book_id": book.book_id,
                "title": book.title,
                "ip_value": round(book.ltv_score, 2) if book.ltv_score else 0,
                "ip_value_level": book.ip_value_level,
                "total_heat_integral": book.total_heat_integral
            })
            
        return result

    def get_payment_power(self):
        """
        粉丝付费能力分析
        基于 ads_capital_future_purchasing_power 表
        """
        statement = select(AdsCapitalFuturePurchasingPower).limit(200) # 限制数量
        books = self.session.exec(statement).all()
        
        return [
            {
                "book_id": b.book_id,
                "title": b.title,
                "arpu": b.avg_arpu,
                "fan_quality": b.avg_fan_quality,
                "level": b.investment_value_level
            }
            for b in books
        ]
