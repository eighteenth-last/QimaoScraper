from sqlmodel import Session, select, func
from backend.models.models import AdsUserLayeredRecommendation, AdsUserAvoidPitfalls
from typing import Optional

class ReaderService:
    def __init__(self, session: Session):
        self.session = session

    def get_top_books(self, category: Optional[str] = None, limit: int = 5):
        """
        各分类 Top 5 热门作品
        基于 ads_user_layered_recommendation (分层推荐表)
        """
        statement = select(AdsUserLayeredRecommendation).order_by(AdsUserLayeredRecommendation.numeric_popularity.desc())
        if category:
            statement = statement.where(AdsUserLayeredRecommendation.category1_name == category)
            
        books = self.session.exec(statement.limit(limit)).all()
        
        return [
            {
                "book_id": b.book_id,
                "title": b.title,
                "category": b.category1_name,
                "current_heat": b.numeric_popularity,
                "score": b.numeric_score
            }
            for b in books
        ]
        
    def get_all_categories(self):
        """
        获取所有分类列表，供前端切换
        """
        statement = select(AdsUserLayeredRecommendation.category1_name).distinct()
        return self.session.exec(statement).all()

    def get_high_heat_low_score(self):
        """
        高热低分作品识别
        基于 ads_user_avoid_pitfalls 表，直接查询标志位
        """
        statement = select(AdsUserAvoidPitfalls).where(AdsUserAvoidPitfalls.is_high_heat_low_score == 1)
        books = self.session.exec(statement).all()
        
        return [
            {
                "book_id": b.book_id,
                "title": b.title,
                "heat": b.numeric_popularity,
                "score": b.numeric_score,
                "advice": b.avoidance_advice
            }
            for b in books
        ]
