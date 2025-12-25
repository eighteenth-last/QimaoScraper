from sqlmodel import Session, select, func
from backend.models.models import AdsAuthorReason, AdsCapitalLtv

class AuthorService:
    def __init__(self, session: Session):
        self.session = session

    def get_category_avg_heat(self):
        """
        各类别平均热度
        基于 ads_author_reason 表
        """
        # 直接查询聚合表中的数据
        # 假设表中 category1_name 是唯一的或者已经聚合过的
        statement = select(AdsAuthorReason.category1_name, func.avg(AdsAuthorReason.avg_popularity)).group_by(AdsAuthorReason.category1_name)
        results = self.session.exec(statement).all()
        
        return [{"category": r[0], "avg_heat": round(r[1], 2) if r[1] else 0} for r in results]

    def get_wordcount_vs_heat(self):
        """
        字数与热度关系
        基于 ads_capital_ltv 表 (含有 total_words 和 avg_popularity)
        """
        statement = select(AdsCapitalLtv.total_words, AdsCapitalLtv.avg_popularity).where(AdsCapitalLtv.total_words != None).limit(500) # 限制点数防止前端卡顿
        results = self.session.exec(statement).all()
        
        return [{"word_count": r[0], "heat": r[1]} for r in results if r[0] and r[1]]

    def get_status_heat_compare(self):
        """
        完结 vs 连载热度对比
        基于 ads_capital_ltv 表
        """
        statement = select(AdsCapitalLtv.status, func.avg(AdsCapitalLtv.avg_popularity)).group_by(AdsCapitalLtv.status)
        results = self.session.exec(statement).all()
        return [{"status": r[0], "avg_heat": round(r[1], 2) if r[1] else 0} for r in results if r[0]]
