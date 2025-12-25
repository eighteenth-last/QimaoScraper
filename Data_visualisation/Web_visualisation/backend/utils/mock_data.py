from sqlmodel import Session, select
from backend.models.models import (
    AdsPlatformHeat, 
    AdsAuthorReason, 
    AdsCapitalLtv, 
    AdsCapitalFuturePurchasingPower,
    AdsUserAvoidPitfalls,
    AdsUserLayeredRecommendation
)
import random
from datetime import datetime, timedelta

def init_mock_data(session: Session):
    # 如果已有数据则跳过
    if session.exec(select(AdsPlatformHeat)).first():
        return
        
    print("正在初始化模拟数据 (基于新表结构)...")
    
    categories = ["玄幻", "都市", "仙侠", "科幻", "历史", "奇闻异事"]
    statuses = ["连载", "完结"]
    
    for i in range(50):
        book_id = str(i + 1000)
        title = f"测试小说_{i+1}"
        cat = random.choice(categories)
        status = random.choice(statuses)
        heat = random.uniform(1000, 100000)
        
        # 1. Platform Heat
        p_heat = AdsPlatformHeat(
            book_id=book_id,
            title=title,
            category1_name=cat,
            rank_date=datetime.now().date(),
            numeric_popularity=heat,
            prev_day_popularity=heat * 0.9,
            popularity_growth_rate=0.1,
            is_new_book=1 if i < 10 else 0
        )
        session.add(p_heat)
        
        # 2. Capital LTV
        ltv = AdsCapitalLtv(
            book_id=book_id,
            title=title,
            total_words=random.randint(100000, 3000000),
            avg_popularity=heat,
            status=status,
            ltv_score=random.uniform(60, 99),
            ip_value_level="S" if heat > 80000 else "A"
        )
        session.add(ltv)
        
        # 3. Purchasing Power
        power = AdsCapitalFuturePurchasingPower(
            book_id=book_id,
            title=title,
            avg_arpu=random.uniform(10, 200),
            avg_fan_quality=random.uniform(0, 1),
            investment_value_level="High"
        )
        session.add(power)
        
        # 4. Avoid Pitfalls (部分数据)
        if i % 5 == 0:
            pit = AdsUserAvoidPitfalls(
                book_id=book_id,
                title=title,
                is_high_heat_low_score=1,
                numeric_popularity=heat,
                numeric_score=3.5,
                avoidance_advice="热度虚高，谨慎入坑"
            )
            session.add(pit)
            
        # 5. Recommendation
        rec = AdsUserLayeredRecommendation(
            book_id=book_id,
            title=title,
            category1_name=cat,
            numeric_popularity=heat,
            numeric_score=random.uniform(7.0, 9.8)
        )
        session.add(rec)
        
    # 6. Author Reason (聚合)
    for cat in categories:
        reason = AdsAuthorReason(
            category1_name=cat,
            avg_popularity=random.uniform(5000, 20000),
            avg_score=random.uniform(7.5, 8.5),
            words_range="100w-200w"
        )
        session.add(reason)

    session.commit()
    print("模拟数据初始化完成")
