from typing import Optional
from sqlmodel import Field, SQLModel
from datetime import date, datetime

# 1. 平台热度表
class AdsPlatformHeat(SQLModel, table=True):
    __tablename__ = "ads_platform_heat"
    book_id: Optional[str] = Field(default=None, primary_key=True)
    title: Optional[str] = None
    author: Optional[str] = None
    category1_name: Optional[str] = None
    category2_name: Optional[str] = None
    rank_name: Optional[str] = None
    rank_date: Optional[date] = None
    numeric_popularity: Optional[float] = None
    numeric_read_count: Optional[float] = None
    numeric_score: Optional[float] = None
    status: Optional[str] = None
    prev_day_popularity: Optional[float] = None
    popularity_growth_rate: Optional[float] = None
    is_new_book: Optional[int] = None

# 2. 作者归因表 (聚合数据)
class AdsAuthorReason(SQLModel, table=True):
    __tablename__ = "ads_author_reason"
    # 由于该表可能是聚合结果，没有明确的主键，我们使用复合主键或由数据库自动生成
    # 这里为了 SQLModel 兼容性，假设 category1_name + words_range 唯一，或者仅作为查询对象
    # 在实际 ORM 中如果没有主键会报错，我们临时指定 category1_name 为主键，实际查询可能需要注意
    category1_name: str = Field(primary_key=True) 
    avg_popularity: Optional[float] = None
    avg_score: Optional[float] = None
    words_range: Optional[str] = None
    status: Optional[str] = None

# 3. 资本 LTV 表
class AdsCapitalLtv(SQLModel, table=True):
    __tablename__ = "ads_capital_ltv"
    book_id: Optional[str] = Field(default=None, primary_key=True)
    title: Optional[str] = None
    total_words: Optional[float] = None
    avg_popularity: Optional[float] = None
    status: Optional[str] = None
    ip_value_level: Optional[str] = None
    ltv_score: Optional[float] = None
    total_heat_integral: Optional[float] = None
    read_count: Optional[float] = Field(alias="avg_read_count", default=None) # 映射字段

# 4. 资本购买力表
class AdsCapitalFuturePurchasingPower(SQLModel, table=True):
    __tablename__ = "ads_capital_future_purchasing_power"
    book_id: Optional[str] = Field(default=None, primary_key=True)
    title: Optional[str] = None
    avg_arpu: Optional[float] = None
    avg_fan_quality: Optional[float] = None
    investment_value_level: Optional[str] = None

# 5. 用户避坑表
class AdsUserAvoidPitfalls(SQLModel, table=True):
    __tablename__ = "ads_user_avoid_pitfalls"
    book_id: Optional[str] = Field(default=None, primary_key=True)
    title: Optional[str] = None
    is_high_heat_low_score: Optional[int] = None
    numeric_popularity: Optional[float] = None
    numeric_score: Optional[float] = None
    avoidance_advice: Optional[str] = None

# 6. 用户分层推荐表
class AdsUserLayeredRecommendation(SQLModel, table=True):
    __tablename__ = "ads_user_layered_recommendation"
    book_id: Optional[str] = Field(default=None, primary_key=True)
    title: Optional[str] = None
    category1_name: Optional[str] = None
    numeric_popularity: Optional[float] = None
    numeric_score: Optional[float] = None
