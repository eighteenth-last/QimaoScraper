"""
*@BelongsProject: QimaoScraper
*@BelongsPackage: qimao_scrapy
*@Author: 程序员Eighteen
*@Description: 数据库配置 - 男频/女频分表存储
*@Version: 2.0
"""
from sqlalchemy import create_engine, Column, Integer, String, Text, TIMESTAMP, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager

# 数据库配置
DB_CONFIG = {
    'host': '172.31.142.67',
    'user': 'root',
    'password': 'qwer4321',
    'database': 'QimaoScraper',
    'charset': 'utf8mb4'
}

# 创建数据库连接字符串
DATABASE_URL = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}?charset={DB_CONFIG['charset']}"

# 创建引擎
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=3600,
    echo=False
)

# 创建基类
Base = declarative_base()

# 创建会话工厂
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


# 男频小说表模型
class MaleOrientedNovels(Base):
    __tablename__ = 'Male_oriented_novels'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    book_id = Column(String(50), nullable=False, index=True)
    title = Column(String(255))
    author = Column(String(100))
    category1_name = Column(String(50))
    category2_name = Column(String(50))
    words_num = Column(String(50))
    intro = Column(Text)
    image_link = Column(String(500))
    status = Column(String(20))  # 0:连载中 1:完结
    number = Column(String(50))
    unit = Column(String(10))
    rank_name = Column(String(20))  # 大热/新书/完结/收藏/更新
    date_type = Column(String(20))  # 月榜/日榜
    date_month = Column(String(20))  # 202310-202510
    score = Column(String(20))
    read_count = Column(String(50))
    popularity = Column(String(50))
    error = Column(String(10), default='false')
    error_msg = Column(Text)
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())


# 女频小说表模型
class WomensFiction(Base):
    __tablename__ = 'Womens_fiction'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    book_id = Column(String(50), nullable=False, index=True)
    title = Column(String(255))
    author = Column(String(100))
    category1_name = Column(String(50))
    category2_name = Column(String(50))
    words_num = Column(String(50))
    intro = Column(Text)
    image_link = Column(String(500))
    status = Column(String(20))  # 0:连载中 1:完结
    number = Column(String(50))
    unit = Column(String(10))
    rank_name = Column(String(20))  # 大热/新书/完结/收藏/更新
    date_type = Column(String(20))  # 月榜/日榜
    date_month = Column(String(20))  # 202310-202510
    score = Column(String(20))
    read_count = Column(String(50))
    popularity = Column(String(50))
    error = Column(String(10), default='false')
    error_msg = Column(Text)
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())


# 创建所有表
def create_tables():
    """创建所有表"""
    Base.metadata.create_all(bind=engine)
    print("✓ 数据表创建/检查完成")
    print("  - Male_oriented_novels (男频小说)")
    print("  - Womens_fiction (女频小说)")


# 上下文管理器，用于获取数据库会话
@contextmanager
def get_db_session():
    """获取数据库会话的上下文管理器"""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


if __name__ == "__main__":
    # 测试数据库连接
    try:
        create_tables()
        print("✓ 数据库连接成功")
    except Exception as e:
        print(f"✗ 数据库连接失败: {e}")
