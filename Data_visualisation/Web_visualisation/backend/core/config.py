import os

class Settings:
    PROJECT_NAME: str = "Novel Data Insight Dashboard"
    API_V1_STR: str = "/api/v1"
    # 默认使用 SQLite 以便直接运行，生产环境请改为 MySQL 连接字符串
    # 格式: mysql+mysqlconnector://user:password@host:port/database
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite:///./novel_data.db")

settings = Settings()
