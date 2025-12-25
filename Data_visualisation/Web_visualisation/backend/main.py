import sys
import os

# 将当前文件的父目录的父目录添加到 sys.path，以便能够解析 'backend' 模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from backend.core.config import settings
from backend.core.database import create_db_and_tables, get_session
from backend.api.v1 import platform, author, reader, capital
from backend.utils.mock_data import init_mock_data
from sqlmodel import Session
from backend.core.database import engine

@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    with Session(engine) as session:
        init_mock_data(session)
    
    # 启动标识
    print("\n" + "="*50)
    print("🚀 Backend Service Started Successfully!")
    print(f"📄 Docs: http://127.0.0.1:8000/docs")
    print("="*50 + "\n")
    
    yield

app = FastAPI(
    title=settings.PROJECT_NAME,
    openapi_url=f"{settings.API_V1_STR}/openapi.json",
    lifespan=lifespan
)

# CORS 设置，允许前端跨域调用
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册路由
app.include_router(platform.router, prefix=f"{settings.API_V1_STR}/platform", tags=["Platform"])
app.include_router(author.router, prefix=f"{settings.API_V1_STR}/author", tags=["Author"])
app.include_router(reader.router, prefix=f"{settings.API_V1_STR}/reader", tags=["Reader"])
app.include_router(capital.router, prefix=f"{settings.API_V1_STR}/capital", tags=["Capital"])

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("backend.main:app", host="0.0.0.0", port=8000, reload=True)
