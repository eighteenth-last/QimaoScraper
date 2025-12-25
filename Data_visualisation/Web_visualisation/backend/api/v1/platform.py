from fastapi import APIRouter, Depends
from sqlmodel import Session
from backend.core.database import get_session
from backend.services.platform import PlatformService
from backend.schemas.response import ResponseBase
from typing import List, Any

router = APIRouter()

@router.get("/heat-growth", response_model=ResponseBase[List[Any]])
def get_heat_growth(session: Session = Depends(get_session)):
    """
    获取每部作品的热度增长率 Top N
    """
    service = PlatformService(session)
    data = service.get_heat_growth_ranking()
    return ResponseBase(data=data)

@router.get("/new-book-trend", response_model=ResponseBase[List[Any]])
def get_new_book_trend(session: Session = Depends(get_session)):
    """
    获取新作品热度变化趋势 (上线 <= 30 天)
    """
    service = PlatformService(session)
    data = service.get_new_book_trend()
    return ResponseBase(data=data)
