from fastapi import APIRouter, Depends, Query
from sqlmodel import Session
from backend.core.database import get_session
from backend.services.reader import ReaderService
from backend.schemas.response import ResponseBase
from typing import List, Any, Optional

router = APIRouter()

@router.get("/top-books", response_model=ResponseBase[List[Any]])
def get_top_books(
    category: Optional[str] = Query(None, description="分类名称"),
    session: Session = Depends(get_session)
):
    """
    获取热门作品 Top 5
    """
    service = ReaderService(session)
    data = service.get_top_books(category=category)
    return ResponseBase(data=data)

@router.get("/categories", response_model=ResponseBase[List[str]])
def get_categories(session: Session = Depends(get_session)):
    """
    获取所有分类
    """
    service = ReaderService(session)
    data = service.get_all_categories()
    return ResponseBase(data=data)

@router.get("/high-heat-low-score", response_model=ResponseBase[List[Any]])
def get_high_heat_low_score(session: Session = Depends(get_session)):
    """
    获取高热低分作品
    """
    service = ReaderService(session)
    data = service.get_high_heat_low_score()
    return ResponseBase(data=data)
