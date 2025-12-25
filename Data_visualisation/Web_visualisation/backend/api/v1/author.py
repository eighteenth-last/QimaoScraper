from fastapi import APIRouter, Depends
from sqlmodel import Session
from backend.core.database import get_session
from backend.services.author import AuthorService
from backend.schemas.response import ResponseBase
from typing import List, Any

router = APIRouter()

@router.get("/category-avg-heat", response_model=ResponseBase[List[Any]])
def get_category_avg_heat(session: Session = Depends(get_session)):
    """
    获取各类别平均热度
    """
    service = AuthorService(session)
    data = service.get_category_avg_heat()
    return ResponseBase(data=data)

@router.get("/wordcount-vs-heat", response_model=ResponseBase[List[Any]])
def get_wordcount_vs_heat(session: Session = Depends(get_session)):
    """
    获取字数与热度关系数据
    """
    service = AuthorService(session)
    data = service.get_wordcount_vs_heat()
    return ResponseBase(data=data)

@router.get("/status-heat-compare", response_model=ResponseBase[List[Any]])
def get_status_heat_compare(session: Session = Depends(get_session)):
    """
    获取完结 vs 连载热度对比
    """
    service = AuthorService(session)
    data = service.get_status_heat_compare()
    return ResponseBase(data=data)
