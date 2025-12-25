from fastapi import APIRouter, Depends
from sqlmodel import Session
from backend.core.database import get_session
from backend.services.capital import CapitalService
from backend.schemas.response import ResponseBase
from typing import List, Any

router = APIRouter()

@router.get("/ip-value", response_model=ResponseBase[List[Any]])
def get_ip_value(session: Session = Depends(get_session)):
    """
    获取作品 IP 价值排行榜
    """
    service = CapitalService(session)
    data = service.get_ip_value_ranking()
    return ResponseBase(data=data)

@router.get("/payment-power", response_model=ResponseBase[List[Any]])
def get_payment_power(session: Session = Depends(get_session)):
    """
    获取粉丝付费能力分析数据
    """
    service = CapitalService(session)
    data = service.get_payment_power()
    return ResponseBase(data=data)
