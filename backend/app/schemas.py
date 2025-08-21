from pydantic import BaseModel
from typing import Any, Dict, List, Optional, Tuple


class CompanyOut(BaseModel):
    id: int
    name: str
    ticker: Optional[str] = None
    careers_url: Optional[str] = None
    ats_kind: Optional[str] = None

    class Config:
        orm_mode = True


class HiringScoreOut(BaseModel):
    id: int
    company_id: int
    role_family: str
    score: int
    details_json: Optional[dict]

    class Config:
        orm_mode = True


class ScoreRow(BaseModel):
    company_id: int
    company_name: str
    role_family: str
    score: int
    details_json: Optional[Dict] = None
    open_count: int
