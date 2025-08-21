from typing import Optional, Dict
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import (
    String, Integer, DateTime, ForeignKey, JSON, Boolean,
    UniqueConstraint, func, Float
)

class Base(DeclarativeBase):
    pass

# ---------- Core ----------
class Company(Base):
    __tablename__ = "companies"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(200), unique=True, index=True)
    ticker: Mapped[Optional[str]] = mapped_column(String(20))
    careers_url: Mapped[Optional[str]] = mapped_column(String(500))
    ats_kind: Mapped[Optional[str]] = mapped_column(String(50))
    linkedin_url: Mapped[Optional[str]] = mapped_column(String(500))

class JobPosting(Base):
    __tablename__ = "job_postings"
    id: Mapped[int] = mapped_column(primary_key=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id", ondelete="CASCADE"), index=True)
    source_job_id: Mapped[str] = mapped_column(String(128))
    title: Mapped[str] = mapped_column(String(300), index=True)
    department: Mapped[Optional[str]] = mapped_column(String(200))
    location: Mapped[Optional[str]] = mapped_column(String(200))
    remote_ok: Mapped[bool] = mapped_column(default=False)
    role_family: Mapped[Optional[str]] = mapped_column(String(80))
    apply_url: Mapped[Optional[str]] = mapped_column(String(600))
    status: Mapped[str] = mapped_column(String(16), default="OPEN")
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    __table_args__ = (UniqueConstraint("company_id", "source_job_id", name="u_company_sourcejob"),)

# ---------- Signals & Metrics ----------
class Signal(Base):
    __tablename__ = "signals"
    id: Mapped[int] = mapped_column(primary_key=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id", ondelete="CASCADE"), index=True)
    kind: Mapped[str] = mapped_column(String(50))
    happened_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True))
    payload_json: Mapped[Optional[Dict]] = mapped_column(JSON)

class JobMetrics(Base):
    __tablename__ = "job_metrics"
    id: Mapped[int] = mapped_column(primary_key=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id", ondelete="CASCADE"), index=True)
    week_start: Mapped[DateTime] = mapped_column(DateTime(timezone=True), index=True)
    sde_openings: Mapped[int] = mapped_column(Integer)
    sde_new: Mapped[int] = mapped_column(Integer)
    sde_closed: Mapped[int] = mapped_column(Integer)

class HiringScore(Base):
    __tablename__ = "hiring_score"
    id: Mapped[int] = mapped_column(primary_key=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id", ondelete="CASCADE"), index=True)
    role_family: Mapped[str] = mapped_column(String(80))
    computed_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    score: Mapped[int] = mapped_column(Integer)
    details_json: Mapped[Optional[Dict]] = mapped_column(JSON)

# ---------- Sources & Raw Payload ----------
class Source(Base):
    __tablename__ = "sources"
    id: Mapped[int] = mapped_column(primary_key=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id", ondelete="CASCADE"), nullable=False)
    kind: Mapped[str] = mapped_column(String)
    endpoint_url: Mapped[str] = mapped_column(String)
    auth_kind: Mapped[Optional[str]] = mapped_column(String)
    last_ok_at: Mapped[Optional[DateTime]] = mapped_column(DateTime(timezone=True))
    handle: Mapped[Optional[str]] = mapped_column(String)

class JobRaw(Base):
    __tablename__ = "job_raw"
    id: Mapped[int] = mapped_column(primary_key=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id", ondelete="CASCADE"), nullable=False)
    source_id: Mapped[Optional[int]] = mapped_column(ForeignKey("sources.id", ondelete="SET NULL"))
    fetched_at: Mapped[Optional[DateTime]] = mapped_column(DateTime(timezone=True), server_default=func.now())
    payload_json: Mapped[Dict] = mapped_column(JSON, nullable=False)

# ---------- Forecasts ----------
class Forecast(Base):
    __tablename__ = "forecast"
    id: Mapped[int] = mapped_column(primary_key=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id", ondelete="CASCADE"), nullable=False)
    role_family: Mapped[str] = mapped_column(String, nullable=False)
    computed_at: Mapped[Optional[DateTime]] = mapped_column(DateTime(timezone=True), server_default=func.now())
    prob_next_8w: Mapped[float] = mapped_column(Float, nullable=False)
    likely_month: Mapped[str] = mapped_column(String, nullable=False)
    method: Mapped[Optional[str]] = mapped_column(String)
    ci_low: Mapped[Optional[float]] = mapped_column(Float)
    ci_high: Mapped[Optional[float]] = mapped_column(Float)
    features_json: Mapped[Optional[Dict]] = mapped_column(JSON)
