from datetime import datetime

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Index,
    Integer,
    Numeric,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class MinerModel(Base):
    __tablename__ = "miners"

    hotkey = Column(String(48), primary_key=True)
    coldkey = Column(String(48), nullable=False, index=True)
    uid = Column(Integer)
    registered_at = Column(DateTime(timezone=True))
    last_seen = Column(DateTime(timezone=True))
    is_active = Column(Boolean)


class ProcessedEpoch(Base):
    __tablename__ = "processed_epochs"

    epoch_id = Column(String(100), primary_key=True)
    processed_at = Column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )
    weights_submitted = Column(Boolean, default=False)


class EpochAuditModel(Base):
    """write-only audit log — one row per epoch, JSONB for miner details"""

    __tablename__ = "epoch_audit"

    epoch_id = Column(String(100), primary_key=True)
    block_number = Column(BigInteger, nullable=False)
    total_cu = Column(BigInteger, default=0)
    miners_paid = Column(Integer, default=0)
    miners_banned = Column(Integer, default=0)
    total_consumed_usd = Column(Numeric(14, 4))
    alpha_price_usd = Column(Numeric(10, 6))
    tao_price_usd = Column(Numeric(10, 4))
    emissions_alpha = Column(Numeric(14, 4))
    burn_pct = Column(Numeric(5, 4))
    weights_submitted = Column(Boolean, default=False)
    miner_details = Column(JSON, default=dict)
    created_at = Column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )


class MinerVerificationStateModel(Base):
    __tablename__ = "miner_verification_state"

    miner_hotkey = Column(String(48), primary_key=True)
    first_seen_epoch = Column(String(100), nullable=False)
    total_logged_queries = Column(BigInteger, default=0)
    last_logged_check = Column(DateTime(timezone=True))
    logged_pass_count = Column(Integer, default=0)
    logged_fail_count = Column(Integer, default=0)
    is_trusted = Column(Boolean, default=False)
    trusted_at = Column(DateTime(timezone=True))
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )


class VerificationResultModel(Base):
    __tablename__ = "verification_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
    epoch_id = Column(String(100), nullable=False, index=True)
    miner_hotkey = Column(String(48), nullable=False, index=True)
    node_id = Column(String(100), nullable=False)
    chain = Column(String(10), nullable=False)
    verification_type = Column(String(20), nullable=False, server_default="logged")
    method = Column(String(100), nullable=False)
    block_number = Column(BigInteger, nullable=False)
    params = Column(JSON)
    miner_response_hash = Column(String(80))
    ref_response_hash = Column(String(80))
    is_correct = Column(Boolean, nullable=False)
    latency_ref_ms = Column(Integer)
    error_details = Column(Text)


Index(
    "idx_verification_results_incorrect",
    VerificationResultModel.is_correct,
    postgresql_where=~VerificationResultModel.is_correct,
)


class IncidentModel(Base):
    __tablename__ = "incidents"

    id = Column(Integer, primary_key=True, autoincrement=True)
    miner_hotkey = Column(String(48), nullable=False, index=True)
    miner_coldkey = Column(String(48), nullable=False, index=True)
    epoch_id = Column(String(100), nullable=False)
    created_at = Column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )
