import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from validator.common.types import (
    EpochAudit,
    Incident,
    MinerVerificationState,
    VerificationResult,
)
from validator.config import DatabaseConfig
from validator.db.models import (
    Base,
    EpochAuditModel,
    IncidentModel,
    MinerModel,
    MinerVerificationStateModel,
    ProcessedEpoch,
    VerificationResultModel,
)

logger = logging.getLogger(__name__)


class PostgresStore:
    """
    Implements both EpochStore and VerificationStore protocols.
    Single class, single connection pool.
    """

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self._engine = None
        self._session_factory = None

    @property
    def session_factory(self) -> Optional[async_sessionmaker]:
        return self._session_factory

    async def connect(self):
        url = (
            f"postgresql+asyncpg://{self.config.user}:{self.config.password}"
            f"@{self.config.host}:{self.config.port}/{self.config.name}"
        )
        self._engine = create_async_engine(url, echo=False)
        self._session_factory = async_sessionmaker(self._engine, expire_on_commit=False)
        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("Database connected and schema initialized")

    async def close(self):
        if self._engine:
            await self._engine.dispose()

    async def _session(self) -> AsyncSession:
        if not self._session_factory:
            await self.connect()
        return self._session_factory()

    # --- Miners ---

    async def ensure_miner(self, hotkey: str, coldkey: str, uid: int):
        async with await self._session() as session:
            stmt = (
                insert(MinerModel)
                .values(
                    hotkey=hotkey,
                    coldkey=coldkey,
                    uid=uid,
                    registered_at=datetime.now(timezone.utc),
                    last_seen=datetime.now(timezone.utc),
                    is_active=True,
                )
                .on_conflict_do_update(
                    index_elements=["hotkey"],
                    set_={
                        "coldkey": coldkey,
                        "uid": uid,
                        "last_seen": datetime.now(timezone.utc),
                        "is_active": True,
                    },
                )
            )
            await session.execute(stmt)
            await session.commit()

    # --- EpochStore ---

    async def is_epoch_processed(self, epoch_id: str) -> bool:
        async with await self._session() as session:
            result = await session.execute(
                select(ProcessedEpoch.epoch_id).where(
                    ProcessedEpoch.epoch_id == epoch_id
                )
            )
            return result.fetchone() is not None

    async def mark_epoch_processed(
        self, epoch_id: str, weights_submitted: bool = False
    ):
        async with await self._session() as session:
            stmt = (
                insert(ProcessedEpoch)
                .values(
                    epoch_id=epoch_id,
                    processed_at=datetime.now(timezone.utc),
                    weights_submitted=weights_submitted,
                )
                .on_conflict_do_update(
                    index_elements=["epoch_id"],
                    set_={"weights_submitted": weights_submitted},
                )
            )
            await session.execute(stmt)
            await session.commit()

    async def save_epoch_audit(self, audit: EpochAudit):
        async with await self._session() as session:
            stmt = (
                insert(EpochAuditModel)
                .values(
                    epoch_id=audit.epoch_id,
                    block_number=audit.block_number,
                    total_cu=audit.total_cu,
                    miners_paid=audit.miners_paid,
                    miners_banned=audit.miners_banned,
                    total_consumed_usd=Decimal(str(audit.total_consumed_usd)),
                    alpha_price_usd=Decimal(str(audit.alpha_price_usd)),
                    tao_price_usd=Decimal(str(audit.tao_price_usd)),
                    emissions_alpha=Decimal(str(audit.emissions_alpha)),
                    burn_pct=Decimal(str(audit.burn_pct)),
                    weights_submitted=audit.weights_submitted,
                    miner_details=audit.miner_details,
                    created_at=datetime.now(timezone.utc),
                )
                .on_conflict_do_update(
                    index_elements=["epoch_id"],
                    set_={
                        "weights_submitted": audit.weights_submitted,
                        "miner_details": audit.miner_details,
                    },
                )
            )
            await session.execute(stmt)
            await session.commit()

    # --- VerificationStore ---

    async def get_verification_state(
        self, miner_hotkey: str
    ) -> Optional[MinerVerificationState]:
        async with await self._session() as session:
            result = await session.execute(
                select(MinerVerificationStateModel).where(
                    MinerVerificationStateModel.miner_hotkey == miner_hotkey
                )
            )
            row = result.scalars().first()
            if not row:
                return None
            return MinerVerificationState(
                miner_hotkey=row.miner_hotkey,
                first_seen_epoch=row.first_seen_epoch,
                total_logged_queries=row.total_logged_queries,
                last_logged_check=row.last_logged_check,
                logged_pass_count=row.logged_pass_count,
                logged_fail_count=row.logged_fail_count,
                is_trusted=row.is_trusted,
                trusted_at=row.trusted_at,
            )

    async def save_verification_state(self, state: MinerVerificationState):
        async with await self._session() as session:
            stmt = (
                insert(MinerVerificationStateModel)
                .values(
                    miner_hotkey=state.miner_hotkey,
                    first_seen_epoch=state.first_seen_epoch,
                    total_logged_queries=state.total_logged_queries,
                    last_logged_check=state.last_logged_check,
                    logged_pass_count=state.logged_pass_count,
                    logged_fail_count=state.logged_fail_count,
                    is_trusted=state.is_trusted,
                    trusted_at=state.trusted_at,
                )
                .on_conflict_do_update(
                    index_elements=["miner_hotkey"],
                    set_={
                        "total_logged_queries": state.total_logged_queries,
                        "last_logged_check": state.last_logged_check,
                        "logged_pass_count": state.logged_pass_count,
                        "logged_fail_count": state.logged_fail_count,
                        "is_trusted": state.is_trusted,
                        "trusted_at": state.trusted_at,
                    },
                )
            )
            await session.execute(stmt)
            await session.commit()

    async def save_verification_result(
        self,
        result: VerificationResult,
        miner_hotkey: str,
        node_id: str,
        chain: str,
        epoch_id: str,
    ):
        async with await self._session() as session:
            model = VerificationResultModel(
                miner_hotkey=miner_hotkey,
                node_id=node_id,
                chain=chain,
                epoch_id=epoch_id,
                verification_type="logged",
                method=result.method,
                block_number=result.block_number or 0,
                params=result.params,
                miner_response_hash=result.miner_response_hash,
                ref_response_hash=result.ref_response_hash,
                is_correct=result.is_correct,
                latency_ref_ms=result.latency_ref_ms,
                error_details=result.error_details,
            )
            session.add(model)
            await session.commit()

    async def save_incident(self, incident: Incident):
        async with await self._session() as session:
            model = IncidentModel(
                miner_hotkey=incident.miner_hotkey,
                miner_coldkey=incident.miner_coldkey,
                epoch_id=incident.epoch_id,
            )
            session.add(model)
            await session.commit()

    async def increment_pass_count(self, miner_hotkey: str):
        async with await self._session() as session:
            stmt = (
                update(MinerVerificationStateModel)
                .where(MinerVerificationStateModel.miner_hotkey == miner_hotkey)
                .values(
                    logged_pass_count=MinerVerificationStateModel.logged_pass_count + 1
                )
            )
            await session.execute(stmt)
            await session.commit()

    async def increment_fail_count(self, miner_hotkey: str):
        async with await self._session() as session:
            stmt = (
                update(MinerVerificationStateModel)
                .where(MinerVerificationStateModel.miner_hotkey == miner_hotkey)
                .values(
                    logged_fail_count=MinerVerificationStateModel.logged_fail_count + 1
                )
            )
            await session.execute(stmt)
            await session.commit()
