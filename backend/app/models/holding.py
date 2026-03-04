from datetime import date

from sqlalchemy import Date, Float, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class Holding(Base):
    __tablename__ = "holdings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    isin: Mapped[str] = mapped_column(String(32), default="")
    broker: Mapped[str] = mapped_column(String(32), index=True)
    quantity: Mapped[float] = mapped_column(Float)
    average_buy_price: Mapped[float] = mapped_column(Float)
    buy_date: Mapped[date] = mapped_column(Date)
    current_price: Mapped[float] = mapped_column(Float)
    asset_type: Mapped[str] = mapped_column(String(32), default="equity")
    lot_source: Mapped[str] = mapped_column(String(32), default="snapshot_derived")
    sync_run_id: Mapped[str] = mapped_column(String(64), default="")
    data_quality: Mapped[str] = mapped_column(String(16), default="unreliable")
