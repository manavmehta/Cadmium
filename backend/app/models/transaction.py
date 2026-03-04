from datetime import date

from sqlalchemy import Date, Float, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class Transaction(Base):
    __tablename__ = "transactions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    date: Mapped[date] = mapped_column(Date, index=True)
    quantity: Mapped[float] = mapped_column(Float)
    price: Mapped[float] = mapped_column(Float)
    transaction_type: Mapped[str] = mapped_column(String(16))
    broker: Mapped[str] = mapped_column(String(32), index=True)
