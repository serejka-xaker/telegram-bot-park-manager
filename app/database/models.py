# app/models.py
import uuid
from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, DateTime, Float, BigInteger, Text, Index
from sqlalchemy.orm import relationship
from datetime import datetime, timezone, timedelta 
from .database_async import Base
import pytz


def msk_now():
    """Возвращает текущее время по Московскому времени (MSK, UTC+3) с aware datetime"""
    # Используем pytz для получения часового пояса Москвы
    msk_tz = pytz.timezone('Europe/Moscow')
    return datetime.now(msk_tz)

class Admin(Base):
    __tablename__ = "admins"
    id = Column(Integer, primary_key=True, index=True)
    # telegram_id = Column(Integer, unique=True, nullable=False) # СТАРОЕ ОПРЕДЕЛЕНИЕ
    telegram_id = Column(BigInteger, unique=True, nullable=False) # ИЗМЕНЕНО НА BigInteger
    username = Column(String, nullable=True)
    full_name = Column(String, nullable=True)

class Bot(Base):
    __tablename__ = "bots"
    id = Column(Integer, primary_key=True, index=True)
    uuid = Column(String, unique=True, default=lambda: str(uuid.uuid4()))
    token = Column(String, nullable=False)
    name = Column(String, nullable=True)
    description = Column(String, nullable=True)
    payassist_api_key = Column(String, nullable=True)

    merchants = relationship("Merchant", back_populates="bot", cascade="all, delete-orphan")
    access_requests = relationship("AccessRequest", back_populates="bot", cascade="all, delete-orphan")
    orders = relationship("Order", back_populates="bot", cascade="all, delete-orphan")

class Merchant(Base):
    __tablename__ = "merchants"
    id = Column(Integer, primary_key=True, index=True)
    # telegram_id = Column(Integer, nullable=False) # СТАРОЕ ОПРЕДЕЛЕНИЕ
    telegram_id = Column(BigInteger, nullable=False) # ИЗМЕНЕНО НА BigInteger
    username = Column(String, nullable=True)
    full_name = Column(String, nullable=True)
    bot_id = Column(Integer, ForeignKey("bots.id"))

    bot = relationship("Bot", back_populates="merchants")
    orders = relationship("Order", back_populates="merchant", cascade="all, delete-orphan")

class AccessRequest(Base):
    __tablename__ = "access_requests"
    id = Column(Integer, primary_key=True, index=True)
    # telegram_id = Column(Integer, nullable=False) # СТАРОЕ ОПРЕДЕЛЕНИЕ
    telegram_id = Column(BigInteger, nullable=False) # ИЗМЕНЕНО НА BigInteger
    username = Column(String, nullable=True)
    full_name = Column(String, nullable=True)
    bot_id = Column(Integer, ForeignKey("bots.id"))
    created_at = Column(DateTime(timezone=True), default=msk_now)
    approved = Column(Boolean, default=False)

    bot = relationship("Bot", back_populates="access_requests")

class Order(Base):
    __tablename__ = "orders"
    id = Column(Integer, primary_key=True, index=True)
    merchant_id = Column(Integer, ForeignKey("merchants.id"))
    bot_id = Column(Integer, ForeignKey("bots.id"))
    card_id = Column(String(16), nullable=False) # Уточните длину, если не 16
    amount = Column(Float, nullable=False)
    status = Column(String, default="pending")  # pending, paid, failed
    created_at = Column(DateTime(timezone=True), default=msk_now) # Используем вашу функцию
    updated_at = Column(DateTime(timezone=True), default=msk_now, onupdate=msk_now) # Используем вашу функцию

    # --- НОВОЕ ПОЛЕ ---
    payassist_order_id = Column(String, nullable=True) # Храним order_id от PayAssist API

    bot = relationship("Bot", back_populates="orders")
    merchant = relationship("Merchant", back_populates="orders")



class ChildBotError(Base):
    __tablename__ = 'child_bot_errors'

    id = Column(Integer, primary_key=True, index=True)
    bot_uuid = Column(String, nullable=False, index=True) # UUID дочернего бота
    error_message = Column(Text, nullable=False) # Текст ошибки
    error_level = Column(String, default='ERROR') # Уровень лога (ERROR, CRITICAL)
    timestamp = Column(DateTime(timezone=True), default=msk_now) # Время возникновения ошибки
    processed = Column(Boolean, default=False, index=True) # Отправлено в канал или нет