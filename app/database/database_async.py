# database/database_async.py
import os
import time
# --- ИМПОРТ СИНХРОННОГО ENGINE ДЛЯ ПРОВЕРКИ ---
from sqlalchemy import create_engine, text
# --- ИМПОРТ АСИНХРОННОГО ENGINE ДЛЯ ОСНОВНОГО ИСПОЛЬЗОВАНИЯ ---
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import NullPool  # <--- ВАЖНЫЙ ИМПОРТ
from dotenv import load_dotenv

# --- НОВОЕ: Импорт для Base ---
from sqlalchemy.orm import declarative_base

load_dotenv()

DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST", "konstruktor_postgres") # Убедитесь, что имя хоста верно
DB_PORT = os.getenv("POSTGRES_PORT", 5432)
DB_NAME = os.getenv("POSTGRES_DB")

# Используем асинхронный URL для подключения
DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def wait_for_postgres(timeout=30):
    """Ждём, пока PostgreSQL начнёт принимать соединения (синхронно, для инициализации)"""
    # Для проверки подключения используем СИНХРОННЫЙ драйвер и СИНХРОННЫЙ engine
    sync_engine = create_engine( # <-- Используем create_engine, а не create_async_engine
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
        poolclass=NullPool, # Используем NullPool и здесь тоже, если хотите
        pool_pre_ping=True
    )
    start = time.time()
    while True:
        try:
            with sync_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("✅ PostgreSQL готов к подключению!")
            break
        except OperationalError:
            if time.time() - start > timeout:
                raise TimeoutError("❌ Не удалось подключиться к PostgreSQL за отведённое время")
            print("⏳ Ждём PostgreSQL...")
            time.sleep(1)
    # Теперь возвращаем АСИНХРОННЫЙ engine для основного использования
    return create_async_engine(
        DATABASE_URL,
        poolclass=NullPool, # Важно для множества процессов
        pool_pre_ping=True # Проверка соединения
    )

# Ждём PostgreSQL перед созданием engine и сессий
engine = wait_for_postgres()

# --- НОВОЕ: Создание Base ---
Base = declarative_base()

# Создаем асинхронную фабрику сессий
AsyncSessionLocal = async_sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
    class_=AsyncSession
)

async def get_async_db():
    """Асинхронный генератор для получения сессии базы данных"""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()