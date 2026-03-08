# database/database_sync.py
import os
import time
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import OperationalError
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST", "konstruktor_postgres")
DB_PORT = os.getenv("POSTGRES_PORT", 5432)
DB_NAME = os.getenv("POSTGRES_DB")

# Используем синхронный URL для подключения
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def wait_for_postgres(timeout=30):
    """Ждём, пока PostgreSQL начнёт принимать соединения (синхронно, для инициализации)"""
    engine = create_engine(
        DATABASE_URL,
        pool_pre_ping=True
    )
    start = time.time()
    while True:
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("✅ PostgreSQL (sync) готов к подключению!")
            break
        except OperationalError:
            if time.time() - start > timeout:
                raise TimeoutError("❌ Не удалось подключиться к PostgreSQL (sync) за отведённое время")
            print("⏳ Ждём PostgreSQL (sync)...")
            time.sleep(1)
    return engine

# Ждём PostgreSQL перед созданием engine и сессий
engine = wait_for_postgres()

# Создаём сессию для синхронной работы
SessionLocalSync = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base для синхронной БД (может быть та же, что и в database_async, но для ясности определим отдельно)
Base = declarative_base()

def get_sync_db():
    """Генератор для получения сессии базы данных (синхронной)"""
    db = SessionLocalSync()
    try:
        yield db
    finally:
        db.close()