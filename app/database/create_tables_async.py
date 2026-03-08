# database/create_tables_async.py
from .database_async import Base, engine
from . import models # Импортируем models, чтобы Base "знал" о таблицах
import asyncio

async def init_db_async():
    """
    Асинхронная функция для создания таблиц в базе данных.
    """
    async with engine.begin() as conn:
        # Для инициализации (создания) таблиц используем run_sync
        # Это связано с тем, что create_all не имеет асинхронного аналога в SQLAlchemy 2.x
        # и его нужно выполнить в синхронном контексте внутри асинхронного подключения.
        await conn.run_sync(Base.metadata.create_all)
    print("✅ Все таблицы созданы асинхронно!")