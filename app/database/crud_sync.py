# database/crud_sync.py
from sqlalchemy.orm import Session
from .models import ChildBotError # Импортируем только модели, которые нужны для логирования
import uuid
from datetime import timedelta
from .models import msk_now

# --- СИНХРОННАЯ ФУНКЦИЯ ДЛЯ ЗАПИСИ ОШИБКИ ---
def add_child_bot_error_sync(db: Session, bot_uuid: str, error_message: str, error_level: str = 'ERROR'):
    """Добавляет новую запись об ошибке дочернего бота (синхронная версия)."""
    db_error = ChildBotError(
        bot_uuid=bot_uuid,
        error_message=error_message,
        error_level=error_level
    )
    db.add(db_error)
    # db.commit() и db.close() будут вызваны в месте вызова
    return db_error

# Можно добавить и другие CRUD-функции, если они понадобятся в синхронном потоке
# ...