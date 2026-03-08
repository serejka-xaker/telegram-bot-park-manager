# database/crud_async.py (обновлённый)

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete, update
from .models import Admin, Bot, Merchant, AccessRequest, Order, ChildBotError
import uuid
from datetime import timedelta
from .models import msk_now

# ----------------- Admin -----------------
async def add_bot(db: AsyncSession, token: str, name: str = None, description: str = None, uuid: str = None, payassist_api_key: str = None):
    bot = Bot(token=token, name=name, description=description, uuid=uuid or str(uuid.uuid4()), payassist_api_key=payassist_api_key)
    db.add(bot)
    await db.commit()
    await db.refresh(bot)
    return bot

async def add_admin(db: AsyncSession, telegram_id: int, username: str = None, full_name: str = None):
    admin = Admin(telegram_id=telegram_id, username=username, full_name=full_name)
    db.add(admin)
    await db.commit()
    await db.refresh(admin)
    return admin

async def get_admin(db: AsyncSession, telegram_id: int):
    result = await db.execute(select(Admin).filter(Admin.telegram_id == telegram_id))
    return result.scalar_one_or_none()

async def get_all_admins(db: AsyncSession):
    result = await db.execute(select(Admin))
    return result.scalars().all()

async def get_bot_by_uuid(db: AsyncSession, bot_uuid: str):
    result = await db.execute(select(Bot).filter(Bot.uuid == bot_uuid))
    return result.scalar_one_or_none()

# ----------------- Merchant -----------------
async def add_merchant(db: AsyncSession, telegram_id: int, bot_id: int, username: str = None, full_name: str = None):
    merchant = Merchant(telegram_id=telegram_id, bot_id=bot_id, username=username, full_name=full_name)
    db.add(merchant)
    await db.commit()
    await db.refresh(merchant)
    return merchant

async def get_merchant(db: AsyncSession, telegram_id: int, bot_id: int):
    """Получить конкретного мерчанта для конкретного бота."""
    result = await db.execute(
        select(Merchant).filter(
            Merchant.telegram_id == telegram_id,
            Merchant.bot_id == bot_id
        )
    )
    return result.scalar_one_or_none()

async def get_merchants_for_bot(db: AsyncSession, bot_id: int):
    """Получить всех мерчантов для конкретного бота."""
    result = await db.execute(select(Merchant).filter(Merchant.bot_id == bot_id))
    return result.scalars().all()

async def delete_merchant(db: AsyncSession, telegram_id: int, bot_id: int):
    """Удалить конкретного мерчанта из конкретного бота."""
    result = await db.execute(
        select(Merchant).filter(
            Merchant.telegram_id == telegram_id,
            Merchant.bot_id == bot_id
        )
    )
    merchant = result.scalar_one_or_none()
    if merchant:
        await db.delete(merchant)
        await db.commit()
        return True
    return False

# ----------------- Access Request -----------------
async def add_access_request(db: AsyncSession, telegram_id: int, bot_id: int, username: str = None, full_name: str = None):
    req = AccessRequest(telegram_id=telegram_id, bot_id=bot_id, username=username, full_name=full_name)
    db.add(req)
    await db.commit()
    await db.refresh(req)
    return req

async def get_access_request(db: AsyncSession, request_id: int):
    """Получить конкретную заявку по ID."""
    result = await db.execute(select(AccessRequest).filter(AccessRequest.id == request_id))
    return result.scalar_one_or_none()

async def approve_request(db: AsyncSession, request_id: int):
    """
    Асинхронно одобряет заявку на доступ.
    Просто обновляет статус 'approved' на True.
    Возвращает True, если заявка была найдена и обновлена, иначе False.
    """
    # Используем update для одобрения заявки
    stmt = update(AccessRequest).where(AccessRequest.id == request_id).values(approved=True)
    result = await db.execute(stmt)

    # Проверяем, сколько строк было затронуто
    if result.rowcount > 0:
        # Заявка была найдена и обновлена
        await db.commit() # Фиксируем изменения
        return True
    else:
        # Заявка не была найдена
        await db.rollback() # Хотя rollback не обязателен, если не было изменений
        return False

async def reject_request(db: AsyncSession, request_id: int):
    # Используем delete для отклонения заявки
    result = await db.execute(select(AccessRequest).filter(AccessRequest.id == request_id))
    req = result.scalar_one_or_none()
    if req:
        await db.delete(req)
        await db.commit()
    return req # Возвращаем удалённую запись или None

async def get_access_requests_for_bot(db: AsyncSession, bot_id: int):
    """Получить все НЕОДОБРЕННЫЕ заявки для конкретного бота."""
    result = await db.execute(
        select(AccessRequest).filter(
            AccessRequest.bot_id == bot_id,
            AccessRequest.approved == False
        )
    )
    return result.scalars().all()

# ----------------- Orders -----------------
async def create_order(db: AsyncSession, merchant_id: int, bot_id: int, card_id: str, amount: float, payassist_order_id: str = None):
    order = Order(
        merchant_id=merchant_id,
        bot_id=bot_id,
        card_id=card_id,
        amount=amount,
        payassist_order_id=payassist_order_id
    )
    db.add(order)
    await db.commit()
    await db.refresh(order)
    return order

async def get_order_by_payassist_id(db: AsyncSession, payassist_order_id: str):
    """Получает заказ из базы по PayAssist order_id."""
    result = await db.execute(select(Order).filter(Order.payassist_order_id == payassist_order_id))
    return result.scalar_one_or_none()

async def update_order_status(db: AsyncSession, order_id: int, status: str, payassist_order_id: str = None):
    # Используем update для изменения статуса
    stmt = update(Order).where(Order.id == order_id).values(status=status)
    if payassist_order_id:
        stmt = stmt.values(payassist_order_id=payassist_order_id)
    await db.execute(stmt)
    await db.commit()
    # После commit получаем обновленную запись
    result = await db.execute(select(Order).filter(Order.id == order_id))
    return result.scalar_one_or_none()

async def get_bots(db: AsyncSession):
    result = await db.execute(select(Bot))
    return result.scalars().all()

# ----------------- Access Request (дубликаты) -----------------
# Удаляем дублирующиеся функции, оставляем только асинхронные версии выше.

# ----------------- Orders (дубликаты) -----------------
# Удаляем дублирующиеся функции, оставляем только асинхронные версии выше.

# Получение заявок на доступ для конкретного бота (или всех)
async def get_access_requests(db: AsyncSession, bot_id: int = None):
    query = select(AccessRequest).filter(AccessRequest.approved == False)
    if bot_id:
        query = query.filter(AccessRequest.bot_id == bot_id)
    result = await db.execute(query)
    return result.scalars().all()

# Отклонение заявки
# Уже определена как reject_request

# Получение мерчантов для конкретного бота
# Уже определена как get_merchants_for_bot

async def add_child_bot_error(db: AsyncSession, bot_uuid: str, error_message: str, error_level: str = 'ERROR'):
    """Добавляет новую запись об ошибке дочернего бота."""
    db_error = ChildBotError(
        bot_uuid=bot_uuid,
        error_message=error_message,
        error_level=error_level
    )
    db.add(db_error)
    # Не вызываем commit здесь, если планируем вызывать его отдельно в месте вызова
    # await db.commit() # <-- Убираем, если commit делается в bot.py
    # await db.refresh(db_error) # Обычно не нужно для логов
    return db_error

async def get_unprocessed_errors(db: AsyncSession, limit: int = 100):
    """Получает непрочитанные ошибки, отсортированные по времени."""
    result = await db.execute(
        select(ChildBotError)
        .filter(ChildBotError.processed == False)
        .order_by(ChildBotError.timestamp.asc())
        .limit(limit)
    )
    return result.scalars().all()

async def mark_error_as_processed(db: AsyncSession, error_id: int) -> bool:
    """Отмечает конкретную ошибку как обработанную (отправленную).
    Возвращает True, если запись была найдена и обновлена, иначе False."""
    stmt = update(ChildBotError).where(ChildBotError.id == error_id).values(processed=True)
    result = await db.execute(stmt)
    await db.commit()
    # Возвращаем True, если одна строка была обновлена, иначе False
    return result.rowcount > 0

async def delete_old_errors(db: AsyncSession, retention_days: int = 7):
    """
    Удаляет ошибки дочерних ботов, которые старше retention_days дней.
    Возвращает количество удаленных записей.
    """
    cutoff_date = msk_now() - timedelta(days=retention_days)
    stmt = delete(ChildBotError).where(ChildBotError.timestamp < cutoff_date)
    result = await db.execute(stmt)
    await db.commit()
    return result.rowcount
