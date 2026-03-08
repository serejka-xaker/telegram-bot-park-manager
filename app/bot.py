# bot.py
import uuid
from database.create_tables_async import init_db_async
from datetime import datetime
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from database.database_async import get_async_db, AsyncSessionLocal
from database.crud_async import add_bot, get_bots, get_access_requests, approve_request, reject_request, get_access_requests_for_bot, get_access_request, add_merchant, get_merchants_for_bot, delete_merchant
from database.crud_async import add_admin, get_admin, get_all_admins
from database.crud_async import add_child_bot_error, get_unprocessed_errors, mark_error_as_processed, delete_old_errors # для добавления новых админов
from database.models import Bot as BotModel
from database.models import Admin as AdminModel, Merchant
from dotenv import load_dotenv
import os
import sys
import asyncio
from logging import Handler, LogRecord
import logging
import subprocess
from typing import Dict
import html # Для экранирования HTML
from aiogram.exceptions import TelegramBadRequest # Для обработки ошибок Telegram API
from datetime import datetime, timezone, timedelta
import threading
import queue
import atexit
import signal
from sqlalchemy import select
from database.database_sync import get_sync_db, SessionLocalSync
from database.crud_sync import add_child_bot_error_sync
from aiohttp import ClientTimeout
from aiohttp import ClientConnectorError
from aiogram.exceptions import TelegramRetryAfter


class DBErrorHandler(Handler):
    """
    Кастомный логгинг-хендлер для записи ошибок основного бота в базу данных.
    Использует СИНХРОННУЮ сессию в отдельном потоке.
    """
    def __init__(self, bot_uuid: str = 'main_bot'):
        super().__init__()
        self.bot_uuid = bot_uuid
        # Используем потокобезопасную очередь для асинхронной записи ошибок
        self.queue = queue.Queue()
        self._stop_event = threading.Event()
        # Запускаем фоновый поток для обработки очереди
        self._thread = threading.Thread(target=self._process_queue, daemon=True)
        self._thread.start()
        # Устанавливаем уровень для этого обработчика
        self.setLevel(logging.ERROR) # Ловим ошибки уровня ERROR и выше
    def emit(self, record: LogRecord):
        """
        Вызывается при логировании ошибки.
        Добавляет запись в очередь для асинхронной обработки.
        """
        # Форматируем сообщение ошибки
        msg = self.format(record)
        # Помещаем в очередь
        self.queue.put((record.levelname, msg))
    def _process_queue(self):
        """
        Фоновый поток: обрабатывает очередь и записывает ошибки в БД с помощью СИНХРОННОЙ сессии.
        """
        while not self._stop_event.is_set():
            try:
                # Ждем элемент из очереди с таймаутом
                levelname, msg = self.queue.get(timeout=1)
                if levelname.upper() in ['ERROR', 'CRITICAL']: # Обрабатываем только ERROR и CRITICAL
                    # Создаем СИНХРОННУЮ сессию внутри потока
                    db = SessionLocalSync() # <-- Используем синхронную фабрику
                    try:
                        # Записываем ошибку в БД с фиктивным UUID 'main_bot'
                        add_child_bot_error_sync(db, self.bot_uuid, msg, levelname.upper()) # <-- Вызов синхронной функции
                        db.commit() # <-- commit для синхронной сессии
                        # logging.debug(f"Logged error for {self.bot_uuid} to DB: {msg}") # Для отладки
                    except Exception as e:
                        # Если не удалось записать в БД, выводим в консоль
                        print(f"CRITICAL: Could not log error '{levelname}' for {self.bot_uuid} to DB: {e}")
                        print(f"Original message: {msg}")
                        db.rollback() # <-- rollback для синхронной сессии
                    finally:
                        db.close() # <-- close для синхронной сессии
                    # Сообщаем очереди, что задача выполнена
                    self.queue.task_done()
            except queue.Empty:
                # Таймаут прошел, продолжаем цикл
                continue
            except Exception as e:
                # Неожиданная ошибка в обработчике очереди
                print(f"CRITICAL: Error in DBErrorHandler queue processing: {e}")
    def close(self):
        """
        Останавливает фоновый поток.
        """
        self._stop_event.set()
        # Ждем завершения потока
        if self._thread.is_alive():
            self._thread.join()
        super().close()
# --- КОНЕЦ НОВОЕ ---
# --- Добавление DBErrorHandler к корневому логгеру ---
# Сначала настраиваем форматтер, как в дочернем боте
main_bot_formatter = logging.Formatter(
    fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
# Создаем и настраиваем DBErrorHandler
db_error_handler = DBErrorHandler(bot_uuid='main_bot') # Используем 'main_bot' как идентификатор
db_error_handler.setFormatter(main_bot_formatter) # Устанавливаем форматтер
db_error_handler.setLevel(logging.ERROR) # Уровень обработчика
# Получаем корневый логгер и добавляем к нему наш обработчик
root_logger = logging.getLogger()
root_logger.addHandler(db_error_handler)
# Устанавливаем уровень корневого логгера, чтобы ошибки доходили до обработчика
root_logger.setLevel(logging.DEBUG)
# --- Константы ---
BOTS_PER_PAGE = 5  # Количество ботов на одной странице
ADMINS_PER_PAGE = 5 # Количество админов на одной странице
ERROR_CHECK_INTERVAL = 60

class CreateBotStates(StatesGroup):
    waiting_for_token = State()
    waiting_for_name = State()
    waiting_for_description = State()
    waiting_for_payassist_api_key = State()

class AdminStates(StatesGroup):
    waiting_for_admin_telegram_id = State()
    waiting_for_admin_details = State() 
    # Добавим второе состояние
class EditAdminStates(StatesGroup):
    waiting_for_edit_telegram_id = State()
    waiting_for_edit_details = State()
# --- НОВОЕ СОСТОЯНИЕ ---

class CancelCreateBotStates(StatesGroup):
    # Это состояние не используется напрямую, но может быть полезно для отслеживания
    # или очистки при отмене из любого шага создания бота.
    # В данном случае, мы просто очищаем CreateBotStates.
    pass

class BotPanelStates(StatesGroup):
    viewing_requests = State()
    viewing_merchants = State()

class BotManager:
    def __init__(self):
        self.bots: Dict[str, subprocess.Popen] = {}
    def start_bot(self, bot_uuid: str, token: str, payassist_api_key: str):
        if bot_uuid in self.bots:
            return False
        # Передаём bot_uuid как аргумент
        process = subprocess.Popen(
            [sys.executable, "/app/child_bot.py", "--token", token, "--payassist-api-key", payassist_api_key, "--bot-uuid", bot_uuid],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        self.bots[bot_uuid] = process
        return True
    def stop_bot(self, bot_uuid: str):
        process = self.bots.get(bot_uuid)
        if not process:
            return False
        process.terminate()
        process.wait()
        del self.bots[bot_uuid]
        return True
    def restart_bot(self, bot_uuid: str, token: str, payassist_api_key: str):
        self.stop_bot(bot_uuid)
        return self.start_bot(bot_uuid, token, payassist_api_key)
    def stop_all(self):
        """Останавливает всех запущенных ботов."""
        for uuid, process in list(self.bots.items()):
            logging.info(f"Stopping bot {uuid} on shutdown...")
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
        self.bots.clear()

bot_manager = BotManager()

load_dotenv()

API_TOKEN = os.getenv("TOKEN")  # токен главного бота
ADMIN_TG_ID = int(os.getenv("ADMIN_TG_ID"))
ERROR_CHANNEL_CHAT_ID = os.getenv("ERROR_CHANNEL_CHAT_ID")

bot = Bot(token=API_TOKEN,timeout=ClientTimeout(total=30))
dp = Dispatcher()
# --- Вспомогательные клавиатуры ---
def admin_menu_kb():
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🤖 Список дочерних ботов", callback_data="list_bots_page_0")],
            [InlineKeyboardButton(text="👤 Список администраторов", callback_data="list_admins_page_0")], # Добавлена кнопка списка админов
            [InlineKeyboardButton(text="➕ Добавить администратора", callback_data="add_administrator")],
            [InlineKeyboardButton(text="⚙️ Создать дочернего бота", callback_data="create_bot")],
        ]
    )
    return kb
# --- Клавиатура для управления конкретным ботом ---
def bot_control_kb(bot_uuid: str):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="▶️ Запустить", callback_data=f"start_{bot_uuid}"),
                InlineKeyboardButton(text="⏹️ Остановить", callback_data=f"stop_{bot_uuid}"),
                InlineKeyboardButton(text="🔄 Перезапустить", callback_data=f"restart_{bot_uuid}"),
            ],
            [InlineKeyboardButton(text="🗑️ Удалить", callback_data=f"delete_confirm_{bot_uuid}")],
            [InlineKeyboardButton(text="📋 Панель управления", callback_data=f"bot_panel_{bot_uuid}")], # Новая кнопка
            [InlineKeyboardButton(text="◀️ Назад к списку ботов", callback_data="list_bots_page_0")],
        ]
    )
    return kb
def bot_panel_kb(bot_uuid: str):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📋 Заявки на доступ", callback_data=f"bot_requests_{bot_uuid}_0")], # Страница 0
            [InlineKeyboardButton(text="👥 Подключённые мерчанты", callback_data=f"bot_merchants_{bot_uuid}_0")], # Страница 0
            [InlineKeyboardButton(text="◀️ Назад к боту", callback_data=f"view_bot_{bot_uuid}")],
        ]
    )
    return kb
def request_navigation_kb(bot_uuid: str, current_index: int, total_requests: int):
    keyboard = []
    nav_buttons = []
    if total_requests > 1:
        if current_index > 0:
            nav_buttons.append(InlineKeyboardButton(text="⬅️", callback_data=f"req_prev_{bot_uuid}_{current_index}"))
        if current_index < total_requests - 1:
            nav_buttons.append(InlineKeyboardButton(text="➡️", callback_data=f"req_next_{bot_uuid}_{current_index}"))
        if nav_buttons:
            keyboard.append(nav_buttons)
    # Кнопки действий для текущей заявки
    if total_requests > 0:
        keyboard.append([
            InlineKeyboardButton(text="✅ Одобрить", callback_data=f"approve_req_{current_index}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_req_{current_index}")
        ])
    # Кнопка "Назад"
    keyboard.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"bot_panel_{bot_uuid}")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)
def merchant_navigation_kb(bot_uuid: str, current_index: int, total_merchants: int):
    keyboard = []
    nav_buttons = []
    if total_merchants > 1:
        if current_index > 0:
            nav_buttons.append(InlineKeyboardButton(text="⬅️", callback_data=f"mch_prev_{bot_uuid}_{current_index}"))
        if current_index < total_merchants - 1:
            nav_buttons.append(InlineKeyboardButton(text="➡️", callback_data=f"mch_next_{bot_uuid}_{current_index}"))
        if nav_buttons:
            keyboard.append(nav_buttons)
    # Кнопка "Удалить" для текущего мерчанта
    if total_merchants > 0:
        keyboard.append([InlineKeyboardButton(text="🗑️ Удалить", callback_data=f"delete_mch_{current_index}")])
    # Кнопка "Назад"
    keyboard.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"bot_panel_{bot_uuid}")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)
def delete_merchant_confirmation_kb(merchant_db_id: int):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            # callback_data теперь содержит merchant_db_id
            [InlineKeyboardButton(text="❌ Отмена", callback_data=f"mch_del_cancel_{merchant_db_id}")],
            [InlineKeyboardButton(text="🗑️ Подтвердить", callback_data=f"mch_del_confirmed_{merchant_db_id}")],
        ]
    )
    return kb
# --- Клавиатура для подтверждения удаления ---
def delete_confirmation_kb(bot_uuid: str):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data=f"delete_cancel_{bot_uuid}")],
            [InlineKeyboardButton(text="🗑️ Подтвердить удаление", callback_data=f"delete_confirmed_{bot_uuid}")],
            [InlineKeyboardButton(text="◀️ Назад к списку ботов", callback_data="list_bots_page_0")], # Добавляем кнопку "Назад"
        ]
    )
    return kb
# --- Клавиатура для списка ботов с пагинацией ---
def bot_list_kb(bots, page_num, total_pages):
    keyboard = []
    start_index = page_num * BOTS_PER_PAGE
    end_index = start_index + BOTS_PER_PAGE
    page_bots = bots[start_index:end_index]
    for b in page_bots:
        status = "🟢 Запущен" if b.uuid in bot_manager.bots else "🔴 Остановлен"
        keyboard.append([InlineKeyboardButton(text=f"{html.escape(b.name)} | {status}", callback_data=f"view_bot_{b.uuid}")])
    nav_buttons = []
    if page_num > 0:
        nav_buttons.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"list_bots_page_{page_num - 1}"))
    if page_num < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton(text="Вперед ➡️", callback_data=f"list_bots_page_{page_num + 1}"))
    if nav_buttons:
        keyboard.append(nav_buttons)
    keyboard.append([InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)
# --- Клавиатура для списка администраторов с пагинацией ---
def admin_list_kb(admins, page_num, total_pages):
    keyboard = []
    start_index = page_num * ADMINS_PER_PAGE
    end_index = start_index + ADMINS_PER_PAGE
    page_admins = admins[start_index:end_index]
    for a in page_admins:
        # Отображаем username или ID, если username отсутствует
        display_name = a.username or str(a.telegram_id)
        keyboard.append([InlineKeyboardButton(text=html.escape(display_name), callback_data=f"view_admin_{a.telegram_id}")])
    nav_buttons = []
    if page_num > 0:
        nav_buttons.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"list_admins_page_{page_num - 1}"))
    if page_num < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton(text="Вперед ➡️", callback_data=f"list_admins_page_{page_num + 1}"))
    if nav_buttons:
        keyboard.append(nav_buttons)
    keyboard.append([InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)
# --- Клавиатура для управления конкретным администратором ---
def admin_control_kb(admin_telegram_id: int):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"edit_admin_{admin_telegram_id}")],
            [InlineKeyboardButton(text="🗑️ Удалить", callback_data=f"delete_admin_confirm_{admin_telegram_id}")],
            [InlineKeyboardButton(text="◀️ Назад к списку админов", callback_data="list_admins_page_0")], # Добавляем кнопку "Назад"
        ]
    )
    return kb
# --- Клавиатура для подтверждения удаления администратора ---
def delete_admin_confirmation_kb(admin_telegram_id: int):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data=f"delete_admin_cancel_{admin_telegram_id}")],
            [InlineKeyboardButton(text="🗑️ Подтвердить удаление", callback_data=f"delete_admin_confirmed_{admin_telegram_id}")],
            [InlineKeyboardButton(text="◀️ Назад к списку админов", callback_data="list_admins_page_0")], # Добавляем кнопку "Назад"
        ]
    )
    return kb
def access_request_kb(request_id: int):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Одобрить", callback_data=f"approve_{request_id}"),
                InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_{request_id}")
            ]
        ]
    )
    return kb
# Создаём фиксированный часовой пояс для Москвы (UTC+3)
MSK_TZ = timezone(timedelta(hours=3))
def msk_now():
    """Возвращает текущее время по Московскому времени (MSK, UTC+3) с aware datetime"""
    return datetime.now(MSK_TZ)

async def _update_request_message(bot_uuid: str, current_index: int, state: FSMContext, query_or_message_context):
    """
    Вспомогательная функция для обновления сообщения с заявками.
    Принимает контекст (CallbackQuery или Message), чтобы корректно вызвать edit_text.
    """
    async for db in get_async_db():
        try:
            result = await db.execute(select(BotModel).filter(BotModel.uuid == bot_uuid))
            bot_data = result.scalar_one_or_none()
            if not bot_data:
                text = f"❌ Бот с UUID <code>{bot_uuid}</code> не найден."
                reply_markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="list_bots_page_0")]])
                if hasattr(query_or_message_context, 'message'): # CallbackQuery
                    await query_or_message_context.message.edit_text(text, reply_markup=reply_markup)
                else: # Message (например, для отправки нового сообщения при ошибке)
                    await query_or_message_context.edit_text(text, reply_markup=reply_markup)
                return

            # --- СОХРАНЯЕМ АТРИБУТЫ БОТА ---
            bot_name = bot_data.name
            # ---

            # Получаем список заявок из состояния FSM
            data = await state.get_data()
            requests_ids = data.get('requests_list', [])
            if not requests_ids:
                # Если список пуст, возможно, заявки были одобрены/отклонены другим админом
                text = f"Нет больше нерассмотренных заявок для бота '<code>{html.escape(bot_name)}</code>'."
                reply_markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data=f"bot_panel_{bot_uuid}")]])
                if hasattr(query_or_message_context, 'message'): # CallbackQuery
                    await query_or_message_context.message.edit_text(text, parse_mode='HTML', reply_markup=reply_markup)
                else: # Message
                    await query_or_message_context.edit_text(text, parse_mode='HTML', reply_markup=reply_markup)
                return

            current_index = max(0, min(current_index, len(requests_ids) - 1))
            request_id = requests_ids[current_index]
            current_request = await get_access_request(db, request_id)
            if not current_request:
                # Заявка могла быть одобрена/удалена другим админом между вызовами
                # Попробуем обновить список и перейти к следующей
                requests_from_db = await get_access_requests_for_bot(db, bot_data.id)
                updated_requests_ids = [r.id for r in requests_from_db]
                if not updated_requests_ids:
                     text = f"Нет больше нерассмотренных заявок для бота '<code>{html.escape(bot_name)}</code>'."
                     reply_markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data=f"bot_panel_{bot_uuid}")]])
                     if hasattr(query_or_message_context, 'message'): # CallbackQuery
                         await query_or_message_context.message.edit_text(text, parse_mode='HTML', reply_markup=reply_markup)
                     else: # Message
                         await query_or_message_context.edit_text(text, parse_mode='HTML', reply_markup=reply_markup)
                     return
                # Обновляем индекс и список в состоянии
                new_index = min(current_index, len(updated_requests_ids) - 1)
                await state.update_data(requests_list=updated_requests_ids, current_request_index=new_index)
                # Рекурсивно вызываем для обновления с новыми данными
                # (Важно: избегайте бесконечной рекурсии, добавив лимит вызовов при необходимости)
                await _update_request_message(bot_uuid, new_index, state, query_or_message_context)
                return

            # --- СОХРАНЯЕМ АТРИБУТЫ ЗАЯВКИ ---
            req_telegram_id = current_request.telegram_id
            req_username = current_request.username
            req_full_name = current_request.full_name
            req_created_at_str = current_request.created_at.strftime('%Y-%m-%d %H:%M:%S')
            # ---

            details_text = (
                f"<b>Заявка #{current_index + 1} из {len(requests_ids)}:</b>\n"
                f"<b>Telegram ID:</b> <code>{req_telegram_id}</code>\n"
                f"<b>Username:</b> <code>{html.escape(req_username or 'N/A')}</code>\n"
                f"<b>Имя:</b> <code>{html.escape(req_full_name or 'N/A')}</code>\n"
                f"<b>Время запроса:</b> <code>{req_created_at_str}</code>\n"
                f"<b>Бот:</b> <code>{html.escape(bot_name)}</code>"
            )
            reply_markup = request_navigation_kb(bot_uuid, current_index, len(requests_ids))
            if hasattr(query_or_message_context, 'message'): # CallbackQuery
                await query_or_message_context.message.edit_text(details_text, parse_mode='HTML', reply_markup=reply_markup)
            else: # Message
                await query_or_message_context.edit_text(details_text, parse_mode='HTML', reply_markup=reply_markup)
        except Exception as e:
            logging.error(f"Error in _update_request_message for bot {bot_uuid}, index {current_index}: {e}")
            text = "❌ Произошла ошибка при обновлении списка заявок."
            reply_markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")]])
            if hasattr(query_or_message_context, 'message'): # CallbackQuery
                await query_or_message_context.message.edit_text(text, reply_markup=reply_markup)
            else: # Message
                await query_or_message_context.edit_text(text, reply_markup=reply_markup)

@dp.callback_query(F.data.startswith("bot_panel_"))
async def bot_panel_start(query: CallbackQuery):
    bot_uuid = query.data.split('_', 2)[2]
    await query.message.edit_text(f"Панель управления ботом <code>{bot_uuid}</code>:", parse_mode='HTML', reply_markup=bot_panel_kb(bot_uuid))
    await query.answer()

@dp.callback_query(F.data.startswith("bot_requests_"))
async def bot_requests_start(query: CallbackQuery, state: FSMContext):
    parts = query.data.split('_')
    if len(parts) < 4:
        await query.message.edit_text("Ошибка: Неверные данные запроса.")
        await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")]]))
        await query.answer()
        return
    bot_uuid = parts[2]
    page_num = int(parts[3]) # page_num теперь будет 0, если вызывается из bot_panel_start
    async for db in get_async_db():
        try:
            result = await db.execute(select(BotModel).filter(BotModel.uuid == bot_uuid))
            bot_data = result.scalar_one_or_none()
            if not bot_data:
                await query.message.edit_text(f"❌ Бот с UUID <code>{bot_uuid}</code> не найден.", parse_mode='HTML')
                await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="list_bots_page_0")]]))
                await query.answer()
                return

            # --- СОХРАНЯЕМ АТРИБУТЫ БОТА ---
            bot_name = bot_data.name
            # ---

            requests = await get_access_requests_for_bot(db, bot_data.id)
            total_requests = len(requests)
            if total_requests == 0:
                await query.message.edit_text(f"Нет нерассмотренных заявок для бота '<code>{html.escape(bot_name)}</code>'.", parse_mode='HTML')
                await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data=f"bot_panel_{bot_uuid}")]]))
                await query.answer()
                return

            # current_index = max(0, min(page_num, total_requests - 1)) # page_num использовался как индекс, теперь используем 0 при первом входе
            current_index = 0 # При первом входе всегда показываем первую заявку
            requests_ids = [r.id for r in requests] # Сохраняем список ID заявок
            current_request_id = requests_ids[current_index]
            current_request = await get_access_request(db, current_request_id) # Получаем заявку по ID
            if not current_request:
                # Не должно произойти, если get_access_requests_for_bot возвращает актуальные данные
                await query.message.edit_text("❌ Заявка не найдена.")
                await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data=f"bot_panel_{bot_uuid}")]]))
                await query.answer()
                return

            # --- СОХРАНЯЕМ АТРИБУТЫ ЗАЯВКИ ---
            req_telegram_id = current_request.telegram_id
            req_username = current_request.username
            req_full_name = current_request.full_name
            req_created_at_str = current_request.created_at.strftime('%Y-%m-%d %H:%M:%S')
            # ---

            details_text = (
                f"<b>Заявка #{current_index + 1} из {len(requests_ids)}:</b>\n"
                f"<b>Telegram ID:</b> <code>{req_telegram_id}</code>\n"
                f"<b>Username:</b> <code>{html.escape(req_username or 'N/A')}</code>\n"
                f"<b>Имя:</b> <code>{html.escape(req_full_name or 'N/A')}</code>\n"
                f"<b>Время запроса:</b> <code>{req_created_at_str}</code>\n"
                f"<b>Бот:</b> <code>{html.escape(bot_name)}</code>"
            )
            reply_markup = request_navigation_kb(bot_uuid, current_index, total_requests)
            await state.update_data(bot_uuid=bot_uuid, requests_list=requests_ids, current_request_index=current_index)
            await query.message.edit_text(details_text, parse_mode='HTML', reply_markup=reply_markup)
            await state.set_state(BotPanelStates.viewing_requests)
        except Exception as e:
            logging.error(f"Error fetching access requests for bot {bot_uuid}: {e}")
            await query.message.edit_text("❌ Произошла ошибка при получении заявок.")
            await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")]]))

    await query.answer()

@dp.callback_query(BotPanelStates.viewing_requests, F.data.startswith("req_prev_"))
async def req_prev(query: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    bot_uuid = data['bot_uuid']
    requests_ids = data['requests_list']
    current_index = data['current_request_index']
    new_index = current_index - 1
    if new_index < 0:
        new_index = len(requests_ids) - 1 # Зацикливание
    await state.update_data(current_request_index=new_index)
    await _update_request_message(bot_uuid, new_index, state, query) # Вызов вспомогательной функции
    await query.answer()

@dp.callback_query(BotPanelStates.viewing_requests, F.data.startswith("req_next_"))
async def req_next(query: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    bot_uuid = data['bot_uuid']
    requests_ids = data['requests_list']
    current_index = data['current_request_index']
    new_index = current_index + 1
    if new_index >= len(requests_ids):
        new_index = 0 # Зацикливание
    await state.update_data(current_request_index=new_index)
    await _update_request_message(bot_uuid, new_index, state, query) # Вызов вспомогательной функции
    await query.answer()

@dp.callback_query(BotPanelStates.viewing_requests, F.data.startswith("approve_req_"))
async def approve_current_request(query: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    bot_uuid = data['bot_uuid']
    requests_ids = data['requests_list']
    current_index = data['current_request_index']
    request_id = requests_ids[current_index]
    async for db in get_async_db(): # <-- Контекст асинхронной сессии
        try:
            request = await get_access_request(db, request_id)
            if not request:
                await query.message.edit_text("❌ Заявка больше не существует.")
                await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data=f"bot_panel_{bot_uuid}")]]))
                await query.answer()
                return
            # --- НОВОЕ: Сохраняем атрибуты request ДО вызова approve_request ---
            request_bot_id = request.bot_id
            request_telegram_id = request.telegram_id
            request_username = request.username
            request_full_name = request.full_name
            # ---
            # Одобряем заявку
            approved_req = await approve_request(db, request_id)
            if not approved_req:
                await query.message.edit_text("❌ Не удалось одобрить заявку.")
                await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data=f"bot_panel_{bot_uuid}")]]))
                await query.answer()
                return
            # Получаем данные бота
            result = await db.execute(select(BotModel).filter(BotModel.id == request_bot_id)) # <-- Используем request_bot_id
            bot_data = result.scalar_one_or_none()
            if not bot_data:
                await query.message.edit_text(f"❌ Бот, связанный с заявкой, не найден.")
                await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="list_bots_page_0")]]))
                await query.answer()
                return
            # --- НОВОЕ: Сохраняем атрибуты bot_data ДО использования объекта ---
            bot_token = bot_data.token # <-- Сохраняем token до обращения к bot_data.token
            bot_name = bot_data.name   # <-- Сохраняем name для использования в сообщении
            # ---
            # Добавляем мерчанта
            await add_merchant(db, request_telegram_id, request_bot_id, request_username, request_full_name) # <-- Используем сохранённые значения
            await db.commit()
            # Отправляем сообщение мерчанту, используя сохранённый токен
            try:
                async with Bot(token=bot_token) as child_bot: # <-- Используем async with
                    welcome_message = "Добро пожаловать! Ваш запрос на доступ к боту одобрен.\nСписок доступных команд:\n/balance - Проверить баланс платежного аккаунта\n/pay [номер карты] [сумма] - Создать платежную ссылку для оплаты картой\n/paysbp [сумма] - Создать платежную ссылку для оплаты через СБП"
                    await child_bot.send_message(request_telegram_id, welcome_message, parse_mode='HTML') # <-- Использование сессии
                    logging.info(f"Welcome message sent to merchant {request_telegram_id} for bot {bot_name}.")
            except Exception as e:
                logging.error(f"Failed to send welcome message to merchant {request_telegram_id}: {e}")
            # Удаляем одобренную заявку из списка в состоянии
            updated_requests_ids = [rid for rid in requests_ids if rid != request_id]
            if not updated_requests_ids:
                await query.message.edit_text(f"✅ Заявка одобрена. Мерчант добавлен. Нет больше нерассмотренных заявок.", parse_mode='HTML')
                await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data=f"bot_panel_{bot_uuid}")]]))
            else:
                new_index = min(current_index, len(updated_requests_ids) - 1)
                await state.update_data(requests_list=updated_requests_ids, current_request_index=new_index)
                await _update_request_message(bot_uuid, new_index, state, query) # Вызов вспомогательной функции
        except Exception as e:
            logging.error(f"Error in approve_current_request for request {request_id} for bot {bot_uuid}: {e}", exc_info=True) # Добавлено exc_info=True для полного трейсбека
            await query.message.edit_text("❌ Произошла ошибка при одобрении заявки.")
            await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")]]))

    await query.answer()

@dp.callback_query(BotPanelStates.viewing_requests, F.data.startswith("reject_req_"))
async def reject_current_request(query: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    bot_uuid = data['bot_uuid']
    requests_ids = data['requests_list']
    current_index = data['current_request_index']
    request_id = requests_ids[current_index]
    async for db in get_async_db():
        try:
            request = await get_access_request(db, request_id)
            if not request:
                await query.message.edit_text("❌ Заявка больше не существует.")
                await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data=f"bot_panel_{bot_uuid}")]]))
                await query.answer()
                return
            # Отклоняем заявку
            rejected_req = await reject_request(db, request_id)
            if not rejected_req:
                await query.message.edit_text("❌ Не удалось отклонить заявку.")
                await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data=f"bot_panel_{bot_uuid}")]]))
                await query.answer()
                return
            # Удаляем отклонённую заявку из списка в состоянии
            updated_requests_ids = [rid for rid in requests_ids if rid != request_id]
            if not updated_requests_ids:
                # Если список заявок пуст, показываем сообщение и возвращаем к панели
                await query.message.edit_text(f"❌ Заявка отклонена. Нет больше нерассмотренных заявок.", parse_mode='HTML')
                await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data=f"bot_panel_{bot_uuid}")]]))
            else:
                # Обновляем список в состоянии
                # Выбираем индекс следующей заявки (тот же, если не последняя, иначе предпоследнюю)
                new_index = min(current_index, len(updated_requests_ids) - 1)
                await state.update_data(requests_list=updated_requests_ids, current_request_index=new_index)
                # Обновляем сообщение с новой заявкой
                await _update_request_message(bot_uuid, new_index, state, query) # Вызов вспомогательной функции
        except Exception as e:
            logging.error(f"Error rejecting request {request_id} for bot {bot_uuid}: {e}")
            await query.message.edit_text("❌ Произошла ошибка при отклонении заявки.")
            await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")]]))

    await query.answer()

async def _update_merchant_message(bot_uuid: str, current_index: int, state: FSMContext, query_or_message_context):
    """
    Вспомогательная функция для обновления сообщения со списком мерчантов.
    Принимает контекст (CallbackQuery или Message), чтобы корректно вызвать edit_text.
    """
    async for db in get_async_db():
        try:
            result = await db.execute(select(BotModel).filter(BotModel.uuid == bot_uuid))
            bot_data = result.scalar_one_or_none()
            if not bot_data:
                text = f"❌ Бот с UUID <code>{bot_uuid}</code> не найден."
                reply_markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="list_bots_page_0")]])
                if hasattr(query_or_message_context, 'message'): # CallbackQuery
                    await query_or_message_context.message.edit_text(text, parse_mode='HTML', reply_markup=reply_markup)
                else: # Message
                    await query_or_message_context.edit_text(text, parse_mode='HTML', reply_markup=reply_markup)
                return

            # --- СОХРАНЯЕМ АТРИБУТЫ БОТА ---
            bot_name = bot_data.name
            # ---

            # Получаем список мерчантов из состояния FSM
            data = await state.get_data()
            merchants_telegram_ids = data.get('merchants_list', []) # Список ID
            if not merchants_telegram_ids:
                text = f"Нет подключённых мерчантов для бота '<code>{html.escape(bot_name)}</code>'."
                reply_markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data=f"bot_panel_{bot_uuid}")]])
                if hasattr(query_or_message_context, 'message'): # CallbackQuery
                    await query_or_message_context.message.edit_text(text, parse_mode='HTML', reply_markup=reply_markup)
                else: # Message
                    await query_or_message_context.edit_text(text, parse_mode='HTML', reply_markup=reply_markup)
                return

            current_index = max(0, min(current_index, len(merchants_telegram_ids) - 1))
            merchant_telegram_id = merchants_telegram_ids[current_index]
            # Получаем мерчанта из базы по ID
            result = await db.execute(
                select(Merchant).filter(
                    Merchant.telegram_id == merchant_telegram_id,
                    Merchant.bot_id == bot_data.id
                )
            )
            current_merchant = result.scalar_one_or_none()
            if not current_merchant:
                # Мерчант мог быть удалён другим админом
                # Попробуем обновить список и перейти к следующему
                merchants_from_db = await get_merchants_for_bot(db, bot_data.id)
                updated_merchants_ids = [m.telegram_id for m in merchants_from_db]
                if not updated_merchants_ids:
                     text = f"Нет подключённых мерчантов для бота '<code>{html.escape(bot_name)}</code>'."
                     reply_markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data=f"bot_panel_{bot_uuid}")]])
                     if hasattr(query_or_message_context, 'message'): # CallbackQuery
                         await query_or_message_context.message.edit_text(text, parse_mode='HTML', reply_markup=reply_markup)
                     else: # Message
                         await query_or_message_context.edit_text(text, parse_mode='HTML', reply_markup=reply_markup)
                     return
                # Обновляем индекс и список в состоянии
                new_index = min(current_index, len(updated_merchants_ids) - 1)
                await state.update_data(merchants_list=updated_merchants_ids, current_merchant_index=new_index)
                # Рекурсивно вызываем для обновления с новыми данными
                await _update_merchant_message(bot_uuid, new_index, state, query_or_message_context)
                return

            # --- СОХРАНЯЕМ АТРИБУТЫ МЕРЧАНТА ---
            mch_telegram_id = current_merchant.telegram_id
            mch_username = current_merchant.username
            mch_full_name = current_merchant.full_name
            # ---

            details_text = (
                f"<b>Мерчант #{current_index + 1} из {len(merchants_telegram_ids)}:</b>\n"
                f"<b>Telegram ID:</b> <code>{mch_telegram_id}</code>\n"
                f"<b>Username:</b> <code>{html.escape(mch_username or 'N/A')}</code>\n"
                f"<b>Имя:</b> <code>{html.escape(mch_full_name or 'N/A')}</code>\n"
                f"<b>Бот:</b> <code>{html.escape(bot_name)}</code>"
            )
            reply_markup = merchant_navigation_kb(bot_uuid, current_index, len(merchants_telegram_ids))
            if hasattr(query_or_message_context, 'message'): # CallbackQuery
                await query_or_message_context.message.edit_text(details_text, parse_mode='HTML', reply_markup=reply_markup)
            else: # Message
                await query_or_message_context.edit_text(details_text, parse_mode='HTML', reply_markup=reply_markup)
        except Exception as e:
            logging.error(f"Error in _update_merchant_message for bot {bot_uuid}, index {current_index}: {e}")
            text = "❌ Произошла ошибка при обновлении списка мерчантов."
            reply_markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")]])
            if hasattr(query_or_message_context, 'message'): # CallbackQuery
                await query_or_message_context.message.edit_text(text, reply_markup=reply_markup)
            else: # Message
                await query_or_message_context.edit_text(text, reply_markup=reply_markup)

@dp.callback_query(F.data.startswith("bot_merchants_"))
async def bot_merchants_start(query: CallbackQuery, state: FSMContext):
    parts = query.data.split('_')
    if len(parts) < 4:
        await query.message.edit_text("Ошибка: Неверные данные запроса.")
        await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")]]))
        await query.answer()
        return
    bot_uuid = parts[2]
    page_num = int(parts[3]) # page_num теперь будет 0, если вызывается из bot_panel_start
    async for db in get_async_db():
        try:
            result = await db.execute(select(BotModel).filter(BotModel.uuid == bot_uuid))
            bot_data = result.scalar_one_or_none()
            if not bot_data:
                await query.message.edit_text(f"❌ Бот с UUID <code>{bot_uuid}</code> не найден.", parse_mode='HTML')
                await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="list_bots_page_0")]]))
                await query.answer()
                return

            # --- СОХРАНЯЕМ АТРИБУТЫ БОТА ---
            bot_name = bot_data.name
            # ---

            merchants = await get_merchants_for_bot(db, bot_data.id) # Используем новую функцию из CRUD
            total_merchants = len(merchants)
            if total_merchants == 0:
                await query.message.edit_text(f"Нет подключённых мерчантов для бота '<code>{html.escape(bot_name)}</code>'.", parse_mode='HTML')
                await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data=f"bot_panel_{bot_uuid}")]]))
                await query.answer()
                return

            # current_index = max(0, min(page_num, total_merchants - 1)) # page_num использовался как индекс, теперь используем 0 при первом входе
            current_index = 0 # При первом входе всегда показываем первого мерчанта
            merchants_ids = [m.telegram_id for m in merchants] # Сохраняем список ID мерчантов
            current_merchant_telegram_id = merchants_ids[current_index]
            result = await db.execute(
                select(Merchant).filter(
                    Merchant.telegram_id == current_merchant_telegram_id,
                    Merchant.bot_id == bot_data.id
                )
            )
            current_merchant = result.scalar_one_or_none()
            if not current_merchant:
                # Не должно произойти, если get_merchants_for_bot возвращает актуальные данные
                await query.message.edit_text("❌ Мерчант не найден.")
                await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data=f"bot_panel_{bot_uuid}")]]))
                await query.answer()
                return

            # --- СОХРАНЯЕМ АТРИБУТЫ МЕРЧАНТА ---
            mch_telegram_id = current_merchant.telegram_id
            mch_username = current_merchant.username
            mch_full_name = current_merchant.full_name
            # ---

            details_text = (
                f"<b>Мерчант #{current_index + 1} из {total_merchants}:</b>\n"
                f"<b>Telegram ID:</b> <code>{mch_telegram_id}</code>\n"
                f"<b>Username:</b> <code>{html.escape(mch_username or 'N/A')}</code>\n"
                f"<b>Имя:</b> <code>{html.escape(mch_full_name or 'N/A')}</code>\n"
                f"<b>Бот:</b> <code>{html.escape(bot_name)}</code>"
            )
            reply_markup = merchant_navigation_kb(bot_uuid, current_index, total_merchants)
            await state.update_data(bot_uuid=bot_uuid, merchants_list=merchants_ids, current_merchant_index=current_index)
            await query.message.edit_text(details_text, parse_mode='HTML', reply_markup=reply_markup)
            await state.set_state(BotPanelStates.viewing_merchants)
        except Exception as e:
            logging.error(f"Error fetching merchants for bot {bot_uuid}: {e}")
            await query.message.edit_text("❌ Произошла ошибка при получении мерчантов.")
            await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")]]))

    await query.answer()

@dp.callback_query(BotPanelStates.viewing_merchants, F.data.startswith("mch_prev_"))
async def mch_prev(query: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    bot_uuid = data['bot_uuid']
    merchants_ids = data['merchants_list']
    current_index = data['current_merchant_index']
    new_index = current_index - 1
    if new_index < 0:
        new_index = len(merchants_ids) - 1 # Зацикливание
    await state.update_data(current_merchant_index=new_index)
    await _update_merchant_message(bot_uuid, new_index, state, query) # Вызов вспомогательной функции
    await query.answer()

@dp.callback_query(BotPanelStates.viewing_merchants, F.data.startswith("mch_next_"))
async def mch_next(query: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    bot_uuid = data['bot_uuid']
    merchants_ids = data['merchants_list']
    current_index = data['current_merchant_index']
    new_index = current_index + 1
    if new_index >= len(merchants_ids):
        new_index = 0 # Зацикливание
    await state.update_data(current_merchant_index=new_index)
    await _update_merchant_message(bot_uuid, new_index, state, query) # Вызов вспомогательной функции
    await query.answer()

@dp.callback_query(BotPanelStates.viewing_merchants, F.data.startswith("delete_mch_"))
async def delete_current_merchant(query: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    bot_uuid = data['bot_uuid']
    merchants_ids = data['merchants_list']
    current_index = data['current_merchant_index']
    merchant_telegram_id = merchants_ids[current_index]
    async for db in get_async_db():
        try:
            result = await db.execute(select(BotModel).filter(BotModel.uuid == bot_uuid))
            bot_data = result.scalar_one_or_none()
            if not bot_data:
                await query.message.edit_text(f"❌ Бот с UUID <code>{bot_uuid}</code> не найден.", parse_mode='HTML')
                await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="list_bots_page_0")]]))
                await query.answer()
                return

            # --- СОХРАНЯЕМ АТРИБУТЫ БОТА ---
            bot_name = bot_data.name
            # ---

            # --- ПОЛУЧАЕМ ЗАПИСЬ МЕРЧАНТА ПО TELEGRAM_ID И BOT_ID, ЧТОБЫ ВЗЯТЬ ЕЁ СОБСТВЕННЫЙ ID ---
            result = await db.execute(
                select(Merchant).filter(
                    Merchant.telegram_id == merchant_telegram_id,
                    Merchant.bot_id == bot_data.id
                )
            )
            current_merchant_db_record = result.scalar_one_or_none()
            if not current_merchant_db_record:
                 await query.message.edit_text("❌ Мерчант больше не существует.")
                 await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data=f"bot_panel_{bot_uuid}")]]))
                 await query.answer()
                 return

            # --- СОХРАНЯЕМ АТРИБУТЫ МЕРЧАНТА ---
            mch_record_username = current_merchant_db_record.username
            mch_record_telegram_id = current_merchant_db_record.telegram_id
            # ---

            confirmation_text = f"Вы уверены, что хотите удалить мерчант '<code>{html.escape(mch_record_username or 'ID: ' + str(mch_record_telegram_id))}</code>' (ID: <code>{mch_record_telegram_id}</code>) из бота '<code>{html.escape(bot_name)}</code>'?⚠️ Это действие необратимо!"
            # Передаём ID записи в базе данных, а не UUID бота и Telegram ID
            reply_markup = delete_merchant_confirmation_kb(current_merchant_db_record.id)
            # Попробуем отредактировать сообщение
            await query.message.edit_text(confirmation_text, parse_mode='HTML', reply_markup=reply_markup)
        except TelegramBadRequest as e:
            if "BUTTON_DATA_INVALID" in e.message:
                logging.error(f"Failed to edit message in delete_current_merchant (showing confirmation): {e}")
                # Попробуем отправить новое сообщение с ошибкой и кнопкой "назад"
                await query.message.answer("❌ Произошла ошибка при подготовке подтверждения удаления. Пожалуйста, вернитесь назад и повторите попытку.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data=f"bot_panel_{bot_uuid}")]]))
            else:
                # Другая ошибка Telegram, обрабатываем как обычно
                logging.error(f"Telegram error in delete_current_merchant: {e}")
                await query.message.edit_text("❌ Произошла ошибка Telegram при подготовке подтверждения удаления.")
                await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")]]))
        except Exception as e:
            logging.error(f"Error preparing to delete merchant {merchant_telegram_id} for bot {bot_uuid}: {e}")
            # Попробуем отредактировать сообщение в случае общей ошибки
            try:
                await query.message.edit_text("❌ Произошла внутренняя ошибка.")
                await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")]]))
            except TelegramBadRequest as tg_e:
                logging.error(f"Failed to edit message in delete_current_merchant (general error): {tg_e}")
                # Если и это не удалось, можно попробовать отправить новое сообщение
                try:
                    await query.message.answer("❌ Произошла внутренняя ошибка.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")]]))
                except TelegramBadRequest as send_e:
                    logging.error(f"Failed to send error message in delete_current_merchant: {send_e}")

    await query.answer()

@dp.callback_query(F.data.startswith("mch_del_cancel_"))
async def cancel_delete_merchant(query: CallbackQuery, state: FSMContext):
    # Ожидаемый формат: mch_del_cancel_{merchant_db_id}
    prefix = "mch_del_cancel_"
    if not query.data.startswith(prefix):
        await query.message.edit_text("Ошибка: Неверные данные запроса.")
        await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")]]))
        await query.answer()
        return
    try:
        merchant_db_id_str = query.data[len(prefix):] # "489410710"
        merchant_db_id = int(merchant_db_id_str)
    except (ValueError, IndexError):
        await query.message.edit_text("Ошибка: Неверный формат данных запроса.")
        await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")]]))
        await query.answer()
        return
    # --- ПОЛУЧАЕМ BOT_UUID И CURRENT_INDEX ИЗ СОСТОЯНИЯ ---
    data = await state.get_data()
    current_index = data.get('current_merchant_index', 0)
    async for db in get_async_db():
        try:
            # Находим запись мерчанта по её ID
            result = await db.execute(select(Merchant).filter(Merchant.id == merchant_db_id))
            merchant_record = result.scalar_one_or_none()
            if not merchant_record:
                await query.message.edit_text("❌ Мерчант больше не существует.")
                await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")]]))
                await query.answer()
                return

            # --- СОХРАНЯЕМ АТРИБУТЫ МЕРЧАНТА ---
            merchant_record_telegram_id = merchant_record.telegram_id
            # ---

            # Находим бота, которому принадлежит мерчант
            result = await db.execute(select(BotModel).filter(BotModel.id == merchant_record.bot_id))
            bot_record = result.scalar_one_or_none()
            if not bot_record:
                await query.message.edit_text("❌ Бот, связанный с мерчантом, не найден.")
                await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")]]))
                await query.answer()
                return

            # --- СОХРАНЯЕМ АТРИБУТЫ БОТА ---
            bot_uuid = bot_record.uuid
            # ---

            # Проверяем, соответствует ли merchant_record.telegram_id одному из merchants_ids в состоянии
            # Это важно, чтобы избежать ошибок, если список изменился
            state_merchants_ids = data.get('merchants_list', [])
            if merchant_record_telegram_id not in state_merchants_ids:
                await query.message.edit_text("❌ Мерчант больше не находится в текущем списке.")
                await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data=f"bot_panel_{bot_uuid}")]]))
                await query.answer()
                return
            # Обновляем индекс, если ID не совпадает с текущим в списке по индексу
            # (например, если список обновился, и элемент сместился)
            new_index = state_merchants_ids.index(merchant_record_telegram_id)
            if new_index != current_index:
                await state.update_data(current_merchant_index=new_index)
            # Нужно обновить сообщение с текущим мерчантом
            await _update_merchant_message(bot_uuid, new_index, state, query) # Вызов вспомогательной функции
            await query.answer() # Отвечаем на callback "удаление отменено"
            return # ВАЖНО: завершаем выполнение здесь, чтобы не вызвать _update_merchant_message дважды
        except Exception as e:
            logging.error(f"Error in cancel_delete_merchant: {e}")
            await query.message.edit_text("❌ Произошла внутренняя ошибка при отмене удаления.")
            await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")]]))

    await query.answer()

@dp.callback_query(F.data.startswith("mch_del_confirmed_"))
async def confirm_delete_merchant(query: CallbackQuery, state: FSMContext):
    # Ожидаемый формат: mch_del_confirmed_{merchant_db_id}
    prefix = "mch_del_confirmed_"
    if not query.data.startswith(prefix):
        await query.message.edit_text("Ошибка: Неверные данные запроса.")
        await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")]]))
        await query.answer()
        return
    try:
        merchant_db_id_str = query.data[len(prefix):] # "489410710"
        merchant_db_id = int(merchant_db_id_str)
    except (ValueError, IndexError):
        await query.message.edit_text("Ошибка: Неверный формат данных запроса.")
        await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")]]))
        await query.answer()
        return
    # --- ПОЛУЧАЕМ BOT_UUID И MERCHANT_TELEGRAM_ID ИЗ БАЗЫ ПО merchant_db_id ---
    async for db in get_async_db():
        try:
            # Находим запись мерчанта по её ID
            result = await db.execute(select(Merchant).filter(Merchant.id == merchant_db_id))
            merchant_record = result.scalar_one_or_none()
            if not merchant_record:
                await query.message.edit_text("❌ Мерчант больше не существует.")
                await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")]]))
                await query.answer()
                return

            # --- СОХРАНЯЕМ АТРИБУТЫ МЕРЧАНТА ---
            merchant_telegram_id = merchant_record.telegram_id
            # ---

            # Находим бота, которому принадлежит мерчант
            result = await db.execute(select(BotModel).filter(BotModel.id == merchant_record.bot_id))
            bot_record = result.scalar_one_or_none()
            if not bot_record:
                await query.message.edit_text("❌ Бот, связанный с мерчантом, не найден.")
                await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")]]))
                await query.answer()
                return

            # --- СОХРАНЯЕМ АТРИБУТЫ БОТА ---
            bot_uuid = bot_record.uuid
            # ---

            # Удаляем мерчанта по его ID
            success = await delete_merchant(db, merchant_telegram_id, bot_record.id)
            if not success:
                # delete_merchant в crud.py удаляет по telegram_id и bot_id, проверим, если запись была удалена между проверкой выше и delete_merchant
                # Но если delete_merchant возвращает False, значит, она не была найдена в момент удаления
                await query.message.edit_text(f"❌ Мерчант с ID <code>{merchant_telegram_id}</code> не найден в этом боте.", parse_mode='HTML')
                await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data=f"bot_panel_{bot_uuid}")]]))
                await query.answer()
                return
            # Обновляем список в состоянии
            data = await state.get_data()
            merchants_ids = data['merchants_list']
            current_index = data['current_merchant_index']
            updated_merchants_ids = [mid for mid in merchants_ids if mid != merchant_telegram_id]
            if not updated_merchants_ids:
                await query.message.edit_text(f"✅ Мерчант с ID <code>{merchant_telegram_id}</code> успешно удалён. Нет больше мерчантов.", parse_mode='HTML')
                await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data=f"bot_panel_{bot_uuid}")]]))
            else:
                # Выбираем индекс следующего мерчанта (тот же, если не последний, иначе предпоследнего)
                new_index = min(current_index, len(updated_merchants_ids) - 1)
                await state.update_data(merchants_list=updated_merchants_ids, current_merchant_index=new_index)
                # Обновляем сообщение с новым списком/мерчантом
                await _update_merchant_message(bot_uuid, new_index, state, query) # Вызов вспомогательной функции
        except TelegramBadRequest as e:
            if "BUTTON_DATA_INVALID" in e.message:
                logging.error(f"Telegram error in confirm_delete_merchant: {e}")
                # Попробуем отправить сообщение об ошибке и кнопку "назад"
                try:
                    await query.message.answer("❌ Произошла ошибка Telegram при подтверждении удаления. Пожалуйста, вернитесь назад и повторите попытку.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data=f"bot_panel_{bot_uuid}")]]))
                except TelegramBadRequest as send_e:
                    logging.error(f"Failed to send error message after BUTTON_DATA_INVALID: {send_e}")
                    # Если и это не удалось, просто логируем
                    pass
            else:
                # Другая ошибка Telegram, обрабатываем как обычно
                logging.error(f"Telegram error in confirm_delete_merchant: {e}")
                await query.message.edit_text("❌ Произошла ошибка Telegram при удалении мерчанта.")
                await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")]]))
        except Exception as e:
            logging.error(f"Error deleting merchant with db_id {merchant_db_id} for bot {bot_uuid}: {e}")
            await query.message.edit_text("❌ Произошла ошибка при удалении мерчанта.")
            await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")]]))

    await query.answer()

@dp.callback_query(F.data.startswith("list_bots_page_"))
async def list_bots_cb(query: CallbackQuery):
    try:
        page_num = int(query.data.split('_')[-1])
    except (ValueError, IndexError):
        page_num = 0
    async for db in get_async_db():
        try:
            all_bots = await get_bots(db)
        except Exception as e:
            logging.error(f"Error fetching bots for list: {e}")
            await query.message.edit_text("❌ Произошла ошибка при получении списка ботов.")
            await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")]]))
            await query.answer()
            return
    total_bots = len(all_bots)
    total_pages = (total_bots + BOTS_PER_PAGE - 1) // BOTS_PER_PAGE
    page_num = max(0, min(page_num, total_pages - 1))
    if total_bots == 0:
        await query.message.edit_text("Список ботов пуст.")
        await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")]]))
        await query.answer()
        return
    reply_markup = bot_list_kb(all_bots, page_num, total_pages)
    await query.message.edit_text(
        f"Список ботов (Страница {page_num + 1} из {total_pages}):",
        reply_markup=reply_markup
    )
    await query.answer()

@dp.callback_query(F.data.startswith("view_bot_"))
async def view_bot_details(query: CallbackQuery):
    bot_uuid = query.data.split('_', 2)[2]
    async for db in get_async_db():
        try:
            result = await db.execute(select(BotModel).filter(BotModel.uuid == bot_uuid))
            bot_data = result.scalar_one_or_none()
            if not bot_data:
                await query.message.edit_text(f"❌ Бот с UUID <code>{bot_uuid}</code> не найден.", parse_mode='HTML')
                await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="list_bots_page_0")]]))
                await query.answer()
                return
        except Exception as e:
            logging.error(f"Error fetching bot details: {e}")
            await query.message.edit_text("❌ Произошла ошибка при получении данных бота.")
            await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="list_bots_page_0")]]))
            await query.answer()
            return

    # --- СОХРАНЯЕМ АТРИБУТЫ БОТА ---
    bot_name = bot_data.name
    bot_uuid_str = str(bot_data.uuid)
    bot_description = bot_data.description
    # ---

    status = "🟢 Запущен" if bot_uuid in bot_manager.bots else "🔴 Остановлен"
    # masked_api_key = '***' if bot_data.payassist_api_key else 'нет'
    details_text = (
        f"<b>Название:</b> <code>{html.escape(bot_name)}</code>\n"
        f"<b>UUID:</b> <code>{html.escape(bot_uuid_str)}</code>\n"
        f"<b>Статус:</b> <code>{status}</code>\n"
        f"<b>Описание:</b> <code>{html.escape(bot_description or 'нет')}</code>"
    )
    reply_markup = bot_control_kb(bot_data.uuid)
    await query.message.edit_text(details_text, parse_mode='HTML')
    await query.message.edit_reply_markup(reply_markup=reply_markup)
    await query.answer()

@dp.callback_query(F.data.startswith("delete_confirm_"))
async def confirm_delete_bot(query: CallbackQuery):
    bot_uuid = query.data.split('_', 2)[2]
    async for db in get_async_db():
        try:
            result = await db.execute(select(BotModel).filter(BotModel.uuid == bot_uuid))
            bot_data = result.scalar_one_or_none()
            if not bot_data:
                await query.message.edit_text(f"❌ Бот с UUID <code>{bot_uuid}</code> не найден.", parse_mode='HTML')
                await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="list_bots_page_0")]]))
                await query.answer()
                return
        except Exception as e:
            logging.error(f"Error fetching bot details for deletion: {e}")
            await query.message.edit_text("❌ Произошла ошибка.")
            await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="list_bots_page_0")]]))
            await query.answer()
            return

    # --- СОХРАНЯЕМ АТРИБУТЫ БОТА ---
    bot_name = bot_data.name
    bot_uuid_str = str(bot_data.uuid)
    # ---

    confirmation_text = f"Вы уверены, что хотите удалить бота '<code>{html.escape(bot_name)}</code>' (UUID: <code>{html.escape(bot_uuid_str)}</code>)?⚠️ Это действие необратимо!"
    reply_markup = delete_confirmation_kb(bot_uuid)
    await query.message.edit_text(confirmation_text, parse_mode='HTML')
    await query.message.edit_reply_markup(reply_markup=reply_markup)
    await query.answer()

@dp.callback_query(F.data.startswith("delete_cancel_"))
async def cancel_delete_bot(query: CallbackQuery):
    bot_uuid = query.data.split('_', 2)[2]
    # Получаем данные бота из БД, чтобы отобразить его детали
    async for db in get_async_db():
        try:
            result = await db.execute(select(BotModel).filter(BotModel.uuid == bot_uuid))
            bot_data = result.scalar_one_or_none()
            if not bot_data:
                await query.message.edit_text(f"❌ Бот с UUID <code>{bot_uuid}</code> не найден.", parse_mode='HTML')
                await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="list_bots_page_0")]]))
                await query.answer()
                return
        except Exception as e:
            logging.error(f"Error fetching bot details in cancel_delete_bot: {e}")
            await query.message.edit_text("❌ Произошла ошибка.")
            await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="list_bots_page_0")]]))
            await query.answer()
            return

    # --- СОХРАНЯЕМ АТРИБУТЫ БОТА ---
    bot_name = bot_data.name
    bot_uuid_str = str(bot_data.uuid)
    bot_description = bot_data.description
    # ---

    # Воссоздаем логику из view_bot_details для формирования текста и клавиатуры
    status = "🟢 Запущен" if bot_uuid in bot_manager.bots else "🔴 Остановлен"
    masked_api_key = '***' if bot_data.payassist_api_key else 'нет'
    details_text = (
        f"<b>Название:</b> <code>{html.escape(bot_name)}</code>\n"
        f"<b>UUID:</b> <code>{html.escape(bot_uuid_str)}</code>\n"
        f"<b>Статус:</b> <code>{status}</code>\n"
        f"<b>Описание:</b> <code>{html.escape(bot_description or 'нет')}</code>"
    )
    reply_markup = bot_control_kb(bot_data.uuid)
    # Редактируем текущее сообщение (подтверждение удаления) на сообщение с деталями бота
    await query.message.edit_text(details_text, parse_mode='HTML', reply_markup=reply_markup)
    await query.answer() # Отвечаем на callback "удаление отменено"

@dp.callback_query(F.data.startswith("delete_confirmed_"))
async def delete_bot(query: CallbackQuery):
    bot_uuid = query.data.split('_', 2)[2]
    async for db in get_async_db():
        try:
            result = await db.execute(select(BotModel).filter(BotModel.uuid == bot_uuid))
            bot_data = result.scalar_one_or_none()
            if not bot_data:
                await query.message.edit_text(f"❌ Бот с UUID <code>{bot_uuid}</code> не найден.", parse_mode='HTML')
                await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="list_bots_page_0")]]))
                await query.answer()
                return

            # --- СОХРАНЯЕМ АТРИБУТЫ БОТА ---
            bot_name = bot_data.name
            bot_uuid_str = str(bot_data.uuid)
            # ---

            if bot_uuid in bot_manager.bots:
                bot_manager.stop_bot(bot_uuid)
                logging.info(f"Bot {bot_name} (UUID: {bot_uuid}) stopped before deletion.")
            await db.delete(bot_data)
            await db.commit()
            logging.info(f"Bot {bot_name} (UUID: {bot_uuid}) deleted from database.")

            await query.message.edit_text(f"✅ Бот '<code>{html.escape(bot_name)}</code>' (UUID: <code>{html.escape(bot_uuid_str)}</code>) успешно удален.", parse_mode='HTML')
            await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="list_bots_page_0")]]))
            await query.answer()
        except Exception as e:
            logging.error(f"Error deleting bot {bot_uuid}: {e}")
            await query.message.edit_text(f"❌ Произошла ошибка при удалении бота: {e}")
            await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="list_bots_page_0")]]))
            await query.answer()

@dp.callback_query(F.data.startswith("start_"))
async def start_bot_cb(query: CallbackQuery):
    bot_uuid = query.data.split("_", 1)[1]
    async for db in get_async_db():
        try:
            result = await db.execute(select(BotModel).filter(BotModel.uuid == bot_uuid))
            bot_data = result.scalar_one_or_none()
            if not bot_data:
                 logging.warning(f"Bot data for UUID {bot_uuid} not found in DB, but start requested.")
                 await query.message.edit_text(f"❌ Бот с UUID <code>{bot_uuid}</code> не найден в базе данных.", parse_mode='HTML')
                 await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="list_bots_page_0")]]))
                 await query.answer()
                 return

            # --- СОХРАНЯЕМ АТРИБУТЫ БОТА ---
            bot_name = bot_data.name
            # ---

            success = bot_manager.start_bot(bot_uuid, bot_data.token, bot_data.payassist_api_key)
            status_message = f"▶️ Бот {bot_name} запущен" if success else f"⚠️ Бот {bot_name} возможно, уже запущен."
            status = "🟢 Запущен" if bot_uuid in bot_manager.bots else "🔴 Остановлен"
            details_text = (
                f"<b>Название:</b> <code>{html.escape(bot_name)}</code>\n"
                f"<b>UUID:</b> <code>{html.escape(str(bot_data.uuid))}</code>\n"
                f"<b>Статус:</b> <code>{status}</code>\n"
                f"<b>Описание:</b> <code>{html.escape(bot_data.description or 'нет')}</code>"
            )
            reply_markup = bot_control_kb(bot_data.uuid)
            # Попробовать отредактировать сообщение
            await query.message.edit_text(details_text, parse_mode='HTML', reply_markup=reply_markup)
            await query.answer(status_message)
        except TelegramBadRequest as e:
            if "message is not modified" in e.message:
                # Сообщение не изменилось, просто ответим
                await query.answer(status_message)
            else:
                # Другая ошибка Telegram, обрабатываем как обычно
                raise
        except Exception as e:
            logging.error(f"Error starting bot {bot_uuid}: {e}")
            await query.message.edit_text(f"❌ Произошла ошибка при запуске бота: {e}")
            await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="list_bots_page_0")]]))
            await query.answer()

@dp.callback_query(F.data.startswith("stop_"))
async def stop_bot_cb(query: CallbackQuery):
    bot_uuid = query.data.split("_", 1)[1]
    async for db in get_async_db():
        try:
            result = await db.execute(select(BotModel).filter(BotModel.uuid == bot_uuid))
            bot_data = result.scalar_one_or_none()
            if not bot_data:
                 logging.warning(f"Bot data for UUID {bot_uuid} not found in DB, but stop requested.")
                 await query.message.edit_text(f"❌ Бот с UUID <code>{bot_uuid}</code> не найден в базе данных.", parse_mode='HTML')
                 await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="list_bots_page_0")]]))
                 await query.answer()
                 return

            # --- СОХРАНЯЕМ АТРИБУТЫ БОТА ---
            bot_name = bot_data.name
            # ---

            bot_manager.stop_bot(bot_uuid)
            status_message = f"⏹️ Бот {bot_name} остановлен"
            status = "🟢 Запущен" if bot_uuid in bot_manager.bots else "🔴 Остановлен"
            details_text = (
                f"<b>Название:</b> <code>{html.escape(bot_name)}</code>\n"
                f"<b>UUID:</b> <code>{html.escape(str(bot_data.uuid))}</code>\n"
                f"<b>Статус:</b> <code>{status}</code>\n"
                f"<b>Описание:</b> <code>{html.escape(bot_data.description or 'нет')}</code>"
            )
            reply_markup = bot_control_kb(bot_data.uuid)
            # Попробовать отредактировать сообщение
            await query.message.edit_text(details_text, parse_mode='HTML', reply_markup=reply_markup)
            await query.answer(status_message)
        except TelegramBadRequest as e:
            if "message is not modified" in e.message:
                # Сообщение не изменилось, просто ответим
                await query.answer(status_message)
            else:
                # Другая ошибка Telegram, обрабатываем как обычно
                raise
        except Exception as e:
            logging.error(f"Error stopping bot {bot_uuid}: {e}")
            await query.message.edit_text(f"❌ Произошла ошибка при остановке бота: {e}")
            await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="list_bots_page_0")]]))
            await query.answer()

@dp.callback_query(F.data.startswith("restart_"))
async def restart_bot_cb(query: CallbackQuery):
    bot_uuid = query.data.split("_", 1)[1]
    async for db in get_async_db():
        try:
            result = await db.execute(select(BotModel).filter(BotModel.uuid == bot_uuid))
            bot_data = result.scalar_one_or_none()
            if not bot_data:
                await query.message.edit_text(f"❌ Бот с UUID <code>{bot_uuid}</code> не найден в базе данных.", parse_mode='HTML')
                await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="list_bots_page_0")]]))
                await query.answer()
                return

            # --- СОХРАНЯЕМ АТРИБУТЫ БОТА ---
            bot_name = bot_data.name
            # ---

            success = bot_manager.restart_bot(bot_uuid, bot_data.token, bot_data.payassist_api_key)
            status_message = f"🔄 Бот {bot_name} перезапущен" if success else f"⚠️ Ошибка при перезапуске бота {bot_name}."
            status = "🟢 Запущен" if bot_uuid in bot_manager.bots else "🔴 Остановлен"
            details_text = (
                f"<b>Название:</b> <code>{html.escape(bot_name)}</code>\n"
                f"<b>UUID:</b> <code>{html.escape(str(bot_data.uuid))}</code>\n"
                f"<b>Статус:</b> <code>{status}</code>\n"
                f"<b>Описание:</b> <code>{html.escape(bot_data.description or 'нет')}</code>"
            )
            reply_markup = bot_control_kb(bot_data.uuid)
            # Попробовать отредактировать сообщение
            await query.message.edit_text(details_text, parse_mode='HTML', reply_markup=reply_markup)
            await query.answer(status_message)
        except TelegramBadRequest as e:
            if "message is not modified" in e.message:
                # Сообщение не изменилось, просто ответим
                await query.answer(status_message)
            else:
                # Другая ошибка Telegram, обрабатываем как обычно
                raise
        except Exception as e:
            logging.error(f"Error restarting bot {bot_uuid}: {e}")
            await query.message.edit_text(f"❌ Произошла ошибка при перезапуске бота: {e}")
            await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="list_bots_page_0")]]))
            await query.answer()

@dp.callback_query(F.data == "admin_menu")
async def admin_menu_cb(query: CallbackQuery):
    await query.message.edit_text("Добро пожаловать, администратор!", reply_markup=admin_menu_kb())
    await query.answer()

@dp.callback_query(F.data == "create_bot")
async def create_bot_start(query: CallbackQuery, state: FSMContext):
    await query.answer()
    # Создаем клавиатуру с кнопкой "Отмена"
    cancel_kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_create_bot")]
        ]
    )
    await query.message.edit_text("Введите Telegram-токен дочернего бота (от @BotFather):", reply_markup=cancel_kb)
    await state.set_state(CreateBotStates.waiting_for_token)

# --- НОВЫЙ ОБРАБОТЧИК ОТМЕНЫ СОЗДАНИЯ БОТА ---
@dp.callback_query(F.data == "cancel_create_bot")
async def cancel_create_bot(query: CallbackQuery, state: FSMContext):
    await query.message.edit_text("Создание бота отменено.")
    await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")]]))
    await state.clear()
    await query.answer()

@dp.message(CreateBotStates.waiting_for_token)
async def create_bot_get_token(message: Message, state: FSMContext):
    token = message.text.strip()
    # Простая проверка на длину и наличие ':' - не гарантирует валидности, но ловит очевидные ошибки
    if len(token) < 46 or ':' not in token:
        # Повторяем запрос с кнопкой "Отмена"
        cancel_kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_create_bot")]
            ]
        )
        await message.answer("Пожалуйста, введите корректный Telegram-токен дочернего бота (от @BotFather).", reply_markup=cancel_kb)
        return
    await state.update_data(token=token)
    await message.answer("Введите название дочернего бота:")
    await state.set_state(CreateBotStates.waiting_for_name)

@dp.message(CreateBotStates.waiting_for_name)
async def create_bot_get_name(message: Message, state: FSMContext):
    name = message.text.strip() or "Без имени"
    await state.update_data(name=name)
    await message.answer("Введите описание дочернего бота (опционально):")
    await state.set_state(CreateBotStates.waiting_for_description)

@dp.message(CreateBotStates.waiting_for_description)
async def create_bot_get_description(message: Message, state: FSMContext):
    description = message.text.strip() or None
    await state.update_data(description=description)
    await message.answer("Введите API-ключ PayAssist в формате Client_id:Client_secret:")
    await state.set_state(CreateBotStates.waiting_for_payassist_api_key)

@dp.message(CreateBotStates.waiting_for_payassist_api_key)
async def create_bot_save(message: Message, state: FSMContext):
    payassist_api_key = message.text.strip()
    data = await state.get_data()
    token = data.get("token")
    name = data.get("name")
    description = data.get("description")
    bot_uuid = str(uuid.uuid4())
    async for db in get_async_db():
        try:
            await add_bot(db, token=token, name=name, description=description, uuid=bot_uuid, payassist_api_key=payassist_api_key)
            success = bot_manager.start_bot(bot_uuid, token, payassist_api_key)
            if not success:
                 logging.warning(f"Bot {bot_uuid} might already be running or failed to start.")
            await db.commit()
            # Создаем клавиатуру с кнопкой "Открыть список ботов"
            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="📋 Открыть список ботов", callback_data="list_bots_page_0")]
                ]
            )
            await message.answer(
                f"✅ Дочерний бот создан!\n"
                f"<b>UUID:</b> <code>{bot_uuid}</code>\n"
                f"<b>Название:</b> <code>{html.escape(name)}</code>\n"
                f"<b>Описание:</b> <code>{html.escape(description or 'нет')}</code>\n"
                f"<b>API ключ PayAssist:</b> <code>{html.escape(payassist_api_key)}</code>",
                parse_mode='HTML',
                reply_markup=keyboard # Прикрепляем клавиатуру к сообщению
            )
        except Exception as e:
            logging.error(f"Error creating bot or starting process: {e}")
            # Клавиатура также передается в случае ошибки
            await message.answer(f"❌ Произошла ошибка при создании бота: {e}", reply_markup=keyboard)
    await state.clear()

@dp.callback_query(F.data == "add_administrator")
async def callback_add_administrator(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    # Создаем клавиатуру с кнопкой "Отмена"
    cancel_kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_add_admin")]
        ]
    )
    await callback.message.edit_text("Введите Telegram ID администратора:", reply_markup=cancel_kb)
    await state.set_state(AdminStates.waiting_for_admin_telegram_id)

@dp.callback_query(F.data == "cancel_add_admin")
async def cancel_add_admin(query: CallbackQuery, state: FSMContext):
    await query.message.edit_text("Добавление администратора отменено.")
    await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")]]))
    await state.clear()
    await query.answer()

@dp.message(AdminStates.waiting_for_admin_telegram_id)
async def process_admin_telegram_id(message: Message, state: FSMContext):
    if not message.text.isdigit():
        # Повторяем запрос с кнопкой "Отмена"
        cancel_kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_add_admin")]
            ]
        )
        await message.answer("Пожалуйста, введите корректный Telegram ID (число).", reply_markup=cancel_kb)
        return
    telegram_id = int(message.text)
    async for db in get_async_db():
        try:
            existing_admin = await get_admin(db, telegram_id)
        except Exception as e:
            logging.error(f"Error checking existing admin: {e}")
            await message.answer("❌ Произошла ошибка при проверке администратора.")
            await state.clear()
            return
    if existing_admin:
        # Повторяем запрос с кнопкой "Отмена"
        cancel_kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_add_admin")]
            ]
        )
        await message.answer(f"Пользователь с ID <code>{telegram_id}</code> уже является администратором. Введите другой ID или нажмите 'Отмена'.", parse_mode='HTML', reply_markup=cancel_kb)
        return
    await state.update_data(telegram_id=telegram_id)
    # Создаем клавиатуру с кнопкой "Отмена"
    cancel_kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_add_admin")]
        ]
    )
    await message.answer(
        f"<b>Telegram ID:</b> <code>{telegram_id}</code>\n"
        f"Теперь введите @username (или '-' если нет) и имя (или '-' если нет), разделённые запятой.\n"
        f"Пример: <code>@username, Иван Иванов</code>\n"
        f"Или: <code>- , Иван Иванов</code>\n"
        f"Или: <code>- , -</code>",
        parse_mode='HTML',
        reply_markup=cancel_kb
    )
    await state.set_state(AdminStates.waiting_for_admin_details)

@dp.message(AdminStates.waiting_for_admin_details)
async def process_admin_details(message: Message, state: FSMContext):
    data = await state.get_data()
    telegram_id = data.get("telegram_id")
    text = message.text.strip()
    parts = [part.strip() for part in text.split(",", 1)]
    if len(parts) < 2:
        cancel_kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_add_admin")]
            ]
        )
        await message.answer("Пожалуйста, введите username и имя, разделённые запятой, или '-' для пропуска.", reply_markup=cancel_kb)
        return

    username_raw = parts[0]
    full_name_raw = parts[1]
    username = username_raw if username_raw != "-" and username_raw.startswith("@") else None
    if username_raw != "-" and username is None:
        cancel_kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_add_admin")]
            ]
        )
        await message.answer("Username должен начинаться с @ или быть '-' для пропуска.", reply_markup=cancel_kb)
        return
    full_name = full_name_raw if full_name_raw != "-" else None

    async for db in get_async_db(): # <-- Контекст асинхронной сессии
        try:
            # --- ПОЛУЧАЕМ НУЖНЫЕ ДАННЫЕ ДО ИЗМЕНЕНИЙ ---
            # В данном случае, мы создаём нового администратора, так что просто используем полученные данные
            # telegram_id, username, full_name уже получены из state и input
            # ---
            # Добавляем администратора
            new_admin = await add_admin(db, telegram_id=telegram_id, username=username, full_name=full_name) 

            admin_telegram_id = new_admin.telegram_id
            admin_username = new_admin.username
            admin_full_name = new_admin.full_name
            
            await db.commit() # <-- Commit изменений
            # --- СОХРАНЯЕМ АТРИБУТЫ НОВОГО АДМИНА СРАЗУ ПОСЛЕ СОЗДАНИЯ/COMMIT ---
            # Это гарантирует, что мы используем значения из объекта, который уже синхронизирован с БД
            
            # ---
            # Создаем клавиатуру с кнопкой "Перейти к списку администраторов"
            list_admins_kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="📋 Перейти к списку администраторов", callback_data="list_admins_page_0")]
                ]
            )
            # Используем сохранённые значения
            await message.answer(f"✅ Администратор добавлен:\n"
                                 f"<b>ID:</b> <code>{admin_telegram_id}</code>\n"
                                 f"<b>Username:</b> <code>{admin_username or 'N/A'}</code>\n"
                                 f"<b>Имя:</b> <code>{admin_full_name or 'N/A'}</code>\n",
                                 parse_mode='HTML',
                                 reply_markup=list_admins_kb)
        except Exception as e:
            logging.error(f"Error adding admin: {e}", exc_info=True) # Добавим трейсбек для отладки
            await message.answer(f"❌ Произошла ошибка при добавлении администратора: {e}")

    await state.clear()

# --- НОВЫЕ ОБРАБОТЧИКИ ДЛЯ СПИСКА АДМИНОВ ---
@dp.callback_query(F.data.startswith("list_admins_page_"))
async def list_admins_cb(query: CallbackQuery):
    try:
        page_num = int(query.data.split('_')[-1])
    except (ValueError, IndexError):
        page_num = 0
    async for db in get_async_db():
        try:
            all_admins = await get_all_admins(db) # Используем новый CRUD метод
        except Exception as e:
            logging.error(f"Error fetching admins for list: {e}")
            await query.message.edit_text("❌ Произошла ошибка при получении списка администраторов.")
            await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")]]))
            await query.answer()
            return
    total_admins = len(all_admins)
    total_pages = (total_admins + ADMINS_PER_PAGE - 1) // ADMINS_PER_PAGE
    page_num = max(0, min(page_num, total_pages - 1))
    if total_admins == 0:
        await query.message.edit_text("Список администраторов пуст.")
        await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")]]))
        await query.answer()
        return
    reply_markup = admin_list_kb(all_admins, page_num, total_pages)
    await query.message.edit_text(
        f"Список администраторов (Страница {page_num + 1} из {total_pages}):",
        reply_markup=reply_markup
    )
    await query.answer()

@dp.callback_query(F.data.startswith("view_admin_"))
async def view_admin_details(query: CallbackQuery):
    admin_telegram_id = int(query.data.split('_', 2)[2])
    async for db in get_async_db():
        try:
            result = await db.execute(select(AdminModel).filter(AdminModel.telegram_id == admin_telegram_id))
            admin_data = result.scalar_one_or_none()
            if not admin_data:
                await query.message.edit_text(f"❌ Администратор с ID <code>{admin_telegram_id}</code> не найден.", parse_mode='HTML')
                await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="list_admins_page_0")]]))
                await query.answer()
                return
        except Exception as e:
            logging.error(f"Error fetching admin details: {e}")
            await query.message.edit_text("❌ Произошла ошибка при получении данных администратора.")
            await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="list_admins_page_0")]]))
            await query.answer()
            return

    # --- СОХРАНЯЕМ АТРИБУТЫ АДМИНА ---
    admin_telegram_id = admin_data.telegram_id
    admin_username = admin_data.username
    admin_full_name = admin_data.full_name
    # ---

    details_text = (
        f"<b>ID:</b> <code>{admin_telegram_id}</code>\n"
        f"<b>Username:</b> <code>{admin_username or 'N/A'}</code>\n"
        f"<b>Имя:</b> <code>{admin_full_name or 'N/A'}</code>"
    )
    reply_markup = admin_control_kb(admin_data.telegram_id)
    await query.message.edit_text(details_text, parse_mode='HTML')
    await query.message.edit_reply_markup(reply_markup=reply_markup)
    await query.answer()

@dp.callback_query(F.data.startswith("edit_admin_"))
async def start_edit_admin(query: CallbackQuery, state: FSMContext):
    admin_telegram_id = int(query.data.split('_', 2)[2])
    async for db in get_async_db():
        try:
            result = await db.execute(select(AdminModel).filter(AdminModel.telegram_id == admin_telegram_id))
            admin_data = result.scalar_one_or_none()
            if not admin_data:
                await query.message.edit_text(f"❌ Администратор с ID <code>{admin_telegram_id}</code> не найден.", parse_mode='HTML')
                await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="list_admins_page_0")]]))
                await query.answer()
                return
        except Exception as e:
            logging.error(f"Error fetching admin details for editing: {e}")
            await query.message.edit_text("❌ Произошла ошибка при получении данных администратора.")
            await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="list_admins_page_0")]]))
            await query.answer()
            return

    # --- СОХРАНЯЕМ АТРИБУТЫ АДМИНА ---
    admin_telegram_id = admin_data.telegram_id
    admin_username = admin_data.username
    admin_full_name = admin_data.full_name
    # ---

    await state.update_data(admin_telegram_id=admin_telegram_id)
    # Создаем клавиатуру с кнопкой "Отмена"
    cancel_kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_edit_admin")]
        ]
    )
    await query.message.edit_text(
        f"<b>Редактирование администратора:</b>\n"
        f"<b>ID:</b> <code>{admin_telegram_id}</code>\n"
        f"<b>Текущий username:</b> <code>{admin_username or 'N/A'}</code>\n"
        f"<b>Текущее имя:</b> <code>{admin_full_name or 'N/A'}</code>\n"
        f"Введите @username (или '-' если нет) и имя (или '-' если нет), разделённые запятой.\n"
        f"Пример: <code>@username, Иван Иванов</code>\n"
        f"Или: <code>- , Иван Иванов</code>\n"
        f"Или: <code>- , -</code>",
        parse_mode='HTML',
        reply_markup=cancel_kb
    )
    await state.set_state(EditAdminStates.waiting_for_edit_details)
    await query.answer()

@dp.callback_query(F.data == "cancel_edit_admin")
async def cancel_edit_admin(query: CallbackQuery, state: FSMContext):
    await query.message.edit_text("Редактирование администратора отменено.")
    # Попробуем вернуться к просмотру администратора, если возможно, иначе к списку
    data = await state.get_data()
    admin_telegram_id = data.get('admin_telegram_id')
    if admin_telegram_id:
        await query.message.edit_reply_markup(reply_markup=admin_control_kb(admin_telegram_id))
    else:
        await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="list_admins_page_0")]]))
    await state.clear()
    await query.answer()

@dp.message(EditAdminStates.waiting_for_edit_details)
async def process_edit_admin_details(message: Message, state: FSMContext):
    data = await state.get_data()
    original_telegram_id = data.get("admin_telegram_id")
    text = message.text.strip()
    parts = [part.strip() for part in text.split(",", 1)]
    if len(parts) < 2:
        cancel_kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_edit_admin")]
            ]
        )
        await message.answer("Пожалуйста, введите username и имя, разделённые запятой, или '-' для пропуска.", reply_markup=cancel_kb)
        return

    username_raw = parts[0]
    full_name_raw = parts[1]
    username = username_raw if username_raw != "-" and username_raw.startswith("@") else None
    if username_raw != "-" and username is None:
        cancel_kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_edit_admin")]
            ]
        )
        await message.answer("Username должен начинаться с @ или быть '-' для пропуска.", reply_markup=cancel_kb)
        return
    full_name = full_name_raw if full_name_raw != "-" else None

    async for db in get_async_db(): # <-- Контекст асинхронной сессии
        try:
            # Находим администратора по ID ДО изменений
            result = await db.execute(select(AdminModel).filter(AdminModel.telegram_id == original_telegram_id))
            admin_to_update = result.scalar_one_or_none()
            if not admin_to_update:
                await message.answer(f"❌ Администратор с ID <code>{original_telegram_id}</code> не найден.", parse_mode='HTML')
                await state.clear()
                return
            # --- СОХРАНЯЕМ СТАРЫЕ ЗНАЧЕНИЯ ДО ИЗМЕНЕНИЯ ОБЪЕКТА ---
            # Это может быть не обязательно, если мы не будем использовать старые значения после commit
            # old_username = admin_to_update.username
            # old_full_name = admin_to_update.full_name
            # Но для консистентности можно сохранить и то, и то, что обновляем
            admin_telegram_id = admin_to_update.telegram_id # <-- Это не изменится, но для ясности
            # ---
            # Обновляем данные в объекте
            admin_to_update.username = username
            admin_to_update.full_name = full_name

            updated_username = admin_to_update.username
            updated_full_name = admin_to_update.full_name
            # db.add не нужно для уже существующего объекта
            await db.commit() # <-- Commit изменений
            # --- СОХРАНЯЕМ НОВЫЕ ЗНАЧЕНИЯ ПОСЛЕ COMMIT ---
            
            # ---
            # Создаем клавиатуру с кнопкой "Назад к списку админов"
            back_to_list_kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="◀️ Назад к списку админов", callback_data="list_admins_page_0")]
                ]
            )
            # Используем сохранённые ОБНОВЛЁННЫЕ значения
            await message.answer(f"✅ Администратор обновлён:\n"
                                 f"<b>ID:</b> <code>{admin_telegram_id}</code>\n"
                                 f"<b>Username:</b> <code>{updated_username or 'N/A'}</code>\n"
                                 f"<b>Имя:</b> <code>{updated_full_name or 'N/A'}</code>",
                                 parse_mode='HTML',
                                 reply_markup=back_to_list_kb)
        except Exception as e:
            logging.error(f"Error updating admin: {e}", exc_info=True) # Добавим трейсбек для отладки
            await message.answer(f"❌ Произошла ошибка при обновлении администратора: {e}")

    await state.clear()

@dp.callback_query(F.data.startswith("delete_admin_confirm_"))
async def confirm_delete_admin(query: CallbackQuery):
    admin_telegram_id = int(query.data.split('_', 3)[3])
    async for db in get_async_db():
        try:
            result = await db.execute(select(AdminModel).filter(AdminModel.telegram_id == admin_telegram_id))
            admin_data = result.scalar_one_or_none()
            if not admin_data:
                await query.message.edit_text(f"❌ Администратор с ID <code>{admin_telegram_id}</code> не найден.", parse_mode='HTML')
                await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="list_admins_page_0")]]))
                await query.answer()
                return
        except Exception as e:
            logging.error(f"Error fetching admin details for deletion confirmation: {e}")
            await query.message.edit_text("❌ Произошла ошибка.")
            await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="list_admins_page_0")]]))
            await query.answer()
            return

    # --- СОХРАНЯЕМ АТРИБУТЫ АДМИНА ---
    admin_username = admin_data.username
    admin_telegram_id = admin_data.telegram_id
    # ---

    confirmation_text = f"Вы уверены, что хотите удалить администратора '<code>{admin_username or 'ID: ' + str(admin_telegram_id)}</code>' (ID: <code>{admin_telegram_id}</code>)?⚠️ Это действие необратимо!"
    reply_markup = delete_admin_confirmation_kb(admin_telegram_id)
    await query.message.edit_text(confirmation_text, parse_mode='HTML')
    await query.message.edit_reply_markup(reply_markup=reply_markup)
    await query.answer()

@dp.callback_query(F.data.startswith("delete_admin_cancel_"))
async def cancel_delete_admin(query: CallbackQuery):
    admin_telegram_id = int(query.data.split('_', 3)[3])
    # Возвращаемся к просмотру администратора
    await view_admin_details(CallbackQuery(id=query.id, from_user=query.from_user, chat_instance=query.chat_instance, data=f"view_admin_{admin_telegram_id}"))
    # await query.answer() # Не вызываем, так как view_admin_details уже вызывает

@dp.callback_query(F.data.startswith("delete_admin_confirmed_"))
async def delete_admin(query: CallbackQuery):
    admin_telegram_id = int(query.data.split('_', 3)[3])
    async for db in get_async_db(): # <-- Контекст асинхронной сессии
        try:
            # Находим администратора по ID ДО удаления
            result = await db.execute(select(AdminModel).filter(AdminModel.telegram_id == admin_telegram_id))
            admin_data = result.scalar_one_or_none()
            if not admin_data:
                await query.message.edit_text(f"❌ Администратор с ID <code>{admin_telegram_id}</code> не найден.", parse_mode='HTML')
                await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="list_admins_page_0")]]))
                await query.answer()
                return

            # --- СОХРАНЯЕМ НУЖНЫЕ АТРИБУТЫ ДО УДАЛЕНИЯ ---
            admin_username = admin_data.username
            admin_full_name = admin_data.full_name
            # ---

            await db.delete(admin_data) # <-- Удаление объекта
            await db.commit() # <-- Commit изменений
            logging.info(f"Admin with ID {admin_telegram_id} deleted from database.")

            # --- ИСПОЛЬЗУЕМ СОХРАНЁННЫЕ ЗНАЧЕНИЯ ---
            display_name = admin_username or f'ID: {admin_telegram_id}'
            await query.message.edit_text(f"✅ Администратор '<code>{html.escape(display_name)}</code>' (ID: <code>{admin_telegram_id}</code>) успешно удален.", parse_mode='HTML')
            await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="list_admins_page_0")]]))
        except Exception as e:
            logging.error(f"Error deleting admin {admin_telegram_id}: {e}", exc_info=True) # Добавим трейсбек для отладки
            await query.message.edit_text(f"❌ Произошла ошибка при удалении администратора: {e}")
            await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="list_admins_page_0")]]))

    await query.answer()

@dp.message(Command("start"))
async def cmd_start(message: Message):
    # logging.error("Тестовая ошибка от основного бота!")
    async for db in get_async_db():
        try:
            result = await db.execute(select(AdminModel).filter(AdminModel.telegram_id == message.from_user.id))
            admin = result.scalar_one_or_none()
        except Exception as e:
            logging.error(f"Error checking admin status: {e}")
            await message.answer("❌ Произошла внутренняя ошибка.")
            return

    if admin or message.from_user.id == ADMIN_TG_ID:
        await message.answer("Добро пожаловать, администратор!", reply_markup=admin_menu_kb())
    # else:
    #     await message.answer("У вас нет прав администратора.")

async def fetch_unprocessed_errors_async():
    """Асинхронная функция для получения ошибок."""
    async for db in get_async_db():
        try:
            errors = await get_unprocessed_errors(db, limit=10)
            errors_data = []
            for err in errors:
                # Копируем данные в словарь, чтобы отвязать от сессии
                errors_data.append({
                    "id": err.id,
                    "bot_uuid": err.bot_uuid,
                    "error_message": err.error_message
                })
            return errors_data
        except Exception as e:
            logging.error(f"Error fetching errors async: {e}")
            return []

async def mark_error_processed_async(error_id):
    """Асинхронная функция для пометки ошибки как обработанной."""
    async for db in get_async_db():
        try:
            await mark_error_as_processed(db, error_id)
            await db.commit()
        except Exception as e:
            logging.error(f"Error marking error {error_id} async: {e}")

# async def monitor_child_bot_db_errors():
#     """
#     Фоновая задача: мониторит таблицу ChildBotError,
#     читает непрочитанные записи и отправляет их в канал через основного бота.
#     """
#     # global bot, ERROR_CHANNEL_CHAT_ID # Убедитесь, что bot и ERROR_CHANNEL_CHAT_ID доступны
#     # ERROR_CHANNEL_CHAT_ID = os.getenv("ERROR_CHANNEL_CHAT_ID") # Получаем ID канала
#     if not ERROR_CHANNEL_CHAT_ID:
#         logging.warning("ERROR_CHANNEL_CHAT_ID environment variable is not set. Error notifications will not be sent.")
#         return
#     logging.info("Started DB error monitoring task.")
#     while True:
#         try:
#             # 1. Получаем ошибки асинхронно
#             unprocessed_errors = await fetch_unprocessed_errors_async()
#             # 2. Подготовим задачи для отправки
#             tasks = []
#             error_ids_to_mark = [] # Список ID ошибок, которые нужно отметить как обработанные
#             for error_data in unprocessed_errors:
#                 # Данные из словаря
#                 err_id = error_data["id"]
#                 bot_uuid = error_data["bot_uuid"]
#                 raw_msg = error_data["error_message"] or "No message"
#                 try:
#                     from html import escape
#                     # Обрезка длинного сообщения
#                     MAX_LEN = 3000
#                     if len(raw_msg) > MAX_LEN:
#                         raw_msg = raw_msg[:MAX_LEN] + "... [ОБРЕЗАНО]"
#                     safe_error_msg = escape(raw_msg)
#                     safe_bot_uuid = escape(bot_uuid)
#                     message_to_send = f"⚠️ Ошибка в боте <b>{safe_bot_uuid}</b> (ID ошибки: {err_id}):\n<code>{safe_error_msg}</code>"
#                     # Создаём корутину для отправки
#                     task = bot.send_message(ERROR_CHANNEL_CHAT_ID, message_to_send, parse_mode='HTML')
#                     tasks.append(task)
#                     await asyncio.sleep(1.1)
#                     error_ids_to_mark.append(err_id) # Сохраняем ID для последующей отметки
#                 except Exception as msg_prep_err:
#                     logging.error(f"Error preparing message for error ID {err_id}: {msg_prep_err}", exc_info=True)
#                     # Помечаем ошибку как обработанную, даже если подготовка сообщения не удалась
#                     await mark_error_processed_async(err_id)
#             # 3. Отправляем все подготовленные сообщения параллельно
#             if tasks:
#                 # gather с return_exceptions=True, чтобы не прерывать другие отправки при ошибке одного сообщения
#                 results = await asyncio.gather(*tasks, return_exceptions=True)
#                 # 4. Обрабатываем результаты отправки и отмечаем ошибки как обработанные
#                 for i, res in enumerate(results):
#                     err_id = error_ids_to_mark[i]
#                     if isinstance(res, Exception):
#                         # Ошибка при отправке конкретного сообщения
#                         if hasattr(res, 'method_call') and hasattr(res, 'result'):
#                             # Это ошибка aiogram
#                             from aiogram.exceptions import TelegramRetryAfter, TelegramBadRequest
#                             if isinstance(res, TelegramRetryAfter):
#                                 logging.warning(f"TelegramRetryAfter for error ID {err_id}: Wait {res.retry_after}s. Will retry later.")
#                                 # Не помечаем как обработанную, оставляем в базе
#                                 continue # Переходим к следующей ошибке
#                             elif isinstance(res, TelegramBadRequest):
#                                 logging.error(f"TelegramBadRequest for error ID {err_id}: {res}")
#                                 # Некоторые BadRequest (например, FloodWait) можно обработать особым образом
#                                 # Но в большинстве случаев, если сообщение не удалось отправить из-за BadRequest,
#                                 # его, возможно, стоит пропустить и отметить как обработанное, чтобы не застрять.
#                                 # Решение зависит от конкретной ошибки BadRequest.
#                                 # Пока что, для простоты, помечаем как обработанное.
#                                 await mark_error_processed_async(err_id)
#                                 continue
#                         # Любая другая ошибка при отправке
#                         logging.critical(f"Failed to send error notification (ID: {err_id}) to channel {ERROR_CHANNEL_CHAT_ID}: {res}", exc_info=False)
#                         # Не помечаем как обработанную, оставляем в базе для повторной попытки
#                         continue
#                     else:
#                         # Успешно отправлено
#                         logging.info(f"Error notification (ID: {err_id}) sent to channel {ERROR_CHANNEL_CHAT_ID}.")
#                         # Помечаем ошибку как обработанную
#                         await mark_error_processed_async(err_id)
#                         logging.debug(f"Marked error (ID: {err_id}) as processed in DB.")
#         except Exception as e:
#             logging.error(f"Unexpected error in monitor_child_bot_db_errors loop: {e}", exc_info=True)
#         except ClientConnectorError as e:
#             logging.warning(f"Network error (DNS/connection): {e}. Retrying in 30s...")
#             await asyncio.sleep(30)
#             continue 
#         # 5. Ждем перед следующей проверкой
#         await asyncio.sleep(ERROR_CHECK_INTERVAL)


async def monitor_child_bot_db_errors():
    """
    Фоновая задача: мониторит таблицу ChildBotError,
    читает непрочитанные записи и отправляет их в канал через основного бота.
    """
    if not ERROR_CHANNEL_CHAT_ID:
        logging.warning("ERROR_CHANNEL_CHAT_ID environment variable is not set. Error notifications will not be sent.")
        return
    logging.info("Started DB error monitoring task.")
    
    # Ключевые слова для фильтрации ошибок flood control
    FLOOD_CONTROL_KEYWORDS = [
        "Flood control exceeded",
        "Too Many Requests",
        "retry after",
        "retry_after",
        "429: Too Many Requests"
    ]
    
    while True:
        try:
            # 1. Получаем ошибки асинхронно
            unprocessed_errors = await fetch_unprocessed_errors_async()
            if not unprocessed_errors:
                # Если ошибок нет, просто ждем следующего цикла
                await asyncio.sleep(ERROR_CHECK_INTERVAL)
                continue
                
            logging.info(f"Found {len(unprocessed_errors)} unprocessed errors to process")
            
            # 2. Фильтруем ошибки flood control перед отправкой
            filtered_errors = []
            flood_control_ids = []
            
            for error_data in unprocessed_errors:
                err_id = error_data["id"]
                raw_msg = error_data["error_message"] or ""
                
                # Проверяем, является ли ошибка ошибкой flood control
                is_flood_control = any(
                    keyword.lower() in raw_msg.lower() 
                    for keyword in FLOOD_CONTROL_KEYWORDS
                )
                
                if is_flood_control:
                    logging.info(f"Skipping flood control error ID {err_id}: '{raw_msg[:100]}...'")
                    flood_control_ids.append(err_id)
                else:
                    filtered_errors.append(error_data)
            
            # 3. Помечаем ошибки flood control как обработанные, но не отправляем их
            for err_id in flood_control_ids:
                await mark_error_processed_async(err_id)
                logging.info(f"Marked flood control error ID {err_id} as processed (without sending)")
            
            # 4. Отправляем только отфильтрованные ошибки
            for i, error_data in enumerate(filtered_errors):
                err_id = error_data["id"]
                bot_uuid = error_data["bot_uuid"]
                raw_msg = error_data["error_message"] or "No message"
                
                try:
                    from html import escape
                    # Обрезка длинного сообщения
                    MAX_LEN = 3000
                    if len(raw_msg) > MAX_LEN:
                        raw_msg = raw_msg[:MAX_LEN] + "... [ОБРЕЗАНО]"
                    safe_error_msg = escape(raw_msg)
                    safe_bot_uuid = escape(bot_uuid)
                    message_to_send = (
                        f"⚠️ Ошибка в боте <b>{safe_bot_uuid}</b> (ID ошибки: {err_id}):\n"
                        f"<code>{safe_error_msg}</code>"
                    )
                    
                    # Пытаемся отправить сообщение
                    try:
                        await bot.send_message(ERROR_CHANNEL_CHAT_ID, message_to_send, parse_mode='HTML')
                        logging.info(f"Error notification (ID: {err_id}) sent to channel {ERROR_CHANNEL_CHAT_ID}.")
                        await mark_error_processed_async(err_id)
                    except TelegramRetryAfter as e:
                        # Обработка ошибки flood control
                        logging.warning(f"Flood control exceeded. Waiting {e.retry_after} seconds before retry.")
                        await asyncio.sleep(e.retry_after + 5)  # Дополнительная задержка
                        # Не прерываем цикл, продолжаем после ожидания
                        continue
                    except Exception as send_err:
                        logging.error(f"Failed to send error notification (ID: {err_id}): {send_err}")
                        # Для критических ошибок всё же помечаем как обработанные
                        error_str = str(send_err)
                        if any(critical in error_str for critical in ["400 Bad Request", "403 Forbidden", "PEER_ID_INVALID", "chat not found"]):
                            logging.warning(f"Marking error ID {err_id} as processed despite send failure")
                            await mark_error_processed_async(err_id)
                        continue
                except Exception as msg_prep_err:
                    logging.error(f"Error preparing message for error ID {err_id}: {msg_prep_err}")
                    await mark_error_processed_async(err_id)
                
                # Добавляем задержку между отправками для избежания flood control
                if i < len(filtered_errors) - 1:  # Не добавляем задержку после последнего сообщения
                    await asyncio.sleep(3.0)  # Безопасная задержка между сообщениями
            
        except ClientConnectorError as e:
            logging.warning(f"Network error: {e}. Retrying in 30s...")
            await asyncio.sleep(30)
        except Exception as e:
            logging.error(f"Unexpected error in monitoring loop: {e}", exc_info=True)
            await asyncio.sleep(5)
        
        # Ждем перед следующей проверкой
        await asyncio.sleep(ERROR_CHECK_INTERVAL)

async def clear_all_errors_and_reset_counter():
    """Удаляет ВСЕ записи из таблицы ошибок и сбрасывает счетчик ID до 1."""
    logging.info("🧹 Начинаю ПОЛНУЮ очистку таблицы ошибок и сброс счетчика...")
    
    async for db in get_async_db():
        try:
            # 1. Удаляем все записи из таблицы
            from sqlalchemy import text
            
            # Используем TRUNCATE с RESTART IDENTITY для удаления всех записей и сброса счетчика
            await db.execute(text("TRUNCATE TABLE child_bot_errors RESTART IDENTITY"))
            await db.commit()
            
            logging.error("✅ УСПЕШНО удалены ВСЕ записи и сброшен счетчик ID до 1.")
            return True
            
        except Exception as e:
            logging.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА при очистке таблицы: {e}")
            await db.rollback()
            return False


def cleanup_bots():
    bot_manager.stop_all()

atexit.register(cleanup_bots)

def signal_handler(sig, frame):
    logging.info("Received termination signal. Cleaning up...")
    cleanup_bots()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

async def scheduled_db_cleanup():
    """Фоновая задача: удаляет старые логи (асинхронно для Loop)."""
    while True:
        try:
            logging.info("🧹 Starting scheduled DB cleanup...")
            # Запуск в потоке не требуется, используем асинхронную функцию
            async for db in get_async_db():
                count = 0
                try:
                    count = await delete_old_errors(db, retention_days=7)
                except Exception as e:
                    logging.error(f"Cleanup error: {e}")
                break # Выходим из цикла after one iteration
            if count > 0:
                logging.info(f"✅ DB Cleanup: Deleted {count} old logs.")
        except Exception as e:
            logging.critical(f"Error in cleanup task: {e}")
        await asyncio.sleep(86400) # 24 часа

async def main():
    await init_db_async()
    monitor_db_task = asyncio.create_task(monitor_child_bot_db_errors())
    cleanup_task = asyncio.create_task(scheduled_db_cleanup())
    try:
        # Запускаем основной polling основного бота
        await dp.start_polling(bot) # Используем await вместо asyncio.run
    finally:
        # Отменяем задачу мониторинга при завершении
        monitor_db_task.cancel()
        cleanup_task.cancel()
        try:
            await monitor_db_task # Теперь await используется внутри async функции
        except asyncio.CancelledError:
            pass # Ожидаемое поведение при отмене
        logging.info("Main polling stopped and DB monitor task cancelled.")

if __name__ == "__main__":
    asyncio.run(main())
