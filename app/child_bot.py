# child_bot.py
import argparse
import asyncio
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.filters import Command
from dotenv import load_dotenv
import os
import base64
import aiohttp
import logging # Оставлен для логирования в консоль, но не для ошибок
# --- ОБНОВЛЕНО: Импорты для асинхронной БД ---
from database.database_async import get_async_db, AsyncSessionLocal
from database.models import Bot as BotModel, AccessRequest
from database.crud_async import add_access_request, get_merchant,create_order,get_order_by_payassist_id,update_order_status # Импортируем нужные *асинхронные* функции
# --- НОВОЕ: Импорты для записи ошибок в БД ---
from database.crud_async import add_child_bot_error # Используем асинхронную версию
# --- КОНЕЦ НОВОЕ ---
from datetime import datetime, timezone # Добавляем timezone к импорту
import uuid
from sqlalchemy import select # Необходимо для асинхронных запросов

# --- НАСТРОЙКА ЛОГИРОВАНИЯ (только для консоли) ---
# Убираем глобальный basicConfig
# logging.basicConfig(level=logging.DEBUG)

# Создаём форматтер
formatter = logging.Formatter(
    fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Создаём общий хендлер (например, для вывода в консоль)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG) # Уровень хендлера
console_handler.setFormatter(formatter)

load_dotenv()

parser = argparse.ArgumentParser()
parser.add_argument("--token", required=True)
parser.add_argument("--payassist-api-key", required=False, default=None)
parser.add_argument("--bot-uuid", required=True) # Добавляем аргумент для UUID
args = parser.parse_args()
TOKEN = args.token
PAYASSIST_API_KEY = args.payassist_api_key
BOT_UUID = args.bot_uuid # Получаем UUID

# --- СОЗДАЁМ ЛОГГЕР С ИМЕНЕМ, ВКЛЮЧАЮЩИМ BOT_UUID (только для отладки, не для ошибок) ---
logger_name = f"child_bot_{BOT_UUID}"
logger = logging.getLogger(logger_name)
logger.setLevel(logging.DEBUG) # Уровень логгера

# Добавляем хендлер к логгеру
logger.addHandler(console_handler)

# Кодируем PAYASSIST_API_KEY в Base64
PAYASSIST_API_KEY_BASE64 = None
if PAYASSIST_API_KEY:
    encoded_bytes = base64.b64encode(PAYASSIST_API_KEY.encode('utf-8'))
    PAYASSIST_API_KEY_BASE64 = encoded_bytes.decode('utf-8')

bot = Bot(token=TOKEN)
dp = Dispatcher()

PAYASSIST_BASE_URL = 'https://api.payassist.io/p2p'

# --- ХРАНЕНИЕ СОСТОЯНИЯ ПЛАТЕЖЕЙ ---
# Для простоты используем словарь. В реальном приложении используйте БД.
# Структура: { (user_id, message_id): order_id }
# payment_orders = {}

async def get_balance():
    """Функция для получения баланса с помощью API PayAssist."""
    if not PAYASSIST_API_KEY_BASE64:
        return "API ключ не указан."

    headers = {
        'Authorization': f'Basic {PAYASSIST_API_KEY_BASE64}',
        'Content-Type': 'application/json'
    }

    data = {
        "header": {
            "txName": "accountsBalance"
        },
        "reqData": {
            "provider": ["CARD"],
            "currency": ["RUB"]
        }
    }

    # --- ОБНОВЛЕНО: try...except для записи ошибки в БД ---
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f'{PAYASSIST_BASE_URL}/accounts/fetch-balance',
                headers=headers,
                json=data
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    return result
                else:
                    error_msg = f"Balance API request failed with status {response.status}: {await response.text()}"
                    # Записываем ошибку в БД
                    async for db in get_async_db():
                        try:
                            await add_child_bot_error(db, BOT_UUID, error_msg, "ERROR")
                            await db.commit()
                        except Exception as db_err:
                            print(f"CRITICAL: Could not log error to DB: {db_err}")
                            await db.rollback()
                        finally:
                            await db.close() # Убедимся, что сессия закрыта
                    return f"Ошибка: {response.status}, {await response.text()}"
    except Exception as e:
        error_msg = f"Balance API request exception: {e}"
        # Записываем ошибку в БД
        async for db in get_async_db():
            try:
                await add_child_bot_error(db, BOT_UUID, error_msg, "ERROR")
                await db.commit()
            except Exception as db_err:
                print(f"CRITICAL: Could not log error to DB: {db_err}")
                await db.rollback()
            finally:
                await db.close() # Убедимся, что сессия закрыта
        return f"Ошибка при выполнении запроса: {e}"

def format_balance_response(response_data):
    """Форматирует ответ API в читаемый вид."""
    if not response_data.get("result", {}).get("status"):
        return f"Ошибка: {response_data.get('result', {}).get('message', 'Неизвестная ошибка')}"

    accounts = response_data.get("responseData", {}).get("accounts", [])
    user_id = response_data.get("responseData", {}).get("user_id")

    if not accounts:
        return "Счета не найдены."

    output = f"💳 Информация о балансе:\n\n"
    output += f"👤 ID пользователя: `{user_id}`\n\n"

    for i, account in enumerate(accounts, start=1):
        currency = account.get("currency", "N/A")
        provider = account.get("provider", "N/A")
        balance = account.get("sum", 0)
        address = account.get("address", "N/A")
        can_deposit = "✅" if account.get("deposit") else "❌"
        can_withdraw = "✅" if account.get("withdraw") else "❌"
        min_withdraw = account.get("minWithdraw", "Не указан")

        output += f"### Счёт #{i}:\n"
        output += f"   - Адрес: `{address}`\n"
        output += f"   - Платформа: {provider}\n"
        output += f"   - Валюта: **{currency}**\n"
        output += f"   - Баланс: **{balance} {currency}**\n"
        output += f"   - Мин. сумма вывода: `{min_withdraw} {currency}`\n"
        output += f"   - Пополнение: {can_deposit}\n"
        output += f"   - Вывод: {can_withdraw}\n\n"

    return output

# --- КНОПКА ДЛЯ ПОЛУЧЕНИЯ ДОСТУПА ---
def get_access_kb():
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔑 Получить доступ", callback_data="request_access")]
        ]
    )
    return kb

# --- КНОПКА ДЛЯ ПРОВЕРКИ СТАТУСА ---
def check_payment_kb(payassist_order_id: str):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Проверить", callback_data=f"check_payment_{payassist_order_id}")]
        ]
    )
    return kb

# --- НЕАКТИВНАЯ КНОПКА ---
def inactive_kb():
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Успешно оплачено", callback_data="payment_success")] # Неактивная кнопка
        ]
    )
    return kb

@dp.message(Command("start"))
async def cmd_start(message: Message):
    # Проверяем, есть ли пользователь в списке мерчантов для этого бота
    # --- ОБНОВЛЕНО: try...except для записи ошибки в БД ---
    async for db in get_async_db():
        try:
            # Нужно получить ID бота из базы по UUID
            result = await db.execute(select(BotModel).filter(BotModel.uuid == BOT_UUID))
            bot_record = result.scalar_one_or_none()
            if not bot_record:
                # Записываем ошибку в БД
                error_msg = f"Bot record with UUID {BOT_UUID} not found in database!"
                try:
                    await add_child_bot_error(db, BOT_UUID, error_msg, "ERROR")
                    await db.commit()
                except Exception as db_err:
                    print(f"CRITICAL: Could not log error to DB: {db_err}")
                    await db.rollback()
                await message.answer("❌ Внутренняя ошибка: информация о боте не найдена.")
                return

            merchant = await get_merchant(db, message.from_user.id, bot_record.id)
            # await db.commit() # Не обязательно для SELECT
        except Exception as e: # Ловим общее исключение
            await db.rollback()
            # Записываем ошибку в БД
            error_msg = f"Error checking merchant status: {e}"
            try:
                await add_child_bot_error(db, BOT_UUID, error_msg, "ERROR")
                await db.commit()
            except Exception as db_err:
                print(f"CRITICAL: Could not log error to DB: {db_err}")
                await db.rollback()
            await message.answer("❌ Произошла внутренняя ошибка.")
            return
        finally:
            await db.close()

    if merchant:
        # --- ОБНОВЛЕНО: Добавлена подсказка по командам ---
        await message.answer(
            "Добро пожаловать! Список доступных команд:\n"
            "/balance - Проверить баланс платежного аккаунта\n"
            "/pay [номер карты] [сумма] - Создать платежную ссылку для оплаты картой\n"
            "/paysbp [сумма] - Создать платежную ссылку для оплаты через СБП"
        )
    else:
        await message.answer("Привет! Нажмите кнопку ниже, чтобы запросить доступ.", reply_markup=get_access_kb())

@dp.callback_query(F.data == "request_access")
async def request_access_callback(query: CallbackQuery):
    user = query.from_user
    user_id = user.id
    username = user.username
    full_name = user.full_name

    # --- ОБНОВЛЕНО: try...except для записи ошибки в БД ---
    async for db in get_async_db():
        try:
            # Получаем запись бота по UUID
            result = await db.execute(select(BotModel).filter(BotModel.uuid == BOT_UUID))
            bot_record = result.scalar_one_or_none()
            if not bot_record:
                # Записываем ошибку в БД
                error_msg = f"Bot record with UUID {BOT_UUID} not found in database!"
                try:
                    await add_child_bot_error(db, BOT_UUID, error_msg, "ERROR")
                    await db.commit()
                except Exception as db_err:
                    print(f"CRITICAL: Could not log error to DB: {db_err}")
                    await db.rollback()
                await query.message.edit_text("❌ Внутренняя ошибка: информация о боте не найдена.")
                await query.answer()
                return

            # Проверяем, нет ли уже активной заявки
            result = await db.execute(
                select(AccessRequest).filter(
                    AccessRequest.telegram_id == user_id,
                    AccessRequest.bot_id == bot_record.id,
                    AccessRequest.approved == False
                )
            )
            existing_request = result.scalar_one_or_none()
            if existing_request:
                await query.message.edit_text("❌ Ваша заявка на доступ уже подана и находится на рассмотрении.")
                await query.answer()
                return

            # Проверяем, нет ли уже одобренной заявки (мерчанта)
            merchant = await get_merchant(db, user_id, bot_record.id)
            if merchant:
                await query.message.edit_text("❌ У вас уже есть доступ к этому боту.")
                await query.answer()
                return

            # Добавляем заявку в базу данных главного бота
            await add_access_request(db, user_id, bot_record.id, username, full_name)
            await db.commit()
            logger.info(f"Access request added for user {user_id} to bot {BOT_UUID}.")

            await query.message.edit_text("✅ Заявка отправлена. Ожидайте решения администратора.")
        except Exception as e: # Ловим общее исключение
            await db.rollback()
            # Записываем ошибку в БД
            error_msg = f"Error processing access request: {e}"
            try:
                await add_child_bot_error(db, BOT_UUID, error_msg, "ERROR")
                await db.commit()
            except Exception as db_err:
                print(f"CRITICAL: Could not log error to DB: {db_err}")
                await db.rollback()
            await query.message.edit_text("❌ Произошла ошибка при отправке заявки.")
        finally:
            await db.close()
    await query.answer()

# --- КОМАНДА /pay ---
@dp.message(Command("pay"))
async def cmd_pay(message: Message):
    # --- ОБНОВЛЕНО: try...except для записи ошибки в БД ---
    async for db in get_async_db():
        try:
            # Проверяем мерчанта
            result = await db.execute(select(BotModel).filter(BotModel.uuid == BOT_UUID))
            bot_record = result.scalar_one_or_none()
            if not bot_record:
                # Записываем ошибку в БД
                error_msg = f"Bot record with UUID {BOT_UUID} not found in database!"
                try:
                    await add_child_bot_error(db, BOT_UUID, error_msg, "ERROR")
                    await db.commit()
                except Exception as db_err:
                    print(f"CRITICAL: Could not log error to DB: {db_err}")
                    await db.rollback()
                await message.reply("❌ Внутренняя ошибка: информация о боте не найдена.")
                return

            merchant = await get_merchant(db, message.from_user.id, bot_record.id)
            if not merchant:
                await message.reply("❌ У вас нет доступа к этому функционалу.")
                return

            # Разбор аргументов
            args = message.text.split()
            if len(args) != 3:
                await message.reply("❌ Неверный формат команды. Используйте: /pay <card_id> <amount>")
                return

            card_id = args[1]
            amount_str = args[2]

            # Проверка card_id (16 цифр)
            if not card_id.isdigit() or len(card_id) != 16:
                await message.reply("❌ Неверный формат card_id. Ожидается 16-значное число.")
                return

            # Проверка amount
            try:
                amount = float(amount_str)
                if amount <= 0:
                    raise ValueError("Сумма должна быть положительной")
            except ValueError:
                await message.reply("❌ Неверный формат amount. Ожидается число.")
                return

            # Формирование запроса к API
            if not PAYASSIST_API_KEY_BASE64:
                await message.reply("❌ API ключ PayAssist не настроен.")
                return

            headers = {
                'Authorization': f'Basic {PAYASSIST_API_KEY_BASE64}',
                'Content-Type': 'application/json'
            }

            # --- ИСПОЛЬЗУЕМ МИНИМАЛЬНЫЙ НАБОР ПОЛЕЙ ДЛЯ /pay ---
            api_data = {
                "header": {"txName": "CreateBillInvoice"},
                "reqData": {
                    "amount": amount,
                    "currency": "RUB",
                    "address": card_id[-4:], # Последние 4 цифры карты
                    "recipient": BOT_UUID,
                }
            }

            logger.info(f"Sending /pay request to API. Endpoint: {PAYASSIST_BASE_URL}/bill-payment/invoice/create, Data: {api_data}")

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f'{PAYASSIST_BASE_URL}/bill-payment/invoice/create', # <-- Правильный эндпоинт
                    headers=headers,
                    json=api_data
                ) as response:
                    logger.info(f"PayAssist API responded with status: {response.status}")
                    if response.status == 200:
                        api_response = await response.json()
                        logger.info(f"PayAssist API full response: {api_response}")
                        if api_response.get("result", {}).get("status"):
                            order_id = api_response.get("responseData", {}).get("_id")
                            payment_url = api_response.get("responseData", {}).get("paymentLink")

                            if order_id and payment_url:
                                order_record = await create_order(
                                    db,
                                    merchant_id=merchant.id,
                                    bot_id=bot_record.id,
                                    card_id=card_id, # Сохраняем card_id
                                    amount=amount,
                                    payassist_order_id=order_id
                                )

                                await message.reply(f"Ссылка на оплату: {payment_url}", reply_markup=check_payment_kb(order_id))
                            else:
                                # API не вернул ожидаемые поля
                                error_msg = f"API response missing 'paymentLink' or '_id': {api_response}"
                                try:
                                    await add_child_bot_error(db, BOT_UUID, error_msg, "ERROR")
                                    await db.commit()
                                except Exception as db_err:
                                    print(f"CRITICAL: Could not log error to DB: {db_err}")
                                    await db.rollback()
                                await message.reply("❌ Не удалось создать ссылку. API вернул неожиданный ответ.")
                        else:
                            # API вернул ошибку
                            error_msg = api_response.get("result", {}).get("message", "Неизвестная ошибка API")
                            try:
                                await add_child_bot_error(db, BOT_UUID, error_msg, "ERROR")
                                await db.commit()
                            except Exception as db_err:
                                print(f"CRITICAL: Could not log error to DB: {db_err}")
                                await db.rollback()
                            await message.reply(f"❌ Не удалось создать ссылку. Ошибка API: {error_msg}")
                    else:
                        # --- НОВОЕ: Обработка случая, когда status != 200 ---
                        error_text_body = await response.text() # Читаем тело ответа как текст
                        error_msg = f"PayAssist API request failed with status {response.status}. Response body: {error_text_body}"
                        try:
                            await add_child_bot_error(db, BOT_UUID, error_msg, "ERROR")
                            await db.commit()
                        except Exception as db_err:
                            print(f"CRITICAL: Could not log error to DB: {db_err}")
                            await db.rollback()
                        # Возвращаем пользователю более информативное сообщение, возможно, включая статус
                        await message.reply(f"❌ Не удалось создать ссылку. Ошибка API: Request failed with status {response.status}. Details {error_text_body}.")

        except Exception as e: # Ловим общее исключение
            # Записываем ошибку в БД
            error_msg = f"Error in /pay command: {e}"
            try:
                await add_child_bot_error(db, BOT_UUID, error_msg, "ERROR")
                await db.commit()
            except Exception as db_err:
                print(f"CRITICAL: Could not log error to DB: {db_err}")
                await db.rollback()
            await message.reply(f"❌ Произошла ошибка при обработке команды /pay. {e}")
        finally:
            await db.close()


@dp.message(Command("paysbp"))
async def cmd_paysbp(message: Message):
    # --- ОБНОВЛЕНО: try...except для записи ошибки в БД ---
    async for db in get_async_db():
        try:
            # Проверяем мерчанта
            result = await db.execute(select(BotModel).filter(BotModel.uuid == BOT_UUID))
            bot_record = result.scalar_one_or_none()
            if not bot_record:
                # Записываем ошибку в БД
                error_msg = f"Bot record with UUID {BOT_UUID} not found in database!"
                try:
                    await add_child_bot_error(db, BOT_UUID, error_msg, "ERROR")
                    await db.commit()
                except Exception as db_err:
                    print(f"CRITICAL: Could not log error to DB: {db_err}")
                    await db.rollback()
                await message.reply("❌ Внутренняя ошибка: информация о боте не найдена.")
                return

            merchant = await get_merchant(db, message.from_user.id, bot_record.id)
            if not merchant:
                await message.reply("❌ У вас нет доступа к этому функционалу.")
                return

            # Разбор аргументов
            args = message.text.split()
            if len(args) != 2: # --- ИЗМЕНЕНО: Ожидаем только сумму ---
                await message.reply("❌ Неверный формат команды. Используйте: /paysbp <amount>")
                return

            amount_str = args[1] # --- ИЗМЕНЕНО: получаем сумму из второго аргумента ---

            # Проверка amount
            try:
                amount = float(amount_str)
                if amount <= 0:
                    raise ValueError("Сумма должна быть положительной")
            except ValueError:
                await message.reply("❌ Неверный формат amount. Ожидается число.")
                return

            # Формирование запроса к API
            if not PAYASSIST_API_KEY_BASE64:
                await message.reply("❌ API ключ PayAssist не настроен.")
                return

            headers = {
                'Authorization': f'Basic {PAYASSIST_API_KEY_BASE64}',
                'Content-Type': 'application/json'
            }

            # card_id = '2200152945809328'
            # api_data = {
            #     "header": {"txName": "CreateBillInvoice"},
            #     "reqData": {
            #         "amount": amount,
            #         # "address": card_id[-4:], # Последние 4 цифры карты
            #         "recipient": BOT_UUID,
            #         "paySource": "sbp",
            #     }
            # }

            api_data = {
                "header": {"txName": "CreateBillInvoice"},
                "reqData": {
                    "amount": amount,
                    "currency": "RUB",
                    "paySource": "sbp",
                    "recipient": BOT_UUID,
                }
            }


            logger.info(f"Sending /paysbp request to API. Endpoint: {PAYASSIST_BASE_URL}/bill-payment/invoice/create, Data: {api_data}")

            async with aiohttp.ClientSession() as session:
                # Используем правильный эндпоинт для обычного инвойса
                api_endpoint = f'{PAYASSIST_BASE_URL}/bill-payment/invoice/create'
                async with session.post(
                    api_endpoint,
                    headers=headers,
                    json=api_data
                ) as response:
                    logger.info(f"PayAssist SBP API responded with status: {response.status}")
                    # --- ПОКАЖЕМ ТЕЛО ОТВЕТА ДЛЯ ДИАГНОСТИКИ ---
                    response_text = await response.text()
                    logger.info(f"PayAssist SBP API response body: {response_text}")
                    # --- КОНЕЦ ДИАГНОСТИКИ ---
                    if response.status == 200:
                        api_response = await response.json()
                        logger.info(f"PayAssist SBP API full response: {api_response}")
                        if api_response.get("result", {}).get("status"):
                            order_id = api_response.get("responseData", {}).get("_id")
                            payment_url = api_response.get("responseData", {}).get("paymentLink")

                            if order_id and payment_url:
                                # Создаём запись заказа в БД
                                # card_id для SBP сохраняем как ID пользователя (или можно оставить пустым/заглушкой)
                                order_record = await create_order(
                                    db,
                                    merchant_id=merchant.id,
                                    bot_id=bot_record.id,
                                    card_id=str(message.from_user.id), # --- СОХРАНЯЕМ ID ПОЛЬЗОВАТЕЛЯ ---
                                    amount=amount,
                                    payassist_order_id=order_id
                                )

                                await message.reply(f"Ссылка на оплату через СБП: {payment_url}", reply_markup=check_payment_kb(order_id))
                            else:
                                # API не вернул ожидаемые поля (order_id или paymentLink)
                                error_msg = f"API response missing 'order_id' or 'paymentLink' for SBP: {api_response}"
                                try:
                                    await add_child_bot_error(db, BOT_UUID, error_msg, "ERROR")
                                    await db.commit()
                                except Exception as db_err:
                                    print(f"CRITICAL: Could not log error to DB: {db_err}")
                                    await db.rollback()
                                await message.reply("❌ Не удалось создать счёт для СБП. API вернул неожиданный ответ (отсутствует ID заказа или ссылка).")
                        else:
                            # API вернул ошибку
                            error_msg = api_response.get("result", {}).get("message", "Неизвестная ошибка API")
                            try:
                                await add_child_bot_error(db, BOT_UUID, error_msg, "ERROR")
                                await db.commit()
                            except Exception as db_err:
                                print(f"CRITICAL: Could not log error to DB: {db_err}")
                                await db.rollback()
                            await message.reply(f"❌ Не удалось создать счёт для СБП. Ошибка API: {error_msg}")
                    else:
                        # --- НОВОЕ: Обработка случая, когда status != 200 ---
                        # error_text_body = await response.text() # Читаем тело ответа как текст (уже сделано выше как response_text)
                        error_msg = f"PayAssist SBP API request failed with status {response.status}. Response body: {response_text}"
                        try:
                            await add_child_bot_error(db, BOT_UUID, error_msg, "ERROR")
                            await db.commit()
                        except Exception as db_err:
                            print(f"CRITICAL: Could not log error to DB: {db_err}")
                            await db.rollback()
                        # Возвращаем пользователю более информативное сообщение, возможно, включая статус
                        await message.reply(f"❌ Не удалось создать счёт для СБП. Ошибка API: Request failed with status {response.status}. Details {response_text}.")

        except Exception as e: # Ловим общее исключение
            # Записываем ошибку в БД
            error_msg = f"Error in /paysbp command: {e}"
            try:
                await add_child_bot_error(db, BOT_UUID, error_msg, "ERROR")
                await db.commit()
            except Exception as db_err:
                print(f"CRITICAL: Could not log error to DB: {db_err}")
                await db.rollback()
            await message.reply(f"❌ Произошла ошибка при обработке команды /paysbp. {e}")
        finally:
            await db.close()


# --- КОМАНДА /balance ---
@dp.message(Command("balance"))
async def cmd_balance(message: Message):
    # --- ОБНОВЛЕНО: try...except для записи ошибки в БД ---
    async for db in get_async_db():
        try:
            # Проверяем мерчанта
            result = await db.execute(select(BotModel).filter(BotModel.uuid == BOT_UUID))
            bot_record = result.scalar_one_or_none()
            if not bot_record:
                # Записываем ошибку в БД
                error_msg = f"Bot record with UUID {BOT_UUID} not found in database!"
                try:
                    await add_child_bot_error(db, BOT_UUID, error_msg, "ERROR")
                    await db.commit()
                except Exception as db_err:
                    print(f"CRITICAL: Could not log error to DB: {db_err}")
                    await db.rollback()
                await message.reply("❌ Внутренняя ошибка: информация о боте не найдена.")
                return

            merchant = await get_merchant(db, message.from_user.id, bot_record.id)
            if not merchant:
                await message.reply("❌ У вас нет доступа к этому функционалу.")
                return

            raw_data = await get_balance()
            if isinstance(raw_data, dict):
                if raw_data.get("result", {}).get("status"):
                    accounts = raw_data.get("responseData", {}).get("accounts", [])
                    rub_account = next((acc for acc in accounts if acc.get("currency") == "RUB"), None)
                    if rub_account:
                        balance = rub_account.get("sum", 0)
                        response_text = f"Ваш текущий баланс: {balance} RUB"
                    else:
                        response_text = "Баланс в RUB не найден."
                else:
                    error_msg = raw_data.get("result", {}).get("message", "Неизвестная ошибка")
                    response_text = f"Ошибка при получении баланса: {error_msg}"
            else:
                # Это строка с ошибкой
                response_text = raw_data

            await message.reply(response_text)

        except Exception as e: # Ловим общее исключение
            # Записываем ошибку в БД
            error_msg = f"Error in /balance command: {e}"
            try:
                await add_child_bot_error(db, BOT_UUID, error_msg, "ERROR")
                await db.commit()
            except Exception as db_err:
                print(f"CRITICAL: Could not log error to DB: {db_err}")
                await db.rollback()
            await message.reply("❌ Произошла ошибка при получении баланса.")
        finally:
            await db.close()

@dp.callback_query(F.data.startswith("check_payment_"))
async def check_payment_callback(query: CallbackQuery):
    # Извлекаем payassist_order_id из callback_data
    payassist_order_id = query.data.split('check_payment_', 1)[1] # "check_payment_<id>" -> "<id>"

    # --- ПОЛУЧАЕМ ЗАКАЗ ИЗ БАЗЫ ПО PAYASSIST ORDER ID ---
    # --- ОБНОВЛЕНО: try...except для записи ошибки в БД ---
    async for db in get_async_db():
        try:
            order_record = await get_order_by_payassist_id(db, payassist_order_id)
            if not order_record:
                # Не нашли order_id в БД, возможно, заказ был создан в другой сессии бота или не был сохранён корректно
                await query.answer(f"❌ Информация о платеже утеряна или не найдена в базе данных.", show_alert=True)
                return

            # Проверяем статус через API PayAssist
            if not PAYASSIST_API_KEY_BASE64:
                await query.answer("❌ API ключ PayAssist не настроен.", show_alert=True)
                return

            headers = {
                'Authorization': f'Basic {PAYASSIST_API_KEY_BASE64}',
                'Content-Type': 'application/json'
            }

            api_data = {
                "header": {"txName": "fetchOrderInfo"},
                "reqData": {"order_id": payassist_order_id}
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f'{PAYASSIST_BASE_URL}/transfer/get-order-info',
                    headers=headers,
                    json=api_data
                ) as api_response:
                    response_json = await api_response.json()
                    logger.info(f"Check status API response: {response_json}")

                    if api_response.status == 200 and response_json.get("result", {}).get("status"):
                        transactions = response_json.get("responseData", {}).get("transactions", [])
                        if transactions:
                            status = transactions[0].get("real_status", "UNKNOWN")
                            if status in ["DELIVERED", "APPROVED"]:
                                # Обновляем статус в БД
                                await update_order_status(db, order_record.id, "paid", payassist_order_id) # commit внутри
                                # Заменяем кнопку
                                await query.message.edit_reply_markup(reply_markup=inactive_kb())
                                await query.answer("✅ Успешно оплачено!", show_alert=True)
                            else:
                                await query.answer("❌ Ордер не оплачен.", show_alert=True)
                        else:
                            await query.answer("❌ Информация о транзакции отсутствует.", show_alert=True)
                    else:
                        error_msg = response_json.get("result", {}).get("message", f"HTTP {api_response.status}")
                        # Записываем ошибку в БД
                        try:
                            await add_child_bot_error(db, BOT_UUID, error_msg, "ERROR")
                            await db.commit()
                        except Exception as db_err:
                            print(f"CRITICAL: Could not log error to DB: {db_err}")
                            await db.rollback()
                        await query.answer(f"Ошибка API: {error_msg}", show_alert=True)

        except Exception as e: # Ловим общее исключение
            # Записываем ошибку в БД
            error_msg = f"Error checking payment status for order {payassist_order_id}: {e}"
            try:
                await add_child_bot_error(db, BOT_UUID, error_msg, "ERROR")
                await db.commit()
            except Exception as db_err:
                print(f"CRITICAL: Could not log error to DB: {db_err}")
                await db.rollback()
            await query.answer("❌ Произошла ошибка при проверке статуса.", show_alert=True)
        finally:
            await db.close()

@dp.message(F.text)
async def echo(message: Message):
    # Проверяем, есть ли пользователь в списке мерчантов для этого бота
    # --- ОБНОВЛЕНО: try...except для записи ошибки в БД ---
    async for db in get_async_db():
        try:
            # Нужно получить ID бота из базы по UUID
            result = await db.execute(select(BotModel).filter(BotModel.uuid == BOT_UUID))
            bot_record = result.scalar_one_or_none()
            if not bot_record:
                # Записываем ошибку в БД
                error_msg = f"Bot record with UUID {BOT_UUID} not found in database!"
                try:
                    await add_child_bot_error(db, BOT_UUID, error_msg, "ERROR")
                    await db.commit()
                except Exception as db_err:
                    print(f"CRITICAL: Could not log error to DB: {db_err}")
                    await db.rollback()
                await message.reply("❌ Внутренняя ошибка: информация о боте не найдена.")
                return

            merchant = await get_merchant(db, message.from_user.id, bot_record.id)
            # await db.commit() # Не обязательно для SELECT
        except Exception as e: # Ловим общее исключение
            await db.rollback()
            # Записываем ошибку в БД
            error_msg = f"Error checking merchant status: {e}"
            try:
                await add_child_bot_error(db, BOT_UUID, error_msg, "ERROR")
                await db.commit()
            except Exception as db_err:
                print(f"CRITICAL: Could not log error to DB: {db_err}")
                await db.rollback()
            await message.reply("❌ Произошла внутренняя ошибка.")
            return
        finally:
            await db.close()

    if not merchant:
        # Если пользователь не является мерчантом, просто игнорируем сообщение или отправляем приветствие
        await message.reply("Для доступа к функционалу бота, пожалуйста, запросите доступ через /start.")
        return

    # Только если пользователь является мерчантом, обрабатываем команды
    # Обработка команд /pay и /balance уже вынесена в отдельные хендлеры
    # Здесь можно обрабатывать другие текстовые сообщения, если нужно
    # или игнорировать
    # await message.reply(f"Привет! Вы написали: {message.text}")

if __name__ == "__main__":
    # Инициализация базы данных может быть здесь, если она используется в child_bot
    # init_db() # Необходимо, если child_bot использует общую БД
    asyncio.run(dp.start_polling(bot))
