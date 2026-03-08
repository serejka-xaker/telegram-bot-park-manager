FROM python:3.11-slim

WORKDIR /app

# Копируем requirements.txt из корня проекта
COPY requirements.txt .

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь код из папки app
COPY app/ .

# Запуск скрипта
CMD ["python", "bot.py"]
