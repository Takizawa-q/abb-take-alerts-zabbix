import logging
import os
import sys
from logging.handlers import RotatingFileHandler

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)  # Создаём папку для логов, если её нет

LOG_FILE = os.path.join(LOG_DIR, "zabbix.log")
logger = logging.getLogger("zabbix_parser")

# Проверяем, что обработчики не добавлены (избегаем дублирования)
if not logger.hasHandlers():
    logger.setLevel(logging.INFO)

    # Форматтер для сообщений
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s %(module)s - %(funcName)s: %(message)s"
    )

    # Обработчик для консоли
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Обработчик для файла с ротацией
    try:
        file_handler = RotatingFileHandler(
            LOG_FILE,
            encoding="utf-8",
            maxBytes=50 * 1024 * 1024,  # Уменьшил размер до 50MB
            backupCount=3,
            delay=True  # Добавил delay=True для Windows
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception as e:
        # Если не удается создать RotatingFileHandler, используем обычный FileHandler
        try:
            file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            logger.warning(f"Using regular FileHandler instead of RotatingFileHandler: {e}")
        except Exception as e2:
            logger.error(f"Failed to create file handler: {e2}")

    # Отключаем распространение логов к root logger
    logger.propagate = False