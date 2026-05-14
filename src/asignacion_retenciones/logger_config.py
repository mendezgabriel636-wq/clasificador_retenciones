import logging
import os
from datetime import datetime
from pytz import timezone, utc

tz = timezone("America/Guayaquil")
os.makedirs("logs", exist_ok=True)

class LoggerManager:
    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        self.current_date = None

        log_format = "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"
        date_format = "%Y-%m-%d %H:%M:%S"
        formatter = logging.Formatter(log_format, datefmt=date_format)

        def custom_time(*args):
            utc_dt = utc.localize(datetime.utcnow())
            converted = utc_dt.astimezone(tz)
            return converted.timetuple()

        logging.Formatter.converter = custom_time

        self.formatter = formatter
        self._update_handlers_if_needed()

    def _get_log_filename(self):
        today = datetime.now(tz).strftime("%Y-%m-%d")
        return f"logs/logs_{today}.txt", today

    def _update_handlers_if_needed(self):
        log_filename, today = self._get_log_filename()

        if self.current_date != today:
            # Eliminar todos los handlers actuales
            for handler in self.logger.handlers[:]:
                self.logger.removeHandler(handler)

            # Crear nuevos handlers
            file_handler = logging.FileHandler(log_filename, mode="a", encoding="utf-8")
            file_handler.setFormatter(self.formatter)

            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(self.formatter)

            self.logger.addHandler(file_handler)
            self.logger.addHandler(stream_handler)

            self.current_date = today

    def info(self, message, *args, **kwargs):
        self._update_handlers_if_needed()
        self.logger.info(message, *args, **kwargs)

    def error(self, message, *args, **kwargs):
        self._update_handlers_if_needed()
        self.logger.error(message, *args, **kwargs)

    def warning(self, message, *args, **kwargs):
        self._update_handlers_if_needed()
        self.logger.warning(message, *args, **kwargs)
