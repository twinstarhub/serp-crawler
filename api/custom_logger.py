import logging
import os
from typing import Any


class ColoredFormatter(logging.Formatter):
    """A formatter to add colors to the log messages."""
    def __init__(self, *args, **kwargs):
        """Initialize the formatter."""
        super().__init__(*args, **kwargs)
        self._colors = {
            "ERROR": "\033[31m",
            "WARNING": "\033[33m",
            "SUCCESS": "\033[32m",
            "DEBUG": "\033[34m",
            "INFO": "\033[36m",
            "END": "\033[0m"
        }

    def format(self, record: logging.LogRecord) -> str:
        """Format the log message."""
        msg = super().format(record)
        if record.funcName == "success":
            return f"{self._colors['SUCCESS']}{msg.replace('INFO', 'SUCCESS')}{self._colors['END']}"
        if record.levelname in self._colors:
            return f"{self._colors[record.levelname]}{msg}{self._colors['END']}"
        return msg


class BaseLogger:
    """An abstract class for a custom logger."""
    def __init__(self, name: str):
        """Initialize the logger."""
        self.name = name
        self.logger = logging.getLogger(self.name)
        formatter = self.get_formatter()
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(os.getenv("LOG_LEVEL", "ERROR"))
        self.logger = self.get_adapter() or self.logger

    def __getattr__(self, __name: str) -> Any:
        """Class will act as a proxy for the logger attribute."""
        if __name == "logger":
            return self.__dict__.get(__name)
        return getattr(self.logger, __name)

    def get_formatter(self):
        """Return the formatter for the logger."""
        return ColoredFormatter("[%(name)s] %(message)s")

    def get_adapter(self):
        """Return the logger adapter."""
        return None

    def success(self, msg: str, *args, **kwargs):
        """Log a success message."""
        self.logger.log(logging.INFO, msg, *args, **kwargs)


class PlatformLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        """Add the platform as an extra field in the log records."""
        kwargs["extra"] = kwargs.get("extra", {})
        kwargs["extra"]["platform"] = self.extra["platform"]
        return msg, kwargs


class PlatformLogger(BaseLogger):
    """A custom logger for the platform."""
    def __init__(self, platform: str):
        """Initialize the logger."""
        super().__init__(platform)
    
    def get_formatter(self):
        """Return the formatter for the logger."""
        return ColoredFormatter("[%(levelname)s][%(platform)s][%(username)s] %(message)s")

    def get_adapter(self):
        """Return the logger adapter."""
        return PlatformLoggerAdapter(self.logger, {"platform": self.name})


class CacherLogger(BaseLogger):
    """A custom logger for the cacher."""
    def __init__(self):
        """Initialize the logger."""
        super().__init__("Cacher")

    def get_formatter(self):
        """Return the formatter for the logger."""
        return ColoredFormatter("[%(levelname)s][%(name)s] %(message)s")


class MongoLogger(BaseLogger):
    """A custom logger for the MongoDB."""
    def __init__(self):
        """Initialize the logger."""
        super().__init__("MongoDB")

    def get_formatter(self):
        """Return the formatter for the logger."""
        return ColoredFormatter("[%(levelname)s][%(name)s] %(message)s")
