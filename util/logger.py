import logging
import os
from datetime import datetime
import inspect

class Logger:

    def __init__(self, log_dir=None, log_filename=None):
        # Si no se pasa log_dir, usar nombre del módulo que llamó al Logger
        if log_dir is None:
            caller_frame = inspect.stack()[1]
            caller_file = caller_frame.filename
            caller_module = os.path.basename(os.path.dirname(caller_file))
            log_dir = os.path.join(
                os.path.dirname(os.path.abspath(os.path.join(__file__, '..', '..'))),
                caller_module, 'log'
            )
        os.makedirs(log_dir, exist_ok=True)

        if log_filename is None:
            log_filename = f"log_{datetime.now().strftime('%Y-%m-%d')}.txt"

        self.log_path = os.path.join(log_dir, log_filename)


    def info(self, msg):
        self._write_log("INFO", msg)

    def error(self, msg):
        self._write_log("ERROR", msg)

    def warning(self, msg):
        self._write_log("WARNING", msg)

    def _write_log(self, level, msg):
        with open(self.log_path, 'a', encoding='utf-8') as f:
            f.write(f"[{level}] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {msg}\n")


