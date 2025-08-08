from PySide6.QtCore import QThread, Signal, QMutex, QTimer
import requests
from utils import *
from config import *


class MetricsManager(QThread):
    sendNow = Signal()

    def __init__(
        self, parent=None, name: str = "MetricsManager", batch_limit: int = 50000
    ):
        super().__init__(parent)
        self.setObjectName(name)
        self.batch_limit = batch_limit
        self.buffer = []
        self.mutex = QMutex()
        self.running = True
        self.sendNow.connect(self._send_batch)
        self.timer = QTimer()
        self.timer.setInterval(1000)
        self.timer.timeout.connect(self.check_and_send)
        self.timer.moveToThread(self)
        self.start()

    def add_metric(self, metric_line: str):
        self.mutex.lock()
        self.buffer.append(metric_line)
        if len(self.buffer) >= self.batch_limit:
            self.sendNow.emit()
        self.mutex.unlock()

    def set_batch_limit(self, limit: int):
        self.batch_limit = limit

    def _send_batch(self):
        self.mutex.lock()
        if self.buffer:
            try:
                # requests.post(vm_import_url, data="".join(self.buffer))
                print("".join(self.buffer))  # For debugging purposes
            except requests.RequestException as e:
                print(f"⚠️ Error sending metrics batch: {e}")
            finally:
                self.buffer = []
        self.mutex.unlock()

    def check_and_send(self):
        self.mutex.lock()
        if self.buffer:
            self._send_batch()
        self.mutex.unlock()

    def run(self):
        self.timer.start()
        while self.running:
            self.msleep(100)  # Keep thread alive

    def stop(self):
        self.running = False
        self.timer.stop()
        self.wait()
        self.mutex.lock()
        if self.buffer:
            self._send_batch()
        self.mutex.unlock()
