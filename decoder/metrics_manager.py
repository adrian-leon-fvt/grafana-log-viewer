from PySide6.QtCore import QThread, Signal, QMutex, QTimer
import requests
from utils import *
from config import *


class MetricsManager(QThread):

    def __init__(
        self, parent=None, name: str = "MetricsManager", batch_limit: int = 50000
    ):
        super().__init__(parent)
        self.setObjectName(name)
        self.batch_limit = batch_limit
        self.buffer = []
        self.mutex = QMutex()
        self.running = True
        self.start()

    def add_metric(self, metric_line: str):
        self.mutex.lock()
        self.buffer.append(metric_line)
        if len(self.buffer) >= self.batch_limit:
            self._send_batch()
        self.mutex.unlock()

    def set_batch_limit(self, limit: int):
        self.batch_limit = limit

    def _send_batch(self):
        # Only lock for the minimum time needed
        self.mutex.lock()
        batch = self.buffer.copy()
        self.buffer = []
        self.mutex.unlock()
        if batch:
            try:
                requests.post(vm_import_url, data="".join(batch))
                # print("".join(batch))  # For debugging purposes
            except requests.RequestException as e:
                print(f"⚠️ Error sending metrics batch: {e}")

    def check_and_send(self):
        self.mutex.lock()
        has_data = bool(self.buffer)
        self.mutex.unlock()
        if has_data:
            self._send_batch()

    def run(self):
        while self.running:
            self.msleep(100)  # Keep thread alive

    def stop(self):
        self.running = False
        self.wait()
        self.mutex.lock()
        if self.buffer:
            self._send_batch()
        self.mutex.unlock()


if __name__ == "__main__":
    import sys
    from PySide6.QtWidgets import (
        QApplication,
        QWidget,
        QVBoxLayout,
        QHBoxLayout,
        QPushButton,
        QLabel,
        QLineEdit,
        QTextEdit,
        QSpinBox,
    )

    from datetime import datetime
    from utils import make_metric_line

    class MetricsManagerDemo(QWidget):
        def __init__(self):
            super().__init__()
            self.setWindowTitle("MetricsManager Demo")
            self.resize(520, 420)
            layout = QVBoxLayout(self)

            self.mm = MetricsManager(batch_limit=2)

            # Batch limit controls
            batch_row = QHBoxLayout()
            batch_label = QLabel("Batch limit:")
            self.batch_spin = QSpinBox()
            self.batch_spin.setRange(1, 100000)
            self.batch_spin.setValue(self.mm.batch_limit)
            self.batch_spin.valueChanged.connect(self.mm.set_batch_limit)
            batch_row.addWidget(batch_label)
            batch_row.addWidget(self.batch_spin)
            layout.addLayout(batch_row)

            # Metric form
            form_layout = QVBoxLayout()
            self.metric_name_edit = QLineEdit()
            self.metric_name_edit.setPlaceholderText("Metric name")
            self.message_edit = QLineEdit()
            self.message_edit.setPlaceholderText("Message")
            self.unit_edit = QLineEdit()
            self.unit_edit.setPlaceholderText("Unit")
            self.value_edit = QLineEdit()
            self.value_edit.setPlaceholderText("Value (float)")
            self.timestamp_edit = QLineEdit()
            self.timestamp_edit.setPlaceholderText("Timestamp (float, blank for now)")
            self.job_edit = QLineEdit()
            self.job_edit.setPlaceholderText("Job (optional)")

            form_layout.addWidget(QLabel("Metric name:"))
            form_layout.addWidget(self.metric_name_edit)
            form_layout.addWidget(QLabel("Message:"))
            form_layout.addWidget(self.message_edit)
            form_layout.addWidget(QLabel("Unit:"))
            form_layout.addWidget(self.unit_edit)
            form_layout.addWidget(QLabel("Value:"))
            form_layout.addWidget(self.value_edit)
            form_layout.addWidget(QLabel("Timestamp:"))
            form_layout.addWidget(self.timestamp_edit)
            form_layout.addWidget(QLabel("Job:"))
            form_layout.addWidget(self.job_edit)

            self.send_btn = QPushButton("Add Metric")
            self.send_btn.clicked.connect(self.send_metric)
            form_layout.addWidget(self.send_btn)
            layout.addLayout(form_layout)

            # Output area
            self.output = QTextEdit()
            self.output.setReadOnly(True)
            layout.addWidget(self.output)

            # Patch MetricsManager to print to output
            def print_to_output(text):
                self.output.append(text)

            def patched_send_batch():
                batch = self.mm.buffer.copy()
                self.mm.buffer = []
                if batch:
                    try:
                        print_to_output("Sending batch:\n" + "\n".join(batch))
                    except Exception as e:
                        print_to_output(f"⚠️ Error sending metrics batch: {e}")
            self.mm._send_batch = patched_send_batch

            # QTimer in main thread to call check_and_send
            self.timer = QTimer(self)
            self.timer.setInterval(1000)
            self.timer.timeout.connect(self.mm.check_and_send)
            self.timer.start()

        def send_metric(self):
            metric_name = self.metric_name_edit.text().strip()
            message = self.message_edit.text().strip()
            unit = self.unit_edit.text().strip()
            value = self.value_edit.text().strip()
            timestamp = self.timestamp_edit.text().strip()
            job = self.job_edit.text().strip()

            # Validate required fields
            if not metric_name or not message or not unit or not value:
                self.output.append("⚠️ Please fill in all required fields.")
                return
            try:
                value = float(value)
            except ValueError:
                self.output.append("⚠️ Value must be a float.")
                return
            if timestamp:
                try:
                    timestamp_val = float(timestamp)
                except ValueError:
                    self.output.append("⚠️ Timestamp must be a float.")
                    return
            else:
                timestamp_val = datetime.now().timestamp()

            metric_line = make_metric_line(
                metric_name=metric_name,
                message=message,
                unit=unit,
                value=value,
                timestamp=timestamp_val,
                job=job,
            )
            self.mm.add_metric(metric_line)
            self.output.append(f"Metric added:\n{metric_line.strip()}")
            self.metric_name_edit.clear()
            self.message_edit.clear()
            self.unit_edit.clear()
            self.value_edit.clear()
            self.timestamp_edit.clear()
            self.job_edit.clear()

        def closeEvent(self, event):
            self.mm.stop()
            event.accept()

    app = QApplication(sys.argv)
    demo = MetricsManagerDemo()
    demo.show()
    sys.exit(app.exec())
