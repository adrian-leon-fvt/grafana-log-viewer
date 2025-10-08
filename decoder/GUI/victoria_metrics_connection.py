from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QCheckBox,
)
from PySide6.QtCore import Signal, Qt

import requests

VM_DEFAULT_URL = "http://localhost:8428"

VM_API_DELETE = "/api/v1/admin/tsdb/delete_series"
VM_API_EXPORT = "/api/v1/export"
VM_API_EXPORT_CSV = "/api/v1/export/csv"
VM_API_EXPORT_NATIVE = "/api/v1/export/native"
VM_API_IMPORT = "/api/v1/import"
VM_API_IMPORT_CSV = "/api/v1/import/csv"
VM_API_IMPORT_NATIVE = "/api/v1/import/native"
VM_API_IMPORT_PROMETHEUS = "/api/v1/import/prometheus"
VM_API_LABELS = "/api/v1/labels"
VM_API_LABEL_VALUES = "/api/v1/label/…/values"
VM_API_QUERY = "/api/v1/query"
VM_API_QUERY_RANGE = "/api/v1/query_range"
VM_API_SERIES = "/api/v1/series"
VM_API_STATUS_TSDB = "/api/v1/status/tsdb"


class VictoriaMetricsConnectionWidget(QWidget):
    urlChanged = Signal(str)
    sendingToggled = Signal(bool)

    def __init__(self, parent=None, initial_url: str = VM_DEFAULT_URL, enabled: bool = False):
        super().__init__(parent)
        self._init_ui(initial_url, enabled)

    def _init_ui(self, initial_url, enabled):
        layout = QVBoxLayout(self)

        # URL label and LineEdit in the same row
        url_row = QHBoxLayout()
        url_label = QLabel("VictoriaMetrics URL:")
        url_row.addWidget(url_label)

        self.url_edit = QLineEdit(initial_url)
        self.url_edit.setPlaceholderText("Enter import URL...")
        url_row.addWidget(self.url_edit)

        layout.addLayout(url_row)

        # Row for Apply, Test, Status
        btn_row = QHBoxLayout()
        update_url_btn = QPushButton("Apply")
        update_url_btn.setFixedWidth(60)
        update_url_btn.clicked.connect(self._emit_url_changed)
        self.url_edit.returnPressed.connect(self._emit_url_changed)

        test_btn = QPushButton("Test")
        test_btn.setFixedWidth(60)
        test_btn.clicked.connect(self._test_url)

        self.status_label = QLabel("")

        btn_row.addWidget(update_url_btn)
        btn_row.addWidget(test_btn)
        btn_row.addWidget(self.status_label)
        btn_row.addStretch(1)
        layout.addLayout(btn_row)

        # Enable/disable sending toggle
        toggle_row = QHBoxLayout()
        self.sending_checkbox = QCheckBox("Enable Sending")
        self.sending_checkbox.setChecked(enabled)
        self.sending_checkbox.toggled.connect(self._emit_sending_toggled)
        toggle_row.addWidget(self.sending_checkbox)
        layout.addLayout(toggle_row)

        layout.addStretch(1)

    def _test_url(self) -> bool:
        url = self.get_url()
        if "/api/" in url:
            url = url[: url.index("/api/")]
        if url.endswith("/"):
            url = url[:-1]
        self.status_label.setText("Testing...")
        self.status_label.setStyleSheet("color: orange;")
        try:
            resp = requests.get(
                f"{url}{VM_API_QUERY}", params={"query": "up"}, timeout=0.5
            )
            if resp.status_code == 200:
                self.status_label.setText("Valid URL")
                self.status_label.setStyleSheet("color: green;")
                return True
            else:
                full_text = f"Status: {resp.status_code} | {resp.reason}"
                max_chars = 32
                if len(full_text) > max_chars:
                    display_text = full_text[: max_chars - 3] + "..."
                    self.status_label.setText(display_text)
                    # Wrap tooltip text every 40 chars
                    wrapped = "\n".join(
                        [full_text[i : i + 40] for i in range(0, len(full_text), 40)]
                    )
                    self.status_label.setToolTip(wrapped)
                else:
                    self.status_label.setText(full_text)
                    self.status_label.setToolTip("")
                self.status_label.setStyleSheet("color: red;")
        except Exception as e:
            full_text = f"Invalid URL: {e}"
            max_chars = 32
            if len(full_text) > max_chars:
                display_text = full_text[: max_chars - 3] + "..."
                self.status_label.setText(display_text)
                # Wrap tooltip text every 40 chars
                wrapped = "\n".join(
                    [full_text[i : i + 40] for i in range(0, len(full_text), 40)]
                )
                self.status_label.setToolTip(wrapped)
            else:
                self.status_label.setText(full_text)
                self.status_label.setToolTip("")
            self.status_label.setStyleSheet("color: red;")
        return False

    def _emit_url_changed(self):
        url = self.url_edit.text()
        if "/api/" in url:
            self.status_label.setText("URL must not include '/api/' portion")
            self.status_label.setStyleSheet("color: orange;")
            return
        if self._test_url():
            self.urlChanged.emit(url)

    def _emit_sending_toggled(self, checked):
        self.sendingToggled.emit(checked)

    def set_url(self, url: str):
        self.url_edit.setText(url)

    def set_enabled(self, enabled: bool):
        self.sending_checkbox.setChecked(enabled)

    def get_url(self) -> str:
        return self.url_edit.text()

    def is_enabled(self) -> bool:
        return self.sending_checkbox.isChecked()


if __name__ == "__main__":
    import sys
    from PySide6.QtWidgets import QApplication

    app = QApplication(sys.argv)
    widget = VictoriaMetricsConnectionWidget(
        initial_url="http://localhost:8428/", enabled=True
    )
    widget.urlChanged.connect(lambda url: print(f"URL changed: {url}"))
    widget.sendingToggled.connect(lambda enabled: print(f"Sending enabled: {enabled}"))
    widget.resize(400, 100)
    widget.show()
    sys.exit(app.exec())
