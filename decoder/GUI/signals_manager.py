from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QPushButton,
    QLabel,
    QLineEdit,
    QTableWidget,
    QTableWidgetItem,
    QCheckBox,
    QHeaderView,
    QAbstractItemView,
    QStyle,
)
from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QShortcut, QKeySequence

import re


class SignalsManager(QWidget):
    signalsChecked = Signal()
    jobNameChanged = Signal(str)
    can_id_hex_mode = False

    def __init__(self, parent=None, name: str = "", signals: list = []):
        super().__init__(parent)
        self.signals = signals
        self.name = name
        self._init_ui()

    def _init_ui(self):
        layout = QVBoxLayout(self)

        # Job name row
        job_row = QHBoxLayout()
        job_label = QLabel("Job name:")
        self.job_name_edit = QLineEdit(self.name)
        self.job_name_edit.setPlaceholderText("Enter job name...")
        self.update_job_name_btn = QPushButton()
        self.update_job_name_btn.setIcon(
            self.style().standardIcon(QStyle.StandardPixmap.SP_DialogApplyButton)
        )
        self.update_job_name_btn.setFixedWidth(28)
        def emit_job_name_change():
            self.jobNameChanged.emit(self.job_name_edit.text())
        self.update_job_name_btn.clicked.connect(emit_job_name_change)
        self.job_name_edit.returnPressed.connect(emit_job_name_change)
        job_row.addWidget(job_label)
        job_row.addWidget(self.job_name_edit)
        job_row.addWidget(self.update_job_name_btn)
        layout.addLayout(job_row)

        # Search bar
        search_row = QHBoxLayout()
        search_row.setAlignment(Qt.AlignmentFlag.AlignLeft)
        self.loupe_btn = QPushButton("✖")
        self.loupe_btn.setFixedWidth(28)
        self.loupe_btn.setToolTip("Clear search box")
        self.search_label = QLabel("Search (regex):")
        self.search_box = QLineEdit()
        self.search_box.setPlaceholderText("Type to filter signals...")
        self.loupe_btn.clicked.connect(lambda: self.search_box.setText(""))
        search_row.addWidget(self.loupe_btn)
        search_row.addWidget(self.search_label)
        search_row.addWidget(self.search_box)
        layout.addLayout(search_row)

        # Buttons
        btn_layout = QHBoxLayout()
        self.toggle_canid_btn = QPushButton("CAN ID: Dec")
        self.toggle_canid_btn.setToolTip(
            "Toggle CAN ID display between decimal and hex (Ctrl+H)"
        )
        self.toggle_canid_btn.clicked.connect(self.toggle_canid_mode)
        btn_layout.addWidget(self.toggle_canid_btn)
        self.select_all_btn = QPushButton("Select All")
        self.deselect_all_btn = QPushButton("Deselect All")
        self.enable_selected_btn = QPushButton("Enable Selected")
        self.disable_selected_btn = QPushButton("Disable Selected")
        self.show_checked_toggle = QCheckBox("Show checked")
        self.show_checked_toggle.setToolTip("Show only checked signals")
        self.show_checked_toggle.stateChanged.connect(self.filter_table)
        btn_layout.addWidget(self.select_all_btn)
        btn_layout.addWidget(self.deselect_all_btn)
        btn_layout.addWidget(self.enable_selected_btn)
        btn_layout.addWidget(self.disable_selected_btn)
        btn_layout.addWidget(self.show_checked_toggle)
        btn_layout.addStretch(1)
        layout.addLayout(btn_layout)

        # Keyboard shortcut for CAN ID toggle (global)
        self.canid_shortcut = QShortcut(QKeySequence("Ctrl+H"), self)
        self.canid_shortcut.setAutoRepeat(True)
        self.canid_shortcut.activated.connect(self.toggle_canid_mode)

        # Table for signals
        self.table = QTableWidget()
        self.table.setColumnCount(6)
        self.table.setHorizontalHeaderLabels(
            ["", "Signal Name", "Message", "CAN ID", "", "Mux"]
        )
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.Fixed)
        self.table.setColumnWidth(0, 32)
        # Other columns remain interactive
        for col in [2, 3, 5]:
            header.setSectionResizeMode(col, QHeaderView.ResizeMode.Interactive)
        # Make 'Signal Name' column resize automatically
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
        self.table.setSortingEnabled(True)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        layout.addWidget(self.table)

        self.select_all_btn.clicked.connect(self.select_all)
        self.deselect_all_btn.clicked.connect(self.deselect_all)
        self.enable_selected_btn.clicked.connect(self.enable_selected)
        self.disable_selected_btn.clicked.connect(self.disable_selected)
        self.table.selectionModel().selectionChanged.connect(
            self.update_enable_disable_buttons
        )
        self.update_enable_disable_buttons()

        self.search_box.textChanged.connect(self.filter_table)
        self.table.keyPressEvent = self.table_keyPressEvent

        self.populate_table()

    def populate_table(self):
        self.table.setRowCount(len(self.signals))
        for i, sig in enumerate(self.signals):
            cb = QCheckBox()
            self.table.setCellWidget(i, 0, cb)
            # Center align the checkbox
            cb_item = self.table.item(i, 0)
            if cb_item:
                cb_item.setTextAlignment(Qt.AlignmentFlag.AlignHCenter)
            self.table.setItem(i, 1, QTableWidgetItem(sig["name"]))
            self.table.setItem(i, 2, QTableWidgetItem(sig["message"]))
            canid_val = sig["can_id"]
            if self.can_id_hex_mode:
                canid_str = hex(canid_val)
            else:
                canid_str = str(canid_val)
            self.table.setItem(i, 3, QTableWidgetItem(canid_str))
            ext_item = self.table.setItem(
                i, 4, QTableWidgetItem("X" if sig.get("extended", "") else "S")
            )
            if ext_item:
                ext_item.setTextAlignment(Qt.AlignmentFlag.AlignVCenter)
            self.table.setItem(i, 5, QTableWidgetItem(sig["mux"]))

            # Emit a signal when checkbox is toggled
            cb.stateChanged.connect(lambda _: self.signalsChecked.emit())

    def add_signals(self, signals):
        self.signals.append(signals)
        self.populate_table()

    def remove_signals(self, signals):
        self.signals = [lst for lst in self.signals if lst != signals]
        self.populate_table()

    def filter_table(self):
        pattern = self.search_box.text()
        show_checked = self.show_checked_toggle.isChecked()
        if not pattern:
            regex = None
        else:
            try:
                regex = re.compile(pattern, re.IGNORECASE)
                self.search_box.setStyleSheet("")
            except re.error:
                self.search_box.setStyleSheet("background: #ffcccc;")
                for row in range(self.table.rowCount()):
                    self.table.setRowHidden(row, True)
                return
        for row in range(self.table.rowCount()):
            match = True
            # Filter by regex
            if regex:
                match = False
                for col in range(1, 5):
                    item = self.table.item(row, col)
                    if item and regex.search(str(item.text())):
                        match = True
                        break
            # Filter by checked
            if show_checked:
                cb = self.table.cellWidget(row, 0)
                if not (isinstance(cb, QCheckBox) and cb.isChecked()):
                    match = False
            self.table.setRowHidden(row, not match)

    def table_keyPressEvent(self, event):
        if event.key() == Qt.Key.Key_Space:
            selected = self.table.selectionModel().selectedRows()
            for idx in selected:
                row = idx.row()
                cb = self.table.cellWidget(row, 0)
                if isinstance(cb, QCheckBox):
                    cb.blockSignals(True)
                    cb.setChecked(not cb.isChecked())
                    cb.blockSignals(False)
            self.signalsChecked.emit()
            event.accept()
        else:
            QTableWidget.keyPressEvent(self.table, event)

    def select_all(self):
        for row in range(self.table.rowCount()):
            if self.table.isRowHidden(row):
                continue
            cb = self.table.cellWidget(row, 0)
            if isinstance(cb, QCheckBox):
                cb.blockSignals(True)
                cb.setChecked(True)
                cb.blockSignals(False)
        self.signalsChecked.emit()

    def deselect_all(self):
        for row in range(self.table.rowCount()):
            if self.table.isRowHidden(row):
                continue
            cb = self.table.cellWidget(row, 0)
            if isinstance(cb, QCheckBox):
                cb.blockSignals(True)
                cb.setChecked(False)
                cb.blockSignals(False)
        self.signalsChecked.emit()

    def enable_selected(self):
        selected = self.table.selectionModel().selectedRows()
        for idx in selected:
            row = idx.row()
            cb = self.table.cellWidget(row, 0)
            if isinstance(cb, QCheckBox):
                cb.blockSignals(True)
                cb.setChecked(True)
                cb.blockSignals(False)
        self.signalsChecked.emit()

    def disable_selected(self):
        selected = self.table.selectionModel().selectedRows()
        for idx in selected:
            row = idx.row()
            cb = self.table.cellWidget(row, 0)
            if isinstance(cb, QCheckBox):
                cb.blockSignals(True)
                cb.setChecked(False)
                cb.blockSignals(False)
        self.signalsChecked.emit()

    def toggle_canid_mode(self):
        self.can_id_hex_mode = not self.can_id_hex_mode
        if self.can_id_hex_mode:
            self.toggle_canid_btn.setText("CAN ID: Hex")
        else:
            self.toggle_canid_btn.setText("CAN ID: Dec")
        self.populate_table()

    def update_enable_disable_buttons(self):
        has_selection = bool(self.table.selectionModel().selectedRows())
        self.enable_selected_btn.setEnabled(has_selection)
        self.disable_selected_btn.setEnabled(has_selection)


if __name__ == "__main__":
    import sys
    from PySide6.QtWidgets import QApplication

    # Example signals data
    example_signals = [
        {
            "name": "Speed",
            "message": "VehicleData",
            "can_id": 123,
            "extended": True,
            "mux": "A",
        },
        {
            "name": "RPM",
            "message": "EngineData",
            "can_id": 456,
            "extended": False,
            "mux": "B",
        },
        {
            "name": "Temp",
            "message": "EnvData",
            "can_id": 789,
            "extended": True,
            "mux": "C",
        },
    ]

    app = QApplication(sys.argv)
    widget = SignalsManager(name="Demo Job", signals=example_signals)
    widget.setWindowTitle("SignalsTab Demo")
    widget.resize(700, 400)
    widget.show()
    sys.exit(app.exec())
