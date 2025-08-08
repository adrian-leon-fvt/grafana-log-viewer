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
)
from PySide6.QtCore import Qt, Signal

import re


class SignalsTab(QWidget):
    signalsChecked = Signal()

    def __init__(self, parent=None, name: str = "", signals: list = []):
        super().__init__(parent)
        self.signals = signals
        self.name = name
        self._init_ui()

    def _init_ui(self):
        layout = QVBoxLayout(self)
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
        self.select_all_btn = QPushButton("Select All")
        self.deselect_all_btn = QPushButton("Deselect All")
        self.enable_selected_btn = QPushButton("Enable Selected")
        self.disable_selected_btn = QPushButton("Disable Selected")
        btn_layout.addWidget(self.select_all_btn)
        btn_layout.addWidget(self.deselect_all_btn)
        btn_layout.addWidget(self.enable_selected_btn)
        btn_layout.addWidget(self.disable_selected_btn)
        btn_layout.addStretch(1)
        layout.addLayout(btn_layout)

        # Table for signals
        self.table = QTableWidget()
        self.table.setColumnCount(5)
        self.table.setHorizontalHeaderLabels(
            ["", "Signal Name", "Message", "CAN ID", "Mux"]
        )
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.Fixed)
        self.table.setColumnWidth(0, 32)
        for col in range(1, 5):
            header.setSectionResizeMode(col, QHeaderView.ResizeMode.Interactive)
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
            self.table.setItem(i, 1, QTableWidgetItem(sig["name"]))
            self.table.setItem(i, 2, QTableWidgetItem(sig["message"]))
            self.table.setItem(i, 3, QTableWidgetItem(str(sig["can_id"])))
            self.table.setItem(i, 4, QTableWidgetItem(sig["mux"]))

            # Center align the checkbox
            cb_item = self.table.itemAt(i, 0)
            if cb_item:
                cb_item.setTextAlignment(Qt.AlignmentFlag.AlignHCenter)

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
        if not pattern:
            for row in range(self.table.rowCount()):
                self.table.setRowHidden(row, False)
            self.search_box.setStyleSheet("")
            return
        try:
            regex = re.compile(pattern, re.IGNORECASE)
            self.search_box.setStyleSheet("")
        except re.error:
            self.search_box.setStyleSheet("background: #ffcccc;")
            for row in range(self.table.rowCount()):
                self.table.setRowHidden(row, True)
            return
        for row in range(self.table.rowCount()):
            match = False
            for col in range(1, 5):
                item = self.table.item(row, col)
                if item and regex.search(str(item.text())):
                    match = True
                    break
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

    def update_enable_disable_buttons(self):
        has_selection = bool(self.table.selectionModel().selectedRows())
        self.enable_selected_btn.setEnabled(has_selection)
        self.disable_selected_btn.setEnabled(has_selection)
