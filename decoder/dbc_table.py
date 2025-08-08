from PySide6.QtWidgets import (
    QTableWidget,
    QTableWidgetItem,
    QHeaderView,
    QAbstractItemView,
    QPushButton,
)

from PySide6.QtCore import Qt, Signal

import os

import cantools
import cantools.database

class DbcTable(QTableWidget):
    dbcAdded = Signal(int)  # Signal emitted when a DBC file is added
    dbcRemoved = Signal(int)  # Signal emitted when a DBC file is removed

    def __init__(self, parent=None, cols=3):
        super().__init__(0, cols, parent)
        self.setHorizontalHeaderLabels([" ", "DBC File", "Status"])
        self.horizontalHeader().setSectionResizeMode(
            0, QHeaderView.ResizeMode.ResizeToContents
        )
        self.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self.horizontalHeader().setSectionResizeMode(
            2, QHeaderView.ResizeMode.ResizeToContents
        )
        self.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.setAcceptDrops(True)
        self.setDragDropMode(QAbstractItemView.DragDropMode.DropOnly)
        self.files = set()
        self.db_cache = {}  # path -> loaded cantools db
        

    def showEvent(self, event):
        super().showEvent(event)
        # Set minimum height to fit at least 4 rows
        row_height = self.verticalHeader().defaultSectionSize()
        header_height = self.horizontalHeader().height()
        self.setMinimumHeight(header_height + row_height * 4 + 4)  # +4 for grid lines

    def add_dbc_file(self, path):
        if path in self.files:
            return
        valid, error_msg = self.validate_dbc(path)
        db = None
        if valid:
            try:
                db = cantools.database.load_file(path)
            except Exception as e:
                valid = False
                error_msg = str(e)
        row = self.rowCount()
        self.insertRow(row)
        file_item = QTableWidgetItem(os.path.basename(path))
        file_item.setToolTip(path)
        status_item = QTableWidgetItem("Valid" if valid else "Invalid")
        if not valid and error_msg:
            status_item.setToolTip(str(error_msg))
        # Add remove button
        remove_btn = QPushButton("🗑️")
        remove_btn.setToolTip("Remove DBC file")
        remove_btn.setFixedSize(28, 28)

        def remove_row():
            self.remove_dbc_row(row)

        remove_btn.clicked.connect(remove_row)
        self.setCellWidget(row, 0, remove_btn)
        self.setItem(row, 1, file_item)
        self.setItem(row, 2, status_item)

        self.files.add(path)
        if valid and db:
            self.db_cache[path] = db

        # Emit signal that a DBC file was added
        self.dbcAdded.emit(row)
        
    def remove_dbc_row(self, row):
        file_item = self.item(row, 1)
        if file_item:
            name = file_item.text()
            # Find full path in self.files
            for f in list(self.files):
                if os.path.basename(f) == name:
                    self.files.remove(f)
                    if f in self.db_cache:
                        del self.db_cache[f]
                    break
        self.removeRow(row)
        # Emit signal that a DBC file was removed
        self.dbcRemoved.emit(row)

    def keyPressEvent(self, event):
        # Allow deleting selected rows with Del or Backspace
        if event.key() in (Qt.Key.Key_Delete, Qt.Key.Key_Backspace):
            selected = self.selectionModel().selectedRows()
            changed = False
            for idx in sorted([s.row() for s in selected], reverse=True):
                self.remove_dbc_row(idx)
                changed = True
            event.accept()
        else:
            super().keyPressEvent(event)

    def validate_dbc(self, path):
        try:
            cantools.database.load_file(path)
            return True, None
        except Exception as e:
            return False, str(e) if str(e) else "Invalid"

    def get_valid_files(self):
        return [
            f
            for i, f in enumerate(self.files)
            if (
                self.item(i, 1) is not None
                and getattr(self.item(i, 1), "text", lambda: None)() == "Valid"
            )
        ]