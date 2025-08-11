from PySide6.QtWidgets import (
    QApplication,
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
    QFileDialog,
    QProgressBar,
    QDialog,
    QProgressBar,
)
from PySide6.QtCore import Qt, Signal, QThread, QObject, Slot
import os
import sys
import can
import glob
import datetime

SUPPORTED_EXTENSIONS = [".asc", ".blf", ".csv", ".db", ".log", ".mf4", ".trc"]


class LogFileManager(QWidget):

    filesChecked = Signal()
    fileStatusChanged = Signal(str)

    class FileAddWorker(QObject):
        progress = Signal(int)
        fileTimesUpdated = Signal(str, str, str)  # file_path, start, end
        finished = Signal()

        def __init__(self, file_paths):
            super().__init__()
            self.file_paths = file_paths
            self._cancelled = False

        def cancel(self):
            self._cancelled = True

        def run(self):
            total = len(self.file_paths)
            for idx, file_path in enumerate(self.file_paths):
                if self._cancelled:
                    break
                start_posix = ""
                end_posix = ""
                try:
                    messages = list(can.LogReader(file_path))
                    if messages:
                        start_posix = str(messages[0].timestamp)
                        end_posix = str(messages[-1].timestamp)
                except Exception:
                    pass
                self.fileTimesUpdated.emit(file_path, start_posix, end_posix)
                self.progress.emit(int((idx + 1) / total * 100))
            self.finished.emit()

    def __init__(self, parent=None, files=None):
        super().__init__(parent)
        self.files = files if files is not None else []
        self.setAcceptDrops(True)
        self._init_ui()

    def _init_ui(self):

        layout = QVBoxLayout(self)

        # Progress bar for file adding
        self.progress_bar = QProgressBar()
        self.progress_bar.setMinimum(0)
        self.progress_bar.setMaximum(100)
        self.progress_bar.setValue(0)
        self.progress_bar.setVisible(False)

        layout.addWidget(self.progress_bar)

        # File add row
        file_row = QHBoxLayout()
        self.add_file_btn = QPushButton("Add Log File")
        self.add_file_btn.clicked.connect(self.add_file_dialog)
        file_row.addWidget(self.add_file_btn)
        self.add_folder_btn = QPushButton("Add Folder")
        self.add_folder_btn.clicked.connect(self.add_folder_dialog)
        file_row.addWidget(self.add_folder_btn)
        layout.addLayout(file_row)

        # Search bar (similar to SignalsManager)
        search_row = QHBoxLayout()
        search_row.setAlignment(Qt.AlignmentFlag.AlignLeft)
        self.loupe_btn = QPushButton("✖")
        self.loupe_btn.setFixedWidth(28)
        self.loupe_btn.setToolTip("Clear search box")
        self.search_label = QLabel("Search (regex):")
        self.search_box = QLineEdit()
        self.search_box.setPlaceholderText("Type to filter files...")
        self.loupe_btn.clicked.connect(lambda: self.search_box.setText(""))
        search_row.addWidget(self.loupe_btn)
        search_row.addWidget(self.search_label)
        search_row.addWidget(self.search_box)
        layout.addLayout(search_row)

        self.search_box.textChanged.connect(self.filter_table)

        # Controls row (similar to SignalsManager)
        controls_row = QHBoxLayout()
        self.select_all_btn = QPushButton("Select All")
        self.deselect_all_btn = QPushButton("Deselect All")
        self.enable_selected_btn = QPushButton("Enable Selected")
        self.disable_selected_btn = QPushButton("Disable Selected")
        self.show_checked_toggle = QCheckBox("Show checked")
        self.show_checked_toggle.setToolTip("Show only checked files")
        self.show_checked_toggle.stateChanged.connect(self.filter_table)
        controls_row.addWidget(self.select_all_btn)
        controls_row.addWidget(self.deselect_all_btn)
        controls_row.addWidget(self.enable_selected_btn)
        controls_row.addWidget(self.disable_selected_btn)
        controls_row.addWidget(self.show_checked_toggle)
        controls_row.addStretch(1)
        layout.addLayout(controls_row)

        self.select_all_btn.clicked.connect(self.select_all)
        self.deselect_all_btn.clicked.connect(self.deselect_all)
        self.enable_selected_btn.clicked.connect(self.enable_selected)
        self.disable_selected_btn.clicked.connect(self.disable_selected)

        # Table for log files
        self.table = QTableWidget()
        self.table.setColumnCount(5)
        self.table.setHorizontalHeaderLabels(
            ["", "File Name", "Status", "Start", "End"]
        )
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.Fixed)
        self.table.setColumnWidth(0, 32)
        # File Name, Status columns: Interactive
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Interactive)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.Interactive)
        # Start and End columns: Resize to contents
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
        self.table.setSortingEnabled(True)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        layout.addWidget(self.table)

        self.populate_table()

    def populate_table(self):
        self.table.setRowCount(len(self.files))
        for i, file in enumerate(self.files):
            cb = QCheckBox()
            self.table.setCellWidget(i, 0, cb)
            name_item = QTableWidgetItem(file.get("parents") + file.get("name", ""))
            # Set absolute path as tooltip
            name_item.setToolTip(file.get("path", ""))
            self.table.setItem(i, 1, name_item)
            self.table.setItem(i, 2, QTableWidgetItem(file.get("status", "")))
            # Pretty print start and end times
            start_val = file.get("start", "")
            end_val = file.get("end", "")

            def pretty(ts):
                try:
                    ts_float = float(ts)
                    dt = datetime.datetime.fromtimestamp(ts_float)
                    return dt.strftime("%Y-%m-%d %H:%M:%S.%f%Z")
                except Exception:
                    return ""

            self.table.setItem(i, 3, QTableWidgetItem(pretty(start_val)))
            self.table.setItem(i, 4, QTableWidgetItem(pretty(end_val)))
            cb.stateChanged.connect(lambda _: self.filesChecked.emit())

    def add_folder_dialog(self):
        from PySide6.QtWidgets import QFileDialog
        folder = QFileDialog.getExistingDirectory(self, "Select Folder", os.getcwd())
        if folder:
            log_files = self.find_log_files_in_folder(folder)
            if log_files:
                self.add_files_with_progress(log_files)

    def find_log_files_in_folder(self, folder):
        log_files = []
        # Search for all supported extensions recursively
        for ext in SUPPORTED_EXTENSIONS:
            def either(c):
                return f'[{c.lower()}{c.upper()}]' if c.isalpha() else c
            pattern = os.path.join(folder, f"**/*{''.join(either(c) for c in ext)}")
            found = [f for f in glob.glob(pattern, recursive=True) if os.path.splitext(f)[1].lower() == ext]
            log_files.extend(found)
        return log_files

    def filter_table(self):
        pattern = self.search_box.text() if hasattr(self, "search_box") else ""
        show_checked = self.show_checked_toggle.isChecked()
        if not pattern:
            regex = None
        else:
            import re

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

    def select_all(self):
        for row in range(self.table.rowCount()):
            if self.table.isRowHidden(row):
                continue
            cb = self.table.cellWidget(row, 0)
            if isinstance(cb, QCheckBox):
                cb.blockSignals(True)
                cb.setChecked(True)
                cb.blockSignals(False)
        self.filesChecked.emit()

    def deselect_all(self):
        for row in range(self.table.rowCount()):
            if self.table.isRowHidden(row):
                continue
            cb = self.table.cellWidget(row, 0)
            if isinstance(cb, QCheckBox):
                cb.blockSignals(True)
                cb.setChecked(False)
                cb.blockSignals(False)
        self.filesChecked.emit()

    def enable_selected(self):
        selected = self.table.selectionModel().selectedRows()
        for idx in selected:
            row = idx.row()
            cb = self.table.cellWidget(row, 0)
            if isinstance(cb, QCheckBox):
                cb.blockSignals(True)
                cb.setChecked(True)
                cb.blockSignals(False)
        self.filesChecked.emit()

    def disable_selected(self):
        selected = self.table.selectionModel().selectedRows()
        for idx in selected:
            row = idx.row()
            cb = self.table.cellWidget(row, 0)
            if isinstance(cb, QCheckBox):
                cb.blockSignals(True)
                cb.setChecked(False)
                cb.blockSignals(False)
        self.filesChecked.emit()

    def add_files_with_progress(self, files):
        # Filter out files already added (by absolute path)
        existing_paths = set(f["path"] for f in self.files)
        new_files = [f for f in files if f not in existing_paths]
        if not new_files:
            return
        # Add files immediately with empty start/end
        common_prefix = os.path.commonpath(new_files)
        for file_path in new_files:
            rel_path = os.path.relpath(os.path.dirname(file_path), common_prefix)
            parents = "" if rel_path == "." else rel_path + os.sep
            file_info = {
                "name": os.path.basename(file_path),
                "parents": parents,
                "status": "New",
                "start": "",
                "end": "",
                "path": file_path,
            }
            self.files.append(file_info)
        self.populate_table()
        # Create modal progress dialog
        self.progress_dialog = QDialog(self)
        self.progress_dialog.setWindowTitle("Loading Log Files...")
        self.progress_dialog.setModal(True)
        vlayout = QVBoxLayout(self.progress_dialog)
        vlayout.addWidget(QLabel("Reading log files, please wait..."))
        self.progress_bar = QProgressBar()
        self.progress_bar.setMinimum(0)
        self.progress_bar.setMaximum(100)
        self.progress_bar.setValue(0)
        vlayout.addWidget(self.progress_bar)
        btn_row = QHBoxLayout()
        cancel_btn = QPushButton("Cancel")
        btn_row.addStretch(1)
        btn_row.addWidget(cancel_btn)
        vlayout.addLayout(btn_row)
        self.progress_dialog.setLayout(vlayout)
        self.add_thread = QThread()
        self.worker = LogFileManager.FileAddWorker(new_files)
        self.worker.moveToThread(self.add_thread)
        self.worker.progress.connect(self.progress_bar.setValue)
        self.worker.fileTimesUpdated.connect(self.on_file_times_updated)
        self.worker.finished.connect(self.progress_dialog.close)
        self.worker.finished.connect(self.add_thread.quit)
        self.worker.finished.connect(self.worker.deleteLater)
        self.add_thread.finished.connect(self.add_thread.deleteLater)
        cancel_btn.clicked.connect(self.worker.cancel)
        cancel_btn.clicked.connect(self.progress_dialog.reject)
        self.add_thread.started.connect(self.worker.run)
        self.add_thread.start()
        self.progress_dialog.show()

    def add_file_dialog(self):
        # Only allow files supported by python-can log reader
        file_types = (
            "Log Files (" + " ".join(f"*{ext}" for ext in SUPPORTED_EXTENSIONS) + ")"
        )
        files, _ = QFileDialog.getOpenFileNames(
            self, "Add Log Files", os.getcwd(), file_types
        )
        if files:
            self.add_files_with_progress(files)

    def keyPressEvent(self, event):
        # Remove selected file(s) on Delete or Backspace
        if event.key() in (Qt.Key.Key_Delete, Qt.Key.Key_Backspace):
            selected = self.table.selectionModel().selectedRows()
            names_to_remove = []
            for idx in selected:
                row = idx.row()
                name_item = self.table.item(row, 1)
                if name_item:
                    names_to_remove.append(name_item.text())
            for name in names_to_remove:
                self.remove_file(name)
            event.accept()
        else:
            super().keyPressEvent(event)

    @Slot(str, str, str)
    def on_file_times_updated(self, file_path, start, end):
        for f in self.files:
            if f["path"] == file_path:
                f["start"] = start
                f["end"] = end
        self.populate_table()

    def add_file(self, file_path):
        # Try to extract start and end timestamps using python-can
        start_time = ""
        end_time = ""
        messages = list(can.LogReader(file_path))
        # If at least one message, get first timestamp
        if messages:
            start_time = str(messages[0].timestamp)
            end_time = str(messages[-1].timestamp)

        file_info = {
            "name": os.path.basename(file_path),
            "status": "New",
            "start": start_time,
            "end": end_time,
            "path": file_path,
        }
        self.files.append(file_info)
        self.populate_table()

    def remove_file(self, file_name):
        self.files = [f for f in self.files if f["name"] != file_name]
        self.populate_table()

    def set_file_status(self, file_name, status):
        for f in self.files:
            if f["name"] == file_name:
                f["status"] = status
                self.fileStatusChanged.emit(file_name)
        self.populate_table()

    def set_file_times(self, file_name, start, end):
        for f in self.files:
            if f["name"] == file_name:
                f["start"] = start
                f["end"] = end
        self.populate_table()

    def dragEnterEvent(self, event):
        if event.mimeData().hasUrls():
            for url in event.mimeData().urls():
                if url.isLocalFile():
                    local_path = url.toLocalFile()
                    ext = os.path.splitext(local_path)[1].lower()
                    if ext in SUPPORTED_EXTENSIONS or os.path.isdir(local_path):
                        event.acceptProposedAction()
                        return
        event.ignore()

    def dropEvent(self, event):
        if event.mimeData().hasUrls():
            file_paths = []
            folder_paths = []
            for url in event.mimeData().urls():
                if url.isLocalFile():
                    file_path = url.toLocalFile()
                    if os.path.isdir(file_path):
                        folder_paths.append(file_path)
                    else:
                        ext = os.path.splitext(file_path)[1].lower()
                        if ext in SUPPORTED_EXTENSIONS:
                            file_paths.append(file_path)
                
            # Add files from dropped folders
            for folder in folder_paths:
                log_files = self.find_log_files_in_folder(folder)
                file_paths.extend(log_files)
            if file_paths:
                self.add_files_with_progress(file_paths)
        event.acceptProposedAction()
    
    def closeEvent(self, event):
        # Ensure any running threads are properly stopped
        add_thread = getattr(self, "add_thread", None)
        if add_thread is not None and hasattr(add_thread, "isRunning"):
            try:
                if add_thread.isRunning():
                    if hasattr(self, "worker"):
                        self.worker.cancel()
                    add_thread.quit()
                    add_thread.wait()
            except RuntimeError:
                pass  # Thread already deleted
        event.accept()


if __name__ == "__main__":

    app = QApplication(sys.argv)
    widget = LogFileManager()
    widget.setWindowTitle("LogFileManager Demo")
    widget.resize(1000, 400)
    widget.show()
    sys.exit(app.exec())
