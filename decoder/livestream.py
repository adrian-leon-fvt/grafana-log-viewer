import sys
import can
import io
import os
import contextlib
import cantools
import cantools.database

from threading import Lock

from PySide6.QtWidgets import (
    QApplication,
    QMainWindow,
    QLabel,
    QVBoxLayout,
    QWidget,
    QPushButton,
    QComboBox,
    QHBoxLayout,
    QScrollArea,
    QFrame,
    QSizePolicy,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QFileDialog,
    QAbstractItemView,
    QHeaderView,
    QTabWidget,
    QGridLayout,
    QCheckBox,
    QLineEdit,
)

from PySide6.QtCore import Qt, QTimer, QThread, Signal, QObject
from PySide6.QtGui import QColor, QPalette

from config import (
    vm_export_url,
    vm_import_url,
    vm_query_url,
    vm_query_range_url,
)

# Capture stdout while detecting
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    all_configs = can.detect_available_configs()

# Get the stdout output as a string
stdout_output = buf.getvalue()

# Print it for debugging (optional)
# print(stdout_output)

# Common failure message patterns in python-can
failure_keywords = [
    "unavailable",
    "won't be able",
    "Failed to load",
    "could not import",
    "does not work",
    "has not been initialized",
    "not installed",
    "required for",
    "No module named",
]

# Parse failed interface names from stdout
excluded_interfaces = set()
for line in stdout_output.splitlines():
    for keyword in failure_keywords:
        if keyword.lower() in line.lower():
            parts = line.split()
            if parts:
                excluded_interfaces.add(parts[0].lower())

# Build the final interface list
valid_interfaces = {
    cfg["interface"]
    for cfg in all_configs
    if cfg["interface"].lower() not in excluded_interfaces
}

# Optional: re-run filtered configs
filtered_configs = can.detect_available_configs(interfaces=list(valid_interfaces))

print("Working interfaces:", valid_interfaces)
print("Filtered configs:", filtered_configs)


class DeviceScanner(QObject):
    devices_found = Signal(list, str)

    def __init__(self):
        super().__init__()

    def scan(self):
        try:
            import can

            available = can.detect_available_configs(interfaces=list(valid_interfaces))
            self.devices_found.emit(
                available,
                "",
            )
        except Exception as e:
            self.devices_found.emit([], f"Error scanning devices: {e}")


# DBC file table area
class DbcTable(QTableWidget):

    def __init__(self, parent=None, get_busses=None, on_dbc_assignment_changed=None):
        super().__init__(0, 4, parent)
        self.setHorizontalHeaderLabels(["DBC File", "Status", "Assigned Bus", " "])
        self.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.horizontalHeader().setSectionResizeMode(
            1, QHeaderView.ResizeMode.ResizeToContents
        )
        self.horizontalHeader().setSectionResizeMode(
            2, QHeaderView.ResizeMode.ResizeToContents
        )
        self.horizontalHeader().setSectionResizeMode(
            3, QHeaderView.ResizeMode.ResizeToContents
        )
        self.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.setAcceptDrops(True)
        self.setDragDropMode(QAbstractItemView.DragDropMode.DropOnly)
        self.files = set()
        self.db_cache = {}  # path -> loaded cantools db
        self.get_busses = get_busses  # function to get current busses
        self.on_dbc_assignment_changed = on_dbc_assignment_changed

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
                import cantools

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
        self.setItem(row, 0, file_item)
        self.setItem(row, 1, status_item)

        # Add dropdown for bus assignment
        combo = QComboBox()
        combo.setEditable(False)
        combo.addItem("")  # Unassigned
        if self.get_busses:
            for bus in self.get_busses():
                combo.addItem(bus)
        self.setCellWidget(row, 2, combo)
        # Add remove button
        remove_btn = QPushButton("🗑️")
        remove_btn.setToolTip("Remove DBC file")
        remove_btn.setFixedSize(28, 28)

        def remove_row():
            self.remove_dbc_row(row)

        remove_btn.clicked.connect(remove_row)
        self.setCellWidget(row, 3, remove_btn)
        self.files.add(path)
        if valid and db:
            self.db_cache[path] = db
        # Connect signal to update bus tab when assignment changes
        if self.on_dbc_assignment_changed:
            combo.currentIndexChanged.connect(self.on_dbc_assignment_changed)

    def remove_dbc_row(self, row):
        file_item = self.item(row, 0)
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
        # Update bus tabs/signals if any row was removed
        if self.on_dbc_assignment_changed:
            self.on_dbc_assignment_changed()

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
            import cantools

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


class MainWindow(QMainWindow):

    class BusReader(QThread):
        update_signals_requested = Signal(list)
        update_status = Signal(str)

        def __init__(self, bus, bus_name, parent=None, status_callback=None):
            super().__init__(parent)
            from threading import Lock

            self.bus = bus
            self.bus_name = bus_name
            self.running = True
            self._dbc_lock = Lock()
            self.db = cantools.database.Database()
            self._pending_signals = None
            self.update_signals_requested.connect(self._do_update_signals)
            self.update_status.connect(status_callback) if status_callback else None

        def run(self):
            while self.running:
                # Check for pending signal updates
                if self._pending_signals is not None:
                    self._do_update_signals(self._pending_signals)
                    self._pending_signals = None
                try:
                    msg = self.bus.recv(timeout=0.2)
                    if msg is None:
                        continue
                    with self._dbc_lock:
                        db = self.db
                    try:
                        decoded = db.decode_message(msg.arbitration_id, msg.data)
                        if decoded:
                            print(
                                f"[{self.bus_name}] {db.get_message_by_frame_id(msg.arbitration_id).name} [{msg.timestamp}]: {decoded}"
                            )
                    except Exception:
                        pass
                except Exception as e:
                    print(f"[{self.bus_name}] Error reading: {e}")

        def stop(self):
            self.running = False
            self.wait()

        def update_signals(self, messages):
            # Request update in background thread
            if hasattr(self, "update_status"):
                self.update_status.emit(f"Updating signals for {self.bus_name}...")
            self._pending_signals = messages
            self.update_signals_requested.emit(messages)

        def _do_update_signals(self, messages):
            # messages: list of cantools.db.Message (with only selected signals)
            total_signals = sum(len(msg.signals) for msg in messages)
            print(
                f"Updating signals for {self.bus_name}: {len(messages)} messages and {total_signals} signals"
            )
            if hasattr(self, "update_status"):
                self.update_status.emit(
                    f"Signals updated for {self.bus_name} ({len(messages)} messages, {total_signals} signals)"
                )
            with self._dbc_lock:
                self.db = cantools.database.Database()
                for msg in messages:
                    self.db._add_message(msg)

    def __init__(self):
        super().__init__()
        self.setWindowTitle("Live Stream Decoder")
        self.setAcceptDrops(True)

        # Central widget and main layout
        self.tabs = QTabWidget()
        self.setCentralWidget(self.tabs)

        # Main tab content
        main_tab = QWidget()
        main_layout = QVBoxLayout()
        main_tab.setLayout(main_layout)

        # Status bar for messages
        self.status = self.statusBar()
        self.status.showMessage("Select a CAN device and bitrate.")

        # Grid layout for controls
        grid = QGridLayout()

        # Controls
        self.device_combo = QComboBox()
        self.device_combo.setFixedWidth(320)
        self.refresh_btn = QPushButton("🔄")
        self.refresh_btn.setToolTip("Refresh device list")
        self.refresh_btn.setFixedSize(32, 32)
        app_instance = QApplication.instance()
        if isinstance(app_instance, QApplication):
            palette = app_instance.palette()
            bg_color = palette.color(QPalette.ColorRole.Window).name()
        else:
            bg_color = "#ffffff"
        qcolor = QColor(bg_color)
        darker_color = qcolor.darker(110).name()
        self.refresh_btn.setStyleSheet(
            f"""
            QPushButton {{
                border-radius: 16px;
                min-width: 32px;
                min-height: 32px;
                max-width: 32px;
                max-height: 32px;
            }}
            QPushButton:hover {{
                background: {darker_color};
            }}
        """
        )

        self.bitrate_combo = QComboBox()
        self.bitrate_combo.addItems(["1M", "500k", "250k", "125k"])
        self.bitrate_combo.setToolTip("Select CAN bitrate")
        self.bitrate_combo.setCurrentIndex(1)

        self.connect_btn = QPushButton("Connect")

        # Row 0: labels (reload button first)
        grid.addWidget(QLabel(""), 0, 0)  # Empty for button
        grid.addWidget(QLabel("Device:"), 0, 1)
        grid.addWidget(QLabel("Bitrate:"), 0, 2)
        grid.addWidget(QLabel(""), 0, 3)  # Empty for button alignment

        # Row 1: controls (reload button first)
        grid.addWidget(self.refresh_btn, 1, 0)
        grid.addWidget(self.device_combo, 1, 1)
        grid.addWidget(self.bitrate_combo, 1, 2)
        grid.addWidget(self.connect_btn, 1, 3)

        # Align everything to the top
        main_layout.addLayout(grid)

        # Scrollable chip area for connected busses
        self.chip_scroll = QScrollArea()
        self.chip_scroll.setWidgetResizable(True)
        self.chip_container = QWidget()
        self.chip_layout = QHBoxLayout()
        self.chip_layout.setContentsMargins(0, 0, 0, 0)
        self.chip_layout.setSpacing(8)
        self.chip_container.setLayout(self.chip_layout)
        self.chip_scroll.setWidget(self.chip_container)
        main_layout.addWidget(self.chip_scroll)
        main_layout.addStretch(1)

        # DBC area (resizable) with drag-and-drop support for the entire area (including splitter)
        class DragDropDbcArea(QWidget):
            def __init__(self, dbc_table, parent=None):
                super().__init__(parent)
                self.dbc_table = dbc_table
                self.setAcceptDrops(True)
                # Layout for this area
                layout = QVBoxLayout(self)
                # Compose the DBC area and splitter
                dbc_layout = QVBoxLayout()
                dbc_layout.addWidget(self.dbc_table)
                self.add_dbc_btn = QPushButton("Add DBC File(s)...")
                dbc_layout.addWidget(self.add_dbc_btn)
                dbc_widget = QWidget()
                dbc_widget.setLayout(dbc_layout)
                splitter = QSplitter(Qt.Orientation.Vertical)
                splitter.addWidget(dbc_widget)
                splitter.addWidget(QWidget())  # Filler for resizing
                splitter.setSizes([200, 100])
                layout.addWidget(splitter)

            def dragEnterEvent(self, event):
                if event.mimeData().hasUrls():
                    for url in event.mimeData().urls():
                        if url.toLocalFile().lower().endswith(".dbc"):
                            event.acceptProposedAction()
                            event.setDropAction(Qt.DropAction.CopyAction)
                            self.setCursor(Qt.CursorShape.DragCopyCursor)
                            return
                self.unsetCursor()
                event.ignore()

            def dropEvent(self, event):
                self.unsetCursor()
                if event.mimeData().hasUrls():
                    for url in event.mimeData().urls():
                        path = url.toLocalFile()
                        if path.lower().endswith(".dbc"):
                            self.dbc_table.add_dbc_file(path)
                    event.acceptProposedAction()
                else:
                    event.ignore()

            def dragLeaveEvent(self, event):
                self.unsetCursor()
                event.accept()

        def get_busses():
            return list(self.connected_busses.keys())

        self.dbc_table = DbcTable(
            get_busses=get_busses,
            on_dbc_assignment_changed=self.on_dbc_assignment_changed,
        )
        self.dbc_area = DragDropDbcArea(self.dbc_table)
        main_layout.addWidget(self.dbc_area)

        def open_dbc_dialog():
            files, _ = QFileDialog.getOpenFileNames(
                self, "Select DBC Files", "", "DBC Files (*.dbc)"
            )
            for f in files:
                self.dbc_table.add_dbc_file(f)

        # Connect the button inside DragDropDbcArea
        self.dbc_area.add_dbc_btn.clicked.connect(open_dbc_dialog)

        self.tabs.addTab(main_tab, "Main")

        # Store bus tabs: device_name -> QWidget
        self.bus_tabs = {}

        self._bus_checked_signals = {}

        # Store connected busses and chips
        self.connected_busses = {}  # device_name: (bus, chip_widget)

        # Device scanner thread setup
        self.scanner = DeviceScanner()
        self.scanner_thread = QThread()
        self.scanner.moveToThread(self.scanner_thread)
        self.scanner.devices_found.connect(self.update_devices)
        self.scanner_thread.start()

        # Button actions
        self.refresh_btn.clicked.connect(self.scanner.scan)
        self.connect_btn.clicked.connect(self.connect_device)

        # Update connect button state when device selection changes
        self.device_combo.currentIndexChanged.connect(self._update_connect_button)
        self._update_connect_button()  # Initial state

        # Initial scan
        self.scanner.scan()

    def on_dbc_assignment_changed(self):
        # Called when a DBC file is assigned/unassigned to a bus
        # Update signals for each active reader and refresh all bus tabs
        for device in list(self.bus_tabs.keys()):
            # Update reader signals if bus is connected
            if device in self.connected_busses:
                bus, chip, reader = self.connected_busses[device]
                checked_signals = self._get_checked_signals_for_bus(device)
                selected_msgs = []
                for row in range(self.dbc_table.rowCount()):
                    widget = self.dbc_table.cellWidget(row, 2)
                    if isinstance(widget, QComboBox) and widget.currentText() == device:
                        for f in self.dbc_table.files:
                            item = self.dbc_table.item(row, 0)
                            if item is not None and item.text() in f:
                                db = self.dbc_table.db_cache.get(f)
                                if db:
                                    selected_msgs.extend(
                                        self._build_filtered_messages(
                                            db, checked_signals
                                        )
                                    )
                                else:
                                    print(f"DBC not loaded for {f}")
                reader.update_signals(selected_msgs)
            self._add_bus_tab(device)

    def closeEvent(self, event):
        # Gracefully stop all BusReader threads
        for device, (bus, chip, reader) in list(self.connected_busses.items()):
            try:
                reader.stop()
            except Exception:
                pass
        # Gracefully stop the scanner thread
        if hasattr(self, "scanner_thread") and self.scanner_thread.isRunning():
            self.scanner_thread.quit()
            self.scanner_thread.wait()
        super().closeEvent(event)

    def update_dbc_bus_dropdowns(self):
        # Update all bus assignment dropdowns in the DBC table
        for row in range(self.dbc_table.rowCount()):
            widget = self.dbc_table.cellWidget(row, 2)
            if isinstance(widget, QComboBox):
                current = widget.currentText()
                widget.clear()
                widget.addItem("")
                for bus in self.connected_busses.keys():
                    widget.addItem(bus)
                idx = widget.findText(current)
                if idx >= 0:
                    widget.setCurrentIndex(idx)

    def update_devices(self, devices_dict, error_msg):
        if error_msg:
            self.status.showMessage(error_msg)
        current = self.device_combo.currentText()
        self.device_combo.blockSignals(True)
        self.device_combo.clear()
        max_display_len = (
            36  # chars to show before truncating (fits most socketcand names)
        )
        for dev_dict in devices_dict:
            name = f"{dev_dict['interface']} {dev_dict['channel']}"
            if dev_dict["interface"] == "kvaser" and dev_dict["device_name"].startswith(
                "Kvaser Virtual"
            ):
                name = f"{dev_dict['device_name']} {dev_dict['channel']}"
            display = (
                name
                if len(name) <= max_display_len
                else name[: max_display_len - 3] + "..."
            )
            data = {
                "name": name,
                "display_name": display,
                "interface": dev_dict["interface"],
                "channel": dev_dict["channel"],
                "AutoDetectedConfig": dev_dict,
            }
            self.device_combo.addItem(display, data)
            idx = self.device_combo.findText(display)
            # Set tooltip for each item if truncated
            if idx >= 0:
                tooltip = name if len(name) > max_display_len else ""
                self.device_combo.setItemData(
                    idx, tooltip, role=Qt.ItemDataRole.ToolTipRole
                )
        # Restore previous selection if possible
        idx = -1
        for i in range(self.device_combo.count()):
            if (
                self.device_combo.itemData(i) == current
                or self.device_combo.itemText(i) == current
            ):
                idx = i
                break
        if idx >= 0:
            self.device_combo.setCurrentIndex(idx)
        self.device_combo.blockSignals(False)
        self._update_connect_button()

    # ...existing code...

    def dragEnterEvent(self, event):
        if event.mimeData().hasUrls():
            for url in event.mimeData().urls():
                if url.toLocalFile().lower().endswith(".dbc"):
                    event.acceptProposedAction()
                    event.setDropAction(Qt.DropAction.CopyAction)
                    self.setCursor(Qt.CursorShape.DragCopyCursor)
                    return
        self.unsetCursor()
        event.ignore()

    def dropEvent(self, event):
        self.unsetCursor()
        if event.mimeData().hasUrls():
            for url in event.mimeData().urls():
                path = url.toLocalFile()
                if path.lower().endswith(".dbc"):
                    self.dbc_table.add_dbc_file(path)
            event.acceptProposedAction()
        else:
            event.ignore()

    def dragLeaveEvent(self, event):
        self.unsetCursor()
        event.accept()

    def _update_connect_button(self):
        idx = self.device_combo.currentIndex()
        device = self.device_combo.itemData(idx) if idx >= 0 else {}
        if device and device["name"] in self.connected_busses:
            self.connect_btn.setEnabled(False)
        else:
            self.connect_btn.setEnabled(True)

    def _set_bus_filters_for_device(self, device):
        # Set python-can filters for the bus to only allow selected CAN IDs
        if device not in self.connected_busses:
            return
        bus, _, _ = self.connected_busses[device]
        tab = self.bus_tabs.get(device)
        if not tab:
            return
        table = None
        for child in tab.children():
            if isinstance(child, QTableWidget):
                table = child
                break
        if table is None:
            return
        can_ids = set()
        for row in range(table.rowCount()):
            cb = table.cellWidget(row, 0)
            if isinstance(cb, QCheckBox) and cb.isChecked():
                can_id_item = table.item(row, 3)
                if can_id_item:
                    try:
                        can_id_val = (
                            int(str(can_id_item.text()), 16)
                            if str(can_id_item.text()).startswith("0x")
                            else int(str(can_id_item.text()))
                        )
                        can_ids.add(can_id_val)
                    except Exception:
                        continue
        # Ensure unique CAN IDs in filters
        unique_can_ids = list(can_ids)
        filters = (
            [{"can_id": cid, "can_mask": 0x1FFFFFFF} for cid in unique_can_ids]
            if unique_can_ids
            else None
        )
        try:
            bus.set_filters(filters)
        except Exception as e:
            print(f"Failed to set filters for {device}: {e}")

    def connect_device(self):
        # Get the full device name from the combobox data
        idx = self.device_combo.currentIndex()
        device = self.device_combo.itemData(idx) if idx >= 0 else {}
        bitrate = self.bitrate_combo.currentText()
        if not device:
            self.status.showMessage("No device selected.")
            return
        if device["name"] in self.connected_busses:
            self.status.showMessage(f"Already connected to {device['name']}.")
            return
        try:
            iface = device["interface"]
            channel = device["channel"]
            _bitrate = (
                int(bitrate[:-1]) * 1000
                if bitrate[-1] == "k"
                else int(bitrate[:-1]) * 1000000
            )
            bus = can.interface.Bus(
                interface=iface,
                channel=channel,
                bitrate=int(_bitrate),
                state=can.BusState.PASSIVE,
            )
            self.status.showMessage(f"Connected to {device['name']} at {bitrate} bps.")
            # Create chip for this connection
            chip = self._create_chip(device["name"], bitrate)
            self.chip_layout.addWidget(chip)
            # Start a thread to read messages for this bus
            reader = self.BusReader(
                bus, device["name"], status_callback=self.status.showMessage
            )
            reader.start()
            self.connected_busses[device["name"]] = (bus, chip, reader)
            self._update_connect_button()
            self.update_dbc_bus_dropdowns()
            self._add_bus_tab(device["name"])
        except Exception as e:
            self.status.showMessage(f"Connection failed: {e}")

    def _add_bus_tab(self, device):
        # Remove if already exists
        # Save checked signals before removing tab
        checked_signals = set()
        if device in self.bus_tabs:
            tab = self.bus_tabs[device]
            for child in tab.children():
                if isinstance(child, QTableWidget):
                    table = child
                    for row in range(table.rowCount()):
                        cb = table.cellWidget(row, 0)
                        name_item = table.item(row, 1)
                        if isinstance(cb, QCheckBox) and cb.isChecked() and name_item:
                            checked_signals.add(name_item.text())
            idx = self.tabs.indexOf(self.bus_tabs[device])
            if idx >= 0:
                self.tabs.removeTab(idx)
        self._bus_checked_signals[device] = checked_signals
        # Create new tab for this bus
        tab = QWidget()
        layout = QVBoxLayout()
        tab.setLayout(layout)

        search_row = QHBoxLayout()
        loupe_btn = QPushButton("🔍")
        loupe_btn.setFixedWidth(28)
        loupe_btn.setToolTip("Show/hide search box")
        search_label = QLabel("Search (regex):")
        search_box = QLineEdit()
        search_box.setPlaceholderText("Type to filter signals...")
        search_label.setVisible(False)
        search_box.setVisible(False)

        def toggle_search():
            vis = not search_box.isVisible()
            search_box.setVisible(vis)
            search_label.setVisible(vis)
            if not vis:
                search_box.setText("")  # Clear filter when hiding

        loupe_btn.clicked.connect(toggle_search)
        search_row.addWidget(loupe_btn)
        search_row.addWidget(search_label)
        search_row.addWidget(search_box)
        layout.addLayout(search_row)
        # Table for signals
        table = QTableWidget()
        table.setColumnCount(5)
        table.setHorizontalHeaderLabels(
            ["", "Signal Name", "Message", "CAN ID", "Mux Value(s)"]
        )
        # Make all columns resizable by user
        header = table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.Fixed)
        table.setColumnWidth(0, 32)  # Small width for checkbox column
        for col in range(1, 5):
            header.setSectionResizeMode(col, QHeaderView.ResizeMode.Interactive)
        table.setSortingEnabled(True)
        # Enable multiple row selection
        table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        # Populate table with signals from assigned DBCs
        signals = self._get_signals_for_bus(device)
        table.setRowCount(len(signals))
        checked_signals = self._bus_checked_signals.get(device, set())
        for i, sig in enumerate(signals):
            # Checkbox
            cb = QCheckBox()
            if sig["name"] in checked_signals:
                cb.setChecked(True)
            table.setCellWidget(i, 0, cb)
            cb_item = table.itemAt(i, 0)
            if cb_item:
                cb_item.setTextAlignment(Qt.AlignmentFlag.AlignHCenter)
            # Signal name
            table.setItem(i, 1, QTableWidgetItem(sig["name"]))
            # Message name
            table.setItem(i, 2, QTableWidgetItem(sig["message"]))
            # CAN ID
            table.setItem(i, 3, QTableWidgetItem(str(sig["can_id"])))
            # Mux values
            table.setItem(i, 4, QTableWidgetItem(sig["mux"]))

            cb.checkStateChanged.connect(
                lambda _, dev=device: (
                    self._set_bus_filters_for_device(dev),
                    self._update_busreader_signals(dev),
                )
            )

        # Search/filter logic
        def filter_table():
            import re

            pattern = search_box.text()
            if not pattern:
                for row in range(table.rowCount()):
                    table.setRowHidden(row, False)
                search_box.setStyleSheet("")
                return
            try:
                regex = re.compile(pattern, re.IGNORECASE)
                search_box.setStyleSheet("")
            except re.error:
                # Invalid regex: mark box red, hide all rows
                search_box.setStyleSheet("background: #ffcccc;")
                for row in range(table.rowCount()):
                    table.setRowHidden(row, True)
                return
            for row in range(table.rowCount()):
                match = False
                for col in range(1, 5):  # Don't search checkbox col
                    item = table.item(row, col)
                    if item and regex.search(str(item.text())):
                        match = True
                        break
                table.setRowHidden(row, not match)

        search_box.textChanged.connect(filter_table)

        # Add keyPressEvent to toggle checkboxes with spacebar
        def table_keyPressEvent(event):
            if event.key() == Qt.Key.Key_Space:
                selected = table.selectionModel().selectedRows()
                changed = False
                for idx in selected:
                    row = idx.row()
                    cb = table.cellWidget(row, 0)
                    if isinstance(cb, QCheckBox):
                        cb.blockSignals(True)
                        cb.setChecked(not cb.isChecked())
                        cb.blockSignals(False)
                        changed = True
                if changed:
                    self._set_bus_filters_for_device(device)
                    self._update_busreader_signals(device)
                event.accept()
            else:
                QTableWidget.keyPressEvent(table, event)

        table.keyPressEvent = table_keyPressEvent

        # Add Select All / Deselect All / Enable Selected / Disable Selected buttons
        btn_layout = QHBoxLayout()
        select_all_btn = QPushButton("Select All")
        deselect_all_btn = QPushButton("Deselect All")
        enable_selected_btn = QPushButton("Enable Selected")
        disable_selected_btn = QPushButton("Disable Selected")
        btn_layout.addWidget(select_all_btn)
        btn_layout.addWidget(deselect_all_btn)
        btn_layout.addWidget(enable_selected_btn)
        btn_layout.addWidget(disable_selected_btn)
        btn_layout.addStretch(1)

        def select_all():
            for row in range(table.rowCount()):
                if table.isRowHidden(row):
                    continue
                cb = table.cellWidget(row, 0)
                if isinstance(cb, QCheckBox):
                    cb.blockSignals(True)
                    cb.setChecked(True)
                    cb.blockSignals(False)

        def deselect_all():
            for row in range(table.rowCount()):
                if table.isRowHidden(row):
                    continue
                cb = table.cellWidget(row, 0)
                if isinstance(cb, QCheckBox):
                    cb.blockSignals(True)
                    cb.setChecked(False)
                    cb.blockSignals(False)

        def enable_selected():
            selected = table.selectionModel().selectedRows()
            for idx in selected:
                row = idx.row()
                cb = table.cellWidget(row, 0)
                if isinstance(cb, QCheckBox):
                    cb.blockSignals(True)
                    cb.setChecked(True)
                    cb.blockSignals(False)

        def disable_selected():
            selected = table.selectionModel().selectedRows()
            for idx in selected:
                row = idx.row()
                cb = table.cellWidget(row, 0)
                if isinstance(cb, QCheckBox):
                    cb.blockSignals(True)
                    cb.setChecked(False)
                    cb.blockSignals(False)

        select_all_btn.clicked.connect(
            lambda: (
                select_all(),
                self._set_bus_filters_for_device(device),
                self._update_busreader_signals(device),
            )
        )
        deselect_all_btn.clicked.connect(
            lambda: (
                deselect_all(),
                self._set_bus_filters_for_device(device),
                self._update_busreader_signals(device),
            )
        )
        enable_selected_btn.clicked.connect(
            lambda: (
                enable_selected(),
                self._set_bus_filters_for_device(device),
                self._update_busreader_signals(device),
            )
        )
        disable_selected_btn.clicked.connect(
            lambda: (
                disable_selected(),
                self._set_bus_filters_for_device(device),
                self._update_busreader_signals(device),
            )
        )

        # Disable enable/disable selected buttons if nothing is selected
        def update_enable_disable_buttons():
            has_selection = bool(table.selectionModel().selectedRows())
            enable_selected_btn.setEnabled(has_selection)
            disable_selected_btn.setEnabled(has_selection)

        table.selectionModel().selectionChanged.connect(
            lambda *_: update_enable_disable_buttons()
        )
        update_enable_disable_buttons()

        layout.addLayout(btn_layout)
        layout.addWidget(table)
        self.tabs.addTab(tab, device)
        self.bus_tabs[device] = tab
        # Set filters initially
        self._set_bus_filters_for_device(device)

    def _get_signals_for_bus(self, device):
        # Find all DBCs assigned to this bus
        assigned = []
        for row in range(self.dbc_table.rowCount()):
            widget = self.dbc_table.cellWidget(row, 2)
            if isinstance(widget, QComboBox) and widget.currentText() == device:
                # Find the file path for this row
                for f in self.dbc_table.files:
                    item = self.dbc_table.item(row, 0)
                    if item is not None and item.text() in f:
                        assigned.append(f)
        # For each DBC, extract signals
        signals = []
        try:
            for dbc_path in assigned:
                db = (
                    self.dbc_table.db_cache.get(dbc_path)
                    if hasattr(self, "dbc_table")
                    else None
                )
                if not db:
                    continue
                # Use db.messages if available, else fallback to db._messages for compatibility
                messages = getattr(db, "messages", None)
                if messages is None:
                    messages = getattr(db, "_messages", [])
                for msg in messages:
                    for sig in msg.signals:
                        mux = ""
                        if getattr(sig, "is_multiplexer", False):
                            mux = f"Multiplexer: {sig.name}"
                        elif getattr(sig, "multiplexer_ids", None) is not None:
                            mux = f"Muxed: {sig.multiplexer_ids}"
                        signals.append(
                            {
                                "name": sig.name,
                                "message": msg.name,
                                "can_id": hex(msg.frame_id),
                                "mux": mux,
                            }
                        )
        except ImportError:
            pass
        return signals

    def _get_checked_signals_for_bus(self, device):
        checked_signals = set()
        tab = self.bus_tabs.get(device)
        if tab:
            for child in tab.children():
                if isinstance(child, QTableWidget):
                    table = child
                    for i in range(table.rowCount()):
                        cb = table.cellWidget(i, 0)
                        name_item = table.item(i, 1)
                        message_item = table.item(i, 2)
                        if (
                            isinstance(cb, QCheckBox)
                            and cb.isChecked()
                            and name_item
                            and message_item
                        ):
                            checked_signals.add((name_item.text(), message_item.text()))
        return checked_signals

    def _build_filtered_messages(self, db, checked_signals):
        messages = getattr(db, "messages", None)
        if messages is None:
            messages = getattr(db, "_messages", [])
        filtered_msgs = []
        for msg in messages:
            filtered_signals = [
                s for s in msg.signals if (s.name, msg.name) in checked_signals
            ]
            if filtered_signals:
                new_msg = cantools.database.Message(
                    frame_id=msg.frame_id,
                    name=msg.name,
                    length=msg.length,
                    signals=filtered_signals,
                    contained_messages=msg.contained_messages,
                    header_id=msg.header_id,
                    header_byte_order=msg.header_byte_order,
                    unused_bit_pattern=msg.unused_bit_pattern,
                    senders=msg.senders,
                    comment=msg.comment,
                    send_type=msg.send_type,
                    cycle_time=msg.cycle_time,
                    dbc_specifics=(
                        msg.dbc_specifics if hasattr(msg, "dbc_specifics") else None
                    ),
                    autosar_specifics=(
                        msg.autosar_specifics
                        if hasattr(msg, "autosar_specifics")
                        else None
                    ),
                    is_extended_frame=msg.is_extended_frame,
                    is_fd=msg.is_fd,
                    bus_name=msg.bus_name,
                    signal_groups=msg.signal_groups,
                    strict=msg.strict if hasattr(msg, "strict") else False,
                    protocol=msg.protocol,
                )
                filtered_msgs.append(new_msg)
        return filtered_msgs

    def _update_busreader_signals(self, device):
        if device in self.connected_busses:
            bus, chip, reader = self.connected_busses[device]
            checked_signals = self._get_checked_signals_for_bus(device)
            selected_msgs = []
            for row in range(self.dbc_table.rowCount()):
                widget = self.dbc_table.cellWidget(row, 2)
                if isinstance(widget, QComboBox) and widget.currentText() == device:
                    for f in self.dbc_table.files:
                        item = self.dbc_table.item(row, 0)
                        if item is not None and item.text() in f:
                            db = self.dbc_table.db_cache.get(f)
                            if db:
                                selected_msgs.extend(
                                    self._build_filtered_messages(db, checked_signals)
                                )
                            else:
                                print(f"DBC not loaded for {f}")
            reader.update_signals(selected_msgs)

    def _create_chip(self, device, bitrate):

        chip = QFrame()
        chip.setObjectName("chip")
        chip.setStyleSheet(
            """
            QFrame#chip {
                border: 1px solid #bbb;
                border-radius: 12px;
                padding: 2px 8px 2px 8px;
            }
        """
        )
        layout = QHBoxLayout()
        layout.setContentsMargins(8, 2, 2, 2)
        layout.setSpacing(4)
        label = QLabel(f"{device} @ {bitrate}")
        close_btn = QPushButton("❌")
        close_btn.setFixedSize(20, 20)
        close_btn.setStyleSheet("border: none; background: transparent;")
        close_btn.clicked.connect(lambda: self._disconnect_bus(device))
        layout.addWidget(label)
        layout.addWidget(close_btn)
        chip.setLayout(layout)
        chip.setSizePolicy(QSizePolicy.Policy.Maximum, QSizePolicy.Policy.Fixed)
        return chip

    def _disconnect_bus(self, device):
        # Remove chip and disconnect bus
        if device in self.connected_busses:
            bus, chip, reader = self.connected_busses.pop(device)
            try:
                reader.stop()
            except Exception:
                pass
            try:
                bus.shutdown()
            except Exception:
                pass
            chip.setParent(None)
            chip.deleteLater()
            self.status.showMessage(f"Disconnected {device}.")
            self._update_connect_button()
            self.update_dbc_bus_dropdowns()
            # Remove tab for this bus
            if device in self.bus_tabs:
                idx = self.tabs.indexOf(self.bus_tabs[device])
                if idx >= 0:
                    self.tabs.removeTab(idx)
                del self.bus_tabs[device]


from PySide6.QtCore import Qt

if __name__ == "__main__":
    app = QApplication([])
    mw = MainWindow()
    mw.resize(600, 600)
    mw.show()
    sys.exit(app.exec())
