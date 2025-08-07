import sys
import can
import io
import os

import contextlib
from can.typechecking import AutoDetectedConfig
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
)

from PySide6.QtCore import Qt, QTimer, QThread, Signal, QObject
from PySide6.QtGui import QColor, QPalette


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
                [f"{cfg['interface']} {cfg['channel']}" for cfg in available], ""
            )
        except Exception as e:
            self.devices_found.emit([], f"Error scanning devices: {e}")


# DBC file table area
class DbcTable(QTableWidget):

    def __init__(self, parent=None, get_busses=None, on_dbc_assignment_changed=None):
        super().__init__(0, 3, parent)
        self.setHorizontalHeaderLabels(["DBC File", "Status", "Assigned Bus"])
        self.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.horizontalHeader().setSectionResizeMode(
            1, QHeaderView.ResizeMode.ResizeToContents
        )
        self.horizontalHeader().setSectionResizeMode(
            2, QHeaderView.ResizeMode.ResizeToContents
        )
        self.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.setAcceptDrops(True)
        self.setDragDropMode(QAbstractItemView.DragDropMode.DropOnly)
        self.files = set()
        self.get_busses = get_busses  # function to get current busses
        self.on_dbc_assignment_changed = on_dbc_assignment_changed

    def showEvent(self, event):
        super().showEvent(event)
        # Set minimum height to fit at least 4 rows
        row_height = self.verticalHeader().defaultSectionSize()
        header_height = self.horizontalHeader().height()
        self.setMinimumHeight(header_height + row_height * 4 + 4)  # +4 for grid lines

    def dragEnterEvent(self, event):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()
        else:
            super().dragEnterEvent(event)

    def dropEvent(self, event):
        if event.mimeData().hasUrls():
            for url in event.mimeData().urls():
                path = url.toLocalFile()
                if path.lower().endswith(".dbc"):
                    self.add_dbc_file(path)
            event.acceptProposedAction()
        else:
            super().dropEvent(event)

    def add_dbc_file(self, path):
        if path in self.files:
            return
        valid, error_msg = self.validate_dbc(path)
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
        from PySide6.QtWidgets import QComboBox

        combo = QComboBox()
        combo.setEditable(False)
        combo.addItem("")  # Unassigned
        if self.get_busses:
            for bus in self.get_busses():
                combo.addItem(bus)
        self.setCellWidget(row, 2, combo)
        self.files.add(path)
        # Connect signal to update bus tab when assignment changes
        if self.on_dbc_assignment_changed:
            combo.currentIndexChanged.connect(self.on_dbc_assignment_changed)

    def keyPressEvent(self, event):
        # Allow deleting selected rows with Del or Backspace
        if event.key() in (Qt.Key.Key_Delete, Qt.Key.Key_Backspace):
            selected = self.selectionModel().selectedRows()
            changed = False
            for idx in sorted([s.row() for s in selected], reverse=True):
                # Remove file from self.files
                file_item = self.item(idx, 0)
                if file_item:
                    name = file_item.text()
                    # Find full path in self.files
                    for f in list(self.files):
                        if os.path.basename(f) == name:
                            self.files.remove(f)
                            break
                self.removeRow(idx)
                changed = True
            event.accept()
            # Update bus tabs/signals if any row was removed
            if self.on_dbc_assignment_changed:
                self.on_dbc_assignment_changed()
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

    def __init__(self):
        super().__init__()
        self.setWindowTitle("Live Stream Decoder")

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

        # DBC area (resizable)
        dbc_area = QWidget()
        dbc_layout = QVBoxLayout()
        dbc_area.setLayout(dbc_layout)

        def get_busses():
            return list(self.connected_busses.keys())

        self.dbc_table = DbcTable(
            get_busses=get_busses,
            on_dbc_assignment_changed=self.on_dbc_assignment_changed,
        )
        dbc_layout.addWidget(self.dbc_table)
        self.add_dbc_btn = QPushButton("Add DBC File(s)...")
        dbc_layout.addWidget(self.add_dbc_btn)
        splitter = QSplitter(Qt.Orientation.Vertical)
        splitter.addWidget(dbc_area)
        splitter.addWidget(QWidget())  # Filler for resizing
        splitter.setSizes([200, 100])
        main_layout.addWidget(splitter)

        def open_dbc_dialog():
            files, _ = QFileDialog.getOpenFileNames(
                self, "Select DBC Files", "", "DBC Files (*.dbc)"
            )
            for f in files:
                self.dbc_table.add_dbc_file(f)

        self.add_dbc_btn.clicked.connect(open_dbc_dialog)

        self.tabs.addTab(main_tab, "Main")

        # Store bus tabs: device_name -> QWidget
        self.bus_tabs = {}

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
        # Refresh all bus tabs
        print("DBC assignment changed, refreshing bus tabs")
        for device in list(self.bus_tabs.keys()):
            self._add_bus_tab(device)

    def closeEvent(self, event):
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

    def update_devices(self, device_names, error_msg):
        if error_msg:
            self.status.showMessage(error_msg)
        current = self.device_combo.currentText()
        self.device_combo.blockSignals(True)
        self.device_combo.clear()
        max_display_len = (
            36  # chars to show before truncating (fits most socketcand names)
        )
        for name in device_names:
            display = (
                name
                if len(name) <= max_display_len
                else name[: max_display_len - 3] + "..."
            )
            self.device_combo.addItem(display, name)
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

    def _update_connect_button(self):
        idx = self.device_combo.currentIndex()
        device = self.device_combo.itemData(idx) if idx >= 0 else ""
        if device and device in self.connected_busses:
            self.connect_btn.setEnabled(False)
        else:
            self.connect_btn.setEnabled(True)

    def connect_device(self):
        # Get the full device name from the combobox data
        idx = self.device_combo.currentIndex()
        device = self.device_combo.itemData(idx) if idx >= 0 else ""
        bitrate = self.bitrate_combo.currentText()
        if not device:
            self.status.showMessage("No device selected.")
            return
        if device in self.connected_busses:
            self.status.showMessage(f"Already connected to {device}.")
            return
        try:
            iface, name = device.split(" ")
            _bitrate = (
                int(bitrate[:-1]) * 1000
                if bitrate[-1] == "k"
                else int(bitrate[:-1]) * 1000000
            )
            bus = can.interface.Bus(
                interface=iface, channel=name, bitrate=int(_bitrate)
            )
            self.status.showMessage(f"Connected to {device} at {bitrate} bps.")
            # Create chip for this connection
            chip = self._create_chip(device, bitrate)
            self.chip_layout.addWidget(chip)
            self.connected_busses[device] = (bus, chip)
            self._update_connect_button()
            self.update_dbc_bus_dropdowns()
            self._add_bus_tab(device)
        except Exception as e:
            self.status.showMessage(f"Connection failed: {e}")

    def _add_bus_tab(self, device):
        # Remove if already exists
        if device in self.bus_tabs:
            idx = self.tabs.indexOf(self.bus_tabs[device])
            if idx >= 0:
                self.tabs.removeTab(idx)
        # Create new tab for this bus
        tab = QWidget()
        layout = QVBoxLayout()
        tab.setLayout(layout)
        # Table for signals
        table = QTableWidget()
        table.setColumnCount(5)
        table.setHorizontalHeaderLabels(
            ["", "Signal Name", "Message", "CAN ID", "Mux Value(s)"]
        )
        # Make all columns resizable by user
        header = table.horizontalHeader()
        for col in range(5):
            header.setSectionResizeMode(col, QHeaderView.ResizeMode.Interactive)
        table.setSortingEnabled(True)
        # Enable multiple row selection
        table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        # Populate table with signals from assigned DBCs
        signals = self._get_signals_for_bus(device)
        table.setRowCount(len(signals))
        for i, sig in enumerate(signals):
            # Checkbox
            cb = QCheckBox()
            table.setCellWidget(i, 0, cb)
            # Signal name
            table.setItem(i, 1, QTableWidgetItem(sig["name"]))
            # Message name
            table.setItem(i, 2, QTableWidgetItem(sig["message"]))
            # CAN ID
            table.setItem(i, 3, QTableWidgetItem(str(sig["can_id"])))
            # Mux values
            table.setItem(i, 4, QTableWidgetItem(sig["mux"]))

        # Add keyPressEvent to toggle checkboxes with spacebar
        def table_keyPressEvent(event):
            if event.key() == Qt.Key.Key_Space:
                selected = table.selectionModel().selectedRows()
                for idx in selected:
                    row = idx.row()
                    cb = table.cellWidget(row, 0)
                    if isinstance(cb, QCheckBox):
                        cb.setChecked(not cb.isChecked())
                event.accept()
            else:
                # Call base implementation
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
                cb = table.cellWidget(row, 0)
                if isinstance(cb, QCheckBox):
                    cb.setChecked(True)

        def deselect_all():
            for row in range(table.rowCount()):
                cb = table.cellWidget(row, 0)
                if isinstance(cb, QCheckBox):
                    cb.setChecked(False)

        def enable_selected():
            selected = table.selectionModel().selectedRows()
            for idx in selected:
                row = idx.row()
                cb = table.cellWidget(row, 0)
                if isinstance(cb, QCheckBox):
                    cb.setChecked(True)

        def disable_selected():
            selected = table.selectionModel().selectedRows()
            for idx in selected:
                row = idx.row()
                cb = table.cellWidget(row, 0)
                if isinstance(cb, QCheckBox):
                    cb.setChecked(False)

        select_all_btn.clicked.connect(select_all)
        deselect_all_btn.clicked.connect(deselect_all)
        enable_selected_btn.clicked.connect(enable_selected)
        disable_selected_btn.clicked.connect(disable_selected)

        # Disable enable/disable selected buttons if nothing is selected
        def update_enable_disable_buttons():
            has_selection = bool(table.selectionModel().selectedRows())
            enable_selected_btn.setEnabled(has_selection)
            disable_selected_btn.setEnabled(has_selection)

        table.selectionModel().selectionChanged.connect(lambda *_: update_enable_disable_buttons())
        update_enable_disable_buttons()

        layout.addLayout(btn_layout)
        layout.addWidget(table)
        self.tabs.addTab(tab, device)
        self.bus_tabs[device] = tab

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
            import cantools

            for dbc_path in assigned:
                try:
                    db = cantools.database.load_file(dbc_path)
                except Exception:
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

    def _create_chip(self, device, bitrate):
        from PySide6.QtWidgets import QFrame, QLabel, QPushButton, QHBoxLayout

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
            bus, chip = self.connected_busses.pop(device)
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
