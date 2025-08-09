import sys
import can
import io
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
    QTableWidget,
    QFileDialog,
    QHeaderView,
    QTabWidget,
    QGridLayout,
    QCheckBox,
)

from PySide6.QtCore import Qt, QTimer, QThread, Signal, QObject
from PySide6.QtGui import QColor, QPalette

from dbc_table import DbcTable as DbcTableBase
from signals_tab import SignalsTab
from metrics_manager import MetricsManager
from utils import make_metric_line

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
            available.sort(
                key=lambda d: (str(d.get("interface", "")), str(d.get("channel", "")))
            )
            self.devices_found.emit(
                available,
                "",
            )
        except Exception as e:
            self.devices_found.emit([], f"Error scanning devices: {e}")


# DBC file table area
class DbcTable(DbcTableBase):
    def __init__(self, parent=None, get_busses=None, on_dbc_assignment_changed=None):
        super().__init__(parent, cols=4)
        self.setHorizontalHeaderLabels([" ", "DBC File", "Status", "Assigned Bus"])
        self.horizontalHeader().setSectionResizeMode(
            3, QHeaderView.ResizeMode.ResizeToContents
        )
        self.get_busses = get_busses  # function to get current busses
        self.on_dbc_assignment_changed = on_dbc_assignment_changed

        # Connect the dbcAdded signal to _add_bus_box
        self.dbcAdded.connect(self._dbc_added)
        self.dbcRemoved.connect(lambda _: self._dbc_removed())

    def _dbc_added(self, row):
        # Add dropdown for bus assignment
        combo = QComboBox()
        combo.setEditable(False)
        combo.addItem("")  # Unassigned
        if self.get_busses:
            for bus in self.get_busses():
                combo.addItem(bus)
        status = self.item(row, 2)
        if status and status.text() != "Valid":
            combo.setDisabled(True)
        self.setCellWidget(row, 3, combo)
        # Connect signal to update bus tab when assignment changes
        if self.on_dbc_assignment_changed:
            combo.currentIndexChanged.connect(self.on_dbc_assignment_changed)

    def _dbc_removed(self):
        if self.on_dbc_assignment_changed:
            self.on_dbc_assignment_changed()


# Singleton MetricsManager instance
metrics_manager = MetricsManager()


class MainWindow(QMainWindow):

    class BusReader(QThread):
        update_signals_requested = Signal(list)
        update_status = Signal(str)

        def __init__(self, bus, bus_name, parent=None, status_callback=None):
            super().__init__(parent)
            self.bus = bus
            self.bus_name = bus_name
            self.running = True
            self._dbc_lock = Lock()
            self.db = cantools.database.Database()
            self.update_signals_requested.connect(self._do_update_signals)
            self.update_status.connect(status_callback) if status_callback else None
            self.job_name: str = bus_name

        def run(self):
            while self.running:
                try:
                    msg = self.bus.recv(timeout=0.2)
                    if msg is None:
                        continue
                    with self._dbc_lock:
                        db = self.db
                    try:
                        decoded = db.decode_message(msg.arbitration_id, msg.data)
                        if decoded and isinstance(decoded, dict):
                            message_obj = db.get_message_by_frame_id(msg.arbitration_id)
                            for sig_name, value in decoded.items():
                                if not isinstance(value, (int, float)):
                                    continue
                                # Find signal object for unit if available
                                unit = ""
                                for sig in message_obj.signals:
                                    if sig.name == sig_name:
                                        unit = getattr(sig, "unit", "")
                                        break
                                metric_line = make_metric_line(
                                    metric_name=sig_name,
                                    message=message_obj.name,
                                    unit=unit,
                                    value=value,
                                    timestamp=msg.timestamp,
                                    job=self.job_name,
                                )
                                metrics_manager.add_metric(metric_line)
                    except Exception:
                        pass
                except Exception as e:
                    print(f"[{self.bus_name}] Error reading: {e}")

        def stop(self):
            self.running = False
            self.wait()

        def update_signals(self, messages):
            if hasattr(self, "update_status"):
                self.update_status.emit(f"Updating signals for {self.bus_name}...")
            self.update_signals_requested.emit(messages)

        def _do_update_signals(self, messages):
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
        # Set width to fit "500k"
        font_metrics = self.bitrate_combo.fontMetrics()
        text_width = (
            font_metrics.horizontalAdvance("500k") + 32
        )  # padding for dropdown arrow
        self.bitrate_combo.setFixedWidth(text_width)

        self.connect_btn = QPushButton("Connect")
        # Set width to fit "Connect"
        font_metrics = self.connect_btn.fontMetrics()
        text_width = (
            font_metrics.horizontalAdvance("Connect") + 24
        )  # padding for button
        self.connect_btn.setFixedWidth(text_width)

        self.disconnect_all_btn = QPushButton("Disconnect All")
        self.disconnect_all_btn.setToolTip("Disconnect from all devices")
        font_metrics = self.disconnect_all_btn.fontMetrics()
        text_width = font_metrics.horizontalAdvance("Disconnect All") + 24
        self.disconnect_all_btn.setFixedWidth(text_width)

        # Row 0: labels (reload button first)
        grid.addWidget(QLabel(""), 0, 0)  # Empty for button
        grid.addWidget(QLabel("Device:"), 0, 1)
        grid.addWidget(QLabel("Bitrate:"), 0, 2)
        grid.addWidget(QLabel(""), 0, 3)  # Empty for button alignment
        grid.addWidget(QLabel(""), 0, 4)  # Empty for button alignment

        # Row 1: controls (reload button first)
        grid.addWidget(self.refresh_btn, 1, 0)
        grid.addWidget(self.device_combo, 1, 1)
        grid.addWidget(self.bitrate_combo, 1, 2)
        grid.addWidget(self.connect_btn, 1, 3)
        grid.addWidget(self.disconnect_all_btn, 1, 4)

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
        # Set minimum height to fit 3 rows of chips (each chip ~32px + spacing)
        chip_height = 32
        spacing = self.chip_layout.spacing()
        rows = 3
        min_height = rows * chip_height + (rows - 1) * spacing + 16
        self.chip_scroll.setMinimumHeight(min_height)
        self.chip_scroll.setMaximumHeight(min_height + 32)
        # Make chip area expand horizontally with window
        self.chip_scroll.setSizePolicy(
            QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed
        )
        main_layout.addWidget(self.chip_scroll)

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
                layout.addWidget(dbc_widget)

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

        def disconnect_all_busses():
            for device, (bus, chip, reader) in list(self.connected_busses.items()):
                try:
                    reader.stop()
                except Exception:
                    pass
                bus.shutdown()
                self.chip_layout.removeWidget(chip)
                chip.deleteLater()
                del self.connected_busses[device]
                # Remove tab for this bus
                if device in self.bus_tabs:
                    idx = self.tabs.indexOf(self.bus_tabs[device])
                    if idx >= 0:
                        self.tabs.removeTab(idx)
                    del self.bus_tabs[device]

        self.disconnect_all_btn.clicked.connect(disconnect_all_busses)

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
                    widget = self.dbc_table.cellWidget(row, 3)
                    if isinstance(widget, QComboBox) and widget.currentText() == device:
                        for f in self.dbc_table.files:
                            item = self.dbc_table.item(row, 1)
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
        # Stop metrics_manager thread and timer gracefully
        if hasattr(metrics_manager, "stop"):
            metrics_manager.stop()

        super().closeEvent(event)

    def update_dbc_bus_dropdowns(self):
        # Update all bus assignment dropdowns in the DBC table
        for row in range(self.dbc_table.rowCount()):
            widget = self.dbc_table.cellWidget(row, 3)
            status = self.dbc_table.item(row, 2)

            # Disable if DBC file is not valid
            if status and status.text() != "Valid":
                widget.setDisabled(True)

            if isinstance(widget, QComboBox):
                current = widget.currentText()
                widget.clear()
                widget.addItem("")
                for bus in self.connected_busses.keys():
                    widget.addItem(bus)
                idx = widget.findText(current)
                if idx >= 0:
                    widget.setCurrentIndex(idx)
                    widget.setDisabled(False)

    def update_devices(self, devices_dict, error_msg):
        if error_msg:
            self.status.showMessage(error_msg)
        current = self.device_combo.currentText()
        self.device_combo.blockSignals(True)
        self.device_combo.clear()
        for dev_dict in devices_dict:
            name = f"{dev_dict['interface']} {dev_dict['channel']}"
            if dev_dict["interface"] == "kvaser" and dev_dict["device_name"].startswith(
                "Kvaser Virtual"
            ):
                name = f"{dev_dict['device_name']} {dev_dict['channel']}"
            data = {
                "name": name,
                "display_name": name,
                "interface": dev_dict["interface"],
                "channel": dev_dict["channel"],
                "AutoDetectedConfig": dev_dict,
            }
            self.device_combo.addItem(name, data)
            idx = self.device_combo.findText(name)
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
                        msg_item = table.item(row, 2)
                        if (
                            isinstance(cb, QCheckBox)
                            and cb.isChecked()
                            and name_item
                            and msg_item
                        ):
                            checked_signals.add((name_item.text(), msg_item.text()))
            idx = self.tabs.indexOf(self.bus_tabs[device])
            if idx >= 0:
                self.tabs.removeTab(idx)
        self._bus_checked_signals[device] = checked_signals
        # Get signals from DBC files assigned to this bus
        signals = self._get_signals_for_bus(device)
        # Create new tab for this bus
        tab = SignalsTab(parent=self, name=device, signals=signals)
        tab.signalsChecked.connect(
            lambda: (
                self._set_bus_filters_for_device(device),
                self._update_busreader_signals(device),
            )
        )

        self.tabs.addTab(tab, device)
        self.bus_tabs[device] = tab
        # Set filters initially
        self._set_bus_filters_for_device(device)
        # Connect the jobNameChanged signal to update the reader
        if device in self.connected_busses:
            _, _, reader = self.connected_busses[device]
            tab.jobNameChanged.connect(lambda name: setattr(reader, "job_name", name))

    def _get_signals_for_bus(self, device):
        # Find all DBCs assigned to this bus
        assigned = []
        for row in range(self.dbc_table.rowCount()):
            widget = self.dbc_table.cellWidget(row, 3)
            if isinstance(widget, QComboBox) and widget.currentText() == device:
                # Find the file path for this row
                for f in self.dbc_table.files:
                    item = self.dbc_table.item(row, 1)
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
                widget = self.dbc_table.cellWidget(row, 3)
                if isinstance(widget, QComboBox) and widget.currentText() == device:
                    for f in self.dbc_table.files:
                        item = self.dbc_table.item(row, 1)
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


if __name__ == "__main__":
    app = QApplication([])
    mw = MainWindow()
    mw.resize(600, 600)
    mw.show()
    sys.exit(app.exec())
