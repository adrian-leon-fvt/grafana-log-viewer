
import sys
import can
import io
import contextlib
from can.typechecking import AutoDetectedConfig
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QLabel, QVBoxLayout, QWidget,
    QPushButton, QComboBox, QHBoxLayout,
    QScrollArea, QFrame, QSizePolicy
)
from PySide6.QtCore import QTimer, QThread, Signal, QObject
from PySide6.QtGui import QColor, QPalette


# Capture stdout while detecting
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    all_configs = can.detect_available_configs()

# Get the stdout output as a string
stdout_output = buf.getvalue()

# Print it for debugging (optional)
#print(stdout_output)

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
    cfg["interface"] for cfg in all_configs
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
            self.devices_found.emit([f"{cfg['interface']} {cfg['channel']}" for cfg in available],"")
        except Exception as e:
            self.devices_found.emit([], f"Error scanning devices: {e}")


class MainWindow(QMainWindow):

    def __init__(self):
        super().__init__()
        self.setWindowTitle("Live Stream Decoder")

        # Central widget and main layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout()

        # Status bar for messages
        self.status = self.statusBar()
        self.status.showMessage("Select a CAN device and bitrate.")

        # Grid layout for controls
        from PySide6.QtWidgets import QGridLayout
        grid = QGridLayout()

        # Controls
        self.device_combo = QComboBox()
        # Set width to fit typical socketcan/socketcand device names (e.g., 'socketcan:can0', 'socketcand:192.168.1.10:29536:can0')
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
        self.refresh_btn.setStyleSheet(f"""
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
        """)

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
        central_widget.setLayout(main_layout)

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


    def update_devices(self, device_names, error_msg):
        if error_msg:
            self.status.showMessage(error_msg)
        current = self.device_combo.currentText()
        self.device_combo.blockSignals(True)
        self.device_combo.clear()
        max_display_len = 36  # chars to show before truncating (fits most socketcand names)
        for name in device_names:
            display = name if len(name) <= max_display_len else name[:max_display_len-3] + '...'
            self.device_combo.addItem(display, name)
            idx = self.device_combo.findText(display)
            # Set tooltip for each item if truncated
            if idx >= 0:
                tooltip = name if len(name) > max_display_len else ""
                self.device_combo.setItemData(idx, tooltip, role=Qt.ItemDataRole.ToolTipRole)
        # Restore previous selection if possible
        idx = -1
        for i in range(self.device_combo.count()):
            if self.device_combo.itemData(i) == current or self.device_combo.itemText(i) == current:
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
            _bitrate = int(bitrate[:-1]) * 1000 if bitrate[-1] == 'k' else int(bitrate[:-1]) * 1000000
            bus = can.interface.Bus(interface=iface, channel=name, bitrate=int(_bitrate))
            self.status.showMessage(f"Connected to {device} at {bitrate} bps.")
            # Create chip for this connection
            chip = self._create_chip(device, bitrate)
            self.chip_layout.addWidget(chip)
            self.connected_busses[device] = (bus, chip)
            self._update_connect_button()
        except Exception as e:
            self.status.showMessage(f"Connection failed: {e}")

    def _create_chip(self, device, bitrate):
        from PySide6.QtWidgets import QFrame, QLabel, QPushButton, QHBoxLayout
        chip = QFrame()
        chip.setObjectName("chip")
        chip.setStyleSheet("""
            QFrame#chip {
                border: 1px solid #bbb;
                border-radius: 12px;
                padding: 2px 8px 2px 8px;
            }
        """)
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

from PySide6.QtCore import Qt

if __name__ == "__main__":
    app = QApplication([])
    mw = MainWindow()
    mw.resize(400, 200)
    mw.show()
    sys.exit(app.exec())