from PySide6.QtWidgets import QFrame, QHBoxLayout, QLabel, QPushButton, QSizePolicy
from PySide6.QtCore import QTimer


class BusChip(QFrame):
    def __init__(self, device, bitrate, disconnect_callback=None, parent=None):
        super().__init__(parent)
        self.setObjectName("chip")
        self.setStyleSheet(
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
        # Bus state indicator
        self.state_indicator = QLabel()
        self.state_indicator.setFixedSize(16, 16)
        self.state_indicator.setStyleSheet(
            "border-radius: 8px; background: yellow; border: 1px solid #888;"
        )
        layout.addWidget(self.state_indicator)

        label = QLabel(f"{device} @ {bitrate}")
        layout.addWidget(label)

        close_btn = QPushButton("❌")
        close_btn.setFixedSize(20, 20)
        close_btn.setStyleSheet("border: none; background: transparent;")
        if disconnect_callback:
            close_btn.clicked.connect(lambda: disconnect_callback(device))
        layout.addWidget(close_btn)
        self.setLayout(layout)
        self.setSizePolicy(QSizePolicy.Policy.Maximum, QSizePolicy.Policy.Fixed)

        # Attach indicator and blink logic
        self.blink_timer = QTimer()
        self.blink_timer.setInterval(100)  # 0.1Hz = 10s, but use 5s for visible blink
        self.blink_state = False
        self.last_blink = 0
        self.blink_timer.timeout.connect(lambda: self.set_indicator_blink(False))
        self.blink_timer.setSingleShot(True)

    def set_state(self, state: str):
        # state: 'error', 'active', 'passive'
        color = {
            "error": "red",
            "active": "green",
            "passive": "yellow",
        }.get(state, "gray")
        self.state_indicator.setStyleSheet(
            f"border-radius: 8px; background: {color}; border: 1px solid #888;"
        )
        self.state_indicator.setToolTip(state)

    def set_indicator_blink(self, blink):
        # Skip if already blinking
        if self.blink_timer.isActive() == blink:
            return

        # blink: True to blink, False to restore
        if blink:
            self.state_indicator.setStyleSheet(
                self.state_indicator.styleSheet() + "; border: 2px solid #222;"
            )
            self.blink_timer.start()
        else:
            # Remove extra border
            style = self.state_indicator.styleSheet()
            style = style.replace("; border: 2px solid #222;", "")
            self.state_indicator.setStyleSheet(style)


if __name__ == "__main__":
    import sys
    from PySide6.QtWidgets import QApplication, QVBoxLayout, QWidget, QPushButton

    app = QApplication(sys.argv)
    win = QWidget()
    layout = QVBoxLayout(win)
    chip = BusChip("TestBus", "500k")
    layout.addWidget(chip)
    # Add buttons to test state and blink
    btn_active = QPushButton("Set Active")
    btn_error = QPushButton("Set Error")
    btn_passive = QPushButton("Set Passive")
    btn_blink = QPushButton("Blink")
    btn_active.clicked.connect(lambda: chip.set_state("active"))
    btn_error.clicked.connect(lambda: chip.set_state("error"))
    btn_passive.clicked.connect(lambda: chip.set_state("passive"))
    btn_blink.clicked.connect(lambda: chip.set_indicator_blink(True))
    layout.addWidget(btn_active)
    layout.addWidget(btn_error)
    layout.addWidget(btn_passive)
    layout.addWidget(btn_blink)
    win.resize(300, 100)
    win.show()
    sys.exit(app.exec())
