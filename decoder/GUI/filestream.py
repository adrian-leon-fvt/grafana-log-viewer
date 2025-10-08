import sys
import os
import optparse
import cantools
import cantools.database
from can import LogReader

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
from PySide6.QtGui import QColor, QPalette, QIcon

from decoder.GUI.signals_manager import SignalsManager
from decoder.GUI.metrics_manager import MetricsManager
from utils import make_metric_line


# Singleton MetricsManager instance
metrics_manager = MetricsManager()


class MainWindow(QMainWindow):

    def __init__(self):
        super().__init__()
        self.setWindowTitle("File Decoder")
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

        self.tabs.addTab(main_tab, "Main")

    def closeEvent(self, event):
        # Gracefully stop metrics_manager thread if running
        if hasattr(metrics_manager, "stop"):
            metrics_manager.stop()
        super().closeEvent(event)


if __name__ == "__main__":
    app = QApplication([])
    icon_path = os.path.join(os.path.dirname(__file__), "icon.png")
    app.setWindowIcon(QIcon(icon_path))
    mw = MainWindow()
    mw.showMaximized()
    mw.setWindowIcon(QIcon(icon_path))
    sys.exit(app.exec())
