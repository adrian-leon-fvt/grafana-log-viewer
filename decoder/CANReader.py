import can
import logging
from typing import Optional, Dict
from can import Message
from DBCDecoder import DBCDecoder


class CANReader:
    def __init__(self, interface: str, channel: str, dbc_decoder: Optional[DBCDecoder] = None):
        """
        Initialize CAN bus reader

        Args:
            interface: CAN interface type ('socketcan', 'virtual', etc.)
            channel: CAN channel name ('vcan0', 'can0', etc.)
            dbc_decoder: Optional DBC decoder instance
        """
        self.interface = interface
        self.channel = channel
        self.bus = None
        self.dbc_decoder = dbc_decoder
        self.logger = logging.getLogger(self.__class__.__name__)

    def connect(self) -> bool:
        try:
            self.bus = can.interface.Bus(
                interface=self.interface,
                channel=self.channel,
                receive_own_messages=False
            )
            self.logger.info(f"Connected to {self.channel}")
            return True
        except Exception as e:
            self.logger.error(f"Connection failed: {str(e)}")
            return False

    def read_message(self, timeout: float = 1.0) -> Optional[Message]:
        try:
            msg = self.bus.recv(timeout)
            if msg:
                self._log_message(msg)
            return msg
        except can.CanError as e:
            self.logger.error(f"Read error: {str(e)}")
            return None

    def read_decoded_message(self, timeout: float = 1.0) -> Optional[list]:
        msg = self.read_message(timeout)
        if msg and self.dbc_decoder:
            return [msg.timestamp, self.dbc_decoder.decode_message(msg)]
        return [None, msg]

    def _log_message(self, msg: Message):
        if self.dbc_decoder:
            decoded = self.dbc_decoder.decode_message(msg)
            if decoded:
                signals = ', '.join(f"{k}: {v}" for k,
                                    v in decoded['decoded_signals'].items())
                self.logger.info(
                    f"DECODED | {decoded['message'].name} | "
                    f"Signals: {signals} | "
                    f"Timestamp: {msg.timestamp:.1f}"
                )
                return

        # If decoding failed, log raw message
        data_str = ' '.join(f"{byte:02X}" for byte in msg.data)
        self.logger.info(
            f"RAW | ID: {msg.arbitration_id:04X} | "
            f"Data: {data_str} | "
            f"Timestamp: {msg.timestamp:.1f}"
        )

    # Proper shutdown
    def shutdown(self):
        if self.bus:
            self.bus.shutdown()
            self.logger.info("CAN connection closed")
