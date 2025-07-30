import cantools
import logging
from typing import Dict, Optional, List
from can import Message


class DBCDecoder:
    def __init__(self, dbc_paths: List[str]):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.db = self._load_dbc_files(dbc_paths)

    def _load_dbc_files(self, dbc_paths: List[str]) -> cantools.db.Database:
        """Load and merge multiple DBC files into single database"""
        db = cantools.db.Database()

        for path in dbc_paths:
            try:
                db.add_dbc_file(path)
                self.logger.info(f"Successfully loaded DBC file: {path}")
            except Exception as e:
                self.logger.error(f"Failed to load DBC file {path}: {str(e)}")
                raise

        return db

    def decode_message(self, msg: Message) -> Optional[Dict]:
        """
        Decode raw CAN message using DBC definitions

        Args:
            msg: Raw CAN message to decode
        Returns:
            Dictionary containing:
            - timestamp: Message timestamp
            - message_name: Name from DBC
            - signals: Dictionary of signal names to values
            or None if decoding fails
        """
        try:
            decoded_signals = self.db.decode_message(
                msg.arbitration_id, msg.data)
            # return {
            #     'timestamp': msg.timestamp,
            #     'message_name': self.db.get_message_by_frame_id(msg.arbitration_id).name,
            #     'signals': decoded_signals
            # }
            return {
                "message": self.db.get_message_by_frame_id(msg.arbitration_id),
                "decoded_signals": decoded_signals
            }
        except KeyError:
            # No matching message ID in DBC
            return None
        except Exception as e:
            self.logger.warning(
                f"Decoding failed for ID {msg.arbitration_id:04X}: {str(e)}"
            )
            return None

    def get_message_by_name(self, name: str) -> Optional[cantools.db.Message]:
        try:
            return self.db.get_message_by_name(name)
        except KeyError:
            return None
