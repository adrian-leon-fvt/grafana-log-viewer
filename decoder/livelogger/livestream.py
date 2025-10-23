import sys
import logging
import requests
from pathlib import Path

if __name__ == "__main__":
    sys.path.append(str(Path(__file__).parent.parent.parent))

from decoder.config import *
from decoder.utils import *
from decoder.livelogger.DBCDecoder import DBCDecoder
from decoder.livelogger.CANReader import CANReader


def livestream(server: str):
    logger = logging.getLogger("livestream")
    setup_simple_logger(logger, level=logging.INFO, format=LOG_FORMAT)

    if is_victoriametrics_online(server):
        dbc_decoder = None
        if DBC_FILE_PATHS:
            logger.info("Initializing DBC decoder...")
            dbc_decoder = DBCDecoder(DBC_FILE_PATHS)

        # Initialize CAN reader
        logger.info("Initializing CAN reader...")
        can_reader = CANReader(
            interface=CAN_INTERFACE, channel=CAN_CHANNEL, dbc_decoder=dbc_decoder
        )

        if not can_reader.connect():
            logger.error("Failed to initialize CAN interface")
            return

        logger.info("Starting CAN monitoring...")
        while True:
            try:
                result = can_reader.read_decoded_message()
                if result is None:
                    continue
                timestamp, message_data = result
                if not message_data:
                    continue
                message = message_data["message"]
                decoded_signals = message_data["decoded_signals"]
                if message:
                    for signal in decoded_signals.keys():
                        value, unit = decoded_signals[signal]
                        data = make_metric_line(
                            message.name,
                            signal,
                            unit,
                            value,
                            timestamp,
                            job="d65_livestream",
                        )
                        try:
                            requests.post(
                                server + vmapi_import_prometheus, data="".join(data)
                            )
                        except Exception as e:
                            logging.error(f"\n ‼️ Error sending data: {e}")

            except KeyboardInterrupt:  # Shutting down properly
                break

        logger.info("Shutting down...")
        can_reader.shutdown()
