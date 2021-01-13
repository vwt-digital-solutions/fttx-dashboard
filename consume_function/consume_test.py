import json

import config
from consume_functions import process_asbuilt
import logging

logging.basicConfig(level=logging.INFO)


def fake_handler():
    subscription = "vwt-d-gew1-odh-hub-fttx-asbuilt-meters-push-sub"
    topic_config = config.topic_config.get(subscription)
    with open("/Users/markbruisten/Desktop/message_asbuilt.json", "r") as f:
        data = json.load(f)
        logging.info('Data loaded, start consume function')

    records = data[topic_config.get("subject")]
    process_asbuilt(records, topic_config)


if __name__ == "__main__":
    fake_handler()
