import json

import config
from consume_functions import process_fiberconnect
import logging

logging.basicConfig(level=logging.INFO)


def fake_handler():
    subscription = "vwt-d-gew1-odh-hub-it-fbr-connect-new-constr-push-sub"
    topic_config = config.topic_config.get(subscription)
    with open("2020_12_10_06.archive", "r") as f:
        data = json.load(f)

    for message in data:
        records = message[topic_config.get("subject")]
        process_fiberconnect(records, topic_config)


if __name__ == "__main__":
    fake_handler()
