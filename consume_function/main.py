import config
import logging

from consume_functions import parse_request, process_fiberconnect, process_asbuilt, toggles


def handler(request):
    try:
        data, subscription = parse_request(request)
        topic_config = config.topic_config.get(subscription)
        records = data[topic_config.get('subject')]
        if not toggles.consume_meters:
            logging.info(f'Read message from subscription {subscription}')
            process_fiberconnect(records, topic_config)
        else:
            logging.info(f'Read message from subscription {subscription}')

            if topic_config.get('name') == 'fiberconnect':
                process_fiberconnect(records, topic_config)
            elif topic_config.get('name') == 'asbuilt-meters':
                process_asbuilt(records, topic_config)

    except Exception as e:
        logging.error(f'Extracting of data failed: {e}')
        return 'Error', 500

    return 'OK', 200
