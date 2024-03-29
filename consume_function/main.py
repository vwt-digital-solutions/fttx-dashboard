import config
import logging

from consume_functions import parse_request, process_fiberconnect, process_default


def handler(request):
    try:
        data, subscription = parse_request(request)
        topic_config = config.topic_config.get(subscription)
        records = data[topic_config.get('subject')]
        logging.info(f'Read message from subscription {subscription}')
        if topic_config.get('name') == 'fiberconnect':
            process_fiberconnect(records, topic_config)
        elif topic_config.get('name') in ['baan-realisation', 'baan-budget']:
            process_default(records, topic_config)
        else:
            logging.info(f'subscription not found in consume_function: {subscription}')

    except Exception as e:
        logging.error(f'Extracting of data failed: {e}', exc_info=True)
        return 'Error', 500

    return 'OK', 200
