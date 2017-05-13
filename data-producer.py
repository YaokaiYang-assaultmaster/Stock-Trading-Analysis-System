"""Summary

1. talk to any kafka and topic, configurable
2. fetch stock price every second

Attributes:
    kafka_broker (str): The ip:port address of Kafka broker
    logger (logger): Log of this program
    symbol (str): Abbreviation of the stock we want to record
    topic_name (str): Name of the Kafka topic that is used to store stock info
"""

from kafka import KafkaProducer
from googlefinance import getQuotes
from kafka.errors import KafkaError, KafkaTimeoutError
from apscheduler.schedulers.background import BackgroundScheduler

import atexit
import logging
import argparse
import json
import time

logging.basicConfig()
logger = logging.getLogger('data-producer')
logger.setLevel(logging.DEBUG)

symbol = 'AAPL'
topic_name = 'stock'
kafka_broker = '127.0.0.1:9092'


def shutdown_hook():
    """Summary

    Release resorces at exiting
    Speecifically, releases kafka producer and scheduler
    """
    try:
        producer.flush(10)
        logger.info('shutdown resources')
    except KafkaError:
        logger.warn('failed to flush kafka')
    finally:
        producer.close(10)
        schedule.shutdown()


def fetch_price(producer, symbol):
    """Summary

    Use googlefinance's getQuotes() api to quote a stock's info.
    Store the info into Kafka

    Args:
        producer (KafkaProducer):
            A kafka client that publishes records to the Kafka cluster
        symbol (str): Abbreviation of the stock we want to record
    """
    try:
        stock_info = json.dumps(getQuotes(symbol))

        logger.debug('received stock price %s' % (stock_info))
        producer.send(topic=topic_name, value=stock_info,
                      timestamp_ms=time.time())
    except KafkaTimeoutError as timeout_error:
        logger.warn(
            'failed to send stock price for %s to kafka due to timeout'
            % (symbol))
        print(timeout_error)
    except Exception:
        logger.warn(
            'failed to send stock price for %s due to unknown reason'
            % (symbol))


if __name__ == '__main__':
    # - argument parser
    parser = argparse.ArgumentParser()
    parser.add_argument('symbol', help='the symbol of the stock')
    parser.add_argument('topic_name', help='the kafka topic to push to')
    parser.add_argument('kafka_broker', help='the location of kafka broker')

    args = parser.parse_args()
    symbol = args.symbol
    topic_name = args.topic_name
    kafka_broker = args.kafka_broker

    producer = KafkaProducer(bootstrap_servers=kafka_broker)

    schedule = BackgroundScheduler()
    schedule.add_executor('threadpool')

    schedule.add_job(fetch_price, 'interval', [
                     producer, symbol], seconds=1, id=symbol)

    schedule.start()

    atexit.register(shutdown_hook)

    while True:
        pass
