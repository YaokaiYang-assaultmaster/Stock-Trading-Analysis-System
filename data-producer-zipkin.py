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
from py_zipkin.zipkin import zipkin_span

import atexit
import logging
import argparse
import json
import time
import requests

logging.basicConfig()
logger = logging.getLogger('data-producer')
logger.setLevel(logging.DEBUG)

symbol = 'AAPL'
topic_name = 'stock'
kafka_broker = '127.0.0.1:9092'


def http_transport_handler(span):
    body = '\x0c\x00\x00\x00\x01' + span
    requests.post(
        'http://localhost:9411/api/v1/spans',
        data=body,
        headers={'Content-Type': 'application/x-thrift'}
        )


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


# Add instrumentation in code for zipkin tracing
@zipkin_span(service_name='StockTradingAnalysis', span_name='fetch_price')
def fetch_price(symbol):
    return json.dumps(getQuotes(symbol))


# Add instrumentation in code for zipkin tracing
@zipkin_span(service_name='StockTradingAnalysis', span_name='send_to_kafka')
def send_to_kafka(producer, price):
    try:
        logger.debug('retrived stock info %s', price)
        producer.send(topic=topic_name, value=price,
                      timestamp_ms=time.time())
        logger.debug('sent stock price for %s', symbol)

    except KafkaTimeoutError as timeout_error:
        logger.warn(
            'failed to send stock price for %s to kafka due to timeout'
            % (symbol), exc_info=1)
        print(timeout_error)
    except Exception:
        logger.warn(
            'failed to send stock price for %s due to unknown reason'
            % (symbol), exc_info=1)


def fetch_price_and_send(producer, symbol):
    """Summary
    Use googlefinance's getQuotes() api to quote a stock's info.
    Store the info into Kafka

    Args:
        producer (KafkaProducer):
            A kafka client that publishes records to the Kafka cluster
        symbol (str): Abbreviation of the stock we want to record
    """
    with zipkin_span(
        service_name='StockTradingAnalysis', 
        span_name='data_producer', 
        transport_handler=http_transport_handler, 
        sample_rate=100.0
        ):
        logger.debug('start to fetch stock price for %s', symbol)
        price = fetch_price(symbol)
        send_to_kafka(producer, price)


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

    schedule.add_job(fetch_price_and_send, 'interval', [
                     producer, symbol], seconds=1, id=symbol)

    schedule.start()

    atexit.register(shutdown_hook)

    while True:
        pass
