"""Summary
- point to any kafka cluster and topic
- point to any cassandra cluster + table
- read data from given kafka topic
- store processed data into cassandra table
Attributes:
    cassandra_cluster (str): the list of contact points to try connecting for cluster discovery
    kafka_cluster (str): bootstrap_servers the consumer should contact to bootstrap initial kafka cluster metadata
    keyspace (str): name of the keyspace where table will be created
    logger (logger): a python logger object used for logging
    table (str): name of the table in the cassandra [keyspace] where the processed data will be stored
    topic_name (str): kafka topic name where we read raw data from. 
"""



import argparse
import atexit
import datetime
import logging
import json
from cassandra.cluster import Cluster
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# default values
topic_name = 'stock-analyzer'
kafka_cluster = 'localhost:9092'
cassandra_cluster = 'localhost'
keyspace = 'bittiger'
table = 'stock'

# create logger object
logging.basicConfig()
logger = logging.getLogger('data_storage')
logger.setLevel(logging.DEBUG)


def shutdown_hook(consumer, session):
    """Summary
    release resources while shutdown
    
    Args:
        consumer (KafkaConsumer): the kafka consumer object that will be closed
        session (Session): the session object that will be shutdown
    """
    logger.debug('start to release resources')
    consumer.close()
    session.shutdown()


def persist_data(message, session, prepared_insert_stmt):
    """Summary
    process raw message from kafka and store data into cassandra
    
    Args:
        message (JSON): JSON string that contains the raw data
        session (Session): the session object that is connected to current cassandra cluster 
    """
    try:
        logger.debug('start to save message %s' % (message, ))
        parsed_message = json.loads(message)[0]
        symbol = parsed_message.get('StockSymbol')
        price = float(parsed_message.get('LastTradePrice'))
        trade_time = parsed_message.get('LastTradeDateTime')

        # convert the datetime string into a datetime object
        trade_time = datetime.datetime.strptime(trade_time, "%Y-%m-%dT%H:%M:%SZ")

        session.execute(prepared_insert_stmt.bind((symbol, trade_time, price)))

        logger.debug('saved message to cassandra')
    except Exception as err:

        # Record both our message and error trace into log file
        logger.error('cannot save message', exc_info=1)


def main():
    """Summary
    initialize kafka consumer and kafka session.
    create database if not exists. 
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('topic_name', help='target kafka topic')
    parser.add_argument('kafka_cluster', help='target kafka cluster')
    parser.add_argument('cassandra_cluster', help='target cassandra cluster')
    parser.add_argument('keyspace', help='target keyspace')
    parser.add_argument('table', help='target table')

    args = parser.parse_args()
    topic_name = args.topic_name
    kafka_cluster = args.kafka_cluster
    cassandra_cluster = args.cassandra_cluster
    keyspace = args.keyspace
    table = args.table

    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=kafka_cluster
    )

    cassandra_cluster_obj = Cluster(
        contact_points=cassandra_cluster.split(',')
    )

    session = cassandra_cluster_obj.connect()

    session.execute(
        """
        CREATE KEYSPACE if not exists %s 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'} 
        AND durable_writes = 'true'
        """
        % (keyspace, )
    )

    session.set_keyspace(keyspace)

    session.execute(
        """
        CREATE TABLE if not exists %s
        (stock_symbol text, trade_time timestamp, trade_price float, PRIMARY KEY (stock_symbol, trade_time))
        """
        % (table, )
    )

    atexit.register(shutdown_hook, consumer, session)

    # prepare a statement for insertation
    # reuse the statement for performance
    prepared_insert_stmt = session.prepare(
        """
        INSERT INTO %s (stock_symbol, trade_time, trade_price) VALUES (?, ?, ?)
        """
        % (table, )
        )
    for msg in consumer:
        persist_data(msg.value, session, prepared_insert_stmt)

if __name__ == '__main__':
    main()
