"""
Process stock information using spark streaming.
Extracts the average, minimum and maximum price out of every rdd. 
Send back the generated information into kafka again with a specific topic name

Attributes:
    broker (str): Kafka broker address
    logger (logger): Logging handler
    new_topic (str): Kafka topic name to which the generated data is sent to
    topic (str): Kafka topic name from which raw data is got

"""

# - read from any kafka broker and topic
# - perform average every 5s
# - write data back to kafka

import sys
import logging
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError


topic = 'stock-price'
broker = 'localhost'
new_topic = 'average-stock-price'


logging.basicConfig()
logger = logging.getLogger('stream-process')
logger.setLevel(logging.DEBUG)

def process(rdd):
    """
    Process raw stock info and extract average, minimum and maximum price of every batch.
    
    Args:
        rdd (spark rdd): The spark rdd that is being processed
    
    """
    # - calculate average
    num_of_record = rdd.count()

    if num_of_record == 0:
        return

    min_price = float(sys.maxint)
    max_price = float(-sys.maxint)

    def get_min_price(min_price, record):
        """
        Compare two prices and reserves the smaller one. 
        
        Args:
            min_price (float): current minimum price
            record (record): raw data record in rdd

        """
        curr_price = float(json.loads(record[1].decode('utf-8'))[0].get('LastTradePrice'))
        if min_price > curr_price:
            min_price = curr_price

    def get_max_price(max_price, record):
        """
        Compare two prices and reserves the greater one. 

        
        Args:
            max_price (float): current maximum price
            record (record): raw data record in rdd
        """
        curr_pricce = float(json.loads(record[1].decode('utf-8'))[0].get('LastTradePrice'))
        if max_price < curr_pricce:
            max_price = curr_pricce

    # get min_price
    rdd.foreach(lambda record: get_min_price(min_price, record))

    # get max_price
    rdd.foreach(lambda record: get_max_price(max_price, record))

    price_sum = rdd.map(lambda record: float(json.loads(record[1].decode('utf-8'))[0].get('LastTradePrice')))
    price_sum = price_sum.reduce(lambda a, b: a + b)
    average = price_sum / num_of_record

    logger.info('received %d records from kafka, average price is %f' % (num_of_record, average))

    data = json.dumps({
        'average': average,
        'max_price': max_price,
        'min_price': min_price
        })

    try:
        kafka_producer.send(target_topic, value=data)
    except KafkaTimeoutError:
        logger.warn('unable to store data due to kafka timeout', exc_info=1)


if __name__ == '__main__':
    # args: kafka broker, kafka original topic, kafka target topic
    
    if len(sys.argv) != 4:
        print('usage: stream-process.py kafka-broker kafka-original-topic kafka-new-topic')
        exit(1)

    # initialize a Spark Context on localhost named AverageStockPrice, abling to handle 2 tasks simultaneously
    sc = SparkContext('local[2]', 'AverageStockPrice')
    sc.setLogLevel('DEBUG')

    # initialize a Streaming Context based on spark context. 
    # set up a mini-batch(Dstream) every 5 seconds
    ssc = StreamingContext(sc, 5)

    broker, topic, target_topic = sys.argv[1:]

    # Realize exactly 1 delivery via direct kafka stream
    direct_kafka_stream = KafkaUtils.createDirectStream(ssc, [topic], {'metadata.broker.list': broker})
    direct_kafka_stream.foreachRDD(process)

    kafka_producer = KafkaProducer(
        bootstrap_servers=broker
    )

    ssc.start()
    ssc.awaitTermination()
