FROM python:2.7.13-alpine

RUN mkdir /code
WORKDIR /code
ADD data-producer.py /code
ADD requirements.txt /code
RUN pip install -r requirements.txt

CMD python data-producer.py AAPL stock-analyzer localhost:9092