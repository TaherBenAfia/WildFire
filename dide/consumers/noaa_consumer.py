from confluent_kafka import Consumer
import uuid
from dide.config.kafka.noaa_kafka_config import  NOAA_CONSUMER_CONFIG, NOAA_TOPIC
    


def main():
    consumer = Consumer(NOAA_CONSUMER_CONFIG)
    consumer.subscribe([NOAA_TOPIC])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f'Consumer error: {msg.error()}')
                continue

            key_bytes = msg.key()
            key = key_bytes.decode("utf-8") if key_bytes is not None else None
            value_bytes = msg.value()
            value = value_bytes.decode("utf-8") if value_bytes is not None else None
            print(f'Received message: key={key} value={value}')
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
