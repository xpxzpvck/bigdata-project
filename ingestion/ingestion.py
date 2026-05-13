import json
import logging
import os
from requests_sse import EventSource
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

STREAM_URL = 'https://stream.wikimedia.org/v2/stream/page-create'
USER_AGENT = "WikipediaAnalyticsPipeline/1.0 (tepliakov.pn@ucu.edu.ua)"

KAFKA_BROKER = 'kafka:9092'
KAFKA_TOPIC = 'raw-page-creates'

def create_kafka_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5
        )
        logger.info(f"Successfuly connected to Kafka at {KAFKA_BROKER}")
        return producer
    except KafkaError as e:
        logger.error(f"Error connecting to Kafka: {e}")
        raise

def start_ingestion():
    producer = create_kafka_producer()
    last_id = None # Variable to keep track of the last processed event ID for reconnection purposes
    
    headers = {
        "User-Agent": USER_AGENT
    }

    while True:
        try:
            if last_id:
                headers['Last-Event-ID'] = last_id
                logger.info(f"Reconnecting. Resuming from event: {last_id}")
            else:
                logger.info("Establishing new connection to EventStreams...")

            with EventSource(STREAM_URL, headers=headers) as stream:
                for event in stream:
                    if event.type == 'message':
                        try:
                            change = json.loads(event.data)
                        except ValueError:
                            logger.warning("Received invalid JSON, skipping.")
                            continue

                        # Canary events are used by Wikimedia for testing and monitoring, we want to skip them
                        if change.get('meta', {}).get('domain') == 'canary':
                            continue
                        
                        domain = change.get('meta', {}).get('domain', 'unknown')
                        producer.send(
                            KAFKA_TOPIC, 
                            key=domain.encode('utf-8'), 
                            value=change
                        )
                        
                        last_id = event.last_event_id
                        
        except Exception as e:
            # Wikimedia servers forcefully disconnect every 15 minutes. 
            # This is expected behavior, so we just log it and continue to the next iteration of the loop.
            logger.warning(f"Connection interrupted ({e}). Attempting to reconnect...")

if __name__ == "__main__":
    start_ingestion()