import json
import time
import logging
from requests_sse import EventSource
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Конфігурація
STREAM_URL = 'https://stream.wikimedia.org/v2/stream/page-create'
KAFKA_BROKER = 'kafka:9092'  # Адреса брокера з docker-compose
KAFKA_TOPIC = 'raw-page-creates'
USER_AGENT = "WikipediaAnalyticsPipeline/1.0 (student@example.com)"

def create_kafka_producer():
    """Створює та повертає Kafka Producer."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5
        )
        logger.info(f"Успішно підключено до Kafka на {KAFKA_BROKER}")
        return producer
    except KafkaError as e:
        logger.error(f"Помилка підключення до Kafka: {e}")
        raise

def start_ingestion():
    producer = create_kafka_producer()
    last_id = None  # Зберігаємо ID останньої події для відновлення потоку
    
    headers = {
        "User-Agent": USER_AGENT
    }

    while True:
        try:
            # Додаємо Last-Event-ID до заголовків, якщо ми перепідключаємося
            if last_id:
                headers['Last-Event-ID'] = last_id
                logger.info(f"Перепідключення. Відновлення з події: {last_id}")
            else:
                logger.info("Встановлення нового з'єднання з EventStreams...")

            with EventSource(STREAM_URL, headers=headers) as stream:
                for event in stream:
                    if event.type == 'message':
                        try:
                            change = json.loads(event.data)
                        except ValueError:
                            logger.warning("Отримано невалідний JSON, пропускаємо.")
                            continue

                        # 1. Відфільтровуємо canary події (підводний камінь)
                        if change.get('meta', {}).get('domain') == 'canary':
                            continue
                        
                        # 2. Відправляємо подію в Kafka
                        producer.send(KAFKA_TOPIC, value=change)
                        
                        # 3. Оновлюємо last_id для потенційного перепідключення
                        last_id = event.last_event_id
                        
        except Exception as e:
            # Сервери Wikimedia примусово розривають з'єднання кожні 15 хвилин.
            # Це очікувана поведінка, тому ми просто логуємо і йдемо на нову ітерацію циклу.
            logger.warning(f"З'єднання перервано ({e}). Перепідключення через 5 секунд...")
            time.sleep(5)

if __name__ == "__main__":
    # Даємо Kafka час на запуск (особливо актуально при старті через docker-compose)
    logger.info("Очікування запуску Kafka (10 секунд)...")
    time.sleep(10)
    start_ingestion()