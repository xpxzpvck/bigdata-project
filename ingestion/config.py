from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    kafka_broker: str
    kafka_topic_raw_events: str

settings = Settings()