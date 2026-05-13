from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    cassandra_host: str
    cassandra_keyspace: str
    redis_host: str
    redis_port: int
    
    class Config:
        env_file = ".env"

settings = Settings()