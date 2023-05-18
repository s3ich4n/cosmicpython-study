from typing import Union

from pydantic import (
    BaseSettings,
    Field,
    PostgresDsn,
    RedisDsn,
)


class ServerDescriptionSettings(BaseSettings):
    API_STR: str = "/v1"

    OPENAPI_URL: str = f"/{API_STR}/openapi.json"

    REST_SERVICE_NAME: str = "서비스 이름"
    REST_SERVICE_DESCRIPTION: str = "서비스 설명"
    REST_SERVICE_VERSION: str = "0.1.0"


class DataSettings(BaseSettings):
    DB_URI: Union[PostgresDsn, str] = Field(
        env="DATABASE_PG_URL",
        default="sqlite+aiosqlite:///:memory:",
    )


class MessageBrokerSettings(BaseSettings):
    REDIS_URI: Union[RedisDsn, str] = Field(
        env="REDIS_URL",
        default="redis://localhost:6379/0",
    )


class Settings(BaseSettings):
    DEBUG: bool = Field(env="DEBUG", default=True)

    desc: ServerDescriptionSettings = ServerDescriptionSettings()
    data: DataSettings = DataSettings()
    broker: MessageBrokerSettings = MessageBrokerSettings()

    class Config:
        case_sensitive = True
