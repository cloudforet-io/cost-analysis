from pydantic import BaseModel

__all__ = ['DataSourceInitRequest', 'DataSourceVerifyRequest']


class DataSourceInitRequest(BaseModel):
    options: dict
    domain_id: str


class DataSourceVerifyRequest(BaseModel):
    options: dict
    secret_data: dict
    domain_id: str
