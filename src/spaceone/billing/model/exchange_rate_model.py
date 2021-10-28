from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class ExchangeRate(MongoModel):
    currency = StringField(max_length=40, unique_with='domain_id')
    rate = FloatField(required=True)
    domain_id = StringField(max_length=255)

    meta = {
        'updatable_fields': [
            'rate'
        ],
        'ordering': [
            'currency'
        ],
        'indexes': [
            'currency',
            'domain_id'
        ]
    }
