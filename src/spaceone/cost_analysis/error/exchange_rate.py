from spaceone.core.error import *


class ERROR_UNSUPPORTED_CURRENCY(ERROR_INVALID_ARGUMENT):
    _message = 'Unsupported currency. (supported currency = {supported_currency})'
