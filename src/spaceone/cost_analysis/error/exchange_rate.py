from spaceone.core.error import *


class ERROR_UNSUPPORTED_CURRENCY(ERROR_INVALID_ARGUMENT):
    _message = 'Unsupported currency. (supported currency = {supported_currency})'


class ERROR_CHANGE_STATE(ERROR_INVALID_ARGUMENT):
    _message = 'Set the exchange rate first before changing the state. (currency = {currency})'
