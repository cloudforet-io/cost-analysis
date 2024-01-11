from spaceone.core.error import *


class ERROR_INVALID_DATE_RANGE(ERROR_INVALID_ARGUMENT):
    _message = '{reason}'


class ERROR_NOT_SUPPORT_QUERY_OPTION(ERROR_INVALID_ARGUMENT):
    _message = 'Not support query option. (query_option = {query_option})'


class ERROR_NOT_SUPPORT_OPERATOR(ERROR_INVALID_ARGUMENT):
    _message = "Not support operator. (key = {key}, supported_operator = {operator})"
