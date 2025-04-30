from spaceone.core.error import *


class ERROR_ADJUSTMENT_ORDER_BELOW_MINIMUM(ERROR_INVALID_ARGUMENT):
    _message = "Order must be greater than or equal to 1. (order = {order})"


class ERROR_ADJUSTMENT_ORDER_EXCEEDS_MAXIMUM(ERROR_INVALID_ARGUMENT):
    _message = "The provided order exceeds the maximum allowed order for existing adjustments. (order = {order}, maximum_order = {maximum_order})"
