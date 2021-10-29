from spaceone.core.error import *


class ERROR_ONLY_ONF_OF_PROJECT_OR_PROJECT_GROUP(ERROR_INVALID_ARGUMENT):
    _message = 'Only one of project_id or project_group_id is allowed.'
